"""Re-check availability of previously scraped Mudah listings.

Reads the master Excel file, picks rows due for re-checking based on a cadence
policy, probes each listing URL via MudahClient (shared throttle), classifies
the response, and writes back four summary columns plus an append-only log.

Schema additions to the master file:
    first_seen_at      : first time we saw this ads_id
    last_seen_at       : last time the listing was confirmed AVAILABLE
    last_checked_at    : last time we probed (regardless of outcome)
    availability_status: 'available' | 'unavailable' | 'unknown'

Append-only audit log:
    data/master/availability_log.csv  with columns:
        ads_id, url, checked_at, http_status, detected_status

detected_status taxonomy:
    available     : HTTP 200 and __NEXT_DATA__ has the ad's adDetails block
    soft_404      : HTTP 200 but the ad block is gone (Mudah's "ad not found" page)
    removed       : HTTP 404 or 410
    blocked       : HTTP 403 (Cloudflare bot block — TRANSIENT, not unavailable)
    transient     : HTTP 5xx, timeout, connection error
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict

import pandas as pd

from mudah_client import MudahClient


# -----------------------------------------------------------------------------
# Logging — same UTF-8 setup as script.py
# -----------------------------------------------------------------------------

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    pass

os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/recheck.log', encoding='utf-8'),
        logging.StreamHandler(),
    ],
)


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

NEW_COLUMNS = ['first_seen_at', 'last_seen_at', 'last_checked_at', 'availability_status']
LOG_PATH = 'data/master/availability_log.csv'
LOG_COLUMNS = ['ads_id', 'url', 'checked_at', 'http_status', 'detected_status']


# -----------------------------------------------------------------------------
# Classification
# -----------------------------------------------------------------------------

def classify_response(status_code: Optional[int], body: Optional[str], ads_no: str) -> str:
    """Return one of: available, soft_404, removed, blocked, transient."""
    if status_code is None:
        return 'transient'
    if status_code in (404, 410):
        return 'removed'
    if status_code == 403:
        return 'blocked'
    if status_code >= 500:
        return 'transient'
    if status_code != 200 or not body:
        return 'transient'

    # 200 OK — verify the ad block actually exists in __NEXT_DATA__
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body, re.S)
    if not match:
        return 'soft_404'
    try:
        data = json.loads(match.group(1))
        ad = (
            data.get('props', {})
                .get('initialState', {})
                .get('adDetails', {})
                .get('byID', {})
                .get(ads_no)
        )
        if ad and ad.get('attributes'):
            return 'available'
        return 'soft_404'
    except (json.JSONDecodeError, AttributeError):
        return 'soft_404'


def status_from_detected(detected: str, previous: Optional[str]) -> str:
    """Collapse the detailed detected_status into the public availability_status.

    Transient outcomes (blocked, transient) keep the previous status — we don't
    know the truth, so don't lie. If there was no previous status, return 'unknown'.
    """
    if detected == 'available':
        return 'available'
    if detected in ('removed', 'soft_404'):
        return 'unavailable'
    # blocked or transient
    return previous if previous in ('available', 'unavailable') else 'unknown'


# -----------------------------------------------------------------------------
# Cadence policy
# -----------------------------------------------------------------------------

def should_recheck(row: pd.Series, now: datetime) -> bool:
    """Decide whether this row is due for re-checking.

    Policy:
      - never checked before -> always check
      - currently 'unavailable' AND last_checked > 14 days ago -> stop checking
      - first 7 days from first_seen -> daily
      - 7-30 days -> every 3 days
      - 30+ days -> weekly
    """
    last_checked = _parse_dt(row.get('last_checked_at'))
    if last_checked is None:
        return True

    if row.get('availability_status') == 'unavailable':
        if (now - last_checked) > timedelta(days=14):
            return False  # graveyard — give up

    first_seen = _parse_dt(row.get('first_seen_at')) or last_checked
    age = now - first_seen
    elapsed = now - last_checked

    if age <= timedelta(days=7):
        return elapsed >= timedelta(hours=20)   # ~daily
    if age <= timedelta(days=30):
        return elapsed >= timedelta(days=3)
    return elapsed >= timedelta(days=7)


def _parse_dt(value) -> Optional[datetime]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        try:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None


# -----------------------------------------------------------------------------
# Master file I/O
# -----------------------------------------------------------------------------

def load_master(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Master file not found: {path}")
    df = pd.read_excel(path)
    for col in NEW_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def save_master(df: pd.DataFrame, path: str) -> None:
    df.to_excel(path, index=False)


def append_log(rows: List[Dict]) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    df = pd.DataFrame(rows, columns=LOG_COLUMNS)
    header = not os.path.exists(LOG_PATH)
    df.to_csv(LOG_PATH, mode='a', header=header, index=False, encoding='utf-8')


# -----------------------------------------------------------------------------
# Core re-check loop
# -----------------------------------------------------------------------------

def recheck(master_path: str, limit: Optional[int] = None, force_all: bool = False, dry_run: bool = False) -> None:
    df = load_master(master_path)
    if 'url' not in df.columns or 'ads_id' not in df.columns:
        raise ValueError("Master file must have 'url' and 'ads_id' columns. Run a scrape first.")

    now = datetime.now()
    if force_all:
        due_mask = df['url'].notna() & (df['url'] != '')
    else:
        due_mask = df.apply(lambda r: bool(r.get('url')) and should_recheck(r, now), axis=1)

    due_idx = df.index[due_mask].tolist()
    if limit is not None:
        due_idx = due_idx[:limit]

    logging.info(f"Master: {len(df)} rows. Due for re-check: {len(due_idx)}.")
    if dry_run:
        logging.info("Dry run — no requests will be made.")
        return

    client = MudahClient()
    log_rows: List[Dict] = []

    for n, idx in enumerate(due_idx, 1):
        row = df.loc[idx]
        url = str(row['url'])
        ads_no = str(row['ads_id'])
        previous_status = row.get('availability_status') if isinstance(row.get('availability_status'), str) else None

        status_code, body = client.get_status(url)
        detected = classify_response(status_code, body, ads_no)
        new_status = status_from_detected(detected, previous_status)
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if _parse_dt(df.at[idx, 'first_seen_at']) is None:
            df.at[idx, 'first_seen_at'] = ts
        df.at[idx, 'last_checked_at'] = ts
        if detected == 'available':
            df.at[idx, 'last_seen_at'] = ts
        df.at[idx, 'availability_status'] = new_status

        log_rows.append({
            'ads_id': ads_no,
            'url': url,
            'checked_at': ts,
            'http_status': status_code if status_code is not None else '',
            'detected_status': detected,
        })

        logging.info(f"[{n}/{len(due_idx)}] {ads_no} -> http={status_code} detected={detected} status={new_status}")

        # Periodic flush so a crash mid-run doesn't lose progress
        if n % 25 == 0:
            save_master(df, master_path)
            append_log(log_rows)
            log_rows = []

    save_master(df, master_path)
    append_log(log_rows)
    logging.info("Re-check complete.")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Re-check availability of scraped Mudah listings")
    parser.add_argument("--master", default="data/master/MasterMudahCarData.xlsx", help="Path to master Excel file")
    parser.add_argument("--limit", type=int, default=None, help="Max number of rows to check this run")
    parser.add_argument("--all", action="store_true", help="Ignore cadence policy and re-check every row that has a URL")
    parser.add_argument("--dry-run", action="store_true", help="Compute the due set and log it, but make no HTTP requests")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    recheck(args.master, limit=args.limit, force_all=args.all, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

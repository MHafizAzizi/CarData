"""Re-check availability of previously scraped Mudah listings (SQLite-backed).

Reads candidate rows from the per-category SQLite database, probes each
listing URL via MudahClient (shared throttle), classifies the response, and
writes back four summary columns on `listings` plus an append row to
`availability_checks`.

Categories:
    cars        -> data/master/cardata_cars.db
    motorcycles -> data/master/cardata_motorcycles.db
    both        -> run cars then motorcycles through ONE MudahClient so the
                   3-4s throttle spans both DBs in a single process.

detected_status taxonomy (stored in availability_checks.detected_status):
    available : HTTP 200 and __NEXT_DATA__ has the ad's adDetails block
    soft_404  : HTTP 200 but the ad block is gone (Mudah's "ad not found" page)
    removed   : HTTP 404 or 410
    blocked   : HTTP 403 (Cloudflare bot block — TRANSIENT, not unavailable)
    transient : HTTP 5xx, timeout, connection error

availability_status (the public summary):
    available      <- detected_status='available'
    unavailable    <- detected_status in ('removed','soft_404')
    (unchanged)    <- detected_status in ('blocked','transient')  — preserve previous
                      to avoid lying when we got rate-limited or hit a transient error
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from db import connect, db_path_for, CATEGORIES
from mudah_client import MudahClient

# Anchor all paths to the project root regardless of cwd
_ROOT = Path(__file__).resolve().parent.parent
_LOGS_DIR = _ROOT / "logs"

# -----------------------------------------------------------------------------
# Logging — same UTF-8 setup as script.py
# -----------------------------------------------------------------------------

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    pass

_LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(_LOGS_DIR / 'recheck.log', encoding='utf-8'),
        logging.StreamHandler(),
    ],
)


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
    """Collapse detected_status into the public availability_status.

    Transient outcomes preserve the previous status so we never lie about
    availability based on a 403/timeout. If there was no previous status,
    return 'unknown'.
    """
    if detected == 'available':
        return 'available'
    if detected in ('removed', 'soft_404'):
        return 'unavailable'
    return previous if previous in ('available', 'unavailable') else 'unknown'


# -----------------------------------------------------------------------------
# Cadence policy
# -----------------------------------------------------------------------------

def should_recheck(row: sqlite3.Row, now: datetime) -> bool:
    """Decide whether this row is due for re-checking.

    Policy:
      - never checked before -> always check
      - currently 'unavailable' AND last_checked > 14 days ago -> stop checking (graveyard)
      - first 7 days from first_seen -> daily-ish (>= 20h since last check)
      - 7-30 days -> every 3 days
      - 30+ days -> weekly
    """
    last_checked = _parse_dt(row['last_checked_at'])
    if last_checked is None:
        return True

    if row['availability_status'] == 'unavailable':
        if (now - last_checked) > timedelta(days=14):
            return False

    first_seen = _parse_dt(row['first_seen_at']) or last_checked
    age = now - first_seen
    elapsed = now - last_checked

    if age <= timedelta(days=7):
        return elapsed >= timedelta(hours=20)
    if age <= timedelta(days=30):
        return elapsed >= timedelta(days=3)
    return elapsed >= timedelta(days=7)


def _parse_dt(value) -> Optional[datetime]:
    if value is None:
        return None
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
# Core re-check loop
# -----------------------------------------------------------------------------

def select_due_rows(
    conn: sqlite3.Connection,
    *,
    force_all: bool,
    limit: Optional[int],
) -> List[sqlite3.Row]:
    """Read all rows with a URL, then apply the cadence filter in Python.

    Could be pushed into SQL, but the tiered policy is cleaner to read here
    and the row count is small enough (tens of thousands max) that filtering
    in Python is essentially free.
    """
    rows = conn.execute(
        "SELECT ads_id, url, first_seen_at, last_seen_at, last_checked_at, availability_status "
        "FROM listings WHERE url IS NOT NULL AND url != ''"
    ).fetchall()
    now = datetime.now()
    due = rows if force_all else [r for r in rows if should_recheck(r, now)]
    if limit is not None:
        due = due[:limit]
    return due


def recheck_category(
    category: str,
    *,
    client: MudahClient,
    limit: Optional[int],
    force_all: bool,
    dry_run: bool,
) -> None:
    db_file = db_path_for(category)
    if not os.path.exists(db_file):
        logging.warning(f"[{category}] DB not found at {db_file} — skipping. Run migrate_xlsx_to_db.py or scrape with --update-db first.")
        return

    conn = connect(category)
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    due = select_due_rows(conn, force_all=force_all, limit=limit)
    logging.info(f"[{category}] DB rows: {total}. Due for re-check: {len(due)}.")

    if dry_run:
        logging.info(f"[{category}] dry run — no requests will be made.")
        return

    for n, row in enumerate(due, 1):
        ads_id = row['ads_id']
        url = row['url']
        ads_no = str(ads_id)
        previous_status = row['availability_status']

        status_code, body = client.get_status(url)
        detected = classify_response(status_code, body, ads_no)
        new_status = status_from_detected(detected, previous_status)
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with conn:
            if detected == 'available':
                conn.execute(
                    "UPDATE listings SET last_seen_at=?, last_checked_at=?, availability_status=? WHERE ads_id=?",
                    (ts, ts, new_status, ads_id),
                )
            else:
                conn.execute(
                    "UPDATE listings SET last_checked_at=?, availability_status=? WHERE ads_id=?",
                    (ts, new_status, ads_id),
                )
            conn.execute(
                "INSERT INTO availability_checks (ads_id, checked_at, http_status, detected_status) "
                "VALUES (?, ?, ?, ?)",
                (ads_id, ts, status_code, detected),
            )

        logging.info(
            f"[{category}] [{n}/{len(due)}] {ads_id} -> http={status_code} detected={detected} status={new_status}"
        )

    logging.info(f"[{category}] re-check complete.")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Re-check availability of scraped Mudah listings (SQLite)")
    parser.add_argument(
        "--category",
        choices=[*CATEGORIES, "both"],
        default="both",
        help="Which database(s) to re-check (default: both, sequentially through one client)",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max number of rows to check (per category)")
    parser.add_argument("--all", action="store_true", help="Ignore cadence and re-check every row that has a URL")
    parser.add_argument("--dry-run", action="store_true", help="Compute the due set and log it; make no HTTP requests")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    targets: Iterable[str] = CATEGORIES if args.category == "both" else (args.category,)
    client = MudahClient()  # ONE client across categories so the throttle is shared

    for cat in targets:
        recheck_category(cat, client=client, limit=args.limit, force_all=args.all, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

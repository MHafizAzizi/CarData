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
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple

from db import connect, db_path_for, CATEGORIES
from mudah_client import MudahClient

# DB column holding the make name, per category (set by clean.py title-casing).
MAKE_COL = {
    "cars": "make",
    "motorcycles": "motorcycle_make",
}

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

def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Return True if `column` exists on `table` in the connected DB."""
    return any(
        r[1] == column
        for r in conn.execute(f"PRAGMA table_info({table})")
    )


def _infer_sold(ad_expiry_str: Optional[str], now: datetime) -> str:
    """Classify a just-disappeared listing as likely_sold, likely_expired, or unknown.

    Logic:
        disappeared before ad_expiry  -> 'likely_sold'   (seller removed it themselves)
        disappeared at/after ad_expiry -> 'likely_expired' (listing lapsed)
        no ad_expiry available         -> 'unknown'
    """
    if not ad_expiry_str:
        return "unknown"
    try:
        # API returns 'YYYY-MM-DD HH:MM:SS'; fromisoformat needs 'T' separator on
        # older Python versions, so normalise the space.
        expiry = datetime.fromisoformat(ad_expiry_str.replace(" ", "T"))
        return "likely_sold" if now < expiry else "likely_expired"
    except (ValueError, TypeError):
        return "unknown"


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

    ad_expiry is included only when the column exists (schema v5+); older DBs
    get NULL for every row so _infer_sold() returns 'unknown'.
    """
    extra = ", ad_expiry" if _has_column(conn, "listings", "ad_expiry") else ""
    # Include the make column (cars: make, motorcycles: motorcycle_make) so the
    # API recheck can gate the unavailable decision on per-make completion.
    for mc in ("make", "motorcycle_make"):
        if _has_column(conn, "listings", mc):
            extra += f", {mc}"
            break
    rows = conn.execute(
        f"SELECT ads_id, url, first_seen_at, last_seen_at, last_checked_at, "
        f"availability_status{extra} "
        "FROM listings WHERE url IS NOT NULL AND url != ''"
    ).fetchall()
    now = datetime.now()
    due = rows if force_all else [r for r in rows if should_recheck(r, now)]
    if limit is not None:
        due = due[:limit]
    return due


def _sweep_active_ids(
    client,
    category: str,
    *,
    page_sleep: float = 1.5,
    max_page_retries: int = 4,
) -> Tuple[Set[int], Set[str]]:
    """Sweep all makes via EagleSearch API → (active_ids, completed_makes_cf).

    ``active_ids`` is the set of every ``ads_id`` currently returned by the API
    across all makes. ``completed_makes_cf`` is the casefolded set of make names
    whose pagination finished cleanly (no retry exhaustion) — only listings of
    a *completed* make may be safely marked unavailable, so a Cloudflare block
    on one make never produces false "sold" flags for that make.

    Mirrors the resilient page loop in ``backfill_ad_expiry.fetch_expiry_for_make``:
    each page goes through ``client.fetch_page`` (throttle + network/429 retry
    baked in) wrapped in outer exponential backoff on ``EagleAPIError``.
    """
    from eagle_client import MAX_LIMIT, MAX_OFFSET, EagleAPIError
    from backfill_ad_expiry import load_makes

    makes = load_makes(category)
    active: Set[int] = set()
    completed: Set[str] = set()
    failed: List[str] = []

    for i, make in enumerate(makes, 1):
        make_id = str(int(make["id"]))
        make_name = make["name"]
        offset = 0
        ok = True
        before = len(active)

        while offset < MAX_OFFSET:
            ads = None
            for attempt in range(max_page_retries + 1):
                try:
                    ads, _meta = client.fetch_page(
                        category, offset=offset, limit=MAX_LIMIT, make_id=make_id,
                    )
                    break
                except EagleAPIError as e:
                    if attempt < max_page_retries:
                        wait = page_sleep * (2 ** attempt)
                        logging.warning(
                            f"  [{make_name}] offset={offset} {e} — "
                            f"retry {attempt + 1}/{max_page_retries} in {wait:.1f}s"
                        )
                        time.sleep(wait)
                    else:
                        logging.error(
                            f"  [{make_name}] offset={offset} failed after "
                            f"{max_page_retries} retries: {e} — make incomplete"
                        )
            if ads is None:
                ok = False
                break
            if not ads:
                break
            for ad in ads:
                aid = ad.get("ads_id")
                if aid:
                    active.add(int(aid))
            offset += len(ads)
            time.sleep(page_sleep)

        if ok:
            completed.add(make_name.casefold())
        else:
            failed.append(make_name)

        logging.info(
            f"[{category}] [{i}/{len(makes)}] {make_name:<20} "
            f"active+={len(active) - before:>4}{'' if ok else '  INCOMPLETE'}"
        )

    if failed:
        logging.warning(
            f"[{category}] {len(failed)} make(s) INCOMPLETE — their listings will "
            f"NOT be marked unavailable this run: {failed}"
        )
    return active, completed


def recheck_category_api(
    category: str,
    *,
    client,
    limit: Optional[int],
    force_all: bool,
    dry_run: bool,
) -> None:
    """API-based availability recheck — ~300x faster than per-listing HTML.

    A listing present in the API active-set is available; one absent (and whose
    make completed cleanly) is gone → ``_infer_sold`` classifies it from
    ``ad_expiry``. Listings whose make did not complete, or whose make is not in
    the reference sweep at all, are left untouched (status + last_checked_at
    preserved) so the next run retries them.
    """
    db_file = db_path_for(category)
    if not os.path.exists(db_file):
        logging.warning(f"[{category}] DB not found at {db_file} — skipping. Run 1_scrape.py then 2_migrate.py first.")
        return

    conn = connect(category)
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    due = select_due_rows(conn, force_all=force_all, limit=limit)
    logging.info(f"[{category}] DB rows: {total}. Due for re-check: {len(due)}.")

    if dry_run:
        logging.info(f"[{category}] dry run — no requests will be made.")
        return

    make_col = MAKE_COL[category]
    has_make_col = _has_column(conn, "listings", make_col)
    has_sold_inference_col = _has_column(conn, "listings", "sold_inference")
    has_ad_expiry_col = _has_column(conn, "listings", "ad_expiry")

    logging.info(f"[{category}] Sweeping active listings via EagleSearch API...")
    active, completed = _sweep_active_ids(client, category)
    logging.info(
        f"[{category}] Sweep done: {len(active):,} active ads_ids, "
        f"{len(completed)} make(s) completed."
    )

    n_avail = n_gone = n_skipped = 0
    now = datetime.now()

    for row in due:
        ads_id = row['ads_id']
        previous_status = row['availability_status']
        make_cf = (row[make_col].casefold() if has_make_col and row[make_col] else None)
        ts = now.strftime('%Y-%m-%d %H:%M:%S')

        if ads_id in active:
            detected, new_status = 'available', 'available'
            n_avail += 1
            with conn:
                conn.execute(
                    "UPDATE listings SET last_seen_at=?, last_checked_at=?, availability_status=? WHERE ads_id=?",
                    (ts, ts, new_status, ads_id),
                )
                conn.execute(
                    "INSERT INTO availability_checks (ads_id, checked_at, http_status, detected_status) "
                    "VALUES (?, ?, ?, ?)",
                    (ads_id, ts, None, detected),
                )
            continue

        # Absent from active-set. Only trust this if the make completed cleanly.
        if make_cf is None or make_cf not in completed:
            n_skipped += 1
            continue  # preserve status + last_checked_at → retried next run

        detected, new_status = 'soft_404', 'unavailable'
        n_gone += 1
        sold_inf = None
        with conn:
            if has_sold_inference_col:
                ad_expiry = row['ad_expiry'] if has_ad_expiry_col else None
                sold_inf = _infer_sold(ad_expiry, now)
                conn.execute(
                    "UPDATE listings SET last_checked_at=?, availability_status=?, sold_inference=? WHERE ads_id=?",
                    (ts, new_status, sold_inf, ads_id),
                )
            else:
                conn.execute(
                    "UPDATE listings SET last_checked_at=?, availability_status=? WHERE ads_id=?",
                    (ts, new_status, ads_id),
                )
            conn.execute(
                "INSERT INTO availability_checks (ads_id, checked_at, http_status, detected_status) "
                "VALUES (?, ?, ?, ?)",
                (ads_id, ts, None, detected),
            )

    logging.info(
        f"[{category}] API re-check complete. available={n_avail:,} "
        f"gone={n_gone:,} skipped(incomplete/unknown make)={n_skipped:,}"
    )


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
        logging.warning(f"[{category}] DB not found at {db_file} — skipping. Run 1_scrape.py then 2_migrate.py first.")
        return

    conn = connect(category)
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    due = select_due_rows(conn, force_all=force_all, limit=limit)
    logging.info(f"[{category}] DB rows: {total}. Due for re-check: {len(due)}.")

    if dry_run:
        logging.info(f"[{category}] dry run — no requests will be made.")
        return

    has_sold_inference_col = _has_column(conn, "listings", "sold_inference")
    has_ad_expiry_col = _has_column(conn, "listings", "ad_expiry")

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
            elif detected in ('removed', 'soft_404'):
                # Listing is gone — infer whether it sold or expired
                if has_sold_inference_col:
                    ad_expiry = row['ad_expiry'] if has_ad_expiry_col else None
                    sold_inf = _infer_sold(ad_expiry, datetime.now())
                    conn.execute(
                        "UPDATE listings SET last_checked_at=?, availability_status=?, sold_inference=? "
                        "WHERE ads_id=?",
                        (ts, new_status, sold_inf, ads_id),
                    )
                else:
                    conn.execute(
                        "UPDATE listings SET last_checked_at=?, availability_status=? WHERE ads_id=?",
                        (ts, new_status, ads_id),
                    )
            else:
                # blocked / transient — preserve previous status, don't touch sold_inference
                conn.execute(
                    "UPDATE listings SET last_checked_at=?, availability_status=? WHERE ads_id=?",
                    (ts, new_status, ads_id),
                )
            conn.execute(
                "INSERT INTO availability_checks (ads_id, checked_at, http_status, detected_status) "
                "VALUES (?, ?, ?, ?)",
                (ads_id, ts, status_code, detected),
            )

        sold_inf_label = ""
        if detected in ('removed', 'soft_404') and has_sold_inference_col:
            sold_inf_label = f" sold_inference={sold_inf}"
        logging.info(
            f"[{category}] [{n}/{len(due)}] {ads_id} -> http={status_code} "
            f"detected={detected} status={new_status}{sold_inf_label}"
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
    parser.add_argument(
        "--method",
        choices=("api", "html"),
        default="api",
        help="api (default): one EagleSearch sweep per make, ~300x faster, "
             "present/absent only. html: per-listing detail GET, slower but "
             "distinguishes removed/soft_404/blocked.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    targets: Iterable[str] = CATEGORIES if args.category == "both" else (args.category,)

    if args.method == "api":
        from eagle_client import EagleClient
        # Politer pacing than the scraper default — same config the backfill
        # used to dodge Cloudflare on a sustained full make sweep.
        client = EagleClient(max_retries=5, request_interval=(1.5, 2.5))
        for cat in targets:
            recheck_category_api(cat, client=client, limit=args.limit, force_all=args.all, dry_run=args.dry_run)
    else:
        client = MudahClient()  # ONE client across categories so the throttle is shared
        for cat in targets:
            recheck_category(cat, client=client, limit=args.limit, force_all=args.all, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

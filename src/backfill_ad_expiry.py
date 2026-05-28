"""Backfill ad_expiry for existing motorcycle listings.

Queries the EagleSearch API make-by-make to retrieve the current ad_expiry
for any listing that is still active, then patches matching rows in the DB.

Only touches rows where ad_expiry IS NULL. Rows that have already sold or
expired (no longer in search results) are left as NULL; recheck.py will
classify those as sold_inference='unknown'.

Usage:
    python src/backfill_ad_expiry.py
    python src/backfill_ad_expiry.py --dry-run        # show stats, no DB writes
    python src/backfill_ad_expiry.py --category cars  # default: motorcycles
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, Optional

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

from db import connect, db_path_for
from eagle_client import EagleClient, BASE_URL, CATEGORY_IDS

MAKE_ID_PARAM = {
    "motorcycles": "motorcycle_make_id",
    "cars": "make_id",
}

REFERENCE_MAKES = {
    "motorcycles": _ROOT / "data" / "reference" / "motorcycles_makes.json",
    "cars": _ROOT / "data" / "reference" / "cars_makes.json",
}


def load_makes(category: str) -> list:
    path = REFERENCE_MAKES[category]
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def fetch_expiry_for_make(
    client: EagleClient,
    category: str,
    make_id: int,
    make_name: str,
) -> Dict[int, str]:
    """Return {ads_id: ad_expiry} for all currently-active listings of this make."""
    results: Dict[int, str] = {}
    make_param = MAKE_ID_PARAM[category]
    offset = 0
    from eagle_client import MAX_LIMIT, MAX_OFFSET

    while offset < MAX_OFFSET:
        try:
            resp = client.scraper.get(
                BASE_URL,
                params={
                    "category": CATEGORY_IDS[category],
                    "type": "sell",
                    "from": offset,
                    "limit": MAX_LIMIT,
                    make_param: make_id,
                },
                headers=client._headers(),
                timeout=15,
            )
            payload = resp.json()
        except Exception as e:
            logging.warning(f"  [{make_name}] offset={offset} error: {e} — skipping page")
            time.sleep(1.0)
            break

        data = payload.get("data", []) or []
        if not data:
            break

        for entry in data:
            attrs = entry.get("attributes", {}) if isinstance(entry, dict) else {}
            if not attrs:
                continue
            list_id = attrs.get("list_id")
            ad_expiry = attrs.get("ad_expiry")
            if list_id and ad_expiry:
                results[int(list_id)] = str(ad_expiry)

        offset += len(data)
        # Throttle between pages
        time.sleep(0.75)

    return results


def backfill(category: str, *, dry_run: bool = False) -> None:
    conn = connect(category)

    # Load ads_ids that need backfilling
    rows = conn.execute(
        "SELECT ads_id FROM listings WHERE ad_expiry IS NULL"
    ).fetchall()
    need_set = {r[0] for r in rows}
    logging.info(f"[{category}] {len(need_set):,} rows need ad_expiry backfill")

    if not need_set:
        logging.info(f"[{category}] Nothing to do.")
        return

    makes = load_makes(category)
    client = EagleClient()

    total_updated = 0
    total_found = 0

    for i, make in enumerate(makes, 1):
        make_id = int(make["id"])
        make_name = make["name"]

        expiry_map = fetch_expiry_for_make(client, category, make_id, make_name)
        # Filter to only rows we actually need
        relevant = {k: v for k, v in expiry_map.items() if k in need_set}
        total_found += len(relevant)

        if not relevant:
            logging.info(f"  [{i}/{len(makes)}] {make_name:<20} API={len(expiry_map):>4}  match=0")
            continue

        if not dry_run:
            with conn:
                conn.executemany(
                    "UPDATE listings SET ad_expiry=? WHERE ads_id=?",
                    [(v, k) for k, v in relevant.items()],
                )
            total_updated += len(relevant)

        logging.info(
            f"  [{i}/{len(makes)}] {make_name:<20} API={len(expiry_map):>4}  "
            f"match={len(relevant):>4}  {'(dry-run)' if dry_run else 'updated'}"
        )

    pct = total_found / len(need_set) * 100 if need_set else 0
    logging.info(
        f"[{category}] Backfill complete. "
        f"Found {total_found:,}/{len(need_set):,} ({pct:.1f}%) listings still active. "
        f"{'Would update' if dry_run else 'Updated'} {total_found if dry_run else total_updated:,} rows."
    )
    if not dry_run:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE ad_expiry IS NULL"
        ).fetchone()[0]
        logging.info(
            f"[{category}] Rows still NULL after backfill: {remaining:,} "
            f"(these listings have sold/expired — recheck.py will set sold_inference='unknown')"
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill ad_expiry for existing listings")
    p.add_argument(
        "--category",
        choices=("cars", "motorcycles"),
        default="motorcycles",
        help="Which DB to backfill (default: motorcycles)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without writing to DB",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.dry_run:
        logging.info("--- DRY-RUN mode — no DB changes will be made ---")
    backfill(args.category, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

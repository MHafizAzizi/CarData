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
import logging
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

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

from db import connect
from eagle_client import EagleClient
from reference import load_makes


def fetch_expiry_for_make(
    client: EagleClient,
    category: str,
    make_id: int,
    make_name: str,
) -> Tuple[Dict[int, str], bool]:
    """Return ({ads_id: ad_expiry}, ok) for currently-active listings of this make.

    ``ok`` is False when the sweep was INCOMPLETE (a page exhausted retries) —
    the make should be re-run rather than trusted as "no active listings".
    See ``EagleClient.fetch_make_resilient`` for the retry semantics.
    """
    ads, ok = client.fetch_make_resilient(category, str(make_id), label=make_name)
    results: Dict[int, str] = {}
    for ad in ads:
        ads_id = ad.get("ads_id")
        ad_expiry = ad.get("ad_expiry")
        if ads_id and ad_expiry:
            results[int(ads_id)] = str(ad_expiry)
    return results, ok


def backfill(
    category: str,
    *,
    dry_run: bool = False,
    only_makes: Optional[set] = None,
) -> None:
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

    makes = load_makes(category, required=True)
    if only_makes:
        wanted = {m.casefold() for m in only_makes}
        makes = [m for m in makes if m["name"].casefold() in wanted]
        missing = wanted - {m["name"].casefold() for m in makes}
        if missing:
            logging.warning(f"[{category}] --makes not found, ignored: {sorted(missing)}")
        logging.info(f"[{category}] Restricted to {len(makes)} make(s): "
                     f"{[m['name'] for m in makes]}")
        if not makes:
            logging.error(f"[{category}] No matching makes — nothing to do.")
            return

    # Slower throttle + extra network retries than the default scraper config:
    # the first run tripped a Cloudflare interstitial on the back half of makes
    # under sustained load. Politer pacing avoids the block in the first place.
    client = EagleClient(max_retries=5, request_interval=(1.5, 2.5))

    total_updated = 0
    total_found = 0
    failed_makes: list = []

    for i, make in enumerate(makes, 1):
        make_id = int(make["id"])
        make_name = make["name"]

        expiry_map, ok = fetch_expiry_for_make(client, category, make_id, make_name)
        if not ok:
            failed_makes.append(make_name)
        # Filter to only rows we actually need
        relevant = {k: v for k, v in expiry_map.items() if k in need_set}
        total_found += len(relevant)

        flag = "" if ok else "  INCOMPLETE"
        if not relevant:
            logging.info(f"  [{i}/{len(makes)}] {make_name:<20} API={len(expiry_map):>4}  match=0{flag}")
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
            f"match={len(relevant):>4}  {'(dry-run)' if dry_run else 'updated'}{flag}"
        )

    pct = total_found / len(need_set) * 100 if need_set else 0
    logging.info(
        f"[{category}] Backfill complete. "
        f"Found {total_found:,}/{len(need_set):,} ({pct:.1f}%) listings still active. "
        f"{'Would update' if dry_run else 'Updated'} {total_found if dry_run else total_updated:,} rows."
    )
    if failed_makes:
        logging.warning(
            f"[{category}] {len(failed_makes)} make(s) INCOMPLETE (page failed after "
            f"retries) — re-run with: --makes \"{','.join(failed_makes)}\""
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
    p.add_argument(
        "--makes",
        default=None,
        help="Comma-separated make names to restrict to (case-insensitive). "
             "Use to re-run only the makes reported INCOMPLETE on a prior run.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.dry_run:
        logging.info("--- DRY-RUN mode — no DB changes will be made ---")
    only_makes = (
        {m.strip() for m in args.makes.split(",") if m.strip()}
        if args.makes else None
    )
    backfill(args.category, dry_run=args.dry_run, only_makes=only_makes)


if __name__ == "__main__":
    main()

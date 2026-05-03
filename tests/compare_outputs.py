"""Integration comparison: new HybridScraper vs old script.py MudahScraper.

Runs both scrapers on a small overlapping set of cars and asserts that
shared fields (ads_id, subject, make, model) are populated and that
new API-only fields appear in the new output.

Strategy:
    1. Run HybridScraper with --depth all on N cars.
    2. For each ad_id collected, feed its URL into MudahScraper.get_car_info()
       to get the same listing via the HTML path.
    3. Diff the two records on shared columns.

Why diff per-ad rather than running script.py separately:
    script.py paginates by page+state+brand and returns its own arbitrary set
    of listings; matching to API listings is brittle. Driving HTML directly
    by URL guarantees same-ad comparison.

Usage:
    python tests/compare_outputs.py [--n 5]
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_ROOT / "src"))

from eagle_client import EagleClient  # noqa: E402
from detail_client import DetailClient  # noqa: E402
from mudah_client import MudahClient  # noqa: E402
from scraper import HybridScraper  # noqa: E402
from script import MudahScraper  # noqa: E402


# Fields expected to match exactly (or near-exactly) between API+detail and HTML
SHARED_FIELDS_TO_COMPARE = (
    "ads_id",
    "subject",
    "make",
    "model",
)

# Fields ONLY the new scraper produces (must be present, may be partly null)
API_ONLY_FIELDS = (
    "old_price",
    "year_verified",
    "store_verified",
    "bundle",
    "media_count",
    "mileage_bucket",
    "car_loan_payment",
    "car_loan_eligible",
)


def run_hybrid(n: int, tmp_dir: Path) -> pd.DataFrame:
    """Run HybridScraper with depth=all to get a complete set."""
    logging.info(f"=== Running HybridScraper (n={n}, depth=all) ===")
    eagle = EagleClient()
    mudah = MudahClient()
    detail = DetailClient(mudah)
    scraper = HybridScraper("cars", eagle, detail, tmp_dir)
    csv_path = scraper.run(max_ads=n, depth="all", checkpoint_every=1000)
    df = pd.read_csv(csv_path)
    logging.info(f"HybridScraper produced {len(df)} rows, {len(df.columns)} cols")
    return df


def run_html_for_urls(urls: list, ads_ids: list) -> pd.DataFrame:
    """Drive script.py's MudahScraper.get_car_info directly for each URL."""
    logging.info(f"=== Running script.py.get_car_info for {len(urls)} URLs ===")
    s = MudahScraper(max_workers=1)
    rows = []
    for url, ads_id in zip(urls, ads_ids):
        attrs = s.get_car_info(url)
        if attrs is None:
            logging.warning(f"HTML scrape returned None for {url}")
            rows.append({"ads_id": ads_id, "url": url, "_failed": True})
            continue
        flat = s._attrs_to_dict(attrs)
        flat["ads_id"] = int(ads_id)
        flat["url"] = url
        rows.append(flat)
    df = pd.DataFrame(rows)
    logging.info(f"HTML produced {len(df)} rows, {len(df.columns)} cols")
    return df


def compare(df_new: pd.DataFrame, df_old: pd.DataFrame) -> int:
    """Compare row-by-row on shared ads_id set. Returns number of mismatches."""
    df_new = df_new.set_index("ads_id")
    df_old = df_old.set_index("ads_id")

    shared_ids = df_new.index.intersection(df_old.index)
    logging.info(f"Comparing {len(shared_ids)} shared ads_ids")

    mismatches = 0

    print("\n=== Field coverage check (new only) ===")
    # Sparse fields are expected to be absent when no ad in this batch
    # has a value. That's a CSV column-dropping artifact, not a bug.
    SPARSE_FIELDS = {"old_price", "year_verified", "store_verified", "bundle"}

    for f in API_ONLY_FIELDS:
        if f not in df_new.columns:
            label = "absent (sparse-OK)" if f in SPARSE_FIELDS else "MISSING (BUG)"
            print(f"  {f:25s} {label}")
            if f not in SPARSE_FIELDS:
                mismatches += 1
            continue
        n_pop = df_new.loc[shared_ids, f].notna().sum()
        print(f"  {f:25s} populated {n_pop}/{len(shared_ids)}")

    # Fields where casing differences between API + HTML are expected
    # (clean.py title-cases these in post-processing).
    CASE_INSENSITIVE_FIELDS = {"make", "model"}

    print("\n=== Per-ad comparison (shared fields, ads_id excluded — used as index) ===")
    fields = [f for f in SHARED_FIELDS_TO_COMPARE if f != "ads_id"]
    for ads_id in shared_ids:
        row_new = df_new.loc[ads_id]
        row_old = df_old.loc[ads_id]
        if row_old.get("_failed"):
            print(f"  ad {ads_id}: HTML scrape failed; skipping diff")
            continue

        print(f"\n  ad {ads_id}")
        for field in fields:
            v_new = row_new.get(field)
            v_old = row_old.get(field)

            # Coerce blank/NaN to None for fair comparison
            v_new_norm = None if pd.isna(v_new) or v_new == "" else str(v_new).strip()
            v_old_norm = None if pd.isna(v_old) or v_old == "" else str(v_old).strip()

            if field in CASE_INSENSITIVE_FIELDS:
                v_new_cmp = v_new_norm.lower() if v_new_norm else None
                v_old_cmp = v_old_norm.lower() if v_old_norm else None
            else:
                v_new_cmp = v_new_norm
                v_old_cmp = v_old_norm

            if v_new_cmp == v_old_cmp:
                if field in CASE_INSENSITIVE_FIELDS and v_new_norm != v_old_norm:
                    mark = "OK (case)"
                else:
                    mark = "OK"
            else:
                mark = "DIFF"
                mismatches += 1

            shown_new = (str(v_new_norm)[:60] + "...") if v_new_norm and len(str(v_new_norm)) > 60 else v_new_norm
            shown_old = (str(v_old_norm)[:60] + "...") if v_old_norm and len(str(v_old_norm)) > 60 else v_old_norm
            print(f"    {field:12s} [{mark:9s}] new={shown_new!r:64s} old={shown_old!r}")

    print(f"\n=== Summary: {mismatches} mismatch(es) across {len(shared_ids)} ads ===")
    return mismatches


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=5, help="Number of ads to compare (default: 5)")
    args = p.parse_args()

    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )

    tmp = _ROOT / "data" / "raw" / "_compare_tmp"
    tmp.mkdir(parents=True, exist_ok=True)

    # 1. Hybrid (API + HTML detail)
    df_new = run_hybrid(args.n, tmp)

    # 2. HTML-only for the same URLs
    urls = df_new["url"].tolist()
    ids = df_new["ads_id"].tolist()
    df_old = run_html_for_urls(urls, ids)

    # 3. Compare
    mismatches = compare(df_new, df_old)

    # Cleanup
    for f in tmp.glob("*.csv"):
        f.unlink()
    tmp.rmdir()

    sys.exit(0 if mismatches == 0 else 1)


if __name__ == "__main__":
    main()

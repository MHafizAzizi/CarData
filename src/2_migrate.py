"""Load raw CSV files (or legacy master xlsx) into the per-category SQLite DBs.

Primary workflow — CSV (recommended):
    Run interactively and pick cars (1) or motorcycles (2).
    The script loads every CSV file currently in data/raw/<category>/ and,
    after a successful migration, moves the processed files into
    data/old/<category>/ so the next run only sees freshly scraped files.

Legacy workflow — xlsx (cars only):
    Pass --xlsx <path> to load from a master Excel file instead of CSVs.
    Useful for backfilling historical data that was never scraped as CSV.

Usage:
    python src/2_migrate.py                  # interactive prompt
    python src/2_migrate.py --category cars  # skip prompt
    python src/2_migrate.py --dry-run        # preview, write/move nothing
    python src/2_migrate.py --xlsx data/master/MasterMudahCarData.xlsx
"""

import argparse
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

from db import connect, db_path_for, CATEGORIES, set_meta

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).resolve().parent.parent
_LOGS_DIR = _ROOT / "logs"
_RAW_DIR = _ROOT / "data" / "raw"
_OLD_DIR = _ROOT / "data" / "old"

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass

_LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(_LOGS_DIR / "migrate.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# ---------------------------------------------------------------------------
# Schema column sets (must match schema_*.sql)
# ---------------------------------------------------------------------------

SHARED_COLUMNS: List[str] = [
    "ads_id", "url", "subject", "price",
    "condition", "manufactured_date", "region", "subregion",
    "seller_name", "company_ad", "published",
    "first_seen_at", "last_seen_at", "last_checked_at", "availability_status",
    # API-only columns added by schema v1 → v2 (both categories)
    "old_price", "store_verified", "bundle", "media_count",
    # ad_expiry / sold_inference added by schema v5 (both categories)
    "ad_expiry", "sold_inference",
]

CAR_ONLY_COLUMNS: List[str] = [
    "make", "model", "car_type", "transmission", "engine_capacity",
    "variant", "fuel_type",
    # API-only columns added by schema v1 → v2 (cars only)
    "car_loan_eligible", "car_loan_payment", "car_loan_tenure", "has_car_grant",
    # cars-only after v6 (dropped from motorcycles — always NULL there)
    "year_verified", "mileage_bucket",
]

MOTORCYCLE_ONLY_COLUMNS: List[str] = ["motorcycle_make", "motorcycle_model"]

CATEGORY_COLUMNS: Dict[str, Set[str]] = {
    "cars": set(SHARED_COLUMNS) | set(CAR_ONLY_COLUMNS),
    "motorcycles": set(SHARED_COLUMNS) | set(MOTORCYCLE_ONLY_COLUMNS),
}

# ---------------------------------------------------------------------------
# Interactive prompt
# ---------------------------------------------------------------------------

def _prompt_category() -> str:
    print("\nSelect category:")
    print("  1. cars")
    print("  2. motorcycles")
    while True:
        raw = input("Enter 1 or 2: ").strip()
        if raw == "1":
            return "cars"
        if raw == "2":
            return "motorcycles"
        print("Please enter 1 or 2.")

# ---------------------------------------------------------------------------
# CSV discovery + archival
# ---------------------------------------------------------------------------

def _find_csvs(category: str) -> List[Path]:
    """Return every CSV in data/raw/<category>/, oldest-first."""
    folder = _RAW_DIR / category
    if not folder.exists():
        logging.warning(f"[{category}] Raw folder not found: {folder}")
        return []
    csvs = sorted(folder.glob("*.csv"), key=lambda p: p.stat().st_mtime)
    logging.info(f"[{category}] Found {len(csvs)} CSV(s) in {folder}")
    return csvs


def _archive_csv(category: str, src: Path) -> None:
    """Move a processed CSV into data/old/<category>/. Disambiguate name collisions."""
    dest_dir = _OLD_DIR / category
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    if dest.exists():
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        dest = dest_dir / f"{src.stem}_{ts}{src.suffix}"
    shutil.move(str(src), str(dest))
    logging.info(f"[{category}] Archived {src.name} -> {dest.relative_to(_ROOT)}")

# ---------------------------------------------------------------------------
# DataFrame preparation
# ---------------------------------------------------------------------------

def prepare_dataframe(df: pd.DataFrame, category: str, source_label: str = "") -> pd.DataFrame:
    """Coerce a DataFrame into the shape the listings table expects."""
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if "first_seen_at" not in df.columns or df["first_seen_at"].isna().all():
        if "Tarikh_Kemaskini" in df.columns:
            df["first_seen_at"] = df["Tarikh_Kemaskini"].fillna(today).astype(str)
        else:
            df["first_seen_at"] = today

    # Stamp last_seen_at to now on every ingest — each scrape is fresh evidence
    # the ad existed at this moment. UPSERT mode will overwrite the old value;
    # INSERT OR IGNORE mode leaves it on rows that already exist.
    if "last_seen_at" not in df.columns or df["last_seen_at"].isna().all():
        df["last_seen_at"] = today

    if "availability_status" not in df.columns:
        df["availability_status"] = "unknown"

    # Keep only columns the schema knows
    valid = CATEGORY_COLUMNS[category]
    keep = [c for c in df.columns if c in valid]
    dropped = [c for c in df.columns if c not in valid]
    if dropped:
        logging.debug(f"[{category}]{' ' + source_label if source_label else ''} dropping {len(dropped)} unknown columns")
    df = df[keep].copy()

    if "ads_id" not in df.columns:
        raise ValueError(f"Source missing required `ads_id` column ({source_label or 'unknown file'}).")
    df["ads_id"] = pd.to_numeric(df["ads_id"], errors="coerce")
    bad = df["ads_id"].isna().sum()
    if bad:
        logging.warning(f"[{category}] dropping {bad} rows with non-numeric ads_id")
        df = df[df["ads_id"].notna()].copy()
    df["ads_id"] = df["ads_id"].astype("int64")

    df = df.astype(object).where(df.notna(), None)
    return df

# ---------------------------------------------------------------------------
# xlsx classification (legacy path)
# ---------------------------------------------------------------------------

def _classify_xlsx_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
    """Split xlsx rows into (cars_df, motorcycles_df, unknown_count)."""
    has_make = (
        df["make"].notna() & (df["make"].astype(str).str.strip() != "")
        if "make" in df.columns
        else pd.Series(False, index=df.index)
    )
    has_moto = (
        df["motorcycle_make"].notna() & (df["motorcycle_make"].astype(str).str.strip() != "")
        if "motorcycle_make" in df.columns
        else pd.Series(False, index=df.index)
    )
    cars_mask = has_make & ~has_moto
    moto_mask = has_moto & ~has_make
    unknown_mask = ~(cars_mask | moto_mask)
    return df[cars_mask].copy(), df[moto_mask].copy(), int(unknown_mask.sum())

# ---------------------------------------------------------------------------
# Insert helper
# ---------------------------------------------------------------------------

def _insert_rows(
    conn,
    df: pd.DataFrame,
    *,
    dry_run: bool,
    mode: str = "upsert",
) -> Tuple[int, int]:
    """Insert rows into listings. Returns (attempted, newly_inserted).

    mode='upsert' (default): ON CONFLICT(ads_id) DO UPDATE — re-scraping the
        same ad refreshes every column except first_seen_at, which preserves
        the true first-seen date.
    mode='ignore': INSERT OR IGNORE — re-scraping the same ad is a no-op.
        Useful for backfilling historical data without overwriting curated rows.
    """
    if df.empty:
        return 0, 0
    cols = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    quoted_cols = ", ".join(f'"{c}"' for c in cols)

    if mode == "ignore":
        sql = f"INSERT OR IGNORE INTO listings ({quoted_cols}) VALUES ({placeholders})"
    elif mode == "upsert":
        # Update every column on conflict EXCEPT ads_id (the key) and
        # first_seen_at (must stay immutable across re-scrapes).
        update_cols = [c for c in cols if c not in ("ads_id", "first_seen_at")]
        if update_cols:
            update_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in update_cols)
            sql = (
                f"INSERT INTO listings ({quoted_cols}) VALUES ({placeholders}) "
                f"ON CONFLICT(ads_id) DO UPDATE SET {update_clause}"
            )
        else:
            # Edge case: only ads_id + first_seen_at present — nothing to update
            sql = f"INSERT OR IGNORE INTO listings ({quoted_cols}) VALUES ({placeholders})"
    else:
        raise ValueError(f"Unknown insert mode: {mode!r}")

    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    if dry_run:
        logging.info(f"DRY RUN ({mode}): would process {len(rows)} rows")
        return len(rows), 0

    before = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    with conn:
        conn.executemany(sql, rows)
    after = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    return len(rows), after - before

# ---------------------------------------------------------------------------
# Migration modes
# ---------------------------------------------------------------------------

def migrate_csv(category: str, *, dry_run: bool, mode: str = "upsert") -> None:
    """Load every CSV from data/raw/<category>/ into the DB, then archive each one."""
    csv_files = _find_csvs(category)

    if not csv_files:
        print(f"[{category}] No CSV files to migrate.")
        return

    # Label used in log lines so user can tell at a glance whether rows
    # were genuinely new or refreshed in place.
    other_label = "updated" if mode == "upsert" else "skipped"

    # init=True: migrating CSVs into a fresh DB is a legitimate creation path
    conn = connect(category, init=True)
    total_attempted = total_inserted = total_archived = 0
    run_start = datetime.now()

    for csv_path in csv_files:
        logging.info(f"[{category}] Loading {csv_path.name} …")
        try:
            df = pd.read_csv(csv_path, low_memory=False)
        except Exception as exc:
            logging.error(f"[{category}] Failed to read {csv_path.name}: {exc}")
            continue

        try:
            prepared = prepare_dataframe(df, category, source_label=csv_path.name)
        except ValueError as exc:
            logging.error(f"[{category}] Skipping {csv_path.name}: {exc}")
            continue

        attempted, inserted = _insert_rows(conn, prepared, dry_run=dry_run, mode=mode)
        other = attempted - inserted
        logging.info(
            f"[{category}] {csv_path.name}: attempted={attempted}, "
            f"inserted={inserted}, {other_label}={other}"
        )
        total_attempted += attempted
        total_inserted += inserted

        if dry_run:
            logging.info(f"[{category}] DRY RUN: would archive {csv_path.name}")
        else:
            try:
                _archive_csv(category, csv_path)
                total_archived += 1
            except OSError as exc:
                logging.error(f"[{category}] Failed to archive {csv_path.name}: {exc}")

    logging.info(
        f"[{category}] Migration complete (mode={mode}) — "
        f"total attempted={total_attempted}, inserted={total_inserted}, "
        f"{other_label}={total_attempted - total_inserted}, archived={total_archived}"
    )

    if not dry_run:
        set_meta(conn, "last_migrated_at", run_start.strftime("%Y-%m-%d %H:%M:%S"))


def migrate_xlsx(xlsx_path: Path, category: str, *, dry_run: bool, mode: str = "ignore") -> None:
    """Load a master xlsx file into the DB (legacy path, primarily for cars).

    Defaults to mode='ignore' because xlsx backfills typically run once on a
    fresh DB, and the xlsx columns don't always carry fresh API fields.
    """
    if not xlsx_path.exists():
        logging.error(f"Source xlsx not found: {xlsx_path}")
        sys.exit(1)

    logging.info(f"Reading {xlsx_path} …")
    df = pd.read_excel(xlsx_path)
    logging.info(f"Read {len(df)} rows, {len(df.columns)} columns")

    cars_df, moto_df, unknown_n = _classify_xlsx_rows(df)
    logging.info(f"Classified: cars={len(cars_df)}, motorcycles={len(moto_df)}, unknown={unknown_n}")
    if unknown_n:
        logging.warning(f"{unknown_n} rows had neither `make` nor `motorcycle_make` — skipped")

    slice_df = cars_df if category == "cars" else moto_df
    if slice_df.empty:
        logging.info(f"[{category}] No rows to load from xlsx.")
        return

    prepared = prepare_dataframe(slice_df, category, source_label=xlsx_path.name)
    logging.info(f"[{category}] {len(prepared)} rows ready → {db_path_for(category)}")

    conn = connect(category, init=True)
    attempted, inserted = _insert_rows(conn, prepared, dry_run=dry_run, mode=mode)
    other = attempted - inserted
    other_label = "updated" if mode == "upsert" else "skipped"
    logging.info(f"[{category}] attempted={attempted}, inserted={inserted}, {other_label}={other}")

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migrate raw CSV files (or legacy xlsx) into the per-category SQLite databases"
    )
    p.add_argument(
        "--category",
        choices=list(CATEGORIES),
        default=None,
        help="Category to migrate (cars or motorcycles). Prompted if omitted.",
    )
    p.add_argument(
        "--xlsx",
        default=None,
        metavar="PATH",
        help="Load from a master Excel file instead of raw CSVs (legacy path).",
    )
    p.add_argument("--dry-run", action="store_true", help="Preview what would be loaded; write nothing.")
    p.add_argument(
        "--mode",
        choices=("upsert", "ignore"),
        default=None,
        help=(
            "upsert (default for CSV): refresh existing ads via ON CONFLICT DO UPDATE; "
            "ignore (default for xlsx): keep existing rows, only insert new ones."
        ),
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Interactive category prompt when not supplied via flag
    category = args.category or _prompt_category()

    if args.dry_run:
        logging.info("--- DRY RUN mode — no data will be written ---")

    if args.xlsx:
        mode = args.mode or "ignore"
        migrate_xlsx(Path(args.xlsx), category, dry_run=args.dry_run, mode=mode)
    else:
        mode = args.mode or "upsert"
        migrate_csv(category, dry_run=args.dry_run, mode=mode)


if __name__ == "__main__":
    main()

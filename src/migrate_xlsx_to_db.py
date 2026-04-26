"""Load raw CSV files (or legacy master xlsx) into the per-category SQLite DBs.

Primary workflow — CSV (recommended):
    Run interactively and pick cars (1) or motorcycles (2).
    The script scans data/raw/<category>/ for any CSV files created since the
    last successful migration, loads them all, and updates last_migrated_at in
    the database's meta table so the next run only picks up new files.

Legacy workflow — xlsx (cars only):
    Pass --xlsx <path> to load from a master Excel file instead of CSVs.
    Useful for backfilling historical data that was never scraped as CSV.

Usage:
    python src/migrate_xlsx_to_db.py                  # interactive prompt
    python src/migrate_xlsx_to_db.py --category cars  # skip prompt
    python src/migrate_xlsx_to_db.py --dry-run        # preview, write nothing
    python src/migrate_xlsx_to_db.py --xlsx data/master/MasterMudahCarData.xlsx
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from db import connect, db_path_for, CATEGORIES, get_meta, set_meta

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).resolve().parent.parent
_LOGS_DIR = _ROOT / "logs"
_RAW_DIR = _ROOT / "data" / "raw"

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
    "ads_id", "url", "subject", "body", "price",
    "condition", "manufactured_date", "mileage",
    "location", "region", "subregion",
    "seller_name", "company_ad", "published",
    "first_seen_at", "last_seen_at", "last_checked_at", "availability_status",
]

CAR_ONLY_COLUMNS: List[str] = [
    "make", "model", "car_type", "transmission", "engine_capacity",
    "family", "variant", "series", "style",
    "seat", "country_origin", "cc", "comp_ratio", "kw",
    "torque", "engine", "fuel_type", "length", "width", "height",
    "wheelbase", "kerbwt", "fueltk", "brake_front", "brake_rear",
    "suspension_front", "suspension_rear", "steering",
    "tyres_front", "tyres_rear", "wheel_rim_front", "wheel_rim_rear",
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
# CSV discovery — only files newer than last_migrated_at
# ---------------------------------------------------------------------------

def _last_migrated_at(category: str) -> Optional[datetime]:
    """Read last_migrated_at from the DB meta table. Returns None if never run."""
    conn = connect(category)
    raw = get_meta(conn, "last_migrated_at")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        logging.warning(f"[{category}] Unreadable last_migrated_at value '{raw}' — treating as never migrated")
        return None


def _find_new_csvs(category: str, since: Optional[datetime]) -> List[Path]:
    """Return CSV files in data/raw/<category>/ that are newer than *since*.

    If *since* is None, all CSVs in the folder are returned (first-ever run).
    Files are sorted oldest-first so they are loaded in chronological order.
    """
    folder = _RAW_DIR / category
    if not folder.exists():
        logging.warning(f"[{category}] Raw folder not found: {folder}")
        return []

    csvs = sorted(folder.glob("*.csv"), key=lambda p: p.stat().st_mtime)

    if since is None:
        logging.info(f"[{category}] First migration — loading all {len(csvs)} CSV(s) found")
        return csvs

    # st_mtime is a Unix timestamp (seconds); compare with since (local naive datetime)
    since_ts = since.timestamp()
    new = [p for p in csvs if p.stat().st_mtime > since_ts]
    logging.info(
        f"[{category}] {len(new)} new CSV(s) since last migration "
        f"({since.strftime('%Y-%m-%d %H:%M:%S')}) out of {len(csvs)} total"
    )
    return new

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

def _insert_rows(conn, df: pd.DataFrame, *, dry_run: bool) -> Tuple[int, int]:
    """Insert rows with INSERT OR IGNORE. Returns (attempted, inserted)."""
    if df.empty:
        return 0, 0
    cols = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    sql = f"INSERT OR IGNORE INTO listings ({quoted_cols}) VALUES ({placeholders})"
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    if dry_run:
        logging.info(f"DRY RUN: would attempt to insert {len(rows)} rows")
        return len(rows), 0

    before = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    with conn:
        conn.executemany(sql, rows)
    after = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    return len(rows), after - before

# ---------------------------------------------------------------------------
# Migration modes
# ---------------------------------------------------------------------------

def migrate_csv(category: str, *, dry_run: bool) -> None:
    """Load all new CSV files from data/raw/<category>/ into the SQLite DB."""
    since = _last_migrated_at(category)
    csv_files = _find_new_csvs(category, since)

    if not csv_files:
        print(f"[{category}] No new CSV files to migrate.")
        return

    conn = connect(category)
    total_attempted = total_inserted = 0
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

        attempted, inserted = _insert_rows(conn, prepared, dry_run=dry_run)
        skipped = attempted - inserted
        logging.info(
            f"[{category}] {csv_path.name}: attempted={attempted}, "
            f"inserted={inserted}, skipped={skipped}"
        )
        total_attempted += attempted
        total_inserted += inserted

    logging.info(
        f"[{category}] Migration complete — "
        f"total attempted={total_attempted}, inserted={total_inserted}, "
        f"skipped={total_attempted - total_inserted}"
    )

    if not dry_run:
        set_meta(conn, "last_migrated_at", run_start.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info(f"[{category}] Updated last_migrated_at = {run_start.strftime('%Y-%m-%d %H:%M:%S')}")


def migrate_xlsx(xlsx_path: Path, category: str, *, dry_run: bool) -> None:
    """Load a master xlsx file into the DB (legacy path, primarily for cars)."""
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

    conn = connect(category)
    attempted, inserted = _insert_rows(conn, prepared, dry_run=dry_run)
    skipped = attempted - inserted
    logging.info(f"[{category}] attempted={attempted}, inserted={inserted}, skipped={skipped}")

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
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Interactive category prompt when not supplied via flag
    category = args.category or _prompt_category()

    if args.dry_run:
        logging.info("--- DRY RUN mode — no data will be written ---")

    if args.xlsx:
        migrate_xlsx(Path(args.xlsx), category, dry_run=args.dry_run)
    else:
        migrate_csv(category, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

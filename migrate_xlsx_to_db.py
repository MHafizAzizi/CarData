"""One-shot loader: import an existing master xlsx into the per-category SQLite DBs.

Reads the master Excel file, classifies each row as cars or motorcycles by which
make-style column is populated, drops columns the target schema doesn't know
about, backfills `first_seen_at` from `Tarikh_Kemaskini` (or today if missing),
and inserts using INSERT OR IGNORE so re-runs are safe.

Usage:
    python migrate_xlsx_to_db.py                         # both categories
    python migrate_xlsx_to_db.py --category cars         # cars only
    python migrate_xlsx_to_db.py --category motorcycles  # motorcycles only
    python migrate_xlsx_to_db.py --dry-run               # report, write nothing
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Set, Tuple

import pandas as pd

from db import connect, db_path_for, CATEGORIES


try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/migrate.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


# Columns each schema accepts (must match schema_*.sql)
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


def classify_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
    """Split rows into (cars_df, motorcycles_df, unknown_count) by which make field is populated."""
    has_make = df["make"].notna() & (df["make"].astype(str).str.strip() != "") if "make" in df.columns else pd.Series(False, index=df.index)
    has_moto = (
        df["motorcycle_make"].notna() & (df["motorcycle_make"].astype(str).str.strip() != "")
        if "motorcycle_make" in df.columns
        else pd.Series(False, index=df.index)
    )

    cars_mask = has_make & ~has_moto
    moto_mask = has_moto & ~has_make
    unknown_mask = ~(cars_mask | moto_mask)

    return df[cars_mask].copy(), df[moto_mask].copy(), int(unknown_mask.sum())


def prepare_dataframe(df: pd.DataFrame, category: str) -> pd.DataFrame:
    """Coerce a category-specific slice of the xlsx into the shape `listings` expects."""
    today = datetime.now().strftime("%Y-%m-%d")

    if "first_seen_at" not in df.columns:
        if "Tarikh_Kemaskini" in df.columns:
            df["first_seen_at"] = df["Tarikh_Kemaskini"].fillna(today).astype(str)
        else:
            df["first_seen_at"] = today

    if "availability_status" not in df.columns:
        df["availability_status"] = "unknown"

    valid = CATEGORY_COLUMNS[category]
    keep = [c for c in df.columns if c in valid]
    dropped = [c for c in df.columns if c not in valid]
    if dropped:
        logging.info(f"[{category}] dropping {len(dropped)} unknown columns: {sorted(dropped)}")
    df = df[keep].copy()

    if "ads_id" not in df.columns:
        raise ValueError("Source rows missing required `ads_id` column.")
    df["ads_id"] = pd.to_numeric(df["ads_id"], errors="coerce")
    bad = df["ads_id"].isna().sum()
    if bad:
        logging.warning(f"[{category}] dropping {bad} rows with non-numeric ads_id")
        df = df[df["ads_id"].notna()].copy()
    df["ads_id"] = df["ads_id"].astype("int64")

    df = df.astype(object).where(df.notna(), None)
    return df


def insert_rows(conn, df: pd.DataFrame, *, dry_run: bool) -> int:
    if df.empty:
        return 0
    cols = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    sql = f"INSERT OR IGNORE INTO listings ({quoted_cols}) VALUES ({placeholders})"

    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    if dry_run:
        logging.info(f"DRY RUN: would attempt to insert {len(rows)} rows")
        return 0

    with conn:
        cur = conn.executemany(sql, rows)
        return cur.rowcount if cur.rowcount is not None else 0


def load_category(slice_df: pd.DataFrame, category: str, *, dry_run: bool) -> None:
    if slice_df.empty:
        logging.info(f"[{category}] no rows to load")
        return

    prepared = prepare_dataframe(slice_df, category)
    logging.info(f"[{category}] {len(prepared)} rows ready for insert into {db_path_for(category)}")

    conn = connect(category)
    before = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    inserted = insert_rows(conn, prepared, dry_run=dry_run)
    after = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    skipped = len(prepared) - (after - before)
    logging.info(f"[{category}] listings: {before} -> {after} (inserted={inserted}, skipped duplicates={skipped})")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load master xlsx into the per-category SQLite databases")
    p.add_argument("--xlsx", default="data/master/MasterMudahCarData.xlsx", help="Source Excel file")
    p.add_argument(
        "--category",
        choices=[*CATEGORIES, "both"],
        default="both",
        help="Load only one category (default: both)",
    )
    p.add_argument("--dry-run", action="store_true", help="Report what would happen, write nothing")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not os.path.exists(args.xlsx):
        logging.error(f"Source xlsx not found: {args.xlsx}")
        sys.exit(1)

    logging.info(f"Reading {args.xlsx}")
    df = pd.read_excel(args.xlsx)
    logging.info(f"Read {len(df)} rows, {len(df.columns)} columns")

    cars_df, moto_df, unknown_n = classify_rows(df)
    logging.info(
        f"Classified rows: cars={len(cars_df)}, motorcycles={len(moto_df)}, unknown={unknown_n}"
    )
    if unknown_n:
        logging.warning(
            f"{unknown_n} rows had neither `make` nor `motorcycle_make` populated — skipping them"
        )

    targets = CATEGORIES if args.category == "both" else (args.category,)
    for cat in targets:
        slice_df = cars_df if cat == "cars" else moto_df
        load_category(slice_df, cat, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

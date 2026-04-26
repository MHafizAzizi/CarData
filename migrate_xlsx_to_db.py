"""One-shot loader: import an existing master xlsx into the SQLite database.

Reads the master Excel file, drops columns the schema doesn't know about,
backfills `first_seen_at` from `Tarikh_Kemaskini` (or today if missing), and
inserts into the `listings` table using INSERT OR IGNORE so re-runs are safe.

Usage:
    python migrate_xlsx_to_db.py
    python migrate_xlsx_to_db.py --xlsx data/master/MasterMudahCarData.xlsx --db data/master/cardata.db
    python migrate_xlsx_to_db.py --dry-run    # report what would be inserted
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import List, Set

import pandas as pd

from db import connect, DEFAULT_DB_PATH


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


# Columns the listings table accepts (must match schema.sql)
LISTING_COLUMNS: List[str] = [
    "ads_id", "url", "subject", "body", "price",
    "condition", "make", "model", "motorcycle_make", "motorcycle_model",
    "manufactured_date", "mileage",
    "location", "region", "subregion",
    "seller_name", "company_ad",
    "car_type", "transmission", "engine_capacity",
    "family", "variant", "series",
    "style", "seat", "country_origin", "cc", "comp_ratio", "kw",
    "torque", "engine", "fuel_type", "length", "width", "height",
    "wheelbase", "kerbwt", "fueltk", "brake_front", "brake_rear",
    "suspension_front", "suspension_rear", "steering", "tyres_front",
    "tyres_rear", "wheel_rim_front", "wheel_rim_rear",
    "published",
    "first_seen_at", "last_seen_at", "last_checked_at", "availability_status",
]
LISTING_COL_SET: Set[str] = set(LISTING_COLUMNS)


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce the xlsx DataFrame into the shape `listings` expects."""
    today = datetime.now().strftime("%Y-%m-%d")

    # Backfill first_seen_at from Tarikh_Kemaskini (the old scrape-date column),
    # falling back to today's date for rows that lack it.
    if "first_seen_at" not in df.columns:
        if "Tarikh_Kemaskini" in df.columns:
            df["first_seen_at"] = df["Tarikh_Kemaskini"].fillna(today).astype(str)
        else:
            df["first_seen_at"] = today

    if "availability_status" not in df.columns:
        df["availability_status"] = "unknown"

    # Drop unknown columns (e.g. Tarikh_Kemaskini, any legacy fields)
    keep = [c for c in df.columns if c in LISTING_COL_SET]
    dropped = [c for c in df.columns if c not in LISTING_COL_SET]
    if dropped:
        logging.info(f"Dropping {len(dropped)} unknown columns: {dropped}")
    df = df[keep].copy()

    # ads_id MUST be a usable integer
    if "ads_id" not in df.columns:
        raise ValueError("Master file is missing the required `ads_id` column.")
    df["ads_id"] = pd.to_numeric(df["ads_id"], errors="coerce")
    bad = df["ads_id"].isna().sum()
    if bad:
        logging.warning(f"Dropping {bad} rows with non-numeric ads_id")
        df = df[df["ads_id"].notna()].copy()
    df["ads_id"] = df["ads_id"].astype("int64")

    # Replace pandas NA/NaN with None so sqlite stores SQL NULL
    df = df.astype(object).where(df.notna(), None)

    return df


def insert_rows(conn, df: pd.DataFrame, *, dry_run: bool) -> int:
    """Insert rows using INSERT OR IGNORE so re-running is idempotent. Returns count inserted."""
    cols = list(df.columns)
    placeholders = ", ".join(["?"] * len(cols))
    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    sql = f"INSERT OR IGNORE INTO listings ({quoted_cols}) VALUES ({placeholders})"

    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    if dry_run:
        logging.info(f"DRY RUN: would attempt to insert {len(rows)} rows")
        return 0

    with conn:  # single transaction for the whole batch
        cur = conn.executemany(sql, rows)
        return cur.rowcount if cur.rowcount is not None else 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load master xlsx into the SQLite database")
    p.add_argument("--xlsx", default="data/master/MasterMudahCarData.xlsx", help="Source Excel file")
    p.add_argument("--db", default=DEFAULT_DB_PATH, help="Target SQLite database file")
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

    df = prepare_dataframe(df)
    logging.info(f"After cleanup: {len(df)} rows, {len(df.columns)} columns mapped to listings")

    conn = connect(args.db)
    before = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    inserted = insert_rows(conn, df, dry_run=args.dry_run)
    after = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

    logging.info(
        f"listings count: {before} -> {after} (inserted={inserted}, "
        f"skipped duplicates={len(df) - (after - before)})"
    )

    if not args.dry_run:
        version = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]
        logging.info(f"Database ready at {args.db} (schema v{version})")


if __name__ == "__main__":
    main()

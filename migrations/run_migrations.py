"""Idempotent SQLite schema migrations for CarData.

v1 -> v2: Adds API-only and hybrid-tracking columns introduced by the
EagleSearch + HTML hybrid scraper.

Shared columns (cars + motorcycles):
    old_price, year_verified, store_verified, bundle, media_count,
    mileage_bucket, last_detail_fetched_at, detail_fetch_status

Cars only:
    car_loan_eligible, car_loan_payment, car_loan_tenure, has_car_grant

Usage:
    python migrations/run_migrations.py                       # interactive prompt
    python migrations/run_migrations.py --category cars
    python migrations/run_migrations.py --category motorcycles
    python migrations/run_migrations.py --category both
    python migrations/run_migrations.py --category both --dry-run

The runner is idempotent:
- Each ALTER TABLE is guarded by a PRAGMA table_info(...) presence check.
- The whole migrate() exits early when meta.schema_version >= target.
- Re-running on an already-migrated DB is a no-op (logs and exits).

Why a Python runner instead of plain .sql files:
SQLite has no "ALTER TABLE ... ADD COLUMN IF NOT EXISTS". Running the same
.sql twice would fail on the second run. The runner sidesteps that by
checking column existence before each ADD.
"""

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import List, Tuple

# Make src/ importable so we can reuse db.connect()/schema_version()
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_ROOT / "src"))

from db import CATEGORIES, connect, schema_version, set_meta  # noqa: E402

# ---------------------------------------------------------------------------
# Migration spec — v1 -> v2
# ---------------------------------------------------------------------------

TARGET_VERSION = 2

# (column_name, sqlite_type) — applied to BOTH cars and motorcycles
SHARED_NEW_COLS: List[Tuple[str, str]] = [
    ("old_price",              "INTEGER"),
    ("year_verified",          "INTEGER"),
    ("store_verified",         "TEXT"),
    ("bundle",                 "TEXT"),
    ("media_count",            "INTEGER"),
    ("mileage_bucket",         "TEXT"),
    ("last_detail_fetched_at", "TEXT"),
    ("detail_fetch_status",    "TEXT"),
]

# Applied to cars only
CAR_EXTRA_COLS: List[Tuple[str, str]] = [
    ("car_loan_eligible", "INTEGER"),
    ("car_loan_payment",  "INTEGER"),
    ("car_loan_tenure",   "INTEGER"),
    ("has_car_grant",     "INTEGER"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Return True if `column` already exists on `table`."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == column for r in rows)


def _safe_add_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    col_type: str,
    *,
    dry_run: bool,
) -> bool:
    """ALTER TABLE ... ADD COLUMN, guarded by existence check.

    Returns True if a column was actually added (or would be added in dry-run).
    """
    if _column_exists(conn, table, column):
        logging.debug(f"  · {table}.{column} already exists, skipping")
        return False
    if dry_run:
        logging.info(f"  + [DRY-RUN] would add {table}.{column} {col_type}")
        return True
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    logging.info(f"  + added {table}.{column} {col_type}")
    return True


def _columns_for(category: str) -> List[Tuple[str, str]]:
    cols = list(SHARED_NEW_COLS)
    if category == "cars":
        cols.extend(CAR_EXTRA_COLS)
    return cols


# ---------------------------------------------------------------------------
# Per-category migrate
# ---------------------------------------------------------------------------

def migrate(category: str, *, dry_run: bool = False) -> None:
    """Run v1 -> v2 migration on the given category's DB."""
    if category not in CATEGORIES:
        raise ValueError(
            f"Unknown category: {category!r}. Expected one of {CATEGORIES}."
        )

    # init=True is fine: schema_*.sql is idempotent (CREATE TABLE IF NOT EXISTS)
    conn = connect(category)
    current = schema_version(conn)
    logging.info(f"[{category}] current schema_version = {current}")

    if current >= TARGET_VERSION:
        logging.info(
            f"[{category}] already at v{current} (target v{TARGET_VERSION}); "
            f"nothing to do"
        )
        return

    cols = _columns_for(category)
    logging.info(
        f"[{category}] migrating v{current} -> v{TARGET_VERSION} "
        f"({len(cols)} columns to evaluate)"
    )

    added = 0
    # All ADDs in one transaction so a partial failure rolls back cleanly.
    with conn:
        for col_name, col_type in cols:
            if _safe_add_column(conn, "listings", col_name, col_type, dry_run=dry_run):
                added += 1

        if dry_run:
            logging.info(
                f"[{category}] DRY-RUN: would add {added} column(s); "
                f"schema_version unchanged"
            )
            return

        # Bump schema_version inside the same transaction
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(TARGET_VERSION),),
        )

    logging.info(
        f"[{category}] migration complete — added {added} column(s); "
        f"schema_version now {TARGET_VERSION}"
    )

    # Audit trail
    set_meta(
        conn,
        "last_migration_v2_at",
        __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _prompt_category() -> str:
    print("\nSelect category to migrate:")
    print("  1. cars")
    print("  2. motorcycles")
    print("  3. both")
    while True:
        raw = input("Enter 1, 2, or 3: ").strip()
        if raw == "1":
            return "cars"
        if raw == "2":
            return "motorcycles"
        if raw == "3":
            return "both"
        print("Please enter 1, 2, or 3.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Apply CarData schema v1 -> v2 migration "
                    "(adds EagleSearch API + hybrid-tracking columns)"
    )
    p.add_argument(
        "--category",
        choices=("cars", "motorcycles", "both"),
        default=None,
        help="Which DB to migrate. Prompts if omitted.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview ALTER TABLEs; do not modify the DB.",
    )
    return p.parse_args()


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    args = parse_args()
    target = args.category or _prompt_category()

    if args.dry_run:
        logging.info("--- DRY-RUN mode — no DB changes will be made ---")

    targets = list(CATEGORIES) if target == "both" else [target]
    for cat in targets:
        migrate(cat, dry_run=args.dry_run)

    logging.info("All requested migrations complete.")


if __name__ == "__main__":
    main()

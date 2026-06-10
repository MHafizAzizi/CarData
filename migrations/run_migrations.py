"""Idempotent SQLite schema migrations for CarData.

v1 -> v2: Adds API-only and hybrid-tracking columns introduced by the
EagleSearch + HTML hybrid scraper.

Shared columns (cars + motorcycles):
    old_price, year_verified, store_verified, bundle, media_count,
    mileage_bucket, last_detail_fetched_at, detail_fetch_status

Cars only:
    car_loan_eligible, car_loan_payment, car_loan_tenure, has_car_grant

v2 -> v3: Drops the `body` column from listings (cars + motorcycles).
The full seller description is no longer stored — see commit history for
rationale.

v3 -> v4: Retypes text-stored numeric columns to INTEGER so SQL MIN/MAX/
ORDER BY work without explicit CAST. SQLite has no ALTER COLUMN TYPE, so
we do a table swap: create new table with proper types, copy data with
CAST, drop old, rename.

Retyped columns (both categories):
    price, mileage, manufactured_date, company_ad
Cars only:
    engine_capacity, cc

v4 -> v5: Adds ad_expiry (listing renewal deadline from EagleSearch API)
and sold_inference (likely_sold / likely_expired / unknown) to both
categories. ad_expiry lets recheck.py distinguish sold listings (disappeared
before expiry) from expired ones (lapsed after expiry).

v5 -> v6: Drops permanently-empty / dead-weight columns (verified 0% fill).
Cars: 25 Phase-2 HTML spec columns (removed with Phase 2, never repopulated
by the API-only scraper) plus mileage, location, last_detail_fetched_at,
detail_fetch_status. Motorcycles: mileage, location, mileage_bucket,
year_verified (the API returns none of these for category=1040) plus the two
detail_* Phase-2 remnants. ad_expiry is intentionally KEPT (real data for
motorcycles; 0% on cars is a capture bug, not dead weight).

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

TARGET_VERSION = 6

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

# v2 -> v3: columns to drop from `listings` (both categories)
DROPPED_COLS_V3: List[str] = ["body"]

# v3 -> v4: columns to retype from TEXT to INTEGER (numeric data stored as text).
RETYPE_COLS_BOTH: List[str] = ["price", "mileage", "manufactured_date", "company_ad"]
RETYPE_COLS_CARS: List[str] = ["engine_capacity", "cc"]

# v4 -> v5: new columns for both categories
V5_SHARED_COLS: List[Tuple[str, str]] = [
    ("ad_expiry",      "TEXT"),
    ("sold_inference", "TEXT"),
]

# v5 -> v6: drop permanently-empty / dead-weight columns (verified 0% fill).
# ad_expiry is deliberately NOT here — it's real data for motorcycles and the
# cars 0% is a known capture bug, not dead weight.
DROPPED_COLS_V6_CARS: List[str] = [
    # Phase-2 remnants (Phase 2 removed; API-only scraper never fills these)
    "mileage", "location", "last_detail_fetched_at", "detail_fetch_status",
    # Phase-2 HTML spec columns — all 0% fill, no API source
    "family", "series", "style", "seat", "country_origin", "cc",
    "comp_ratio", "kw", "torque", "engine", "length", "width", "height",
    "wheelbase", "kerbwt", "fueltk", "brake_front", "brake_rear",
    "suspension_front", "suspension_rear", "steering",
    "tyres_front", "tyres_rear", "wheel_rim_front", "wheel_rim_rear",
]

DROPPED_COLS_V6_MOTORCYCLES: List[str] = [
    # API returns none of these for category=1040 — permanently NULL
    "mileage", "location", "mileage_bucket", "year_verified",
    # Phase-2 remnants
    "last_detail_fetched_at", "detail_fetch_status",
]


def _dropped_cols_v6(category: str) -> List[str]:
    return (
        DROPPED_COLS_V6_CARS if category == "cars"
        else DROPPED_COLS_V6_MOTORCYCLES
    )

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


def _safe_drop_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    *,
    dry_run: bool,
) -> bool:
    """ALTER TABLE ... DROP COLUMN, guarded by existence check.

    Requires SQLite >= 3.35 (2021-03). Returns True if the drop ran (or would
    run in dry-run).
    """
    if not _column_exists(conn, table, column):
        logging.debug(f"  · {table}.{column} already absent, skipping")
        return False
    if dry_run:
        logging.info(f"  - [DRY-RUN] would drop {table}.{column}")
        return True
    conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
    logging.info(f"  - dropped {table}.{column}")
    return True


def _columns_for(category: str) -> List[Tuple[str, str]]:
    cols = list(SHARED_NEW_COLS)
    if category == "cars":
        cols.extend(CAR_EXTRA_COLS)
    return cols


# ---------------------------------------------------------------------------
# Per-category migrate
# ---------------------------------------------------------------------------

def _migrate_v1_to_v2(
    conn: sqlite3.Connection, category: str, *, dry_run: bool
) -> None:
    """Add EagleSearch + hybrid-tracking columns."""
    cols = _columns_for(category)
    logging.info(
        f"[{category}] step v1 -> v2 ({len(cols)} columns to evaluate)"
    )

    added = 0
    with conn:
        for col_name, col_type in cols:
            if _safe_add_column(conn, "listings", col_name, col_type, dry_run=dry_run):
                added += 1

        if dry_run:
            logging.info(
                f"[{category}] DRY-RUN v1->v2: would add {added} column(s)"
            )
            return

        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '2') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )

    logging.info(
        f"[{category}] v1 -> v2 complete — added {added} column(s)"
    )
    set_meta(
        conn,
        "last_migration_v2_at",
        __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def _migrate_v2_to_v3(
    conn: sqlite3.Connection, category: str, *, dry_run: bool
) -> None:
    """Drop columns no longer stored (currently: body)."""
    logging.info(
        f"[{category}] step v2 -> v3 ({len(DROPPED_COLS_V3)} column(s) to drop)"
    )

    dropped = 0
    with conn:
        for col_name in DROPPED_COLS_V3:
            if _safe_drop_column(conn, "listings", col_name, dry_run=dry_run):
                dropped += 1

        if dry_run:
            logging.info(
                f"[{category}] DRY-RUN v2->v3: would drop {dropped} column(s)"
            )
            return

        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '3') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )

    logging.info(
        f"[{category}] v2 -> v3 complete — dropped {dropped} column(s)"
    )
    set_meta(
        conn,
        "last_migration_v3_at",
        __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def _migrate_v3_to_v4(
    conn: sqlite3.Connection, category: str, *, dry_run: bool
) -> None:
    """Retype TEXT columns that hold numeric values to INTEGER.

    SQLite has no `ALTER COLUMN TYPE`, so we do the standard table-swap:
        1. CREATE TABLE listings_new with proper INTEGER types
        2. INSERT INTO listings_new SELECT ... CAST(...) ... FROM listings
        3. DROP TABLE listings; ALTER TABLE listings_new RENAME TO listings
        4. Recreate indexes

    The new column definitions preserve everything from the existing schema
    (PRIMARY KEY, NOT NULL, DEFAULT, CHECK) and only flip the type for the
    columns listed in RETYPE_COLS_*.
    """
    retype = set(RETYPE_COLS_BOTH)
    if category == "cars":
        retype.update(RETYPE_COLS_CARS)

    # Snapshot the current schema so we can rebuild it faithfully
    cols_info = conn.execute("PRAGMA table_info(listings)").fetchall()
    present_retypes = sorted(c["name"] for c in cols_info if c["name"] in retype)

    logging.info(
        f"[{category}] step v3 -> v4 "
        f"({len(present_retypes)} column(s) to retype: {present_retypes})"
    )

    if not present_retypes:
        logging.info(f"[{category}] v3 -> v4: no columns to retype")
    elif dry_run:
        logging.info(
            f"[{category}] DRY-RUN v3->v4: would retype {present_retypes} to INTEGER"
        )
        return
    else:
        # Build new column clauses, preserving constraints
        col_clauses: List[str] = []
        col_names: List[str] = []
        for col in cols_info:
            name = col["name"]
            new_type = "INTEGER" if name in retype else (col["type"] or "TEXT")
            clause = f"{name} {new_type}"
            if col["pk"]:
                clause += " PRIMARY KEY"
            if col["notnull"]:
                clause += " NOT NULL"
            if col["dflt_value"] is not None:
                clause += f" DEFAULT {col['dflt_value']}"
            # availability_status has a CHECK constraint we must preserve
            if name == "availability_status":
                clause += (
                    " CHECK (availability_status IN "
                    "('available','unavailable','unknown'))"
                )
            col_clauses.append(clause)
            col_names.append(name)

        # Build SELECT expressions: CAST retyped cols, pass-through the rest.
        # Wrap CAST in NULLIF to keep empty strings as NULL after coercion.
        select_exprs = [
            f"CAST(NULLIF({n}, '') AS INTEGER) AS {n}" if n in retype else n
            for n in col_names
        ]

        create_sql = f"CREATE TABLE listings_new ({', '.join(col_clauses)})"
        insert_sql = (
            f"INSERT INTO listings_new ({', '.join(col_names)}) "
            f"SELECT {', '.join(select_exprs)} FROM listings"
        )

        with conn:
            conn.execute(create_sql)
            conn.execute(insert_sql)
            conn.execute("DROP TABLE listings")
            conn.execute("ALTER TABLE listings_new RENAME TO listings")
            # Recreate the listings index (availability_checks index survives)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_listings_status "
                "ON listings(availability_status, last_checked_at)"
            )

    # Bump schema_version (no-op DRY-RUN already returned above)
    with conn:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '4') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )

    logging.info(f"[{category}] v3 -> v4 complete — retyped {len(present_retypes)} column(s)")
    set_meta(
        conn,
        "last_migration_v4_at",
        __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def _migrate_v4_to_v5(
    conn: sqlite3.Connection, category: str, *, dry_run: bool
) -> None:
    """Add ad_expiry and sold_inference columns (both categories)."""
    logging.info(
        f"[{category}] step v4 -> v5 ({len(V5_SHARED_COLS)} columns to evaluate)"
    )

    added = 0
    with conn:
        for col_name, col_type in V5_SHARED_COLS:
            if _safe_add_column(conn, "listings", col_name, col_type, dry_run=dry_run):
                added += 1

        if dry_run:
            logging.info(
                f"[{category}] DRY-RUN v4->v5: would add {added} column(s)"
            )
            return

        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '5') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )

    logging.info(
        f"[{category}] v4 -> v5 complete — added {added} column(s)"
    )
    set_meta(
        conn,
        "last_migration_v5_at",
        __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def _migrate_v5_to_v6(
    conn: sqlite3.Connection, category: str, *, dry_run: bool
) -> None:
    """Drop permanently-empty / dead-weight columns (per-category)."""
    cols = _dropped_cols_v6(category)
    logging.info(
        f"[{category}] step v5 -> v6 ({len(cols)} column(s) to drop)"
    )

    dropped = 0
    with conn:
        for col_name in cols:
            if _safe_drop_column(conn, "listings", col_name, dry_run=dry_run):
                dropped += 1

        if dry_run:
            logging.info(
                f"[{category}] DRY-RUN v5->v6: would drop {dropped} column(s)"
            )
            return

        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '6') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )

    logging.info(
        f"[{category}] v5 -> v6 complete — dropped {dropped} column(s)"
    )
    set_meta(
        conn,
        "last_migration_v6_at",
        __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def migrate(category: str, *, dry_run: bool = False) -> None:
    """Run all pending migrations on the given category's DB."""
    if category not in CATEGORIES:
        raise ValueError(
            f"Unknown category: {category!r}. Expected one of {CATEGORIES}."
        )

    # init=True: migrations are the canonical DB-creation entry point and
    # schema_*.sql is idempotent (CREATE TABLE IF NOT EXISTS)
    conn = connect(category, init=True)
    current = schema_version(conn)
    logging.info(f"[{category}] current schema_version = {current}")

    if current >= TARGET_VERSION:
        logging.info(
            f"[{category}] already at v{current} (target v{TARGET_VERSION}); "
            f"nothing to do"
        )
        return

    logging.info(
        f"[{category}] migrating v{current} -> v{TARGET_VERSION}"
    )

    if current < 2:
        _migrate_v1_to_v2(conn, category, dry_run=dry_run)
    if current < 3:
        _migrate_v2_to_v3(conn, category, dry_run=dry_run)
    if current < 4:
        _migrate_v3_to_v4(conn, category, dry_run=dry_run)
    if current < 5:
        _migrate_v4_to_v5(conn, category, dry_run=dry_run)
    if current < 6:
        _migrate_v5_to_v6(conn, category, dry_run=dry_run)

    logging.info(
        f"[{category}] migration complete; "
        f"schema_version now {TARGET_VERSION if not dry_run else current}"
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

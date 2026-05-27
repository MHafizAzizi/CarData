"""Tests for migrations/run_migrations.py — schema v1 -> v3 runner."""

import sqlite3
import sys
from pathlib import Path

import pytest

# Make migrations/ importable
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "migrations"))
sys.path.insert(0, str(_ROOT / "src"))

import run_migrations as rm  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures: in-memory v1 DBs
# ---------------------------------------------------------------------------

def _read_schema(category: str) -> str:
    path = _ROOT / f"schema_{category}.sql"
    return path.read_text(encoding="utf-8")


def _make_v1_db(tmp_path: Path, category: str) -> sqlite3.Connection:
    """Create a v1 DB on disk (tmp_path) by applying the canonical schema_*.sql."""
    db_file = tmp_path / f"cardata_{category}.db"
    conn = sqlite3.connect(db_file, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.executescript(_read_schema(category))
    return conn


def _columns_of(conn: sqlite3.Connection, table: str) -> set:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def _schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    return int(row["value"]) if row else 0


# ---------------------------------------------------------------------------
# _column_exists
# ---------------------------------------------------------------------------

class TestColumnExists:
    def test_returns_true_for_existing_column(self, tmp_path):
        conn = _make_v1_db(tmp_path, "cars")
        assert rm._column_exists(conn, "listings", "ads_id") is True

    def test_returns_false_for_missing_column(self, tmp_path):
        conn = _make_v1_db(tmp_path, "cars")
        assert rm._column_exists(conn, "listings", "old_price") is False


# ---------------------------------------------------------------------------
# _safe_add_column
# ---------------------------------------------------------------------------

class TestSafeAddColumn:
    def test_adds_when_absent(self, tmp_path):
        conn = _make_v1_db(tmp_path, "cars")
        added = rm._safe_add_column(conn, "listings", "old_price", "INTEGER", dry_run=False)
        assert added is True
        assert "old_price" in _columns_of(conn, "listings")

    def test_skips_when_present(self, tmp_path):
        conn = _make_v1_db(tmp_path, "cars")
        rm._safe_add_column(conn, "listings", "old_price", "INTEGER", dry_run=False)
        added_again = rm._safe_add_column(conn, "listings", "old_price", "INTEGER", dry_run=False)
        assert added_again is False  # second call is a no-op

    def test_dry_run_does_not_modify_db(self, tmp_path):
        conn = _make_v1_db(tmp_path, "cars")
        before = _columns_of(conn, "listings")
        added = rm._safe_add_column(conn, "listings", "old_price", "INTEGER", dry_run=True)
        after = _columns_of(conn, "listings")
        assert added is True  # reports it would add
        assert before == after  # but did not


# ---------------------------------------------------------------------------
# _columns_for
# ---------------------------------------------------------------------------

class TestColumnsFor:
    def test_motorcycles_only_shared(self):
        cols = rm._columns_for("motorcycles")
        names = {c[0] for c in cols}
        assert names == {c[0] for c in rm.SHARED_NEW_COLS}
        # No car-loan fields
        assert "car_loan_eligible" not in names

    def test_cars_includes_extras(self):
        cols = rm._columns_for("cars")
        names = {c[0] for c in cols}
        assert names == {c[0] for c in rm.SHARED_NEW_COLS} | {c[0] for c in rm.CAR_EXTRA_COLS}
        assert "car_loan_eligible" in names
        assert "has_car_grant" in names


# ---------------------------------------------------------------------------
# migrate() — full workflow with monkeypatched db.connect
# ---------------------------------------------------------------------------

@pytest.fixture
def patched_connect(tmp_path, monkeypatch):
    """Redirect rm.connect to use tmp_path DBs (no schema init — manual control)."""
    def fake_connect(category, *, path=None, init=True):
        # Mirror db.connect tuning
        db_file = tmp_path / f"cardata_{category}.db"
        # Apply v1 schema if file is brand-new and init=True
        is_new = not db_file.exists()
        conn = sqlite3.connect(db_file, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.executescript("PRAGMA journal_mode=WAL;PRAGMA foreign_keys=ON;")
        if init and is_new:
            conn.executescript(_read_schema(category))
        return conn

    monkeypatch.setattr(rm, "connect", fake_connect)
    return tmp_path


class TestMigrate:
    def test_migrate_cars_adds_all_columns(self, patched_connect):
        rm.migrate("cars", dry_run=False)
        # Re-open and inspect
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        for col, _type in rm.SHARED_NEW_COLS:
            assert col in cols, f"missing {col}"
        for col, _type in rm.CAR_EXTRA_COLS:
            assert col in cols, f"missing {col}"
        assert _schema_version(conn) == 4

    def test_migrate_motorcycles_adds_only_shared(self, patched_connect):
        rm.migrate("motorcycles", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        for col, _type in rm.SHARED_NEW_COLS:
            assert col in cols
        # Car-only cols MUST NOT be present
        for col, _type in rm.CAR_EXTRA_COLS:
            assert col not in cols, f"{col} should not be in motorcycles DB"
        assert _schema_version(conn) == 4

    def test_migrate_idempotent(self, patched_connect):
        rm.migrate("cars", dry_run=False)
        # Second call must be a clean no-op
        rm.migrate("cars", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        assert _schema_version(conn) == 4
        # All columns still present, no duplicates (sqlite would raise if duplicated)
        cols = _columns_of(conn, "listings")
        for col, _type in rm.SHARED_NEW_COLS + rm.CAR_EXTRA_COLS:
            assert col in cols

    def test_migrate_dry_run_does_not_change_db(self, patched_connect):
        rm.migrate("cars", dry_run=True)
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        # None of the new cols should exist
        for col, _type in rm.SHARED_NEW_COLS:
            assert col not in cols
        # schema_version unchanged at 1
        assert _schema_version(conn) == 1

    def test_migrate_unknown_category_raises(self, patched_connect):
        with pytest.raises(ValueError, match="Unknown category"):
            rm.migrate("trucks", dry_run=False)


# ---------------------------------------------------------------------------
# v2 -> v3: drop body column
# ---------------------------------------------------------------------------

class TestV3DropBody:
    def _make_v2_db_with_body(self, tmp_path: Path, category: str) -> Path:
        """Construct a v2-state DB that still carries the legacy body column."""
        db_file = tmp_path / f"cardata_{category}.db"
        conn = sqlite3.connect(db_file, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.executescript(_read_schema(category))
        conn.execute("ALTER TABLE listings ADD COLUMN body TEXT")
        # Add v2 cols so schema is consistent
        for col, ctype in rm.SHARED_NEW_COLS:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {ctype}")
        if category == "cars":
            for col, ctype in rm.CAR_EXTRA_COLS:
                conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {ctype}")
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '2') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        conn.close()
        return db_file

    def test_drops_body_and_bumps_version(self, tmp_path, monkeypatch):
        self._make_v2_db_with_body(tmp_path, "cars")

        def fake_connect(category, *, path=None, init=True):
            db_file = tmp_path / f"cardata_{category}.db"
            conn = sqlite3.connect(db_file, isolation_level=None)
            conn.row_factory = sqlite3.Row
            return conn

        monkeypatch.setattr(rm, "connect", fake_connect)
        rm.migrate("cars", dry_run=False)

        conn = sqlite3.connect(tmp_path / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        assert "body" not in cols
        assert _schema_version(conn) == 4

    def test_dry_run_keeps_body(self, tmp_path, monkeypatch):
        self._make_v2_db_with_body(tmp_path, "cars")

        def fake_connect(category, *, path=None, init=True):
            db_file = tmp_path / f"cardata_{category}.db"
            conn = sqlite3.connect(db_file, isolation_level=None)
            conn.row_factory = sqlite3.Row
            return conn

        monkeypatch.setattr(rm, "connect", fake_connect)
        rm.migrate("cars", dry_run=True)

        conn = sqlite3.connect(tmp_path / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        assert "body" in _columns_of(conn, "listings")
        assert _schema_version(conn) == 2


# ---------------------------------------------------------------------------
# v3 -> v4: retype TEXT numeric columns to INTEGER
# ---------------------------------------------------------------------------

class TestV4Retype:
    def _column_type(self, conn: sqlite3.Connection, table: str, col: str) -> str:
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall():
            if row["name"] == col:
                return row["type"]
        return ""

    def test_retypes_numeric_columns_to_integer(self, patched_connect):
        # Run full v1 -> v4 chain on a fresh DB
        rm.migrate("motorcycles", dry_run=False)

        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row

        assert _schema_version(conn) == 4
        for col in rm.RETYPE_COLS_BOTH:
            assert self._column_type(conn, "listings", col) == "INTEGER", (
                f"motorcycles.{col} should be INTEGER post-v4"
            )

    def test_cars_retypes_extra_columns(self, patched_connect):
        rm.migrate("cars", dry_run=False)

        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row

        for col in rm.RETYPE_COLS_BOTH + rm.RETYPE_COLS_CARS:
            assert self._column_type(conn, "listings", col) == "INTEGER"

    def test_existing_string_data_is_cast(self, patched_connect):
        # Insert TEXT data before running v4 to verify CAST during table swap.
        db_file = patched_connect / "cardata_motorcycles.db"
        conn = sqlite3.connect(db_file, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.executescript(_read_schema("motorcycles"))
        # Note: schema files were already updated to INTEGER for these cols,
        # so insert via TEXT-affinity-typed values to simulate a pre-v4 DB
        # state. We force the column types back to TEXT for the test.
        conn.execute("ALTER TABLE listings RENAME TO listings_old")
        conn.executescript("""
            CREATE TABLE listings (
                ads_id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                price TEXT,
                mileage TEXT,
                manufactured_date TEXT,
                company_ad TEXT,
                first_seen_at TEXT NOT NULL,
                availability_status TEXT NOT NULL DEFAULT 'unknown'
                    CHECK (availability_status IN ('available','unavailable','unknown'))
            );
        """)
        # Add v2 + drop body so we're a clean v3 with TEXT numeric cols
        for col, ctype in rm.SHARED_NEW_COLS:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {ctype}")
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '3') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        conn.execute(
            "INSERT INTO listings (ads_id, price, mileage, manufactured_date, "
            "company_ad, first_seen_at) "
            "VALUES (1, '12500', '50000', '2020', '0', '2026-01-01')"
        )
        conn.close()

        # Now run v4 migration via patched_connect (already monkeypatched)
        rm.migrate("motorcycles", dry_run=False)

        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT price, typeof(price) AS pt, "
            "       mileage, typeof(mileage) AS mt, "
            "       manufactured_date, typeof(manufactured_date) AS dt, "
            "       company_ad, typeof(company_ad) AS ct "
            "FROM listings WHERE ads_id = 1"
        ).fetchone()
        assert row["pt"] == "integer", f"price typeof={row['pt']}"
        assert row["mt"] == "integer", f"mileage typeof={row['mt']}"
        assert row["dt"] == "integer", f"manufactured_date typeof={row['dt']}"
        assert row["ct"] == "integer", f"company_ad typeof={row['ct']}"
        assert row["price"] == 12500
        assert row["mileage"] == 50000

    def test_empty_string_becomes_null(self, patched_connect):
        # Empty strings in TEXT columns should NULLIF to NULL, not 0.
        db_file = patched_connect / "cardata_motorcycles.db"
        conn = sqlite3.connect(db_file, isolation_level=None)
        conn.executescript(_read_schema("motorcycles"))
        conn.execute("ALTER TABLE listings RENAME TO listings_old")
        conn.executescript("""
            CREATE TABLE listings (
                ads_id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                price TEXT,
                mileage TEXT,
                manufactured_date TEXT,
                company_ad TEXT,
                first_seen_at TEXT NOT NULL,
                availability_status TEXT NOT NULL DEFAULT 'unknown'
                    CHECK (availability_status IN ('available','unavailable','unknown'))
            );
        """)
        for col, ctype in rm.SHARED_NEW_COLS:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {ctype}")
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '3') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        conn.execute(
            "INSERT INTO listings (ads_id, price, mileage, first_seen_at) "
            "VALUES (2, '', '', '2026-01-01')"
        )
        conn.close()

        rm.migrate("motorcycles", dry_run=False)

        conn = sqlite3.connect(db_file)
        row = conn.execute(
            "SELECT price, mileage FROM listings WHERE ads_id = 2"
        ).fetchone()
        assert row[0] is None
        assert row[1] is None

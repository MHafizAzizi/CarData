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
        assert _schema_version(conn) == 3

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
        assert _schema_version(conn) == 3

    def test_migrate_idempotent(self, patched_connect):
        rm.migrate("cars", dry_run=False)
        # Second call must be a clean no-op
        rm.migrate("cars", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        assert _schema_version(conn) == 3
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
        assert _schema_version(conn) == 3

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

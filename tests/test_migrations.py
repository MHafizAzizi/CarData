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
        cars_dropped = set(rm.DROPPED_COLS_V6_CARS)
        for col, _type in rm.SHARED_NEW_COLS:
            if col in cars_dropped:
                continue  # added at v2, dropped again at v6
            assert col in cols, f"missing {col}"
        for col, _type in rm.CAR_EXTRA_COLS:
            assert col in cols, f"missing {col}"
        for col, _type in rm.V5_SHARED_COLS:
            assert col in cols, f"missing v5 col {col}"
        # v6 dead-weight columns must be gone — except those legitimately
        # re-added as mcdParams spec cols at v9 (e.g. country_origin, series).
        v9_readded = {c for c, _ in rm.V9_CAR_COLS}
        for col in cars_dropped - v9_readded:
            assert col not in cols, f"{col} should be dropped at v6"
        assert _schema_version(conn) == 10

    def test_migrate_motorcycles_adds_only_shared(self, patched_connect):
        rm.migrate("motorcycles", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        moto_dropped = set(rm.DROPPED_COLS_V6_MOTORCYCLES)
        for col, _type in rm.SHARED_NEW_COLS:
            if col in moto_dropped:
                continue  # added at v2, dropped again at v6
            assert col in cols
        # Car-only cols MUST NOT be present
        for col, _type in rm.CAR_EXTRA_COLS:
            assert col not in cols, f"{col} should not be in motorcycles DB"
        for col, _type in rm.V5_SHARED_COLS:
            assert col in cols, f"missing v5 col {col}"
        for col in moto_dropped:
            assert col not in cols, f"{col} should be dropped at v6"
        assert _schema_version(conn) == 10

    def test_migrate_idempotent(self, patched_connect):
        rm.migrate("cars", dry_run=False)
        # Second call must be a clean no-op
        rm.migrate("cars", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        assert _schema_version(conn) == 10
        # All surviving columns still present, no duplicates (sqlite would raise
        # if duplicated)
        cols = _columns_of(conn, "listings")
        cars_dropped = set(rm.DROPPED_COLS_V6_CARS)
        for col, _type in rm.SHARED_NEW_COLS + rm.CAR_EXTRA_COLS + rm.V5_SHARED_COLS:
            if col in cars_dropped:
                continue
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
        # v2->v3 drops body; v5->v6 drops dead cols; v6->v10 adds type/spec cols
        # → full chain ends at v10
        assert _schema_version(conn) == 10

    def test_drops_body_and_adds_v5_cols(self, tmp_path, monkeypatch):
        """v2->v3 drops body; v4->v5 adds ad_expiry + sold_inference in same run."""
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
        assert "ad_expiry" in cols
        assert "sold_inference" in cols

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
        # Run full v1 -> v5 chain on a fresh DB; v4 retype is one of the steps
        rm.migrate("motorcycles", dry_run=False)

        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row

        assert _schema_version(conn) == 10  # full chain ends at v10
        moto_dropped = set(rm.DROPPED_COLS_V6_MOTORCYCLES)
        for col in rm.RETYPE_COLS_BOTH:
            if col in moto_dropped:
                continue  # retyped at v4, then dropped at v6 (e.g. mileage)
            assert self._column_type(conn, "listings", col) == "INTEGER", (
                f"motorcycles.{col} should be INTEGER post-v4"
            )

    def test_cars_retypes_extra_columns(self, patched_connect):
        rm.migrate("cars", dry_run=False)

        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row

        cars_dropped = set(rm.DROPPED_COLS_V6_CARS)
        for col in rm.RETYPE_COLS_BOTH + rm.RETYPE_COLS_CARS:
            if col in cars_dropped:
                continue  # retyped at v4, then dropped at v6 (e.g. mileage, cc)
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
        # mileage is cast to INTEGER at v4 but dropped at v6, so it is not
        # selectable post-migration — verify the surviving retyped columns.
        row = conn.execute(
            "SELECT price, typeof(price) AS pt, "
            "       manufactured_date, typeof(manufactured_date) AS dt, "
            "       company_ad, typeof(company_ad) AS ct "
            "FROM listings WHERE ads_id = 1"
        ).fetchone()
        assert row["pt"] == "integer", f"price typeof={row['pt']}"
        assert row["dt"] == "integer", f"manufactured_date typeof={row['dt']}"
        assert row["ct"] == "integer", f"company_ad typeof={row['ct']}"
        assert row["price"] == 12500

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

        # mileage is dropped at v6; price ('' -> NULLIF -> NULL) still verifies
        # the empty-string-to-NULL behaviour from the v4 CAST.
        conn = sqlite3.connect(db_file)
        row = conn.execute(
            "SELECT price FROM listings WHERE ads_id = 2"
        ).fetchone()
        assert row[0] is None


# ---------------------------------------------------------------------------
# v4 -> v5: ad_expiry + sold_inference
# ---------------------------------------------------------------------------

class TestV5AddExpiry:
    def _make_v4_db(self, tmp_path: Path, category: str) -> Path:
        """Construct a v4-state DB (post body-drop, pre ad_expiry/sold_inference).

        The schema files now include v5 columns, so we explicitly drop them
        after applying the schema to simulate a genuine pre-v5 DB state.
        """
        db_file = tmp_path / f"cardata_{category}.db"
        conn = sqlite3.connect(db_file, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.executescript(_read_schema(category))
        # Add v2 cols
        for col, ctype in rm.SHARED_NEW_COLS:
            if col not in {c for c, _ in rm.V5_SHARED_COLS}:
                conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {ctype}")
        if category == "cars":
            for col, ctype in rm.CAR_EXTRA_COLS:
                conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {ctype}")
        # Drop body (v3): add it first so we can drop it
        conn.execute("ALTER TABLE listings ADD COLUMN body TEXT")
        conn.execute("ALTER TABLE listings DROP COLUMN body")
        # Remove v5 cols that came from the updated schema file so this fixture
        # accurately represents a pre-v5 DB state.
        for col, _ in rm.V5_SHARED_COLS:
            try:
                conn.execute(f"ALTER TABLE listings DROP COLUMN {col}")
            except Exception:
                pass  # column not present — no-op
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '4') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        conn.close()
        return db_file

    def test_adds_ad_expiry_and_sold_inference(self, tmp_path, monkeypatch):
        self._make_v4_db(tmp_path, "motorcycles")

        def fake_connect(category, *, path=None, init=True):
            db_file = tmp_path / f"cardata_{category}.db"
            conn = sqlite3.connect(db_file, isolation_level=None)
            conn.row_factory = sqlite3.Row
            return conn

        monkeypatch.setattr(rm, "connect", fake_connect)
        rm.migrate("motorcycles", dry_run=False)

        conn = sqlite3.connect(tmp_path / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        assert "ad_expiry" in cols
        assert "sold_inference" in cols
        assert _schema_version(conn) == 10

    def test_v5_idempotent_on_v5_db(self, tmp_path, monkeypatch):
        """Running migrate() on an already-v5 DB is a clean no-op."""
        self._make_v4_db(tmp_path, "cars")

        calls = []

        def fake_connect(category, *, path=None, init=True):
            db_file = tmp_path / f"cardata_{category}.db"
            conn = sqlite3.connect(db_file, isolation_level=None)
            conn.row_factory = sqlite3.Row
            return conn

        monkeypatch.setattr(rm, "connect", fake_connect)
        rm.migrate("cars", dry_run=False)  # first run: reaches v7
        rm.migrate("cars", dry_run=False)  # second run: no-op

        conn = sqlite3.connect(tmp_path / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        assert _schema_version(conn) == 10

    def test_dry_run_does_not_add_v5_cols(self, tmp_path, monkeypatch):
        self._make_v4_db(tmp_path, "motorcycles")

        def fake_connect(category, *, path=None, init=True):
            db_file = tmp_path / f"cardata_{category}.db"
            conn = sqlite3.connect(db_file, isolation_level=None)
            conn.row_factory = sqlite3.Row
            return conn

        monkeypatch.setattr(rm, "connect", fake_connect)
        rm.migrate("motorcycles", dry_run=True)

        conn = sqlite3.connect(tmp_path / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        assert "ad_expiry" not in cols
        assert "sold_inference" not in cols
        assert _schema_version(conn) == 4


# ---------------------------------------------------------------------------
# v5 -> v6: drop permanently-empty / dead-weight columns
# ---------------------------------------------------------------------------

class TestV6DropDeadCols:
    def test_cars_drops_dead_cols_keeps_ad_expiry(self, patched_connect):
        """Cars full chain to v6 drops 29 dead cols, keeps ad_expiry + live cols."""
        rm.migrate("cars", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")

        # v9 re-adds some v6-dropped cols as mcdParams spec cols (e.g.
        # country_origin, series) — exclude those from the dropped check.
        v9_readded = {c for c, _ in rm.V9_CAR_COLS}
        for col in set(rm.DROPPED_COLS_V6_CARS) - v9_readded:
            assert col not in cols, f"{col} should be dropped at v6"
        # ad_expiry is explicitly NOT dropped
        assert "ad_expiry" in cols
        # cars-only survivors
        assert "mileage_bucket" in cols
        assert "year_verified" in cols
        assert "variant" in cols
        assert _schema_version(conn) == 10

    def test_motorcycles_drops_dead_cols_keeps_ad_expiry(self, patched_connect):
        rm.migrate("motorcycles", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")

        for col in rm.DROPPED_COLS_V6_MOTORCYCLES:
            assert col not in cols, f"{col} should be dropped at v6"
        assert "ad_expiry" in cols
        assert _schema_version(conn) == 10

    def test_v6_dry_run_keeps_cols(self, tmp_path, monkeypatch):
        """Dry-run on a v5 DB drops nothing and leaves version at 5."""
        # Build a genuine v5 DB by running the real chain, then rolling the
        # version marker back to 5 with all v6-target cols intact.
        db_file = tmp_path / "cardata_cars.db"
        conn = sqlite3.connect(db_file, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.executescript(_read_schema("cars"))
        for col, ctype in rm.SHARED_NEW_COLS:
            if col not in _columns_of(conn, "listings"):
                conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {ctype}")
        for col, ctype in rm.CAR_EXTRA_COLS:
            if col not in _columns_of(conn, "listings"):
                conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {ctype}")
        # ensure a v6-target col exists so we can prove dry-run keeps it
        if "mileage" not in _columns_of(conn, "listings"):
            conn.execute("ALTER TABLE listings ADD COLUMN mileage INTEGER")
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '5') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        conn.close()

        def fake_connect(category, *, path=None, init=True):
            c = sqlite3.connect(tmp_path / f"cardata_{category}.db", isolation_level=None)
            c.row_factory = sqlite3.Row
            return c

        monkeypatch.setattr(rm, "connect", fake_connect)
        rm.migrate("cars", dry_run=True)

        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        assert "mileage" in _columns_of(conn, "listings")
        assert _schema_version(conn) == 5


# ---------------------------------------------------------------------------
# v6 -> v7: motorcycle type columns
# ---------------------------------------------------------------------------

class TestV7MotorcycleTypeCols:
    def test_motorcycles_gains_type_cols(self, patched_connect):
        rm.migrate("motorcycles", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        for col, _type in rm.V7_MOTORCYCLE_COLS:
            assert col in cols, f"missing v7 col {col}"
        assert _schema_version(conn) == 10

    def test_cars_version_bump_without_motorcycle_type_col(self, patched_connect):
        # type_group legitimately appears on cars at v8, so only the
        # motorcycle-specific column proves v7 left cars alone.
        rm.migrate("cars", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        assert "motorcycle_type" not in cols, "motorcycle_type must not be added to cars"
        assert _schema_version(conn) == 10

    def test_v7_idempotent(self, patched_connect):
        rm.migrate("motorcycles", dry_run=False)
        rm.migrate("motorcycles", dry_run=False)  # no-op
        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        assert _schema_version(conn) == 10


# ---------------------------------------------------------------------------
# v7 -> v8: car vehicle-type columns
# ---------------------------------------------------------------------------

class TestV8CarVehicleTypeCols:
    def test_cars_gains_vehicle_type_cols(self, patched_connect):
        rm.migrate("cars", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        for col, _type in rm.V8_CAR_COLS:
            assert col in cols, f"missing v8 col {col}"
        assert _schema_version(conn) == 10

    def test_motorcycles_version_bump_without_vehicle_type_col(self, patched_connect):
        # type_group legitimately exists on motorcycles since v7, so only
        # the car-specific column proves v8 left motorcycles alone.
        rm.migrate("motorcycles", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        assert "vehicle_type" not in cols, "vehicle_type must not be added to motorcycles"
        assert _schema_version(conn) == 10

    def test_v8_idempotent(self, patched_connect):
        rm.migrate("cars", dry_run=False)
        rm.migrate("cars", dry_run=False)  # no-op
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        assert _schema_version(conn) == 10


# ---------------------------------------------------------------------------
# v9 -> v10: motorcycle OEM-spec columns
# ---------------------------------------------------------------------------

class TestV10MotorcycleSpecCols:
    def test_motorcycles_gains_spec_cols(self, patched_connect):
        rm.migrate("motorcycles", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        for col, _type in rm.V10_MOTORCYCLE_COLS:
            assert col in cols, f"missing v10 col {col}"
        assert _schema_version(conn) == 10

    def test_cars_version_bump_without_spec_cols(self, patched_connect):
        # engine_cc/comp_ratio etc. legitimately exist on cars since v9, so
        # the moto-only provenance cols prove v10 left cars alone.
        rm.migrate("cars", dry_run=False)
        conn = sqlite3.connect(patched_connect / "cardata_cars.db")
        conn.row_factory = sqlite3.Row
        cols = _columns_of(conn, "listings")
        assert "spec_match" not in cols, "spec_match must not be added to cars"
        assert "spec_source" not in cols, "spec_source must not be added to cars"
        assert _schema_version(conn) == 10

    def test_v10_idempotent(self, patched_connect):
        rm.migrate("motorcycles", dry_run=False)
        rm.migrate("motorcycles", dry_run=False)  # no-op
        conn = sqlite3.connect(patched_connect / "cardata_motorcycles.db")
        conn.row_factory = sqlite3.Row
        assert _schema_version(conn) == 10

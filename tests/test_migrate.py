"""Tests for 2_migrate.py — CSV → DB column round-trip and upsert semantics.

The bug class these pin down: a scraped column silently dropped at migrate
time because the module's column sets lag the schema (exactly how the cars
ad_expiry 0% bug happened — the API and clean steps were fine; 2_migrate's
SHARED_COLUMNS just didn't list the column).
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

# Registered by conftest.py (2_migrate.py is not an importable module name)
from migrate import CATEGORY_COLUMNS, _insert_rows, prepare_dataframe  # noqa: E402


@pytest.fixture
def cars_conn(tmp_path):
    from db import connect

    conn = connect("cars", path=tmp_path / "cars.db", init=True)
    yield conn
    conn.close()


def _schema_columns(conn) -> set:
    return {r["name"] for r in conn.execute("PRAGMA table_info(listings)")}


# ---------------------------------------------------------------------------
# Column-set drift guards
# ---------------------------------------------------------------------------

def _fully_migrated_schema_cols(tmp_path, monkeypatch, category) -> set:
    """Schema columns after schema_*.sql + all migrations (the real prod shape).

    schema_*.sql alone is the v1 base; v2+ columns (old_price, bundle, …) are
    added by migrations/run_migrations.py, so the drift check must compare
    against the fully migrated table.
    """
    import sqlite3

    sys.path.insert(0, str(_ROOT / "migrations"))
    import run_migrations as rm

    def fake_connect(cat, *, path=None, init=True):
        db_file = tmp_path / f"cardata_{cat}.db"
        is_new = not db_file.exists()
        conn = sqlite3.connect(db_file, isolation_level=None)
        conn.row_factory = sqlite3.Row
        if init and is_new:
            schema = (_ROOT / f"schema_{cat}.sql").read_text(encoding="utf-8")
            conn.executescript(schema)
        return conn

    monkeypatch.setattr(rm, "connect", fake_connect)
    rm.migrate(category, dry_run=False)

    conn = sqlite3.connect(tmp_path / f"cardata_{category}.db")
    conn.row_factory = sqlite3.Row
    try:
        return _schema_columns(conn)
    finally:
        conn.close()


class TestColumnSetsMatchSchema:
    """CATEGORY_COLUMNS must stay in sync with the fully migrated schema. If a
    column is in the schema but missing here, every scrape silently loses that
    field at migrate time (the ad_expiry 0% bug)."""

    @pytest.mark.parametrize("category", ["cars", "motorcycles"])
    def test_every_schema_column_is_accepted_by_migrate(
        self, tmp_path, monkeypatch, category
    ):
        schema_cols = _fully_migrated_schema_cols(tmp_path, monkeypatch, category)
        missing = schema_cols - CATEGORY_COLUMNS[category]
        assert not missing, (
            f"[{category}] schema columns not in 2_migrate.CATEGORY_COLUMNS — "
            f"these would be silently dropped on every migrate: {sorted(missing)}"
        )

    @pytest.mark.parametrize("category", ["cars", "motorcycles"])
    def test_migrate_accepts_no_phantom_columns(self, tmp_path, monkeypatch, category):
        schema_cols = _fully_migrated_schema_cols(tmp_path, monkeypatch, category)
        phantom = CATEGORY_COLUMNS[category] - schema_cols
        assert not phantom, (
            f"[{category}] 2_migrate.CATEGORY_COLUMNS lists columns absent from "
            f"the schema — inserts would crash: {sorted(phantom)}"
        )


# ---------------------------------------------------------------------------
# CSV → DB round-trip
# ---------------------------------------------------------------------------

def _scrape_shaped_df() -> pd.DataFrame:
    """A frame shaped like a real EagleSearch cars scrape CSV."""
    return pd.DataFrame([
        {
            "ads_id": 111,
            "url": "https://www.mudah.my/a-111.htm",
            "subject": "2020 Perodua Myvi 1.5 AV",
            "price": 45000,
            "make": "Perodua",
            "model": "Myvi",
            "ad_expiry": "2026-08-01 00:00:00",
            "region": "Selangor",
            "company_ad": 1,
            "not_a_schema_column": "junk",  # must be dropped, not crash
        },
        {
            "ads_id": 222,
            "url": "https://www.mudah.my/a-222.htm",
            "subject": "2018 Proton Saga",
            "price": 25000,
            "make": "Proton",
            "model": "Saga",
            "ad_expiry": "2026-07-15 00:00:00",
            "region": "Johor",
            "company_ad": 0,
            "not_a_schema_column": "junk",
        },
    ])


class TestCsvRoundTrip:
    def test_scrape_columns_survive_to_db(self, cars_conn):
        prepared = prepare_dataframe(_scrape_shaped_df(), "cars")
        _insert_rows(cars_conn, prepared, dry_run=False)

        row = cars_conn.execute(
            "SELECT * FROM listings WHERE ads_id=111"
        ).fetchone()
        assert row["subject"] == "2020 Perodua Myvi 1.5 AV"
        assert row["price"] == 45000
        assert row["make"] == "Perodua"
        # The regression that motivated this file:
        assert row["ad_expiry"] == "2026-08-01 00:00:00"

    def test_unknown_columns_dropped_not_fatal(self, cars_conn):
        prepared = prepare_dataframe(_scrape_shaped_df(), "cars")
        assert "not_a_schema_column" not in prepared.columns
        attempted, inserted = _insert_rows(cars_conn, prepared, dry_run=False)
        assert (attempted, inserted) == (2, 2)

    def test_missing_ads_id_raises(self):
        df = _scrape_shaped_df().drop(columns=["ads_id"])
        with pytest.raises(ValueError, match="ads_id"):
            prepare_dataframe(df, "cars")

    def test_non_numeric_ads_id_rows_dropped(self, cars_conn):
        df = _scrape_shaped_df()
        df["ads_id"] = df["ads_id"].astype(object)
        df.loc[0, "ads_id"] = "garbage"
        prepared = prepare_dataframe(df, "cars")
        assert list(prepared["ads_id"]) == [222]

    def test_first_seen_and_availability_defaults_stamped(self, cars_conn):
        prepared = prepare_dataframe(_scrape_shaped_df(), "cars")
        _insert_rows(cars_conn, prepared, dry_run=False)
        row = cars_conn.execute(
            "SELECT first_seen_at, last_seen_at, availability_status "
            "FROM listings WHERE ads_id=111"
        ).fetchone()
        assert row["first_seen_at"]
        assert row["last_seen_at"]
        assert row["availability_status"] == "unknown"

    def test_dry_run_writes_nothing(self, cars_conn):
        prepared = prepare_dataframe(_scrape_shaped_df(), "cars")
        attempted, inserted = _insert_rows(cars_conn, prepared, dry_run=True)
        assert attempted == 2 and inserted == 0
        assert cars_conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Upsert semantics
# ---------------------------------------------------------------------------

class TestUpsertSemantics:
    def test_upsert_refreshes_but_preserves_first_seen_at(self, cars_conn):
        first = prepare_dataframe(_scrape_shaped_df(), "cars")
        first["first_seen_at"] = "2026-01-01 00:00:00"
        _insert_rows(cars_conn, first, dry_run=False, mode="upsert")

        rescrape = _scrape_shaped_df()
        rescrape.loc[rescrape["ads_id"] == 111, "price"] = 42000  # price drop
        second = prepare_dataframe(rescrape, "cars")
        second["first_seen_at"] = "2026-06-10 00:00:00"
        attempted, newly = _insert_rows(cars_conn, second, dry_run=False, mode="upsert")

        assert (attempted, newly) == (2, 0)  # no new rows, refreshed in place
        row = cars_conn.execute(
            "SELECT price, first_seen_at FROM listings WHERE ads_id=111"
        ).fetchone()
        assert row["price"] == 42000                      # refreshed
        assert row["first_seen_at"] == "2026-01-01 00:00:00"  # immutable

    def test_ignore_mode_never_overwrites(self, cars_conn):
        first = prepare_dataframe(_scrape_shaped_df(), "cars")
        _insert_rows(cars_conn, first, dry_run=False, mode="ignore")

        rescrape = _scrape_shaped_df()
        rescrape.loc[rescrape["ads_id"] == 111, "price"] = 1
        second = prepare_dataframe(rescrape, "cars")
        attempted, newly = _insert_rows(cars_conn, second, dry_run=False, mode="ignore")

        assert (attempted, newly) == (2, 0)
        row = cars_conn.execute(
            "SELECT price FROM listings WHERE ads_id=111"
        ).fetchone()
        assert row["price"] == 45000  # untouched

    def test_unknown_mode_raises(self, cars_conn):
        prepared = prepare_dataframe(_scrape_shaped_df(), "cars")
        with pytest.raises(ValueError, match="mode"):
            _insert_rows(cars_conn, prepared, dry_run=False, mode="merge")

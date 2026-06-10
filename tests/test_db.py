"""Tests for db.connect creation semantics.

connect() must not silently create an empty DB on a read path: a missing
file raises FileNotFoundError unless init=True (the migrate entry points).
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from db import connect, schema_version  # noqa: E402


class TestConnectCreationSemantics:
    def test_missing_db_raises_by_default(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            connect("cars", path=tmp_path / "nope.db")

    def test_init_true_creates_and_applies_schema(self, tmp_path):
        db_file = tmp_path / "fresh.db"
        conn = connect("cars", path=db_file, init=True)
        try:
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(listings)")}
            assert "ads_id" in cols
            # meta table exists too (schema_version readable, 0 or seeded)
            assert schema_version(conn) >= 0
        finally:
            conn.close()
        assert db_file.exists()

    def test_existing_db_connects_without_init(self, tmp_path):
        db_file = tmp_path / "existing.db"
        connect("cars", path=db_file, init=True).close()

        conn = connect("cars", path=db_file)
        try:
            assert conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0] == 0
        finally:
            conn.close()

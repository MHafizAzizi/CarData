"""Tests for src/load_manual_moto_specs.py — manual moto spec loader."""

import json
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

import load_manual_moto_specs as lm  # noqa: E402


def _make_specs_db(tmp_path):
    conn = sqlite3.connect(tmp_path / "specs.db", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE model_specs (make TEXT, model TEXT, source TEXT, "
        "source_url TEXT, engine_cc REAL, power_hp REAL, torque_nm REAL, "
        "transmission TEXT)"
    )
    return conn


def test_load_manual_reads_specs(tmp_path):
    p = tmp_path / "m.json"
    p.write_text(json.dumps({"specs": [{"make": "voge", "model": "sr3", "engine_cc": 244}]}),
                 encoding="utf-8")
    rows = lm.load_manual(p)
    assert rows == [{"make": "voge", "model": "sr3", "engine_cc": 244}]


def test_upsert_inserts_only_present_fields(tmp_path):
    conn = _make_specs_db(tmp_path)
    lm.upsert(conn, {"make": "voge", "model": "formica-rossa-150",
                     "engine_cc": 149.6, "power_hp": 15.4,
                     "source_url": "http://x"})
    r = conn.execute("SELECT * FROM model_specs").fetchone()
    assert r["engine_cc"] == 149.6
    assert r["power_hp"] == 15.4
    assert r["source"] == "manual"
    assert r["torque_nm"] is None  # absent field stays NULL


def test_upsert_is_idempotent(tmp_path):
    conn = _make_specs_db(tmp_path)
    row = {"make": "voge", "model": "sr3", "engine_cc": 244, "source_url": "http://x"}
    lm.upsert(conn, row)
    row["engine_cc"] = 250  # corrected value
    lm.upsert(conn, row)
    rows = conn.execute("SELECT engine_cc FROM model_specs WHERE model='sr3'").fetchall()
    assert len(rows) == 1            # replaced, not duplicated
    assert rows[0]["engine_cc"] == 250


def test_repo_manual_json_is_valid():
    """The shipped curated file parses and every row has make+model+source_url."""
    rows = lm.load_manual()
    assert rows, "manual specs file should not be empty"
    for r in rows:
        assert r.get("make") and r.get("model"), f"row missing identity: {r}"
        assert r.get("source_url"), f"row missing source_url: {r}"

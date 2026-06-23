"""Tests for src/enrich_cars_specs.py — deterministic carbase car enricher."""

import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

import enrich_cars_specs as ec  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# carbase model_specs columns the enricher reads.
_SPEC_COLS = (
    "make", "model", "year", "engine_cc", "power_hp", "torque_nm",
    "kerb_weight_kg", "seats", "body_type", "engine_tech",
    "length_mm", "width_mm", "height_mm", "wheelbase_mm",
)
_TEXT = {"make", "model", "body_type", "engine_tech"}

# Honda City: one engine per cc -> confident. Mercedes C-Class cc=1991: many
# tunes (184..258) -> ambiguous power, dims still fillable.
_SPEC_ROWS = [
    ("honda", "city", 2016, 1497, 120, 145, 1100, 5, "Sedan", "i-VTEC",
     4440, 1695, 1477, 2600),
    ("honda", "city", 2020, 1498, 121, 145, 1153, 5, "Sedan", "i-VTEC",
     4553, 1748, 1467, 2600),
    ("toyota", "corolla-altis", 2018, 1798, 140, 173, 1310, 5, "Sedan",
     "Dual VVT-i", 4620, 1775, 1455, 2700),
    ("mercedes-benz", "c-class", 2015, 1991, 184, 300, 1480, 5, "Sedan", "Turbo",
     4686, 1810, 1442, 2840),
    ("mercedes-benz", "c-class", 2016, 1991, 245, 370, 1525, 5, "Sedan", "Turbo",
     4686, 1810, 1442, 2840),
]


def _make_specs_db(tmp_path):
    conn = sqlite3.connect(tmp_path / "specs.db", isolation_level=None)
    conn.row_factory = sqlite3.Row
    coldefs = ", ".join(
        f"{c} {'TEXT' if c in _TEXT else 'INTEGER'}" for c in _SPEC_COLS)
    conn.execute(f"CREATE TABLE model_specs ({coldefs})")
    conn.executemany(
        f"INSERT INTO model_specs ({', '.join(_SPEC_COLS)}) "
        f"VALUES ({', '.join('?' * len(_SPEC_COLS))})",
        _SPEC_ROWS,
    )
    return conn


_LISTING_SPEC_COLS = [
    "engine_cc", "length_mm", "width_mm", "height_mm", "wheelbase_mm",
    "kerb_weight_kg", "seat_capacity", "body_style", "engine_type",
    "peak_power_kw", "peak_torque_nm", "spec_match", "spec_source",
]


def _make_cars_db(tmp_path, rows):
    """rows: (ads_id, make, model, year, cc[, engine_cc_prefill])."""
    conn = sqlite3.connect(tmp_path / "cars.db", isolation_level=None)
    conn.row_factory = sqlite3.Row
    cols = ("ads_id INTEGER PRIMARY KEY, make TEXT, model TEXT, "
            "manufactured_date INTEGER, engine_capacity INTEGER, "
            + ", ".join(f"{c} {'TEXT' if c in ('body_style','engine_type','spec_match','spec_source') else 'INTEGER'}"
                        for c in _LISTING_SPEC_COLS))
    conn.execute(f"CREATE TABLE listings ({cols})")
    for r in rows:
        ads_id, make, model, year, cc = r[:5]
        prefill = r[5] if len(r) > 5 else None
        conn.execute(
            "INSERT INTO listings (ads_id, make, model, manufactured_date, "
            "engine_capacity, engine_cc) VALUES (?,?,?,?,?,?)",
            (ads_id, make, model, year, cc, prefill),
        )
    return conn


@pytest.fixture
def aliases():
    return {}, {"Toyota|Altis": "corolla-altis"}


@pytest.fixture
def index(tmp_path):
    return ec.build_index(_make_specs_db(tmp_path))


# ---------------------------------------------------------------------------
# norm
# ---------------------------------------------------------------------------

class TestNorm:
    def test_strip_lower(self):
        assert ec.norm("C-Class") == "cclass"
        assert ec.norm("3 Series") == "3series"

    def test_none(self):
        assert ec.norm(None) == ""

    def test_make_hyphen(self):
        assert ec.norm_make("Mercedes Benz", {}) == "mercedes-benz"

    def test_make_alias(self):
        assert ec.norm_make("Merc", {"merc": "mercedes-benz"}) == "mercedes-benz"


# ---------------------------------------------------------------------------
# match
# ---------------------------------------------------------------------------

class TestMatch:
    def test_cc_full_confident(self, index, aliases):
        # Honda City cc 1500 -> matches 1497/1498 (one power tune) -> cc_full
        tier, row, conf = ec.match("Honda", "City", 1500, 2020, index, *aliases)
        assert tier == "cc_full"
        assert conf is True
        assert row["year"] == 2020  # year-nearest pick

    def test_cc_dims_ambiguous(self, index, aliases):
        # Merc C-Class cc 1991 -> two tunes 184/245 (>10% spread) -> cc_dims
        tier, row, conf = ec.match("Mercedes Benz", "C-Class", 1991, 2016,
                                   index, *aliases)
        assert tier == "cc_dims"
        assert conf is False

    def test_null_no_cc_candidate(self, index, aliases):
        tier, row, _ = ec.match("Honda", "City", 3000, 2020, index, *aliases)
        assert tier == "null"
        assert row is None

    def test_null_cc_none(self, index, aliases):
        tier, row, _ = ec.match("Honda", "City", None, 2020, index, *aliases)
        assert tier == "null"

    def test_no_model(self, index, aliases):
        tier, row, _ = ec.match("Honda", "Zxyzzy", 1500, 2020, index, *aliases)
        assert tier == "no_model"

    def test_no_make_catalog(self, index, aliases):
        tier, row, _ = ec.match("Ferrari", "F40", 2900, 1990, index, *aliases)
        assert tier == "no_make_catalog"

    def test_model_alias(self, index, aliases):
        # "Altis" -> carbase model "altis" via alias
        tier, row, _ = ec.match("Toyota", "Altis", 1800, 2018, index, *aliases)
        assert tier == "cc_full"
        assert row["model"] == "corolla-altis"


# ---------------------------------------------------------------------------
# enrich
# ---------------------------------------------------------------------------

class TestEnrich:
    def test_cc_full_writes_power_and_dims(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        cars = _make_cars_db(tmp_path, [(1, "Honda", "City", 2020, 1498)])
        ec.enrich(cars, specs, *aliases)
        row = cars.execute("SELECT * FROM listings WHERE ads_id=1").fetchone()
        assert row["engine_cc"] == 1498          # listing's own cc
        assert row["length_mm"] == 4553          # year-nearest dims
        assert row["peak_power_kw"] == round(121 * ec.HP_TO_KW)
        assert row["peak_torque_nm"] == 145
        assert row["spec_match"] == "cc_full"
        assert row["spec_source"] == "carbase"

    def test_cc_dims_fills_dims_not_power(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        cars = _make_cars_db(tmp_path, [(1, "Mercedes Benz", "C-Class", 2016, 1991)])
        ec.enrich(cars, specs, *aliases)
        row = cars.execute("SELECT * FROM listings WHERE ads_id=1").fetchone()
        assert row["length_mm"] == 4686
        assert row["kerb_weight_kg"] is not None
        assert row["peak_power_kw"] is None      # ambiguous -> NULL
        assert row["peak_torque_nm"] is None
        assert row["spec_match"] == "cc_dims"

    def test_null_records_tier_no_specs(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        cars = _make_cars_db(tmp_path, [(1, "Honda", "City", 2020, 3000)])
        ec.enrich(cars, specs, *aliases)
        row = cars.execute("SELECT * FROM listings WHERE ads_id=1").fetchone()
        assert row["engine_cc"] is None
        assert row["spec_match"] == "null"
        assert row["spec_source"] is None

    def test_skips_mcdparams_era(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        # ads_id >= 115M must never be touched
        cars = _make_cars_db(tmp_path, [(115_000_001, "Honda", "City", 2020, 1498)])
        ec.enrich(cars, specs, *aliases)
        row = cars.execute("SELECT spec_match FROM listings WHERE ads_id=115000001").fetchone()
        assert row["spec_match"] is None

    def test_skips_already_filled_engine_cc(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        # engine_cc prefilled (mcdParams straggler in old era) -> skip
        cars = _make_cars_db(tmp_path, [(1, "Honda", "City", 2020, 1498, 1498)])
        ec.enrich(cars, specs, *aliases)
        row = cars.execute("SELECT spec_match FROM listings WHERE ads_id=1").fetchone()
        assert row["spec_match"] is None

    def test_idempotent_skips_processed(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        cars = _make_cars_db(tmp_path, [(1, "Honda", "City", 2020, 1498)])
        ec.enrich(cars, specs, *aliases)
        specs.execute("UPDATE model_specs SET power_hp=999 WHERE year=2020 AND model='city'")
        ec.enrich(cars, specs, *aliases)
        row = cars.execute("SELECT peak_power_kw FROM listings WHERE ads_id=1").fetchone()
        assert row["peak_power_kw"] == round(121 * ec.HP_TO_KW)  # unchanged

    def test_force_reprocesses(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        cars = _make_cars_db(tmp_path, [(1, "Honda", "City", 2020, 1498)])
        ec.enrich(cars, specs, *aliases)
        # engine_cc now set -> force must still re-touch (force drops spec_match
        # guard; engine_cc IS NULL guard would block, so force also bypasses it)
        specs.execute("UPDATE model_specs SET length_mm=9999 WHERE year=2020 AND model='city'")
        ec.enrich(cars, specs, *aliases, force=True)
        row = cars.execute("SELECT length_mm FROM listings WHERE ads_id=1").fetchone()
        assert row["length_mm"] == 9999

    def test_dry_run_writes_nothing(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        cars = _make_cars_db(tmp_path, [(1, "Honda", "City", 2020, 1498)])
        stats = ec.enrich(cars, specs, *aliases, dry_run=True)
        row = cars.execute("SELECT spec_match FROM listings WHERE ads_id=1").fetchone()
        assert row["spec_match"] is None
        assert stats["cc_full"] == 1

    def test_tier_counts(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        cars = _make_cars_db(tmp_path, [
            (1, "Honda", "City", 2020, 1498),            # cc_full
            (2, "Mercedes Benz", "C-Class", 2016, 1991), # cc_dims
            (3, "Honda", "City", 2020, 3000),            # null
            (4, "Honda", "Zxyzzy", 2020, 1498),          # no_model
            (5, "Ferrari", "F40", 1990, 2900),           # no_make_catalog
        ])
        stats = ec.enrich(cars, specs, *aliases)
        assert stats["cc_full"] == 1
        assert stats["cc_dims"] == 1
        assert stats["null"] == 1
        assert stats["no_model"] == 1
        assert stats["no_make_catalog"] == 1


# ---------------------------------------------------------------------------
# load_aliases
# ---------------------------------------------------------------------------

class TestLoadAliases:
    def test_parses_and_excludes_null(self, tmp_path):
        import json
        p = tmp_path / "al.json"
        p.write_text(json.dumps({
            "_make_alias": {"merc": "mercedes-benz"},
            "aliases": {
                "Toyota|Altis": {"decision": "corolla-altis"},
                "Proton|Wira": {"decision": "NULL"},
            },
        }), encoding="utf-8")
        make_alias, model_alias = ec.load_aliases(p)
        assert make_alias == {"merc": "mercedes-benz"}
        assert model_alias == {"Toyota|Altis": "corolla-altis"}

    def test_missing_file_returns_empty(self, tmp_path):
        make_alias, model_alias = ec.load_aliases(tmp_path / "nope.json")
        assert make_alias == {}
        assert model_alias == {}

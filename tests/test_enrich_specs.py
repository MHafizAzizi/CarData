"""Tests for src/enrich_specs.py — deterministic moto spec matcher/enricher."""

import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

import enrich_specs as es  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures: tiny in-memory specs + listings DBs
# ---------------------------------------------------------------------------

# (make, model, engine_cc, power_hp, torque_nm, kerb_weight_kg, fuel_tank_l,
#  engine_type, transmission, fuel_type, cooling, seat_height_mm,
#  wheelbase_mm, compression_ratio)
_SPEC_ROWS = [
    ("yamaha", "nvx", 155, 15, 13.9, 122, 5.5, "4-Stroke", "CVT", "Petrol",
     "Liquid Cooled", 790, 1350, "11.6:1"),
    ("yamaha", "135lc-fi", 135, 12, 12.1, 115, 4.1, "4-Stroke", "Manual",
     "Petrol", "Liquid Cooled", 770, 1245, "10.9:1"),
    ("yamaha", "y15zr", 150, 14, 13.8, 118, 4.2, "4-Stroke", "Manual",
     "Petrol", "Liquid Cooled", 780, 1280, "11.0:1"),
    # two models sharing a prefix → ambiguity test ("mt" -> mt-15 / mt-25)
    ("yamaha", "mt-15", 155, 18, 14.1, 138, 10, "4-Stroke", "Manual",
     "Petrol", "Liquid Cooled", 810, 1335, "11.6:1"),
    ("yamaha", "mt-25", 250, 35, 23.6, 165, 14, "4-Stroke", "Manual",
     "Petrol", "Liquid Cooled", 780, 1380, "11.5:1"),
    ("suzuki", "raider-150-fi", 150, 18, 13.8, 119, 4.0, "4-Stroke", "Manual",
     "Petrol", "Liquid Cooled", 765, 1280, "11.5:1"),
]

_SPEC_COLS = (
    "make", "model", "engine_cc", "power_hp", "torque_nm", "kerb_weight_kg",
    "fuel_tank_l", "engine_type", "transmission", "fuel_type", "cooling",
    "seat_height_mm", "wheelbase_mm", "compression_ratio",
)


def _make_specs_db(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "specs.db", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE model_specs ("
        + ", ".join(f"{c} {'TEXT' if c in ('make','model','engine_type','transmission','fuel_type','cooling','compression_ratio') else 'REAL'}" for c in _SPEC_COLS)
        + ")"
    )
    conn.executemany(
        f"INSERT INTO model_specs ({', '.join(_SPEC_COLS)}) "
        f"VALUES ({', '.join('?' * len(_SPEC_COLS))})",
        _SPEC_ROWS,
    )
    return conn


_SPEC_TARGET_COLS = [
    "engine_cc", "power_hp", "torque_nm", "kerb_weight_kg", "fuel_tank_l",
    "engine_type", "transmission", "fuel_type", "cooling", "seat_height_mm",
    "wheelbase_mm", "comp_ratio", "spec_match", "spec_source",
]


def _make_moto_db(tmp_path: Path, rows) -> sqlite3.Connection:
    """rows: list of (ads_id, make, model[, engine_cc_prefill])."""
    conn = sqlite3.connect(tmp_path / "moto.db", isolation_level=None)
    conn.row_factory = sqlite3.Row
    cols = "ads_id INTEGER PRIMARY KEY, motorcycle_make TEXT, motorcycle_model TEXT, " \
           + ", ".join(f"{c} {'REAL' if c in ('engine_cc','power_hp','torque_nm','kerb_weight_kg','fuel_tank_l') else ('INTEGER' if c in ('seat_height_mm','wheelbase_mm') else 'TEXT')}" for c in _SPEC_TARGET_COLS)
    conn.execute(f"CREATE TABLE listings ({cols})")
    for r in rows:
        ads_id, make, model = r[0], r[1], r[2]
        prefill = r[3] if len(r) > 3 else None
        conn.execute(
            "INSERT INTO listings (ads_id, motorcycle_make, motorcycle_model, engine_cc) "
            "VALUES (?, ?, ?, ?)",
            (ads_id, make, model, prefill),
        )
    return conn


@pytest.fixture
def aliases():
    make_alias = {"mbp morbidelli": "morbidelli", "sm sport": "sm-sport"}
    model_alias = {"Yamaha|R25": "mt-25", "Suzuki|Raider R150 Fi": "raider-150-fi"}
    return make_alias, model_alias


# ---------------------------------------------------------------------------
# norm / norm_make
# ---------------------------------------------------------------------------

class TestNorm:
    def test_strips_and_lowercases(self):
        assert es.norm("135Lc") == "135lc"
        assert es.norm("Y15ZR") == "y15zr"
        assert es.norm("Ego Avantiz") == "egoavantiz"

    def test_none_and_empty(self):
        assert es.norm(None) == ""
        assert es.norm("") == ""

    def test_make_keeps_hyphen(self):
        assert es.norm_make("Qj Motor", {}) == "qj-motor"
        assert es.norm_make("Honda", {}) == "honda"

    def test_make_alias_applied(self):
        alias = {"mbp morbidelli": "morbidelli"}
        assert es.norm_make("Mbp Morbidelli", alias) == "morbidelli"


# ---------------------------------------------------------------------------
# match tiers
# ---------------------------------------------------------------------------

class TestMatch:
    @pytest.fixture
    def index(self, tmp_path):
        return es.build_index(_make_specs_db(tmp_path))

    def test_exact(self, index, aliases):
        tier, row = es.match("Yamaha", "Nvx", index, *aliases)
        assert tier == "exact"
        assert row["engine_cc"] == 155

    def test_prefix_unambiguous(self, index, aliases):
        # "135Lc" -> norm "135lc"; spec "135lc-fi" -> "135lcfi" startswith "135lc"
        tier, row = es.match("Yamaha", "135Lc", index, *aliases)
        assert tier == "prefix"
        assert row["model"] == "135lc-fi"

    def test_prefix_ambiguous_returns_null(self, index, aliases):
        # "Mt" normalizes to "mt"; both mt-15 and mt-25 startswith "mt" -> ambiguous
        tier, row = es.match("Yamaha", "Mt", index, *aliases)
        assert tier == "null"
        assert row is None

    def test_alias(self, index, aliases):
        # "R25" has no exact/prefix; alias maps Yamaha|R25 -> mt-25
        tier, row = es.match("Yamaha", "R25", index, *aliases)
        assert tier == "alias"
        assert row["model"] == "mt-25"

    def test_no_make_catalog(self, index, aliases):
        tier, row = es.match("Ducati", "Panigale", index, *aliases)
        assert tier == "no_make_catalog"
        assert row is None

    def test_null_when_no_match(self, index, aliases):
        tier, row = es.match("Yamaha", "Zxyzzy", index, *aliases)
        assert tier == "null"
        assert row is None


# ---------------------------------------------------------------------------
# enrich
# ---------------------------------------------------------------------------

class TestEnrich:
    def test_writes_specs_and_provenance(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        moto = _make_moto_db(tmp_path, [(1, "Yamaha", "Nvx")])
        es.enrich(moto, specs, *aliases)
        row = moto.execute(
            "SELECT engine_cc, power_hp, transmission, spec_match, spec_source "
            "FROM listings WHERE ads_id=1"
        ).fetchone()
        assert row["engine_cc"] == 155
        assert row["power_hp"] == 15
        assert row["transmission"] == "CVT"
        assert row["spec_match"] == "exact"
        assert row["spec_source"] == "zigwheels"

    def test_spec_source_from_row_source_column(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        specs.execute("ALTER TABLE model_specs ADD COLUMN source TEXT")
        specs.execute("UPDATE model_specs SET source='motomalaysia' WHERE model='nvx'")
        moto = _make_moto_db(tmp_path, [(1, "Yamaha", "Nvx")])
        es.enrich(moto, specs, *aliases)
        row = moto.execute("SELECT spec_source FROM listings WHERE ads_id=1").fetchone()
        assert row["spec_source"] == "motomalaysia"

    def test_maps_compression_ratio_to_comp_ratio(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        moto = _make_moto_db(tmp_path, [(1, "Yamaha", "Nvx")])
        es.enrich(moto, specs, *aliases)
        row = moto.execute("SELECT comp_ratio FROM listings WHERE ads_id=1").fetchone()
        assert row["comp_ratio"] == "11.6:1"

    def test_null_match_leaves_specs_null_records_tier(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        moto = _make_moto_db(tmp_path, [(1, "Yamaha", "Zxyzzy")])
        es.enrich(moto, specs, *aliases)
        row = moto.execute(
            "SELECT engine_cc, spec_match, spec_source FROM listings WHERE ads_id=1"
        ).fetchone()
        assert row["engine_cc"] is None
        assert row["spec_match"] == "null"
        assert row["spec_source"] is None

    def test_idempotent_skips_processed(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        moto = _make_moto_db(tmp_path, [(1, "Yamaha", "Nvx")])
        es.enrich(moto, specs, *aliases)
        # Corrupt the spec source row; a second (non-force) run must NOT re-touch
        # the already-processed listing.
        specs.execute("UPDATE model_specs SET engine_cc=999 WHERE model='nvx'")
        es.enrich(moto, specs, *aliases)
        row = moto.execute("SELECT engine_cc FROM listings WHERE ads_id=1").fetchone()
        assert row["engine_cc"] == 155  # unchanged, not 999

    def test_force_reprocesses(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        moto = _make_moto_db(tmp_path, [(1, "Yamaha", "Nvx")])
        es.enrich(moto, specs, *aliases)
        specs.execute("UPDATE model_specs SET engine_cc=999 WHERE model='nvx'")
        es.enrich(moto, specs, *aliases, force=True)
        row = moto.execute("SELECT engine_cc FROM listings WHERE ads_id=1").fetchone()
        assert row["engine_cc"] == 999

    def test_returns_tier_counts(self, tmp_path, aliases):
        specs = _make_specs_db(tmp_path)
        moto = _make_moto_db(tmp_path, [
            (1, "Yamaha", "Nvx"),       # exact
            (2, "Yamaha", "135Lc"),     # prefix
            (3, "Yamaha", "R25"),       # alias
            (4, "Ducati", "Panigale"),  # no_make_catalog
            (5, "Yamaha", "Zxyzzy"),    # null
        ])
        stats = es.enrich(moto, specs, *aliases)
        assert stats["exact"] == 1
        assert stats["prefix"] == 1
        assert stats["alias"] == 1
        assert stats["no_make_catalog"] == 1
        assert stats["null"] == 1


# ---------------------------------------------------------------------------
# load_aliases
# ---------------------------------------------------------------------------

class TestLoadAliases:
    def test_parses_make_and_model_maps(self, tmp_path):
        import json
        p = tmp_path / "al.json"
        p.write_text(json.dumps({
            "_make_alias": {"sm sport": "sm-sport"},
            "aliases": {
                "Yamaha|R25": {"decision": "mt-25"},
                "Honda|Ex5": {"decision": "NULL"},
            },
        }), encoding="utf-8")
        make_alias, model_alias = es.load_aliases(p)
        assert make_alias == {"sm sport": "sm-sport"}
        assert model_alias == {"Yamaha|R25": "mt-25"}  # NULL excluded

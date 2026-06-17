"""Tests for the carbase.my spec scraper (Phase 1).

Pure-function coverage: numeric normalizers, link/url parsers, the variant-page
parser (against a saved HTML fixture), and model_specs DB upsert idempotency.
The live-network CarbaseClient is not unit-tested (same policy as eagle/mudah
clients).
"""

import json
import sqlite3
from pathlib import Path

import pytest

from scrape_carbase_specs import (
    _to_int, _to_float,
    extract_model_links, extract_variant_links, parse_url_parts,
    parse_variant_page, init_specs_db, upsert_spec,
)

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "carbase_variant_wrv.html"


class TestNumericNormalizers:
    @pytest.mark.parametrize("raw,expected", [
        ("1,498 cc", 1498),
        ("240 Nm", 240),
        ("1,108 kg", 1108),
        ("RM 89,900.00", 89900),
        ("200 km/h", 200),
        ("5", 5),
        ("-", None),
        ("", None),
        (None, None),
        ("N/A", None),
    ])
    def test_to_int(self, raw, expected):
        assert _to_int(raw) == expected

    @pytest.mark.parametrize("raw,expected", [
        ("11.0 seconds", 11.0),
        ("6.0 L/100 km", 6.0),
        ("8.3", 8.3),
        ("-", None),
        ("", None),
        (None, None),
    ])
    def test_to_float(self, raw, expected):
        assert _to_float(raw) == expected


class TestModelLinks:
    HTML = """
    <a href="/honda/civic">Civic</a>
    <a href="/honda/cr-v">CR-V</a>
    <a href="/honda/civic?x=1#frag">Civic dup</a>
    <a href="/honda">brand self</a>
    <a href="/honda/civic/fe-facelift/1.5-rs-turbo-2025">variant too deep</a>
    <a href="/toyota/vios">other brand</a>
    """

    def test_extracts_model_paths(self):
        links = extract_model_links(self.HTML, "honda")
        assert links == ["/honda/civic", "/honda/cr-v"]

    def test_excludes_brand_self_and_deeper(self):
        links = extract_model_links(self.HTML, "honda")
        assert "/honda" not in links
        assert "/honda/civic/fe-facelift/1.5-rs-turbo-2025" not in links

    def test_excludes_other_brands(self):
        assert "/toyota/vios" not in extract_model_links(self.HTML, "honda")


class TestVariantLinks:
    HTML = """
    <a href="/honda/wr-v/dg4/s-2023">S</a>
    <a href="/honda/wr-v/dg4/rs-2023">RS</a>
    <a href="/honda/wr-v/dg4/user-review">noise</a>
    <a href="/honda/wr-v/dg4/exterior">noise</a>
    <a href="/honda/wr-v/dg4/generations">noise</a>
    <a href="/honda/wr-v/dg4/owner-reviews">noise</a>
    <a href="/honda/wr-v/dg4/rs-2023?ref=x">dup</a>
    <a href="/honda/wr-v">model</a>
    """

    def test_keeps_year_suffixed_variants(self):
        links = extract_variant_links(self.HTML, "honda")
        assert "/honda/wr-v/dg4/s-2023" in links
        assert "/honda/wr-v/dg4/rs-2023" in links

    def test_drops_noise_pages(self):
        links = extract_variant_links(self.HTML, "honda")
        for noise in ("user-review", "exterior", "generations", "owner-reviews"):
            assert not any(noise in l for l in links)

    def test_dedupes_on_query_string(self):
        links = extract_variant_links(self.HTML, "honda")
        assert links.count("/honda/wr-v/dg4/rs-2023") == 1


class TestParseUrlParts:
    def test_full_variant_url(self):
        parts = parse_url_parts("/honda/civic/fe-facelift/1.5-rs-turbo-2025")
        assert parts == {
            "make": "honda", "model": "civic", "generation": "fe-facelift",
            "variant": "1.5-rs-turbo", "year": 2025,
        }

    def test_absolute_url(self):
        parts = parse_url_parts("https://www.carbase.my/honda/wr-v/dg4/s-2023")
        assert parts["make"] == "honda"
        assert parts["model"] == "wr-v"
        assert parts["variant"] == "s"
        assert parts["year"] == 2023

    def test_multi_token_variant_keeps_all_but_year(self):
        parts = parse_url_parts("/perodua/myvi/m900/1.5-av-2022")
        assert parts["variant"] == "1.5-av"
        assert parts["year"] == 2022


class TestParseVariantPage:
    """Against the saved Honda WR-V S 2023 fixture."""

    URL = "https://www.carbase.my/honda/wr-v/dg4/s-2023"

    @pytest.fixture(scope="class")
    def row(self):
        html = _FIXTURE.read_text(encoding="utf-8")
        return parse_variant_page(html, self.URL)

    def test_identity_from_url(self, row):
        assert row["make"] == "honda"
        assert row["model"] == "wr-v"
        assert row["generation"] == "dg4"
        assert row["variant"] == "s"
        assert row["year"] == 2023
        assert row["source_url"] == self.URL

    def test_engine_and_power(self, row):
        assert row["engine_cc"] == 1498
        assert row["power_hp"] == 121          # labelled "Horsepower"
        assert row["torque_nm"] == 145
        assert row["engine_tech"].startswith("16-valve DOHC")
        assert row["fuel_type"] == "Petrol"

    def test_drivetrain_and_performance(self, row):
        assert row["transmission"] == "CVT Automatic"   # Drivetrain.Type
        assert row["driveline"] == "Front Wheel Drive"
        assert row["accel_0_100"] == 11.0
        assert row["top_speed_kmh"] == 160
        assert row["fuel_economy_l100"] == 6.0          # "Rated Economy"

    def test_dimensions(self, row):
        assert row["length_mm"] == 4060
        assert row["width_mm"] == 1780
        assert row["height_mm"] == 1576
        assert row["wheelbase_mm"] == 2485
        assert row["kerb_weight_kg"] == 1108
        assert row["boot_l"] == 380
        assert row["seats"] == 5
        assert row["doors"] == 5

    def test_body_type_from_ldjson(self, row):
        assert row["body_type"] == "SUV"

    def test_price_peninsular(self, row):
        assert row["price_peninsular"] == 89900

    def test_raw_specs_is_superset_json(self, row):
        raw = json.loads(row["raw_specs"])
        # ambiguous cross-section labels preserved (Drivetrain.Type vs Chassis.Type)
        assert any("Horsepower" in str(v) for v in raw.values())
        assert row["scraped_at"]


class TestSpecsDb:
    def _row(self, url, power):
        return {
            "make": "honda", "model": "wr-v", "generation": "dg4",
            "variant": "s", "year": 2023, "source_url": url,
            "scraped_at": "2026-06-17T00:00:00+00:00", "body_type": "SUV",
            "engine_cc": 1498, "power_hp": power, "torque_nm": 145,
            "raw_specs": "{}",
        }

    def test_init_creates_model_specs_table(self, tmp_path):
        conn = init_specs_db(tmp_path / "specs.db")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(model_specs)")}
        assert {"make", "model", "variant", "year", "power_hp",
                "raw_specs", "source_url", "scraped_at"} <= cols

    def test_upsert_inserts_row(self, tmp_path):
        conn = init_specs_db(tmp_path / "specs.db")
        upsert_spec(conn, self._row("/honda/wr-v/dg4/s-2023", 121))
        n = conn.execute("SELECT COUNT(*) FROM model_specs").fetchone()[0]
        assert n == 1

    def test_upsert_is_idempotent_on_source_url(self, tmp_path):
        conn = init_specs_db(tmp_path / "specs.db")
        upsert_spec(conn, self._row("/honda/wr-v/dg4/s-2023", 121))
        upsert_spec(conn, self._row("/honda/wr-v/dg4/s-2023", 999))  # same url, new value
        rows = conn.execute("SELECT power_hp FROM model_specs").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 999  # second write refreshed the field

    def test_distinct_urls_are_separate_rows(self, tmp_path):
        conn = init_specs_db(tmp_path / "specs.db")
        upsert_spec(conn, self._row("/honda/wr-v/dg4/s-2023", 121))
        upsert_spec(conn, self._row("/honda/wr-v/dg4/rs-2023", 121))
        n = conn.execute("SELECT COUNT(*) FROM model_specs").fetchone()[0]
        assert n == 2

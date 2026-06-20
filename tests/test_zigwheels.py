"""Tests for the zigwheels.my motorcycle spec scraper.

Parser coverage against a saved spec-page fixture, URL identity extraction,
sitemap URL discovery (English-only dedupe), and model_specs upsert idempotency.
The live-network client is not unit-tested (same policy as the carbase/mudah
clients).
"""

import sqlite3
from pathlib import Path

from scrape_zigwheels_specs import (
    _identity_from_url, discover_spec_urls, parse_spec_page,
    init_specs_db, upsert_spec,
)

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "zigwheels_spec_nvx.html"
_URL = "https://www.zigwheels.my/new-motorcycles/yamaha/nvx/specifications"


def test_identity_from_url():
    assert _identity_from_url(_URL) == ("yamaha", "nvx")


def test_parse_spec_page():
    row = parse_spec_page(_FIXTURE.read_text(encoding="utf-8"), _URL)
    # identity
    assert row["make"] == "yamaha"
    assert row["model"] == "nvx"
    assert row["year"] == 2026          # from <title> "Yamaha NVX 2026 ..."
    # curated numerics (see fixture li rows)
    assert row["engine_cc"] == 155
    assert row["power_hp"] == 15
    assert row["torque_nm"] == 13.9
    assert row["kerb_weight_kg"] == 122
    assert row["fuel_tank_l"] == 5.5
    assert row["wheelbase_mm"] == 1350
    assert row["seat_height_mm"] == 790
    assert row["seating_capacity"] == 2
    # curated text
    assert row["category"] == "Scooter"
    assert row["transmission"] == "CVT"
    assert row["compression_ratio"] == "11.6:1"
    # raw_specs keeps the full sweep
    assert "Bore X Stroke" in row["raw_specs"]


class _FakeClient:
    """Returns a fixed sitemap body for discover_spec_urls."""

    def __init__(self, xml):
        self._xml = xml

    def get_status(self, url):
        return 200, self._xml


def test_discover_spec_urls_dedupes_malay_mirror():
    xml = """
    <urlset>
      <url><loc>https://www.zigwheels.my/new-motorcycles/yamaha/nvx/specifications</loc></url>
      <url><loc>https://www.zigwheels.my/ms/new-motorcycles/yamaha/nvx/specifications</loc></url>
      <url><loc>https://www.zigwheels.my/new-motorcycles/honda/rsx/specifications</loc></url>
      <url><loc>https://www.zigwheels.my/new-motorcycles/yamaha/nvx/overview</loc></url>
    </urlset>
    """
    urls = discover_spec_urls(_FakeClient(xml))
    assert urls == [
        "https://www.zigwheels.my/new-motorcycles/yamaha/nvx/specifications",
        "https://www.zigwheels.my/new-motorcycles/honda/rsx/specifications",
    ]


def test_upsert_idempotent(tmp_path):
    conn = init_specs_db(tmp_path / "t.db")
    row = parse_spec_page(_FIXTURE.read_text(encoding="utf-8"), _URL)
    upsert_spec(conn, row)
    upsert_spec(conn, row)  # second time must not duplicate
    n = conn.execute("SELECT COUNT(*) FROM model_specs").fetchone()[0]
    assert n == 1

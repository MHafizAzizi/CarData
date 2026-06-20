"""Tests for src/scrape_motomalaysia_specs.py — spec-page parser + coercion."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

import scrape_motomalaysia_specs as mm  # noqa: E402

_FIXTURE = (_ROOT / "tests" / "fixtures" / "motomalaysia_spec_125zr.html").read_text(encoding="utf-8")


class TestCoercion:
    def test_to_float_unit_strips_unit_and_comma(self):
        assert mm._to_float_unit("124.3 CC") == 124.3
        assert mm._to_float_unit("5.5 Liters") == 5.5
        assert mm._to_float_unit("106 kg") == 106.0

    def test_to_int_unit_handles_comma(self):
        assert mm._to_int_unit("1,250 mm") == 1250
        assert mm._to_int_unit("752 mm") == 752

    def test_missing_dash_is_none(self):
        assert mm._to_float_unit("–") is None
        assert mm._to_float_unit("-") is None
        assert mm._to_float_unit("") is None

    def test_power_kw_converted_to_hp(self):
        # 12.8 kW -> 12.8 * 1.34102 = 17.2 hp
        assert mm._power_to_hp("12.8kW @ 8,000 RPM") == 17.2

    def test_power_hp_passthrough(self):
        assert mm._power_to_hp("15 hp @ 8000 RPM") == 15.0


class TestParseSpecPage:
    def test_extracts_all_target_fields(self):
        spec = mm.parse_spec_page(_FIXTURE)
        assert spec["engine_cc"] == 124.3
        assert spec["torque_nm"] == 16.1
        assert spec["kerb_weight_kg"] == 106.0
        assert spec["fuel_tank_l"] == 5.5
        assert spec["seat_height_mm"] == 752
        assert spec["wheelbase_mm"] == 1250
        assert spec["compression_ratio"] == "6.5:1"
        assert spec["transmission"] == "6-Speed"
        assert spec["cooling"] == "Air Cooled"
        assert spec["engine_type"] == "2-Stroke"
        assert spec["power_hp"] == 17.2  # 12.8 kW converted

    def test_returns_none_for_absent_fields(self):
        spec = mm.parse_spec_page(_FIXTURE)
        # fuel_type is not provided by motomalaysia -> key absent or None
        assert spec.get("fuel_type") is None

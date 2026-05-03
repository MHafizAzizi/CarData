"""Tests for src/detail_client.py — HTML detail-page parser.

Pure-function tests run without HTTP. DetailClient.fetch tests mock the
underlying MudahClient.get so no network access happens.
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import RequestException

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

import detail_client as dc  # noqa: E402
from detail_client import (  # noqa: E402
    DETAIL_FIELDS,
    DetailClient,
    _extract_detail_fields,
    _parse_categoryparams,
    _parse_mcdparams,
    _parse_published,
)


# ---------------------------------------------------------------------------
# _parse_published
# ---------------------------------------------------------------------------

class TestParsePublished:
    def test_today(self):
        out = _parse_published("Today 15:49")
        today = datetime.now().strftime("%Y-%m-%d")
        assert out == f"{today} 15:49"

    def test_yesterday(self):
        out = _parse_published("Yesterday 09:00")
        y = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        assert out == f"{y} 09:00"

    def test_relative_date_with_time(self):
        # Day in current year e.g. "22 Apr 09:15"
        out = _parse_published("22 Apr 09:15")
        # Either current-year or last-year; just assert format
        assert len(out) == 16  # YYYY-MM-DD HH:MM
        assert out.endswith("09:15")

    def test_empty(self):
        assert _parse_published("") == ""

    def test_unrecognized_passes_through(self):
        # No matching format -> return original
        assert _parse_published("hello world") == "hello world"


# ---------------------------------------------------------------------------
# _parse_mcdparams
# ---------------------------------------------------------------------------

class TestParseMcdparams:
    def test_returns_flat_dict_for_known_fields(self):
        mcd = [
            {
                "params": [
                    {"id": "kw", "value": "120 kW", "realValue": 120},
                    {"id": "torque", "value": "200 Nm", "realValue": 200},
                    {"id": "engine", "value": "1.6L"},
                ]
            }
        ]
        out = _parse_mcdparams(mcd)
        assert out == {"kw": 120, "torque": 200, "engine": "1.6L"}

    def test_drops_unknown_fields(self):
        mcd = [
            {
                "params": [
                    {"id": "kw", "realValue": 100, "value": "100 kW"},
                    {"id": "random_field", "realValue": "x", "value": "x"},
                ]
            }
        ]
        out = _parse_mcdparams(mcd)
        assert "kw" in out
        assert "random_field" not in out

    def test_empty_or_none(self):
        assert _parse_mcdparams([]) == {}
        assert _parse_mcdparams(None) == {}

    def test_handles_malformed_groups(self):
        # Mix of valid + garbage; should not raise
        mcd = [
            None,
            "not a dict",
            {"params": [None, "garbage", {"id": "kw", "value": "5"}]},
        ]
        out = _parse_mcdparams(mcd)
        assert out == {"kw": "5"}

    def test_realvalue_preferred_over_value(self):
        mcd = [{"params": [{"id": "kw", "realValue": 100, "value": "100 kW"}]}]
        assert _parse_mcdparams(mcd)["kw"] == 100

    def test_falls_back_to_value_when_realvalue_empty(self):
        mcd = [{"params": [{"id": "kw", "realValue": "", "value": "100 kW"}]}]
        assert _parse_mcdparams(mcd)["kw"] == "100 kW"


# ---------------------------------------------------------------------------
# _parse_categoryparams
# ---------------------------------------------------------------------------

class TestParseCategoryparams:
    def test_keeps_known_fields(self):
        cat = [
            {"id": "mileage", "realValue": "50000", "value": "50,000 km"},
            {"id": "variant", "value": "EZL"},
            {"id": "subject", "value": "ignored"},  # not in DETAIL_FIELDS
        ]
        out = _parse_categoryparams(cat)
        assert out["mileage"] == "50000"
        assert out["variant"] == "EZL"
        assert "subject" not in out

    def test_empty(self):
        assert _parse_categoryparams([]) == {}
        assert _parse_categoryparams(None) == {}


# ---------------------------------------------------------------------------
# _extract_detail_fields
# ---------------------------------------------------------------------------

def _build_next_data(ads_id, *, cat_params=None, mcd_params=None):
    return {
        "props": {
            "initialState": {
                "adDetails": {
                    "byID": {
                        str(ads_id): {
                            "attributes": {
                                "categoryParams": cat_params or [],
                                "mcdParams": mcd_params or [],
                            }
                        }
                    }
                }
            }
        }
    }


class TestExtractDetailFields:
    def test_full_car_extraction(self):
        nd = _build_next_data(
            114522690,
            cat_params=[
                {"id": "mileage", "realValue": "84000", "value": "84,000 km"},
                {"id": "variant", "value": "EZL"},
                {"id": "family", "value": "Compact"},
            ],
            mcd_params=[
                {"params": [
                    {"id": "kw", "realValue": 70},
                    {"id": "torque", "realValue": 117},
                    {"id": "length", "realValue": 3690},
                    {"id": "brake_front", "value": "Disc"},
                ]}
            ],
        )
        out = _extract_detail_fields(nd, 114522690)
        assert out["mileage"] == "84000"
        assert out["variant"] == "EZL"
        assert out["family"] == "Compact"
        assert out["kw"] == 70
        assert out["length"] == 3690
        assert out["brake_front"] == "Disc"

    def test_missing_ad_returns_empty_dict(self):
        nd = _build_next_data(999)  # we'll query a different id
        out = _extract_detail_fields(nd, 12345)
        assert out == {}

    def test_no_params(self):
        nd = _build_next_data(1)
        out = _extract_detail_fields(nd, 1)
        assert out == {}

    def test_invalid_input_raises(self):
        with pytest.raises(ValueError):
            _extract_detail_fields("not a dict", 1)


# ---------------------------------------------------------------------------
# DetailClient.fetch — mocked HTTP
# ---------------------------------------------------------------------------

def _make_response(html: str, status_code: int = 200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = html
    resp.raise_for_status.return_value = None
    return resp


def _wrap_in_html(next_data_dict):
    """Embed __NEXT_DATA__ JSON inside a minimal HTML doc."""
    payload = json.dumps(next_data_dict)
    return f'<html><body><script id="__NEXT_DATA__" type="application/json">{payload}</script></body></html>'


@pytest.fixture
def mock_client():
    """A MudahClient stand-in with .get mocked."""
    client = MagicMock()
    return client


class TestDetailClientFetch:
    def test_ok_path(self, mock_client):
        nd = _build_next_data(
            114522690,
            cat_params=[{"id": "mileage", "realValue": "12345"}],
            mcd_params=[{"params": [{"id": "kw", "realValue": 100}]}],
        )
        mock_client.get.return_value = _make_response(_wrap_in_html(nd))
        dc_ = DetailClient(mock_client)

        out = dc_.fetch("https://www.mudah.my/foo-114522690.htm", 114522690)
        assert out["detail_fetch_status"] == "ok"
        assert "last_detail_fetched_at" in out
        assert out["mileage"] == "12345"
        assert out["kw"] == 100

    def test_request_exception_returns_error(self, mock_client):
        mock_client.get.side_effect = RequestException("connection refused")
        dc_ = DetailClient(mock_client)
        out = dc_.fetch("https://www.mudah.my/foo-1.htm", 1)
        assert out["detail_fetch_status"] == "error"
        assert "last_detail_fetched_at" in out
        # No data fields when error
        assert "mileage" not in out

    def test_missing_script_tag_returns_error(self, mock_client):
        mock_client.get.return_value = _make_response("<html><body>No script here</body></html>")
        dc_ = DetailClient(mock_client)
        out = dc_.fetch("https://www.mudah.my/foo-1.htm", 1)
        assert out["detail_fetch_status"] == "error"

    def test_malformed_json_returns_error(self, mock_client):
        bad = '<html><script id="__NEXT_DATA__" type="application/json">{not valid json}</script></html>'
        mock_client.get.return_value = _make_response(bad)
        dc_ = DetailClient(mock_client)
        out = dc_.fetch("https://www.mudah.my/foo-1.htm", 1)
        assert out["detail_fetch_status"] == "error"

    def test_unexpected_exception_returns_error(self, mock_client):
        mock_client.get.side_effect = RuntimeError("cloudscraper had a bad day")
        dc_ = DetailClient(mock_client)
        out = dc_.fetch("https://www.mudah.my/foo-1.htm", 1)
        assert out["detail_fetch_status"] == "error"

    def test_status_ok_but_ad_not_in_payload(self, mock_client):
        # Detail page reachable but the requested ad_id is not in byID
        nd = _build_next_data(99999, cat_params=[{"id": "mileage", "value": "9"}])
        mock_client.get.return_value = _make_response(_wrap_in_html(nd))
        dc_ = DetailClient(mock_client)
        out = dc_.fetch("https://www.mudah.my/foo-1.htm", 1)
        # Still 'ok' (no error parsing) but no detail fields populated
        assert out["detail_fetch_status"] == "ok"
        assert "mileage" not in out


# ---------------------------------------------------------------------------
# DETAIL_FIELDS sanity
# ---------------------------------------------------------------------------

class TestDetailFields:
    def test_contains_expected_categories(self):
        # mileage from attributes/categoryParams
        assert "mileage" in DETAIL_FIELDS
        assert "body" not in DETAIL_FIELDS
        # chassis specs from mcdParams
        for field in ("kw", "torque", "length", "wheelbase", "tyres_front",
                      "wheel_rim_rear", "brake_front", "suspension_rear"):
            assert field in DETAIL_FIELDS, f"missing {field}"
        # Trim/family
        for field in ("family", "variant", "series", "style", "seat", "country_origin"):
            assert field in DETAIL_FIELDS, f"missing {field}"

    def test_no_overlap_with_api_fields(self):
        # These are populated by the API, not the HTML detail
        api_only = {"old_price", "year_verified", "store_verified",
                    "bundle", "media_count", "mileage_bucket",
                    "car_loan_eligible", "car_loan_payment"}
        for f in api_only:
            assert f not in DETAIL_FIELDS, f"{f} should not be in DETAIL_FIELDS"

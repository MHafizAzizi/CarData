"""Tests for src/eagle_client.py — EagleSearch API wrapper.

Tests are split into two groups:
- Pure normalization tests (no HTTP, no mocking)
- HTTP behavior tests (mock the .scraper.get() method)
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

import eagle_client as ec  # noqa: E402
from eagle_client import (  # noqa: E402
    BASE_URL,
    CATEGORY_IDS,
    MAX_LIMIT,
    MAX_OFFSET,
    EagleAPIError,
    EagleAuthError,
    EagleClient,
    _coerce_bool_int,
    _format_mileage_bucket,
    _normalize_ad,
)


# ---------------------------------------------------------------------------
# _format_mileage_bucket
# ---------------------------------------------------------------------------

class TestFormatMileageBucket:
    def test_full_bucket(self):
        assert _format_mileage_bucket({"gte": 50000, "lte": 59999}) == "50000-59999"

    def test_only_gte(self):
        assert _format_mileage_bucket({"gte": 100000}) == "100000-"

    def test_only_lte(self):
        assert _format_mileage_bucket({"lte": 9999}) == "-9999"

    def test_empty_dict(self):
        assert _format_mileage_bucket({}) is None

    def test_none(self):
        assert _format_mileage_bucket(None) is None

    def test_non_dict(self):
        assert _format_mileage_bucket("50000") is None
        assert _format_mileage_bucket(12345) is None


# ---------------------------------------------------------------------------
# _coerce_bool_int
# ---------------------------------------------------------------------------

class TestCoerceBoolInt:
    def test_true_false(self):
        assert _coerce_bool_int(True) == 1
        assert _coerce_bool_int(False) == 0

    def test_int_truthy(self):
        assert _coerce_bool_int(1) == 1
        assert _coerce_bool_int(0) == 0

    def test_string_variants(self):
        assert _coerce_bool_int("true") == 1
        assert _coerce_bool_int("false") == 0
        assert _coerce_bool_int("1") == 1
        assert _coerce_bool_int("yes") == 1

    def test_none(self):
        assert _coerce_bool_int(None) is None

    def test_unparseable(self):
        assert _coerce_bool_int("maybe") is None


# ---------------------------------------------------------------------------
# _normalize_ad — cars
# ---------------------------------------------------------------------------

CAR_FIXTURE = {
    "ad_id": 134517092,                  # dropped (separate internal ID)
    "list_id": 114522690,                # -> ads_id (URL-embedded primary key)
    "subject": "2010 Perodua MYVI 1.3",
    "price": 9800,
    "old_price": 12000,
    "manufactured_year": "2010",
    "name": "Hj Azman",
    "subarea_name": "Shah Alam",
    "region_name": "Selangor",
    "make_name": "Perodua",
    "model_name": "MyVi",
    "make_id": "13",                     # dropped
    "model_id": "1714",                  # dropped
    "transmission_name": "Auto",
    "fueltype": "petrol",
    "engine_capacity": "1298",
    "condition_name": "Used",
    "company_ad": False,
    "store_verified": "verified",
    "year_verified": True,
    "media_count": 12,
    "mileage": {"gte": 80000, "lte": 84999},
    "car_loan_eligible": True,
    "car_loan_payment": 2506,
    "car_loan_tenure": 6,
    "has_car_grant": True,
    "bundle": "47",
    "date": "2026-05-02 10:18:34",
    "adview_url": "https://www.mudah.my/2010-perodua-myvi-114522690.htm",
    "ad_expiry": "2026-07-01 10:18:34",
    "rank": {"score": "0"},              # dropped (not in mapping)
}


class TestNormalizeAdCars:
    def test_renames_to_db_columns(self):
        out = _normalize_ad(CAR_FIXTURE, "cars")
        assert out["ads_id"] == 114522690  # from list_id, NOT ad_id
        assert out["make"] == "Perodua"
        assert out["model"] == "MyVi"
        assert out["seller_name"] == "Hj Azman"
        assert out["subregion"] == "Shah Alam"
        assert out["region"] == "Selangor"
        assert out["transmission"] == "Auto"
        assert out["fuel_type"] == "petrol"
        assert out["manufactured_date"] == "2010"
        assert out["url"] == "https://www.mudah.my/2010-perodua-myvi-114522690.htm"
        assert out["published"] == "2026-05-02 10:18:34"

    def test_drops_unknown_fields(self):
        out = _normalize_ad(CAR_FIXTURE, "cars")
        assert "list_id" not in out
        assert "ad_id" not in out
        assert "make_id" not in out
        assert "model_id" not in out
        assert "rank" not in out

    def test_mileage_bucket(self):
        out = _normalize_ad(CAR_FIXTURE, "cars")
        assert out["mileage_bucket"] == "80000-84999"

    def test_bool_flags_to_int(self):
        out = _normalize_ad(CAR_FIXTURE, "cars")
        assert out["year_verified"] == 1
        assert out["has_car_grant"] == 1
        assert out["car_loan_eligible"] == 1
        assert out["company_ad"] == 0  # False -> 0

    def test_loan_fields_pass_through(self):
        out = _normalize_ad(CAR_FIXTURE, "cars")
        assert out["car_loan_payment"] == 2506
        assert out["car_loan_tenure"] == 6

    def test_old_price_present(self):
        out = _normalize_ad(CAR_FIXTURE, "cars")
        assert out["old_price"] == 12000

    def test_ad_expiry_captured(self):
        out = _normalize_ad(CAR_FIXTURE, "cars")
        assert out["ad_expiry"] == "2026-07-01 10:18:34"


# ---------------------------------------------------------------------------
# _normalize_ad — motorcycles
# ---------------------------------------------------------------------------

MOTO_FIXTURE = {
    "ad_id": 134169908,                  # dropped (separate internal ID)
    "list_id": 114210850,                # -> ads_id
    "subject": "SM Sport 110E NEW",
    "price": 3350,
    "manufactured_year": "2026",
    "name": "Fook Yong Motor Trading SB",
    "region_name": "Johor",
    "subarea_name": "Johor Bahru",
    "motorcycle_make_name": "SM Sport",
    "motorcycle_model_name": "110E",
    "motorcycle_make_id": "49",          # dropped
    "motorcycle_model_id": "993",        # dropped
    "condition_name": "New",
    "company_ad": True,
    "store_verified": "unverified",
    "media_count": 12,
    "bundle": "75",
    "date": "2026-05-02 10:13:36",
    "adview_url": "https://www.mudah.my/sm-sport-110e-114210850.htm",
    "ad_expiry": "2026-07-01 10:13:36",
}


class TestNormalizeAdMotorcycles:
    def test_motorcycle_make_model_renamed(self):
        out = _normalize_ad(MOTO_FIXTURE, "motorcycles")
        assert out["motorcycle_make"] == "SM Sport"
        assert out["motorcycle_model"] == "110E"

    def test_no_car_only_fields(self):
        out = _normalize_ad(MOTO_FIXTURE, "motorcycles")
        # Cars-only renames must not appear
        assert "make" not in out
        assert "model" not in out
        assert "transmission" not in out
        assert "fuel_type" not in out
        assert "car_loan_eligible" not in out
        assert "has_car_grant" not in out

    def test_motorcycle_id_fields_dropped(self):
        out = _normalize_ad(MOTO_FIXTURE, "motorcycles")
        assert "motorcycle_make_id" not in out
        assert "motorcycle_model_id" not in out

    def test_company_ad_true(self):
        out = _normalize_ad(MOTO_FIXTURE, "motorcycles")
        assert out["company_ad"] == 1

    def test_no_mileage_bucket_when_absent(self):
        out = _normalize_ad(MOTO_FIXTURE, "motorcycles")
        # Motos rarely have mileage; absent in fixture
        assert "mileage_bucket" not in out

    def test_ad_expiry_captured(self):
        out = _normalize_ad(MOTO_FIXTURE, "motorcycles")
        assert out["ad_expiry"] == "2026-07-01 10:13:36"


# ---------------------------------------------------------------------------
# _normalize_ad — error / edge cases
# ---------------------------------------------------------------------------

class TestNormalizeAdErrors:
    def test_unknown_category_raises(self):
        with pytest.raises(ValueError, match="Unknown category"):
            _normalize_ad(CAR_FIXTURE, "trucks")

    def test_minimal_input_no_keyerror(self):
        out = _normalize_ad({"list_id": 999}, "cars")
        assert out == {"ads_id": 999}


# ---------------------------------------------------------------------------
# EagleClient.fetch_page — HTTP behavior (mocked)
# ---------------------------------------------------------------------------

def _mock_response(status_code: int, json_body=None, text="") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body if json_body is not None else {}
    resp.text = text
    if status_code >= 400:
        from requests import HTTPError
        resp.raise_for_status.side_effect = HTTPError(f"HTTP {status_code}")
    else:
        resp.raise_for_status.return_value = None
    return resp


@pytest.fixture
def client():
    """EagleClient with throttle disabled (interval=0) for fast tests."""
    c = EagleClient(request_interval=(0.0, 0.0), max_retries=1, retry_waits=(0,))
    return c


class TestFetchPage:
    def test_returns_normalized_list_and_meta(self, client):
        body = {
            "data": [
                {"type": "ads", "id": 1, "attributes": CAR_FIXTURE},
            ],
            "meta": {"total-results": 100, "took": 5},
        }
        with patch.object(client.scraper, "get", return_value=_mock_response(200, body)):
            ads, meta = client.fetch_page("cars", offset=0, limit=10)
        assert len(ads) == 1
        assert ads[0]["ads_id"] == CAR_FIXTURE["list_id"]
        assert ads[0]["make"] == "Perodua"
        assert meta["total-results"] == 100

    def test_unknown_category_raises(self, client):
        with pytest.raises(ValueError, match="Unknown category"):
            client.fetch_page("trucks")

    def test_403_raises_auth_error(self, client):
        with patch.object(client.scraper, "get", return_value=_mock_response(403)):
            with pytest.raises(EagleAuthError):
                client.fetch_page("cars")

    def test_401_raises_auth_error(self, client):
        with patch.object(client.scraper, "get", return_value=_mock_response(401)):
            with pytest.raises(EagleAuthError):
                client.fetch_page("cars")

    def test_500_raises_api_error_after_retries(self, client):
        with patch.object(client.scraper, "get", return_value=_mock_response(500)):
            with pytest.raises(EagleAPIError):
                client.fetch_page("cars")

    def test_malformed_json_raises_api_error(self, client):
        bad_resp = MagicMock()
        bad_resp.status_code = 200
        bad_resp.text = "<html>not json</html>"
        bad_resp.raise_for_status.return_value = None
        bad_resp.json.side_effect = ValueError("not json")
        with patch.object(client.scraper, "get", return_value=bad_resp):
            with pytest.raises(EagleAPIError, match="non-JSON"):
                client.fetch_page("cars")

    def test_clamps_limit_above_max(self, client):
        body = {"data": [], "meta": {"total-results": 0}}
        captured = {}
        def fake_get(url, params=None, **kwargs):
            captured["params"] = params
            return _mock_response(200, body)
        with patch.object(client.scraper, "get", side_effect=fake_get):
            try:
                client.fetch_page("cars", offset=0, limit=999)
            except EagleAPIError:
                pass  # offset=0 + empty raises; not what we test here
        assert captured["params"]["limit"] == MAX_LIMIT

    def test_year_sets_manufactured_year_param_for_cars(self, client):
        captured = {}
        def fake_get(url, params=None, **kwargs):
            captured["params"] = params
            return _mock_response(200, {"data": [], "meta": {"total-results": 0}})
        with patch.object(client.scraper, "get", side_effect=fake_get):
            try:
                client.fetch_page("cars", year=2020, make_id="35", model_id="100")
            except EagleAPIError:
                pass
        assert captured["params"]["manufactured_year"] == 2020
        assert captured["params"]["make_id"] == "35"

    def test_no_year_omits_manufactured_year_param(self, client):
        captured = {}
        def fake_get(url, params=None, **kwargs):
            captured["params"] = params
            return _mock_response(200, {"data": [], "meta": {"total-results": 0}})
        with patch.object(client.scraper, "get", side_effect=fake_get):
            try:
                client.fetch_page("cars")
            except EagleAPIError:
                pass
        assert "manufactured_year" not in captured["params"]

    def test_skips_malformed_ads(self, client):
        body = {
            "data": [
                {"type": "ads", "id": 1, "attributes": CAR_FIXTURE},
                {"type": "ads", "id": 2},  # no attributes -> skipped
                {"not": "an ad"},          # garbage -> skipped
            ],
            "meta": {"total-results": 3},
        }
        with patch.object(client.scraper, "get", return_value=_mock_response(200, body)):
            ads, _ = client.fetch_page("cars")
        assert len(ads) == 1


# ---------------------------------------------------------------------------
# EagleClient.fetch_all — pagination behavior
# ---------------------------------------------------------------------------

class TestFetchAll:
    def test_yields_pages_until_empty(self, client):
        page1 = {
            "data": [{"type": "ads", "id": i, "attributes": {**CAR_FIXTURE, "list_id": i}}
                     for i in range(200)],
            "meta": {"total-results": 250},
        }
        page2 = {
            "data": [{"type": "ads", "id": i, "attributes": {**CAR_FIXTURE, "list_id": i}}
                     for i in range(200, 250)],
            "meta": {"total-results": 250},
        }
        empty = {"data": [], "meta": {"total-results": 250}}
        responses = [_mock_response(200, page1), _mock_response(200, page2), _mock_response(200, empty)]
        with patch.object(client.scraper, "get", side_effect=responses):
            pages = list(client.fetch_all("cars"))
        assert len(pages) == 2
        assert len(pages[0]) == 200
        assert len(pages[1]) == 50

    def test_respects_max_ads(self, client):
        """fetch_all should pass shrinking `limit` as max_ads is approached
        and stop after the cap is hit."""
        captured_limits = []

        def fake_get(url, params=None, **kwargs):
            captured_limits.append(params["limit"])
            # Server honors `limit` exactly
            n = params["limit"]
            offset = params["from"]
            return _mock_response(200, {
                "data": [{"type": "ads", "id": offset + i,
                          "attributes": {**CAR_FIXTURE, "ad_id": offset + i}}
                         for i in range(n)],
                "meta": {"total-results": 1000},
            })

        with patch.object(client.scraper, "get", side_effect=fake_get):
            pages = list(client.fetch_all("cars", max_ads=300))

        total = sum(len(p) for p in pages)
        assert total == 300
        # First call gets MAX_LIMIT (200), second call gets remaining (100)
        assert captured_limits[0] == MAX_LIMIT
        assert captured_limits[1] == 100

    def test_empty_at_offset_zero_raises(self, client):
        empty = {"data": [], "meta": {"total-results": 0}}
        with patch.object(client.scraper, "get", return_value=_mock_response(200, empty)):
            with pytest.raises(EagleAPIError, match="no results at offset=0"):
                list(client.fetch_all("cars"))

    def test_stops_at_depth_cap(self, client):
        """When offset >= MAX_OFFSET, stop without further requests."""
        # Provide one full page so first call succeeds, then fetch_all should
        # advance offset and stop voluntarily once it crosses MAX_OFFSET.
        # We simulate this by setting max_ads close to MAX_OFFSET + 1 page.
        page = {
            "data": [{"type": "ads", "id": i, "attributes": {**CAR_FIXTURE, "list_id": i}}
                     for i in range(200)],
            "meta": {"total-results": 30000},
        }
        # 50 pages of 200 = 10_000 (exactly MAX_OFFSET)
        responses = [_mock_response(200, page) for _ in range(60)]
        with patch.object(client.scraper, "get", side_effect=responses) as mock_get:
            pages = list(client.fetch_all("cars"))
        # Should make at most 50 calls (10_000 / 200)
        assert mock_get.call_count <= 50
        # And total ads yielded should match calls * 200
        assert sum(len(p) for p in pages) == mock_get.call_count * 200


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------

class TestConstants:
    def test_category_ids(self):
        assert CATEGORY_IDS["cars"] == 1020
        assert CATEGORY_IDS["motorcycles"] == 1040

    def test_max_limit(self):
        assert MAX_LIMIT == 200

    def test_max_offset(self):
        assert MAX_OFFSET == 10_000

    def test_base_url(self):
        assert BASE_URL == "https://search.mudah.my/v1/search"


# ---------------------------------------------------------------------------
# Retry-After parsing + 429 handling
# ---------------------------------------------------------------------------

from mudah_client import parse_retry_after  # noqa: E402


class TestParseRetryAfter:
    def test_integer_seconds(self):
        assert parse_retry_after("30") == 30.0
        assert parse_retry_after("  60  ") == 60.0

    def test_float_seconds(self):
        assert parse_retry_after("12.5") == 12.5

    def test_negative_clamped_to_zero(self):
        assert parse_retry_after("-5") == 0.0

    def test_http_date_past_returns_zero(self):
        assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0

    def test_http_date_future_returns_positive(self):
        # 100 years from now-ish — must be positive
        out = parse_retry_after("Sat, 01 Jan 2125 00:00:00 GMT")
        assert out is not None and out > 0

    def test_none_or_empty(self):
        assert parse_retry_after(None) is None
        assert parse_retry_after("") is None

    def test_garbage(self):
        assert parse_retry_after("soon") is None


class Test429Handling:
    def test_429_then_success_recovers(self):
        """A 429 should trigger a back-off and a retry; the retry's 200
        response is returned to the caller (no exception raised)."""
        c = EagleClient(request_interval=(0.0, 0.0), max_retries=2, retry_waits=(0,))

        first = MagicMock()
        first.status_code = 429
        first.headers = {"Retry-After": "0"}
        first.raise_for_status.side_effect = AssertionError("should not be called")

        body = {"data": [{"type": "ads", "id": 1, "attributes": CAR_FIXTURE}], "meta": {}}
        second = _mock_response(200, body)

        with patch.object(c.scraper, "get", side_effect=[first, second]):
            ads, _ = c.fetch_page("cars", offset=0, limit=10)
        assert len(ads) == 1

    def test_429_uses_retry_after_header_value(self):
        """parse_retry_after's output should be slept between attempts."""
        c = EagleClient(request_interval=(0.0, 0.0), max_retries=1, retry_waits=(0,))

        first = MagicMock()
        first.status_code = 429
        first.headers = {"Retry-After": "7"}

        body = {"data": [{"type": "ads", "id": 1, "attributes": CAR_FIXTURE}], "meta": {}}
        second = _mock_response(200, body)

        with patch.object(c.scraper, "get", side_effect=[first, second]), \
             patch("eagle_client.time.sleep") as sleep_mock:
            c.fetch_page("cars", offset=0, limit=10)

        # Among all sleep calls, one must be exactly 7.0 (Retry-After honored)
        slept = [call.args[0] for call in sleep_mock.call_args_list if call.args]
        assert 7.0 in slept

    def test_429_caps_at_max_wait(self):
        """Retry-After larger than RATE_LIMIT_MAX_WAIT should be capped."""
        from mudah_client import RATE_LIMIT_MAX_WAIT
        c = EagleClient(request_interval=(0.0, 0.0), max_retries=1, retry_waits=(0,))

        first = MagicMock()
        first.status_code = 429
        first.headers = {"Retry-After": str(int(RATE_LIMIT_MAX_WAIT * 10))}

        body = {"data": [{"type": "ads", "id": 1, "attributes": CAR_FIXTURE}], "meta": {}}
        second = _mock_response(200, body)

        with patch.object(c.scraper, "get", side_effect=[first, second]), \
             patch("eagle_client.time.sleep") as sleep_mock:
            c.fetch_page("cars", offset=0, limit=10)

        slept = [call.args[0] for call in sleep_mock.call_args_list if call.args]
        assert max(slept) == RATE_LIMIT_MAX_WAIT

    def test_429_default_when_header_missing(self):
        from mudah_client import RATE_LIMIT_DEFAULT_WAIT
        c = EagleClient(request_interval=(0.0, 0.0), max_retries=1, retry_waits=(0,))

        first = MagicMock()
        first.status_code = 429
        first.headers = {}  # no Retry-After

        body = {"data": [{"type": "ads", "id": 1, "attributes": CAR_FIXTURE}], "meta": {}}
        second = _mock_response(200, body)

        with patch.object(c.scraper, "get", side_effect=[first, second]), \
             patch("eagle_client.time.sleep") as sleep_mock:
            c.fetch_page("cars", offset=0, limit=10)

        slept = [call.args[0] for call in sleep_mock.call_args_list if call.args]
        assert RATE_LIMIT_DEFAULT_WAIT in slept

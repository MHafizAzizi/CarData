import json
import pytest
from unittest.mock import MagicMock, patch
from bs4 import BeautifulSoup

# Patch cloudscraper and fake_useragent before importing MudahScraper
with patch("cloudscraper.create_scraper"), patch("fake_useragent.UserAgent"):
    from src.script import MudahScraper


@pytest.fixture
def scraper():
    with patch("cloudscraper.create_scraper"), patch("fake_useragent.UserAgent"):
        return MudahScraper(max_retries=1, base_delay=0)


# ---------------------------------------------------------------------------
# parse_mcdparams
# ---------------------------------------------------------------------------

class TestParseMcdparams:
    def test_returns_flat_list(self, scraper):
        input_data = [
            {
                "params": [
                    {"realValue": "1.5", "id": "cc", "value": "1500", "label": "Engine CC"},
                    {"realValue": "100", "id": "kw", "value": "100", "label": "Power"},
                ]
            }
        ]
        result = scraper.parse_mcdparams(input_data)
        assert len(result) == 2
        assert result[0] == {"realValue": "1.5", "id": "cc", "value": "1500", "label": "Engine CC"}
        assert result[1]["id"] == "kw"

    def test_empty_input(self, scraper):
        assert scraper.parse_mcdparams([]) == []

    def test_group_with_no_params_key(self, scraper):
        assert scraper.parse_mcdparams([{"other": "stuff"}]) == []

    def test_missing_fields_default_to_empty_string(self, scraper):
        input_data = [{"params": [{"id": "torque"}]}]
        result = scraper.parse_mcdparams(input_data)
        assert result[0]["realValue"] == ""
        assert result[0]["value"] == ""
        assert result[0]["label"] == ""

    def test_multiple_groups(self, scraper):
        input_data = [
            {"params": [{"id": "cc", "realValue": "", "value": "1500", "label": "CC"}]},
            {"params": [{"id": "kw", "realValue": "", "value": "80", "label": "KW"}]},
        ]
        result = scraper.parse_mcdparams(input_data)
        assert len(result) == 2
        assert {r["id"] for r in result} == {"cc", "kw"}


# ---------------------------------------------------------------------------
# _attrs_to_dict
# ---------------------------------------------------------------------------

class TestAttrsToDict:
    def test_filters_to_known_keys_only(self, scraper):
        attrs = [
            {"id": "make", "value": "Toyota"},
            {"id": "unknown_field", "value": "ignored"},
            {"id": "price", "value": "50000"},
        ]
        result = scraper._attrs_to_dict(attrs)
        assert result == {"make": "Toyota", "price": "50000"}
        assert "unknown_field" not in result

    def test_empty_attrs(self, scraper):
        assert scraper._attrs_to_dict([]) == {}

    def test_all_unknown_keys(self, scraper):
        attrs = [{"id": "foo", "value": "bar"}, {"id": "baz", "value": "qux"}]
        assert scraper._attrs_to_dict(attrs) == {}

    def test_all_known_keys(self, scraper):
        attrs = [{"id": "make", "value": "Honda"}, {"id": "model", "value": "Civic"}]
        result = scraper._attrs_to_dict(attrs)
        assert result == {"make": "Honda", "model": "Civic"}


# ---------------------------------------------------------------------------
# get_page_urls
# ---------------------------------------------------------------------------

class TestGetPageUrls:
    def test_single_page_cars(self, scraper):
        urls = scraper.get_page_urls("selangor", "cars", "toyota", 1, 1)
        assert len(urls) == 1
        assert "selangor" in urls[0]
        assert "toyota" in urls[0]
        assert "cars-for-sale" in urls[0]
        assert urls[0].endswith("1")

    def test_single_page_motorcycles(self, scraper):
        urls = scraper.get_page_urls("malaysia", "motorcycles", "royal-enfield", 1, 1)
        assert len(urls) == 1
        assert "motorcycles-for-sale" in urls[0]
        assert "royal-enfield" in urls[0]

    def test_multiple_pages(self, scraper):
        urls = scraper.get_page_urls("johor", "cars", "honda", 1, 5)
        assert len(urls) == 5
        assert urls[0].endswith("1")
        assert urls[-1].endswith("5")

    def test_no_brand_uses_all_cars_url(self, scraper):
        urls = scraper.get_page_urls("malaysia", "cars", "", 1, 1)
        assert "cars-for-sale?" in urls[0]
        assert "/none" not in urls[0]

    def test_no_brand_motorcycles_url(self, scraper):
        urls = scraper.get_page_urls("malaysia", "motorcycles", "", 1, 1)
        assert "motorcycles-for-sale?" in urls[0]

    def test_brand_none_string_uses_all_cars_url(self, scraper):
        urls = scraper.get_page_urls("malaysia", "cars", "none", 1, 1)
        assert "cars-for-sale?" in urls[0]

    def test_state_lowercased(self, scraper):
        urls = scraper.get_page_urls("Selangor", "cars", "Toyota", 1, 1)
        assert "selangor" in urls[0]

    def test_empty_state_defaults_to_malaysia(self, scraper):
        urls = scraper.get_page_urls("", "cars", "", 1, 1)
        assert "malaysia" in urls[0]


# ---------------------------------------------------------------------------
# get_car_urls — unit tests using mock HTML responses
# ---------------------------------------------------------------------------

MOCK_LD_JSON_HTML = """
<html><head>
<script type="application/ld+json">
[{}, {}, {"itemListElement": [
    {"item": {"url": "https://www.mudah.my/cars/toyota-vios-123.html"}},
    {"item": {"url": "https://www.mudah.my/cars/honda-civic-456.html"}}
]}]
</script>
</head><body></body></html>
"""

MOCK_NEXT_DATA_HTML = """
<html><body>
<script id="__NEXT_DATA__">
{"props": {"pageProps": {"items": [
    {"url": "https://www.mudah.my/cars/proton-saga-789.html"},
    {"url": "https://www.mudah.my/cars/perodua-myvi-101.html"}
]}}}
</script>
</body></html>
"""

MOCK_EMPTY_HTML = "<html><body><p>No listings</p></body></html>"


class TestGetCarUrls:
    def _mock_response(self, html: str):
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    def test_extracts_urls_from_ld_json(self, scraper):
        with patch.object(scraper, "_make_request", return_value=self._mock_response(MOCK_LD_JSON_HTML)):
            urls = scraper.get_car_urls("http://fake.url", expected_urls=1, max_retries=1)
        assert len(urls) == 2
        assert any("toyota-vios" in u for u in urls)
        assert any("honda-civic" in u for u in urls)

    def test_extracts_urls_from_next_data(self, scraper):
        with patch.object(scraper, "_make_request", return_value=self._mock_response(MOCK_NEXT_DATA_HTML)):
            urls = scraper.get_car_urls("http://fake.url", expected_urls=1, max_retries=1)
        assert len(urls) == 2
        assert any("proton-saga" in u for u in urls)
        assert any("perodua-myvi" in u for u in urls)

    def test_returns_empty_on_no_listings(self, scraper):
        with patch.object(scraper, "_make_request", return_value=self._mock_response(MOCK_EMPTY_HTML)):
            urls = scraper.get_car_urls("http://fake.url", expected_urls=40, max_retries=1)
        assert urls == []

    def test_returns_best_result_after_retries(self, scraper):
        responses = [
            self._mock_response(MOCK_LD_JSON_HTML),   # 2 URLs
            self._mock_response(MOCK_EMPTY_HTML),      # 0 URLs (never reached)
        ]
        with patch.object(scraper, "_make_request", side_effect=responses):
            urls = scraper.get_car_urls("http://fake.url", expected_urls=40, max_retries=2)
        assert len(urls) == 2

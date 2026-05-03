"""Tests for src/scraper.py — HybridScraper orchestrator."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from scraper import (  # noqa: E402
    DEPTH_CHOICES,
    HybridScraper,
)
from eagle_client import EagleAuthError  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _api_ad(ads_id: int, *, with_mileage: bool = False) -> dict:
    out = {
        "ads_id": ads_id,
        "subject": f"Listing {ads_id}",
        "price": 10000,
        "make": "Toyota",
        "model": "Vios",
        "url": f"https://www.mudah.my/foo-{ads_id}.htm",
        "manufactured_date": "2020",
        "mileage_bucket": "50000-59999",
    }
    if with_mileage:
        out["mileage"] = "55000"
    return out


def _detail_payload() -> dict:
    return {
        "mileage": "55000",
        "kw": 70,
        "torque": 117,
        "detail_fetch_status": "ok",
        "last_detail_fetched_at": "2026-05-02 10:00:00",
    }


@pytest.fixture
def mock_eagle():
    return MagicMock()


@pytest.fixture
def mock_detail():
    m = MagicMock()
    m.fetch.return_value = _detail_payload()
    return m


@pytest.fixture
def scraper(tmp_path, mock_eagle, mock_detail):
    return HybridScraper(
        category="cars",
        eagle_client=mock_eagle,
        detail_client=mock_detail,
        output_dir=tmp_path,
    )


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

class TestInit:
    def test_unknown_category_raises(self, tmp_path, mock_eagle, mock_detail):
        with pytest.raises(ValueError, match="Unknown category"):
            HybridScraper("trucks", mock_eagle, mock_detail, tmp_path)

    def test_creates_output_dir(self, tmp_path, mock_eagle, mock_detail):
        out = tmp_path / "subdir" / "raw"
        HybridScraper("cars", mock_eagle, mock_detail, out)
        assert out.exists()


# ---------------------------------------------------------------------------
# _needs_detail
# ---------------------------------------------------------------------------

class TestNeedsDetail:
    def test_depth_none_always_false(self, scraper):
        ad_with = _api_ad(1, with_mileage=True)
        ad_without = _api_ad(2)
        assert scraper._needs_detail(ad_with, "none") is False
        assert scraper._needs_detail(ad_without, "none") is False

    def test_depth_all_always_true(self, scraper):
        ad_with = _api_ad(1, with_mileage=True)
        ad_without = _api_ad(2)
        assert scraper._needs_detail(ad_with, "all") is True
        assert scraper._needs_detail(ad_without, "all") is True

    def test_depth_missing_true_when_mileage_absent(self, scraper):
        assert scraper._needs_detail(_api_ad(1), "missing") is True

    def test_depth_missing_false_when_mileage_present(self, scraper):
        assert scraper._needs_detail(_api_ad(1, with_mileage=True), "missing") is False

    def test_depth_missing_true_when_mileage_empty_string(self, scraper):
        ad = _api_ad(1)
        ad["mileage"] = ""
        assert scraper._needs_detail(ad, "missing") is True


# ---------------------------------------------------------------------------
# _phase1_collect
# ---------------------------------------------------------------------------

class TestPhase1Collect:
    def test_aggregates_pages(self, scraper, mock_eagle):
        page1 = [_api_ad(i) for i in range(5)]
        page2 = [_api_ad(i) for i in range(5, 8)]
        mock_eagle.fetch_all.return_value = iter([page1, page2])

        ads = scraper._phase1_collect(max_ads=None)

        assert len(ads) == 8
        mock_eagle.fetch_all.assert_called_once_with("cars", max_ads=None)

    def test_respects_max_ads_truncation(self, scraper, mock_eagle):
        # Even if API returned more, scraper truncates to max_ads
        page = [_api_ad(i) for i in range(50)]
        mock_eagle.fetch_all.return_value = iter([page])

        ads = scraper._phase1_collect(max_ads=10)
        assert len(ads) == 10

    def test_empty_input(self, scraper, mock_eagle):
        mock_eagle.fetch_all.return_value = iter([])
        ads = scraper._phase1_collect(max_ads=100)
        assert ads == []


# ---------------------------------------------------------------------------
# _phase2_enrich
# ---------------------------------------------------------------------------

class TestPhase2Enrich:
    def test_depth_all_fetches_every_ad(self, scraper, mock_detail):
        ads = [_api_ad(i) for i in range(3)]
        out = scraper._phase2_enrich(ads, "all", checkpoint_every=100, timestamp="t")
        assert mock_detail.fetch.call_count == 3
        assert all(ad.get("detail_fetch_status") == "ok" for ad in out)
        # mileage populated from detail
        assert all(ad.get("mileage") == "55000" for ad in out)

    def test_depth_missing_skips_ads_with_mileage(self, scraper, mock_detail):
        ads = [_api_ad(0, with_mileage=True), _api_ad(1), _api_ad(2, with_mileage=True)]
        out = scraper._phase2_enrich(ads, "missing", checkpoint_every=100, timestamp="t")
        # Only 1 fetch — for the one without mileage
        assert mock_detail.fetch.call_count == 1
        # The skipped ones get 'skipped' status
        statuses = [ad["detail_fetch_status"] for ad in out]
        assert statuses.count("skipped") == 2
        assert statuses.count("ok") == 1

    def test_missing_url_or_ads_id_marked_error(self, scraper, mock_detail):
        ads = [_api_ad(1)]
        ads[0].pop("url")  # corrupt the ad
        out = scraper._phase2_enrich(ads, "all", checkpoint_every=100, timestamp="t")
        # detail.fetch should NOT have been called
        mock_detail.fetch.assert_not_called()
        assert out[0]["detail_fetch_status"] == "error"
        assert "last_detail_fetched_at" in out[0]

    def test_checkpoint_writes_partial_csv(self, scraper, mock_detail, tmp_path):
        ads = [_api_ad(i) for i in range(5)]
        scraper._phase2_enrich(ads, "all", checkpoint_every=2, timestamp="ts")
        # Two checkpoints expected: at 2 and at 4 (5th doesn't hit boundary)
        partials = list(tmp_path.glob("*phase2_partial*.csv"))
        assert len(partials) == 2


# ---------------------------------------------------------------------------
# _write_csv
# ---------------------------------------------------------------------------

class TestWriteCsv:
    def test_writes_csv_with_correct_filename(self, scraper, tmp_path):
        ads = [_api_ad(1), _api_ad(2)]
        path = scraper._write_csv(ads, "20260502120000", suffix="final")
        assert path.exists()
        assert path.parent == tmp_path
        assert "mudah_eagle_cars_final_20260502120000" in path.name

    def test_empty_ads_writes_empty_csv(self, scraper):
        path = scraper._write_csv([], "ts", suffix="empty")
        assert path.exists()
        # pd.DataFrame().to_csv writes an empty file (no header). Accept either form.
        content = path.read_text(encoding="utf-8")
        assert content.strip() == ""

    def test_csv_round_trip_preserves_columns(self, scraper):
        ads = [_api_ad(1), _api_ad(2)]
        path = scraper._write_csv(ads, "ts", suffix="rt")
        df = pd.read_csv(path)
        assert "ads_id" in df.columns
        assert "url" in df.columns
        assert df["ads_id"].tolist() == [1, 2]


# ---------------------------------------------------------------------------
# run() — full integration with mocked clients
# ---------------------------------------------------------------------------

class TestRun:
    def test_full_run_depth_missing(self, scraper, mock_eagle, mock_detail, tmp_path):
        # 3 ads from API, 1 already has mileage
        page = [_api_ad(0, with_mileage=True), _api_ad(1), _api_ad(2)]
        mock_eagle.fetch_all.return_value = iter([page])

        path = scraper.run(max_ads=None, depth="missing", checkpoint_every=100)

        assert path.exists()
        assert "final" in path.name
        # 2 fetches (only ads without mileage)
        assert mock_detail.fetch.call_count == 2
        # Phase-1 checkpoint should be cleaned up
        phase1_files = list(tmp_path.glob("*phase1*.csv"))
        assert phase1_files == []

    def test_run_depth_none_skips_phase2(self, scraper, mock_eagle, mock_detail):
        page = [_api_ad(i) for i in range(3)]
        mock_eagle.fetch_all.return_value = iter([page])

        scraper.run(max_ads=None, depth="none", checkpoint_every=100)
        mock_detail.fetch.assert_not_called()

    def test_run_depth_all_fetches_all(self, scraper, mock_eagle, mock_detail):
        page = [_api_ad(i, with_mileage=True) for i in range(3)]  # all have mileage
        mock_eagle.fetch_all.return_value = iter([page])

        scraper.run(max_ads=None, depth="all", checkpoint_every=100)
        # depth=all overrides mileage presence
        assert mock_detail.fetch.call_count == 3

    def test_run_invalid_depth_raises(self, scraper):
        with pytest.raises(ValueError, match="Invalid depth"):
            scraper.run(depth="halfway")

    def test_run_empty_phase1_returns_empty_csv(self, scraper, mock_eagle, mock_detail):
        mock_eagle.fetch_all.return_value = iter([])

        path = scraper.run(max_ads=None, depth="missing")
        assert path.exists()
        assert "empty" in path.name
        mock_detail.fetch.assert_not_called()

    def test_run_propagates_eagle_auth_error(self, scraper, mock_eagle):
        mock_eagle.fetch_all.side_effect = EagleAuthError("403 forbidden")
        with pytest.raises(EagleAuthError):
            scraper.run(max_ads=10, depth="missing")


# ---------------------------------------------------------------------------
# DEPTH_CHOICES constant
# ---------------------------------------------------------------------------

class TestConstants:
    def test_depth_choices(self):
        assert DEPTH_CHOICES == ["none", "missing", "all"]

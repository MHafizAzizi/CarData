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
# Partial cleanup
# ---------------------------------------------------------------------------

class TestCheckpointCleanup:
    def test_phase2_partials_removed_after_final(
        self, scraper, mock_eagle, mock_detail, tmp_path
    ):
        # 5 ads, checkpoint every 2 -> partials at 2 and 4
        page = [_api_ad(i) for i in range(5)]
        mock_eagle.fetch_all.return_value = iter([page])
        scraper.run(max_ads=None, depth="all", checkpoint_every=2)

        partials = list(tmp_path.glob("*phase2_partial*.csv"))
        assert partials == []
        phase1s = list(tmp_path.glob("*phase1*.csv"))
        assert phase1s == []


# ---------------------------------------------------------------------------
# Phase 2 retry pass
# ---------------------------------------------------------------------------

class TestPhase2RetryErrors:
    def test_errors_retried_once(self, scraper, mock_detail):
        # First call -> error, second (retry pass) -> ok
        responses = [
            {"detail_fetch_status": "error", "last_detail_fetched_at": "t1"},
            {
                "detail_fetch_status": "ok",
                "mileage": "55000",
                "last_detail_fetched_at": "t2",
            },
        ]
        mock_detail.fetch.side_effect = responses

        ads = [_api_ad(1)]
        out = scraper._phase2_enrich(ads, "all", checkpoint_every=100, timestamp="t")
        assert mock_detail.fetch.call_count == 2
        assert out[0]["detail_fetch_status"] == "ok"
        assert out[0]["mileage"] == "55000"

    def test_retry_skipped_when_no_errors(self, scraper, mock_detail):
        ads = [_api_ad(1)]
        scraper._phase2_enrich(ads, "all", checkpoint_every=100, timestamp="t")
        # Only the main loop fetch — no retry pass call
        assert mock_detail.fetch.call_count == 1

    def test_retry_skips_ads_without_url(self, scraper, mock_detail):
        # main loop hits the missing-url branch and marks 'error' WITHOUT calling fetch
        ad = _api_ad(1)
        ad.pop("url")
        out = scraper._phase2_enrich([ad], "all", checkpoint_every=100, timestamp="t")
        # Retry pass must skip url-less ads (otherwise it would call fetch(None, ...))
        mock_detail.fetch.assert_not_called()
        assert out[0]["detail_fetch_status"] == "error"


# ---------------------------------------------------------------------------
# Resume capability
# ---------------------------------------------------------------------------

class TestResume:
    def test_find_resume_prefers_phase2_partial(self, scraper, tmp_path):
        (tmp_path / "mudah_eagle_cars_phase1_20260501.csv").write_text("a")
        (tmp_path / "mudah_eagle_cars_phase2_partial_100_20260502.csv").write_text("b")
        path = scraper._find_resume_checkpoint()
        assert path is not None
        assert "phase2_partial" in path.name

    def test_find_resume_falls_back_to_phase1(self, scraper, tmp_path):
        (tmp_path / "mudah_eagle_cars_phase1_20260501.csv").write_text("a")
        path = scraper._find_resume_checkpoint()
        assert path is not None
        assert "phase1" in path.name

    def test_find_resume_returns_none_when_empty(self, scraper):
        assert scraper._find_resume_checkpoint() is None

    def test_find_resume_picks_most_recent(self, scraper, tmp_path):
        import time as _time
        old = tmp_path / "mudah_eagle_cars_phase2_partial_100_20260501.csv"
        new = tmp_path / "mudah_eagle_cars_phase2_partial_200_20260502.csv"
        old.write_text("a")
        _time.sleep(0.01)
        new.write_text("b")
        assert scraper._find_resume_checkpoint() == new

    def test_load_checkpoint_converts_nan_to_none(self, scraper, tmp_path):
        # Round-trip a row with a missing field to confirm NaN -> None
        ads = [_api_ad(1, with_mileage=True), _api_ad(2)]  # ad 2 has no mileage
        path = scraper._write_csv(ads, "ts", suffix="phase2_partial_2")
        loaded = scraper._load_checkpoint(path)
        # The ad without mileage should have None, not NaN
        ad2 = next(a for a in loaded if a["ads_id"] == 2)
        assert ad2.get("mileage") is None

    def test_run_resume_loads_checkpoint_and_skips_phase1(
        self, scraper, mock_eagle, mock_detail, tmp_path
    ):
        # Pre-seed a phase2_partial CSV with one ad-with-mileage and one without
        ads = [_api_ad(1, with_mileage=True), _api_ad(2)]
        scraper._write_csv(ads, "20260501", suffix="phase2_partial_1")

        path = scraper.run(depth="missing", checkpoint_every=100, resume=True)

        # Phase 1 NOT called
        mock_eagle.fetch_all.assert_not_called()
        # Only one ad needed enrichment (ad 2, no mileage)
        assert mock_detail.fetch.call_count == 1
        # Final CSV exists; resume source cleaned up
        assert path.exists()
        assert "final" in path.name
        partials = list(tmp_path.glob("*phase2_partial*.csv"))
        assert partials == []

    def test_run_resume_raises_when_no_checkpoint(self, scraper):
        with pytest.raises(FileNotFoundError, match="No checkpoint CSV"):
            scraper.run(depth="missing", resume=True)


# ---------------------------------------------------------------------------
# Incremental Phase 1 checkpoint
# ---------------------------------------------------------------------------

class TestPhase1IncrementalCheckpoint:
    def test_phase1_csv_written_per_page(self, scraper, mock_eagle, tmp_path):
        # Yield 2 pages; with timestamp, phase1 CSV should be (re)written each time
        page1 = [_api_ad(i) for i in range(5)]
        page2 = [_api_ad(i) for i in range(5, 8)]
        mock_eagle.fetch_all.return_value = iter([page1, page2])

        scraper._phase1_collect(max_ads=None, timestamp="ts")
        # The final state should be one phase1 CSV with all 8 rows
        phase1s = list(tmp_path.glob("*phase1*.csv"))
        assert len(phase1s) == 1
        df = pd.read_csv(phase1s[0])
        assert len(df) == 8

    def test_phase1_no_checkpoint_without_timestamp(self, scraper, mock_eagle, tmp_path):
        page = [_api_ad(i) for i in range(5)]
        mock_eagle.fetch_all.return_value = iter([page])
        scraper._phase1_collect(max_ads=None)  # no timestamp
        assert list(tmp_path.glob("*phase1*.csv")) == []


# ---------------------------------------------------------------------------
# DEPTH_CHOICES constant
# ---------------------------------------------------------------------------

class TestConstants:
    def test_depth_choices(self):
        assert DEPTH_CHOICES == ["none", "missing", "all"]

"""Tests for src/scraper.py — HybridScraper orchestrator (Phase 1 only)."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from scraper import HybridScraper  # noqa: E402
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


@pytest.fixture
def mock_eagle():
    return MagicMock()


@pytest.fixture
def scraper(tmp_path, mock_eagle):
    return HybridScraper(
        category="cars",
        eagle_client=mock_eagle,
        output_dir=tmp_path,
    )


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

class TestInit:
    def test_unknown_category_raises(self, tmp_path, mock_eagle):
        with pytest.raises(ValueError, match="Unknown category"):
            HybridScraper("trucks", mock_eagle, tmp_path)

    def test_creates_output_dir(self, tmp_path, mock_eagle):
        out = tmp_path / "subdir" / "raw"
        HybridScraper("cars", mock_eagle, out)
        assert out.exists()


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
        mock_eagle.fetch_all.assert_called_once_with("cars", max_ads=None, make_id=None, model_id=None)

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
    def test_basic_run(self, scraper, mock_eagle, tmp_path):
        page = [_api_ad(i) for i in range(3)]
        mock_eagle.fetch_all.return_value = iter([page])

        path = scraper.run(max_ads=None)

        assert path.exists()
        assert "final" in path.name
        # Phase-1 checkpoint should be cleaned up after final is written
        phase1_files = list(tmp_path.glob("*phase1*.csv"))
        assert phase1_files == []

    def test_run_max_ads_respected(self, scraper, mock_eagle):
        page = [_api_ad(i) for i in range(50)]
        mock_eagle.fetch_all.return_value = iter([page])

        path = scraper.run(max_ads=10)
        df = pd.read_csv(path)
        assert len(df) == 10

    def test_run_empty_phase1_returns_empty_csv(self, scraper, mock_eagle):
        mock_eagle.fetch_all.return_value = iter([])

        path = scraper.run(max_ads=None)
        assert path.exists()
        assert "empty" in path.name

    def test_run_propagates_eagle_auth_error(self, scraper, mock_eagle):
        mock_eagle.fetch_all.side_effect = EagleAuthError("403 forbidden")
        with pytest.raises(EagleAuthError):
            scraper.run(max_ads=10)


# ---------------------------------------------------------------------------
# Checkpoint cleanup
# ---------------------------------------------------------------------------

class TestCheckpointCleanup:
    def test_phase1_removed_after_final(self, scraper, mock_eagle, tmp_path):
        page = [_api_ad(i) for i in range(5)]
        mock_eagle.fetch_all.return_value = iter([page])
        scraper.run(max_ads=None)

        phase1s = list(tmp_path.glob("*phase1*.csv"))
        assert phase1s == []

    def test_final_csv_exists_after_cleanup(self, scraper, mock_eagle, tmp_path):
        page = [_api_ad(i) for i in range(5)]
        mock_eagle.fetch_all.return_value = iter([page])
        final = scraper.run(max_ads=None)

        assert final.exists()
        finals = list(tmp_path.glob("*final*.csv"))
        assert len(finals) == 1


# ---------------------------------------------------------------------------
# Resume capability
# ---------------------------------------------------------------------------

class TestResume:
    def test_find_resume_returns_phase1_checkpoint(self, scraper, tmp_path):
        (tmp_path / "mudah_eagle_cars_phase1_20260501.csv").write_text("a")
        path = scraper._find_resume_checkpoint()
        assert path is not None
        assert "phase1" in path.name

    def test_find_resume_returns_none_when_empty(self, scraper):
        assert scraper._find_resume_checkpoint() is None

    def test_find_resume_picks_most_recent(self, scraper, tmp_path):
        import time as _time
        old = tmp_path / "mudah_eagle_cars_phase1_20260501.csv"
        new = tmp_path / "mudah_eagle_cars_phase1_20260502.csv"
        old.write_text("a")
        _time.sleep(0.01)
        new.write_text("b")
        assert scraper._find_resume_checkpoint() == new

    def test_load_checkpoint_converts_nan_to_none(self, scraper, tmp_path):
        # Round-trip a row with a missing field to confirm NaN -> None
        ads = [_api_ad(1, with_mileage=True), _api_ad(2)]  # ad 2 has no mileage
        path = scraper._write_csv(ads, "ts", suffix="phase1")
        loaded = scraper._load_checkpoint(path)
        ad2 = next(a for a in loaded if a["ads_id"] == 2)
        assert ad2.get("mileage") is None

    def test_run_resume_loads_checkpoint_and_skips_phase1(
        self, scraper, mock_eagle, tmp_path
    ):
        # Pre-seed a phase1 checkpoint CSV
        ads = [_api_ad(1, with_mileage=True), _api_ad(2)]
        scraper._write_csv(ads, "20260501", suffix="phase1")

        path = scraper.run(resume=True)

        # Phase 1 NOT called
        mock_eagle.fetch_all.assert_not_called()
        assert path.exists()
        assert "final" in path.name
        # Checkpoint CSV cleaned up
        phase1s = list(tmp_path.glob("*phase1*.csv"))
        assert phase1s == []

    def test_run_resume_raises_when_no_checkpoint(self, scraper):
        with pytest.raises(FileNotFoundError, match="No checkpoint CSV"):
            scraper.run(resume=True)


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

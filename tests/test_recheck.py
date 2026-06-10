"""Tests for recheck.py decision logic.

Covers the three pieces whose failure mode is *lying about which listings
sold*: sold/expired inference, the recheck cadence policy, and the
present/absent/INCOMPLETE three-way split in the API recheck — including the
false-sold guard (a listing absent from the sweep must NOT be marked
unavailable unless its make's pagination completed cleanly).

The API sweep itself (_sweep_active_ids) is stubbed: these tests pin the
decision logic downstream of it, not the HTTP loop.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

import recheck  # noqa: E402
from recheck import (  # noqa: E402
    _infer_sold,
    recheck_category_api,
    select_due_rows,
    should_recheck,
    status_from_detected,
)

NOW = datetime(2026, 6, 10, 12, 0, 0)


# ---------------------------------------------------------------------------
# _infer_sold
# ---------------------------------------------------------------------------

class TestInferSold:
    def test_disappeared_before_expiry_is_likely_sold(self):
        assert _infer_sold("2026-07-01 00:00:00", NOW) == "likely_sold"

    def test_disappeared_after_expiry_is_likely_expired(self):
        assert _infer_sold("2026-05-01 00:00:00", NOW) == "likely_expired"

    def test_disappeared_exactly_at_expiry_is_likely_expired(self):
        assert _infer_sold("2026-06-10 12:00:00", NOW) == "likely_expired"

    def test_no_expiry_is_unknown(self):
        assert _infer_sold(None, NOW) == "unknown"
        assert _infer_sold("", NOW) == "unknown"

    def test_malformed_expiry_is_unknown(self):
        assert _infer_sold("not-a-date", NOW) == "unknown"

    def test_iso_t_separator_also_accepted(self):
        assert _infer_sold("2026-07-01T00:00:00", NOW) == "likely_sold"


# ---------------------------------------------------------------------------
# should_recheck — cadence policy
# ---------------------------------------------------------------------------

def _row(*, first_seen=None, last_checked=None, status="available"):
    fmt = "%Y-%m-%d %H:%M:%S"
    return {
        "first_seen_at": first_seen.strftime(fmt) if first_seen else None,
        "last_checked_at": last_checked.strftime(fmt) if last_checked else None,
        "availability_status": status,
    }


class TestShouldRecheckCadence:
    def test_never_checked_is_always_due(self):
        assert should_recheck(_row(first_seen=NOW - timedelta(days=100)), NOW)

    def test_unavailable_over_14_days_is_graveyard(self):
        row = _row(
            first_seen=NOW - timedelta(days=60),
            last_checked=NOW - timedelta(days=15),
            status="unavailable",
        )
        assert not should_recheck(row, NOW)

    def test_unavailable_within_14_days_still_checked(self):
        row = _row(
            first_seen=NOW - timedelta(days=60),
            last_checked=NOW - timedelta(days=13),
            status="unavailable",
        )
        assert should_recheck(row, NOW)

    def test_fresh_listing_daily_cadence(self):
        # age <= 7d: due after 20h, not before
        first_seen = NOW - timedelta(days=3)
        assert not should_recheck(
            _row(first_seen=first_seen, last_checked=NOW - timedelta(hours=19)), NOW
        )
        assert should_recheck(
            _row(first_seen=first_seen, last_checked=NOW - timedelta(hours=21)), NOW
        )

    def test_mid_age_listing_3_day_cadence(self):
        # 7d < age <= 30d: due after 3 days
        first_seen = NOW - timedelta(days=20)
        assert not should_recheck(
            _row(first_seen=first_seen, last_checked=NOW - timedelta(days=2)), NOW
        )
        assert should_recheck(
            _row(first_seen=first_seen, last_checked=NOW - timedelta(days=3)), NOW
        )

    def test_old_listing_weekly_cadence(self):
        # age > 30d: due after 7 days
        first_seen = NOW - timedelta(days=90)
        assert not should_recheck(
            _row(first_seen=first_seen, last_checked=NOW - timedelta(days=6)), NOW
        )
        assert should_recheck(
            _row(first_seen=first_seen, last_checked=NOW - timedelta(days=7)), NOW
        )


# ---------------------------------------------------------------------------
# status_from_detected — transient outcomes must not lie
# ---------------------------------------------------------------------------

class TestStatusFromDetected:
    def test_available(self):
        assert status_from_detected("available", None) == "available"

    def test_removed_and_soft_404_are_unavailable(self):
        assert status_from_detected("removed", "available") == "unavailable"
        assert status_from_detected("soft_404", "available") == "unavailable"

    def test_blocked_preserves_previous(self):
        assert status_from_detected("blocked", "available") == "available"
        assert status_from_detected("blocked", "unavailable") == "unavailable"

    def test_transient_with_no_previous_is_unknown(self):
        assert status_from_detected("transient", None) == "unknown"


# ---------------------------------------------------------------------------
# DB-backed tests: select_due_rows + recheck_category_api
# ---------------------------------------------------------------------------

@pytest.fixture
def cars_db(tmp_path, monkeypatch):
    """Real cars schema in a tmp file; recheck's module-level db hooks patched."""
    from db import connect as db_connect

    db_file = tmp_path / "cardata_cars.db"
    conn = db_connect("cars", path=db_file, init=True)

    monkeypatch.setattr(recheck, "db_path_for", lambda category: db_file)
    monkeypatch.setattr(
        recheck, "connect", lambda category, **kw: db_connect("cars", path=db_file, init=True)
    )
    yield conn
    conn.close()


def _seed(conn, ads_id, *, make="Toyota", url="default",
          status="available", ad_expiry=None, last_checked=None):
    if url == "default":
        url = f"https://www.mudah.my/a-{ads_id}.htm"  # url column is UNIQUE
    conn.execute(
        "INSERT INTO listings (ads_id, url, make, availability_status, ad_expiry, "
        "first_seen_at, last_checked_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ads_id, url, make, status, ad_expiry, "2026-05-01 00:00:00", last_checked),
    )


class TestSelectDueRows:
    def test_rows_without_url_are_excluded(self, cars_db):
        _seed(cars_db, 1)
        _seed(cars_db, 2, url=None)
        cars_db.execute("UPDATE listings SET url='' WHERE ads_id=1")
        _seed(cars_db, 3)
        due = select_due_rows(cars_db, force_all=True, limit=None)
        assert [r["ads_id"] for r in due] == [3]

    def test_force_all_bypasses_cadence(self, cars_db):
        # checked 1 hour ago — not due under any cadence tier
        an_hour_ago = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        _seed(cars_db, 1, last_checked=an_hour_ago)
        assert select_due_rows(cars_db, force_all=False, limit=None) == []
        assert len(select_due_rows(cars_db, force_all=True, limit=None)) == 1

    def test_limit_caps_result(self, cars_db):
        for i in range(5):
            _seed(cars_db, i + 1)
        assert len(select_due_rows(cars_db, force_all=True, limit=3)) == 3

    def test_make_and_ad_expiry_columns_present_in_rows(self, cars_db):
        # the API recheck's false-sold guard and sold inference both need these
        _seed(cars_db, 1, make="Perodua", ad_expiry="2026-07-01 00:00:00")
        row = select_due_rows(cars_db, force_all=True, limit=None)[0]
        assert row["make"] == "Perodua"
        assert row["ad_expiry"] == "2026-07-01 00:00:00"


class TestRecheckCategoryApiSplit:
    """Present → available; absent+completed → unavailable (+inference);
    absent+incomplete/unknown make → untouched."""

    @pytest.fixture
    def run_api_recheck(self, cars_db, monkeypatch):
        def _run(active_ids, completed_makes):
            monkeypatch.setattr(
                recheck, "_sweep_active_ids",
                lambda client, category, **kw: (active_ids, completed_makes),
            )
            recheck_category_api(
                "cars", client=None, limit=None, force_all=True, dry_run=False,
            )
        return _run

    def _status(self, conn, ads_id):
        return conn.execute(
            "SELECT availability_status, sold_inference, last_checked_at "
            "FROM listings WHERE ads_id=?", (ads_id,)
        ).fetchone()

    def test_present_in_sweep_is_available(self, cars_db, run_api_recheck):
        _seed(cars_db, 1, status="unknown")
        run_api_recheck({1}, {"toyota"})
        row = self._status(cars_db, 1)
        assert row["availability_status"] == "available"
        assert row["last_checked_at"] is not None

    def test_absent_with_completed_make_is_unavailable_likely_sold(
        self, cars_db, run_api_recheck
    ):
        future = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        _seed(cars_db, 1, ad_expiry=future)
        run_api_recheck(set(), {"toyota"})
        row = self._status(cars_db, 1)
        assert row["availability_status"] == "unavailable"
        assert row["sold_inference"] == "likely_sold"

    def test_absent_with_completed_make_past_expiry_is_likely_expired(
        self, cars_db, run_api_recheck
    ):
        _seed(cars_db, 1, ad_expiry="2026-01-01 00:00:00")
        run_api_recheck(set(), {"toyota"})
        row = self._status(cars_db, 1)
        assert row["availability_status"] == "unavailable"
        assert row["sold_inference"] == "likely_expired"

    def test_absent_without_expiry_is_unknown_inference(self, cars_db, run_api_recheck):
        _seed(cars_db, 1, ad_expiry=None)
        run_api_recheck(set(), {"toyota"})
        row = self._status(cars_db, 1)
        assert row["availability_status"] == "unavailable"
        assert row["sold_inference"] == "unknown"

    def test_false_sold_guard_incomplete_make_is_skipped(self, cars_db, run_api_recheck):
        """The core guard: absent listing of an INCOMPLETE make keeps its
        status and last_checked_at so the next run retries it."""
        _seed(cars_db, 1, make="Honda", status="available")
        run_api_recheck(set(), {"toyota"})  # honda did not complete
        row = self._status(cars_db, 1)
        assert row["availability_status"] == "available"
        assert row["sold_inference"] is None
        assert row["last_checked_at"] is None  # untouched → retried next run

    def test_null_make_is_skipped(self, cars_db, run_api_recheck):
        _seed(cars_db, 1, make=None, status="available")
        run_api_recheck(set(), {"toyota"})
        assert self._status(cars_db, 1)["availability_status"] == "available"

    def test_make_matching_is_case_insensitive(self, cars_db, run_api_recheck):
        _seed(cars_db, 1, make="TOYOTA")
        run_api_recheck(set(), {"toyota"})
        assert self._status(cars_db, 1)["availability_status"] == "unavailable"

    def test_audit_rows_written_for_decided_listings_only(
        self, cars_db, run_api_recheck
    ):
        _seed(cars_db, 1)                              # present → decided
        _seed(cars_db, 2)                              # absent+completed → decided
        _seed(cars_db, 3, make="Honda")                # absent+incomplete → skipped
        run_api_recheck({1}, {"toyota"})
        audited = {
            r["ads_id"]
            for r in cars_db.execute("SELECT ads_id FROM availability_checks")
        }
        assert audited == {1, 2}

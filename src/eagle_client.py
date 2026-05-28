"""Wrapper for Mudah's internal EagleSearch JSON API.

Endpoint discovered via JS bundle inspection:
    GET https://search.mudah.my/v1/search?category=<id>&type=sell&from=<offset>&limit=<n>

Category IDs:
    cars        = 1020
    motorcycles = 1040

Empirical caps (probed):
    - limit > 200 silently truncates to ~24, so MAX_LIMIT = 200
    - from >= 10_000 returns empty data (depth cap matches HTML 250-page limit)

Public API:
    EagleClient.fetch_page(category, offset, limit) -> (list[dict], meta_dict)
    EagleClient.fetch_all(category, max_ads=None)   -> iterator of page lists

Each ad dict is already normalized: API field names mapped to DB column names
via _normalize_ad(). Fields not in the schema are dropped.

Throttling:
    Owns its own threading.Lock + last-request timestamp, separate from
    MudahClient. The 0.5-1s API throttle does not block the 3-4s HTML throttle
    (they run in parallel during a hybrid scraper run).

Errors:
    EagleAuthError  - 401/403 (signal to caller to fall back to HTML-only)
    EagleAPIError   - non-2xx after retries, malformed JSON, no results at offset=0
"""

import json
import logging
import random
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import cloudscraper
import requests
from fake_useragent import UserAgent
from requests.exceptions import RequestException

from mudah_client import (
    RATE_LIMIT_DEFAULT_WAIT,
    RATE_LIMIT_MAX_WAIT,
    parse_retry_after,
)


# ---------------------------------------------------------------------------
# Constants — discovered via probe_api scripts
# ---------------------------------------------------------------------------

BASE_URL = "https://search.mudah.my/v1/search"

CATEGORY_IDS: Dict[str, int] = {
    "cars": 1020,
    "motorcycles": 1040,
}

# Per-category API filter parameter names. Cars use the bare `make_id`/
# `model_id`; motorcycles use the prefixed `motorcycle_make_id`/`motorcycle_model_id`
# (verified empirically — passing `make_id` to category=1040 returns 0 results).
_FILTER_PARAM_NAMES: Dict[str, Dict[str, str]] = {
    "cars":        {"make_id": "make_id",            "model_id": "model_id"},
    "motorcycles": {"make_id": "motorcycle_make_id", "model_id": "motorcycle_model_id"},
}

MAX_LIMIT = 200       # Server hard cap; >200 silently truncates to ~24
MAX_OFFSET = 10_000   # Empirical depth cap; offset >= this returns empty


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class EagleAPIError(RuntimeError):
    """Generic EagleSearch failure (non-2xx after retries, malformed payload)."""


class EagleAuthError(EagleAPIError):
    """API returned 401/403. Caller should fall back to HTML-only mode."""


# ---------------------------------------------------------------------------
# Field-name normalization (pure function; testable without HTTP)
# ---------------------------------------------------------------------------

# Direct rename mapping shared by both categories
_RENAME_SHARED: Dict[str, str] = {
    "subject": "subject",
    "price": "price",
    "old_price": "old_price",
    "manufactured_year": "manufactured_date",
    "name": "seller_name",
    "company_ad": "company_ad",
    "condition_name": "condition",
    "region_name": "region",
    "subarea_name": "subregion",
    "date": "published",
    "adview_url": "url",
    "store_verified": "store_verified",
    "year_verified": "year_verified",
    "media_count": "media_count",
    "bundle": "bundle",
    "ad_expiry": "ad_expiry",
}

_RENAME_CARS: Dict[str, str] = {
    "make_name": "make",
    "model_name": "model",
    "car_type_name": "car_type",
    "transmission_name": "transmission",
    "fueltype": "fuel_type",
    "engine_capacity": "engine_capacity",
    "car_loan_eligible": "car_loan_eligible",
    "car_loan_payment": "car_loan_payment",
    "car_loan_tenure": "car_loan_tenure",
    "has_car_grant": "has_car_grant",
}

_RENAME_MOTORCYCLES: Dict[str, str] = {
    "motorcycle_make_name": "motorcycle_make",
    "motorcycle_model_name": "motorcycle_model",
}


def _format_mileage_bucket(raw: Any) -> Optional[str]:
    """Convert API mileage object {'gte': 50000, 'lte': 59999} to '50000-59999'.

    Returns None when input is missing, malformed, or empty.
    """
    if not isinstance(raw, dict):
        return None
    gte = raw.get("gte")
    lte = raw.get("lte")
    if gte is None and lte is None:
        return None
    return f"{gte if gte is not None else ''}-{lte if lte is not None else ''}"


def _coerce_bool_int(val: Any) -> Optional[int]:
    """Bool-like values -> 0/1 INTEGER. None passes through."""
    if val is None:
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, (int, float)):
        return int(bool(val))
    if isinstance(val, str):
        low = val.strip().lower()
        if low in ("true", "1", "yes"):
            return 1
        if low in ("false", "0", "no", ""):
            return 0
    return None


def _normalize_ad(raw_attrs: Dict[str, Any], category: str) -> Dict[str, Any]:
    """Map raw EagleSearch `attributes` dict to a DB-ready record.

    Pure function: no side effects, no I/O. Tested directly with fixture dicts.

    - Renames API keys to DB column names per plan tables.
    - Drops keys not in the schema (list_id, *_id refs, rank, etc.).
    - Coerces mileage object to mileage_bucket string.
    - Coerces year_verified, has_car_grant, car_loan_eligible to int.
    """
    out: Dict[str, Any] = {}

    # ads_id is the canonical primary key.
    # API uses `list_id` for the URL-embedded ID we treat as ads_id;
    # API's `ad_id` is a separate internal system ID we don't track.
    if "list_id" in raw_attrs:
        out["ads_id"] = int(raw_attrs["list_id"])

    # Shared renames
    for src, dst in _RENAME_SHARED.items():
        if src in raw_attrs:
            out[dst] = raw_attrs[src]

    # Category-specific renames
    if category == "cars":
        for src, dst in _RENAME_CARS.items():
            if src in raw_attrs:
                out[dst] = raw_attrs[src]
    elif category == "motorcycles":
        for src, dst in _RENAME_MOTORCYCLES.items():
            if src in raw_attrs:
                out[dst] = raw_attrs[src]
    else:
        raise ValueError(f"Unknown category: {category!r}")

    # Mileage bucket
    if "mileage" in raw_attrs:
        bucket = _format_mileage_bucket(raw_attrs["mileage"])
        if bucket is not None:
            out["mileage_bucket"] = bucket

    # Bool-int coercion for flag fields
    for flag in ("year_verified", "has_car_grant", "car_loan_eligible", "company_ad"):
        if flag in out:
            out[flag] = _coerce_bool_int(out[flag])

    return out


# ---------------------------------------------------------------------------
# EagleClient
# ---------------------------------------------------------------------------

class EagleClient:
    """HTTP client for EagleSearch API.

    - Lighter throttle than MudahClient (0.5-1s vs 3-4s) since it's a JSON API.
    - Same fixed retry backoff schedule (2s -> 3s -> 5s).
    - Owns its own lock so it does not block HTML scraping.
    """

    DEFAULT_RETRY_WAITS: Tuple[int, ...] = (2, 3, 5)
    DEFAULT_REQUEST_INTERVAL: Tuple[float, float] = (0.5, 1.0)

    def __init__(
        self,
        max_retries: int = 3,
        retry_waits: Tuple[int, ...] = DEFAULT_RETRY_WAITS,
        request_interval: Tuple[float, float] = DEFAULT_REQUEST_INTERVAL,
        timeout: float = 15.0,
    ) -> None:
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        self.max_retries = max_retries
        self.retry_waits = retry_waits
        self.request_interval = request_interval
        self.timeout = timeout
        self.ua = UserAgent(platforms=["pc"])

        self._request_lock = threading.Lock()
        self._last_request_time = 0.0

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        return {
            "User-Agent": self.ua.random,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Origin": "https://www.mudah.my",
            "Referer": "https://www.mudah.my/",
            "DNT": "1",
            "Connection": "keep-alive",
        }

    def _throttle(self) -> None:
        """Block until at least request_interval has passed since last request."""
        with self._request_lock:
            target_gap = random.uniform(*self.request_interval)
            elapsed = time.monotonic() - self._last_request_time
            if elapsed < target_gap:
                time.sleep(target_gap - elapsed)
            self._last_request_time = time.monotonic()

    def _get(self, params: Dict[str, Any]) -> requests.Response:
        """GET BASE_URL with params, throttle + retry. Raises EagleAuthError on 401/403."""
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                if attempt == 0:
                    self._throttle()
                else:
                    wait = self.retry_waits[min(attempt - 1, len(self.retry_waits) - 1)]
                    time.sleep(wait)

                resp = self.scraper.get(
                    BASE_URL,
                    params=params,
                    headers=self._headers(),
                    timeout=self.timeout,
                )

                # Auth errors -> dedicated exception so caller can fall back
                if resp.status_code in (401, 403):
                    raise EagleAuthError(
                        f"EagleSearch returned {resp.status_code}; auth required or blocked"
                    )

                # Rate-limited: honor Retry-After if present, else default back-off
                if resp.status_code == 429 and attempt < self.max_retries:
                    wait = parse_retry_after(resp.headers.get("Retry-After"))
                    if wait is None:
                        wait = RATE_LIMIT_DEFAULT_WAIT
                    wait = min(wait, RATE_LIMIT_MAX_WAIT)
                    logging.warning(
                        f"EagleSearch returned 429; backing off {wait:.1f}s "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp

            except EagleAuthError:
                # Don't retry auth failures — they are permanent within a session
                raise

            except RequestException as e:
                last_error = e
                if attempt < self.max_retries:
                    logging.warning(
                        f"EagleSearch request failed ({params}). "
                        f"Retrying... ({attempt + 1}/{self.max_retries})"
                    )
                else:
                    logging.error(f"EagleSearch max retries reached. Error: {e}")

        assert last_error is not None
        raise EagleAPIError(f"EagleSearch failed after {self.max_retries} retries: {last_error}")

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def fetch_page(
        self,
        category: str,
        offset: int = 0,
        limit: int = MAX_LIMIT,
        make_id: Optional[str] = None,
        model_id: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Fetch one page of normalized ads.

        Args:
            make_id:  Optional Mudah make ID to filter results (e.g. "6" for Toyota)
            model_id: Optional Mudah model ID to filter results (e.g. "1702" for Vios)

        Returns:
            (ads, meta) where:
                ads  = list of dicts with DB column names (normalized)
                meta = raw meta block from API (total-results, took, etc.)

        Raises:
            EagleAuthError  on 401/403
            EagleAPIError   on other HTTP errors after retries, or malformed JSON
            ValueError      on unknown category
        """
        if category not in CATEGORY_IDS:
            raise ValueError(
                f"Unknown category: {category!r}. Expected one of {list(CATEGORY_IDS)}."
            )
        if limit > MAX_LIMIT:
            logging.warning(
                f"Requested limit {limit} > MAX_LIMIT {MAX_LIMIT}; "
                f"clamping (server would silently truncate to ~24)"
            )
            limit = MAX_LIMIT

        params = {
            "category": CATEGORY_IDS[category],
            "type": "sell",
            "from": offset,
            "limit": limit,
        }
        # Cars use bare make_id/model_id; motorcycles use the motorcycle_ prefix.
        filter_keys = _FILTER_PARAM_NAMES[category]
        if make_id:
            params[filter_keys["make_id"]] = make_id
        if model_id:
            params[filter_keys["model_id"]] = model_id

        resp = self._get(params)

        try:
            payload = resp.json()
        except (json.JSONDecodeError, ValueError) as e:
            snippet = resp.text[:500]
            logging.error(f"EagleSearch returned non-JSON body: {snippet!r}")
            raise EagleAPIError(f"EagleSearch returned non-JSON: {e}") from e

        meta = payload.get("meta", {}) or {}
        raw_data = payload.get("data", []) or []

        normalized: List[Dict[str, Any]] = []
        for entry in raw_data:
            attrs = entry.get("attributes") if isinstance(entry, dict) else None
            if not isinstance(attrs, dict):
                continue
            try:
                normalized.append(_normalize_ad(attrs, category))
            except (ValueError, TypeError, KeyError) as e:
                logging.warning(
                    f"Skipping malformed ad in EagleSearch response: {e} "
                    f"(attrs keys: {list(attrs)[:10]}...)"
                )

        return normalized, meta

    def fetch_all(
        self,
        category: str,
        max_ads: Optional[int] = None,
        make_id: Optional[str] = None,
        model_id: Optional[str] = None,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Yield pages of normalized ads until depth cap, empty batch, or max_ads.

        Each yielded item is one page (list of dicts), not a flat stream.
        Caller can checkpoint per page.

        Args:
            make_id:  Optional Mudah make ID to narrow results to one make.
            model_id: Optional Mudah model ID to narrow results to one model.

        Stops when:
            - empty batch returned (normal end of pagination)
            - offset >= MAX_OFFSET (depth cap)
            - len(yielded) >= max_ads (caller-imposed cap)

        Raises:
            EagleAPIError if first call (offset=0) returns empty - signals
                          wrong filter or API outage.
        """
        offset = 0
        yielded_count = 0

        while True:
            if offset >= MAX_OFFSET:
                logging.info(
                    f"[{category}] Reached EagleSearch depth cap "
                    f"(offset={offset}, MAX_OFFSET={MAX_OFFSET}). Stopping."
                )
                return

            remaining = None if max_ads is None else max_ads - yielded_count
            if remaining is not None and remaining <= 0:
                return
            limit = MAX_LIMIT if remaining is None else min(MAX_LIMIT, remaining)

            ads, meta = self.fetch_page(
                category, offset=offset, limit=limit,
                make_id=make_id, model_id=model_id,
            )
            logging.info(
                f"[{category}] offset={offset} got={len(ads)} "
                f"(total-results={meta.get('total-results')}, took={meta.get('took')}ms)"
            )

            if not ads:
                if offset == 0:
                    raise EagleAPIError(
                        f"[{category}] EagleSearch returned no results at offset=0; "
                        f"check category/type filters"
                    )
                # Normal end of pagination
                return

            yield ads
            yielded_count += len(ads)
            offset += len(ads)

"""HTTP client for carbase.my.

Mirrors mudah_client.MudahClient: a cloudscraper session, a global throttle so
requests are spaced out, fixed-schedule retry backoff, and Retry-After/429
honoring (reusing mudah_client's parse_retry_after + RATE_LIMIT_* constants, the
same way eagle_client does).

carbase recon (2026-06-17) showed no Cloudflare challenge and ~0.5s/page, but
sustained-load behavior was untested, so the default pacing is deliberately
polite (~1.5-2.0s between requests).
"""

import logging
import random
import threading
import time
from typing import Dict, Optional, Tuple

import cloudscraper
from requests.exceptions import RequestException

from mudah_client import (
    parse_retry_after,
    RATE_LIMIT_DEFAULT_WAIT,
    RATE_LIMIT_MAX_WAIT,
)

BASE_URL = "https://www.carbase.my"


class CarbaseClient:
    """Throttled, retrying HTTP client for carbase.my."""

    DEFAULT_RETRY_WAITS = (2, 3, 5)
    DEFAULT_REQUEST_INTERVAL = (1.5, 2.0)

    def __init__(
        self,
        max_retries: int = 4,
        retry_waits: Tuple[int, ...] = DEFAULT_RETRY_WAITS,
        request_interval: Tuple[float, float] = DEFAULT_REQUEST_INTERVAL,
        timeout: float = 20.0,
    ) -> None:
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        self.max_retries = max_retries
        self.retry_waits = retry_waits
        self.request_interval = request_interval
        self.timeout = timeout
        self._request_lock = threading.Lock()
        self._last_request_time = 0.0

    def _throttle(self) -> None:
        with self._request_lock:
            target_gap = random.uniform(*self.request_interval)
            elapsed = time.monotonic() - self._last_request_time
            if elapsed < target_gap:
                time.sleep(target_gap - elapsed)
            self._last_request_time = time.monotonic()

    def _url(self, path: str) -> str:
        return path if path.startswith("http") else f"{BASE_URL}{path}"

    def get_status(self, path: str) -> Tuple[Optional[int], Optional[str]]:
        """GET a carbase path with throttle + retry. Returns (status_code, text).

        Returns (None, None) only if every attempt raised (timeout/connection).
        HTTP error statuses (403/404/5xx) are returned, not raised, so the caller
        can log + skip a dead subtree without aborting the crawl. 429 is retried
        with Retry-After backoff inside the loop.
        """
        url = self._url(path)
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                if attempt == 0:
                    self._throttle()
                else:
                    time.sleep(self.retry_waits[min(attempt - 1, len(self.retry_waits) - 1)])

                resp = self.scraper.get(url, timeout=self.timeout)

                if resp.status_code == 429 and attempt < self.max_retries:
                    wait = parse_retry_after(resp.headers.get("Retry-After"))
                    wait = min(wait if wait is not None else RATE_LIMIT_DEFAULT_WAIT,
                               RATE_LIMIT_MAX_WAIT)
                    logging.warning(f"429 for {url}; backing off {wait:.1f}s "
                                    f"(attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(wait)
                    continue

                return resp.status_code, resp.text

            except RequestException as e:
                last_error = e
                if attempt < self.max_retries:
                    logging.warning(f"Request failed for {url}. Retrying... "
                                    f"({attempt + 1}/{self.max_retries})")
                else:
                    logging.error(f"Max retries reached for {url}. Error: {e}")

        logging.warning(f"Probe failed for {url}: {last_error}")
        return None, None

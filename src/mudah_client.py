"""Shared HTTP client for Mudah.my requests.

Provides a single class, MudahClient, that owns:
- a cloudscraper session,
- a global rate-limit lock so all requests are spaced 3-4s apart,
- a fixed-schedule retry backoff (2s -> 3s -> 5s).

Both the scraper (script.py) and the availability re-checker (recheck.py) should
use this client so they share the same throttle and never hammer Mudah even when
run concurrently within the same process.
"""

import random
import threading
import time
import logging
from typing import Dict, Optional, Tuple

import cloudscraper
import requests
from fake_useragent import UserAgent
from requests.exceptions import RequestException


class MudahClient:
    """HTTP client with rate limiting + retry backoff for Mudah.my."""

    DEFAULT_RETRY_WAITS = (2, 3, 5)
    DEFAULT_REQUEST_INTERVAL = (3.0, 4.0)

    def __init__(
        self,
        max_retries: int = 3,
        retry_waits: Tuple[int, ...] = DEFAULT_RETRY_WAITS,
        request_interval: Tuple[float, float] = DEFAULT_REQUEST_INTERVAL,
        timeout: float = 30.0,
    ) -> None:
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )
        self.max_retries = max_retries
        self.retry_waits = retry_waits
        self.request_interval = request_interval
        self.timeout = timeout
        self.ua = UserAgent()

        self._request_lock = threading.Lock()
        self._last_request_time = 0.0

    def _random_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }

    def _throttle(self) -> None:
        """Block until at least request_interval has passed since the last request.
        Holding the lock during sleep serializes pacing across worker threads."""
        with self._request_lock:
            target_gap = random.uniform(*self.request_interval)
            elapsed = time.monotonic() - self._last_request_time
            if elapsed < target_gap:
                time.sleep(target_gap - elapsed)
            self._last_request_time = time.monotonic()

    def get(self, url: str, *, raise_for_status: bool = True) -> requests.Response:
        """GET a URL with throttling + fixed-schedule retry backoff.

        Raises the last RequestException if all retries fail.
        """
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                if attempt == 0:
                    self._throttle()
                else:
                    wait = self.retry_waits[min(attempt - 1, len(self.retry_waits) - 1)]
                    time.sleep(wait)

                response = self.scraper.get(url, headers=self._random_headers(), timeout=self.timeout)
                if raise_for_status:
                    response.raise_for_status()
                return response

            except RequestException as e:
                last_error = e
                if attempt < self.max_retries:
                    logging.warning(f"Request failed for {url}. Retrying... ({attempt + 1}/{self.max_retries})")
                else:
                    logging.error(f"Max retries reached for {url}. Error: {e}")

        assert last_error is not None
        raise last_error

    def get_status(self, url: str) -> Tuple[Optional[int], Optional[str]]:
        """Probe a URL and return (status_code, body_text) without raising on HTTP errors.

        Returns (None, None) if the request never produced a response (timeout, connection error, etc.).
        Used by the availability checker so 404/410/403 are classified rather than thrown.
        """
        try:
            response = self.get(url, raise_for_status=False)
            return response.status_code, response.text
        except RequestException as e:
            logging.warning(f"Probe failed for {url}: {e}")
            return None, None

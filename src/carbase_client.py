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
from typing import Optional, Tuple

from requests.exceptions import RequestException

from mudah_client import ThrottledSession

BASE_URL = "https://www.carbase.my"


class CarbaseClient(ThrottledSession):
    """Throttled, retrying HTTP client for carbase.my (no per-request headers)."""

    DEFAULT_RETRY_WAITS = (2, 3, 5)
    DEFAULT_REQUEST_INTERVAL = (1.5, 2.0)

    def __init__(
        self,
        max_retries: int = 4,
        retry_waits: Tuple[int, ...] = DEFAULT_RETRY_WAITS,
        request_interval: Tuple[float, float] = DEFAULT_REQUEST_INTERVAL,
        timeout: float = 20.0,
    ) -> None:
        super().__init__(
            max_retries=max_retries,
            retry_waits=retry_waits,
            request_interval=request_interval,
            timeout=timeout,
        )

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
        try:
            resp = self._request(url)
            return resp.status_code, resp.text
        except RequestException as e:
            logging.warning(f"Probe failed for {url}: {e}")
            return None, None

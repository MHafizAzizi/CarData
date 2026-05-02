"""HTML detail-page parser for Mudah.my listings.

Carved from src/script.py's MudahScraper.get_car_info(). Extracts only the
fields that EagleSearch API does NOT return:

- body (full seller description)
- mileage (exact, not the API's bucket)
- chassis specs from mcdParams (kw, torque, length, wheelbase, ...)
- car classification fields not always in API (family, variant, series, ...)

The API plus this detail parser together produce a complete row.

Public API:
    DetailClient(mudah_client).fetch(url, ads_id) -> dict

`fetch()` never raises — it returns a status-stamped dict so the orchestrator
loop stays simple. Use `detail_fetch_status` field ('ok' / 'error') to branch.

Module-level pure functions are exposed for testing without HTTP:
    _clean_body, _parse_published, _parse_mcdparams, _extract_detail_fields
"""

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup as bs
from requests.exceptions import RequestException

from mudah_client import MudahClient


# ---------------------------------------------------------------------------
# Field set
# ---------------------------------------------------------------------------

# Fields we expect to populate ONLY from the HTML detail page.
# Order is descriptive (body first), not significant.
DETAIL_FIELDS = (
    "body", "mileage", "kw", "torque", "cc", "comp_ratio",
    "length", "width", "height", "wheelbase", "kerbwt", "fueltk",
    "brake_front", "brake_rear", "suspension_front", "suspension_rear",
    "steering", "tyres_front", "tyres_rear", "wheel_rim_front",
    "wheel_rim_rear", "family", "variant", "series", "style",
    "seat", "country_origin", "engine",
)

_DETAIL_FIELD_SET = frozenset(DETAIL_FIELDS)


# ---------------------------------------------------------------------------
# Pure helpers (carved from script.py — kept identical for behavior parity)
# ---------------------------------------------------------------------------

def _clean_body(text: Optional[str]) -> str:
    """Normalize Mudah's HTML-flavored ad body to plain text."""
    if not text:
        return ""
    # Mudah uses <br>, <br/>, <br /> as line breaks; collapse all to \n
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    # Strip any other stray HTML tags (rare, but defensive)
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def _parse_published(text: str) -> str:
    """Convert Mudah's relative date strings ('Today 15:49', 'Yesterday 10:30',
    '22 Apr 09:15') to absolute 'YYYY-MM-DD HH:MM'."""
    if not text:
        return ""
    today = datetime.now()
    text = text.strip()
    low = text.lower()
    try:
        if low.startswith("today"):
            time_part = text.split(" ", 1)[1] if " " in text else "00:00"
            return f"{today.strftime('%Y-%m-%d')} {time_part}"
        if low.startswith("yesterday"):
            y = today - timedelta(days=1)
            time_part = text.split(" ", 1)[1] if " " in text else "00:00"
            return f"{y.strftime('%Y-%m-%d')} {time_part}"
        try:
            dt = datetime.strptime(f"{today.year} {text}", "%Y %d %b %H:%M")
            if dt > today:
                dt = dt.replace(year=today.year - 1)
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass
        try:
            return datetime.strptime(text, "%d %b %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    except Exception:
        pass
    return text


def _parse_mcdparams(mcd_params: List[Dict]) -> Dict[str, Any]:
    """Flatten Mudah's mcdParams blob into a {param_id: value} dict.

    Differs from script.py's parse_mcdparams: returns a flat dict (not list of
    dicts) and only retains keys that appear in DETAIL_FIELDS — so the caller
    can merge it directly into a single record.
    """
    out: Dict[str, Any] = {}
    for group in mcd_params or []:
        if not isinstance(group, dict):
            continue
        for entry in (group.get("params") or []):
            if not isinstance(entry, dict):
                continue
            key = entry.get("id")
            if not key or key not in _DETAIL_FIELD_SET:
                continue
            # Prefer realValue when present (numeric), fall back to value (display string)
            real = entry.get("realValue")
            display = entry.get("value")
            out[key] = real if real not in (None, "") else display
    return out


def _parse_categoryparams(cat_params: List[Dict]) -> Dict[str, Any]:
    """Flatten attributes.categoryParams into a {id: value} dict, filtered to
    DETAIL_FIELDS only."""
    out: Dict[str, Any] = {}
    for entry in cat_params or []:
        if not isinstance(entry, dict):
            continue
        key = entry.get("id")
        if not key or key not in _DETAIL_FIELD_SET:
            continue
        real = entry.get("realValue")
        display = entry.get("value")
        out[key] = real if real not in (None, "") else display
    return out


def _extract_detail_fields(next_data: Dict, ads_id: int) -> Dict[str, Any]:
    """Pure parser: given parsed __NEXT_DATA__ JSON + ads_id, return a dict
    of DETAIL_FIELDS-keyed values plus 'subject'/'published' if present.

    Returns only fields that have a non-empty value. The caller adds tracking
    metadata (status, timestamp). Raises only on truly malformed input
    (e.g., next_data is not a dict). DetailClient.fetch wraps this and
    swallows everything.
    """
    if not isinstance(next_data, dict):
        raise ValueError("next_data must be a dict")

    props = next_data.get("props") or {}
    ad_block = (
        props.get("initialState", {})
             .get("adDetails", {})
             .get("byID", {})
             .get(str(ads_id), {})
    )
    attributes = ad_block.get("attributes") or {}

    out: Dict[str, Any] = {}

    # Body lives directly on attributes
    body_raw = attributes.get("body")
    if body_raw:
        cleaned = _clean_body(body_raw)
        if cleaned:
            out["body"] = cleaned

    # categoryParams (mileage, family, variant, series, style, seat, ...)
    out.update(_parse_categoryparams(attributes.get("categoryParams") or []))

    # mcdParams (kw, torque, length, brake_*, tyres_*, ...)
    out.update(_parse_mcdparams(attributes.get("mcdParams") or []))

    # Drop empty / whitespace-only values
    return {k: v for k, v in out.items() if v not in (None, "", " ")}


# ---------------------------------------------------------------------------
# DetailClient
# ---------------------------------------------------------------------------

class DetailClient:
    """Fetches and parses one Mudah.my .htm listing page.

    Uses a shared MudahClient so the 3-4s HTML throttle is honored.
    Pass the same MudahClient instance from the orchestrator to ensure
    throttle is shared across phases.

    `fetch()` never raises. The return dict always includes:
        detail_fetch_status   = 'ok' | 'error'
        last_detail_fetched_at = ISO datetime string
    On 'ok', also includes any populated DETAIL_FIELDS keys.
    """

    def __init__(self, client: MudahClient) -> None:
        self.client = client

    def fetch(self, url: str, ads_id: int) -> Dict[str, Any]:
        """Fetch + parse one listing's detail page.

        Always returns a dict (never raises). On error, the dict is just the
        two tracking fields with status='error'.
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            response = self.client.get(url)
        except RequestException as e:
            logging.warning(f"DetailClient: request failed for {url}: {e}")
            return {
                "detail_fetch_status": "error",
                "last_detail_fetched_at": now,
            }
        except Exception as e:
            # Defensive: cloudscraper sometimes raises non-RequestException
            logging.warning(f"DetailClient: unexpected error fetching {url}: {e}")
            return {
                "detail_fetch_status": "error",
                "last_detail_fetched_at": now,
            }

        try:
            soup = bs(response.text, "html.parser")
            script_tag = (
                soup.find("script", id="__NEXT_DATA__")
                or soup.find("script", type="application/json")
            )
            if not script_tag or not script_tag.string:
                logging.warning(f"DetailClient: no __NEXT_DATA__ in {url}")
                return {
                    "detail_fetch_status": "error",
                    "last_detail_fetched_at": now,
                }

            data = json.loads(script_tag.string)
            extracted = _extract_detail_fields(data, ads_id)

        except (json.JSONDecodeError, ValueError, TypeError, KeyError) as e:
            logging.warning(f"DetailClient: parse failed for {url}: {e}")
            return {
                "detail_fetch_status": "error",
                "last_detail_fetched_at": now,
            }

        extracted["detail_fetch_status"] = "ok"
        extracted["last_detail_fetched_at"] = now
        return extracted

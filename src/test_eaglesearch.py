"""Quick smoke test for Mudah's internal EagleSearch API.

Pulls 100 motorcycle + 100 car listings from
    https://search.mudah.my/v1/search

Writes raw JSON to:
    data/raw/eagle_motorcycles_100.json
    data/raw/eagle_cars_100.json

Plus a trimmed CSV-friendly summary so the field shape is easy to eyeball.

Usage:
    python src/test_eaglesearch.py
"""

import json
import logging
import sys
from pathlib import Path

import cloudscraper

_ROOT = Path(__file__).resolve().parent.parent
_OUT_DIR = _ROOT / "data" / "raw"

# Discovered from JS bundle: gateway.mudah.my/_next/static/chunks/...
_BASE = "https://search.mudah.my/v1/search"

# category ids from /v1/categories
_CATEGORY_IDS = {
    "cars": 1020,
    "motorcycles": 1040,
}

# Server caps limit at ~200 per request; pull in batches if needed.
_PAGE_LIMIT = 100


def fetch(category: str, total: int = 100) -> list[dict]:
    """Pull `total` ads for given category via EagleSearch /v1/search."""
    cat_id = _CATEGORY_IDS[category]
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )

    collected: list[dict] = []
    offset = 0
    while len(collected) < total:
        params = {
            "category": cat_id,
            "type": "sell",
            "from": offset,
            "limit": min(_PAGE_LIMIT, total - len(collected)),
        }
        logging.info(f"[{category}] GET {_BASE} params={params}")
        resp = scraper.get(_BASE, params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        batch = payload.get("data", [])
        meta = payload.get("meta", {})
        logging.info(
            f"[{category}] received {len(batch)} ads "
            f"(meta total={meta.get('total-results')}, took={meta.get('took')}ms)"
        )
        if not batch:
            logging.warning(f"[{category}] empty batch at offset={offset}, stopping")
            break
        collected.extend(batch)
        offset += len(batch)

    return collected[:total]


def summarize(ads: list[dict]) -> list[dict]:
    """Flatten ad attributes into a rows-friendly view."""
    rows = []
    for ad in ads:
        a = ad.get("attributes", {})
        mileage = a.get("mileage")
        if isinstance(mileage, dict):
            mileage = f"{mileage.get('gte', '')}-{mileage.get('lte', '')}"
        rows.append({
            "ad_id": a.get("ad_id"),
            "list_id": a.get("list_id"),
            "subject": a.get("subject"),
            "price": a.get("price"),
            "old_price": a.get("old_price"),
            "manufactured_year": a.get("manufactured_year"),
            "mileage": mileage,
            "make": a.get("make_name") or a.get("motorcycle_make_name"),
            "model": a.get("model_name") or a.get("motorcycle_model_name"),
            "condition": a.get("condition_name"),
            "transmission": a.get("transmission_name"),
            "fueltype": a.get("fueltype"),
            "engine_capacity": a.get("engine_capacity"),
            "region": a.get("region_name"),
            "subarea": a.get("subarea_name"),
            "company_ad": a.get("company_ad"),
            "seller": a.get("name"),
            "store_verified": a.get("store_verified"),
            "year_verified": a.get("year_verified"),
            "media_count": a.get("media_count"),
            "date": a.get("date"),
            "adview_url": a.get("adview_url"),
        })
    return rows


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    for category in ("motorcycles", "cars"):
        ads = fetch(category, total=100)
        raw_path = _OUT_DIR / f"eagle_{category}_100.json"
        raw_path.write_text(
            json.dumps(ads, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logging.info(f"[{category}] wrote {raw_path.relative_to(_ROOT)} ({len(ads)} ads)")

        summary = summarize(ads)
        summary_path = _OUT_DIR / f"eagle_{category}_100_summary.json"
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logging.info(f"[{category}] wrote {summary_path.relative_to(_ROOT)}")

        # Field-coverage sanity check
        if summary:
            sample = summary[0]
            populated = sum(1 for v in sample.values() if v not in (None, "", "-"))
            logging.info(
                f"[{category}] sample row populated fields: {populated}/{len(sample)} "
                f"(first ad_id={sample['ad_id']})"
            )


if __name__ == "__main__":
    main()

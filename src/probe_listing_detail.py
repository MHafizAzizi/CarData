"""Probe a single Mudah listing detail page.

Fetches __NEXT_DATA__ and dumps the full `attributes` dict so we can see
whether OEM spec fields (engine_cc, torque, power, dimensions, etc.) are
available without HTML scraping.

Usage:
    python src/probe_listing_detail.py
    python src/probe_listing_detail.py --url https://www.mudah.my/...
    python src/probe_listing_detail.py --id 114983088
"""

import argparse
import json
import re
import sys
from pathlib import Path

import cloudscraper

_DEFAULT_URL = "https://www.mudah.my/2016-toyota-vios-1-5-e-enhanced-a-1-owner-114983088.htm"
_ROOT = Path(__file__).resolve().parent.parent


def fetch(url: str) -> str:
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    resp = scraper.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_next_data(html: str) -> dict:
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        raise RuntimeError("__NEXT_DATA__ not found — Cloudflare stripped HTML or soft_404")
    return json.loads(m.group(1))


def find_ad_attributes(data: dict, ads_id: str) -> dict | None:
    """Navigate props.initialState.adDetails.byID.{ads_id}.attributes"""
    by_id = (
        data.get("props", {})
            .get("initialState", {})
            .get("adDetails", {})
            .get("byID", {})
    )
    ad = by_id.get(ads_id) or by_id.get(str(ads_id))
    if ad:
        return ad.get("attributes")
    # Fallback: return first ad found
    for v in by_id.values():
        if isinstance(v, dict) and "attributes" in v:
            return v["attributes"]
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=_DEFAULT_URL)
    parser.add_argument("--id", dest="ads_id", default=None,
                        help="Override ads_id extraction (default: parse from URL)")
    parser.add_argument("--dump-full", action="store_true",
                        help="Also dump full __NEXT_DATA__ to data/raw/probe_next_data.json")
    args = parser.parse_args()

    # Extract ads_id from URL if not provided
    ads_id = args.ads_id
    if not ads_id:
        m = re.search(r"-(\d+)\.htm", args.url)
        if m:
            ads_id = m.group(1)

    print(f"Fetching: {args.url}")
    html = fetch(args.url)
    print(f"HTML size: {len(html):,} bytes")

    data = extract_next_data(html)
    print(f"__NEXT_DATA__ top-level keys: {list(data.keys())}")

    props = data.get("props", {})
    initial_state = props.get("initialState", {})
    print(f"initialState keys: {list(initial_state.keys())}")

    ad_details = initial_state.get("adDetails", {})
    by_id = ad_details.get("byID", {})
    print(f"adDetails.byID keys: {list(by_id.keys())}")

    attrs = find_ad_attributes(data, ads_id) if ads_id else None
    if attrs is None:
        # Maybe structure differs — show pageProps
        page_props = props.get("pageProps", {})
        print(f"\nFallback — pageProps keys: {list(page_props.keys())}")
        if "adDetails" in page_props:
            attrs = page_props["adDetails"].get("attributes")

    if attrs is None:
        print("\nERROR: could not find attributes dict — dumping full __NEXT_DATA__ structure")
        out = _ROOT / "data" / "raw" / "probe_next_data.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote: {out}")
        sys.exit(1)

    print(f"\n=== attributes ({len(attrs)} keys) ===")
    # Print sorted so spec fields cluster together
    for k in sorted(attrs.keys()):
        v = attrs[k]
        # Truncate long values
        sv = json.dumps(v, ensure_ascii=False)
        if len(sv) > 120:
            sv = sv[:117] + "..."
        print(f"  {k}: {sv}")

    # Highlight OEM spec candidates
    spec_keywords = {"engine", "cc", "torque", "power", "compression", "bore",
                     "dimension", "length", "width", "height", "weight", "wheelbase",
                     "fuel_tank", "brake", "suspension", "seat", "series", "variant",
                     "country", "origin", "type"}
    found = {k: v for k, v in attrs.items()
             if any(kw in k.lower() for kw in spec_keywords)}
    if found:
        print(f"\n=== spec-candidate keys ({len(found)}) ===")
        for k, v in found.items():
            print(f"  {k}: {json.dumps(v, ensure_ascii=False)}")
    else:
        print("\nNo spec-candidate keys found in attributes.")

    if args.dump_full:
        out = _ROOT / "data" / "raw" / "probe_next_data.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nFull __NEXT_DATA__ written: {out}")


if __name__ == "__main__":
    main()

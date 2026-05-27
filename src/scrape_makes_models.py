"""One-shot reference scraper: pull make + model lists from Mudah for both categories.

Also supports a --variants mode that builds a variant/trim vocabulary per car model
by sampling listing subjects from model-specific search pages. Variant info is NOT
available in the EagleSearch API — this is the only way to get it without Phase 2
HTML detail scraping.

Usage:
    # Refresh makes + models (both categories)
    python src/scrape_makes_models.py

    # Build variant vocabulary for specific makes (cars only)
    python src/scrape_makes_models.py --variants --makes toyota honda perodua proton

    # Build for all makes (slow — 1,500+ HTTP requests; use --makes to scope)
    python src/scrape_makes_models.py --variants --all-makes

Outputs (default mode):
    data/reference/cars_makes.json
    data/reference/cars_models.json
    data/reference/motorcycles_makes.json
    data/reference/motorcycles_models.json

Outputs (--variants mode):
    data/reference/cars_variants.json
        {model_slug: {make, model, by_cc: {cc: [tokens]}, _all: [tokens]}}

Run manually whenever the brand list needs refreshing (Mudah adds new makes
roughly once a year). Run --variants once after initial scrape, then re-run
when new models are added.
"""

import argparse
import json
import logging
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import cloudscraper

_ROOT = Path(__file__).resolve().parent.parent
_OUT_DIR = _ROOT / "data" / "reference"

_CATEGORY_URLS = {
    "cars": "https://www.mudah.my/malaysia/cars-for-sale?o=1",
    "motorcycles": "https://www.mudah.my/malaysia/motorcycles-for-sale?o=1",
}

# Anchor each filter at its enclosing object. The structure is uniform:
#   "<name>":{"filter":{...inner config...},"values":[...the data we want...]}
# A single name like "Alfa Romeo" / "Adiva" lets us cheaply detect whether the
# response actually carries the full filterOptions blob (Mudah serves a stripped
# variant a majority of the time).
_MAKE_ANCHORS = {
    "cars": '"make":{',
    "motorcycles": '"motorcycle_make":{',
}
_MODEL_ANCHORS = {
    "cars": '"model":{',
    "motorcycles": '"motorcycle_model":{',
}
_FULL_PAGE_SIGNALS = {
    "cars": "Alfa Romeo",
    "motorcycles": "Adiva",
}


def _parse_values_array_at(txt: str, val_idx: int) -> list | None:
    """Bracket-match a `"values":[ ... ]` block starting at `val_idx`.

    Returns parsed list, or None if the slice fails to parse as JSON.
    """
    arr_start = val_idx + len('"values":')  # points at '['
    depth = 0
    in_str = False
    esc = False
    for i in range(arr_start, len(txt)):
        c = txt[i]
        if esc:
            esc = False
        elif c == "\\":
            esc = True
        elif c == '"':
            in_str = not in_str
        elif not in_str:
            if c == "[":
                depth += 1
            elif c == "]":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(txt[arr_start:i + 1])
                    except json.JSONDecodeError:
                        return None
    return None


def _extract_values_after_filter(txt: str, anchor: str) -> list:
    """Find the filter object opened by `anchor` (e.g. `"make":{`) and return
    the next `"values":[ ... ]` array, bracket-matched.

    Filter objects on Mudah have the shape:
        "<name>":{"filter":{...inner config...},"values":[...data...]}
    The first `"values":[` after the anchor is always the data array.
    """
    key_idx = txt.find(anchor)
    if key_idx < 0:
        raise ValueError(f"anchor {anchor!r} not found")
    val_idx = txt.find('"values":[', key_idx)
    if val_idx < 0:
        raise ValueError(f"'values':[ not found after {anchor!r}")
    parsed = _parse_values_array_at(txt, val_idx)
    if parsed is None:
        raise ValueError(f"could not parse values array after {anchor!r}")
    return parsed


def _fetch_category(category: str, *, max_attempts: int = 8) -> dict:
    """Return {'makes': [...], 'models': {make_slug: [...]} } for one category.

    Mudah serves a stripped HTML variant most of the time (no filterOptions
    blob); retry with a fresh cloudscraper session until we hit the full
    response, detected by presence of a known make name.
    """
    url = _CATEGORY_URLS[category]
    signal = _FULL_PAGE_SIGNALS[category]

    txt = ""
    for attempt in range(1, max_attempts + 1):
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        logging.info(f"[{category}] Fetching {url} (attempt {attempt}/{max_attempts})")
        resp = scraper.get(url)
        resp.raise_for_status()
        txt = resp.text
        if signal in txt:
            break
        logging.warning(
            f"[{category}] response missing {signal!r} signal "
            f"(len={len(txt)}); retrying"
        )
        time.sleep(2 + attempt)
    else:
        raise RuntimeError(
            f"[{category}] could not get full response containing {signal!r} "
            f"after {max_attempts} attempts"
        )

    makes = _extract_values_after_filter(txt, _MAKE_ANCHORS[category])
    logging.info(f"[{category}] makes: {len(makes)}")

    try:
        model_groups = _extract_values_after_filter(txt, _MODEL_ANCHORS[category])
    except ValueError:
        # Motorcycles don't expose model_id filter in eager state -> skip.
        logging.warning(f"[{category}] no model_id filter in page; models will be empty")
        model_groups = []

    # Index groups by parent_id (= make id)
    id_to_slug = {m["id"]: m["slug"] for m in makes}
    models_by_slug: dict = {m["slug"]: [] for m in makes}
    for grp in model_groups:
        slug = id_to_slug.get(grp.get("parent_id"))
        if slug:
            models_by_slug[slug] = grp.get("values", [])

    total_models = sum(len(v) for v in models_by_slug.values())
    logging.info(f"[{category}] total models across all makes: {total_models}")

    return {"makes": makes, "models": models_by_slug}


# ---------------------------------------------------------------------------
# Variant vocabulary builder
# ---------------------------------------------------------------------------

# Noise to strip when extracting the variant token from a listing subject.
# Pattern: {year} {Make} {MODEL} {engine_size} {VARIANT} (A/M) {seller noise}
_YEAR_PAT = re.compile(r"\b(19|20)\d{2}\b")
_ENGINE_SIZE_PAT = re.compile(r"\b\d+\.\d+\b")          # 1.3, 1.5, 2.0 …
_TRANSMISSION_PAT = re.compile(
    r"\(A\)|\(M\)|\bAuto\b|\bManual\b|\bA/T\b|\bM/T\b", re.I
)
_CONDITION_PAT = re.compile(r"\b(used|new|baru)\b", re.I)
_SEPARATOR_NOISE_PAT = re.compile(r"[-/|~].*$")
_TRAILING_NOISE_PAT = re.compile(
    r"\b(for\s*sale|tip\s*?top|good\s*cond\w*|low\s*mileage|full\s*loan|"
    r"ready\s*stock|ready|urgent|service\s*record|warranty|waranty|waranti|"
    r"tiptop|murah|nego|negotiable|facelift|spec|edition|import|"
    r"no\s*accident|accident\s*free|one\s*owner|1\s*owner)\b.*$",
    re.I,
)


def extract_variant_tokens(subject: str, make_name: str, model_name: str) -> str:
    """Strip year, make, model, engine size, transmission, and seller noise from
    a listing subject. The residual is the likely variant/trim descriptor.

    Examples:
        "2020 Toyota ALPHARD 2.5 SC (A) ROOF ACC"  →  "SC"
        "Honda City 1.5 HATCHBACK V (A) 2022"       →  "HATCHBACK V"
        "2010 Perodua MYVI 1.3 EZL (LIMITED EDITION) (A)" → "EZL LIMITED EDITION"
    """
    txt = subject
    txt = _YEAR_PAT.sub("", txt)
    if make_name:
        txt = re.sub(re.escape(make_name), "", txt, flags=re.I)
    if model_name:
        txt = re.sub(re.escape(model_name), "", txt, flags=re.I)
    txt = _ENGINE_SIZE_PAT.sub("", txt)
    txt = _TRANSMISSION_PAT.sub("", txt)
    txt = _CONDITION_PAT.sub("", txt)
    txt = _SEPARATOR_NOISE_PAT.sub("", txt)
    txt = _TRAILING_NOISE_PAT.sub("", txt)
    txt = re.sub(r"[()]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip(" -/|")
    return txt


def _fetch_model_page_listings(
    make_slug: str, model_slug: str, *, max_attempts: int = 5
) -> List[Dict]:
    """Fetch the first search-results page for a specific model and return
    a list of {subject, engine_capacity, make_name, model_name} dicts.

    Hits: https://www.mudah.my/malaysia/cars-for-sale/{make_slug}/{model_slug}?o=1
    Listing data lives in __NEXT_DATA__ → props.pageProps.items[].attributes.
    Returns [] if the model has no active listings or the page can't be parsed.
    """
    url = f"https://www.mudah.my/malaysia/cars-for-sale/{make_slug}/{model_slug}?o=1"

    for attempt in range(1, max_attempts + 1):
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        try:
            resp = scraper.get(url, timeout=15)
            resp.raise_for_status()
        except Exception as exc:
            if attempt == max_attempts:
                logging.warning(f"[{make_slug}/{model_slug}] fetch failed: {exc}")
                return []
            time.sleep(2 + attempt)
            continue

        m = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.S
        )
        if not m:
            if attempt == max_attempts:
                logging.warning(f"[{make_slug}/{model_slug}] no __NEXT_DATA__ found")
                return []
            time.sleep(2 + attempt)
            continue

        try:
            nd = json.loads(m.group(1))
        except json.JSONDecodeError:
            logging.warning(f"[{make_slug}/{model_slug}] __NEXT_DATA__ not valid JSON")
            return []

        items = (
            nd.get("props", {}).get("pageProps", {}).get("items")
            or nd.get("props", {}).get("pageProps", {}).get("listings")
            or []
        )

        results = []
        for item in items:
            if not isinstance(item, dict):
                continue
            attrs = item.get("attributes", {})
            subject = attrs.get("subject", "")
            if not subject:
                continue
            results.append({
                "subject": subject,
                "engine_capacity": str(attrs.get("engine_capacity", "") or ""),
                "make_name": attrs.get("make_name", ""),
                "model_name": attrs.get("model_name", ""),
            })

        return results

    return []


def fetch_variants(makes_filter: Optional[List[str]] = None) -> Dict:
    """Build a variant vocabulary for cars by sampling listing subjects.

    For each (make, model) in cars_models.json, fetches one search results
    page and strips noise from subjects to extract variant tokens, grouped
    by engine_capacity.

    Args:
        makes_filter: if given, only process these make slugs.
                      Without this, pass --all-makes to run everything.

    Returns:
        {model_slug: {make, model, by_cc: {cc: [tokens]}, _all: [tokens]}}
    """
    models_path = _OUT_DIR / "cars_models.json"
    makes_path = _OUT_DIR / "cars_makes.json"

    if not models_path.exists() or not makes_path.exists():
        raise RuntimeError(
            "Run 'python src/scrape_makes_models.py' first to populate "
            "cars_models.json and cars_makes.json."
        )

    models_by_make: Dict = json.loads(models_path.read_text(encoding="utf-8"))
    makes_list: List = json.loads(makes_path.read_text(encoding="utf-8"))
    make_slug_to_name = {m["slug"]: m["name"] for m in makes_list}

    if makes_filter:
        missing = [s for s in makes_filter if s not in models_by_make]
        if missing:
            logging.warning(f"Unknown make slugs (skipped): {missing}")
        models_by_make = {k: v for k, v in models_by_make.items() if k in makes_filter}

    total_makes = len(models_by_make)
    variants: Dict = {}

    for i, (make_slug, model_list) in enumerate(models_by_make.items(), 1):
        make_name = make_slug_to_name.get(make_slug, make_slug)
        logging.info(
            f"[{i}/{total_makes}] {make_slug} ({len(model_list)} models)"
        )

        for model in model_list:
            model_slug = model["slug"]
            model_name = model["name"]

            listings = _fetch_model_page_listings(make_slug, model_slug)
            time.sleep(1.5)

            if not listings:
                continue

            by_cc: Dict = defaultdict(list)
            for listing in listings:
                cc = listing["engine_capacity"]
                token = extract_variant_tokens(
                    listing["subject"],
                    listing["make_name"] or make_name,
                    listing["model_name"] or model_name,
                )
                if token:
                    by_cc[cc].append(token)

            if not by_cc:
                continue

            cc_variants: Dict = {}
            all_tokens: List = []
            for cc in sorted(by_cc):
                top = [t for t, _ in Counter(by_cc[cc]).most_common(15)]
                cc_variants[cc] = top
                all_tokens.extend(top)

            variants[model_slug] = {
                "make": make_slug,
                "model": model_name,
                "by_cc": cc_variants,
                "_all": list(dict.fromkeys(all_tokens)),
            }

    return variants


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--variants",
        action="store_true",
        help="Build variant/trim vocabulary per model (cars only). "
             "Requires cars_models.json to exist — run without flags first.",
    )
    makes_group = parser.add_mutually_exclusive_group()
    makes_group.add_argument(
        "--makes",
        nargs="+",
        metavar="MAKE_SLUG",
        help="Limit --variants to specific make slugs "
             "(e.g. toyota honda perodua proton nissan)",
    )
    makes_group.add_argument(
        "--all-makes",
        action="store_true",
        help="Run --variants across all 128 makes (~1,500 HTTP requests; slow).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.variants:
        if not args.makes and not args.all_makes:
            parser.error(
                "--variants requires --makes <slug ...> or --all-makes.\n"
                "Suggested start: --makes toyota honda perodua proton nissan hyundai"
            )
        makes_filter = args.makes if args.makes else None
        logging.info("Building variant vocabulary for cars...")
        variants = fetch_variants(makes_filter=makes_filter)
        out_path = _OUT_DIR / "cars_variants.json"
        out_path.write_text(
            json.dumps(variants, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        n_models = len(variants)
        n_tokens = sum(len(v.get("_all", [])) for v in variants.values())
        logging.info(
            f"Wrote {out_path.relative_to(_ROOT)} "
            f"({n_models} models, {n_tokens} variant tokens)"
        )
        return

    # Default: refresh makes + models for both categories
    for category in ("cars", "motorcycles"):
        data = _fetch_category(category)

        makes_path = _OUT_DIR / f"{category}_makes.json"
        models_path = _OUT_DIR / f"{category}_models.json"

        makes_path.write_text(
            json.dumps(data["makes"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        models_path.write_text(
            json.dumps(data["models"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logging.info(f"[{category}] wrote {makes_path.relative_to(_ROOT)}")
        logging.info(f"[{category}] wrote {models_path.relative_to(_ROOT)}")


if __name__ == "__main__":
    sys.exit(main())

"""One-shot reference scraper: pull make + model lists from Mudah for both categories.

Mudah embeds the full make/model filter options in __NEXT_DATA__ on each
category landing page. Two HTTP requests get every make and every model for
both cars and motorcycles.

Usage:
    python src/scrape_makes_models.py

Outputs:
    data/reference/cars_makes.json
    data/reference/cars_models.json
    data/reference/motorcycles_makes.json
    data/reference/motorcycles_models.json

Run manually whenever the brand list needs refreshing (Mudah adds new makes
roughly once a year).
"""

import json
import logging
import sys
import time
from pathlib import Path

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


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    _OUT_DIR.mkdir(parents=True, exist_ok=True)

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

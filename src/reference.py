"""Reference-data loaders for CarData.

Single home for the make/model JSON files under data/reference/ so pipeline
scripts don't each re-encode the paths (and don't import each other for it).

Files (produced by scrape_makes_models.py):
    data/reference/cars_makes.json          [{id, name, slug}, ...]
    data/reference/cars_models.json         {make_slug: [{id, name, slug}, ...]}
    data/reference/motorcycles_makes.json
    data/reference/motorcycles_models.json

Curated (hand-maintained, not scraped):
    data/reference/motorcycles_model_types.csv
        (motorcycle_make, motorcycle_model) -> motorcycle_type, type_group
"""

import csv
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_ROOT = Path(__file__).resolve().parent.parent

REFERENCE_DIR = _ROOT / "data" / "reference"


def makes_path(category: str) -> Path:
    return REFERENCE_DIR / f"{category}_makes.json"


def models_path(category: str) -> Path:
    return REFERENCE_DIR / f"{category}_models.json"


def load_makes(category: str, *, required: bool = False) -> List[Dict]:
    """Load the makes list for a category.

    Returns [] when the file is missing unless ``required=True`` — sweep-style
    callers (recheck, backfill) must fail loudly rather than treat a missing
    reference file as "no makes" and silently do nothing.
    """
    path = makes_path(category)
    if not path.exists():
        if required:
            raise FileNotFoundError(
                f"Reference file not found: {path}. "
                f"Run scrape_makes_models.py to generate it."
            )
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_models(category: str) -> Dict[str, List[Dict]]:
    """Load the models dict {make_slug: [{id, name, slug}]} for a category."""
    path = models_path(category)
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def lookup_make(category: str, slug: str) -> Optional[Tuple[str, str]]:
    """Return (name, id) for the given make slug, or None if not found."""
    for make in load_makes(category):
        if make.get("slug") == slug:
            return make["name"], make["id"]
    return None


def lookup_model(category: str, make_slug: str, model_slug: str) -> Optional[Tuple[str, str]]:
    """Return (name, id) for the given model slug under a make, or None if not found."""
    for model in load_models(category).get(make_slug, []):
        if model.get("slug") == model_slug:
            return model["name"], model["id"]
    return None


def model_types_path() -> Path:
    return REFERENCE_DIR / "motorcycles_model_types.csv"


def load_model_types() -> Dict[Tuple[str, str], Tuple[str, str]]:
    """Load the motorcycle type mapping keyed by casefolded (make, model).

    Returns {} when the file is missing. Values are
    (motorcycle_type, type_group), e.g. ('Sport / Superbike', 'Sport').
    """
    path = model_types_path()
    if not path.exists():
        return {}
    mapping: Dict[Tuple[str, str], Tuple[str, str]] = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            key = (
                (row.get("motorcycle_make") or "").casefold().strip(),
                (row.get("motorcycle_model") or "").casefold().strip(),
            )
            mapping[key] = (
                (row.get("motorcycle_type") or "").strip(),
                (row.get("type_group") or "").strip(),
            )
    return mapping

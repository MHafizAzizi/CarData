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
        (motorcycle_make, motorcycle_model) -> motorcycle_type
        The coarse type_group is NOT stored in the CSV — it is derived from
        motorcycle_type via TYPE_TO_GROUP below, so the two can never desync.
    data/reference/cars_model_types.csv
        (car_make, car_model) -> vehicle_type, same convention: the coarse
        type_group is derived via CAR_TYPE_TO_GROUP, never stored.
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


# Granular motorcycle_type -> coarse ML grouping. Single source of truth for
# the grouping; the mapping CSV stores only the granular type.
TYPE_TO_GROUP: Dict[str, str] = {
    "Adventure":                 "Touring / Adventure",
    "Classic / Retro":           "Classic / Retro",
    "Cruiser":                   "Cruiser",
    "Dual-sport / Off-road":     "Off-road",
    "Electric Scooter":          "Electric",
    "Maxi-scooter":              "Scooter",
    "Mini / Monkey Bike":        "Mini bike",
    "Naked / Standard":          "Naked / Standard",
    "Scooter":                   "Scooter",
    "Sport / Superbike":         "Sport",
    "Sport Touring":             "Touring / Adventure",
    "Supermoto":                 "Off-road",
    "Touring":                   "Touring / Adventure",
    "Trike / 3-Wheeler":         "Three-wheeler",
    "Underbone / Moped":         "Underbone / Moped",
    "Unknown / Needs Web Check": "Unknown",
}


def _load_types(
    path: Path,
    make_col: str,
    model_col: str,
    type_col: str,
    group_map: Dict[str, str],
) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """Load a model-type CSV keyed by casefolded (make, model).

    Returns {} when the file is missing. Values are (granular_type, group),
    the group derived from `group_map` (never read from the file). Raises
    ValueError on a type not in `group_map` so a typo in a hand-edited row
    fails loudly instead of writing a bad group.
    """
    if not path.exists():
        return {}
    mapping: Dict[Tuple[str, str], Tuple[str, str]] = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            key = (
                (row.get(make_col) or "").casefold().strip(),
                (row.get(model_col) or "").casefold().strip(),
            )
            gtype = (row.get(type_col) or "").strip()
            if gtype not in group_map:
                raise ValueError(
                    f"{path.name}: unknown {type_col} {gtype!r} for "
                    f"{row.get(make_col)}/{row.get(model_col)} "
                    f"— must be one of {sorted(group_map)}"
                )
            mapping[key] = (gtype, group_map[gtype])
    return mapping


def load_model_types() -> Dict[Tuple[str, str], Tuple[str, str]]:
    """Motorcycle (make, model) -> (motorcycle_type, type_group). See _load_types."""
    return _load_types(
        model_types_path(), "motorcycle_make", "motorcycle_model",
        "motorcycle_type", TYPE_TO_GROUP,
    )


def car_types_path() -> Path:
    return REFERENCE_DIR / "cars_model_types.csv"


# Granular vehicle_type -> coarse ML grouping for cars. Single source of
# truth for the grouping; the mapping CSV stores only the granular type.
CAR_TYPE_TO_GROUP: Dict[str, str] = {
    "Sedan":                     "Sedan",
    "Hatchback":                 "Hatchback",
    "Sedan / Hatchback":         "Sedan / Hatchback",
    "SUV / Crossover":           "SUV / Crossover",
    "MPV / Minivan":             "MPV / Van",
    "Van / Commercial":          "MPV / Van",
    "Pickup / Truck":            "Pickup / Truck",
    "Coupe":                     "Coupe / Sports",
    "Sports car":                "Coupe / Sports",
    "Convertible / Roadster":    "Coupe / Sports",
    "Wagon / Estate":            "Wagon / Estate",
    "Unknown / Needs Web Check": "Unknown",
}

# Mudah API car_type -> canonical vehicle_type for the clean buckets.
# The junk buckets ('4 Wheels', 'Others') are deliberately absent: rows
# carrying those fall back to the curated cars_model_types.csv mapping.
API_CAR_TYPE_TO_VEHICLE: Dict[str, str] = {
    "Sedan":     "Sedan",
    "Suvs":      "SUV / Crossover",
    "Hatchback": "Hatchback",
    "Mpvs":      "MPV / Minivan",
    "Coupe":     "Coupe",
    "Sports":    "Sports car",
    "Pick-Up":   "Pickup / Truck",
}


def load_car_types() -> Dict[Tuple[str, str], Tuple[str, str]]:
    """Car (make, model) -> (vehicle_type, type_group). See _load_types."""
    return _load_types(
        car_types_path(), "car_make", "car_model",
        "vehicle_type", CAR_TYPE_TO_GROUP,
    )


# ---------------------------------------------------------------------------
# Auto-stubbing unmapped (make, model) pairs into the model-type CSVs
# ---------------------------------------------------------------------------

# A stub row carries the only type that is valid yet injects no guess: the
# loader accepts it (it's in TYPE_TO_GROUP / CAR_TYPE_TO_GROUP) and listings
# fill to group 'Unknown' until a human replaces it with the real type.
STUB_TYPE = "Unknown / Needs Web Check"
STUB_STATUS = "Auto-stub"  # find rows awaiting real classification: grep ,Auto-stub$

# Per-category CSV shape: (path fn, make col, model col, type col, full header).
_TYPE_CSV = {
    "motorcycles": (
        model_types_path, "motorcycle_make", "motorcycle_model", "motorcycle_type",
        ["motorcycle_make", "motorcycle_model", "motorcycle_type",
         "classification_method", "evidence", "source_url", "verification_status"],
    ),
    "cars": (
        car_types_path, "car_make", "car_model", "vehicle_type",
        ["car_make", "car_model", "vehicle_type",
         "classification_method", "evidence", "source_url", "verification_status"],
    ),
}


def stub_unmapped_types(category: str, pairs) -> int:
    """Append unmapped (make, model) pairs to the category's model-type CSV as
    'Unknown / Needs Web Check' / 'Auto-stub' stubs, then rewrite the file in
    sorted (make, model) order. Existing pairs are skipped. Returns the count
    added.

    The stub keeps the pipeline self-healing (a valid type → no loader crash)
    while leaving a greppable to-do (verification_status == 'Auto-stub') so the
    pair still surfaces for real classification.
    """
    if category not in _TYPE_CSV:
        raise ValueError(f"unknown category {category!r}")
    path_fn, make_col, model_col, type_col, columns = _TYPE_CSV[category]
    path = path_fn()

    rows: List[Dict] = []
    if path.exists():
        with open(path, encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))

    seen = {
        (r.get(make_col, "").casefold().strip(), r.get(model_col, "").casefold().strip())
        for r in rows
    }
    added = 0
    for make, model in pairs:
        make = (make or "").strip()
        model = (model or "").strip()
        if not make or not model:
            continue
        key = (make.casefold(), model.casefold())
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            make_col: make,
            model_col: model,
            type_col: STUB_TYPE,
            "classification_method": "auto-stub",
            "evidence": "",
            "source_url": "",
            "verification_status": STUB_STATUS,
        })
        added += 1

    if added:
        rows.sort(key=lambda r: (
            r.get(make_col, "").casefold(), r.get(model_col, "").casefold()
        ))
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
    return added

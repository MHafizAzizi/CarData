"""Enrich OLD-ERA car listings with carbase.my specs (schema v11).

The mcdParams sweep (recheck.py --method html) fills the v9 spec columns for
the mcdParams era only (ads_id >= 115M). Older listings carry no mcdParams, so
their spec columns stay NULL. This enricher fills those gaps from the
carbase.my catalog (data/reference/carbase_specs.db) via a DETERMINISTIC
(make, model, engine_capacity) match — never fuzzy variant-string matching,
which is the silent-mismatch trap that killed the original Phase 2 design.

Why engine_capacity is the key: old-era listings already carry the listing's
own cc in `engine_capacity` (filled 93,296/93,294). Matching make+model+cc
narrows the carbase variant pool to one engine, which disambiguates most
mass-market models. German luxury (Merc/BMW) detune one engine into many power
tunes; cc can't separate those, so power/torque are left NULL there (tier
cc_dims) while dimensions/weight (generation-stable) are still filled.

Tiers (recorded in spec_match):
    cc_full          model+cc matched, power unambiguous -> all fields incl power/torque
    cc_dims          model+cc matched, power ambiguous   -> dims/weight/body only
    null             model matched but no cc within tol  -> nothing filled
    no_model         make in catalog, model not          -> nothing
    no_make_catalog  make absent from catalog            -> nothing

Only OLD-ERA rows (ads_id < 115M) with engine_cc IS NULL are touched, so
mcdParams fills are never overwritten. Idempotent: a normal run skips rows with
spec_match already set; --force reprocesses all old-era rows.

Usage:
    python src/enrich_cars_specs.py                # enrich old-era cars
    python src/enrich_cars_specs.py --force        # re-run all old-era rows
    python src/enrich_cars_specs.py --dry-run      # report tiers, write nothing
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from db import connect  # noqa: E402
from cli_utf8 import force_utf8_stdio  # noqa: E402

SPECS_DB_PATH = _ROOT / "data" / "reference" / "carbase_specs.db"
ALIAS_PATH = _ROOT / "data" / "reference" / "car_spec_aliases.json"
SPEC_SOURCE = "carbase"

# Listings older than this carry no mcdParams (matches recheck.MCD_SPECS_MIN_ADS_ID).
MCD_SPECS_MIN_ADS_ID = 115_000_000

# carbase engine_cc vs listing engine_capacity tolerance (rounding/trim drift).
CC_TOLERANCE = 50

# Power tunes within this ratio count as "one engine" -> confident power fill.
POWER_SPREAD_RATIO = 1.10

# hp -> kW (peak_power_kw is stored in kW, same as mcdParams).
HP_TO_KW = 0.7457

# Always filled when a cc candidate is found (generation-stable, picked from the
# year-nearest candidate). (carbase column, listings column, converter)
DIMS_FIELD_MAP = [
    ("length_mm",      "length_mm",      int),
    ("width_mm",       "width_mm",       int),
    ("height_mm",      "height_mm",      int),
    ("wheelbase_mm",   "wheelbase_mm",   int),
    ("kerb_weight_kg", "kerb_weight_kg", int),
    ("seats",          "seat_capacity",  int),
    ("body_type",      "body_style",     str),
    ("engine_tech",    "engine_type",    str),
]

# Filled only when power is unambiguous (tier cc_full).
# (target column, source value func)
POWER_FIELDS = [
    ("peak_power_kw",  lambda r: None if r["power_hp"] is None
                       else round(r["power_hp"] * HP_TO_KW)),
    ("peak_torque_nm", lambda r: r["torque_nm"]),
]

# Every target column the enricher may write (for the UPDATE and dry-run).
TARGET_COLS = (
    ["engine_cc"]
    + [tgt for _, tgt, _ in DIMS_FIELD_MAP]
    + [tgt for tgt, _ in POWER_FIELDS]
)


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def norm(s: str) -> str:
    """Model match key: lowercase, strip everything non-alphanumeric."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def norm_make(s: str, make_alias: dict) -> str:
    """Make match key: alias lookup, else lowercase with spaces -> hyphens
    (carbase makes are hyphenated, e.g. 'mercedes-benz')."""
    low = (s or "").lower()
    return make_alias.get(low, low.replace(" ", "-"))


# ---------------------------------------------------------------------------
# Index + match
# ---------------------------------------------------------------------------

def build_index(specs_conn: sqlite3.Connection) -> dict:
    """{make: {norm(model): [rows]}} — list per model (many variants/years)."""
    idx: dict = defaultdict(lambda: defaultdict(list))
    for r in specs_conn.execute("SELECT * FROM model_specs"):
        idx[r["make"]][norm(r["model"])].append(r)
    return idx


def _year_key(row, year):
    """Sort key: distance from the listing year (unknown years sort last)."""
    ry = row["year"]
    if ry is None or year is None:
        return (1, 0)
    return (0, abs(ry - year))


def match(make, model, cc, year, index, make_alias, model_alias):
    """Return (tier, best_row_or_None, confident_power_bool).

    best_row is the cc-candidate nearest the listing year; confident_power says
    whether the matched cc maps to a single power tune (<= POWER_SPREAD_RATIO).
    """
    smk = norm_make(make, make_alias)
    pool = index.get(smk)
    if not pool:
        return ("no_make_catalog", None, False)

    nm = norm(model)
    rows = pool.get(nm)
    if rows is None:
        slug = model_alias.get(f"{make}|{model}")
        if slug:
            rows = pool.get(norm(slug))
    if not rows:
        return ("no_model", None, False)

    if cc is None:
        return ("null", None, False)

    cands = [r for r in rows
             if r["engine_cc"] is not None and abs(r["engine_cc"] - cc) <= CC_TOLERANCE]
    if not cands:
        return ("null", None, False)

    best = min(cands, key=lambda r: _year_key(r, year))

    powers = {r["power_hp"] for r in cands if r["power_hp"] is not None}
    if not powers:
        confident = False
    elif len(powers) == 1:
        confident = True
    else:
        confident = max(powers) <= min(powers) * POWER_SPREAD_RATIO

    return ("cc_full" if confident else "cc_dims", best, confident)


# ---------------------------------------------------------------------------
# Alias file
# ---------------------------------------------------------------------------

def load_aliases(path):
    """(make_alias dict, model_alias dict). NULL-decided entries excluded."""
    p = Path(path)
    if not p.exists():
        return {}, {}
    data = json.loads(p.read_text(encoding="utf-8"))
    make_alias = data.get("_make_alias", {})
    model_alias = {
        k: v["decision"]
        for k, v in data.get("aliases", {}).items()
        if v.get("decision") and v["decision"] != "NULL"
    }
    return make_alias, model_alias


# ---------------------------------------------------------------------------
# Enrich
# ---------------------------------------------------------------------------

def _build_values(srow, cc, confident):
    """Map a matched carbase row to {target_col: value} for the UPDATE.

    engine_cc takes the listing's OWN cc (exact), not carbase's (±tol). Dims are
    always taken from the year-nearest row; power/torque only when confident.
    """
    vals = {"engine_cc": cc}
    for src, tgt, conv in DIMS_FIELD_MAP:
        raw = srow[src]
        vals[tgt] = conv(raw) if raw is not None else None
    if confident:
        for tgt, fn in POWER_FIELDS:
            vals[tgt] = fn(srow)
    else:
        for tgt, _ in POWER_FIELDS:
            vals[tgt] = None
    return vals


def enrich(cars_conn, specs_conn, make_alias, model_alias, *,
           source: str = SPEC_SOURCE, force: bool = False,
           dry_run: bool = False) -> Counter:
    """Match every (unprocessed) old-era listing and write specs. Returns tier
    counts."""
    index = build_index(specs_conn)

    # Old-era only (ads_id < 115M). mcdParams stragglers (engine_cc set but
    # spec_match NULL) are never touched in either mode.
    #   normal: untouched rows only (engine_cc IS NULL AND spec_match IS NULL)
    #   force:  re-include rows the enricher itself processed (spec_match set),
    #           plus still-untouched rows; mcdParams stragglers stay excluded.
    if force:
        where = (f"WHERE CAST(ads_id AS INTEGER) < {MCD_SPECS_MIN_ADS_ID} "
                 "AND (engine_cc IS NULL OR spec_match IS NOT NULL)")
    else:
        where = (f"WHERE CAST(ads_id AS INTEGER) < {MCD_SPECS_MIN_ADS_ID} "
                 "AND engine_cc IS NULL AND spec_match IS NULL")
    rows = cars_conn.execute(
        f"SELECT ads_id, make, model, manufactured_date AS yr, "
        f"engine_capacity AS cc FROM listings {where}"
    ).fetchall()

    set_clause = ", ".join(f"{c}=?" for c in TARGET_COLS)
    update_matched = (
        f"UPDATE listings SET {set_clause}, spec_match=?, spec_source=? "
        f"WHERE ads_id=?"
    )
    update_unmatched = "UPDATE listings SET spec_match=? WHERE ads_id=?"

    stats: Counter = Counter()
    if dry_run:
        for r in rows:
            tier, _, _ = match(r["make"], r["model"], r["cc"], r["yr"],
                               index, make_alias, model_alias)
            stats[tier] += 1
        return stats

    with cars_conn:
        for r in rows:
            tier, srow, confident = match(
                r["make"], r["model"], r["cc"], r["yr"],
                index, make_alias, model_alias)
            stats[tier] += 1
            if srow is not None:
                vals = _build_values(srow, r["cc"], confident)
                params = [vals[c] for c in TARGET_COLS] + [tier, source, r["ads_id"]]
                cars_conn.execute(update_matched, params)
            else:
                cars_conn.execute(update_unmatched, (tier, r["ads_id"]))
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    force_utf8_stdio()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    p = argparse.ArgumentParser(
        description="Enrich old-era car listings with carbase.my specs.")
    p.add_argument("--force", action="store_true",
                   help="Reprocess all old-era rows, not just unmatched.")
    p.add_argument("--dry-run", action="store_true",
                   help="Report tiers; write nothing.")
    args = p.parse_args()

    make_alias, model_alias = load_aliases(ALIAS_PATH)
    cars = connect("cars")
    specs = sqlite3.connect(SPECS_DB_PATH)
    specs.row_factory = sqlite3.Row

    stats = enrich(cars, specs, make_alias, model_alias,
                   force=args.force, dry_run=args.dry_run)

    total = sum(stats.values())
    filled = stats["cc_full"] + stats["cc_dims"]
    logging.info("%senriched %d rows", "[DRY-RUN] " if args.dry_run else "", total)
    for tier in ("cc_full", "cc_dims", "null", "no_model", "no_make_catalog"):
        logging.info("  %-16s %7d", tier, stats[tier])
    if total:
        logging.info("filled (some specs): %d / %d = %.1f%%",
                     filled, total, 100 * filled / total)
        logging.info("  of which power/torque: %d (%.1f%%)",
                     stats["cc_full"], 100 * stats["cc_full"] / total)


if __name__ == "__main__":
    main()

"""Enrich motorcycle listings with OEM specs (schema v10).

Deterministic (make, model) match of `cardata_motorcycles.db` listings against
`data/reference/motobike_specs.db` (zigwheels.my catalog). No fuzzy matching —
see docs/superpowers/specs/2026-06-20-moto-spec-matching-phase2-design.md.

Tiers (first hit wins, recorded in `spec_match`):
    exact            norm(model) in the make's spec pool
    prefix           unambiguous prefix/containment (exactly one candidate)
    alias            curated data/reference/moto_spec_aliases.json
    null             no match — spec cols left NULL, do not guess
    no_make_catalog  make absent from the spec catalog

Idempotent: a normal run only processes rows with `spec_match IS NULL`
(unprocessed by any source — protects future motomalaysia Phase-2b fills).
`--force` reprocesses everything (e.g. after editing the alias file).

Usage:
    python src/enrich_specs.py                # enrich motorcycles
    python src/enrich_specs.py --force        # re-run all rows
    python src/enrich_specs.py --dry-run      # report tiers, write nothing
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from db import connect  # noqa: E402

SPECS_DB_PATH = _ROOT / "data" / "reference" / "motobike_specs.db"
ALIAS_PATH = _ROOT / "data" / "reference" / "moto_spec_aliases.json"
SPEC_SOURCE = "zigwheels"

# (source column in model_specs, target column in listings)
SPEC_FIELD_MAP = [
    ("engine_cc", "engine_cc"),
    ("power_hp", "power_hp"),
    ("torque_nm", "torque_nm"),
    ("kerb_weight_kg", "kerb_weight_kg"),
    ("fuel_tank_l", "fuel_tank_l"),
    ("engine_type", "engine_type"),
    ("transmission", "transmission"),
    ("fuel_type", "fuel_type"),
    ("cooling", "cooling"),
    ("seat_height_mm", "seat_height_mm"),
    ("wheelbase_mm", "wheelbase_mm"),
    ("compression_ratio", "comp_ratio"),  # source name differs from target
]


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def norm(s: str) -> str:
    """Model match key: lowercase, strip everything non-alphanumeric."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def norm_make(s: str, make_alias: dict) -> str:
    """Make match key: alias lookup, else lowercase with spaces -> hyphens
    (spec makes are hyphenated, e.g. 'qj-motor')."""
    low = (s or "").lower()
    return make_alias.get(low, low.replace(" ", "-"))


# ---------------------------------------------------------------------------
# Index + match
# ---------------------------------------------------------------------------

def build_index(specs_conn: sqlite3.Connection) -> dict:
    """{spec_make: {norm(model): row}} — unique key (verified 0 collisions)."""
    idx: dict = {}
    for r in specs_conn.execute("SELECT * FROM model_specs"):
        idx.setdefault(r["make"], {})[norm(r["model"])] = r
    return idx


def match(make, model, index, make_alias, model_alias):
    """Return (tier, spec_row_or_None)."""
    smk = norm_make(make, make_alias)
    pool = index.get(smk)
    if pool is None:
        return ("no_make_catalog", None)

    nm = norm(model)
    if nm in pool:
        return ("exact", pool[nm])

    cands = [r for k, r in pool.items() if k.startswith(nm) or nm.startswith(k)]
    if len(cands) == 1:
        return ("prefix", cands[0])

    slug = model_alias.get(f"{make}|{model}")
    if slug:
        nslug = norm(slug)
        if nslug in pool:
            return ("alias", pool[nslug])

    return ("null", None)


# ---------------------------------------------------------------------------
# Alias file
# ---------------------------------------------------------------------------

def load_aliases(path):
    """(make_alias dict, model_alias dict). NULL-decided entries excluded."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
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

def enrich(moto_conn, specs_conn, make_alias, model_alias, *,
           source: str = SPEC_SOURCE, force: bool = False,
           dry_run: bool = False) -> Counter:
    """Match every (unprocessed) listing and write specs. Returns tier counts."""
    index = build_index(specs_conn)

    where = "" if force else "WHERE spec_match IS NULL"
    rows = moto_conn.execute(
        f"SELECT ads_id, motorcycle_make, motorcycle_model FROM listings {where}"
    ).fetchall()

    set_clause = ", ".join(f"{tgt}=?" for _, tgt in SPEC_FIELD_MAP)
    update_matched = (
        f"UPDATE listings SET {set_clause}, spec_match=?, spec_source=? "
        f"WHERE ads_id=?"
    )
    update_unmatched = "UPDATE listings SET spec_match=? WHERE ads_id=?"

    stats: Counter = Counter()
    if dry_run:
        for r in rows:
            tier, _ = match(r["motorcycle_make"], r["motorcycle_model"],
                            index, make_alias, model_alias)
            stats[tier] += 1
        return stats

    with moto_conn:
        for r in rows:
            tier, srow = match(r["motorcycle_make"], r["motorcycle_model"],
                               index, make_alias, model_alias)
            stats[tier] += 1
            if srow is not None:
                # spec_source comes from the matched row when model_specs carries
                # a `source` column (zigwheels vs motomalaysia); else the default.
                row_source = srow["source"] if "source" in srow.keys() and srow["source"] else source
                vals = [srow[src] for src, _ in SPEC_FIELD_MAP]
                vals += [tier, row_source, r["ads_id"]]
                moto_conn.execute(update_matched, vals)
            else:
                moto_conn.execute(update_unmatched, (tier, r["ads_id"]))
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    p = argparse.ArgumentParser(description="Enrich motorcycle listings with OEM specs.")
    p.add_argument("--force", action="store_true", help="Reprocess all rows, not just unmatched.")
    p.add_argument("--dry-run", action="store_true", help="Report tiers; write nothing.")
    args = p.parse_args()

    make_alias, model_alias = load_aliases(ALIAS_PATH)
    moto = connect("motorcycles")
    specs = sqlite3.connect(SPECS_DB_PATH)
    specs.row_factory = sqlite3.Row

    stats = enrich(moto, specs, make_alias, model_alias,
                   force=args.force, dry_run=args.dry_run)

    total = sum(stats.values())
    matched = stats["exact"] + stats["prefix"] + stats["alias"]
    logging.info("%senriched %d rows", "[DRY-RUN] " if args.dry_run else "", total)
    for tier in ("exact", "prefix", "alias", "null", "no_make_catalog"):
        logging.info("  %-16s %6d", tier, stats[tier])
    if total:
        logging.info("matched: %d / %d = %.1f%%", matched, total, 100 * matched / total)


if __name__ == "__main__":
    main()

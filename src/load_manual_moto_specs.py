"""Load hand-verified moto specs into motobike_specs.db (source='manual').

Companion to scrape_zigwheels_specs.py / scrape_motomalaysia_specs.py for the
long tail those catalogs can't reach: newer Chinese/Taiwanese brands whose only
spec source is a JS-rendered or offline OEM site. Rather than a brittle parser
per brand, specs are transcribed by hand into data/reference/moto_specs_manual.json
and this loader upserts them as source='manual' rows that enrich_specs.py then
matches like any other catalog entry.

Idempotent: keyed on (make, model, source='manual'), so re-running replaces a
model's row rather than duplicating it. Add rows to the JSON and re-run.

Usage:
    python src/load_manual_moto_specs.py
    python src/load_manual_moto_specs.py --dry-run
"""

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from scrape_motomalaysia_specs import ensure_source_column  # noqa: E402
from cli_utf8 import force_utf8_stdio  # noqa: E402

SPECS_DB_PATH = _ROOT / "data" / "reference" / "motobike_specs.db"
MANUAL_PATH = _ROOT / "data" / "reference" / "moto_specs_manual.json"
SOURCE = "manual"

# model_specs columns a manual row may set (besides make/model/source/source_url).
_SPEC_COLS = [
    "engine_cc", "power_hp", "torque_nm", "kerb_weight_kg", "fuel_tank_l",
    "engine_type", "transmission", "fuel_type", "cooling", "seat_height_mm",
    "wheelbase_mm", "compression_ratio",
]


def load_manual(path=MANUAL_PATH):
    """Return the verified spec dicts from the curated JSON."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return data.get("specs", [])


def upsert(conn, row):
    """Insert/replace one manual spec row, keyed on (make, model, source)."""
    conn.execute(
        "DELETE FROM model_specs WHERE make=? AND model=? AND source=?",
        (row["make"], row["model"], SOURCE),
    )
    cols = ["make", "model", "source", "source_url"] + [c for c in _SPEC_COLS if c in row]
    vals = [row["make"], row["model"], SOURCE, row.get("source_url")] + \
           [row[c] for c in _SPEC_COLS if c in row]
    conn.execute(
        f"INSERT INTO model_specs ({', '.join(cols)}) "
        f"VALUES ({', '.join('?' * len(cols))})",
        vals,
    )


def main():
    force_utf8_stdio()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    ap = argparse.ArgumentParser(description="Load hand-verified moto specs.")
    ap.add_argument("--dry-run", action="store_true", help="report rows; write nothing")
    args = ap.parse_args()

    rows = load_manual()
    logging.info("%d manual spec rows in %s", len(rows), MANUAL_PATH.name)
    if args.dry_run:
        for r in rows:
            logging.info("  would load %s %s (cc=%s)", r["make"], r["model"], r.get("engine_cc"))
        return

    conn = sqlite3.connect(SPECS_DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_source_column(conn)
    with conn:
        for r in rows:
            upsert(conn, r)
            logging.info("  + %s %s cc=%s hp=%s", r["make"], r["model"],
                         r.get("engine_cc"), r.get("power_hp"))
    logging.info("done: %d manual rows loaded", len(rows))


if __name__ == "__main__":
    main()

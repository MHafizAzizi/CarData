"""Phase-2b/2c: cross-fill discontinued-bike specs from motomalaysia.com.

zigwheels.my (motobike_specs.db, source='zigwheels') is a current-catalog only —
discontinued bikes are absent, so many Mudah moto listings match nothing.
motomalaysia.com carries spec+price pages for older/discontinued models with a
richer field set (incl. 2-strokes). This crawls a CURATED, displacement-verified
list of those pages and inserts them into model_specs with source='motomalaysia'.

Identity (make/model slug) is supplied by the curated list and chosen so that
norm(listing make/model) hits the existing enrich_specs matcher at the exact or
prefix tier — NO fuzzy match. Bikes where motomalaysia only has a DIFFERENT
displacement (Honda Vario 150 -> only 160; Adv 150 -> 160; Dash 110 -> 125;
Wave 100 -> Alpha/125; Kriss MR3 -> MR2) are deliberately NOT listed, so they
stay NULL. See docs/superpowers/specs/2026-06-20-moto-spec-matching-phase2-design.md
and the CONTEXT.md Phase-2b audit (2026-06-21).

Usage:
    python src/scrape_motomalaysia_specs.py            # crawl curated list
    python src/scrape_motomalaysia_specs.py --refresh  # re-fetch existing rows
"""

import argparse
import logging
import re
import sqlite3
import sys
from pathlib import Path

from bs4 import BeautifulSoup

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

SPECS_DB_PATH = _ROOT / "data" / "reference" / "motobike_specs.db"
SOURCE = "motomalaysia"
_KW_TO_HP = 1.34102

# Curated, displacement-verified pages. (spec_make, spec_model, url).
# spec_model is chosen so norm(listing model) matches it at exact/prefix tier.
CURATED = [
    ("honda", "ex5", "https://www.motomalaysia.com/honda-ex5-fi-price-specs-malaysia/"),
    ("yamaha", "125zr", "https://www.motomalaysia.com/yamaha-125zr-price-specs-malaysia/"),
    ("yamaha", "fz150i", "https://www.motomalaysia.com/yamaha-fz150i-price-specs-malaysia/"),
    ("yamaha", "mt-07", "https://www.motomalaysia.com/yamaha-mt-07-price-specs-malaysia/"),
    ("kawasaki", "z250", "https://www.motomalaysia.com/kawasaki-z250-abs-2024-price-malaysia/"),
    ("kawasaki", "z800", "https://www.motomalaysia.com/kawasaki-z800-abs-2016-price-specs-malaysia/"),
    ("kawasaki", "ninja-zx-25r", "https://www.motomalaysia.com/kawasaki-ninja-zx-25r-se-2023-price-malaysia/"),
    ("honda", "wave-125i", "https://www.motomalaysia.com/honda-wave-125i-price-specs-malaysia/"),
    ("honda", "wave-dash", "https://www.motomalaysia.com/honda-wave-dash-price-specs-malaysia/"),
    # Phase-2c (2026-06-25): high-volume current/discontinued models present on
    # motomalaysia at the CORRECT displacement, verified before listing. Slugs
    # chosen so norm(listing model) hits enrich at exact/prefix.
    ("kawasaki", "er-6f", "https://www.motomalaysia.com/kawasaki-er-6f-2014-price-specs-malaysia/"),       # 649cc
    ("honda", "cbr1000rr", "https://www.motomalaysia.com/honda-cbr1000rr-2017-price-specs-malaysia/"),     # 999.8cc
    ("modenas", "dinamik", "https://www.motomalaysia.com/modenas-dinamik-2011-price-specs-malaysia/"),     # 118cc
    ("yamaha", "115z", "https://www.motomalaysia.com/yamaha-lagenda-115z-price-specs-malaysia/"),          # 113.7cc (also catches 115zr via prefix)
    ("honda", "pcx-150", "https://www.motomalaysia.com/honda-pcx-price-specs-malaysia/"),                  # 149cc
    ("honda", "future-125", "https://www.motomalaysia.com/honda-future-fi-price-specs-malaysia/"),         # 124.9cc
    # Deliberately NOT listed: Kawasaki Er-6N (absent from motomalaysia),
    # Kawasaki Versys (bare listing is ambiguous X-250 / 650 / 1000 — no guess).
]


# ---------------------------------------------------------------------------
# Coercion
# ---------------------------------------------------------------------------

def _missing(s) -> bool:
    return s is None or str(s).strip() in ("", "-", "–", "—")


def _to_float_unit(s):
    """First number in the string (comma-grouped ok) as float, else None."""
    if _missing(s):
        return None
    m = re.search(r"[\d,]+(?:\.\d+)?", str(s))
    if not m:
        return None
    return float(m.group(0).replace(",", ""))


def _to_int_unit(s):
    f = _to_float_unit(s)
    return int(round(f)) if f is not None else None


def _power_to_hp(s):
    """Max power -> hp. motomalaysia gives kW; convert. hp/PS/bhp pass through."""
    f = _to_float_unit(s)
    if f is None:
        return None
    if re.search(r"kw", str(s), re.IGNORECASE):
        return round(f * _KW_TO_HP, 1)
    return round(f, 1)


def _text(s):
    return None if _missing(s) else str(s).strip()


# label in the page table -> (target column, coercion)
LABEL_MAP = {
    "Displacement": ("engine_cc", _to_float_unit),
    "Max Power": ("power_hp", _power_to_hp),
    "Max Torque": ("torque_nm", _to_float_unit),
    "Weight": ("kerb_weight_kg", _to_float_unit),
    "Fuel Capacity": ("fuel_tank_l", _to_float_unit),
    "Engine": ("engine_type", _text),
    "Transmission": ("transmission", _text),
    "Cooling System": ("cooling", _text),
    "Seat Height": ("seat_height_mm", _to_int_unit),
    "Wheelbase": ("wheelbase_mm", _to_int_unit),
    "Compression Ratio": ("compression_ratio", _text),
}


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------

def parse_spec_page(html: str) -> dict:
    """Extract target spec fields from a motomalaysia spec page.

    The page renders specs as 2-cell table rows (label | value). Returns a dict
    keyed by listings/model_specs column names; absent fields are omitted.
    """
    soup = BeautifulSoup(html, "html.parser")
    spec: dict = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) != 2:
            continue
        label = cells[0].get_text(" ", strip=True)
        value = cells[1].get_text(" ", strip=True)
        if label in LABEL_MAP:
            col, coerce = LABEL_MAP[label]
            out = coerce(value)
            if out is not None and col not in spec:
                spec[col] = out
    return spec


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

# model_specs column names (enrich_specs maps compression_ratio -> listings.comp_ratio)
_TARGET_COLS = [
    "engine_cc", "power_hp", "torque_nm", "kerb_weight_kg", "fuel_tank_l",
    "engine_type", "transmission", "cooling", "seat_height_mm",
    "wheelbase_mm", "compression_ratio",
]


def ensure_source_column(conn: sqlite3.Connection) -> None:
    """Add model_specs.source (default existing rows to 'zigwheels')."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(model_specs)")}
    if "source" not in cols:
        conn.execute("ALTER TABLE model_specs ADD COLUMN source TEXT")
        conn.execute("UPDATE model_specs SET source='zigwheels' WHERE source IS NULL")


def upsert_spec(conn, make, model, spec, url):
    """Insert/replace a motomalaysia spec row keyed by (make, model, source)."""
    conn.execute(
        "DELETE FROM model_specs WHERE make=? AND model=? AND source=?",
        (make, model, SOURCE),
    )
    cols = ["make", "model", "source", "source_url"] + [c for c in _TARGET_COLS if c in spec]
    vals = [make, model, SOURCE, url] + [spec[c] for c in _TARGET_COLS if c in spec]
    conn.execute(
        f"INSERT INTO model_specs ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
        vals,
    )


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------

def crawl(client, conn, curated=CURATED, *, refresh=False):
    ensure_source_column(conn)
    done = 0
    for make, model, url in curated:
        if not refresh:
            exists = conn.execute(
                "SELECT 1 FROM model_specs WHERE make=? AND model=? AND source=?",
                (make, model, SOURCE),
            ).fetchone()
            if exists:
                logging.info("skip (have): %s %s", make, model)
                continue
        status, html = client.get_status(url)
        if status != 200 or not html:
            logging.warning("FAIL %s (status=%s): %s", model, status, url)
            continue
        spec = parse_spec_page(html)
        if "engine_cc" not in spec:
            logging.warning("no displacement parsed, skipping: %s", url)
            continue
        with conn:
            upsert_spec(conn, make, model, spec, url)
        logging.info("ok %-8s %-14s cc=%s hp=%s kg=%s", make, model,
                     spec.get("engine_cc"), spec.get("power_hp"), spec.get("kerb_weight_kg"))
        done += 1
    return done


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    ap = argparse.ArgumentParser(description="Crawl curated motomalaysia spec pages.")
    ap.add_argument("--refresh", action="store_true", help="re-fetch rows already stored")
    args = ap.parse_args()

    from carbase_client import CarbaseClient
    client = CarbaseClient()
    conn = sqlite3.connect(SPECS_DB_PATH)
    conn.row_factory = sqlite3.Row
    n = crawl(client, conn, refresh=args.refresh)
    logging.info("done: %d motomalaysia rows written", n)


if __name__ == "__main__":
    main()

"""zigwheels.my motorcycle full-catalog spec scraper.

Moto counterpart to scrape_carbase_specs.py (which does cars). Crawls
zigwheels.my new-motorcycle /specifications pages and stores per-model specs in a
standalone reference DB at data/reference/motobike_specs.db (table model_specs).
Occasional tool, run quarterly -- NOT part of the weekly run_pipeline.bat loop.

Source is zigwheels' *current* new-bike catalog, so discontinued models won't be
present (same ceiling carbase hits for cars). Spec pages are server-rendered flat
`<li><span>label</span><span class="f-bold">value</span></li>` lists -- one value
per label, no per-variant split -- so parsing is a single label->value sweep.

Discovery is via the spec sitemap (no brand->model walk needed): every spec page
URL is listed in model-specification-motorcycles-listing.xml. The sitemap carries
English + Malay (/ms/) mirrors of each URL; we keep English only.

Usage:
    python src/scrape_zigwheels_specs.py            # full catalog (skips stored URLs)
    python src/scrape_zigwheels_specs.py --limit 20 # smoke test
    python src/scrape_zigwheels_specs.py --refresh  # re-fetch stored rows
"""

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

# Reuse carbase's leading-number parsers -- identical job ('155 cc' -> 155 etc).
from scrape_carbase_specs import _to_int, _to_float
from cli_utf8 import force_utf8_stdio

_ROOT = Path(__file__).resolve().parent.parent
SPECS_DB_PATH = _ROOT / "data" / "reference" / "motobike_specs.db"

SITEMAP_URL = "https://www.zigwheels.my/model-specification-motorcycles-listing.xml"

_YEAR_RE = re.compile(r"(?:19|20)\d\d")


def _clean(value):
    if value is None:
        return None
    v = value.strip()
    return None if v in {"", "-"} else v


# --- discovery -------------------------------------------------------------------

def discover_spec_urls(client):
    """English-only /specifications URLs from the spec sitemap, deduped, in order.

    Drops the Malay (/ms/) mirror of each model so we fetch each page once.
    """
    status, xml = client.get_status(SITEMAP_URL)
    if status != 200 or not xml:
        raise RuntimeError(f"spec sitemap fetch failed (status={status})")
    seen, urls = set(), []
    for loc in re.findall(r"<loc>\s*([^<]+?)\s*</loc>", xml):
        if "/specifications" not in loc or "/ms/" in loc:
            continue
        if loc not in seen:
            seen.add(loc)
            urls.append(loc)
    return urls


# --- spec-page parsing -----------------------------------------------------------

# col -> (zigwheels li label, normalizer). str = take cleaned text. Missing label
# on a page -> None for that col; raw_specs still captures everything scraped.
_SPEC_FIELDS = {
    "category":             ("Category", str),
    "engine_cc":            ("Displacement", _to_int),
    "power_hp":             ("Maximum Power", _to_int),
    "rpm_power":            ("RPM at Maximum Power", _to_int),
    "torque_nm":            ("Maximum Torque", _to_float),
    "rpm_torque":           ("RPM at Maximum Torque", _to_int),
    "cylinders":            ("No. Of Cylinder", _to_int),
    "engine_type":          ("Engine Type", str),
    "cooling":              ("Cooling System", str),
    "valve_config":         ("Valve Configuration", str),
    "compression_ratio":    ("Compression Ratio", str),
    "bore_stroke":          ("Bore X Stroke", str),
    "fuel_system":          ("Fuel Supply System", str),
    "fuel_type":            ("Fuel Type", str),
    "fuel_tank_l":          ("Fuel Tank Capacity", _to_float),
    "fuel_consumption_kmpl": ("Fuel Consumption", _to_float),
    "transmission":         ("Transmission Type", str),
    "kerb_weight_kg":       ("Kerb Weight", _to_int),
    "length_mm":            ("Length", _to_int),
    "width_mm":             ("Width", _to_int),
    "height_mm":            ("Height", _to_int),
    "wheelbase_mm":         ("Wheel Base", _to_int),
    "seat_height_mm":       ("Seat Height", _to_int),
    "ground_clearance_mm":  ("Ground Clearance", _to_int),
    "seating_capacity":     ("Seating Capacity", _to_int),
    "front_suspension":     ("Front Suspension", str),
    "rear_suspension":      ("Rear Suspension", str),
    "front_tyre":           ("Front Tyre", str),
    "rear_tyre":            ("Rear Tyre", str),
}


def _spec_pairs(soup):
    """Every spec row as {label: value}.

    Each row is `<li><span>label</span><span class="f-bold ...">value</span></li>`.
    The f-bold class on the value span is what distinguishes spec rows from the
    page's other two-span <li>s (nav, related-bike cards).
    """
    out = {}
    for li in soup.find_all("li"):
        spans = li.find_all("span")
        if len(spans) != 2:
            continue
        if "f-bold" not in (spans[1].get("class") or []):
            continue
        label = spans[0].get_text(" ", strip=True)
        value = spans[1].get_text(" ", strip=True)
        if label and label not in out:
            out[label] = value
    return out


def _identity_from_url(url):
    """/new-motorcycles/<make>/<model>/specifications -> (make, model) slugs."""
    parts = urlsplit(url).path.strip("/").split("/")
    # ['new-motorcycles', make, model, 'specifications']
    return parts[1], parts[2]


def parse_spec_page(html, source_url):
    """Parse a zigwheels /specifications page into a model_specs row dict.

    Identity (make/model) comes from the URL; year from the page <title>
    ('Yamaha NVX 2026 Full Specs...'). Curated specs come from the labelled
    <li> rows; raw_specs carries the full label/value map for later mining.
    """
    soup = BeautifulSoup(html, "html.parser")
    pairs = _spec_pairs(soup)

    make, model = _identity_from_url(source_url)
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    ym = _YEAR_RE.search(title)

    row = {
        "make": make,
        "model": model,
        "year": int(ym.group(0)) if ym else None,
        "source_url": source_url,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }
    for col, (label, norm) in _SPEC_FIELDS.items():
        raw = pairs.get(label)
        row[col] = _clean(raw) if norm is str else norm(raw)
    row["raw_specs"] = json.dumps(pairs, ensure_ascii=False)
    return row


# --- storage (standalone motobike_specs.db / model_specs) ------------------------

_IDENTITY_COLS = ["make", "model", "year"]
_CURATED_COLS = list(_SPEC_FIELDS.keys())
_PROVENANCE_COLS = ["raw_specs", "source_url", "scraped_at"]
SPEC_COLUMNS = _IDENTITY_COLS + _CURATED_COLS + _PROVENANCE_COLS

_COL_TYPES = {
    "year": "INTEGER", "engine_cc": "INTEGER", "power_hp": "INTEGER",
    "rpm_power": "INTEGER", "rpm_torque": "INTEGER", "cylinders": "INTEGER",
    "kerb_weight_kg": "INTEGER", "length_mm": "INTEGER", "width_mm": "INTEGER",
    "height_mm": "INTEGER", "wheelbase_mm": "INTEGER", "seat_height_mm": "INTEGER",
    "ground_clearance_mm": "INTEGER", "seating_capacity": "INTEGER",
    "torque_nm": "REAL", "fuel_tank_l": "REAL", "fuel_consumption_kmpl": "REAL",
}


def init_specs_db(path=SPECS_DB_PATH):
    """Open (creating if needed) motobike_specs.db; ensure model_specs +
    UNIQUE(source_url) exist. Returns the connection."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.executescript("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
    col_defs = ",\n  ".join(f"{c} {_COL_TYPES.get(c, 'TEXT')}" for c in SPEC_COLUMNS)
    conn.executescript(
        f"CREATE TABLE IF NOT EXISTS model_specs (\n  {col_defs}\n);\n"
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_model_specs_url "
        "ON model_specs(source_url);"
    )
    return conn


def upsert_spec(conn, row):
    """Insert or refresh one model_specs row, keyed on source_url."""
    cols = SPEC_COLUMNS
    placeholders = ", ".join("?" for _ in cols)
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "source_url")
    sql = (
        f"INSERT INTO model_specs ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(source_url) DO UPDATE SET {updates}"
    )
    with conn:
        conn.execute(sql, [row.get(c) for c in cols])


def _stored_urls(conn):
    return {r[0] for r in conn.execute("SELECT source_url FROM model_specs")}


# --- crawl orchestration ---------------------------------------------------------

def crawl(client, conn, urls, limit=None, refresh=False):
    """Fetch each spec URL, parse, upsert. Skips stored URLs unless refresh."""
    done = set() if refresh else _stored_urls(conn)
    summary = {"seen": 0, "upserted": 0, "skipped_existing": 0, "errors": 0}

    for url in urls:
        summary["seen"] += 1
        if url in done:
            summary["skipped_existing"] += 1
            continue
        status, html = client.get_status(url)
        if status != 200 or not html:
            logging.warning(f"  {url} status={status}; skipping")
            summary["errors"] += 1
            continue
        try:
            row = parse_spec_page(html, url)
            upsert_spec(conn, row)
            done.add(url)
            summary["upserted"] += 1
            logging.info(f"  + {row['make']} {row['model']} {row['year']} "
                         f"(cc={row['engine_cc']} hp={row['power_hp']} "
                         f"kg={row['kerb_weight_kg']})")
        except Exception as e:  # noqa: BLE001 -- one bad page must not abort the crawl
            logging.error(f"  parse/store failed for {url}: {e}")
            summary["errors"] += 1

        if limit is not None and summary["upserted"] >= limit:
            logging.info(f"--limit {limit} reached; stopping")
            break
    return summary


def main():
    import argparse

    force_utf8_stdio()

    ap = argparse.ArgumentParser(description="zigwheels.my motorcycle spec scraper")
    ap.add_argument("--limit", type=int, help="stop after N pages (smoke test)")
    ap.add_argument("--refresh", action="store_true",
                    help="re-fetch and overwrite rows already in the DB")
    ap.add_argument("--db", default=str(SPECS_DB_PATH), help="specs DB path")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    # CarbaseClient is a generic throttled+retrying cloudscraper session; it takes
    # absolute URLs as-is, so it serves zigwheels without a dedicated client.
    from carbase_client import CarbaseClient
    client = CarbaseClient()
    conn = init_specs_db(args.db)

    urls = discover_spec_urls(client)
    logging.info(f"discovered {len(urls)} spec URLs")

    summary = crawl(client, conn, urls, limit=args.limit, refresh=args.refresh)
    logging.info(f"done: {summary}")


if __name__ == "__main__":
    main()

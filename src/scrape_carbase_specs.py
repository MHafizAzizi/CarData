"""carbase.my full-catalog spec scraper (Phase 1).

Crawls carbase.my (paultan.org's Malaysian car buyer's guide) brand -> model ->
variant and stores per-variant specifications in a standalone reference DB at
data/reference/carbase_specs.db (table model_specs). Occasional tool, run
quarterly -- NOT part of the weekly run_pipeline.bat loop.

Phase 1 builds the spec catalog only. Matching these specs to Mudah listings is
Phase 2 (separate spec). See docs/superpowers/specs/2026-06-17-carbase-spec-scraper-design.md.

Usage:
    python src/scrape_carbase_specs.py                 # full catalog (skips stored URLs)
    python src/scrape_carbase_specs.py --makes honda,toyota
    python src/scrape_carbase_specs.py --limit 20      # smoke test
    python src/scrape_carbase_specs.py --refresh       # re-fetch stored rows
"""

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

_ROOT = Path(__file__).resolve().parent.parent
SPECS_DB_PATH = _ROOT / "data" / "reference" / "carbase_specs.db"

_INT_RE = re.compile(r"-?\d[\d,]*")
_FLOAT_RE = re.compile(r"-?\d[\d,]*\.?\d*")
_EMPTY = {"", "-", "n/a", "na", "none", "tbc"}


def _to_int(raw):
    """Parse the leading integer out of a carbase value string.

    '1,498 cc' -> 1498, 'RM 89,900.00' -> 89900, '5' -> 5.
    '-' / '' / None / non-numeric -> None.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if s.lower() in _EMPTY:
        return None
    m = _INT_RE.search(s)
    if not m:
        return None
    return int(m.group(0).replace(",", ""))


def _to_float(raw):
    """Parse the leading float out of a carbase value string.

    '11.0 seconds' -> 11.0, '6.0 L/100 km' -> 6.0, '8.3' -> 8.3.
    '-' / '' / None / non-numeric -> None.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if s.lower() in _EMPTY:
        return None
    m = _FLOAT_RE.search(s)
    if not m:
        return None
    return float(m.group(0).replace(",", ""))


# --- link + URL parsing ----------------------------------------------------------

def _hrefs(html):
    """Yield deduped, query/fragment-stripped hrefs from anchor tags."""
    soup = BeautifulSoup(html, "html.parser")
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].split("?")[0].split("#")[0]
        if href not in seen:
            seen.add(href)
            yield href


def extract_model_links(html, make):
    """Model hrefs (/<make>/<model>) from a brand page. Excludes the brand self
    link and deeper variant links."""
    pat = re.compile(rf"^/{re.escape(make)}/[^/]+/?$")
    return [h for h in _hrefs(html) if pat.match(h)]


def extract_variant_links(html, make):
    """Variant-spec hrefs (/<make>/<model>/<generation>/<variant-YYYY>) from a
    model page. The -YYYY year anchor excludes same-depth noise pages
    (user-review, exterior, interior, owner-reviews, generations)."""
    pat = re.compile(rf"^/{re.escape(make)}/[^/]+/[^/]+/[^/]+-(?:19|20)\d\d/?$")
    return [h for h in _hrefs(html) if pat.match(h)]


_GEN_NOISE = {"exterior", "interior", "owner-reviews", "review", "userimages",
              "generations", "colors"}


def extract_generation_links(html, make, model):
    """Generation hrefs (/<make>/<model>/<gen>) from a generations page.

    Excludes noise sub-pages (exterior, interior, etc.) that share the same
    depth.
    """
    pat = re.compile(rf"^/{re.escape(make)}/{re.escape(model)}/([^/]+)/?$")
    out = []
    for h in _hrefs(html):
        m = pat.match(h)
        if m and m.group(1) not in _GEN_NOISE:
            out.append(h)
    return out


def parse_url_parts(url):
    """Split a variant URL into identity fields.

    /honda/civic/fe-facelift/1.5-rs-turbo-2025 ->
        make=honda model=civic generation=fe-facelift variant=1.5-rs-turbo year=2025
    Accepts absolute or path-only URLs.
    """
    path = urlsplit(url).path.strip("/")
    make, model, generation, variant_year = path.split("/")[:4]
    m = re.match(r"^(.*)-((?:19|20)\d\d)$", variant_year)
    variant, year = (m.group(1), int(m.group(2))) if m else (variant_year, None)
    return {
        "make": make, "model": model, "generation": generation,
        "variant": variant, "year": year,
    }


# --- variant spec-page parsing ---------------------------------------------------

# Curated column -> (carbase table id, row label, normalizer). The table id
# disambiguates labels that repeat across sections (e.g. "Type" under both
# Drivetrain and Chassis). None id = scan all sections for the label (pricing
# table has no id).
_SPEC_FIELDS = {
    "engine_tech":       ("Powertrain", "Engine Tech", str),
    "engine_cc":         ("Powertrain", "Capacity", _to_int),
    "power_hp":          ("Powertrain", "Horsepower", _to_int),
    "torque_nm":         ("Powertrain", "Torque", _to_int),
    "fuel_type":         ("Powertrain", "Fuel", str),
    "transmission":      ("Drivetrain", "Type", str),
    "driveline":         ("Drivetrain", "Driveline", str),
    "accel_0_100":       ("Performance-And-Efficiency", "0-100 km/h", _to_float),
    "fuel_economy_l100": ("Performance-And-Efficiency", "Rated Economy", _to_float),
    "top_speed_kmh":     ("Performance-And-Efficiency", "Top Speed", _to_int),
    "length_mm":         ("Dimensions", "Length", _to_int),
    "width_mm":          ("Dimensions", "Width", _to_int),
    "height_mm":         ("Dimensions", "Height", _to_int),
    "wheelbase_mm":      ("Dimensions", "Wheelbase", _to_int),
    "kerb_weight_kg":    ("Dimensions", "Weight", _to_int),
    "boot_l":            ("Dimensions", "Boot Space", _to_int),
    "seats":             ("Dimensions", "Seats", _to_int),
    "doors":             ("Dimensions", "Doors", _to_int),
    "price_peninsular":  (None, "Peninsular", _to_int),
}


def _clean(value):
    """Collapse a '-' placeholder to None for string fields."""
    if value is None:
        return None
    v = value.strip()
    return None if v in {"", "-"} else v


def _extract_sections(soup):
    """Map every carbase spec table to {section_id: {label: value}}.

    Tables with an id keep it; id-less tables (pricing/insurance/warranty) are
    indexed `_misc<N>` so their labels never collide with a real section. Each
    table's title/value cells are balanced and in document order, so a per-table
    zip pairs them correctly even across carbase's malformed (unclosed) <tr>s.
    """
    sections = {}
    misc = 0
    for table in soup.find_all("table"):
        tid = table.get("id")
        if not tid:
            tid = f"_misc{misc}"
            misc += 1
        titles = table.find_all("td", class_="table-title")
        values = table.find_all("td", class_="table-value")
        bucket = sections.setdefault(tid, {})
        for label_td, value_td in zip(titles, values):
            bucket[label_td.get_text(" ", strip=True)] = value_td.get_text(" ", strip=True)
    return sections


def _lookup(sections, section_id, label):
    """Fetch a raw value by (section, label). section_id=None scans all sections."""
    if section_id is not None:
        return sections.get(section_id, {}).get(label)
    for bucket in sections.values():
        if label in bucket:
            return bucket[label]
    return None


def _body_type_from_ldjson(soup):
    """Body type from the ld+json Vehicle blob (the one field it gets right).

    bodyType is a schema.org URL like 'https://schema.org/SUV' -> 'SUV';
    falls back to vehicleConfiguration.
    """
    for tag in soup.find_all("script", type="application/ld+json"):
        if not tag.string:
            continue
        try:
            obj = json.loads(tag.string)
        except json.JSONDecodeError:
            continue
        for o in (obj if isinstance(obj, list) else [obj]):
            if isinstance(o, dict) and o.get("@type") == "Vehicle":
                bt = o.get("bodyType") or o.get("vehicleConfiguration")
                if bt:
                    return _clean(bt.rstrip("/").rsplit("/", 1)[-1])
    return None


def parse_variant_page(html, source_url):
    """Parse a carbase variant spec page into a model_specs row dict.

    Identity (make/model/generation/variant/year) comes from the URL path.
    Curated specs come from the labelled HTML tables (the ld+json Vehicle blob
    is unreliable for numeric specs, so only body_type is taken from it).
    `raw_specs` carries every section/label/value scraped, for later mining.
    """
    soup = BeautifulSoup(html, "html.parser")
    sections = _extract_sections(soup)

    row = parse_url_parts(source_url)
    row["source_url"] = source_url
    row["scraped_at"] = datetime.now(timezone.utc).isoformat()
    row["body_type"] = _body_type_from_ldjson(soup)

    for col, (section_id, label, norm) in _SPEC_FIELDS.items():
        raw = _lookup(sections, section_id, label)
        if norm is str:
            row[col] = _clean(raw)
        else:
            row[col] = norm(raw)

    row["raw_specs"] = json.dumps(sections, ensure_ascii=False)
    return row


# --- storage (standalone carbase_specs.db / model_specs) -------------------------

# Column order is the single source of truth shared by the schema and the upsert.
_IDENTITY_COLS = ["make", "model", "generation", "variant", "year"]
_CURATED_COLS = [
    "body_type", "fuel_type", "engine_cc", "engine_tech", "power_hp", "torque_nm",
    "transmission", "driveline", "accel_0_100", "top_speed_kmh", "fuel_economy_l100",
    "length_mm", "width_mm", "height_mm", "wheelbase_mm", "kerb_weight_kg",
    "boot_l", "seats", "doors", "price_peninsular",
]
_PROVENANCE_COLS = ["raw_specs", "source_url", "scraped_at"]
SPEC_COLUMNS = _IDENTITY_COLS + _CURATED_COLS + _PROVENANCE_COLS

_COL_TYPES = {
    "year": "INTEGER", "engine_cc": "INTEGER", "power_hp": "INTEGER",
    "torque_nm": "INTEGER", "top_speed_kmh": "INTEGER", "length_mm": "INTEGER",
    "width_mm": "INTEGER", "height_mm": "INTEGER", "wheelbase_mm": "INTEGER",
    "kerb_weight_kg": "INTEGER", "boot_l": "INTEGER", "seats": "INTEGER",
    "doors": "INTEGER", "price_peninsular": "INTEGER",
    "accel_0_100": "REAL", "fuel_economy_l100": "REAL",
}


def init_specs_db(path=SPECS_DB_PATH):
    """Open (creating if needed) the standalone carbase_specs.db and ensure the
    model_specs table + UNIQUE(source_url) index exist. Returns the connection."""
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
    """Insert or refresh one model_specs row, keyed on source_url. Re-running the
    scraper overwrites the existing row rather than duplicating it."""
    cols = SPEC_COLUMNS
    placeholders = ", ".join("?" for _ in cols)
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "source_url")
    sql = (
        f"INSERT INTO model_specs ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(source_url) DO UPDATE SET {updates}"
    )
    with conn:
        conn.execute(sql, [row.get(c) for c in cols])


# --- crawl orchestration ---------------------------------------------------------

_BASE_HOST = "https://www.carbase.my"
_BRANDS_PATH = "/car-brands-malaysia"

# A handful of brand <select> slugs don't match their brand-page path (carbase
# renamed the brands). Map them here. Any future drift surfaces as a 404 that the
# crawl logs + skips, so a missing alias is visible rather than silent corruption.
_MAKE_SLUG_ALIASES = {
    "gwm": "great-wall",
    "omoda-jaecoo": "jaecoo",
}


def discover_makes(client):
    """Scrape the live brand list -> ordered make slugs (e.g. ['aion', ...]).

    The brand directory page only links ~17 popular brands as anchors; the full
    73-brand list lives in its `<select name="make">` dropdown. Slugs there use
    '+' for spaces (alfa+romeo -> alfa-romeo); a few are aliased to their actual
    brand-page path via _MAKE_SLUG_ALIASES.
    """
    status, html = client.get_status(_BRANDS_PATH)
    if status != 200 or not html:
        raise RuntimeError(f"brand list fetch failed (status={status})")
    soup = BeautifulSoup(html, "html.parser")
    select = soup.find("select", attrs={"name": "make"})
    if not select:
        raise RuntimeError("brand <select name=make> not found on brand page")
    seen, makes = set(), []
    for opt in select.find_all("option"):
        value = (opt.get("value") or "").strip()
        if not value:
            continue
        slug = value.replace("+", "-")
        slug = _MAKE_SLUG_ALIASES.get(slug, slug)
        if slug not in seen:
            seen.add(slug)
            makes.append(slug)
    return makes


def _stored_urls(conn):
    """Set of source_urls already in the DB (used to skip on resume)."""
    return {r[0] for r in conn.execute("SELECT source_url FROM model_specs")}


def crawl(client, conn, makes, limit=None, refresh=False):
    """Crawl makes -> models -> variants, upserting each variant's specs.

    Skips variant URLs already stored unless refresh=True. Returns a summary dict.
    """
    done = set() if refresh else _stored_urls(conn)
    summary = {"makes": 0, "models": 0, "variants_seen": 0,
               "upserted": 0, "skipped_existing": 0, "errors": 0}

    def _process_variant(vlink):
        """Fetch + upsert one variant page. Returns True if limit reached."""
        summary["variants_seen"] += 1
        url = f"{_BASE_HOST}{vlink}"
        if url in done:
            summary["skipped_existing"] += 1
            return False
        status, vhtml = client.get_status(vlink)
        if status != 200 or not vhtml:
            logging.warning(f"    {vlink} status={status}; skipping")
            summary["errors"] += 1
            return False
        try:
            row = parse_variant_page(vhtml, url)
            upsert_spec(conn, row)
            done.add(url)
            summary["upserted"] += 1
            logging.info(f"    + {row['make']} {row['model']} "
                         f"{row['variant']} {row['year']}")
        except Exception as e:  # noqa: BLE001 -- one bad page must not abort
            logging.error(f"    parse/store failed for {vlink}: {e}")
            summary["errors"] += 1
        return limit is not None and summary["upserted"] >= limit

    for make in makes:
        status, html = client.get_status(f"/{make}")
        summary["makes"] += 1
        if status != 200 or not html:
            logging.warning(f"[{make}] brand page status={status}; skipping")
            summary["errors"] += 1
            continue

        for model_link in extract_model_links(html, make):
            model_slug = model_link.strip("/").split("/")[-1]
            status, mhtml = client.get_status(model_link)
            summary["models"] += 1
            if status != 200 or not mhtml:
                logging.warning(f"  {model_link} status={status}; skipping")
                summary["errors"] += 1
                continue

            # Collect variant links from model page (latest gen)
            all_vlinks = set(extract_variant_links(mhtml, make))

            # Also crawl generation sub-pages for older variants
            gen_status, gen_html = client.get_status(
                f"/{make}/{model_slug}/generations")
            if gen_status == 200 and gen_html:
                for gen_link in extract_generation_links(
                        gen_html, make, model_slug):
                    gs, ghtml = client.get_status(gen_link)
                    if gs == 200 and ghtml:
                        all_vlinks.update(extract_variant_links(ghtml, make))

            for vlink in sorted(all_vlinks):
                if _process_variant(vlink):
                    logging.info(f"--limit {limit} reached; stopping")
                    return summary
    return summary


def main():
    import argparse
    import sys

    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    ap = argparse.ArgumentParser(description="carbase.my full-catalog spec scraper")
    ap.add_argument("--makes", help="comma-separated make slugs (default: all)")
    ap.add_argument("--limit", type=int, help="stop after N variant pages (smoke test)")
    ap.add_argument("--refresh", action="store_true",
                    help="re-fetch and overwrite rows already in the DB")
    ap.add_argument("--db", default=str(SPECS_DB_PATH), help="specs DB path")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    from carbase_client import CarbaseClient
    client = CarbaseClient()
    conn = init_specs_db(args.db)

    if args.makes:
        makes = [m.strip().lower() for m in args.makes.split(",") if m.strip()]
    else:
        makes = discover_makes(client)
        logging.info(f"discovered {len(makes)} makes")

    summary = crawl(client, conn, makes, limit=args.limit, refresh=args.refresh)
    logging.info(f"done: {summary}")


if __name__ == "__main__":
    main()

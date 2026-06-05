"""Compute one-page dashboard aggregates from the cars DB or latest CSV.

Source-of-truth resolution (in order):
    1. --csv <path> or --db <path>     (explicit override)
    2. SQLite DB at data/master/cardata_cars.db, filtered to fresh-schema
       rows (WHERE region IS NOT NULL) — used if >= 100 such rows exist
    3. Latest CSV in data/raw/cars/ or data/raw/test_5pages/

The fresh-schema filter exists because the DB also holds 22K legacy
xlsx rows (region/company_ad NULL, prices as 'RM 32,900' text). Once
all listings are re-scraped via EagleSearch + migrated, the filter is
a no-op.

Writes:
    mockups/dashboard.html      (JSON payload injected inline into
                                 a <script id="__DATA__"> sentinel)
    mockups/aggregates.json     (same payload as standalone JSON)

Usage:
    python src/dashboard_aggregate.py
    python src/dashboard_aggregate.py --csv data/raw/cars/some_file.csv
    python src/dashboard_aggregate.py --db data/master/cardata_cars.db

Cars only for v1.
"""

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = _ROOT / "mockups"
DASHBOARD_HTML = OUT_DIR / "dashboard.html"
DEFAULT_DB = _ROOT / "data" / "master" / "cardata_cars.db"

# Minimum fresh-schema rows in DB before we prefer it over the latest CSV.
# Below this we fall back to the CSV — usually because the EagleSearch
# CSVs haven't been migrated yet and the DB is mostly legacy data.
MIN_FRESH_ROWS_FOR_DB = 100

# Sentinel pattern that the aggregate script replaces in place.
# Matches: <script id="__DATA__" type="application/json">...anything...</script>
_DATA_SENTINEL_RE = re.compile(
    r'(<script\s+id="__DATA__"\s+type="application/json">)(.*?)(</script>)',
    re.DOTALL,
)

TOP_BRANDS = 8
TOP_REGIONS = 5
YEAR_FROM = 2015
YEAR_TO = 2026


def _find_latest_csv() -> Optional[Path]:
    """Return the most recently modified CSV from the known raw dirs.

    Also checks data/old/cars/ since 2_migrate.py archives
    processed CSVs there — that's still a valid source for re-aggregation.
    """
    candidates: List[Path] = []
    for d in [_ROOT / "data" / "raw" / "cars",
              _ROOT / "data" / "raw" / "test_5pages",
              _ROOT / "data" / "old" / "cars"]:
        if d.exists():
            candidates.extend(d.glob("mudah_eagle_cars_*.csv"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load_from_db(db_path: Path) -> Optional[pd.DataFrame]:
    """Load fresh-schema rows from SQLite. Returns None if not enough rows."""
    if not db_path.exists():
        return None
    con = sqlite3.connect(str(db_path))
    try:
        # Filter to rows where the EagleSearch pipeline has populated the
        # core categorical fields. Legacy xlsx rows have these as NULL and
        # would skew every aggregate.
        df = pd.read_sql_query(
            "SELECT * FROM listings WHERE region IS NOT NULL",
            con,
        )
    finally:
        con.close()
    if len(df) < MIN_FRESH_ROWS_FOR_DB:
        return None
    return df


def _select_source(
    explicit_csv: Optional[Path],
    explicit_db: Optional[Path],
) -> Tuple[pd.DataFrame, str]:
    """Resolve the data source per the precedence in the module docstring.

    Returns (DataFrame, source_label). Raises FileNotFoundError if no
    viable source is available.
    """
    if explicit_csv:
        df = pd.read_csv(explicit_csv, encoding="utf-8")
        return df, f"csv:{_rel(explicit_csv)}"

    if explicit_db:
        df = _load_from_db(explicit_db)
        if df is None or df.empty:
            raise RuntimeError(
                f"DB at {explicit_db} has no fresh-schema rows "
                f"(< {MIN_FRESH_ROWS_FOR_DB} with region populated)"
            )
        return df, f"db:{_rel(explicit_db)}"

    # Auto-detect: prefer DB, fall back to CSV
    df = _load_from_db(DEFAULT_DB)
    if df is not None and not df.empty:
        return df, f"db:{_rel(DEFAULT_DB)}"

    csv_path = _find_latest_csv()
    if csv_path is None:
        raise FileNotFoundError(
            "No data source available. Tried DB at "
            f"{_rel(DEFAULT_DB)} (not enough fresh-schema rows) "
            "and CSV in data/raw/cars/, data/raw/test_5pages/, data/old/cars/. "
            "Run 1_scrape.py + 2_migrate.py first."
        )
    df = pd.read_csv(csv_path, encoding="utf-8")
    return df, f"csv:{_rel(csv_path)}"


def _rel(p: Path) -> str:
    """Render a path relative to repo root with forward slashes."""
    p = p.resolve()
    try:
        return p.relative_to(_ROOT).as_posix()
    except ValueError:
        return p.as_posix()


def _share_words(pct: float) -> str:
    """Render share % as 'nearly one in four' for editorial headlines."""
    if pct >= 47.5:
        return "nearly half of"
    if pct >= 32:
        return "roughly a third of"
    if pct >= 22:
        return "nearly one in four"
    if pct >= 18:
        return "roughly one in five"
    if pct >= 13:
        return "about one in eight"
    return f"{pct:.1f}% of"


def _titlecase(s: Any) -> str:
    """Pretty-print a make/region. Handles edge brands ('BMW', 'KIA')."""
    if not isinstance(s, str):
        return str(s) if s is not None else ""
    keep_upper = {"BMW", "KIA", "MG", "MINI", "DFSK", "GMC"}
    if s.upper() in keep_upper:
        return s.upper()
    return s.title()


def _detect_inflection(years: List[Dict[str, Any]]) -> Dict[str, Any]:
    if len(years) < 2:
        return {}
    best = None
    for prev, curr in zip(years, years[1:]):
        if prev["median"] <= 0:
            continue
        ratio = curr["median"] / prev["median"]
        if best is None or ratio > best["ratio"]:
            best = {
                "from_year": prev["year"],
                "to_year": curr["year"],
                "from_median": prev["median"],
                "to_median": curr["median"],
                "ratio": ratio,
            }
    return best or {}


def aggregate(df: pd.DataFrame, source_label: str) -> Dict[str, Any]:
    """Compute the dashboard payload from a prepared DataFrame.

    Caller is responsible for sourcing the DataFrame (DB or CSV) and
    applying any fresh-schema filter. This function just aggregates.
    """
    # Coerce price to numeric (handles legacy 'RM 32,900' text + integer-as-string)
    df = df.copy()
    df["price"] = pd.to_numeric(
        df["price"].astype(str).str.replace("RM", "", regex=False)
                              .str.replace(",", "", regex=False)
                              .str.strip(),
        errors="coerce",
    )
    df = df[df["price"].notna() & (df["price"] > 0)].copy()
    total = len(df)
    if total == 0:
        raise RuntimeError(f"No priced rows in source: {source_label}")

    # ----- KPIs -------------------------------------------------------
    median_price = float(df["price"].median())
    price_min = float(df["price"].min())
    price_max = float(df["price"].max())

    # ----- Brands -----------------------------------------------------
    brands: List[Dict[str, Any]] = []
    if "make" in df.columns:
        bg = (
            df.dropna(subset=["make"])
              .groupby("make")["price"]
              .agg(["count", "median"])
              .sort_values("count", ascending=False)
              .head(TOP_BRANDS)
        )
        for name, row in bg.iterrows():
            brands.append({
                "name": _titlecase(name),
                "n": int(row["count"]),
                "median": int(row["median"]),
                "share": float(row["count"]) / total * 100,
            })

    # ----- Year cohorts ----------------------------------------------
    years: List[Dict[str, Any]] = []
    if "manufactured_date" in df.columns:
        df["_yr"] = pd.to_numeric(df["manufactured_date"], errors="coerce")
        for y in range(YEAR_FROM, YEAR_TO + 1):
            sub = df[df["_yr"] == y]
            if len(sub) >= 5:  # skip thin cohorts
                years.append({
                    "year": y,
                    "n": int(len(sub)),
                    "median": int(sub["price"].median()),
                })

    inflection = _detect_inflection(years)

    # ----- Regions ----------------------------------------------------
    regions: List[Dict[str, Any]] = []
    if "region" in df.columns:
        rg = (
            df.dropna(subset=["region"])
              .groupby("region")["price"]
              .agg(["count", "median"])
              .sort_values("count", ascending=False)
              .head(TOP_REGIONS)
        )
        for name, row in rg.iterrows():
            regions.append({
                "name": _titlecase(name),
                "n": int(row["count"]),
                "median": int(row["median"]),
                "share": float(row["count"]) / total * 100,
            })

    top2_share = sum(r["share"] for r in regions[:2])

    # ----- Seller split ----------------------------------------------
    if "company_ad" in df.columns:
        ca = pd.to_numeric(df["company_ad"], errors="coerce")
        dealer_df = df[ca == 1]
        private_df = df[(ca == 0) | ca.isna()]
    else:
        dealer_df = df.iloc[0:0]
        private_df = df

    dealer_n = len(dealer_df)
    private_n = len(private_df)
    dealer_median = int(dealer_df["price"].median()) if dealer_n else 0
    private_median = int(private_df["price"].median()) if private_n else 0
    premium = (dealer_median / private_median) if private_median > 0 else 0

    seller = {
        "dealer_n": dealer_n,
        "dealer_median": dealer_median,
        "dealer_share": dealer_n / total * 100,
        "private_n": private_n,
        "private_median": private_median,
        "private_share": private_n / total * 100,
        "premium_ratio": premium,
    }

    top_brand = brands[0] if brands else {"name": "?", "share": 0, "n": 0}

    # ----- Editorial headlines (hybrid: numbers auto, copy stays) ----
    next3 = sum(b["n"] for b in brands[1:4]) if len(brands) >= 4 else 0
    catch_phrase = (
        "do not catch it" if next3 < top_brand["n"] else "narrow the gap"
    )
    headlines = {
        "brand": (
            f"<span class='key rust'>{top_brand['name']}</span> holds "
            f"<span class='key'>{_share_words(top_brand['share'])}</span> "
            f"listings — the next three brands together {catch_phrase}."
        ),
        "year": (
            f"<span class='key blue'>{inflection.get('to_year', '—')}</span> "
            "is the inflection — between then and the year before, median "
            f"prices <span class='key'>jumped {inflection.get('ratio', 0):.1f}×</span>."
        ) if inflection else "Median prices climb steadily with model year.",
        "region": (
            f"<span class='key forest'>{regions[0]['name']}</span> &amp; "
            f"<span class='key forest'>{regions[1]['name']}</span> together "
            f"supply <span class='key'>{top2_share:.0f}%</span> of "
            "listings — the rest of Malaysia shares what's left."
        ) if len(regions) >= 2 else "",
        "seller": (
            f"A dealer asks <span class='key rust'>{premium:.2f}×</span> "
            "what a private seller does — yet private sellers post only "
            f"<span class='key'>{seller['private_share']:.1f}%</span> of listings."
        ) if premium > 0 else "",
    }

    intro_static = (
        f"The median car on Mudah.my asks <em>RM {median_price/1000:.1f}K</em>. "
        f"{top_brand['name']} holds {top_brand['share']:.1f}% of listings. "
        "Two states supply more than half the country's feed. And a dealer, "
        f"on average, asks <em>{premium:.2f}×</em> what a private seller does — "
        "though private sellers are nearly invisible on the platform to begin with."
    )

    return {
        "meta": {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "source": source_label,
            "total_listings": total,
            "category": "cars",
        },
        "kpis": {
            "total": total,
            "median_price": int(median_price),
            "price_min": int(price_min),
            "price_max": int(price_max),
            "top_brand": top_brand["name"],
            "top_brand_share": top_brand["share"],
            "dealer_share": seller["dealer_share"],
            "premium_ratio": premium,
        },
        "brands": brands,
        "years": years,
        "inflection": inflection,
        "regions": regions,
        "seller": seller,
        "headlines": headlines,
        "intro_static": intro_static,
    }


def write_outputs(payload: Dict[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    json_text = json.dumps(payload, indent=2, ensure_ascii=False)
    json_path = out_dir / "aggregates.json"
    json_path.write_text(json_text, encoding="utf-8")
    print(f"  wrote {json_path.relative_to(_ROOT)}")

    dashboard = out_dir / "dashboard.html"
    if not dashboard.exists():
        print(
            f"  WARN: {dashboard.relative_to(_ROOT)} not found — skipping inject. "
            "JSON written but dashboard not refreshed.",
            file=sys.stderr,
        )
        return

    html = dashboard.read_text(encoding="utf-8")
    if not _DATA_SENTINEL_RE.search(html):
        print(
            f"  WARN: no <script id=\"__DATA__\"> sentinel in "
            f"{dashboard.relative_to(_ROOT)} — skipping inject.",
            file=sys.stderr,
        )
        return

    # Escape "</script>" inside JSON (impossible with our payload, but defensive)
    safe = json_text.replace("</script>", "<\\/script>")
    new_html = _DATA_SENTINEL_RE.sub(
        lambda m: m.group(1) + "\n" + safe + "\n" + m.group(3),
        html,
        count=1,
    )
    dashboard.write_text(new_html, encoding="utf-8")
    print(f"  updated {dashboard.relative_to(_ROOT)} (data block)")


def main() -> None:
    p = argparse.ArgumentParser(description="Compute dashboard aggregates for cars")
    p.add_argument("--csv", default=None, help="Force CSV source at this path")
    p.add_argument("--db",  default=None, help="Force DB source at this path")
    p.add_argument("--out", default=str(OUT_DIR), help="Output directory")
    args = p.parse_args()

    explicit_csv = Path(args.csv) if args.csv else None
    explicit_db = Path(args.db) if args.db else None

    if explicit_csv and not explicit_csv.exists():
        print(f"ERROR: CSV not found: {explicit_csv}", file=sys.stderr)
        sys.exit(1)
    if explicit_db and not explicit_db.exists():
        print(f"ERROR: DB not found: {explicit_db}", file=sys.stderr)
        sys.exit(1)
    if explicit_csv and explicit_db:
        print("ERROR: pass either --csv or --db, not both.", file=sys.stderr)
        sys.exit(1)

    try:
        df, source_label = _select_source(explicit_csv, explicit_db)
    except (FileNotFoundError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"reading {source_label}  ({len(df):,} rows)")
    payload = aggregate(df, source_label)
    k = payload["kpis"]
    print(
        f"  total={k['total']} top={k['top_brand']} "
        f"median=RM{k['median_price']:,} premium={k['premium_ratio']:.2f}x"
    )
    write_outputs(payload, Path(args.out))


if __name__ == "__main__":
    main()

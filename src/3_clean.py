"""Data cleaning pipeline for CarData SQLite databases (and legacy xlsx).

Usage — DB mode (recommended):
    python src/3_clean.py --category motorcycles
    python src/3_clean.py --category cars
    python src/3_clean.py --category motorcycles --dry-run   # preview only

Usage — variant enrichment (cars only, requires cars_variants.json):
    python src/3_clean.py --category cars --enrich-variants
    python src/3_clean.py --category cars --enrich-variants --dry-run

Usage — type enrichment (requires the category's model-types CSV under
data/reference/: motorcycles_model_types.csv / cars_model_types.csv):
    python src/3_clean.py --category motorcycles --enrich-types
    python src/3_clean.py --category cars --enrich-types
    python src/3_clean.py --category cars --enrich-types --dry-run

Usage — legacy xlsx mode:
    python src/3_clean.py --input raw.xlsx --output clean.xlsx
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

# Covers the main Unicode emoji blocks + variation selectors
_EMOJI_PAT = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F9FF"
    "\U0001FA00-\U0001FA9F"
    "\U00002600-\U000027BF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "‍⏏⏩⌚️〰"
    "]+",
    flags=re.UNICODE,
)

# Trailing sales noise after a separator char in subject
# e.g. "Yamaha MT15 - Full Loan Ready Stock" → "Yamaha MT15"
_SUBJECT_NOISE_PAT = re.compile(
    r"\s*[-~|]\s*(?:full\s*loan|ready\s*stock|low\s*deposit|"
    r"90%\s*credit|free\s*delivery|ready|khm\s+\w+(?:\s+\w+)?)\b.*$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Column cleaners
# ---------------------------------------------------------------------------


def clean_price(series: pd.Series) -> pd.Series:
    """Strip 'RM', commas, and whitespace; convert to nullable integer."""
    cleaned = (
        series.astype(str)
        .str.replace(r"[Rr][Mm]", "", regex=True)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce").astype("Int64")


def clean_engine_capacity(series: pd.Series) -> pd.Series:
    """Strip 'cc'/'CC' suffix; convert to nullable integer."""
    cleaned = (
        series.astype(str)
        .str.replace(r"[Cc]{2}", "", regex=True)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce").astype("Int64")


def clean_manufactured_date(series: pd.Series) -> pd.Series:
    """Normalize manufactured_date: map '1995 or older' → 1995, extract 4-digit year."""
    normalized = series.astype(str).str.replace(
        r"1995\s+or\s+older", "1995", regex=True, flags=re.IGNORECASE
    )
    extracted = normalized.str.extract(r"(\d{4})")[0]
    return pd.to_numeric(extracted, errors="coerce").astype("Int64")


def clean_text(series: pd.Series) -> pd.Series:
    """Title-case and strip whitespace from free-text fields."""
    return series.astype(str).str.strip().str.title().replace("Nan", pd.NA)


def clean_company_ad(series: pd.Series) -> pd.Series:
    """Normalize company_ad string '0'/'1' → nullable integer."""
    return pd.to_numeric(series.astype(str).str.strip(), errors="coerce").astype("Int64")


def clean_subject(series: pd.Series) -> pd.Series:
    """Strip whitespace, emoji, and trailing sales-noise phrases from subject."""

    def _clean(text: str) -> object:
        if not isinstance(text, str):
            return pd.NA
        text = text.strip()
        text = _EMOJI_PAT.sub("", text).strip()
        text = _SUBJECT_NOISE_PAT.sub("", text).strip()
        return text if text and text.lower() != "nan" else pd.NA

    return series.apply(_clean)


# ---------------------------------------------------------------------------
# Dedup helper
# ---------------------------------------------------------------------------


def dedup_reposts(df: pd.DataFrame, make_col: str) -> Tuple[pd.DataFrame, int]:
    """Remove true reposts: same (subject + price + make), keep highest ads_id."""
    if make_col not in df.columns or "subject" not in df.columns:
        return df, 0

    before = len(df)
    price_str = pd.to_numeric(
        df["price"].astype(str).str.replace(r"[^\d]", "", regex=True),
        errors="coerce",
    ).astype(str)

    key = (
        df["subject"].fillna("").str.lower().str.strip()
        + "|"
        + price_str
        + "|"
        + df[make_col].fillna("").str.lower().str.strip()
    )
    df = df.assign(_rkey=key)
    df = (
        df.sort_values("ads_id")
        .drop_duplicates(subset="_rkey", keep="last")
        .drop(columns=["_rkey"])
        .reset_index(drop=True)
    )
    return df, before - len(df)


# ---------------------------------------------------------------------------
# Per-category cleaner maps
# ---------------------------------------------------------------------------

_CARS_CLEANERS: Dict[str, object] = {
    "price":             clean_price,
    "engine_capacity":   clean_engine_capacity,
    "manufactured_date": clean_manufactured_date,
    "make":              clean_text,
    "model":             clean_text,
    "variant":           clean_text,
    "condition":         clean_text,
    "car_type":          clean_text,
    "transmission":      clean_text,
    "fuel_type":         clean_text,
    "company_ad":        clean_company_ad,
    "subject":           clean_subject,
}

_MOTO_CLEANERS: Dict[str, object] = {
    "price":             clean_price,
    "manufactured_date": clean_manufactured_date,
    "motorcycle_make":   clean_text,
    "motorcycle_model":  clean_text,
    "condition":         clean_text,
    "company_ad":        clean_company_ad,
    "subject":           clean_subject,
}

CATEGORY_CLEANERS: Dict[str, Dict] = {
    "cars":        _CARS_CLEANERS,
    "motorcycles": _MOTO_CLEANERS,
}

CATEGORY_MAKE_COL: Dict[str, str] = {
    "cars":        "make",
    "motorcycles": "motorcycle_make",
}


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def clean(df: pd.DataFrame, category: str = "cars") -> pd.DataFrame:
    """Apply all cleaning steps and return a cleaned copy of df."""
    df = df.copy()
    cleaners = CATEGORY_CLEANERS.get(category, _CARS_CLEANERS)

    for col, cleaner in cleaners.items():
        if col in df.columns:
            df[col] = cleaner(df[col])  # type: ignore[operator]

    # Drop rows with no identifying information
    if category == "motorcycles":
        id_cols = [c for c in ("ads_id", "motorcycle_make", "motorcycle_model") if c in df.columns]
    else:
        id_cols = [c for c in ("ads_id", "make", "model") if c in df.columns]

    if id_cols:
        before = len(df)
        df = df.dropna(subset=id_cols, how="all").reset_index(drop=True)
        dropped = before - len(df)
        if dropped:
            print(f"  Dropped {dropped} rows with no identifying info")

    # Dedup on ads_id (exact), keep last seen
    if "ads_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset="ads_id", keep="last").reset_index(drop=True)
        removed = before - len(df)
        if removed:
            print(f"  Removed {removed} duplicate ads_id rows")

    # Dedup true reposts (same subject + price + make)
    make_col = CATEGORY_MAKE_COL.get(category, "make")
    df, repost_count = dedup_reposts(df, make_col)
    if repost_count:
        print(f"  Removed {repost_count} repost duplicates (same subject+price+make)")

    # Flag price outliers
    if "price" in df.columns:
        price_num = (
            df["price"]
            if pd.api.types.is_integer_dtype(df["price"])
            else pd.to_numeric(df["price"], errors="coerce")
        )
        low_price = price_num.notna() & (price_num < 1_000)
        if low_price.any():
            print(
                f"  WARNING: {low_price.sum()} row(s) with price < 1000 — "
                f"ads_id: {df.loc[low_price, 'ads_id'].tolist()}"
            )

    return df


# ---------------------------------------------------------------------------
# Variant resolution (cars only)
# ---------------------------------------------------------------------------

_VARIANTS_REF_PATH = _ROOT / "data" / "reference" / "cars_variants.json"


def resolve_variant(
    model: str,
    engine_capacity,
    subject: str,
    variants_ref: Dict,
) -> Optional[str]:
    """Return the best-match variant token for a listing, or None.

    Looks up the model in cars_variants.json, narrows candidates to those
    associated with the given engine_capacity (falling back to all variants
    for the model), then finds the longest candidate that appears in the
    listing subject.

    Longest-match-first prevents "G" matching before "GR Sport".
    """
    model_slug = re.sub(r"[^a-z0-9]+", "-", (model or "").lower()).strip("-")
    entry = variants_ref.get(model_slug)
    if not entry:
        return None

    cc_str = ""
    if engine_capacity is not None:
        try:
            cc_str = str(int(float(engine_capacity)))
        except (ValueError, TypeError):
            cc_str = str(engine_capacity)

    candidates: List[str] = (
        (entry.get("by_cc") or {}).get(cc_str)
        or entry.get("_all")
        or []
    )
    if not candidates:
        return None

    subject_upper = (subject or "").upper()
    for variant in sorted(candidates, key=len, reverse=True):
        if variant.upper() in subject_upper:
            return variant
    return None


def apply_variant_hints(
    conn,
    *,
    variants_ref_path: Path = _VARIANTS_REF_PATH,
    dry_run: bool = False,
) -> Dict:
    """Populate the `variant` column for fresh-schema rows where it is NULL,
    using the vocabulary in cars_variants.json.

    Only touches rows where variant IS NULL and region IS NOT NULL
    (i.e. fresh API-scraped rows, not legacy xlsx rows).

    Returns stats dict: {candidates, filled, skipped}.
    """
    if not variants_ref_path.exists():
        raise FileNotFoundError(
            f"{variants_ref_path} not found — run:\n"
            "  python src/scrape_makes_models.py --variants --makes toyota honda ..."
        )

    variants_ref: Dict = json.loads(
        variants_ref_path.read_text(encoding="utf-8")
    )

    rows = conn.execute(
        "SELECT ads_id, make, model, engine_capacity, subject "
        "FROM listings "
        "WHERE variant IS NULL AND region IS NOT NULL"
    ).fetchall()

    candidates = len(rows)
    filled = 0
    updates: List[Tuple] = []

    for ads_id, make, model, engine_capacity, subject in rows:
        variant = resolve_variant(model, engine_capacity, subject, variants_ref)
        if variant:
            updates.append((variant, ads_id))
            filled += 1

    skipped = candidates - filled
    print(
        f"  Variant hints: {candidates} candidates, "
        f"{filled} filled, {skipped} unresolved"
    )

    if not dry_run and updates:
        with conn:
            conn.executemany(
                "UPDATE listings SET variant = ? WHERE ads_id = ?", updates
            )

    return {"candidates": candidates, "filled": filled, "skipped": skipped}


# ---------------------------------------------------------------------------
# Motorcycle type enrichment (motorcycles only)
# ---------------------------------------------------------------------------


def apply_type_hints(conn, *, dry_run: bool = False,
                     write_unmapped: bool = False) -> Dict:
    """Populate motorcycle_type / type_group for rows where they are NULL,
    using the curated mapping in data/reference/motorcycles_model_types.csv.

    Unmapped (make, model) pairs are reported with listing counts so new
    models from future scrapes surface as mapping-CSV to-dos. With
    write_unmapped=True they are also appended to the CSV as 'Auto-stub'
    rows (type 'Unknown / Needs Web Check') for later real classification.

    Returns stats dict: {candidates, filled, unmapped_pairs, stubbed}.
    """
    sys.path.insert(0, str(_ROOT / "src"))
    from reference import (  # noqa: PLC0415
        load_model_types, model_types_path, stub_unmapped_types,
    )

    mapping = load_model_types()
    if not mapping:
        raise FileNotFoundError(
            f"{model_types_path()} not found or empty — the motorcycle type "
            "mapping is required for --enrich-types."
        )

    rows = conn.execute(
        "SELECT ads_id, motorcycle_make, motorcycle_model "
        "FROM listings "
        "WHERE motorcycle_type IS NULL"
    ).fetchall()

    candidates = len(rows)
    filled = 0
    updates: List[Tuple] = []
    unmapped: Dict[Tuple[str, str], int] = {}

    for ads_id, make, model in rows:
        key = ((make or "").casefold().strip(), (model or "").casefold().strip())
        hit = mapping.get(key)
        if hit:
            updates.append((hit[0], hit[1], ads_id))
            filled += 1
        else:
            unmapped[(make, model)] = unmapped.get((make, model), 0) + 1

    print(
        f"  Type hints: {candidates} candidates, {filled} filled, "
        f"{len(unmapped)} unmapped pair(s)"
    )
    if unmapped:
        print("  Unmapped (make, model) pairs — add to "
              "data/reference/motorcycles_model_types.csv:")
        for (make, model), n in sorted(unmapped.items(), key=lambda x: -x[1]):
            print(f"    {make} | {model} ({n} listing(s))")

    stubbed = 0
    if write_unmapped and unmapped and not dry_run:
        stubbed = stub_unmapped_types("motorcycles", unmapped.keys())
        print(f"  Wrote {stubbed} new 'Auto-stub' row(s) to "
              f"{model_types_path().name} — set the real type, then re-run.")

    if not dry_run and updates:
        with conn:
            conn.executemany(
                "UPDATE listings SET motorcycle_type = ?, type_group = ? "
                "WHERE ads_id = ?",
                updates,
            )

    return {
        "candidates": candidates,
        "filled": filled,
        "unmapped_pairs": len(unmapped),
        "stubbed": stubbed,
    }


# ---------------------------------------------------------------------------
# Car vehicle-type enrichment (cars only)
# ---------------------------------------------------------------------------


def apply_vehicle_type_hints(conn, *, dry_run: bool = False,
                             write_unmapped: bool = False) -> Dict:
    """Populate vehicle_type / type_group for car rows where they are NULL.

    API-first strategy: a clean API car_type ('Sedan', 'Suvs', ...) is
    normalised per listing via API_CAR_TYPE_TO_VEHICLE; junk buckets
    ('4 Wheels', 'Others') fall back to the curated mapping in
    data/reference/cars_model_types.csv. Junk rows whose (make, model) is
    not in the mapping stay NULL and are reported with listing counts so
    new models from future scrapes surface as mapping-CSV to-dos. With
    write_unmapped=True they are also appended to the CSV as 'Auto-stub'
    rows (type 'Unknown / Needs Web Check') for later real classification.

    Returns stats dict:
    {candidates, filled_api, filled_mapping, unmapped_pairs, stubbed}.
    """
    sys.path.insert(0, str(_ROOT / "src"))
    from reference import (  # noqa: PLC0415
        API_CAR_TYPE_TO_VEHICLE,
        CAR_TYPE_TO_GROUP,
        car_types_path,
        load_car_types,
        stub_unmapped_types,
    )

    mapping = load_car_types()
    if not mapping:
        raise FileNotFoundError(
            f"{car_types_path()} not found or empty — the car vehicle-type "
            "mapping is required for --enrich-types."
        )

    rows = conn.execute(
        "SELECT ads_id, make, model, car_type "
        "FROM listings "
        "WHERE vehicle_type IS NULL"
    ).fetchall()

    candidates = len(rows)
    filled_api = 0
    filled_mapping = 0
    updates: List[Tuple] = []
    unmapped: Dict[Tuple[str, str], int] = {}

    for ads_id, make, model, car_type in rows:
        vtype = API_CAR_TYPE_TO_VEHICLE.get((car_type or "").strip())
        if vtype:
            filled_api += 1
        else:
            key = ((make or "").casefold().strip(), (model or "").casefold().strip())
            hit = mapping.get(key)
            if hit:
                vtype = hit[0]
                filled_mapping += 1
            else:
                unmapped[(make, model)] = unmapped.get((make, model), 0) + 1
                continue
        updates.append((vtype, CAR_TYPE_TO_GROUP[vtype], ads_id))

    print(
        f"  Vehicle-type hints: {candidates} candidates, "
        f"{filled_api} from API car_type, {filled_mapping} from mapping, "
        f"{len(unmapped)} unmapped pair(s)"
    )
    if unmapped:
        print("  Unmapped (make, model) pairs — add to "
              "data/reference/cars_model_types.csv:")
        for (make, model), n in sorted(unmapped.items(), key=lambda x: -x[1]):
            print(f"    {make} | {model} ({n} listing(s))")

    stubbed = 0
    if write_unmapped and unmapped and not dry_run:
        stubbed = stub_unmapped_types("cars", unmapped.keys())
        print(f"  Wrote {stubbed} new 'Auto-stub' row(s) to "
              f"{car_types_path().name} — set the real type, then re-run.")

    if not dry_run and updates:
        with conn:
            conn.executemany(
                "UPDATE listings SET vehicle_type = ?, type_group = ? "
                "WHERE ads_id = ?",
                updates,
            )

    return {
        "candidates": candidates,
        "filled_api": filled_api,
        "filled_mapping": filled_mapping,
        "unmapped_pairs": len(unmapped),
        "stubbed": stubbed,
    }


# ---------------------------------------------------------------------------
# DB clean-in-place
# ---------------------------------------------------------------------------

# Columns that carry tracking state — never overwrite during cleaning
_IMMUTABLE_COLS = frozenset(
    ("ads_id", "url", "first_seen_at", "last_seen_at", "last_checked_at", "availability_status")
)


def clean_db(category: str, *, dry_run: bool = False) -> dict:
    """Read the category DB, clean, write back, return stats dict."""
    sys.path.insert(0, str(_ROOT / "src"))
    from db import connect, set_meta  # noqa: PLC0415

    conn = connect(category)
    df = pd.read_sql("SELECT * FROM listings", conn)
    before_rows = len(df)

    print(f"\n[{category}] Loaded {before_rows} rows")
    df_clean = clean(df, category=category)
    after_rows = len(df_clean)
    n_removed = before_rows - after_rows

    if dry_run:
        print(f"[{category}] DRY RUN — no changes written.")
        print(f"  Before: {before_rows}  After: {after_rows}  Would remove: {n_removed}")
        conn.close()
        return {"before": before_rows, "after": after_rows, "removed": n_removed}

    update_cols: List[str] = [
        c for c in df_clean.columns if c not in _IMMUTABLE_COLS
    ]

    removed_ids = set(df["ads_id"].tolist()) - set(df_clean["ads_id"].tolist())

    with conn:
        # Delete rows removed by dedup/drop logic
        if removed_ids:
            placeholders = ",".join("?" * len(removed_ids))
            conn.execute(
                f"DELETE FROM listings WHERE ads_id IN ({placeholders})",
                list(removed_ids),
            )
            print(f"  Deleted {len(removed_ids)} row(s) from DB")

        # Update cleaned columns for surviving rows
        set_clause = ", ".join(f"{c} = ?" for c in update_cols)
        for _, row in df_clean.iterrows():
            vals = [None if pd.isna(row[c]) else row[c] for c in update_cols]
            vals.append(int(row["ads_id"]))
            conn.execute(
                f"UPDATE listings SET {set_clause} WHERE ads_id = ?",
                vals,
            )

    set_meta(conn, "last_cleaned_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    conn.close()

    print(f"[{category}] Done. Before: {before_rows}  After: {after_rows}  Removed: {n_removed}")
    return {"before": before_rows, "after": after_rows, "removed": n_removed}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CarData cleaning pipeline")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--category",
        choices=["cars", "motorcycles"],
        help="Clean the given SQLite DB category (recommended)",
    )
    mode.add_argument(
        "--input",
        default=None,
        help="Legacy xlsx input path",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Legacy xlsx output path (default: overwrite input)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would change; do not write anything",
    )
    parser.add_argument(
        "--enrich-types",
        action="store_true",
        help="Populate the type columns for NULL rows: motorcycle_type/"
             "type_group from motorcycles_model_types.csv (motorcycles) or "
             "vehicle_type/type_group from API car_type + "
             "cars_model_types.csv (cars).",
    )
    parser.add_argument(
        "--write-unmapped",
        action="store_true",
        help="With --enrich-types: append unmapped (make, model) pairs to the "
             "model-type CSV as 'Auto-stub' rows (type 'Unknown / Needs Web "
             "Check') so the pipeline self-heals. Find them later with "
             "grep ',Auto-stub$' and set the real type.",
    )
    parser.add_argument(
        "--enrich-variants",
        action="store_true",
        help="Populate the variant column for NULL rows using cars_variants.json "
             "(cars only; run scrape_makes_models.py --variants first).",
    )
    return parser.parse_args()


def _prompt_category() -> str:
    """Interactive category prompt (matches scraper.py / migrate_xlsx_to_db.py)."""
    choices = ["cars", "motorcycles"]
    print("\nSelect category to clean:")
    for i, c in enumerate(choices, 1):
        print(f"  {i}. {c}")
    while True:
        raw = input("Enter 1 or 2 [default: 1]: ").strip()
        if raw == "":
            return choices[0]
        try:
            idx = int(raw)
            if 1 <= idx <= len(choices):
                return choices[idx - 1]
        except ValueError:
            pass
        print("Please enter 1 or 2.")


def main() -> None:
    args = _parse_args()

    # Default to DB mode; only fall into legacy xlsx mode if --input was given.
    if args.category is None and args.input is None:
        args.category = _prompt_category()

    if args.category:
        clean_db(args.category, dry_run=args.dry_run)

        if args.enrich_variants:
            if args.category != "cars":
                print("[enrich-variants] Only supported for --category cars; skipping.")
            else:
                sys.path.insert(0, str(_ROOT / "src"))
                from db import connect  # noqa: PLC0415
                conn = connect("cars")
                apply_variant_hints(conn, dry_run=args.dry_run)
                conn.close()

        if args.enrich_types:
            sys.path.insert(0, str(_ROOT / "src"))
            from db import connect  # noqa: PLC0415
            if args.category == "motorcycles":
                conn = connect("motorcycles")
                apply_type_hints(conn, dry_run=args.dry_run,
                                 write_unmapped=args.write_unmapped)
                conn.close()
            elif args.category == "cars":
                conn = connect("cars")
                apply_vehicle_type_hints(conn, dry_run=args.dry_run,
                                         write_unmapped=args.write_unmapped)
                conn.close()
            else:
                print("[enrich-types] Requires --category cars or motorcycles; skipping.")
        return

    # Legacy xlsx mode (only entered when --input was explicitly provided)
    input_path = args.input
    output_path = args.output or input_path

    if not Path(input_path).exists():
        print(f"ERROR: xlsx not found: {input_path}")
        print("       For DB cleaning use:  python src/3_clean.py --category cars")
        sys.exit(1)

    print(f"Reading {input_path} ...")
    df = pd.read_excel(input_path)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    cleaned = clean(df, category="cars")
    print(f"Cleaned: {len(cleaned)} rows remaining")

    if not args.dry_run:
        cleaned.to_excel(output_path, index=False)
        print(f"Saved to {output_path}")
    else:
        print("DRY RUN — not written.")


if __name__ == "__main__":
    main()

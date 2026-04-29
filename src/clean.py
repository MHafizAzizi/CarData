"""Data cleaning pipeline for CarData SQLite databases (and legacy xlsx).

Usage — DB mode (recommended):
    python clean.py --category motorcycles
    python clean.py --category cars
    python clean.py --category motorcycles --dry-run   # preview only

Usage — legacy xlsx mode:
    python clean.py --input raw.xlsx --output clean.xlsx
"""

import argparse
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

# Malaysian mobile: 01x-xxxxxxx or +601x-xxxxxxx
_PHONE_PAT = re.compile(
    r"\b(01[0-9][-\s]?\d{7,8}|\+?60\s?1[0-9][-\s]?\d{7,8})\b"
)

# Decorative separator lines (====, ----, ****, ~~~~ etc.)
_SEPARATOR_PAT = re.compile(r"^[=\-*_~]{5,}\s*$", re.MULTILINE)

# 3+ consecutive blank lines → collapse
_MULTI_BLANK_PAT = re.compile(r"\n{3,}")

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


def clean_mileage(series: pd.Series) -> pd.Series:
    """Strip 'km', commas, and whitespace; convert to nullable integer."""
    cleaned = (
        series.astype(str)
        .str.replace(r"[Kk][Mm]", "", regex=True)
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


def clean_year(series: pd.Series) -> pd.Series:
    """Extract 4-digit year and convert to nullable integer."""
    extracted = series.astype(str).str.extract(r"(\d{4})")[0]
    return pd.to_numeric(extracted, errors="coerce").astype("Int64")


def clean_text(series: pd.Series) -> pd.Series:
    """Title-case and strip whitespace from free-text fields."""
    return series.astype(str).str.strip().str.title().replace("Nan", pd.NA)


def clean_numeric(series: pd.Series) -> pd.Series:
    """Strip any non-numeric characters (except dot) and convert to float."""
    cleaned = series.astype(str).str.replace(r"[^\d.]", "", regex=True).str.strip()
    return pd.to_numeric(cleaned, errors="coerce")


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


def clean_body(series: pd.Series) -> pd.Series:
    """Redact phone numbers, strip emoji + decorators, collapse blanks, null if empty."""

    def _clean(text: object) -> object:
        if not isinstance(text, str) or not text.strip():
            return pd.NA
        text = _PHONE_PAT.sub("[PHONE]", text)
        text = _EMOJI_PAT.sub("", text)
        text = _SEPARATOR_PAT.sub("", text)
        text = _MULTI_BLANK_PAT.sub("\n\n", text)
        text = text.strip()
        return text if len(text) >= 20 else pd.NA

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
    "mileage":           clean_mileage,
    "engine_capacity":   clean_engine_capacity,
    "cc":                clean_engine_capacity,
    "manufactured_date": clean_manufactured_date,
    "make":              clean_text,
    "model":             clean_text,
    "variant":           clean_text,
    "family":            clean_text,
    "series":            clean_text,
    "location":          clean_text,
    "condition":         clean_text,
    "car_type":          clean_text,
    "transmission":      clean_text,
    "fuel_type":         clean_text,
    "country_origin":    clean_text,
    "kw":                clean_numeric,
    "torque":            clean_numeric,
    "length":            clean_numeric,
    "width":             clean_numeric,
    "height":            clean_numeric,
    "wheelbase":         clean_numeric,
    "kerbwt":            clean_numeric,
    "fueltk":            clean_numeric,
    "company_ad":        clean_company_ad,
    "subject":           clean_subject,
    "body":              clean_body,
}

_MOTO_CLEANERS: Dict[str, object] = {
    "price":             clean_price,
    "mileage":           clean_mileage,
    "manufactured_date": clean_manufactured_date,
    "motorcycle_make":   clean_text,
    "motorcycle_model":  clean_text,
    "location":          clean_text,
    "condition":         clean_text,
    "company_ad":        clean_company_ad,
    "subject":           clean_subject,
    "body":              clean_body,
}

CATEGORY_CLEANERS: Dict[str, Dict] = {
    "cars":        _CARS_CLEANERS,
    "motorcycles": _MOTO_CLEANERS,
}

CATEGORY_MAKE_COL: Dict[str, str] = {
    "cars":        "make",
    "motorcycles": "motorcycle_make",
}

# Backward-compat alias used by old xlsx callers
CLEANERS = _CARS_CLEANERS


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
            vals = [
                None if pd.isna(row[c]) else (
                    int(row[c]) if isinstance(row[c], pd.NA.__class__) else row[c]
                )
                for c in update_cols
            ]
            # Safely convert pandas NA / numpy int64
            safe_vals = []
            for v in vals:
                try:
                    if pd.isna(v):
                        safe_vals.append(None)
                        continue
                except (TypeError, ValueError):
                    pass
                safe_vals.append(v)
            safe_vals.append(int(row["ads_id"]))
            conn.execute(
                f"UPDATE listings SET {set_clause} WHERE ads_id = ?",
                safe_vals,
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
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.category:
        clean_db(args.category, dry_run=args.dry_run)
        return

    # Legacy xlsx mode
    input_path = args.input or str(_ROOT / "data" / "master" / "MasterMudahCarData.xlsx")
    output_path = args.output or input_path

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

"""
Data cleaning script for MasterMudahCarData.xlsx.

Usage:
    python clean.py                                  # reads/writes MasterMudahCarData.xlsx
    python clean.py --input raw.xlsx --output clean.xlsx
"""

import argparse
import re
from pathlib import Path

import pandas as pd

# Anchor paths to project root regardless of cwd
_ROOT = Path(__file__).resolve().parent.parent


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


# ---------------------------------------------------------------------------
# Main cleaning pipeline
# ---------------------------------------------------------------------------

CLEANERS = {
    "price":            clean_price,
    "mileage":          clean_mileage,
    "engine_capacity":  clean_engine_capacity,
    "cc":               clean_engine_capacity,
    "manufactured_date": clean_year,
    "make":             clean_text,
    "model":            clean_text,
    "variant":          clean_text,
    "family":           clean_text,
    "series":           clean_text,
    "location":         clean_text,
    "condition":        clean_text,
    "car_type":         clean_text,
    "transmission":     clean_text,
    "fuel_type":        clean_text,
    "country_origin":   clean_text,
    "kw":               clean_numeric,
    "torque":           clean_numeric,
    "length":           clean_numeric,
    "width":            clean_numeric,
    "height":           clean_numeric,
    "wheelbase":        clean_numeric,
    "kerbwt":           clean_numeric,
    "fueltk":           clean_numeric,
}


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col, cleaner in CLEANERS.items():
        if col in df.columns:
            df[col] = cleaner(df[col])

    # Drop rows with no identifying information
    id_cols = [c for c in ("ads_id", "make", "model") if c in df.columns]
    if id_cols:
        df = df.dropna(subset=id_cols, how="all")

    # Deduplicate on ads_id, keeping most recent
    if "ads_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset="ads_id", keep="last").reset_index(drop=True)
        removed = before - len(df)
        if removed:
            print(f"Removed {removed} duplicate ads_id rows")

    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean MasterMudahCarData Excel file")
    parser.add_argument("--input", default=str(_ROOT / "data" / "master" / "MasterMudahCarData.xlsx"), help="Input Excel file")
    parser.add_argument("--output", default=None, help="Output Excel file (default: overwrite input)")
    return parser.parse_args()


def main():
    args = parse_args()
    output_path = args.output or args.input

    print(f"Reading {args.input}...")
    df = pd.read_excel(args.input)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    cleaned = clean(df)
    print(f"Cleaned: {len(cleaned)} rows remaining")

    cleaned.to_excel(output_path, index=False)
    print(f"Saved to {output_path}")


if __name__ == "__main__":
    main()

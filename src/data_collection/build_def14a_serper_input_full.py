"""
Build Serper input CSV for ALL directors using DEF 14A primary employment.

For each director in def14a_extracted_bios.csv, uses primary_company as the
Serper search anchor (falls back to board ticker if primary_company is missing).
Deduplicates by (full_name, ticker) to avoid redundant searches for the same
person at the same company across multiple filing years.

Workflow: WRDS names -> DEF 14A primary role -> Serper -> Revelio -> Apify

Usage:
    python3 src/data_collection/build_def14a_serper_input_full.py
"""

import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

BIOS_PATH = PROJECT_ROOT / "data" / "processed" / "def14a_extracted_bios.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "def14a_serper_input_all_directors.csv"


def main():
    print("Building DEF 14A Serper input for ALL directors...")

    if not BIOS_PATH.exists():
        print(f"ERROR: {BIOS_PATH} not found. Run parse_def14a_bios.py first.")
        return 1

    bios_df = pd.read_csv(BIOS_PATH)
    print(f"  Loaded {len(bios_df):,} extracted bios")

    # Filter to directors only
    bios_df = bios_df[bios_df["role_context"] == "director"].copy()
    print(f"  Filtered to {len(bios_df):,} director bios")

    # Drop rows with no name
    bios_df = bios_df[bios_df["full_name"].notna()].copy()

    # Sort by year (descending) to keep most recent filing when deduplicating
    bios_df = bios_df.sort_values("year", ascending=False)

    # Build Serper input
    serper_input = bios_df[["ticker", "full_name", "primary_company", "primary_role", "is_current"]].copy()
    serper_input.columns = ["board_ticker", "person_name", "primary_company", "role", "is_current"]

    # Use primary_company as anchor; fall back to board_ticker if missing
    serper_input["company"] = serper_input["primary_company"].fillna(serper_input["board_ticker"])
    serper_input["match_source"] = serper_input["primary_company"].notna().map(
        {True: "def14a_primary", False: "board_ticker_fallback"}
    )

    # Remove rows with no company at all
    before_company_filter = len(serper_input)
    serper_input = serper_input[serper_input["company"].notna()].copy()
    if before_company_filter != len(serper_input):
        print(f"  Dropped {before_company_filter - len(serper_input):,} rows with no company")

    # Deduplicate by (person_name, board_ticker) — same person at same company across years
    before_dedup = len(serper_input)
    serper_input = serper_input.drop_duplicates(subset=["person_name", "board_ticker"]).copy()
    print(f"  Deduplicated: {before_dedup:,} -> {len(serper_input):,} unique (person, board) pairs")

    serper_input = serper_input[["person_name", "company", "board_ticker", "role", "is_current", "match_source"]]
    serper_input.to_csv(OUTPUT_PATH, index=False)

    n_primary = (serper_input["match_source"] == "def14a_primary").sum()
    n_fallback = (serper_input["match_source"] == "board_ticker_fallback").sum()

    print(f"\nOutput: {OUTPUT_PATH}")
    print(f"  Total rows: {len(serper_input):,}")
    print(f"  With primary_company anchor: {n_primary:,} ({100*n_primary/len(serper_input):.1f}%)")
    print(f"  With board_ticker fallback: {n_fallback:,} ({100*n_fallback/len(serper_input):.1f}%)")
    print(f"\nNext step:")
    print(f"  python3 src/data_collection/find_urls_serper.py \\")
    print(f"    --input {OUTPUT_PATH} \\")
    print(f"    --run --yes")

    return 0


if __name__ == "__main__":
    sys.exit(main())

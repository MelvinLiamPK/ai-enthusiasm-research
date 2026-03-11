"""
Build Individual Blockholders Database
=======================================

Extracts individual (non-institutional) blockholders from the Schwartz-Ziv
and Volkova blockholder dataset, which is compiled from SEC 13D, 13G, and
13F filings. A blockholder is any entity that owns 5% or more of a public
company's shares.

The raw dataset contains 572,310 records covering institutions, individuals,
and other entities from 1993 to 2023. This script filters to individual
blockholders only — typically founders, insiders, and significant personal
investors — and prepares them for LinkedIn profile matching.

Names are converted from SEC filing format (e.g. "CLOUES EDWARD B II") to
title case ("Cloues Edward B II") for compatibility with downstream scripts.

Data source:
    Schwartz-Ziv/Volkova blockholder dataset
    Downloaded from: http://www.evolkova.info/data/blocks/blockholders.csv
    Reference: Schwartz-Ziv & Volkova (2020)

Input columns used:
    blockholder_name  - name of the blockholder (SEC filing format)
    company_name      - company name (SEC filing format)
    year              - year of the filing
    block_type        - "institution", "individual", or "other"
    position          - percentage of shares held
    blockholder_CIK   - SEC Central Index Key for the blockholder
    company_CIK       - SEC Central Index Key for the company

Output columns:
    year              - fiscal year
    company_name      - company name (title case, cleaned)
    blockholder_name  - individual's full name (title case, cleaned)
    pct_shares        - percentage of shares held
    blockholder_CIK   - SEC identifier for the person
    company_CIK       - SEC identifier for the company

Output files (saved to --output directory):
    blockholders_all.csv       - every individual-company-year record
    blockholders_current.csv   - most recent record per person-company pair
    companies.csv              - unique companies in the dataset
    blockholders.db            - SQLite database with indexed tables

Usage:
    python3 build_blockholders.py                            # Default: individuals only
    python3 build_blockholders.py --include-other            # Also include "other" that look individual
    python3 build_blockholders.py --start-year 2015          # 2015 onwards only
    python3 build_blockholders.py --stats                    # Show dataset statistics, no export
    python3 build_blockholders.py --output data/extracted    # Custom output directory

Requirements:
    pip install pandas
    blockholders.csv in data/raw/
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
import argparse
import re


# =========================
# Configuration
# =========================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # src/data_extraction/../../
DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "blockholders.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "extracted" / "blockholders"

START_YEAR = 2010
END_YEAR = 2025

# Keywords that indicate institutional (not individual) holders
INSTITUTIONAL_KEYWORDS = [
    "LLC", "INC", "CORP", "FUND", "CAPITAL", "MANAGEMENT", "GROUP",
    "TRUST", "PARTNER", "ADVISORS", "ADVISORY", "HOLDINGS", "INVESTMENT",
    "BANK", "INSURANCE", "LP", "L.P.", "ASSET", "ASSOCIATES", "VENTURES",
    "FINANCIAL", "SECURITIES", "MUTUAL", "COMPANY", "CO.", "LTD",
    "FOUNDATION", "EQUITY", "WEALTH",
]


# =========================
# Data Loading & Filtering
# =========================

def load_raw(input_path):
    """Load the raw blockholders CSV."""
    input_path = Path(input_path)
    if not input_path.exists():
        print(f"\n✗ Input file not found: {input_path}")
        print(f"\nDownload from: http://www.evolkova.info/data/blocks/blockholders.csv")
        print(f"Place in: {DEFAULT_INPUT}")
        raise SystemExit(1)

    print(f"Loading raw data from: {input_path}")
    df = pd.read_csv(input_path, low_memory=False)
    print(f"  {len(df):,} rows, {df.shape[1]} columns")
    print(f"  Year range: {df['year'].min()}–{df['year'].max()}")
    return df


def print_stats(df):
    """Show dataset statistics without exporting."""
    print("\n" + "=" * 60)
    print("RAW DATASET STATISTICS")
    print("=" * 60)

    print(f"\n  Total rows:          {len(df):,}")
    print(f"  Unique blockholders: {df['blockholder_name'].nunique():,}")
    print(f"  Unique companies:    {df['company_name'].nunique():,}")
    print(f"  Year range:          {df['year'].min()}–{df['year'].max()}")

    print(f"\n  Block type distribution:")
    for bt, n in df["block_type"].value_counts().items():
        pct = 100 * n / len(df)
        print(f"    {bt:15s} {n:>8,} ({pct:5.1f}%)")

    # Check individuals specifically
    indiv = df[df["block_type"] == "individual"]
    print(f"\n  Individual blockholders:")
    print(f"    Records:           {len(indiv):,}")
    print(f"    Unique people:     {indiv['blockholder_name'].nunique():,}")
    print(f"    Unique companies:  {indiv['company_name'].nunique():,}")

    # Check "other" that might be individuals
    other = df[df["block_type"] == "other"]
    pattern = "|".join(INSTITUTIONAL_KEYWORDS)
    other_individual_mask = ~other["blockholder_name"].str.contains(
        pattern, case=False, na=False
    )
    other_indiv_count = other_individual_mask.sum()
    print(f"\n  'Other' category:")
    print(f"    Total:                    {len(other):,}")
    print(f"    Without inst. keywords:   {other_indiv_count:,} (potential individuals)")
    print(f"    With inst. keywords:      {len(other) - other_indiv_count:,} (likely institutional)")

    # Year distribution for individuals
    print(f"\n  Individual records by year (2010+):")
    indiv_recent = indiv[indiv["year"] >= 2010]
    for year, count in indiv_recent.groupby("year").size().items():
        print(f"    {year}: {count:>6,}")


def _looks_institutional(name):
    """Check if a blockholder name contains institutional keywords."""
    if pd.isna(name):
        return True
    name_upper = name.upper()
    for kw in INSTITUTIONAL_KEYWORDS:
        if kw in name_upper:
            return True
    return False


def clean_name(name):
    """
    Convert SEC filing name format to title case.

    "CLOUES EDWARD B II"  → "Cloues Edward B II"
    "NICHOLS JAMES WILLIAM" → "Nichols James William"

    Preserves suffixes like II, III, IV, Jr, Sr.
    """
    if pd.isna(name):
        return name

    # Title case the whole name
    cleaned = name.strip().title()

    # Fix common suffixes that title() mangles
    suffix_fixes = {
        " Ii": " II", " Iii": " III", " Iv": " IV",
        " Jr.": " Jr.", " Sr.": " Sr.",
        " Jr": " Jr", " Sr": " Sr",
    }
    for wrong, right in suffix_fixes.items():
        if cleaned.endswith(wrong):
            cleaned = cleaned[: -len(wrong)] + right

    return cleaned


def clean_company_name(name):
    """Clean company name from SEC format."""
    if pd.isna(name):
        return name
    # SEC names are often uppercase with weird suffixes
    # Title case but preserve common abbreviations
    cleaned = name.strip().title()
    # Fix common patterns
    cleaned = re.sub(r"\bInc\b", "Inc", cleaned)
    cleaned = re.sub(r"\bCorp\b", "Corp", cleaned)
    cleaned = re.sub(r"\bLlc\b", "LLC", cleaned)
    cleaned = re.sub(r"\bLtd\b", "Ltd", cleaned)
    cleaned = re.sub(r"\bLp\b", "LP", cleaned)
    cleaned = re.sub(r"\bL\.p\.\b", "L.P.", cleaned)
    return cleaned


def filter_individuals(df, start_year, end_year, include_other=False):
    """
    Filter to individual blockholders only.

    Args:
        df:             raw DataFrame
        start_year:     first year to include
        end_year:       last year to include
        include_other:  if True, also include "other" block_type entries
                        that don't match institutional name patterns

    Returns:
        Filtered and cleaned DataFrame
    """
    print(f"\nFiltering to individuals, years {start_year}–{end_year}...")

    # Year filter
    mask = (df["year"] >= start_year) & (df["year"] <= end_year)
    df_filtered = df[mask].copy()
    print(f"  After year filter: {len(df_filtered):,} rows")

    # Block type filter
    if include_other:
        # Include "individual" + "other" that don't look institutional
        indiv_mask = df_filtered["block_type"] == "individual"
        other_mask = (df_filtered["block_type"] == "other") & (
            ~df_filtered["blockholder_name"].apply(_looks_institutional)
        )
        combined_mask = indiv_mask | other_mask
        df_filtered = df_filtered[combined_mask].copy()
        n_from_other = other_mask.sum()
        print(f"  Individual block_type:      {indiv_mask.sum():,}")
        print(f"  'Other' included:           {n_from_other:,}")
        print(f"  Total after filter:         {len(df_filtered):,}")
    else:
        df_filtered = df_filtered[df_filtered["block_type"] == "individual"].copy()
        print(f"  After individual filter:    {len(df_filtered):,} rows")

    # Clean names
    print("  Cleaning names...")
    df_filtered["blockholder_name"] = df_filtered["blockholder_name"].apply(clean_name)
    df_filtered["company_name"] = df_filtered["company_name"].apply(clean_company_name)

    # Rename position → pct_shares to avoid confusion with job title
    df_filtered = df_filtered.rename(columns={"position": "pct_shares"})

    # Select and order output columns
    output_cols = [
        "year",
        "company_name",
        "blockholder_name",
        "pct_shares",
        "blockholder_CIK",
        "company_CIK",
    ]
    df_filtered = df_filtered[output_cols].copy()

    print(f"  Unique individuals:  {df_filtered['blockholder_name'].nunique():,}")
    print(f"  Unique companies:    {df_filtered['company_name'].nunique():,}")

    return df_filtered


# =========================
# Output
# =========================

def save_outputs(blockholders_df, output_dir):
    """
    Save blockholders data as CSV files and a SQLite database.

    Creates:
        blockholders_all.csv      - full dataset
        blockholders_current.csv  - most recent year per person-company
        companies.csv             - unique company list
        blockholders.db           - SQLite with indexed tables
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Current blockholders (deduplicated to most recent year) ---
    current_df = (
        blockholders_df
        .sort_values("year", ascending=False)
        .drop_duplicates(subset=["company_name", "blockholder_name"], keep="first")
    )

    # --- Unique companies ---
    companies_df = (
        blockholders_df[["company_CIK", "company_name"]]
        .drop_duplicates()
        .sort_values("company_name")
    )

    # --- CSVs ---
    print("\nSaving CSV files...")

    all_csv = output_dir / "blockholders_all.csv"
    blockholders_df.to_csv(all_csv, index=False)
    print(f"  {all_csv} ({len(blockholders_df):,} rows)")

    current_csv = output_dir / "blockholders_current.csv"
    current_df.to_csv(current_csv, index=False)
    print(f"  {current_csv} ({len(current_df):,} rows)")

    companies_csv = output_dir / "companies.csv"
    companies_df.to_csv(companies_csv, index=False)
    print(f"  {companies_csv} ({len(companies_df):,} rows)")

    # --- SQLite ---
    db_path = output_dir / "blockholders.db"
    print(f"\nCreating SQLite database: {db_path}")

    conn = sqlite3.connect(db_path)
    blockholders_df.to_sql("blockholder_years", conn, if_exists="replace", index=False)
    current_df.to_sql("current_blockholders", conn, if_exists="replace", index=False)
    companies_df.to_sql("companies", conn, if_exists="replace", index=False)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_by_company ON blockholder_years(company_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_by_name ON blockholder_years(blockholder_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_by_year ON blockholder_years(year)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_by_cik ON blockholder_years(blockholder_CIK)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cb_company ON current_blockholders(company_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cb_name ON current_blockholders(blockholder_name)")
    conn.close()
    print("  Done")

    return {
        "blockholders_df": blockholders_df,
        "current_df": current_df,
        "companies_df": companies_df,
    }


def print_summary(results):
    """Print summary statistics."""
    df = results["blockholders_df"]
    current_df = results["current_df"]
    companies_df = results["companies_df"]

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"\n  Total records:              {len(df):,}")
    print(f"  Unique individuals:         {df['blockholder_name'].nunique():,}")
    print(f"  Unique companies:           {len(companies_df):,}")
    print(f"  Current blockholders:       {len(current_df):,}")
    print(f"  Year range:                 {int(df['year'].min())}–{int(df['year'].max())}")

    print(f"\n  Ownership stake distribution (current):")
    pct = current_df["pct_shares"]
    print(f"    Mean:   {pct.mean():.1f}%")
    print(f"    Median: {pct.median():.1f}%")
    print(f"    Min:    {pct.min():.1f}%")
    print(f"    Max:    {pct.max():.1f}%")

    print("\n  Records by year:")
    for year, count in df.groupby("year").size().items():
        print(f"    {year}: {count:>6,}")


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Extract individual blockholders from Schwartz-Ziv/Volkova dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 build_blockholders.py                       # Individuals only, 2010+
  python3 build_blockholders.py --include-other       # Also include ambiguous "other" category
  python3 build_blockholders.py --start-year 2015     # 2015 onwards only
  python3 build_blockholders.py --stats               # Show statistics, no export
  python3 build_blockholders.py --output data/custom  # Custom output directory

Data source: Schwartz-Ziv/Volkova blockholder dataset (SEC filings)
        """,
    )

    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT),
                        help=f"Path to blockholders.csv (default: {DEFAULT_INPUT})")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT),
                        help=f"Output directory (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--stats", action="store_true",
                        help="Show dataset statistics without exporting")
    parser.add_argument("--include-other", action="store_true",
                        help='Also include "other" block_type entries that look individual')
    parser.add_argument("--start-year", type=int, default=START_YEAR,
                        help=f"First year to include (default: {START_YEAR})")
    parser.add_argument("--end-year", type=int, default=END_YEAR,
                        help=f"Last year to include (default: {END_YEAR})")

    args = parser.parse_args()

    scope = "individuals + other" if args.include_other else "individuals only"

    print("=" * 60)
    print("Blockholders Database Builder")
    print("=" * 60)
    print(f"  Source: {args.input}")
    print(f"  Scope:  {scope}")
    print(f"  Years:  {args.start_year}–{args.end_year}")
    print(f"  Output: {args.output}")

    # Load raw data
    raw_df = load_raw(args.input)

    if args.stats:
        print_stats(raw_df)
        print("\n✓ Use --include-other to also capture individuals in the 'other' category.")
        return

    # Filter and clean
    blockholders_df = filter_individuals(
        raw_df, args.start_year, args.end_year, include_other=args.include_other
    )

    if len(blockholders_df) == 0:
        print("\n✗ No records after filtering. Check year range or block_type values.")
        return

    # Save
    results = save_outputs(blockholders_df, args.output)
    print_summary(results)

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)
    print(f"\nOutput saved to: {args.output}/")
    print("\nNext steps:")
    print("  1. Run combine_people.py to merge directors + executives + blockholders")
    print("  2. Run find_urls.py to discover LinkedIn profiles")

    if not args.include_other:
        print("\nNote: Run with --include-other to also capture individuals")
        print("      classified as 'other' in the raw data (for robustness checks).")


if __name__ == "__main__":
    main()
"""
Combine People from All Sources
=================================

Merges the three extracted datasets — directors, executives, and individual
blockholders — into a single unified file of company-person pairs for
LinkedIn profile discovery and post scraping.

Each source is tagged with its origin. People who appear in multiple sources
(e.g. a CEO who is also a board director) are deduplicated into a single row
with all applicable sources and the most informative title retained.

Deduplication logic:
    1. Normalize names (strip whitespace, standardize case)
    2. Match on (company_name, person_name) across sources
    3. When duplicates are found, merge source tags and keep the most
       specific title (executive title > "Director" > "Blockholder")

Input files (from data/extracted/):
    directors/directors_current.csv
    executives/executives_current.csv
    blockholders/blockholders_current.csv

Output columns:
    company_name   - company name
    person_name    - individual's full name
    position       - role/title (e.g. "President, CEO & Director", "Director", "Blockholder")
    source         - origin dataset(s), pipe-separated (e.g. "director|executive")
    gvkey          - Compustat company ID (if available)
    ticker         - stock ticker (if available)
    execid         - ExecuComp person ID (if available)

Output files (saved to --output directory):
    all_people.csv           - full deduplicated dataset
    all_people_stats.txt     - summary statistics

Usage:
    python3 combine_people.py                            # Default paths
    python3 combine_people.py --output data/combined     # Custom output directory
    python3 combine_people.py --stats                    # Show overlap stats, no export

Requirements:
    pip install pandas
"""

import pandas as pd
from pathlib import Path
import argparse
import re


# =========================
# Configuration
# =========================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # src/data_extraction/../../
EXTRACTED_DIR = PROJECT_ROOT / "data" / "extracted"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "extracted" / "combined"


# =========================
# Loading
# =========================

def load_directors(path):
    """Load and normalize directors data."""
    df = pd.read_csv(path)
    print(f"  Directors:    {len(df):,} rows")

    return pd.DataFrame({
        "company_name": df["company_name"],
        "person_name": df["director_name"],
        "position": "Director",
        "source": "director",
        "gvkey": df["gvkey"],
        "ticker": df["ticker"],
        "execid": None,
    })


def load_executives(path):
    """Load and normalize executives data."""
    df = pd.read_csv(path)
    print(f"  Executives:   {len(df):,} rows")

    return pd.DataFrame({
        "company_name": df["company_name"],
        "person_name": df["executive_name"],
        "position": df["title"],
        "source": "executive",
        "gvkey": df["gvkey"],
        "ticker": df["ticker"],
        "execid": df["execid"],
    })


def load_blockholders(path):
    """Load and normalize blockholders data."""
    df = pd.read_csv(path)
    print(f"  Blockholders: {len(df):,} rows")

    return pd.DataFrame({
        "company_name": df["company_name"],
        "person_name": df["blockholder_name"],
        "position": "Blockholder",
        "source": "blockholder",
        "gvkey": None,
        "ticker": None,
        "execid": None,
    })


# =========================
# Normalization & Deduplication
# =========================

def normalize_name(name):
    """
    Normalize a person name for matching across sources.

    Handles:
        - Case differences: "JOHN SMITH" vs "John Smith"
        - Extra whitespace
        - Common suffix variations
    """
    if pd.isna(name):
        return ""
    name = str(name).strip()
    name = re.sub(r"\s+", " ", name)  # collapse whitespace
    name = name.lower()
    return name


def normalize_company(name):
    """
    Normalize a company name for matching across sources.

    ExecuComp and SEC filings use different formats:
        ExecuComp: "Apple Inc." or "APPLE INC"
        SEC:       "APPLE INC" or "Apple Inc"
    """
    if pd.isna(name):
        return ""
    name = str(name).strip()
    name = re.sub(r"\s+", " ", name)
    name = name.lower()
    # Remove common suffixes for matching
    name = re.sub(r"\b(inc\.?|corp\.?|co\.?|ltd\.?|llc|l\.?p\.?)\s*$", "", name).strip()
    return name


def deduplicate(df):
    """
    Deduplicate people who appear in multiple sources.

    For each (company, person) pair:
        - Merge source tags: "director|executive"
        - Keep the most specific title (executive > director > blockholder)
        - Preserve identifiers (gvkey, ticker, execid) from whichever source has them
    """
    print("\nDeduplicating...")

    # Create normalized keys for matching
    df["_name_key"] = df["person_name"].apply(normalize_name)
    df["_company_key"] = df["company_name"].apply(normalize_company)

    # Sort so executive rows come first (they have the best titles),
    # then directors, then blockholders
    source_priority = {"executive": 0, "director": 1, "blockholder": 2}
    df["_priority"] = df["source"].map(source_priority)
    df = df.sort_values("_priority")

    # Group by normalized (company, person) and aggregate
    groups = df.groupby(["_company_key", "_name_key"], sort=False)

    records = []
    for (company_key, name_key), group in groups:
        if len(company_key) == 0 or len(name_key) == 0:
            continue

        # Take the first row (highest priority source) as base
        base = group.iloc[0].copy()

        # Merge sources
        sources = sorted(group["source"].unique())
        base["source"] = "|".join(sources)

        # Fill identifiers from any source that has them
        for col in ["gvkey", "ticker", "execid"]:
            if pd.isna(base[col]):
                non_null = group[col].dropna()
                if len(non_null) > 0:
                    base[col] = non_null.iloc[0]

        records.append(base)

    result = pd.DataFrame(records)

    # Clean up temp columns
    result = result.drop(columns=["_name_key", "_company_key", "_priority"])
    result = result.reset_index(drop=True)

    return result


# =========================
# Stats
# =========================

def print_overlap_stats(df_deduped, n_directors, n_executives, n_blockholders):
    """Show overlap statistics between sources."""
    print("\n" + "=" * 60)
    print("OVERLAP ANALYSIS")
    print("=" * 60)

    n_total_input = n_directors + n_executives + n_blockholders
    n_deduped = len(df_deduped)
    n_removed = n_total_input - n_deduped

    print(f"\n  Input rows:      {n_total_input:,}")
    print(f"  After dedup:     {n_deduped:,}")
    print(f"  Duplicates:      {n_removed:,} ({100*n_removed/n_total_input:.1f}%)")

    print(f"\n  Source combinations:")
    for combo, count in df_deduped["source"].value_counts().items():
        pct = 100 * count / n_deduped
        print(f"    {combo:35s} {count:>7,} ({pct:5.1f}%)")

    # Company coverage
    n_companies = df_deduped["company_name"].nunique()
    has_gvkey = df_deduped["gvkey"].notna().sum()
    print(f"\n  Unique companies:   {n_companies:,}")
    print(f"  Rows with gvkey:    {has_gvkey:,} ({100*has_gvkey/n_deduped:.1f}%)")
    print(f"  Rows without gvkey: {n_deduped - has_gvkey:,} (blockholder-only companies)")


# =========================
# Output
# =========================

def save_outputs(df, output_dir, n_directors, n_executives, n_blockholders):
    """Save combined dataset."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # CSV
    out_csv = output_dir / "all_people.csv"
    df.to_csv(out_csv, index=False)
    print(f"\n  {out_csv} ({len(df):,} rows)")

    # Stats file
    stats_path = output_dir / "all_people_stats.txt"
    with open(stats_path, "w") as f:
        f.write("Combined People Dataset — Summary Statistics\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n\n")

        f.write("Input:\n")
        f.write(f"  Directors:    {n_directors:,}\n")
        f.write(f"  Executives:   {n_executives:,}\n")
        f.write(f"  Blockholders: {n_blockholders:,}\n")
        f.write(f"  Total input:  {n_directors + n_executives + n_blockholders:,}\n\n")

        f.write("Output:\n")
        f.write(f"  Deduplicated rows:  {len(df):,}\n")
        f.write(f"  Unique people:      {df['person_name'].nunique():,}\n")
        f.write(f"  Unique companies:   {df['company_name'].nunique():,}\n\n")

        f.write("Source combinations:\n")
        for combo, count in df["source"].value_counts().items():
            f.write(f"  {combo:35s} {count:>7,}\n")

    print(f"  {stats_path}")


def print_summary(df):
    """Print final summary."""
    print("\n" + "=" * 60)
    print("FINAL DATASET SUMMARY")
    print("=" * 60)
    print(f"\n  Total rows:         {len(df):,}")
    print(f"  Unique people:      {df['person_name'].nunique():,}")
    print(f"  Unique companies:   {df['company_name'].nunique():,}")

    print(f"\n  Position distribution (top 10):")
    for pos, count in df["position"].value_counts().head(10).items():
        print(f"    {pos:45s} {count:>7,}")


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Combine directors, executives, and blockholders into one dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 combine_people.py                            # Default paths
  python3 combine_people.py --stats                    # Show overlap stats only
  python3 combine_people.py --output data/combined     # Custom output directory

Input: data/extracted/{directors,executives,blockholders}/*_current.csv
        """,
    )

    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT),
                        help=f"Output directory (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--stats", action="store_true",
                        help="Show overlap statistics without saving")
    parser.add_argument("--directors", type=str,
                        default=str(EXTRACTED_DIR / "directors" / "directors_current.csv"),
                        help="Path to directors CSV")
    parser.add_argument("--executives", type=str,
                        default=str(EXTRACTED_DIR / "executives" / "executives_current.csv"),
                        help="Path to executives CSV")
    parser.add_argument("--blockholders", type=str,
                        default=str(EXTRACTED_DIR / "blockholders" / "blockholders_current.csv"),
                        help="Path to blockholders CSV")

    args = parser.parse_args()

    print("=" * 60)
    print("Combine People from All Sources")
    print("=" * 60)

    # Load all three sources
    print("\nLoading sources...")

    sources = []
    n_directors = n_executives = n_blockholders = 0

    # Directors
    directors_path = Path(args.directors)
    if directors_path.exists():
        dir_df = load_directors(directors_path)
        n_directors = len(dir_df)
        sources.append(dir_df)
    else:
        print(f"  ✗ Directors not found: {directors_path}")

    # Executives
    executives_path = Path(args.executives)
    if executives_path.exists():
        exec_df = load_executives(executives_path)
        n_executives = len(exec_df)
        sources.append(exec_df)
    else:
        print(f"  ✗ Executives not found: {executives_path}")

    # Blockholders
    blockholders_path = Path(args.blockholders)
    if blockholders_path.exists():
        block_df = load_blockholders(blockholders_path)
        n_blockholders = len(block_df)
        sources.append(block_df)
    else:
        print(f"  ✗ Blockholders not found: {blockholders_path}")

    if len(sources) == 0:
        print("\n✗ No source files found. Run the build scripts first.")
        return

    # Stack all sources
    print(f"\nStacking {len(sources)} sources...")
    combined = pd.concat(sources, ignore_index=True)
    print(f"  Total rows before dedup: {len(combined):,}")

    # Deduplicate
    deduped = deduplicate(combined)

    # Stats
    print_overlap_stats(deduped, n_directors, n_executives, n_blockholders)

    if args.stats:
        print_summary(deduped)
        return

    # Save
    print("\nSaving outputs...")
    save_outputs(deduped, args.output, n_directors, n_executives, n_blockholders)
    print_summary(deduped)

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)
    print(f"\nOutput saved to: {args.output}/")
    print("\nNext steps:")
    print("  1. Run find_urls.py --input all_people.csv to discover LinkedIn profiles")
    print("  2. Run scrape.py to collect posts")


if __name__ == "__main__":
    main()
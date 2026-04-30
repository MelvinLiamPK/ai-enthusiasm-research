"""
Flag Corporate Entities in LinkedIn URL Dataset
================================================

Adds an `is_entity` boolean column to all_linkedin_urls.csv to mark blockholder
rows that are corporate entities rather than individual people (e.g. "Golden Meditech
Co Ltd", "Foundation Capital Management Co. Vi, Llc"). These slipped through the
block_type filter at extraction time and produced garbage URL matches since LinkedIn
/in/ profiles are for individuals only.

Uses the same INSTITUTIONAL_KEYWORDS logic as build_blockholders.py. Only blockholder
rows are checked — directors and executives are always individuals.

Usage:
    python3 flag_entities.py --stats    # Preview counts, no write
    python3 flag_entities.py --run      # Apply and overwrite all_linkedin_urls.csv
"""

import sys
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DEFAULT_INPUT = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls.csv"

INSTITUTIONAL_KEYWORDS = [
    "LLC", "INC", "CORP", "FUND", "CAPITAL", "MANAGEMENT", "GROUP",
    "TRUST", "PARTNER", "ADVISORS", "ADVISORY", "HOLDINGS", "INVESTMENT",
    "BANK", "INSURANCE", "LP", "L.P.", "ASSET", "ASSOCIATES", "VENTURES",
    "FINANCIAL", "SECURITIES", "MUTUAL", "COMPANY", "CO.", "LTD",
    "FOUNDATION", "EQUITY", "WEALTH",
]


def looks_like_entity(name):
    if pd.isna(name):
        return False
    name_upper = str(name).upper()
    return any(kw in name_upper for kw in INSTITUTIONAL_KEYWORDS)


def flag_entities(df):
    is_blockholder = df["source"].str.contains("blockholder", na=False)
    df["is_entity"] = False
    df.loc[is_blockholder, "is_entity"] = df.loc[is_blockholder, "person_name"].apply(looks_like_entity)
    return df


def print_stats(df):
    total = len(df)
    entities = df["is_entity"].sum()
    blockholders = df["source"].str.contains("blockholder", na=False).sum()
    blockholder_entities = (df["is_entity"] & df["source"].str.contains("blockholder", na=False)).sum()
    entities_with_url = (df["is_entity"] & df["linkedin_url"].notna()).sum()

    print(f"\nDataset: {total:,} rows")
    print(f"  Blockholders:           {blockholders:,}")
    print(f"  Entity blockholders:    {blockholder_entities:,} ({100 * blockholder_entities / blockholders:.1f}% of blockholders)")
    print(f"  Entities with URLs:     {entities_with_url:,}  (bad matches)")
    print(f"\nSample entity names:")
    for name in df[df["is_entity"]]["person_name"].dropna().unique()[:15]:
        print(f"  {name}")


def main():
    parser = argparse.ArgumentParser(
        description="Flag corporate entity blockholders in all_linkedin_urls.csv",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Path to all_linkedin_urls.csv")
    parser.add_argument("--stats", action="store_true", help="Preview counts only, do not write")
    parser.add_argument("--run", action="store_true", help="Apply flag and overwrite input file")
    args = parser.parse_args()

    if not args.stats and not args.run:
        parser.print_help()
        sys.exit(0)

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"✗ File not found: {input_path}")
        sys.exit(1)

    print(f"Loading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"  {len(df):,} rows, {len(df.columns)} columns")

    df = flag_entities(df)
    print_stats(df)

    if args.run:
        backup_path = input_path.with_name(
            f"all_linkedin_urls_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        import shutil
        shutil.copy2(input_path, backup_path)
        print(f"\n  Backup saved: {backup_path.name}")

        df.to_csv(input_path, index=False)
        print(f"  ✓ Written: {input_path.name} ({len(df.columns)} columns, is_entity added)")


if __name__ == "__main__":
    main()

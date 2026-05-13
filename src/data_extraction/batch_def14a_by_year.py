"""
Batch DEF 14A by year range to control costs and enable incremental validation.

Creates manifest subsets for Year1, Year2, etc., with estimated costs.
Allows running validation batches before committing to full run.

Usage:
    python3 src/data_extraction/batch_def14a_by_year.py --years 2022-2025 --output batch_2022_2025.csv
"""

import argparse
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MANIFEST_PATH = PROJECT_ROOT / "data" / "raw" / "def14a" / "manifest.csv"

COST_PER_FILING = 0.0075  # rough estimate based on 5k input + 500 output


def main():
    parser = argparse.ArgumentParser(
        description="Create year-based batches of DEF 14A filings for incremental processing."
    )
    parser.add_argument(
        "--years",
        type=str,
        default="2022-2025",
        help="Year range (e.g. '2022-2025' or '2025' or '2023,2024,2025')",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output manifest CSV (default: batch_YEARS.csv)",
    )
    args = parser.parse_args()

    if not MANIFEST_PATH.exists():
        print(f"Manifest not found: {MANIFEST_PATH}")
        print("Run scrape_def14a.py --mode full --run --yes first")
        return 1

    # Parse year range
    if "," in args.years:
        years = [int(y.strip()) for y in args.years.split(",")]
    elif "-" in args.years:
        start, end = args.years.split("-")
        years = list(range(int(start), int(end) + 1))
    else:
        years = [int(args.years)]

    # Load full manifest
    df = pd.read_csv(MANIFEST_PATH)
    df["year"] = df["year"].astype(int)

    # Filter to selected years
    batch = df[df["year"].isin(years)].copy()
    batch = batch.sort_values(["year", "ticker"]).reset_index(drop=True)

    # Estimate cost
    num_filings = len(batch)
    estimated_cost = num_filings * COST_PER_FILING

    print(f"\nBatch: {','.join(map(str, years))}")
    print(f"  Filings: {num_filings:,}")
    print(f"  Estimated cost: ${estimated_cost:.2f}")
    print(f"  Years covered: {batch['year'].min()}-{batch['year'].max()}")
    print(f"  Unique tickers: {batch['ticker'].nunique()}")

    # Save batch manifest
    if args.output:
        output_path = Path(args.output)
    else:
        year_str = "-".join(map(str, sorted(set(years))))
        output_path = PROJECT_ROOT / "data" / "raw" / "def14a" / f"batch_{year_str}.csv"

    batch.to_csv(output_path, index=False)
    print(f"\nBatch manifest saved: {output_path}")
    print(f"\nTo process this batch:")
    print(f"  1. Scrape:  python3 src/data_extraction/scrape_def14a.py --manifest {output_path} --mode full --run --yes")
    print(f"  2. Parse:   python3 src/data_extraction/parse_def14a_bios.py --manifest {output_path} --mode full --workers 10")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

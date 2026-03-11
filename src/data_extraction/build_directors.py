"""
Build Directors Database from WRDS ExecuComp
=============================================

Extracts all board director records from the WRDS ExecuComp Director
Compensation database (execcomp.directorcomp). This table covers directors
of approximately 1,500 publicly traded US firms (roughly the S&P 1500
universe) from 2006 to present.

Each row in the output represents one director serving at one company in
one year. Directors who serve on multiple boards appear as separate rows.

Data source:
    WRDS ExecuComp → execcomp.directorcomp

Output columns:
    year           - fiscal year of the directorship record
    gvkey          - Compustat Global Company Key (unique, persistent company ID)
    ticker         - stock ticker symbol
    cusip          - CUSIP identifier (note: often empty in this table)
    company_name   - company name as reported in ExecuComp
    director_name  - director's full name

Output files (saved to --output directory):
    directors_all.csv         - every director-company-year record
    directors_current.csv     - most recent record per director-company pair
    companies.csv             - unique companies in the dataset
    directors.db              - SQLite database with indexed tables

Usage:
    python3 build_directors.py                          # Full run, default years (2010-2025)
    python3 build_directors.py --start-year 2020        # Only 2020 onwards
    python3 build_directors.py --output data/extracted  # Custom output directory
    python3 build_directors.py --explore                # Inspect table structure

Requirements:
    pip install wrds pandas
    WRDS account with access to ExecuComp
"""

import wrds
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
import argparse


# =========================
# Configuration
# =========================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # src/data_extraction/../../
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "extracted" / "directors"

START_YEAR = 2010
END_YEAR = 2025


# =========================
# WRDS Queries
# =========================

def explore_tables(db):
    """Inspect execcomp.directorcomp structure and sample data."""
    print("=" * 60)
    print("EXPLORING execcomp.directorcomp")
    print("=" * 60)

    print("\n[1] Columns:")
    try:
        cols = db.raw_sql("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'execcomp'
              AND table_name = 'directorcomp'
            ORDER BY ordinal_position
        """)
        for _, row in cols.iterrows():
            print(f"  {row['column_name']:30s} {row['data_type']}")
    except Exception as e:
        print(f"  Error: {e}")

    print("\n[2] Year range and record counts:")
    try:
        counts = db.raw_sql("""
            SELECT year,
                   COUNT(*) as records,
                   COUNT(DISTINCT dirname) as directors,
                   COUNT(DISTINCT coname) as companies
            FROM execcomp.directorcomp
            GROUP BY year
            ORDER BY year
        """)
        print(counts.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")

    print("\n[3] Sample rows (2024):")
    try:
        sample = db.raw_sql("""
            SELECT year, gvkey, ticker, coname, dirname
            FROM execcomp.directorcomp
            WHERE year = 2024
            ORDER BY coname, dirname
            LIMIT 10
        """)
        print(sample.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")


def get_all_directors(db, start_year, end_year):
    """
    Pull all director records from ExecuComp.

    Args:
        db:         WRDS connection
        start_year: first year to include
        end_year:   last year to include

    Returns:
        DataFrame with columns: year, gvkey, ticker, cusip, company_name, director_name
    """
    print(f"\nQuerying execcomp.directorcomp for years {start_year}–{end_year}...")

    query = f"""
        SELECT DISTINCT
            year,
            gvkey,
            ticker,
            cusip,
            coname   AS company_name,
            dirname  AS director_name
        FROM execcomp.directorcomp
        WHERE year >= {start_year}
          AND year <= {end_year}
        ORDER BY year, company_name, director_name
    """

    df = db.raw_sql(query)
    print(f"  Retrieved {len(df):,} director-company-year records")
    print(f"  Unique directors:  {df['director_name'].nunique():,}")
    print(f"  Unique companies:  {df['company_name'].nunique():,}")
    print(f"  Year range:        {int(df['year'].min())}–{int(df['year'].max())}")

    return df


# =========================
# Output
# =========================

def save_outputs(directors_df, output_dir):
    """
    Save directors data as CSV files and a SQLite database.

    Creates:
        directors_all.csv      - full dataset
        directors_current.csv  - most recent year per director-company
        companies.csv          - unique company list
        directors.db           - SQLite with indexed tables
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Current directors (deduplicated to most recent year) ---
    current_df = (
        directors_df
        .sort_values("year", ascending=False)
        .drop_duplicates(subset=["company_name", "director_name"], keep="first")
    )

    # --- Unique companies ---
    companies_df = (
        directors_df[["gvkey", "ticker", "company_name"]]
        .drop_duplicates()
        .sort_values("company_name")
    )

    # --- CSVs ---
    print("\nSaving CSV files...")

    all_csv = output_dir / "directors_all.csv"
    directors_df.to_csv(all_csv, index=False)
    print(f"  {all_csv} ({len(directors_df):,} rows)")

    current_csv = output_dir / "directors_current.csv"
    current_df.to_csv(current_csv, index=False)
    print(f"  {current_csv} ({len(current_df):,} rows)")

    companies_csv = output_dir / "companies.csv"
    companies_df.to_csv(companies_csv, index=False)
    print(f"  {companies_csv} ({len(companies_df):,} rows)")

    # --- SQLite ---
    db_path = output_dir / "directors.db"
    print(f"\nCreating SQLite database: {db_path}")

    conn = sqlite3.connect(db_path)
    directors_df.to_sql("director_years", conn, if_exists="replace", index=False)
    current_df.to_sql("current_directors", conn, if_exists="replace", index=False)
    companies_df.to_sql("companies", conn, if_exists="replace", index=False)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_dy_company ON director_years(company_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dy_director ON director_years(director_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dy_year ON director_years(year)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dy_gvkey ON director_years(gvkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cd_company ON current_directors(company_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cd_director ON current_directors(director_name)")
    conn.close()
    print("  Done")

    return {
        "directors_df": directors_df,
        "current_df": current_df,
        "companies_df": companies_df,
    }


def print_summary(results):
    """Print summary statistics."""
    directors_df = results["directors_df"]
    current_df = results["current_df"]
    companies_df = results["companies_df"]

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"\n  Total records:          {len(directors_df):,}")
    print(f"  Unique directors:       {directors_df['director_name'].nunique():,}")
    print(f"  Unique companies:       {len(companies_df):,}")
    print(f"  Current directors:      {len(current_df):,}")
    print(f"  Year range:             {int(directors_df['year'].min())}–{int(directors_df['year'].max())}")

    print("\n  Records by year:")
    for year, count in directors_df.groupby("year").size().items():
        print(f"    {int(year)}: {count:>7,}")


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Extract all board directors from WRDS ExecuComp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 build_directors.py                          # Full run
  python3 build_directors.py --start-year 2020        # 2020 onwards only
  python3 build_directors.py --output data/extracted  # Custom output directory
  python3 build_directors.py --explore                # Inspect table structure

Data source: execcomp.directorcomp (~1,500 firms, S&P 1500 universe)
        """,
    )

    parser.add_argument("--explore", action="store_true",
                        help="Inspect table structure and sample data, then exit")
    parser.add_argument("--start-year", type=int, default=START_YEAR,
                        help=f"First year to include (default: {START_YEAR})")
    parser.add_argument("--end-year", type=int, default=END_YEAR,
                        help=f"Last year to include (default: {END_YEAR})")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT),
                        help=f"Output directory (default: {DEFAULT_OUTPUT})")

    args = parser.parse_args()

    print("=" * 60)
    print("Directors Database Builder")
    print("=" * 60)
    print(f"  Source: execcomp.directorcomp (all firms)")
    print(f"  Years:  {args.start_year}–{args.end_year}")
    print(f"  Output: {args.output}")

    # Connect to WRDS
    print("\nConnecting to WRDS...")
    try:
        db = wrds.Connection()
        print("  Connected")
    except Exception as e:
        print(f"\n✗ Could not connect to WRDS: {e}")
        print("\nCheck that you have:")
        print("  1. pip install wrds")
        print("  2. WRDS credentials in ~/.pgpass or environment")
        print("  3. Valid WRDS account with ExecuComp access")
        return

    try:
        if args.explore:
            explore_tables(db)
            return

        # Pull data
        directors_df = get_all_directors(db, args.start_year, args.end_year)

        if len(directors_df) == 0:
            print("\n✗ No records returned. Check year range or WRDS access.")
            return

        # Save
        results = save_outputs(directors_df, args.output)
        print_summary(results)

        print("\n" + "=" * 60)
        print("DONE")
        print("=" * 60)
        print(f"\nOutput saved to: {args.output}/")
        print("\nNext steps:")
        print("  1. Run build_executives.py for executive data")
        print("  2. Run build_blockholders.py for blockholder data")
        print("  3. Run combine_people.py to merge all sources")
        print("  4. Run find_urls.py to discover LinkedIn profiles")

    finally:
        db.close()
        print("\nWRDS connection closed.")


if __name__ == "__main__":
    main()
"""
Build Executives Database from WRDS ExecuComp
===============================================

Extracts named executive officer records from the WRDS ExecuComp Annual
Compensation database (execcomp.anncomp). This table covers the top
compensated executives (typically 5 per firm) of approximately 2,000
publicly traded US firms from 1992 to present.

Each row in the output represents one executive at one company in one year.
Executives serving at multiple companies appear as separate rows. The
dataset includes a unique person identifier (execid) which can be used for
deduplication across firms and years.

Data source:
    WRDS ExecuComp → execcomp.anncomp

Output columns:
    year              - fiscal year
    gvkey             - Compustat Global Company Key (persistent company ID)
    ticker            - stock ticker symbol
    company_name      - company name as reported in ExecuComp
    executive_name    - executive's full name
    execid            - ExecuComp unique person identifier
    title             - detailed role/title (e.g. "President, CEO & Director")
    is_ceo            - True if flagged as CEO in that year
    is_director       - True if the executive also sits on the board
    gender            - gender as reported in ExecuComp

Output files (saved to --output directory):
    executives_all.csv       - every executive-company-year record
    executives_current.csv   - most recent record per executive-company pair
    companies.csv            - unique companies in the dataset
    executives.db            - SQLite database with indexed tables

Usage:
    python3 build_executives.py                          # Full run, default years
    python3 build_executives.py --start-year 2020        # Only 2020 onwards
    python3 build_executives.py --ceo-only               # Only CEOs
    python3 build_executives.py --output data/extracted   # Custom output directory
    python3 build_executives.py --explore                 # Inspect table structure

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
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "extracted" / "executives"

START_YEAR = 2010
END_YEAR = 2025


# =========================
# WRDS Queries
# =========================

def explore_tables(db):
    """Inspect execcomp.anncomp structure and sample data."""
    print("=" * 60)
    print("EXPLORING execcomp.anncomp")
    print("=" * 60)

    print("\n[1] Key columns:")
    try:
        cols = db.raw_sql("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'execcomp'
              AND table_name = 'anncomp'
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
                   COUNT(DISTINCT exec_fullname) as executives,
                   COUNT(DISTINCT coname) as companies
            FROM execcomp.anncomp
            GROUP BY year
            ORDER BY year
        """)
        print(counts.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")

    print("\n[3] Top 20 titles (2020+):")
    try:
        titles = db.raw_sql("""
            SELECT titleann, COUNT(*) as n
            FROM execcomp.anncomp
            WHERE year >= 2020
            GROUP BY titleann
            ORDER BY n DESC
            LIMIT 20
        """)
        print(titles.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")

    print("\n[4] CEO flag distribution (2020+):")
    try:
        ceo = db.raw_sql("""
            SELECT ceoann, COUNT(*) as n
            FROM execcomp.anncomp
            WHERE year >= 2020
            GROUP BY ceoann
        """)
        print(ceo.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")

    print("\n[5] Sample rows (2024):")
    try:
        sample = db.raw_sql("""
            SELECT year, gvkey, ticker, coname, exec_fullname,
                   execid, titleann, ceoann, execdir, gender
            FROM execcomp.anncomp
            WHERE year = 2024
            ORDER BY coname, exec_fullname
            LIMIT 10
        """)
        print(sample.to_string(index=False))
    except Exception as e:
        print(f"  Error: {e}")


def get_all_executives(db, start_year, end_year, ceo_only=False):
    """
    Pull all executive records from ExecuComp.

    Args:
        db:         WRDS connection
        start_year: first year to include
        end_year:   last year to include
        ceo_only:   if True, only return rows flagged as CEO

    Returns:
        DataFrame with executive records
    """
    ceo_filter = "AND ceoann = 'CEO'" if ceo_only else ""
    label = "CEOs only" if ceo_only else "all executives"

    print(f"\nQuerying execcomp.anncomp for years {start_year}–{end_year} ({label})...")

    query = f"""
        SELECT DISTINCT
            year,
            gvkey,
            ticker,
            coname      AS company_name,
            exec_fullname AS executive_name,
            execid,
            titleann    AS title,
            ceoann,
            execdir,
            gender
        FROM execcomp.anncomp
        WHERE year >= {start_year}
          AND year <= {end_year}
          {ceo_filter}
        ORDER BY year, company_name, executive_name
    """

    df = db.raw_sql(query)

    # Clean up boolean-like columns
    df["is_ceo"] = df["ceoann"] == "CEO"
    df["is_director"] = df["execdir"] == 1
    df.drop(columns=["ceoann", "execdir"], inplace=True)

    # Fill missing titles
    df["title"] = df["title"].fillna("Executive (title not reported)")

    print(f"  Retrieved {len(df):,} executive-company-year records")
    print(f"  Unique executives: {df['executive_name'].nunique():,}")
    print(f"  Unique execids:    {df['execid'].nunique():,}")
    print(f"  Unique companies:  {df['company_name'].nunique():,}")
    print(f"  Year range:        {int(df['year'].min())}–{int(df['year'].max())}")
    print(f"  CEOs:              {df['is_ceo'].sum():,}")
    print(f"  Also on board:     {df['is_director'].sum():,}")

    return df


# =========================
# Output
# =========================

def save_outputs(executives_df, output_dir):
    """
    Save executives data as CSV files and a SQLite database.

    Creates:
        executives_all.csv      - full dataset
        executives_current.csv  - most recent year per executive-company
        companies.csv           - unique company list
        executives.db           - SQLite with indexed tables
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Current executives (deduplicated to most recent year) ---
    current_df = (
        executives_df
        .sort_values("year", ascending=False)
        .drop_duplicates(subset=["company_name", "executive_name"], keep="first")
    )

    # --- Unique companies ---
    companies_df = (
        executives_df[["gvkey", "ticker", "company_name"]]
        .drop_duplicates()
        .sort_values("company_name")
    )

    # --- CSVs ---
    print("\nSaving CSV files...")

    all_csv = output_dir / "executives_all.csv"
    executives_df.to_csv(all_csv, index=False)
    print(f"  {all_csv} ({len(executives_df):,} rows)")

    current_csv = output_dir / "executives_current.csv"
    current_df.to_csv(current_csv, index=False)
    print(f"  {current_csv} ({len(current_df):,} rows)")

    companies_csv = output_dir / "companies.csv"
    companies_df.to_csv(companies_csv, index=False)
    print(f"  {companies_csv} ({len(companies_df):,} rows)")

    # --- SQLite ---
    db_path = output_dir / "executives.db"
    print(f"\nCreating SQLite database: {db_path}")

    conn = sqlite3.connect(db_path)
    executives_df.to_sql("executive_years", conn, if_exists="replace", index=False)
    current_df.to_sql("current_executives", conn, if_exists="replace", index=False)
    companies_df.to_sql("companies", conn, if_exists="replace", index=False)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_ey_company ON executive_years(company_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ey_exec ON executive_years(executive_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ey_execid ON executive_years(execid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ey_year ON executive_years(year)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ey_gvkey ON executive_years(gvkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ce_company ON current_executives(company_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ce_exec ON current_executives(executive_name)")
    conn.close()
    print("  Done")

    return {
        "executives_df": executives_df,
        "current_df": current_df,
        "companies_df": companies_df,
    }


def print_summary(results):
    """Print summary statistics."""
    df = results["executives_df"]
    current_df = results["current_df"]
    companies_df = results["companies_df"]

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"\n  Total records:          {len(df):,}")
    print(f"  Unique executives:      {df['executive_name'].nunique():,}")
    print(f"  Unique execids:         {df['execid'].nunique():,}")
    print(f"  Unique companies:       {len(companies_df):,}")
    print(f"  Current executives:     {len(current_df):,}")
    print(f"  Year range:             {int(df['year'].min())}–{int(df['year'].max())}")

    print(f"\n  CEOs:                   {df['is_ceo'].sum():,} records ({current_df['is_ceo'].sum():,} current)")
    print(f"  Also board directors:   {df['is_director'].sum():,} records")

    if "gender" in df.columns:
        gender_counts = current_df["gender"].value_counts()
        print(f"\n  Gender (current executives):")
        for g, n in gender_counts.items():
            print(f"    {g}: {n:,}")

    print("\n  Records by year:")
    for year, count in df.groupby("year").size().items():
        print(f"    {int(year)}: {count:>7,}")


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Extract named executive officers from WRDS ExecuComp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 build_executives.py                          # Full run
  python3 build_executives.py --ceo-only               # Only CEOs
  python3 build_executives.py --start-year 2020        # 2020 onwards only
  python3 build_executives.py --output data/extracted   # Custom output directory
  python3 build_executives.py --explore                 # Inspect table structure

Data source: execcomp.anncomp (~2,000 firms, top 5 execs per firm)
        """,
    )

    parser.add_argument("--explore", action="store_true",
                        help="Inspect table structure and sample data, then exit")
    parser.add_argument("--ceo-only", action="store_true",
                        help="Only extract CEO records")
    parser.add_argument("--start-year", type=int, default=START_YEAR,
                        help=f"First year to include (default: {START_YEAR})")
    parser.add_argument("--end-year", type=int, default=END_YEAR,
                        help=f"Last year to include (default: {END_YEAR})")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT),
                        help=f"Output directory (default: {DEFAULT_OUTPUT})")

    args = parser.parse_args()

    label = "CEOs only" if args.ceo_only else "all executives"

    print("=" * 60)
    print("Executives Database Builder")
    print("=" * 60)
    print(f"  Source: execcomp.anncomp ({label})")
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
        executives_df = get_all_executives(
            db, args.start_year, args.end_year, ceo_only=args.ceo_only
        )

        if len(executives_df) == 0:
            print("\n✗ No records returned. Check year range or WRDS access.")
            return

        # Save
        results = save_outputs(executives_df, args.output)
        print_summary(results)

        print("\n" + "=" * 60)
        print("DONE")
        print("=" * 60)
        print(f"\nOutput saved to: {args.output}/")
        print("\nNext steps:")
        print("  1. Run build_blockholders.py for blockholder data")
        print("  2. Run combine_people.py to merge all sources")
        print("  3. Run find_urls.py to discover LinkedIn profiles")

    finally:
        db.close()
        print("\nWRDS connection closed.")


if __name__ == "__main__":
    main()
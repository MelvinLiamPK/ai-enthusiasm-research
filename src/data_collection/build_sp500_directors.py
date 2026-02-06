"""
Build S&P 500 Directors Database
=================================
This script:
1. Pulls S&P 500 constituents as of end 2025 from CRSP
2. Links PERMNO to GVKEY via CRSP/Compustat merged database
3. Pulls director data for only S&P 500 companies
4. Exports filtered datasets

Data Sources:
- CRSP: crsp.msp500list (monthly S&P 500 constituents)
- CRSP/Compustat Link: crsp.ccmxpf_linktable
- ExecuComp: execcomp.directorcomp (director data)

Usage:
    python3 build_sp500_directors.py                    # Full run
    python3 build_sp500_directors.py --explore          # Just explore available tables
    python3 build_sp500_directors.py --date 2024-12-31  # Specific date for S&P 500

Requirements:
    pip install wrds pandas
    WRDS account with access to CRSP and ExecuComp
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
DATA_DIR = Path("./data")
OUTPUT_DIR = Path("./outputs")

# S&P 500 reference date (last trading day of 2025)
# Note: CRSP data updates annually, so 2025 data may not be available yet
# The script will use the most recent available data
SP500_DATE = "2025-12-31"  # Will fallback to latest available

# Year range for director data
START_YEAR = 2015
END_YEAR = 2025


def explore_tables(db):
    """
    Explore available CRSP tables for S&P 500 data.
    Useful for debugging and understanding data structure.
    """
    print("=" * 60)
    print("EXPLORING WRDS TABLES")
    print("=" * 60)
    
    # Check CRSP index tables
    print("\n[1] Checking CRSP index tables...")
    try:
        tables = db.raw_sql("""
            SELECT DISTINCT table_name 
            FROM information_schema.columns 
            WHERE table_schema = 'crsp' 
            AND table_name LIKE '%sp500%'
            ORDER BY table_name
        """)
        print("S&P 500 related tables in crsp schema:")
        for t in tables['table_name']:
            print(f"  - crsp.{t}")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Check crsp_a_indexes schema
    print("\n[2] Checking crsp_a_indexes schema...")
    try:
        tables = db.raw_sql("""
            SELECT DISTINCT table_name 
            FROM information_schema.columns 
            WHERE table_schema = 'crsp_a_indexes'
            AND table_name LIKE '%sp500%'
            ORDER BY table_name
        """)
        print("S&P 500 tables in crsp_a_indexes schema:")
        for t in tables['table_name']:
            print(f"  - crsp_a_indexes.{t}")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Check msp500list columns
    print("\n[3] Checking crsp.msp500list structure...")
    try:
        cols = db.raw_sql("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = 'crsp' 
            AND table_name = 'msp500list'
            ORDER BY ordinal_position
        """)
        print("Columns in crsp.msp500list:")
        for _, row in cols.iterrows():
            print(f"  - {row['column_name']}: {row['data_type']}")
    except Exception as e:
        print(f"  Error: {e}")
    
    # Sample data from msp500list
    print("\n[4] Sample data from crsp.msp500list...")
    try:
        sample = db.raw_sql("""
            SELECT * FROM crsp.msp500list
            ORDER BY ending DESC
            LIMIT 10
        """)
        print(sample.to_string())
    except Exception as e:
        print(f"  Error: {e}")
    
    # Check CRSP/Compustat link table
    print("\n[5] Checking CRSP/Compustat link table...")
    try:
        cols = db.raw_sql("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = 'crsp' 
            AND table_name = 'ccmxpf_linktable'
            ORDER BY ordinal_position
        """)
        print("Columns in crsp.ccmxpf_linktable:")
        for _, row in cols.iterrows():
            print(f"  - {row['column_name']}: {row['data_type']}")
    except Exception as e:
        print(f"  Error: {e}")


def get_sp500_constituents(db, as_of_date=None):
    """
    Get S&P 500 constituents as of a specific date.
    
    Uses crsp.msp500list which contains monthly S&P 500 membership.
    - permno: CRSP permanent security identifier
    - start: membership start date
    - ending: membership end date (ongoing members have future date)
    
    Args:
        db: WRDS connection
        as_of_date: Date string (YYYY-MM-DD) for S&P 500 membership
                   If None, uses most recent available
    
    Returns:
        DataFrame with S&P 500 constituents
    """
    print("\n" + "=" * 60)
    print("STEP 1: Getting S&P 500 Constituents")
    print("=" * 60)
    
    # First, find the most recent date available
    print("\n[1.1] Finding most recent S&P 500 data...")
    max_date_query = """
        SELECT MAX(ending) as max_date
        FROM crsp.msp500list
    """
    max_date_df = db.raw_sql(max_date_query)
    max_date = max_date_df['max_date'].iloc[0]
    print(f"      Most recent date in data: {max_date}")
    
    # Use requested date or fall back to most recent
    if as_of_date:
        target_date = as_of_date
        print(f"      Requested date: {target_date}")
    else:
        target_date = str(max_date)[:10]
        print(f"      Using most recent date: {target_date}")
    
    # Get constituents as of target date
    # A company is in S&P 500 on date X if start <= X <= ending
    print(f"\n[1.2] Querying S&P 500 members as of {target_date}...")
    
    query = f"""
        SELECT DISTINCT 
            permno,
            start as sp500_start,
            ending as sp500_end
        FROM crsp.msp500list
        WHERE start <= '{target_date}'
          AND ending >= '{target_date}'
        ORDER BY permno
    """
    
    sp500_df = db.raw_sql(query)
    print(f"      Found {len(sp500_df)} S&P 500 constituents")
    
    return sp500_df, target_date


def link_permno_to_gvkey(db, sp500_df):
    """
    Link CRSP PERMNO to Compustat GVKEY using the CCM link table.
    
    The link table (crsp.ccmxpf_linktable) maps PERMNO to GVKEY with
    date ranges indicating when the link is valid.
    
    Args:
        db: WRDS connection
        sp500_df: DataFrame with permno column
    
    Returns:
        DataFrame with permno, gvkey, and company info
    """
    print("\n" + "=" * 60)
    print("STEP 2: Linking PERMNO to GVKEY")
    print("=" * 60)
    
    permnos = sp500_df['permno'].tolist()
    permno_str = ','.join(str(p) for p in permnos)
    
    print(f"\n[2.1] Querying CRSP/Compustat link table...")
    
    # Get the link table data for our PERMNOs
    # Using linktype LC and LU (primary links) and active link (linkprim P, C)
    query = f"""
        SELECT DISTINCT
            a.lpermno as permno,
            a.gvkey,
            a.linktype,
            a.linkprim,
            a.linkdt,
            a.linkenddt
        FROM crsp.ccmxpf_linktable a
        WHERE a.lpermno IN ({permno_str})
          AND a.linktype IN ('LC', 'LU')
          AND a.linkprim IN ('P', 'C')
        ORDER BY a.lpermno, a.linkdt DESC
    """
    
    link_df = db.raw_sql(query)
    print(f"      Found {len(link_df)} link records")
    
    # Keep the most recent valid link per PERMNO
    # (in case of multiple links, prefer the most recent)
    link_df = link_df.sort_values(['permno', 'linkdt'], ascending=[True, False])
    link_df = link_df.drop_duplicates(subset=['permno'], keep='first')
    
    print(f"      After deduplication: {len(link_df)} unique PERMNO-GVKEY links")
    
    # Merge with S&P 500 data
    merged_df = sp500_df.merge(link_df[['permno', 'gvkey']], on='permno', how='left')
    
    # Check for unmatched
    unmatched = merged_df['gvkey'].isna().sum()
    if unmatched > 0:
        print(f"      Warning: {unmatched} PERMNOs could not be linked to GVKEY")
    
    return merged_df


def get_company_info(db, gvkeys):
    """
    Get company names and tickers from Compustat.
    
    Args:
        db: WRDS connection
        gvkeys: List of GVKEYs
    
    Returns:
        DataFrame with gvkey, company name, ticker
    """
    print("\n" + "=" * 60)
    print("STEP 3: Getting Company Information")
    print("=" * 60)
    
    gvkey_str = ','.join(f"'{g}'" for g in gvkeys if pd.notna(g))
    
    print(f"\n[3.1] Querying Compustat for company info...")
    
    # Get company names from Compustat fundamentals
    query = f"""
        SELECT DISTINCT
            gvkey,
            conm as company_name,
            tic as ticker
        FROM comp.company
        WHERE gvkey IN ({gvkey_str})
    """
    
    try:
        company_df = db.raw_sql(query)
        print(f"      Found info for {len(company_df)} companies")
    except Exception as e:
        print(f"      Error querying comp.company: {e}")
        print("      Trying alternative table...")
        
        # Try execcomp as fallback
        query = f"""
            SELECT DISTINCT
                gvkey,
                coname as company_name,
                ticker
            FROM execcomp.co_ifndq
            WHERE gvkey IN ({gvkey_str})
        """
        try:
            company_df = db.raw_sql(query)
            print(f"      Found info for {len(company_df)} companies (from execcomp)")
        except:
            # Final fallback: get from directorcomp itself
            print("      Using directorcomp for company info...")
            company_df = pd.DataFrame({'gvkey': gvkeys})
    
    return company_df


def get_sp500_directors(db, sp500_companies_df, start_year, end_year):
    """
    Get director data for S&P 500 companies only.
    
    Args:
        db: WRDS connection
        sp500_companies_df: DataFrame with gvkey column for S&P 500 companies
        start_year: Start year for director data
        end_year: End year for director data
    
    Returns:
        DataFrame with director data for S&P 500 companies
    """
    print("\n" + "=" * 60)
    print("STEP 4: Getting Director Data for S&P 500 Companies")
    print("=" * 60)
    
    gvkeys = sp500_companies_df['gvkey'].dropna().unique().tolist()
    gvkey_str = ','.join(f"'{g}'" for g in gvkeys)
    
    print(f"\n[4.1] Querying director data for {len(gvkeys)} companies...")
    print(f"      Year range: {start_year} - {end_year}")
    
    query = f"""
        SELECT DISTINCT
            year,
            gvkey,
            ticker,
            cusip,
            coname as company_name,
            dirname as director_name
        FROM execcomp.directorcomp
        WHERE gvkey IN ({gvkey_str})
          AND year >= {start_year} 
          AND year <= {end_year}
        ORDER BY year, coname, dirname
    """
    
    directors_df = db.raw_sql(query)
    print(f"      Retrieved {len(directors_df):,} director-company-year records")
    
    return directors_df


def create_database(directors_df, sp500_df, output_dir):
    """
    Create SQLite database and export CSVs.
    """
    print("\n" + "=" * 60)
    print("STEP 5: Creating Output Files")
    print("=" * 60)
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create SQLite database
    db_path = output_dir / "sp500_directors.db"
    print(f"\n[5.1] Creating database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    # Save full director data
    directors_df.to_sql('director_years', conn, if_exists='replace', index=False)
    
    # Create current directors view (most recent year per director-company)
    current_df = directors_df.sort_values('year', ascending=False).drop_duplicates(
        subset=['company_name', 'director_name'], keep='first'
    )
    current_df.to_sql('current_directors', conn, if_exists='replace', index=False)
    
    # Save S&P 500 companies
    companies_df = directors_df[['gvkey', 'ticker', 'company_name']].drop_duplicates()
    companies_df.to_sql('sp500_companies', conn, if_exists='replace', index=False)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dy_company ON director_years(company_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dy_director ON director_years(director_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dy_year ON director_years(year)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dy_gvkey ON director_years(gvkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cd_company ON current_directors(company_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cd_director ON current_directors(director_name)")
    
    conn.close()
    
    # Export CSVs
    print("\n[5.2] Exporting CSV files...")
    
    directors_csv = output_dir / "sp500_directors.csv"
    directors_df.to_csv(directors_csv, index=False)
    print(f"      {directors_csv} ({len(directors_df):,} rows)")
    
    current_csv = output_dir / "sp500_current_directors.csv"
    current_df.to_csv(current_csv, index=False)
    print(f"      {current_csv} ({len(current_df):,} rows)")
    
    companies_csv = output_dir / "sp500_companies.csv"
    companies_df.to_csv(companies_csv, index=False)
    print(f"      {companies_csv} ({len(companies_df):,} rows)")
    
    # Save S&P 500 constituent list with all linking info
    sp500_csv = output_dir / "sp500_constituents.csv"
    sp500_df.to_csv(sp500_csv, index=False)
    print(f"      {sp500_csv} ({len(sp500_df):,} rows)")
    
    return {
        'directors_df': directors_df,
        'current_df': current_df,
        'companies_df': companies_df,
        'sp500_df': sp500_df
    }


def print_summary(results, sp500_date):
    """Print summary statistics."""
    directors_df = results['directors_df']
    current_df = results['current_df']
    companies_df = results['companies_df']
    sp500_df = results['sp500_df']
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    print(f"\nS&P 500 Reference Date: {sp500_date}")
    print(f"S&P 500 Constituents: {len(sp500_df)}")
    print(f"Companies with Director Data: {len(companies_df)}")
    print(f"\nDirector Data:")
    print(f"  Total director-year records: {len(directors_df):,}")
    print(f"  Unique directors: {directors_df['director_name'].nunique():,}")
    print(f"  Current directors (most recent): {len(current_df):,}")
    
    if 'year' in directors_df.columns:
        print(f"\n  Year range: {int(directors_df['year'].min())} - {int(directors_df['year'].max())}")
        
        print("\n  Records by year:")
        year_counts = directors_df.groupby('year').size()
        for year, count in year_counts.items():
            print(f"    {int(year)}: {count:,}")


def main():
    parser = argparse.ArgumentParser(
        description='Build S&P 500 Directors Database from WRDS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 build_sp500_directors.py                     # Full run with latest S&P 500 data
  python3 build_sp500_directors.py --date 2024-12-31   # S&P 500 as of specific date
  python3 build_sp500_directors.py --explore           # Just explore available tables
  python3 build_sp500_directors.py --output ./my_data  # Custom output directory

Note: 
  - Requires WRDS account with access to CRSP and ExecuComp
  - CRSP S&P 500 data is updated annually, so 2025 may not be available yet
  - Script will automatically use most recent available data
        """
    )
    
    parser.add_argument('--explore', action='store_true',
                        help='Explore available WRDS tables (no data export)')
    parser.add_argument('--date', type=str, default=None,
                        help='S&P 500 reference date (YYYY-MM-DD)')
    parser.add_argument('--start-year', type=int, default=START_YEAR,
                        help=f'Start year for director data (default: {START_YEAR})')
    parser.add_argument('--end-year', type=int, default=END_YEAR,
                        help=f'End year for director data (default: {END_YEAR})')
    parser.add_argument('--output', type=str, default='./data/sp500',
                        help='Output directory (default: ./data/sp500)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("S&P 500 Directors Database Builder")
    print("=" * 60)
    
    # Connect to WRDS
    print("\nConnecting to WRDS...")
    try:
        db = wrds.Connection()
        print("Connected!")
    except Exception as e:
        print(f"\nError connecting to WRDS: {e}")
        print("\nMake sure you have:")
        print("  1. Installed wrds package: pip install wrds")
        print("  2. Set up WRDS credentials (~/.pgpass or environment variables)")
        print("  3. Valid WRDS account with access to CRSP and ExecuComp")
        return
    
    try:
        if args.explore:
            explore_tables(db)
            return
        
        # Step 1: Get S&P 500 constituents
        sp500_df, sp500_date = get_sp500_constituents(db, args.date)
        
        if len(sp500_df) == 0:
            print("\nNo S&P 500 constituents found. Check your date or WRDS access.")
            return
        
        # Step 2: Link PERMNO to GVKEY
        sp500_linked = link_permno_to_gvkey(db, sp500_df)
        
        # Step 3: Get company info
        gvkeys = sp500_linked['gvkey'].dropna().tolist()
        company_df = get_company_info(db, gvkeys)
        
        # Merge company info with S&P 500 data
        if 'company_name' in company_df.columns:
            sp500_linked = sp500_linked.merge(
                company_df[['gvkey', 'company_name', 'ticker']],
                on='gvkey',
                how='left'
            )
        
        # Step 4: Get director data
        directors_df = get_sp500_directors(
            db, sp500_linked, args.start_year, args.end_year
        )
        
        if len(directors_df) == 0:
            print("\nNo director data found. Check year range or company matches.")
            return
        
        # Step 5: Create output files
        results = create_database(directors_df, sp500_linked, args.output)
        
        # Print summary
        print_summary(results, sp500_date)
        
        print("\n" + "=" * 60)
        print("DONE!")
        print("=" * 60)
        print(f"\nOutput files saved to: {args.output}/")
        print("\nNext steps:")
        print("  1. Review sp500_directors.csv for director data")
        print("  2. Update prepare_linkedin_queries.py to use this data")
        print("  3. Run the LinkedIn URL finder pipeline")
        
    finally:
        db.close()
        print("\nWRDS connection closed.")


if __name__ == "__main__":
    main()
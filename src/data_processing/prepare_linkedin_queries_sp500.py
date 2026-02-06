"""
Prepare LinkedIn Search Queries for S&P 500 Directors
======================================================
This script processes the S&P 500 director data and generates
search queries formatted for LinkedIn profile discovery via Google API.

This is the S&P 500 specific version - uses sp500_current_directors.csv
as input (9,423 director-company pairs).

Usage:
    python3 prepare_linkedin_queries_sp500.py              # Process all S&P 500 directors
    python3 prepare_linkedin_queries_sp500.py --prototype  # Interactive single company mode
    python3 prepare_linkedin_queries_sp500.py --company "Apple"  # Specific company
"""

import pandas as pd
import numpy as np
import re
import os
import argparse

# =========================
# Configuration
# =========================
# Input: S&P 500 filtered directors
SP500_DATA_PATH = "../../data/sp500/sp500_current_directors.csv"

# Output directories
PROCESSED_DATA_PATH = "../../data/processed/sp500/"
OUTPUT_PATH = "../../outputs/sp500_batches/"

# Batch size for Google API (1000 queries per batch)
BATCH_SIZE = 1000

# =========================
# Helper Functions
# =========================

def clean_director_name(name):
    """
    Clean director names by removing credentials while preserving 
    generational suffixes (Jr, Sr, III, etc.)
    """
    if pd.isna(name):
        return name
    
    # Generational suffixes to preserve
    gen_suffixes = r'\b(Jr\.?|Sr\.?|I{1,3}|IV|V|VI|VII|VIII|2nd|3rd|4th)\b'
    
    # Extract generational suffix if present
    gen_match = re.search(gen_suffixes, name, re.IGNORECASE)
    gen_suffix = gen_match.group(0) if gen_match else ""
    
    # Credentials to remove
    credentials = [
        r'\bPhD\.?\b', r'\bPh\.D\.?\b', r'\bMD\b', r'\bM\.D\.?\b',
        r'\bMBA\b', r'\bM\.B\.A\.?\b', r'\bCPA\b', r'\bC\.P\.A\.?\b',
        r'\bCFA\b', r'\bJD\b', r'\bJ\.D\.?\b', r'\bEsq\.?\b',
        r'\bPE\b', r'\bP\.E\.?\b', r'\bDr\.?\b'
    ]
    
    cleaned = name
    for cred in credentials:
        cleaned = re.sub(cred, '', cleaned, flags=re.IGNORECASE)
    
    # Remove the generational suffix temporarily (we'll add it back)
    if gen_suffix:
        cleaned = re.sub(gen_suffixes, '', cleaned, flags=re.IGNORECASE)
    
    # Clean up extra whitespace and commas
    cleaned = re.sub(r'\s*,\s*', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    # Add back generational suffix
    if gen_suffix:
        cleaned = f"{cleaned} {gen_suffix}"
    
    return cleaned.strip()


def clean_company_name(name):
    """
    Standardize company names for better LinkedIn search matching.
    """
    if pd.isna(name):
        return name
    
    # Common suffixes to remove/standardize
    suffixes = [
        r'\s*,?\s*Inc\.?\s*$',
        r'\s*,?\s*Corp\.?\s*$',
        r'\s*,?\s*Corporation\s*$',
        r'\s*,?\s*Ltd\.?\s*$',
        r'\s*,?\s*LLC\s*$',
        r'\s*,?\s*L\.L\.C\.?\s*$',
        r'\s*,?\s*PLC\s*$',
        r'\s*,?\s*Co\.?\s*$',
        r'\s*,?\s*Company\s*$',
    ]
    
    cleaned = name
    for suffix in suffixes:
        cleaned = re.sub(suffix, '', cleaned, flags=re.IGNORECASE)
    
    # Clean up whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()


def generate_search_query(row):
    """
    Generate a LinkedIn search query from director name and company.
    Format: "FirstName LastName CompanyName"
    """
    name = row.get('director_name_clean', row.get('director_name', ''))
    company = row.get('company_name_clean', row.get('company_name', ''))
    
    if pd.isna(name) or pd.isna(company):
        return None
    
    return f"{name} {company}"


# =========================
# Main Processing
# =========================

def main():
    print("=" * 60)
    print("LinkedIn Query Preparation - S&P 500 Directors")
    print("=" * 60)
    
    # Create output directories if they don't exist
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    
    # Load S&P 500 director data
    print("\n[1/5] Loading S&P 500 directors data...")
    
    if not os.path.exists(SP500_DATA_PATH):
        print(f"\n❌ ERROR: S&P 500 data not found at {SP500_DATA_PATH}")
        print("\nPlease run build_sp500_directors.py first to generate the S&P 500 dataset.")
        return
    
    df = pd.read_csv(SP500_DATA_PATH)
    print(f"      Loaded {len(df):,} director-company pairs")
    print(f"      Columns: {list(df.columns)}")
    
    # Clean names
    print("\n[2/5] Cleaning director names...")
    df['director_name_clean'] = df['director_name'].apply(clean_director_name)
    
    # Show examples
    sample = df[['director_name', 'director_name_clean']].drop_duplicates().head(5)
    print("      Sample transformations:")
    for _, row in sample.iterrows():
        if row['director_name'] != row['director_name_clean']:
            print(f"        '{row['director_name']}' → '{row['director_name_clean']}'")
    
    # Clean company names
    print("\n[3/5] Cleaning company names...")
    df['company_name_clean'] = df['company_name'].apply(clean_company_name)
    
    # Generate search queries
    print("\n[4/5] Generating search queries...")
    df['search_query'] = df.apply(generate_search_query, axis=1)
    
    # Remove any null queries
    df = df.dropna(subset=['search_query'])
    print(f"      Valid search queries: {len(df):,}")
    
    # =========================
    # Save Outputs
    # =========================
    print("\n[5/5] Saving outputs...")
    print("-" * 60)
    
    # Full processed dataset
    full_output_path = os.path.join(PROCESSED_DATA_PATH, "sp500_directors_processed.csv")
    df.to_csv(full_output_path, index=False)
    print(f"\n[✓] Full dataset: {full_output_path}")
    
    # Search queries only (for Google API)
    queries_df = df[['search_query', 'director_name_clean', 'company_name_clean', 'gvkey', 'ticker']].copy()
    queries_output_path = os.path.join(OUTPUT_PATH, "all_search_queries.csv")
    queries_df.to_csv(queries_output_path, index=False)
    print(f"[✓] All queries: {queries_output_path}")
    
    # Batch files for Google API
    n_batches = (len(queries_df) + BATCH_SIZE - 1) // BATCH_SIZE
    
    print(f"\n[✓] Creating {n_batches} batch files ({BATCH_SIZE} queries each):")
    
    for i in range(n_batches):
        start_idx = i * BATCH_SIZE
        end_idx = min((i + 1) * BATCH_SIZE, len(queries_df))
        batch = queries_df.iloc[start_idx:end_idx]
        
        batch_path = os.path.join(OUTPUT_PATH, f"batch_{i+1:03d}_queries.csv")
        batch.to_csv(batch_path, index=False)
        print(f"      Batch {i+1}: {len(batch):,} queries → {batch_path}")
    
    # =========================
    # Summary
    # =========================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total director-company pairs: {len(df):,}")
    print(f"Unique directors:             {df['director_name_clean'].nunique():,}")
    print(f"Unique companies:             {df['company_name_clean'].nunique():,}")
    print(f"Batch files created:          {n_batches}")
    
    print("\n" + "-" * 60)
    print("GOOGLE API COST ESTIMATE")
    print("-" * 60)
    print(f"Queries:        {len(df):,}")
    print(f"Free tier:      100/day")
    print(f"Paid rate:      $5 per 1,000 queries")
    print(f"Estimated cost: ${(len(df) / 1000) * 5:.2f}")
    
    print("\n✅ Ready for Google Custom Search API!")
    print(f"   Run: python3 find_linkedin_urls_sp500.py --batch 1")
    

def prototype_mode(df, company_search=None):
    """
    Interactive prototype mode - process a single company's directors.
    """
    print("\n" + "=" * 60)
    print("🔬 PROTOTYPE MODE - S&P 500")
    print("=" * 60)
    
    # Clean the data first
    df['director_name_clean'] = df['director_name'].apply(clean_director_name)
    df['company_name_clean'] = df['company_name'].apply(clean_company_name)
    
    # Get unique companies
    companies = df['company_name'].unique()
    
    if company_search is None:
        print(f"\nTotal S&P 500 companies in dataset: {len(companies):,}")
        company_search = input("\nEnter company name to search: ").strip()
    
    # Find matching companies
    matches = [c for c in companies if company_search.lower() in c.lower()]
    
    if not matches:
        print(f"\n❌ No companies found matching '{company_search}'")
        print("\nTry a broader search term, or check these similar names:")
        suggestions = [c for c in companies if any(word.lower() in c.lower() for word in company_search.split())][:10]
        for s in suggestions:
            print(f"  - {s}")
        return None
    
    if len(matches) > 1:
        print(f"\nFound {len(matches)} matching companies:")
        for i, m in enumerate(matches[:20], 1):
            print(f"  {i}. {m}")
        if len(matches) > 20:
            print(f"  ... and {len(matches) - 20} more")
        
        selection = input("\nEnter number to select (or press Enter for first match): ").strip()
        if selection.isdigit() and 1 <= int(selection) <= len(matches):
            selected_company = matches[int(selection) - 1]
        else:
            selected_company = matches[0]
    else:
        selected_company = matches[0]
    
    print(f"\n✓ Selected: {selected_company}")
    
    # Filter to this company
    company_df = df[df['company_name'] == selected_company].copy()
    
    # Generate search queries
    company_df['search_query'] = company_df.apply(generate_search_query, axis=1)
    
    # Display results
    print(f"\n{'=' * 60}")
    print(f"Directors of {selected_company}")
    print(f"{'=' * 60}")
    print(f"Found {len(company_df)} directors:\n")
    
    for _, row in company_df.iterrows():
        print(f"  • {row['director_name_clean']}")
        print(f"    Search query: \"{row['search_query']}\"")
        print()
    
    # Save output
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    
    # Clean filename
    safe_company_name = re.sub(r'[^\w\s-]', '', selected_company).replace(' ', '_')[:50]
    output_file = os.path.join(OUTPUT_PATH, f"prototype_{safe_company_name}_queries.csv")
    
    company_df[['director_name_clean', 'company_name_clean', 'search_query', 'gvkey', 'ticker']].to_csv(output_file, index=False)
    print(f"\n✅ Saved to: {output_file}")
    
    # Also save just the queries for easy copy-paste
    queries_only_file = os.path.join(OUTPUT_PATH, f"prototype_{safe_company_name}_queries_only.txt")
    with open(queries_only_file, 'w') as f:
        for query in company_df['search_query']:
            f.write(query + '\n')
    print(f"✅ Queries only: {queries_only_file}")
    
    return company_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Prepare LinkedIn search queries for S&P 500 directors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 prepare_linkedin_queries_sp500.py              # Process all S&P 500 directors
  python3 prepare_linkedin_queries_sp500.py --prototype  # Interactive single company
  python3 prepare_linkedin_queries_sp500.py --company "Apple"  # Specific company

This script uses data/sp500/sp500_current_directors.csv as input.
Run build_sp500_directors.py first if you haven't already.
        """
    )
    parser.add_argument('--prototype', action='store_true', 
                        help='Run in prototype mode (single company)')
    parser.add_argument('--company', type=str, default=None,
                        help='Company name to search (use with --prototype)')
    args = parser.parse_args()
    
    if args.prototype or args.company:
        # Prototype mode
        print("[1/1] Loading S&P 500 directors data...")
        
        if not os.path.exists(SP500_DATA_PATH):
            print(f"\n❌ ERROR: S&P 500 data not found at {SP500_DATA_PATH}")
            print("\nPlease run build_sp500_directors.py first.")
            exit(1)
            
        df = pd.read_csv(SP500_DATA_PATH)
        print(f"      Loaded {len(df):,} records")
        
        prototype_mode(df, args.company)
    else:
        # Full processing mode
        main()
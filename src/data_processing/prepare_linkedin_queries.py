"""
Step 1: Prepare LinkedIn Search Queries from Director Data
===========================================================
This script processes the WRDS director data and generates
search queries formatted for LinkedIn profile discovery.

Usage:
    python prepare_linkedin_queries.py --prototype --company "Apple"
    python prepare_linkedin_queries.py                              # Full batch mode

Output:
    Prototype: outputs/prototype_{company}_queries.csv
    Batch:     outputs/batch_001_queries.csv, batch_002_queries.csv, ...
"""

import pandas as pd
import numpy as np
import re
import argparse
from pathlib import Path

# =========================
# Path Configuration
# =========================
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "directors.csv"
PROCESSED_DATA_PATH = PROJECT_ROOT / "data" / "processed"
OUTPUT_PATH = PROJECT_ROOT / "outputs"

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
        r'\bPE\b', r'\bP\.E\.?\b', r'\bDr\.?\b',
        r'\bB\.?Sci\.?\b', r'\bB\.?S\.?\b', r'\bM\.?S\.?\b',
        r'\bFAICD\b', r'\bRet\.?\b', r'\bUSMC\b', r'\bUSAF\b',
    ]
    
    cleaned = name
    for cred in credentials:
        cleaned = re.sub(cred, '', cleaned, flags=re.IGNORECASE)
    
    # Remove the generational suffix temporarily (we'll add it back)
    if gen_suffix:
        cleaned = re.sub(gen_suffixes, '', cleaned, flags=re.IGNORECASE)
    
    # Clean up extra whitespace, commas, and trailing periods
    cleaned = re.sub(r'\s*,\s*', ' ', cleaned)
    cleaned = re.sub(r'\.+', ' ', cleaned)  # Replace periods with spaces
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    # Add back generational suffix (without period)
    if gen_suffix:
        gen_suffix_clean = gen_suffix.replace('.', '')
        cleaned = f"{cleaned} {gen_suffix_clean}"
    
    return cleaned.strip()


def clean_company_name(name):
    """Standardize company names for better LinkedIn search matching."""
    if pd.isna(name):
        return name
    
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
    
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()


def generate_search_query(row):
    """Generate a LinkedIn search query from director name and company."""
    name = row.get('director_name_clean', row.get('director_name', ''))
    company = row.get('company_name_clean', row.get('company_name', ''))
    
    if pd.isna(name) or pd.isna(company):
        return None
    
    return f"{name} {company}"


# =========================
# Prototype Mode
# =========================

def run_prototype(company_search):
    """Step 1 Prototype: Prepare queries for a single company."""
    print("=" * 60)
    print("🔬 STEP 1: Prepare LinkedIn Search Queries")
    print("=" * 60)
    
    # Load data
    print(f"\n[1/4] Loading directors data...")
    print(f"      From: {RAW_DATA_PATH}")
    df = pd.read_csv(RAW_DATA_PATH)
    print(f"      Loaded {len(df):,} records")
    
    # Clean the data
    print(f"\n[2/4] Cleaning names...")
    df['director_name_clean'] = df['director_name'].apply(clean_director_name)
    df['company_name_clean'] = df['company_name'].apply(clean_company_name)
    
    # Find matching companies
    companies = df['company_name'].unique()
    matches = [c for c in companies if company_search.lower() in c.lower()]
    
    if not matches:
        print(f"\n✗ No companies found matching '{company_search}'")
        suggestions = [c for c in companies if any(word.lower() in c.lower() for word in company_search.split())][:10]
        if suggestions:
            print("\nDid you mean one of these?")
            for s in suggestions:
                print(f"  - {s}")
        return None
    
    if len(matches) > 1:
        print(f"\n[3/4] Found {len(matches)} matching companies:")
        for i, m in enumerate(matches[:10], 1):
            print(f"      {i}. {m}")
        
        selection = input("\nEnter number to select (or Enter for first): ").strip()
        if selection.isdigit() and 1 <= int(selection) <= len(matches):
            selected_company = matches[int(selection) - 1]
        else:
            selected_company = matches[0]
    else:
        selected_company = matches[0]
    
    print(f"\n✓ Selected: {selected_company}")
    
    # Filter to this company
    company_df = df[df['company_name'] == selected_company].copy()
    company_df = (
        company_df.sort_values('year', ascending=False)
        .drop_duplicates(subset=['director_name_clean'])
    )
    
    # Generate search queries
    company_df['search_query'] = company_df.apply(generate_search_query, axis=1)
    
    # Display directors
    print(f"\n[4/4] Generated queries for {len(company_df)} directors:")
    for _, row in company_df.iterrows():
        print(f"      • {row['director_name_clean']}")
        print(f"        Query: \"{row['search_query']}\"")
    
    # Save output
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r'[^\w\s-]', '', selected_company).replace(' ', '_')[:30]
    output_file = OUTPUT_PATH / f"prototype_{safe_name}_queries.csv"
    
    # Select columns to save
    output_df = company_df[['gvkey', 'ticker', 'company_name', 'director_name', 
                            'director_name_clean', 'company_name_clean', 'search_query', 'year']]
    output_df.to_csv(output_file, index=False)
    
    # Summary
    print(f"\n{'=' * 60}")
    print("✅ STEP 1 COMPLETE")
    print("=" * 60)
    print(f"Company: {selected_company}")
    print(f"Directors: {len(company_df)}")
    print(f"Output: {output_file}")
    print(f"\n→ Next: python find_linkedin_urls.py --prototype --company \"{company_search}\"")
    
    return output_file


# =========================
# Batch Mode
# =========================

def run_batch_mode():
    """Full batch processing for all companies."""
    print("=" * 60)
    print("LinkedIn Query Preparation - Batch Mode")
    print("=" * 60)
    
    PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    
    # Load data
    print("\n[1/5] Loading directors data...")
    print(f"      From: {RAW_DATA_PATH}")
    df = pd.read_csv(RAW_DATA_PATH)
    print(f"      Loaded {len(df):,} records")
    
    # Clean names
    print("\n[2/5] Cleaning director names...")
    df['director_name_clean'] = df['director_name'].apply(clean_director_name)
    
    # Clean company names
    print("\n[3/5] Cleaning company names...")
    df['company_name_clean'] = df['company_name'].apply(clean_company_name)
    
    # Get current directors
    print("\n[4/5] Extracting current directors...")
    current_directors = (
        df.sort_values('year', ascending=False)
        .drop_duplicates(subset=['director_name_clean', 'company_name_clean'])
    )
    print(f"      Unique director-company pairs: {len(current_directors):,}")
    
    # Generate search queries
    print("\n[5/5] Generating search queries...")
    current_directors['search_query'] = current_directors.apply(generate_search_query, axis=1)
    current_directors = current_directors.dropna(subset=['search_query'])
    print(f"      Valid search queries: {len(current_directors):,}")
    
    # Save outputs
    print("\n" + "=" * 60)
    print("Saving outputs...")
    
    # Full processed dataset
    full_output_path = PROCESSED_DATA_PATH / "directors_processed.csv"
    current_directors.to_csv(full_output_path, index=False)
    print(f"\n[✓] Full dataset: {full_output_path}")
    
    # Batch files (1000 per batch)
    queries_df = current_directors[['search_query', 'director_name_clean', 'company_name_clean', 'gvkey', 'ticker']].copy()
    batch_size = 1000
    n_batches = (len(queries_df) + batch_size - 1) // batch_size
    
    print(f"\n[✓] Creating {n_batches} batch files:")
    for i in range(n_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(queries_df))
        batch = queries_df.iloc[start_idx:end_idx]
        
        batch_path = OUTPUT_PATH / f"batch_{i+1:03d}_queries.csv"
        batch.to_csv(batch_path, index=False)
        print(f"      Batch {i+1}: {len(batch)} queries")
    
    # Summary
    print("\n" + "=" * 60)
    print("✅ BATCH PREPARATION COMPLETE")
    print("=" * 60)
    print(f"Total queries: {len(current_directors):,}")
    print(f"Batch files: {n_batches}")
    print(f"\n→ Next: python find_linkedin_urls.py --batch 1")


# =========================
# Main
# =========================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Step 1: Prepare LinkedIn search queries')
    parser.add_argument('--prototype', action='store_true', help='Run prototype mode (single company)')
    parser.add_argument('--company', type=str, default=None, help='Company name to search')
    args = parser.parse_args()
    
    if args.prototype or args.company:
        if not args.company:
            args.company = input("Enter company name: ").strip()
        run_prototype(args.company)
    else:
        run_batch_mode()
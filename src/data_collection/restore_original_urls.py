"""
Restore Original LinkedIn URLs from Batch Files
================================================
Reconstructs all_sp500_linkedin_urls.csv from individual batch files,
which still have the original URLs before --apply was run.

Usage:
    python3 restore_original_urls.py
"""

import pandas as pd
import os

# Configuration - adjust path if needed
BATCH_DIR = '../../data/processed/sp500_linkedin_urls/'
OUTPUT_FILE = '../../data/processed/sp500_linkedin_urls/all_sp500_linkedin_urls_original.csv'

def main():
    print("=" * 60)
    print("Restoring Original LinkedIn URLs from Batch Files")
    print("=" * 60)
    
    # Find batch files
    if not os.path.exists(BATCH_DIR):
        print(f"❌ Directory not found: {BATCH_DIR}")
        return
    
    batch_files = sorted([f for f in os.listdir(BATCH_DIR) if f.startswith('batch_') and f.endswith('_urls.csv')])
    
    if not batch_files:
        print("❌ No batch files found!")
        return
    
    print(f"\nFound {len(batch_files)} batch files")
    
    # Load and combine
    dfs = []
    for f in batch_files:
        path = os.path.join(BATCH_DIR, f)
        df = pd.read_csv(path)
        dfs.append(df)
        
        # Check if this batch has original URLs
        has_urls = df['linkedin_url'].notna().sum()
        print(f"  {f}: {len(df)} rows, {has_urls} URLs")
    
    combined = pd.concat(dfs, ignore_index=True)
    
    # Run verification on combined data
    print(f"\nRunning name verification...")
    
    # Import verification function
    try:
        from linkedin_verification import verify_name_match
    except ImportError:
        import sys
        sys.path.insert(0, os.path.dirname(__file__))
        from linkedin_verification import verify_name_match
    
    # Add verification columns
    combined['verified'] = False
    combined['match_type'] = 'none'
    
    verified_count = 0
    for idx, row in combined.iterrows():
        if pd.notna(row.get('linkedin_url')) and row.get('search_status') == 'found':
            result = verify_name_match(
                row.get('director_name_clean', ''),
                row.get('linkedin_title', '')
            )
            combined.at[idx, 'verified'] = result['verified']
            combined.at[idx, 'match_type'] = result['match_type']
            if result['verified']:
                verified_count += 1
    
    # Stats
    total = len(combined)
    urls_found = combined['linkedin_url'].notna().sum()
    unverified_with_urls = ((combined['verified'] == False) & (combined['linkedin_url'].notna())).sum()
    
    print(f"\n" + "-" * 60)
    print(f"Combined stats:")
    print(f"  Total rows: {total:,}")
    print(f"  URLs found: {urls_found:,}")
    print(f"  Verified: {verified_count:,}")
    print(f"  Unverified (with URLs): {unverified_with_urls:,}")
    
    # Save
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✓ Saved original data to: {OUTPUT_FILE}")
    
    # Also show sample of unverified
    if unverified_with_urls > 0:
        print(f"\n" + "=" * 60)
        print("Sample of unverified (wrong) profiles:")
        print("=" * 60)
        
        unverified = combined[(combined['verified'] == False) & (combined['linkedin_url'].notna())]
        sample = unverified.sample(min(20, len(unverified)), random_state=42)
        
        for _, row in sample.iterrows():
            print(f"\nDirector: {row['director_name_clean']}")
            print(f"Company:  {row['company_name_clean']}")
            print(f"LinkedIn: {row['linkedin_title']}")
    else:
        print("\n⚠ No unverified URLs found in batch files either.")
        print("  The batch files may have also been modified.")


if __name__ == "__main__":
    main()
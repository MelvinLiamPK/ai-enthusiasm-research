"""
Compare Lenient vs Strict Verification Rates
===========================================
This script shows the hit rate difference between:
- LENIENT: Accept if first OR last name matches (old behavior)
- STRICT: Accept only if BOTH first AND last name match (new behavior)
"""

import pandas as pd
import sys
import os

# Add project path
sys.path.insert(0, '/mnt/project')
from linkedin_verification import verify_name_match

def analyze_verification_modes(csv_path):
    """Compare lenient vs strict verification on actual data."""
    
    print("=" * 70)
    print("VERIFICATION MODE COMPARISON")
    print("=" * 70)
    print(f"\nData file: {csv_path}")
    
    # Load data
    df = pd.read_csv(csv_path)
    total = len(df)
    
    # Only process rows with URLs
    has_url = df['linkedin_url'].notna()
    urls_found = has_url.sum()
    
    print(f"\nTotal directors: {total:,}")
    print(f"URLs found: {urls_found:,} ({100*urls_found/total:.1f}%)")
    print(f"No URL found: {total - urls_found:,}")
    
    if urls_found == 0:
        print("\nNo URLs to verify!")
        return
    
    # Get name column
    name_col = 'director_name_clean' if 'director_name_clean' in df.columns else 'director_name'
    
    # Verify all URLs
    strict_verified = 0
    lenient_verified = 0
    
    both_names = 0
    first_only = 0
    last_only = 0
    neither = 0
    
    for idx in df[has_url].index:
        director_name = df.loc[idx, name_col]
        linkedin_title = df.loc[idx, 'linkedin_title'] if 'linkedin_title' in df.columns else None
        
        result = verify_name_match(director_name, linkedin_title)
        
        # Current STRICT mode (requires both)
        if result['verified']:
            strict_verified += 1
        
        # OLD LENIENT mode (requires first OR last)
        if result['matched_first'] or result['matched_last']:
            lenient_verified += 1
        
        # Breakdown
        if result['matched_first'] and result['matched_last']:
            both_names += 1
        elif result['matched_first']:
            first_only += 1
        elif result['matched_last']:
            last_only += 1
        else:
            neither += 1
    
    # Print comparison
    print("\n" + "=" * 70)
    print("VERIFICATION RESULTS")
    print("=" * 70)
    
    print(f"\n{'Mode':<20} {'Verified':<15} {'Rate':<15} {'Lost vs Lenient'}")
    print("-" * 70)
    
    print(f"{'LENIENT (old)':<20} {lenient_verified:>7,}/{urls_found:<5,} {100*lenient_verified/urls_found:>6.1f}%       {'-':<15}")
    print(f"{'STRICT (new)':<20} {strict_verified:>7,}/{urls_found:<5,} {100*strict_verified/urls_found:>6.1f}%       {lenient_verified - strict_verified:,} profiles")
    
    print("\n" + "=" * 70)
    print("MATCH TYPE BREAKDOWN")
    print("=" * 70)
    
    print(f"  ✓ Both names match:    {both_names:>6,}  ({100*both_names/urls_found:>5.1f}%)  ← STRICT accepts these")
    print(f"  ⚠ First name only:     {first_only:>6,}  ({100*first_only/urls_found:>5.1f}%)  ← STRICT rejects")
    print(f"  ⚠ Last name only:      {last_only:>6,}  ({100*last_only/urls_found:>5.1f}%)  ← STRICT rejects")
    print(f"  ✗ Neither name:        {neither:>6,}  ({100*neither/urls_found:>5.1f}%)  ← Both reject")
    
    partial_matches = first_only + last_only
    print(f"\n  Total partial matches: {partial_matches:,} ({100*partial_matches/urls_found:.1f}%)")
    print(f"  These are the profiles STRICT mode filters out as potentially wrong.")
    
    # Recommendation
    print("\n" + "=" * 70)
    print("RECOMMENDATION")
    print("=" * 70)
    
    print(f"\nSTRICT mode trades {partial_matches:,} potentially wrong profiles")
    print(f"for higher confidence that the remaining {strict_verified:,} are correct.")
    print(f"\nVerification rate: {100*strict_verified/urls_found:.1f}% (down from {100*lenient_verified/urls_found:.1f}%)")
    
    if partial_matches > 0:
        print(f"\nThe {partial_matches:,} partial matches likely include:")
        print("  - Wrong people with same first OR last name")
        print("  - Correct people using different professional names")
        print("  - Names where LinkedIn shortened/changed the display")
    
    return {
        'total': total,
        'urls_found': urls_found,
        'strict_verified': strict_verified,
        'lenient_verified': lenient_verified,
        'both_names': both_names,
        'first_only': first_only,
        'last_only': last_only,
        'neither': neither
    }


if __name__ == "__main__":
    # Default to SP500 combined file
    default_path = "../../data/processed/sp500_linkedin_urls/all_sp500_linkedin_urls.csv"
    
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = default_path
    
    if not os.path.exists(csv_path):
        print(f"Error: File not found: {csv_path}")
        print(f"\nUsage: python compare_verification_modes.py [path/to/urls.csv]")
        sys.exit(1)
    
    results = analyze_verification_modes(csv_path)

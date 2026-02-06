"""
Sample Verified LinkedIn URLs
==============================
Get a random sample of VERIFIED matches to manually check quality.

This helps validate that our strict verification is actually catching
the right people, not just filtering profiles.

Usage:
    python3 sample_verified_urls.py                    # 20 random verified profiles
    python3 sample_verified_urls.py --count 50         # 50 random verified profiles
    python3 sample_verified_urls.py --company "Apple"  # All verified Apple profiles
"""

import pandas as pd
import argparse
import sys
import os

# Add project path
sys.path.insert(0, '/mnt/project')
from linkedin_verification import verify_name_match


def sample_verified_profiles(csv_path, sample_size=20, company_filter=None):
    """
    Get a random sample of verified profiles for manual quality checking.
    
    Args:
        csv_path: Path to CSV with LinkedIn URLs
        sample_size: Number of profiles to sample (ignored if company_filter set)
        company_filter: If provided, show all verified profiles for this company
    """
    print("=" * 70)
    print("VERIFIED PROFILE SAMPLE")
    print("=" * 70)
    print(f"\nData file: {csv_path}")
    
    # Load data
    df = pd.read_csv(csv_path)
    
    # Get name column
    name_col = 'director_name_clean' if 'director_name_clean' in df.columns else 'director_name'
    company_col = 'company_name_clean' if 'company_name_clean' in df.columns else 'company_name'
    
    # Re-verify all URLs to get current strict verification
    print("\nRe-verifying with STRICT mode...")
    
    verified_profiles = []
    
    for idx, row in df.iterrows():
        if pd.isna(row.get('linkedin_url')):
            continue
        
        result = verify_name_match(
            row.get(name_col, ''),
            row.get('linkedin_title', '')
        )
        
        # STRICT: Only accept if BOTH names match
        if result['matched_first'] and result['matched_last']:
            verified_profiles.append({
                'director_name': row.get(name_col, ''),
                'company_name': row.get(company_col, ''),
                'linkedin_title': row.get('linkedin_title', ''),
                'linkedin_url': row.get('linkedin_url', ''),
                'matched_first': result['matched_first'],
                'matched_last': result['matched_last'],
            })
    
    verified_df = pd.DataFrame(verified_profiles)
    
    print(f"Total verified profiles: {len(verified_df):,}")
    
    # Apply company filter if specified
    if company_filter:
        mask = verified_df['company_name'].str.contains(company_filter, case=False, na=False)
        verified_df = verified_df[mask]
        print(f"Profiles matching '{company_filter}': {len(verified_df)}")
        sample_df = verified_df
    else:
        # Random sample
        if len(verified_df) < sample_size:
            print(f"Warning: Only {len(verified_df)} verified profiles available")
            sample_df = verified_df
        else:
            sample_df = verified_df.sample(n=sample_size, random_state=42)
            print(f"Random sample size: {sample_size}")
    
    # Display results
    print("\n" + "=" * 70)
    print("VERIFIED PROFILES (for manual quality check)")
    print("=" * 70)
    
    for i, row in sample_df.iterrows():
        print(f"\nDirector: {row['director_name']}")
        print(f"Company:  {row['company_name']}")
        print(f"LinkedIn: {row['linkedin_title']}")
        print(f"URL:      {row['linkedin_url']}")
        print(f"Matched:  first='{row['matched_first']}', last='{row['matched_last']}'")
    
    # Summary stats
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Sample size: {len(sample_df)}")
    print(f"Total verified available: {len(verified_df):,}")
    
    # Save to file
    output_path = "verified_sample.csv"
    sample_df.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")
    print("\nManually review these profiles to check:")
    print("  1. Does the LinkedIn profile appear to be the right person?")
    print("  2. Does the company match (or make sense for a director)?")
    print("  3. Is the profile content relevant for AI enthusiasm analysis?")
    
    return sample_df


def main():
    parser = argparse.ArgumentParser(
        description='Sample verified LinkedIn profiles for quality checking',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 sample_verified_urls.py                    # 20 random profiles
  python3 sample_verified_urls.py --count 50         # 50 random profiles
  python3 sample_verified_urls.py --company "Apple"  # All Apple profiles
  python3 sample_verified_urls.py --company "Microsoft" --count 10
        """
    )
    
    parser.add_argument(
        '--count', '-n',
        type=int,
        default=20,
        help='Number of profiles to sample (default: 20)'
    )
    
    parser.add_argument(
        '--company', '-c',
        type=str,
        default=None,
        help='Filter to specific company (shows all verified profiles for that company)'
    )
    
    parser.add_argument(
        '--file', '-f',
        type=str,
        default='../../data/processed/sp500_linkedin_urls/all_sp500_linkedin_urls.csv',
        help='Path to CSV file with LinkedIn URLs'
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.file):
        print(f"Error: File not found: {args.file}")
        print(f"\nUsage: python3 sample_verified_urls.py --help")
        sys.exit(1)
    
    sample_verified_profiles(args.file, args.count, args.company)


if __name__ == "__main__":
    main()

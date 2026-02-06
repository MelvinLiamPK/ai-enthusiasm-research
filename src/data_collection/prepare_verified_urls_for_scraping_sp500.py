"""
Prepare Verified LinkedIn URLs for Scraping
============================================
This script filters verified LinkedIn URLs (score >= 70) and prepares them
for post scraping with Apify.

Director-Specific Verification:
    Score 100: Full name + Board keyword + Company (Rare, perfect)
    Score 95:  Full name + Board keyword (Strong)
    Score 90:  Full name match (TYPICAL for directors - shows employment)
    Score 80:  Partial name + Board keyword
    Score 70:  Full name + Company (Ambiguous but acceptable)
    
Default threshold: 70 (configurable)

Usage:
    python3 prepare_verified_urls_for_scraping.py                  # Use score >= 70
    python3 prepare_verified_urls_for_scraping.py --threshold 80   # Use score >= 80
    python3 prepare_verified_urls_for_scraping.py --threshold 90   # Use score >= 90
    python3 prepare_verified_urls_for_scraping.py --stats          # Show stats only
"""

import os
import sys
import argparse
import pandas as pd
from datetime import datetime

# =========================
# Configuration
# =========================
INPUT_PATH = "../../data/processed/sp500_linkedin_urls/all_sp500_linkedin_urls.csv"
OUTPUT_DIR = "../../data/processed/sp500_verified/"
APIFY_DIR = "../../outputs/apify_inputs/"

DEFAULT_THRESHOLD = 70  # Minimum match score for verification


# =========================
# Helper Functions
# =========================

def load_verified_data(input_path):
    """Load the LinkedIn URLs with verification scores."""
    if not os.path.exists(input_path):
        print(f"❌ Error: Input file not found: {input_path}")
        print("\nPlease run verification first:")
        print("  python3 find_linkedin_urls_sp500.py --verify")
        sys.exit(1)
    
    print(f"Loading data from: {input_path}")
    df = pd.read_csv(input_path)
    
    # Check for required columns
    required_cols = ['match_score', 'verified', 'linkedin_url']
    missing = [col for col in required_cols if col not in df.columns]
    
    if missing:
        print(f"\n❌ Error: Missing verification columns: {missing}")
        print("\nPlease run verification first:")
        print("  python3 find_linkedin_urls_sp500.py --verify")
        sys.exit(1)
    
    return df


def print_statistics(df, threshold):
    """Print detailed statistics about the dataset."""
    print("\n" + "=" * 80)
    print("VERIFICATION STATISTICS")
    print("=" * 80)
    
    total = len(df)
    has_url = df['linkedin_url'].notna().sum()
    no_url = df['linkedin_url'].isna().sum()
    
    print(f"\nTotal directors: {total:,}")
    print(f"  URLs found: {has_url:,} ({100*has_url/total:.1f}%)")
    print(f"  No URL found: {no_url:,} ({100*no_url/total:.1f}%)")
    
    # Score distribution for URLs that were found
    df_with_urls = df[df['linkedin_url'].notna()]
    
    if len(df_with_urls) > 0:
        print(f"\n" + "-" * 80)
        print("SCORE DISTRIBUTION (for found URLs)")
        print("-" * 80)
        
        score_ranges = [
            (100, 100, "EXCELLENT (100)", "Perfect match"),
            (95, 99, "EXCELLENT (95-99)", "Full name + board keyword"),
            (90, 94, "GOOD (90-94)", "Full name (typical director)"),
            (80, 89, "GOOD (80-89)", "Partial name + board keyword"),
            (70, 79, "FAIR (70-79)", "Full name + company"),
            (60, 69, "WEAK (60-69)", "Partial name only"),
            (30, 59, "WRONG (30-59)", "Company only or low match"),
            (0, 29, "NO_MATCH (0-29)", "No match")
        ]
        
        for min_score, max_score, label, desc in score_ranges:
            mask = (df_with_urls['match_score'] >= min_score) & (df_with_urls['match_score'] <= max_score)
            count = mask.sum()
            if count > 0:
                pct = 100 * count / len(df_with_urls)
                print(f"  {label:<25} {count:>6,} ({pct:>5.1f}%) - {desc}")
        
        print("-" * 80)
        
        # Quality flag distribution
        if 'quality_flag' in df_with_urls.columns:
            print("\nQUALITY FLAGS:")
            quality_counts = df_with_urls['quality_flag'].value_counts()
            for flag, count in quality_counts.items():
                pct = 100 * count / len(df_with_urls)
                print(f"  {flag:<20} {count:>6,} ({pct:>5.1f}%)")
        
        # Board keyword matches
        if 'board_keyword_matched' in df_with_urls.columns:
            board_keyword_count = df_with_urls['board_keyword_matched'].sum()
            pct = 100 * board_keyword_count / len(df_with_urls)
            print(f"\nBoard keywords in title: {board_keyword_count:,} ({pct:.1f}%)")
    
    # Verification threshold analysis
    print(f"\n" + "=" * 80)
    print(f"VERIFICATION AT THRESHOLD >= {threshold}")
    print("=" * 80)
    
    verified = df[df['match_score'] >= threshold]
    verified_with_url = verified[verified['linkedin_url'].notna()]
    
    print(f"\nVerified directors (score >= {threshold}): {len(verified_with_url):,}")
    print(f"  Percentage of total: {100*len(verified_with_url)/total:.1f}%")
    print(f"  Percentage of found URLs: {100*len(verified_with_url)/has_url:.1f}%")
    
    # By company
    if 'company_name' in verified_with_url.columns:
        companies = verified_with_url['company_name'].nunique()
        print(f"  Unique companies: {companies:,}")
        
        # Top companies by verified directors
        print(f"\nTop 10 companies by verified director count:")
        top_companies = verified_with_url['company_name'].value_counts().head(10)
        for company, count in top_companies.items():
            print(f"  {company:<40} {count:>3}")


def filter_verified(df, threshold):
    """Filter to verified directors with LinkedIn URLs."""
    print(f"\nFiltering to score >= {threshold}...")
    
    # Filter: has URL AND meets threshold
    verified = df[
        (df['linkedin_url'].notna()) & 
        (df['match_score'] >= threshold)
    ].copy()
    
    print(f"  Kept: {len(verified):,} directors")
    
    return verified


def save_datasets(df_verified, threshold, output_dir, apify_dir):
    """Save the verified dataset in multiple formats."""
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(apify_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "=" * 80)
    print("SAVING OUTPUTS")
    print("=" * 80)
    
    # 1. Full verified dataset (CSV)
    full_csv = os.path.join(output_dir, f"sp500_directors_verified_score{threshold}plus.csv")
    df_verified.to_csv(full_csv, index=False)
    print(f"\n✓ Full dataset: {full_csv}")
    print(f"  Columns: {len(df_verified.columns)}, Rows: {len(df_verified):,}")
    
    # 2. URLs only for Apify (TXT)
    urls = df_verified['linkedin_url'].unique()
    urls_txt = os.path.join(apify_dir, f"linkedin_urls_score{threshold}plus_{timestamp}.txt")
    with open(urls_txt, 'w') as f:
        for url in urls:
            f.write(url + '\n')
    print(f"\n✓ URLs for Apify: {urls_txt}")
    print(f"  Unique URLs: {len(urls):,}")
    
    # 3. Apify JSON input (for batch scraper)
    import json
    
    apify_json = os.path.join(apify_dir, f"apify_input_score{threshold}plus_{timestamp}.json")
    apify_input = {
        "profileUrls": urls.tolist(),
        "maxPosts": 50,
        "startDate": "",  # Leave empty for all posts
        "endDate": ""
    }
    
    with open(apify_json, 'w') as f:
        json.dump(apify_input, f, indent=2)
    print(f"\n✓ Apify JSON input: {apify_json}")
    print(f"  Profiles: {len(urls):,}")
    print(f"  Max posts per profile: 50")
    
    # 4. Summary metadata
    metadata = {
        'created_at': timestamp,
        'threshold': threshold,
        'total_directors': len(df_verified),
        'unique_urls': len(urls),
        'companies': df_verified['company_name'].nunique() if 'company_name' in df_verified.columns else None,
        'score_distribution': df_verified['match_score'].value_counts().to_dict(),
        'quality_distribution': df_verified['quality_flag'].value_counts().to_dict() if 'quality_flag' in df_verified.columns else None
    }
    
    metadata_json = os.path.join(output_dir, f"verification_metadata_score{threshold}plus.json")
    with open(metadata_json, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"\n✓ Metadata: {metadata_json}")
    
    # 5. Create a summary report
    report_path = os.path.join(output_dir, f"verification_report_score{threshold}plus.txt")
    with open(report_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("VERIFIED DIRECTORS - SUMMARY REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Verification threshold: Score >= {threshold}\n\n")
        f.write(f"Total verified directors: {len(df_verified):,}\n")
        f.write(f"Unique LinkedIn URLs: {len(urls):,}\n")
        if 'company_name' in df_verified.columns:
            f.write(f"Unique companies: {df_verified['company_name'].nunique():,}\n\n")
        
        f.write("Score Distribution:\n")
        for score, count in sorted(df_verified['match_score'].value_counts().items(), reverse=True):
            f.write(f"  Score {score}: {count:,}\n")
        
        if 'quality_flag' in df_verified.columns:
            f.write("\nQuality Flags:\n")
            for flag, count in df_verified['quality_flag'].value_counts().items():
                f.write(f"  {flag}: {count:,}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("NEXT STEPS\n")
        f.write("=" * 80 + "\n\n")
        f.write("1. Upload Apify JSON input to Apify batch scraper\n")
        f.write("2. Configure: 50 posts per profile\n")
        f.write("3. Run scraper\n")
        f.write("4. Download results\n")
        f.write("5. Analyze for AI enthusiasm keywords\n")
    
    print(f"\n✓ Report: {report_path}")
    
    return {
        'full_csv': full_csv,
        'urls_txt': urls_txt,
        'apify_json': apify_json,
        'metadata': metadata_json,
        'report': report_path
    }


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description='Prepare verified LinkedIn URLs for scraping',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 prepare_verified_urls_for_scraping.py                  # Use default threshold (70)
  python3 prepare_verified_urls_for_scraping.py --threshold 80   # More strict
  python3 prepare_verified_urls_for_scraping.py --threshold 90   # Very strict
  python3 prepare_verified_urls_for_scraping.py --stats          # Show stats only

Score Meanings:
  100:    Full name + Board keyword + Company (Perfect)
  95:     Full name + Board keyword (Excellent)
  90:     Full name match (Good - typical director)
  80-85:  Partial name + Board keyword (Good)
  70-79:  Full name + Company (Fair)
  60-69:  Partial name only (Weak - exclude)
        """
    )
    
    parser.add_argument('--threshold', type=int, default=DEFAULT_THRESHOLD,
                        help=f'Minimum match score (default: {DEFAULT_THRESHOLD})')
    parser.add_argument('--stats', action='store_true',
                        help='Show statistics only, do not create output files')
    parser.add_argument('--input', type=str, default=INPUT_PATH,
                        help='Path to verified URLs CSV')
    
    args = parser.parse_args()
    
    # Validate threshold
    if args.threshold < 0 or args.threshold > 100:
        print("❌ Error: Threshold must be between 0 and 100")
        sys.exit(1)
    
    # Load data
    df = load_verified_data(args.input)
    
    # Show statistics
    print_statistics(df, args.threshold)
    
    # If stats only, exit here
    if args.stats:
        print("\n✓ Statistics displayed. No files created (--stats mode).")
        return
    
    # Filter to verified
    df_verified = filter_verified(df, args.threshold)
    
    if len(df_verified) == 0:
        print(f"\n❌ No directors meet the threshold of {args.threshold}")
        print("Try a lower threshold:")
        print(f"  python3 prepare_verified_urls_for_scraping.py --threshold 60")
        sys.exit(1)
    
    # Save outputs
    outputs = save_datasets(df_verified, args.threshold, OUTPUT_DIR, APIFY_DIR)
    
    # Final summary
    print("\n" + "=" * 80)
    print("✅ PREPARATION COMPLETE")
    print("=" * 80)
    print(f"\nVerified directors ready for scraping: {len(df_verified):,}")
    print(f"Unique LinkedIn URLs: {df_verified['linkedin_url'].nunique():,}")
    
    print("\n📋 NEXT STEPS:")
    print("=" * 80)
    print("\n1. Upload to Apify:")
    print(f"   File: {outputs['apify_json']}")
    print(f"   Actor: apimaestro/linkedin-batch-profile-posts-scraper")
    
    print("\n2. Configure Apify:")
    print(f"   - Profiles: {df_verified['linkedin_url'].nunique():,}")
    print(f"   - Max posts per profile: 50")
    print(f"   - Estimated cost: Check Apify pricing")
    
    print("\n3. Run scraper and download results")
    
    print("\n4. Analyze posts for AI keywords:")
    print("   python3 analyze_ai_enthusiasm.py")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Merge LinkedIn Posts with Director/Company Metadata
====================================================
Connects scraped LinkedIn posts back to S&P 500 companies and directors.

This script:
1. Loads the reextracted posts (with engagement data)
2. Loads the verified directors database (with company/ticker info)
3. Matches posts to directors via LinkedIn URL
4. Creates final dataset with company_name, ticker, director_name

Usage:
    python3 src/data_analysis/merge_posts_with_metadata.py
    python3 src/data_analysis/merge_posts_with_metadata.py --posts posts_reextracted_20260203_125602.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import argparse
import sys

# =======================
# Configuration
# =======================

POSTS_DIR = Path("data/processed/sp500_linkedin_posts")
VERIFIED_DIRECTORS = Path("data/processed/sp500_verified/sp500_directors_verified_score70plus.csv")
OUTPUT_DIR = Path("data/processed/sp500_linkedin_posts")

# =======================
# Helper Functions
# =======================

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def find_latest_posts_file():
    """Find the most recent reextracted posts file."""
    posts_files = sorted(POSTS_DIR.glob("posts_reextracted_*.csv"), reverse=True)
    if posts_files:
        return posts_files[0]
    return None

def load_posts(filepath=None):
    """Load posts CSV file."""
    if filepath:
        posts_path = POSTS_DIR / filepath
    else:
        posts_path = find_latest_posts_file()
        if not posts_path:
            raise FileNotFoundError(f"No reextracted posts files found in {POSTS_DIR}")
    
    print(f"\n📂 Loading posts from:")
    print(f"   {posts_path.name}")
    
    df = pd.read_csv(posts_path)
    print(f"   ✓ Loaded {len(df):,} posts")
    
    return df

def load_verified_directors():
    """Load verified directors with company metadata."""
    print(f"\n📂 Loading verified directors from:")
    print(f"   {VERIFIED_DIRECTORS.name}")
    
    if not VERIFIED_DIRECTORS.exists():
        raise FileNotFoundError(f"Verified directors file not found: {VERIFIED_DIRECTORS}")
    
    df = pd.read_csv(VERIFIED_DIRECTORS)
    print(f"   ✓ Loaded {len(df):,} verified director records")
    print(f"   ✓ Unique directors: {df['linkedin_url'].nunique():,}")
    print(f"   ✓ Unique companies: {df['company_name_clean'].nunique():,}")
    
    # Rename columns for consistency
    df = df.rename(columns={
        'company_name_clean': 'company_name',
        'director_name_clean': 'director_name'
    })
    
    return df

def normalize_linkedin_url(url):
    """Normalize LinkedIn URLs for matching."""
    if pd.isna(url):
        return None
    
    url = str(url).strip()
    
    # Remove trailing slashes
    url = url.rstrip('/')
    
    # Remove query parameters and fragments
    if '?' in url:
        url = url.split('?')[0]
    if '#' in url:
        url = url.split('#')[0]
    
    # Standardize /in/ vs /posts/
    # Convert posts URLs to profile URLs
    if '/posts/' in url:
        # Extract username from posts URL
        parts = url.split('/posts/')
        if len(parts) > 1:
            username = parts[1].split('/')[0].split('-')[0]
            url = f"https://www.linkedin.com/in/{username}"
    
    return url.lower()

def merge_posts_with_directors(posts_df, directors_df):
    """Merge posts with director metadata."""
    print_section("MERGING POSTS WITH DIRECTOR METADATA")
    
    # Normalize URLs in both datasets
    print("\n🔄 Normalizing LinkedIn URLs...")
    posts_df['profile_url_normalized'] = posts_df['profile_input'].apply(normalize_linkedin_url)
    directors_df['linkedin_url_normalized'] = directors_df['linkedin_url'].apply(normalize_linkedin_url)
    
    print(f"   Posts with valid URLs: {posts_df['profile_url_normalized'].notna().sum():,}")
    print(f"   Directors with valid URLs: {directors_df['linkedin_url_normalized'].notna().sum():,}")
    
    # Create director lookup with company info
    # Note: A director may serve on multiple S&P 500 boards
    print("\n📋 Creating director-company lookup...")
    director_lookup = directors_df[[
        'linkedin_url_normalized',
        'company_name',
        'ticker',
        'director_name',
        'match_score'
    ]].copy()
    
    # Check for directors on multiple boards
    multi_board = director_lookup.groupby('linkedin_url_normalized')['company_name'].nunique()
    multi_board_count = (multi_board > 1).sum()
    print(f"   ✓ {len(director_lookup):,} director-company relationships")
    print(f"   ✓ Directors serving on multiple S&P 500 boards: {multi_board_count}")
    
    if multi_board_count > 0:
        print(f"\n   📊 Top multi-board directors:")
        top_multi = multi_board.sort_values(ascending=False).head(5)
        for url, count in top_multi.items():
            director_names = director_lookup[director_lookup['linkedin_url_normalized'] == url]['director_name'].iloc[0]
            companies = director_lookup[director_lookup['linkedin_url_normalized'] == url]['company_name'].tolist()
            print(f"      {director_names}: {count} boards")
    
    # Merge posts with directors
    print(f"\n🔗 Merging posts with director data...")
    merged_df = posts_df.merge(
        director_lookup,
        left_on='profile_url_normalized',
        right_on='linkedin_url_normalized',
        how='left'
    )
    
    # Report matching statistics
    matched = merged_df['company_name'].notna().sum()
    unmatched = merged_df['company_name'].isna().sum()
    
    print(f"\n📊 Merge Results:")
    print(f"   ✓ Matched posts: {matched:,} ({matched/len(merged_df)*100:.1f}%)")
    print(f"   ✗ Unmatched posts: {unmatched:,} ({unmatched/len(merged_df)*100:.1f}%)")
    
    if unmatched > 0:
        print(f"\n   ⚠️  Unmatched posts are likely from:")
        print(f"      - Directors who left boards since verification")
        print(f"      - URL normalization mismatches")
        print(f"      - Directors below score threshold")
    
    # Show sample of matched data
    if matched > 0:
        print(f"\n   ✓ Sample matched records:")
        sample = merged_df[merged_df['company_name'].notna()].head(3)
        for idx, row in sample.iterrows():
            print(f"      {row['author_full_name']} → {row['company_name']} ({row['ticker']})")
    
    return merged_df

def create_final_dataset(merged_df):
    """Create final dataset with proper column ordering."""
    print_section("CREATING FINAL DATASET")
    
    # Define column order - company metadata first
    final_columns = [
        # Company & Director Info
        'company_name',
        'ticker',
        'director_name',
        'match_score',
        
        # Author Info
        'author_full_name',
        'author_first_name',
        'author_last_name',
        'author_headline',
        'author_username',
        'profile_input',
        
        # Post Content
        'post_url',
        'post_text',
        'post_type',
        'post_date',
        'post_datetime',
        'post_year',
        'post_month',
        
        # Engagement Metrics
        'total_engagement',
        'total_reactions',
        'likes',
        'comments',
        'reposts',
        'support',
        'love',
        'insight',
        'celebrate',
        'funny',
        
        # Post Metadata
        'text_length',
        'word_count',
        'media_type',
        'media_url',
        'article_url',
        'article_title',
        
        # Technical IDs
        'full_urn',
        'activity_urn',
        'share_urn',
    ]
    
    # Only include columns that exist
    available_columns = [col for col in final_columns if col in merged_df.columns]
    final_df = merged_df[available_columns].copy()
    
    print(f"\n✓ Final dataset created")
    print(f"   Total posts: {len(final_df):,}")
    print(f"   Columns: {len(available_columns)}")
    
    return final_df

def save_final_dataset(df, output_dir):
    """Save the final merged dataset."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"posts_with_metadata_{timestamp}.csv"
    
    print(f"\n💾 Saving final dataset...")
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"   ✓ Saved to: {output_path}")
    print(f"   File size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return output_path

def print_summary(df):
    """Print summary statistics of the merged dataset."""
    print_section("SUMMARY STATISTICS")
    
    matched = df['company_name'].notna()
    
    print(f"\n📊 Dataset Overview:")
    print(f"   Total posts: {len(df):,}")
    print(f"   Posts with company metadata: {matched.sum():,} ({matched.sum()/len(df)*100:.1f}%)")
    print(f"   Unique companies: {df['company_name'].nunique():,}")
    print(f"   Unique directors: {df['director_name'].nunique():,}")
    
    print(f"\n📈 Engagement:")
    print(f"   Total engagement: {df['total_engagement'].sum():,}")
    print(f"   Avg engagement per post: {df['total_engagement'].mean():.1f}")
    
    print(f"\n🏢 Top 10 Companies by Post Volume:")
    if matched.sum() > 0:
        company_posts = df[matched].groupby('company_name').size().sort_values(ascending=False).head(10)
        for company, count in company_posts.items():
            ticker = df[df['company_name'] == company]['ticker'].iloc[0]
            print(f"   {company} ({ticker}): {count:,} posts")
    
    print(f"\n👥 Top 10 Most Active Directors:")
    if matched.sum() > 0:
        director_posts = df[matched].groupby('director_name').agg({
            'post_url': 'count',
            'company_name': 'first',
            'total_engagement': 'sum'
        }).rename(columns={'post_url': 'post_count'})
        director_posts = director_posts.sort_values('post_count', ascending=False).head(10)
        
        for name, row in director_posts.iterrows():
            avg_eng = row['total_engagement'] / row['post_count'] if row['post_count'] > 0 else 0
            print(f"   {name} ({row['company_name']}): {row['post_count']} posts ({avg_eng:.0f} avg engagement)")

# =======================
# Main
# =======================

def main():
    parser = argparse.ArgumentParser(
        description='Merge LinkedIn posts with director/company metadata',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--posts', help='Specific posts CSV file (just filename)')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("  MERGE LINKEDIN POSTS WITH METADATA")
    print("  S&P 500 Board Directors")
    print("=" * 80)
    
    try:
        # Load data
        posts_df = load_posts(args.posts)
        directors_df = load_verified_directors()
        
        # Merge
        merged_df = merge_posts_with_directors(posts_df, directors_df)
        
        # Create final dataset
        final_df = create_final_dataset(merged_df)
        
        # Save
        output_path = save_final_dataset(final_df, OUTPUT_DIR)
        
        # Summary
        print_summary(final_df)
        
        # Next steps
        print_section("NEXT STEPS")
        print("\n✅ Merge complete! You can now:")
        print(f"   1. Explore the merged data:")
        print(f"      python3 src/data_analysis/explore_linkedin_posts_sp500.py {output_path.name}")
        print(f"   2. Analyze AI content by company")
        print(f"   3. Calculate AI enthusiasm scores")
        print(f"   4. Perform statistical analysis")
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
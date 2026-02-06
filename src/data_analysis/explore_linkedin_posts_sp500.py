#!/usr/bin/env python3
"""
Explore LinkedIn Posts Data
===========================
Comprehensive exploration of scraped LinkedIn posts from S&P 500 directors.

Run from your ai-enthusiasm-research directory:
    python3 src/data_analysis/explore_linkedin_posts_sp500.py
    python3 src/data_analysis/explore_linkedin_posts_sp500.py posts_reextracted_20260203_125602.csv
    python3 src/data_analysis/explore_linkedin_posts_sp500.py --all
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import ast
import argparse
import sys
import warnings
warnings.filterwarnings('ignore')

# =======================
# Configuration
# =======================

# Paths
DATA_DIR = Path("data/processed/sp500_linkedin_posts")
VERIFIED_DIRECTORS = Path("data/processed/sp500_verified/sp500_directors_verified_score70plus.csv")
SP500_DIRECTORS = Path("data/sp500/sp500_current_directors.csv")

# AI keywords
AI_KEYWORDS = [
    'artificial intelligence', ' ai ', 'machine learning', ' ml ', 'deep learning',
    'neural network', 'llm', 'large language model', 'generative ai', 'gen ai',
    'chatgpt', 'gpt', 'claude', 'gemini', 'copilot', 'automation', 'algorithm',
    'data science', 'predictive analytics', 'nlp', 'natural language processing',
    'computer vision', 'robotics', 'autonomous'
]

# =======================
# Helper Functions
# =======================

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def parse_date_field(date_str):
    """Parse the post_date field which is a dictionary/JSON string."""
    if pd.isna(date_str):
        return None
    
    try:
        # Handle if it's a string representation of a dict
        if isinstance(date_str, str):
            date_dict = ast.literal_eval(date_str)
        else:
            date_dict = date_str
        
        # Get the date string
        date_value = date_dict.get('date', '')
        
        # Parse it
        return pd.to_datetime(date_value, errors='coerce')
    except:
        return None

def load_all_posts(data_dir, filename=None):
    """Load and combine all posts CSV files, or load a specific file."""
    data_dir = Path(data_dir)
    
    if filename:
        # Load specific file
        file_path = data_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        print(f"Loading specified file:")
        print(f"  ✓ {filename}")
        df = pd.read_csv(file_path)
        print(f"    {len(df):,} rows")
        return df
    
    # Otherwise, load all matching files
    # First try to find reextracted files (these have proper engagement data)
    posts_files = list(data_dir.glob("posts_reextracted_*.csv"))
    
    # Fall back to old verified_directors_posts files if no reextracted files found
    if not posts_files:
        posts_files = list(data_dir.glob("verified_directors_posts_*.csv"))
    
    if not posts_files:
        raise FileNotFoundError(f"No posts files found in {data_dir}")
    
    print(f"Found {len(posts_files)} posts files:")
    
    all_posts = []
    for f in posts_files:
        df = pd.read_csv(f)
        all_posts.append(df)
        print(f"  ✓ {f.name}: {len(df):,} rows")
    
    combined = pd.concat(all_posts, ignore_index=True)
    
    # Remove duplicates based on post_url
    original_len = len(combined)
    combined = combined.drop_duplicates(subset=['post_url'], keep='first')
    if len(combined) < original_len:
        print(f"  ⚠️  Removed {original_len - len(combined):,} duplicate posts")
    
    return combined

def clean_posts_data(df):
    """Clean and prepare posts data."""
    print("\nCleaning data...")
    
    # Parse the date field - handle both dictionary format and string format
    print("  Parsing post_date field...")
    
    # Check if post_datetime already exists (from reextracted files)
    if 'post_datetime' in df.columns:
        # Already parsed, just ensure it's datetime type
        df['post_datetime'] = pd.to_datetime(df['post_datetime'], errors='coerce')
    elif 'post_date' in df.columns:
        # Check if it's a string (already parsed) or dict (needs parsing)
        sample = df['post_date'].dropna().iloc[0] if len(df['post_date'].dropna()) > 0 else None
        
        if sample and isinstance(sample, str) and sample.startswith('{'):
            # It's a dictionary string, needs parsing
            df['post_datetime'] = df['post_date'].apply(parse_date_field)
        else:
            # It's already a date string, just parse it
            df['post_datetime'] = pd.to_datetime(df['post_date'], errors='coerce')
    
    # Create year/month fields only if we have valid datetimes
    if 'post_datetime' in df.columns:
        df['post_year'] = df['post_datetime'].dt.year
        df['post_month'] = df['post_datetime'].dt.to_period('M')
    
    # Numeric columns - handle both old names and new names
    engagement_mapping = {
        'likes': ['likes', 'like'],
        'comments': ['comments'],
        'reposts': ['reposts']
    }
    
    for target_col, possible_cols in engagement_mapping.items():
        if target_col not in df.columns:
            for col in possible_cols:
                if col in df.columns:
                    df[target_col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    break
        else:
            df[target_col] = pd.to_numeric(df[target_col], errors='coerce').fillna(0).astype(int)
    
    # Total engagement - use existing column or calculate
    if 'total_engagement' not in df.columns:
        if all(col in df.columns for col in ['likes', 'comments', 'reposts']):
            df['total_engagement'] = df['likes'] + df['comments'] + df['reposts']
    else:
        df['total_engagement'] = pd.to_numeric(df['total_engagement'], errors='coerce').fillna(0).astype(int)
    
    # Text metrics
    if 'post_text' in df.columns:
        if 'text_length' not in df.columns:
            df['text_length'] = df['post_text'].fillna('').str.len()
        if 'word_count' not in df.columns:
            df['word_count'] = df['post_text'].fillna('').str.split().str.len()
    
    # Extract profile name from URL if not present or if it's all URNs
    if 'profile_name' not in df.columns or df['profile_name'].isna().all():
        print("  Extracting profile name from URL...")
        if 'profile_url' in df.columns:
            df['profile_name'] = df['profile_url'].str.extract(r'linkedin\.com/(?:posts|in)/([^/\?]+)', expand=False)
        elif 'author_username' in df.columns:
            df['profile_name'] = df['author_username']
        elif 'author_full_name' in df.columns:
            df['profile_name'] = df['author_full_name']
    
    print(f"✓ Cleaned {len(df):,} posts")
    return df

# =======================
# Analysis Functions
# =======================

def data_overview(posts_df):
    print_section("DATA OVERVIEW")
    
    # Determine which profile column to use
    profile_col = None
    for col in ['profile_input', 'author_profile_url', 'profile_url']:
        if col in posts_df.columns:
            profile_col = col
            break
    
    print(f"\n📊 Dataset Summary:")
    print(f"   Total posts: {len(posts_df):,}")
    if profile_col:
        print(f"   Unique profiles: {posts_df[profile_col].nunique():,}")
    
    if 'post_datetime' in posts_df.columns:
        valid_dates = posts_df['post_datetime'].notna()
        if valid_dates.any():
            print(f"   Date range: {posts_df.loc[valid_dates, 'post_datetime'].min().date()} to {posts_df.loc[valid_dates, 'post_datetime'].max().date()}")
            print(f"   Posts with valid dates: {valid_dates.sum():,} ({valid_dates.sum()/len(posts_df)*100:.1f}%)")
    
    print(f"\n📋 Available Columns:")
    for col in posts_df.columns:
        print(f"   - {col}")
    
    # Missing data
    print(f"\n❓ Missing Data (>50%):")
    missing = posts_df.isnull().sum()
    missing_pct = (missing / len(posts_df) * 100).round(1)
    for col in missing[missing > 0].index:
        if missing_pct[col] > 50:  # Only show if more than 50% missing
            print(f"   {col}: {missing[col]:,} ({missing_pct[col]}%)")

def data_quality_checks(posts_df):
    print_section("DATA QUALITY CHECKS")
    
    # Determine which profile column exists
    profile_col = None
    for col in ['profile_input', 'author_profile_url', 'profile_url']:
        if col in posts_df.columns:
            profile_col = col
            break
    
    print(f"\n🔍 Critical Fields:")
    if profile_col:
        print(f"   Posts with missing {profile_col}: {posts_df[profile_col].isna().sum():,}")
    print(f"   Posts with missing post_url: {posts_df['post_url'].isna().sum():,}")
    print(f"   Posts with missing post_text: {posts_df['post_text'].isna().sum():,}")
    
    # Check for company metadata (only in old format)
    has_company = 'company_name' in posts_df.columns
    has_ticker = 'ticker' in posts_df.columns
    has_director = 'director_name' in posts_df.columns
    
    if has_company or has_ticker or has_director:
        print(f"\n🔍 Director/Company Metadata:")
        print(f"   company_name column: {'✓ Present' if has_company else '✗ Missing'}")
        print(f"   ticker column: {'✓ Present' if has_ticker else '✗ Missing'}")
        print(f"   director_name column: {'✓ Present' if has_director else '✗ Missing'}")
        
        if has_company:
            null_company = posts_df['company_name'].isna().sum()
            pct = null_company/len(posts_df)*100
            status = "⚠️  CRITICAL" if pct > 90 else "✓"
            print(f"   {status} Posts missing company: {null_company:,} ({pct:.1f}%)")
        
        if has_ticker:
            null_ticker = posts_df['ticker'].isna().sum()
            pct = null_ticker/len(posts_df)*100
            status = "⚠️  CRITICAL" if pct > 90 else "✓"
            print(f"   {status} Posts missing ticker: {null_ticker:,} ({pct:.1f}%)")
        
        if has_director:
            null_director = posts_df['director_name'].isna().sum()
            pct = null_director/len(posts_df)*100
            status = "⚠️  CRITICAL" if pct > 90 else "✓"
            print(f"   {status} Posts missing director: {null_director:,} ({pct:.1f}%)")
    
    print(f"\n🔍 Content Quality:")
    if 'post_text' in posts_df.columns:
        empty_text = (posts_df['post_text'].isna() | (posts_df['post_text'] == '')).sum()
        print(f"   Empty/missing text: {empty_text:,} ({empty_text/len(posts_df)*100:.1f}%)")
    
    # Check engagement data
    if 'total_engagement' in posts_df.columns:
        zero_engagement = (posts_df['total_engagement'] == 0).sum()
        print(f"\n🔍 Engagement Data:")
        print(f"   Posts with 0 engagement: {zero_engagement:,} ({zero_engagement/len(posts_df)*100:.1f}%)")
        has_engagement = (posts_df['total_engagement'] > 0).sum()
        print(f"   Posts with >0 engagement: {has_engagement:,} ({has_engagement/len(posts_df)*100:.1f}%)")
        if zero_engagement/len(posts_df) > 0.9:
            print(f"   ⚠️  WARNING: >90% posts have 0 engagement - likely data extraction issue")

def temporal_analysis(posts_df):
    print_section("TEMPORAL ANALYSIS")
    
    if 'post_datetime' not in posts_df.columns or posts_df['post_datetime'].isna().all():
        print("⚠️  No valid dates found")
        return
    
    dated = posts_df[posts_df['post_datetime'].notna()].copy()
    
    print(f"\n📅 Posts by Year:")
    year_counts = dated['post_year'].value_counts().sort_index()
    for year, count in year_counts.items():
        pct = count / len(dated) * 100
        print(f"   {int(year)}: {count:,} posts ({pct:.1f}%)")
    
    print(f"\n📆 Recent Months (last 12):")
    monthly = dated['post_month'].value_counts().sort_index().tail(12)
    for month, count in monthly.items():
        print(f"   {month}: {count:,} posts")
    
    # Posting frequency
    date_range = (dated['post_datetime'].max() - dated['post_datetime'].min()).days
    if date_range > 0:
        print(f"\n⏰ Activity Metrics:")
        print(f"   Time span: {date_range:,} days")
        print(f"   Avg posts per day: {len(dated) / date_range:.1f}")

def engagement_analysis(posts_df):
    print_section("ENGAGEMENT METRICS")
    
    if 'total_engagement' not in posts_df.columns:
        print("⚠️  Engagement data unavailable")
        return
    
    # Check if we have meaningful engagement data
    has_engagement = (posts_df['total_engagement'] > 0).any()
    
    if not has_engagement:
        print("⚠️  WARNING: All posts show 0 engagement")
        print("   This likely indicates an issue with the data collection/export process.")
        print("   Engagement metrics may need to be re-scraped or extracted from raw JSON.")
        return
    
    print(f"\n💬 Total Engagement:")
    print(f"   Likes: {posts_df['likes'].sum():,}")
    print(f"   Comments: {posts_df['comments'].sum():,}")
    print(f"   Reposts: {posts_df['reposts'].sum():,}")
    print(f"   Combined: {posts_df['total_engagement'].sum():,}")
    
    # Filter to posts with engagement
    engaged = posts_df[posts_df['total_engagement'] > 0]
    
    if len(engaged) > 0:
        print(f"\n📈 Average per Engaged Post ({len(engaged):,} posts):")
        print(f"   Likes: {engaged['likes'].mean():.1f} (median: {engaged['likes'].median():.0f})")
        print(f"   Comments: {engaged['comments'].mean():.1f} (median: {engaged['comments'].median():.0f})")
        print(f"   Reposts: {engaged['reposts'].mean():.1f} (median: {engaged['reposts'].median():.0f})")
        
        print(f"\n🏆 Top 5 Most Engaged Posts:")
        top = engaged.nlargest(5, 'total_engagement')[['profile_name', 'post_datetime', 'total_engagement', 'post_text']]
        for idx, row in top.iterrows():
            text = str(row['post_text'])[:80] + '...' if len(str(row['post_text'])) > 80 else str(row['post_text'])
            date_str = row['post_datetime'].strftime('%Y-%m-%d') if pd.notna(row['post_datetime']) else 'Unknown'
            print(f"   {row['profile_name']} ({date_str}): {row['total_engagement']:,} engagement")
            print(f"      \"{text}\"")

def content_analysis(posts_df):
    print_section("CONTENT ANALYSIS")
    
    if 'text_length' not in posts_df.columns:
        print("⚠️  Text analysis unavailable")
        return
    
    with_text = posts_df[posts_df['text_length'] > 0]
    
    print(f"\n📝 Text Statistics:")
    print(f"   Posts with text: {len(with_text):,} ({len(with_text)/len(posts_df)*100:.1f}%)")
    print(f"   Avg length: {with_text['text_length'].mean():.0f} chars")
    print(f"   Avg words: {with_text['word_count'].mean():.0f}")
    print(f"   Median length: {with_text['text_length'].median():.0f} chars")
    print(f"   Median words: {with_text['word_count'].median():.0f}")
    
    print(f"\n📊 Text Length Distribution:")
    bins = [0, 100, 250, 500, 1000, 2000, float('inf')]
    labels = ['<100', '100-250', '250-500', '500-1k', '1k-2k', '2k+']
    with_text['length_bin'] = pd.cut(with_text['text_length'], bins=bins, labels=labels)
    for label in labels:
        count = (with_text['length_bin'] == label).sum()
        pct = count / len(with_text) * 100
        print(f"   {label} chars: {count:,} posts ({pct:.1f}%)")
    
    # Post types
    if 'post_type' in posts_df.columns:
        print(f"\n🎯 Post Type Distribution:")
        type_counts = posts_df['post_type'].value_counts()
        for post_type, count in type_counts.items():
            pct = count / len(posts_df) * 100
            print(f"   {post_type}: {count:,} ({pct:.1f}%)")

def ai_keyword_analysis(posts_df):
    print_section("AI KEYWORD ANALYSIS")
    
    if 'post_text' not in posts_df.columns:
        print("⚠️  No text data")
        return
    
    posts_df['text_lower'] = posts_df['post_text'].fillna('').str.lower()
    
    # Count keywords
    keyword_counts = {}
    for kw in AI_KEYWORDS:
        count = posts_df['text_lower'].str.contains(kw, case=False, regex=False, na=False).sum()
        if count > 0:
            keyword_counts[kw] = count
    
    # Any AI mention
    total_ai = posts_df['text_lower'].str.contains('|'.join([kw for kw in AI_KEYWORDS]), case=False, na=False).sum()
    
    print(f"\n🤖 AI Content Summary:")
    print(f"   Posts mentioning AI keywords: {total_ai:,} ({total_ai/len(posts_df)*100:.2f}%)")
    print(f"   Posts NOT mentioning AI: {len(posts_df) - total_ai:,} ({(len(posts_df) - total_ai)/len(posts_df)*100:.2f}%)")
    
    if keyword_counts:
        print(f"\n   Top AI keywords found:")
        sorted_kw = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)
        for kw, count in sorted_kw[:15]:
            pct = count / len(posts_df) * 100
            print(f"      '{kw}': {count:,} ({pct:.2f}%)")
    
    # Sample AI posts
    ai_posts = posts_df[posts_df['text_lower'].str.contains('|'.join(AI_KEYWORDS), case=False, na=False)]
    if len(ai_posts) > 0:
        print(f"\n   📄 Sample AI-related posts:")
        sample = ai_posts.sample(min(3, len(ai_posts)))
        for idx, row in sample.iterrows():
            text = str(row['post_text'])[:120] + '...' if len(str(row['post_text'])) > 120 else str(row['post_text'])
            print(f"      \"{text}\"")
    
    posts_df.drop('text_lower', axis=1, inplace=True)

def director_analysis(posts_df):
    print_section("DIRECTOR-LEVEL ANALYSIS")
    
    # Determine which column to use for grouping
    if 'profile_input' in posts_df.columns:
        profile_col = 'profile_input'
        name_col = 'author_full_name' if 'author_full_name' in posts_df.columns else 'profile_name'
    elif 'author_profile_url' in posts_df.columns:
        profile_col = 'author_profile_url'
        name_col = 'author_full_name' if 'author_full_name' in posts_df.columns else 'profile_name'
    else:
        profile_col = 'profile_url'
        name_col = 'profile_name'
    
    # Use profile identifier as the unique identifier
    stats = posts_df.groupby(profile_col).agg({
        'post_url': 'count',
        'total_engagement': 'sum',
        name_col: 'first'
    }).rename(columns={'post_url': 'post_count', name_col: 'name'})
    
    print(f"\n👥 Director Activity:")
    print(f"   Unique directors (profiles): {len(stats):,}")
    print(f"   Avg posts per director: {stats['post_count'].mean():.1f}")
    print(f"   Median posts per director: {stats['post_count'].median():.0f}")
    print(f"   Max posts per director: {stats['post_count'].max()}")
    print(f"   Min posts per director: {stats['post_count'].min()}")
    
    print(f"\n📊 Posts per Director Distribution:")
    bins = [0, 1, 5, 10, 20, 50, 100, float('inf')]
    labels = ['1', '2-5', '6-10', '11-20', '21-50', '51-100', '100+']
    stats['post_bin'] = pd.cut(stats['post_count'], bins=bins, labels=labels)
    for label in labels:
        count = (stats['post_bin'] == label).sum()
        pct = count / len(stats) * 100
        print(f"   {label} posts: {count:,} directors ({pct:.1f}%)")
    
    print(f"\n🏆 Top 10 Most Active Directors:")
    top = stats.nlargest(10, 'post_count')
    for profile_url, row in top.iterrows():
        name = row['name'] if pd.notna(row['name']) else profile_url.split('/')[-1][:50]
        avg_eng = row['total_engagement'] / row['post_count'] if row['post_count'] > 0 else 0
        print(f"   {name}: {row['post_count']} posts ({avg_eng:.0f} avg engagement)")

# =======================
# Main
# =======================

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Explore LinkedIn posts data from S&P 500 directors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 explore_linkedin_posts_sp500.py                              # Load latest reextracted file
  python3 explore_linkedin_posts_sp500.py posts_reextracted_*.csv      # Load specific file
  python3 explore_linkedin_posts_sp500.py --all                        # Load all verified_directors_posts files
        """
    )
    parser.add_argument('filename', nargs='?', help='Specific CSV file to analyze (just the filename, not full path)')
    parser.add_argument('--all', action='store_true', help='Load and combine all posts files instead of just reextracted')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("  LINKEDIN POSTS EXPLORATION")
    print("  S&P 500 Board Directors")
    print("=" * 80)
    
    # Load
    print("\n📂 Loading data...")
    try:
        if args.all:
            # Force loading all files by not looking for reextracted first
            posts_files = list(DATA_DIR.glob("verified_directors_posts_*.csv"))
            if not posts_files:
                posts_files = list(DATA_DIR.glob("posts_reextracted_*.csv"))
            
            if not posts_files:
                print(f"❌ No posts files found in {DATA_DIR}")
                return
            
            print(f"Found {len(posts_files)} posts files:")
            all_posts = []
            for f in posts_files:
                df = pd.read_csv(f)
                all_posts.append(df)
                print(f"  ✓ {f.name}: {len(df):,} rows")
            
            posts_df = pd.concat(all_posts, ignore_index=True)
            original_len = len(posts_df)
            posts_df = posts_df.drop_duplicates(subset=['post_url'], keep='first')
            if len(posts_df) < original_len:
                print(f"  ⚠️  Removed {original_len - len(posts_df):,} duplicate posts")
        else:
            posts_df = load_all_posts(DATA_DIR, filename=args.filename)
        
        posts_df = clean_posts_data(posts_df)
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Analyze
    data_overview(posts_df)
    data_quality_checks(posts_df)
    temporal_analysis(posts_df)
    engagement_analysis(posts_df)
    content_analysis(posts_df)
    ai_keyword_analysis(posts_df)
    director_analysis(posts_df)
    
    # Next steps
    print_section("NEXT STEPS & RECOMMENDATIONS")
    print("\n📋 Immediate Actions:")
    print("   1. ⚠️  CRITICAL: Merge posts with director/company metadata")
    print("      - Match profile_input to verified directors CSV")
    print("      - Add company_name, ticker, director_name columns")
    
    print("\n📋 Analysis Phase:")
    print("   2. Develop AI sentiment scoring algorithm")
    print("   3. Temporal trend analysis for AI enthusiasm")
    print("   4. Company-level aggregations and rankings")
    print("   5. Statistical significance testing")
    
    print(f"\n✅ Exploration complete!")
    print(f"   Posts analyzed: {len(posts_df):,}")
    
    # Determine which profile column to use
    profile_col = None
    for col in ['profile_input', 'author_profile_url', 'profile_url']:
        if col in posts_df.columns:
            profile_col = col
            break
    
    if profile_col:
        print(f"   Unique directors: {posts_df[profile_col].nunique():,}")
    
    ai_posts = (posts_df['post_text'].fillna('').str.lower().str.contains('|'.join(AI_KEYWORDS), case=False, na=False).sum())
    print(f"   AI-related posts: {ai_posts:,}")

if __name__ == "__main__":
    main()
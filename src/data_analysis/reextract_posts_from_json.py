#!/usr/bin/env python3
"""
Re-extract LinkedIn Posts from Raw JSON
========================================
Properly extract engagement metrics and all fields from the raw Apify JSON files.

This script:
1. Reads the raw JSON files (verified_directors_posts_raw_*.json)
2. Extracts ALL fields including engagement stats
3. Creates a clean CSV with proper engagement data
4. Preserves profile_input for merging with director metadata

Usage:
    python3 src/data_analysis/reextract_posts_from_json.py
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# =======================
# Configuration
# =======================

DATA_DIR = Path("data/processed/sp500_linkedin_posts")
OUTPUT_DIR = Path("data/processed/sp500_linkedin_posts")

# =======================
# Extraction Functions
# =======================

def extract_post_from_json(post_obj):
    """Extract a single post from JSON object into a flat dictionary."""
    
    # Basic post info
    extracted = {
        'post_url': post_obj.get('url', ''),
        'post_text': post_obj.get('text', ''),
        'post_type': post_obj.get('post_type', ''),
        'full_urn': post_obj.get('full_urn', ''),
    }
    
    # Posted date info
    posted_at = post_obj.get('posted_at', {})
    extracted['post_date'] = posted_at.get('date', '')
    extracted['post_timestamp'] = posted_at.get('timestamp', '')
    extracted['post_relative'] = posted_at.get('relative', '')
    
    # Author info
    author = post_obj.get('author', {})
    extracted['author_first_name'] = author.get('first_name', '')
    extracted['author_last_name'] = author.get('last_name', '')
    extracted['author_full_name'] = f"{author.get('first_name', '')} {author.get('last_name', '')}".strip()
    extracted['author_headline'] = author.get('headline', '')
    extracted['author_username'] = author.get('username', '')
    extracted['author_profile_url'] = author.get('profile_url', '')
    
    # Stats (ENGAGEMENT DATA!)
    stats = post_obj.get('stats', {})
    extracted['total_reactions'] = stats.get('total_reactions', 0)
    extracted['likes'] = stats.get('like', 0)
    extracted['support'] = stats.get('support', 0)
    extracted['love'] = stats.get('love', 0)
    extracted['insight'] = stats.get('insight', 0)
    extracted['celebrate'] = stats.get('celebrate', 0)
    extracted['funny'] = stats.get('funny', 0)
    extracted['comments'] = stats.get('comments', 0)
    extracted['reposts'] = stats.get('reposts', 0)
    
    # Total engagement (sum of all interactions)
    extracted['total_engagement'] = (
        extracted['total_reactions'] + 
        extracted['comments'] + 
        extracted['reposts']
    )
    
    # Media info
    media = post_obj.get('media', {})
    extracted['media_type'] = media.get('type', '')
    extracted['media_url'] = media.get('url', '')
    
    # Article info (if shared article)
    article = post_obj.get('article', {})
    extracted['article_url'] = article.get('url', '')
    extracted['article_title'] = article.get('title', '')
    
    # Profile input (CRITICAL for merging with director data)
    extracted['profile_input'] = post_obj.get('profile_input', '')
    
    # URN info
    urn = post_obj.get('urn', {})
    extracted['activity_urn'] = urn.get('activity_urn', '')
    extracted['share_urn'] = urn.get('share_urn', '')
    
    return extracted


def process_json_file(json_path):
    """Process a single JSON file and return a DataFrame."""
    print(f"\nProcessing: {json_path.name}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"  Found {len(data):,} posts in file")
    
    # Extract each post
    posts = []
    for post_obj in data:
        try:
            extracted = extract_post_from_json(post_obj)
            posts.append(extracted)
        except Exception as e:
            print(f"  ⚠️  Error extracting post: {e}")
            continue
    
    df = pd.DataFrame(posts)
    print(f"  ✓ Extracted {len(df):,} posts")
    
    return df


def process_all_json_files(data_dir):
    """Process all raw JSON files in the directory."""
    data_dir = Path(data_dir)
    json_files = sorted(data_dir.glob("verified_directors_posts_raw_*.json"))
    
    if not json_files:
        raise FileNotFoundError(f"No raw JSON files found in {data_dir}")
    
    print(f"Found {len(json_files)} raw JSON files")
    
    all_dfs = []
    for json_path in json_files:
        df = process_json_file(json_path)
        all_dfs.append(df)
    
    # Combine all
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\n✓ Combined total: {len(combined):,} posts")
    
    # Remove duplicates based on post_url
    original_len = len(combined)
    combined = combined.drop_duplicates(subset=['post_url'], keep='first')
    if len(combined) < original_len:
        print(f"  ⚠️  Removed {original_len - len(combined):,} duplicate posts")
    
    return combined


def add_computed_fields(df):
    """Add computed fields like parsed dates."""
    print("\nAdding computed fields...")
    
    # Parse date
    df['post_datetime'] = pd.to_datetime(df['post_date'], errors='coerce')
    df['post_year'] = df['post_datetime'].dt.year
    df['post_month'] = df['post_datetime'].dt.to_period('M')
    df['post_day_of_week'] = df['post_datetime'].dt.day_name()
    
    # Text metrics
    df['text_length'] = df['post_text'].fillna('').str.len()
    df['word_count'] = df['post_text'].fillna('').str.split().str.len()
    
    print("✓ Computed fields added")
    return df


def save_extracted_data(df, output_dir):
    """Save the extracted data with timestamp."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"posts_reextracted_{timestamp}.csv"
    
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"\n✓ Saved to: {output_path}")
    return output_path


def print_summary(df):
    """Print summary statistics."""
    print("\n" + "=" * 80)
    print("  EXTRACTION SUMMARY")
    print("=" * 80)
    
    print(f"\n📊 Dataset:")
    print(f"   Total posts: {len(df):,}")
    print(f"   Unique profiles: {df['profile_input'].nunique():,}")
    print(f"   Date range: {df['post_datetime'].min().date()} to {df['post_datetime'].max().date()}")
    
    print(f"\n💬 Engagement Data:")
    print(f"   Total reactions: {df['total_reactions'].sum():,}")
    print(f"   Total likes: {df['likes'].sum():,}")
    print(f"   Total comments: {df['comments'].sum():,}")
    print(f"   Total reposts: {df['reposts'].sum():,}")
    print(f"   Total engagement: {df['total_engagement'].sum():,}")
    
    print(f"\n📈 Engagement Stats:")
    print(f"   Avg engagement per post: {df['total_engagement'].mean():.1f}")
    print(f"   Median engagement per post: {df['total_engagement'].median():.0f}")
    print(f"   Posts with 0 engagement: {(df['total_engagement'] == 0).sum():,}")
    print(f"   Posts with >0 engagement: {(df['total_engagement'] > 0).sum():,}")
    
    print(f"\n🏆 Top 5 Most Engaged Posts:")
    top = df.nlargest(5, 'total_engagement')[['author_full_name', 'total_engagement', 'post_text']]
    for idx, row in top.iterrows():
        text = row['post_text'][:100] + '...' if len(row['post_text']) > 100 else row['post_text']
        print(f"   {row['author_full_name']}: {row['total_engagement']:,} engagement")
        print(f"      \"{text}\"")
    
    print(f"\n📋 Available Columns:")
    print(f"   {', '.join(df.columns)}")


# =======================
# Main
# =======================

def main():
    print("=" * 80)
    print("  RE-EXTRACT LINKEDIN POSTS FROM RAW JSON")
    print("  With Proper Engagement Data")
    print("=" * 80)
    
    try:
        # Process all JSON files
        df = process_all_json_files(DATA_DIR)
        
        # Add computed fields
        df = add_computed_fields(df)
        
        # Print summary
        print_summary(df)
        
        # Save
        output_path = save_extracted_data(df, OUTPUT_DIR)
        
        print("\n" + "=" * 80)
        print("  ✅ EXTRACTION COMPLETE")
        print("=" * 80)
        print(f"\nNext steps:")
        print(f"  1. Run the exploration script on the new CSV:")
        print(f"     python3 src/data_analysis/explore_linkedin_posts_sp500.py")
        print(f"  2. Merge with director metadata using 'profile_input' column")
        print(f"  3. Proceed with AI sentiment analysis")
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print(f"\nMake sure you're running from the ai-enthusiasm-research directory")
        print(f"and that the raw JSON files exist in: {DATA_DIR}")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

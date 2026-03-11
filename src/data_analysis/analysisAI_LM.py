#!/usr/bin/env python3
"""
Analyze AI Sentiment in Director Posts
======================================
Sentiment analysis using Loughran-McDonald financial dictionary for AI-related posts.

Usage:
    python3 src/analysis/analyze_ai_sentiment.py
    python3 src/analysis/analyze_ai_sentiment.py posts_with_metadata_20260203_125602.csv
    python3 src/analysis/analyze_ai_sentiment.py --all-posts  # Analyze all posts, not just AI-related
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime
import argparse
import sys
import warnings
warnings.filterwarnings('ignore')

# =======================
# Configuration
# =======================

# Paths
DATA_DIR = Path("/Users/melvinliam/Documents/Uni/RA-NB/Scraping/ai-enthusiasm-research/data/processed/sp500_linkedin_posts")
OUTPUT_DIR = Path("/Users/melvinliam/Documents/Uni/RA-NB/Scraping/ai-enthusiasm-research/outputs/analysis_results")

# AI keywords (same as explore script)
AI_KEYWORDS = [
    'artificial intelligence', ' ai ', 'machine learning', ' ml ', 'deep learning',
    'neural network', 'llm', 'large language model', 'generative ai', 'gen ai',
    'chatgpt', 'gpt', 'claude', 'gemini', 'copilot', 'automation', 'algorithm',
    'data science', 'predictive analytics', 'nlp', 'natural language processing',
    'computer vision', 'robotics', 'autonomous'
]

# =======================
# Loughran-McDonald Dictionary
# =======================

class LoughranMcDonaldAnalyzer:
    """
    Sentiment analyzer using the Loughran-McDonald financial dictionary.
    
    Reference: Loughran, T., & McDonald, B. (2011). When is a liability not a liability?
    Textual analysis, dictionaries, and 10-Ks. The Journal of Finance, 66(1), 35-65.
    """
    
    def __init__(self):
        print("Loading Loughran-McDonald dictionaries...")
        self.load_dictionaries()
        print(f"   {len(self.positive_words)} positive words")
        print(f"   {len(self.negative_words)} negative words")
        print(f"   {len(self.uncertainty_words)} uncertainty words")
    
    def load_dictionaries(self):
        """
        Load full Loughran-McDonald Master Dictionary from CSV.
        
        Download from: https://sraf.nd.edu/loughranmcdonald-master-dictionary/
        Place 'Loughran-McDonald_MasterDictionary_1993-2024.csv' in the data/ directory (project root).
        """
        import os
        from pathlib import Path
        
        # Get path to project root (2 levels up from this script)
        script_dir = Path(__file__).parent.resolve()
        project_root = script_dir.parent.parent
        lm_dict_path = project_root / 'data' / 'Loughran-McDonald_MasterDictionary_1993-2024.csv'
        
        if not lm_dict_path.exists():
            print(f"\nERROR: Loughran-McDonald dictionary not found at {lm_dict_path}")
            print("Please download from: https://sraf.nd.edu/loughranmcdonald-master-dictionary/")
            print("Place 'Loughran-McDonald_MasterDictionary_1993-2024.csv' in the data/ directory (project root)")
            sys.exit(1)
        
        # Load the full LM dictionary
        lm_dict = pd.read_csv(str(lm_dict_path))
        
        # Extract word sets (convert to lowercase for matching)
        # The CSV has columns like 'Positive', 'Negative', 'Uncertainty' with values > 0 indicating membership
        self.positive_words = set(lm_dict[lm_dict['Positive'] > 0]['Word'].str.lower())
        
        self.negative_words = set(lm_dict[lm_dict["Negative"] > 0]["Word"].str.lower())
        
        self.uncertainty_words = set(lm_dict[lm_dict["Uncertainty"] > 0]["Word"].str.lower())
    
    def preprocess_text(self, text):
        """Clean and tokenize text"""
        if pd.isna(text):
            return []
        
        # Convert to lowercase
        text = str(text).lower()
        
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Remove mentions and hashtags but keep the word
        text = re.sub(r'[@#]', '', text)
        
        # Keep only alphanumeric and spaces
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        
        # Tokenize
        tokens = text.split()
        
        return tokens
    
    def analyze_text(self, text):
        """
        Analyze text and return sentiment scores.
        
        Returns:
            dict with sentiment counts and ratios
        """
        tokens = self.preprocess_text(text)
        word_count = len(tokens)
        
        if word_count == 0:
            return {
                'word_count': 0,
                'positive_count': 0,
                'negative_count': 0,
                'uncertainty_count': 0,
                'positive_ratio': 0,
                'negative_ratio': 0,
                'uncertainty_ratio': 0,
                'net_sentiment': 0,
                'sentiment_ratio': 0
            }
        
        # Count sentiment words
        positive_count = sum(1 for token in tokens if token in self.positive_words)
        negative_count = sum(1 for token in tokens if token in self.negative_words)
        uncertainty_count = sum(1 for token in tokens if token in self.uncertainty_words)
        
        # Calculate ratios (per 1000 words, as is common in finance research)
        positive_ratio = (positive_count / word_count) * 1000
        negative_ratio = (negative_count / word_count) * 1000
        uncertainty_ratio = (uncertainty_count / word_count) * 1000
        
        # Net sentiment (positive - negative)
        net_sentiment = positive_ratio - negative_ratio
        
        # Sentiment ratio (positive / (positive + negative))
        # This avoids division by zero
        sentiment_ratio = (positive_count / (positive_count + negative_count)) if (positive_count + negative_count) > 0 else 0.5
        
        return {
            'word_count': word_count,
            'positive_count': positive_count,
            'negative_count': negative_count,
            'uncertainty_count': uncertainty_count,
            'positive_ratio': positive_ratio,
            'negative_ratio': negative_ratio,
            'uncertainty_ratio': uncertainty_ratio,
            'net_sentiment': net_sentiment,
            'sentiment_ratio': sentiment_ratio
        }
    
    def analyze_dataframe(self, df, text_column='post_text'):
        """
        Analyze sentiment for all posts in a dataframe.
        
        Args:
            df: DataFrame with posts
            text_column: Name of the column containing text to analyze
            
        Returns:
            DataFrame with sentiment scores added
        """
        print(f"\nAnalyzing sentiment for {len(df):,} posts...")
        
        # Analyze each post
        sentiment_results = []
        for count, (idx, row) in enumerate(df.iterrows(), 1):
            if count % 1000 == 0:
                print(f"  Processed {count:,}/{len(df):,} posts...")
            
            result = self.analyze_text(row[text_column])
            sentiment_results.append(result)
        
        # Convert to DataFrame and merge
        sentiment_df = pd.DataFrame(sentiment_results)
        result_df = pd.concat([df.reset_index(drop=True), sentiment_df], axis=1)
        
        print(" Sentiment analysis complete!")
        return result_df


# =======================
# Helper Functions
# =======================

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def is_ai_related(text):
    """Check if text contains AI-related keywords (using word boundaries)"""
    if pd.isna(text):
        return False
    text_lower = str(text).lower()
    
    # Use word boundaries to match whole words only
    # This prevents "innovation" from matching "automation"
    for kw in AI_KEYWORDS:
        # For keywords with spaces already (like ' ai '), use exact matching
        if kw.startswith(' ') and kw.endswith(' '):
            if kw in text_lower:
                return True
        else:
            # For other keywords, use word boundaries
            if re.search(r'\b' + re.escape(kw) + r'\b', text_lower):
                return True
    
    return False

def load_posts(filename=None):
    """Load posts from file or find most recent file"""
    if filename:
        filepath = DATA_DIR / filename
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        print(f"Loading: {filename}")
        return pd.read_csv(filepath)
    
    # Find most recent posts_with_metadata file
    files = list(DATA_DIR.glob("posts_with_metadata_*.csv"))
    if not files:
        # Fall back to reextracted files
        files = list(DATA_DIR.glob("posts_reextracted_*.csv"))
    
    if not files:
        raise FileNotFoundError(f"No posts files found in {DATA_DIR}")
    
    most_recent = max(files, key=lambda p: p.stat().st_mtime)
    print(f"Loading most recent file: {most_recent.name}")
    return pd.read_csv(most_recent)


# =======================
# Analysis Functions
# =======================

def print_summary_statistics(df):
    """Print summary statistics for sentiment scores"""
    print_section("SENTIMENT SUMMARY STATISTICS")
    
    sentiment_cols = ['positive_ratio', 'negative_ratio', 'uncertainty_ratio', 'net_sentiment']
    
    print("\n[STATS] Descriptive Statistics:")
    print(df[sentiment_cols].describe().round(2).to_string())
    
    print("\n[CHART] Distribution:")
    print(f"  Mean net sentiment: {df['net_sentiment'].mean():.2f}")
    print(f"  Median net sentiment: {df['net_sentiment'].median():.2f}")
    print(f"  Std dev net sentiment: {df['net_sentiment'].std():.2f}")
    
    # Classify posts
    positive_posts = (df['net_sentiment'] > 0).sum()
    negative_posts = (df['net_sentiment'] < 0).sum()
    neutral_posts = (df['net_sentiment'] == 0).sum()
    
    print(f"\n  Positive posts (net > 0): {positive_posts:,} ({positive_posts/len(df)*100:.1f}%)")
    print(f"  Negative posts (net < 0): {negative_posts:,} ({negative_posts/len(df)*100:.1f}%)")
    print(f"  Neutral posts (net = 0): {neutral_posts:,} ({neutral_posts/len(df)*100:.1f}%)")

def show_examples(df):
    """Show example posts by sentiment (deduplicated for cleaner display)"""
    print_section("EXAMPLE POSTS")
    
    # Deduplicate by post_text for display purposes only
    df_unique = df.drop_duplicates(subset=['post_text'], keep='first')
    print(f"\nNote: Showing unique posts only for examples ({len(df_unique):,} unique out of {len(df):,} total)")
    
    # Most positive
    print("\n[+] MOST POSITIVE POSTS (Top 3)")
    top_positive = df_unique.nlargest(3, 'net_sentiment')
    for idx, (i, row) in enumerate(top_positive.iterrows(), 1):
        print(f"\n#{idx} - Net Sentiment: {row['net_sentiment']:.2f}")
        print(f"    Positive: {row['positive_count']}, Negative: {row['negative_count']}")
        text = str(row['post_text'])[:300] + '...' if len(str(row['post_text'])) > 300 else str(row['post_text'])
        print(f"    \"{text}\"")
    
    # Most negative
    print("\n[-] MOST NEGATIVE POSTS (Top 3)")
    top_negative = df_unique.nsmallest(3, 'net_sentiment')
    for idx, (i, row) in enumerate(top_negative.iterrows(), 1):
        print(f"\n#{idx} - Net Sentiment: {row['net_sentiment']:.2f}")
        print(f"    Positive: {row['positive_count']}, Negative: {row['negative_count']}")
        text = str(row['post_text'])[:300] + '...' if len(str(row['post_text'])) > 300 else str(row['post_text'])
        print(f"    \"{text}\"")
    
    # Most uncertain
    print("\n[?] MOST UNCERTAIN POSTS (Top 3)")
    top_uncertain = df_unique.nlargest(3, 'uncertainty_ratio')
    for idx, (i, row) in enumerate(top_uncertain.iterrows(), 1):
        print(f"\n#{idx} - Uncertainty Ratio: {row['uncertainty_ratio']:.2f}")
        print(f"    Uncertainty words: {row['uncertainty_count']}")
        text = str(row['post_text'])[:300] + '...' if len(str(row['post_text'])) > 300 else str(row['post_text'])
        print(f"    \"{text}\"")

def analyze_by_company(df):
    """Analyze sentiment by company"""
    if 'ticker' not in df.columns or df['ticker'].isna().all():
        print("\n  No company/ticker information available")
        return
    
    print_section("SENTIMENT BY COMPANY")
    
    company_stats = df.groupby('ticker').agg({
        'post_text': 'count',
        'net_sentiment': ['mean', 'median'],
        'positive_ratio': 'mean',
        'negative_ratio': 'mean'
    }).round(2)
    
    company_stats.columns = ['post_count', 'mean_sentiment', 'median_sentiment', 'mean_positive', 'mean_negative']
    company_stats = company_stats.sort_values('mean_sentiment', ascending=False)
    
    print(f"\n[STATS] Companies with most positive AI sentiment (Top 10):")
    print(company_stats.head(10).to_string())
    
    print(f"\n[STATS] Companies with most negative AI sentiment (Top 10):")
    print(company_stats.tail(10).to_string())

def save_results(df, output_filename):
    """Save results to CSV"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / output_filename
    
    print_section("SAVING RESULTS")
    print(f"\n Saving to: {output_path}")
    df.to_csv(output_path, index=False)
    print(" Done!")
    
    # Also save summary statistics
    summary_path = OUTPUT_DIR / f"summary_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    sentiment_cols = ['positive_ratio', 'negative_ratio', 'uncertainty_ratio', 'net_sentiment']
    summary = df[sentiment_cols].describe()
    summary.to_csv(summary_path)
    print(f" Summary statistics saved to: {summary_path}")
    
    return output_path


# =======================
# Main Execution
# =======================

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Analyze AI sentiment in director posts')
    parser.add_argument('filename', nargs='?', help='Specific posts file to analyze')
    parser.add_argument('--all-posts', action='store_true', help='Analyze all posts, not just AI-related')
    args = parser.parse_args()
    
    print("=" * 80)
    print("  LOUGHRAN-MCDONALD SENTIMENT ANALYSIS")
    print("  AI Enthusiasm in S&P 500 Director Posts")
    print("=" * 80)
    
    # Load data
    print_section("LOADING DATA")
    df = load_posts(args.filename)
    print(f" Loaded {len(df):,} posts")
    
    # Filter to AI-related posts unless --all-posts flag is set
    if not args.all_posts:
        print("\nFiltering to AI-related posts...")
        df['is_ai_related'] = df['post_text'].apply(is_ai_related)
        df_analysis = df[df['is_ai_related']].copy()
        print(f" Found {len(df_analysis):,} AI-related posts ({len(df_analysis)/len(df)*100:.2f}%)")
    else:
        print("\n  Analyzing ALL posts (--all-posts flag set)")
        df_analysis = df.copy()
    
    if len(df_analysis) == 0:
        print("\n No posts to analyze!")
        return
    
    # Initialize analyzer
    print_section("INITIALIZING ANALYZER")
    analyzer = LoughranMcDonaldAnalyzer()
    
    # Analyze sentiment
    print_section("ANALYZING SENTIMENT")
    df_with_sentiment = analyzer.analyze_dataframe(df_analysis, text_column='post_text')
    
    # Display results
    print_summary_statistics(df_with_sentiment)
    show_examples(df_with_sentiment)
    analyze_by_company(df_with_sentiment)
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    post_type = "all_posts" if args.all_posts else "ai_posts"
    output_filename = f"{post_type}_with_sentiment_{timestamp}.csv"
    output_path = save_results(df_with_sentiment, output_filename)
    
    print("\n" + "=" * 80)
    print("   ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nResults saved to: {output_path}")
    print(f"Analyzed {len(df_with_sentiment):,} posts")
    print(f"Mean net sentiment: {df_with_sentiment['net_sentiment'].mean():.2f}")
    
    return df_with_sentiment


if __name__ == "__main__":
    df_results = main()
#!/usr/bin/env python3
"""
Loughran-McDonald Sentiment Analysis — Full Sample
====================================================
Scores ALL posts in posts_combined.csv using the L-M financial dictionary,
then tags AI/COVID subsets for downstream analysis.

Designed for memory-efficient processing on Sherlock (chunked reads).

Usage:
    python3 src/data_analysis/sentiment_analysis_full.py
    python3 src/data_analysis/sentiment_analysis_full.py --posts /path/to/posts_combined.csv
    python3 src/data_analysis/sentiment_analysis_full.py --chunk-size 50000
    python3 src/data_analysis/sentiment_analysis_full.py --ai-only  # Score only AI-related posts

On Sherlock (SLURM):
    sbatch slurm_sentiment_analysis.sh

Outputs (in outputs/sentiment_results/):
    - sentiment_all_posts_YYYYMMDD_HHMMSS.csv      Full scored dataset
    - sentiment_ai_posts_YYYYMMDD_HHMMSS.csv        AI-related subset
    - sentiment_covid_posts_YYYYMMDD_HHMMSS.csv     COVID subset
    - sentiment_summary_YYYYMMDD_HHMMSS.json        Summary statistics
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime
import argparse
import sys
import json
import time
import warnings
warnings.filterwarnings('ignore')


# =======================
# Path Resolution
# =======================

def resolve_project_root():
    """Resolve project root from script location (2 levels up from src/data_analysis/)."""
    return Path(__file__).resolve().parent.parent.parent


def default_paths():
    """Return default file paths relative to project root."""
    root = resolve_project_root()
    return {
        'posts': root / 'data' / 'processed' / 'all_people_linkedin_urls' / 'scraped_posts_combined' / 'posts_combined.csv',
        'lm_dict': root / 'data' / 'Loughran-McDonald_MasterDictionary_1993-2024.csv',
        'output_dir': root / 'outputs' / 'sentiment_results',
    }


# =======================
# Keyword Definitions
# =======================

AI_KEYWORDS = [
    'artificial intelligence', ' ai ', 'machine learning', ' ml ', 'deep learning',
    'neural network', 'llm', 'large language model', 'generative ai', 'gen ai',
    'chatgpt', 'gpt', 'claude', 'gemini', 'copilot', 'automation', 'algorithm',
    'data science', 'predictive analytics', 'nlp', 'natural language processing',
    'computer vision', 'robotics', 'autonomous',
]

COVID_KEYWORDS = [
    'covid', 'covid-19', 'covid19', 'coronavirus', 'pandemic', 'epidemic',
    'lockdown', 'quarantine', 'social distancing', 'remote work', 'work from home',
    'wfh', 'vaccine', 'vaccination', 'pfizer', 'moderna', 'omicron', 'delta variant',
    'ppe', 'mask mandate', 'ventilator', 'flatten the curve', 'shelter in place',
    'essential worker', 'frontline worker',
]


# =======================
# L-M Dictionary Loader
# =======================

def load_lm_dictionary(dict_path):
    """
    Load Loughran-McDonald Master Dictionary.

    Returns:
        tuple: (positive_words, negative_words, uncertainty_words,
                litigious_words, constraining_words, strong_modal_words, weak_modal_words)
                all as frozensets of lowercase strings.
    """
    dict_path = Path(dict_path)
    if not dict_path.exists():
        print(f"\nERROR: L-M dictionary not found at {dict_path}")
        print("Download from: https://sraf.nd.edu/loughranmcdonald-master-dictionary/")
        print(f"Place in: {dict_path.parent}/")
        sys.exit(1)

    print(f"Loading L-M dictionary from: {dict_path}")
    lm = pd.read_csv(str(dict_path))

    positive = frozenset(lm[lm['Positive'] > 0]['Word'].str.lower())
    negative = frozenset(lm[lm['Negative'] > 0]['Word'].str.lower())
    uncertainty = frozenset(lm[lm['Uncertainty'] > 0]['Word'].str.lower())
    litigious = frozenset(lm[lm['Litigious'] > 0]['Word'].str.lower())
    constraining = frozenset(lm[lm['Constraining'] > 0]['Word'].str.lower())

    # Modal words (Strong_Modal / Weak_Modal columns)
    strong_modal = frozenset(lm[lm['Strong_Modal'] > 0]['Word'].str.lower()) if 'Strong_Modal' in lm.columns else frozenset()
    weak_modal = frozenset(lm[lm['Weak_Modal'] > 0]['Word'].str.lower()) if 'Weak_Modal' in lm.columns else frozenset()

    print(f"  Positive: {len(positive):,}  Negative: {len(negative):,}  "
          f"Uncertainty: {len(uncertainty):,}  Litigious: {len(litigious):,}  "
          f"Constraining: {len(constraining):,}")

    return positive, negative, uncertainty, litigious, constraining, strong_modal, weak_modal


# =======================
# Text Preprocessing
# =======================

def preprocess_text(text):
    """Clean and tokenize post text for L-M matching."""
    if pd.isna(text):
        return []
    text = str(text).lower()
    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    # Remove mentions/hashtags but keep word
    text = re.sub(r'[@#]', '', text)
    # Keep only alphanumeric and spaces
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    return text.split()


# =======================
# Sentiment Scoring (vectorised per chunk)
# =======================

def score_chunk(chunk, pos, neg, unc, lit, con):
    """
    Score a DataFrame chunk. Returns the chunk with sentiment columns appended.
    Uses apply() per row — fast enough given L-M is O(n) set lookups.
    """
    results = []
    for text in chunk['post_text']:
        tokens = preprocess_text(text)
        n = len(tokens)
        if n == 0:
            results.append((0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5))
            continue

        pc = sum(1 for t in tokens if t in pos)
        nc = sum(1 for t in tokens if t in neg)
        uc = sum(1 for t in tokens if t in unc)
        lc = sum(1 for t in tokens if t in lit)
        cc = sum(1 for t in tokens if t in con)

        # Ratios per 1000 words (standard in finance NLP)
        pr = (pc / n) * 1000
        nr = (nc / n) * 1000
        ur = (uc / n) * 1000
        net = pr - nr
        polarity = (pc / (pc + nc)) if (pc + nc) > 0 else 0.5

        results.append((n, pc, nc, uc, lc, cc, pr, nr, ur, net, net, polarity))

    cols = [
        'lm_word_count', 'lm_positive_count', 'lm_negative_count',
        'lm_uncertainty_count', 'lm_litigious_count', 'lm_constraining_count',
        'lm_positive_ratio', 'lm_negative_ratio', 'lm_uncertainty_ratio',
        'lm_net_sentiment', 'lm_net_ratio', 'lm_polarity',
    ]
    scores_df = pd.DataFrame(results, columns=cols, index=chunk.index)
    return pd.concat([chunk, scores_df], axis=1)


# =======================
# Keyword Tagging
# =======================

def _has_keyword(text, keywords):
    """Check if text contains any keyword from list (word-boundary aware)."""
    if pd.isna(text) or text == '':
        return False
    t = str(text).lower()
    for kw in keywords:
        kw_stripped = kw.strip()
        if kw.startswith(' ') and kw.endswith(' '):
            if kw in ' ' + t + ' ':
                return True
        else:
            if re.search(r'\b' + re.escape(kw_stripped) + r'\b', t):
                return True
    return False


def tag_keywords(df):
    """Add boolean columns for AI and COVID keyword presence."""
    print("  Tagging AI keywords...")
    df['is_ai_related'] = df['post_text'].apply(lambda t: _has_keyword(t, AI_KEYWORDS))
    print(f"    AI posts: {df['is_ai_related'].sum():,}")

    print("  Tagging COVID keywords...")
    df['is_covid_related'] = df['post_text'].apply(lambda t: _has_keyword(t, COVID_KEYWORDS))
    print(f"    COVID posts: {df['is_covid_related'].sum():,}")

    return df


# =======================
# Summary Statistics
# =======================

def compute_summary(df, label="all"):
    """Compute summary statistics for a (sub)set."""
    stats = {}
    stats['label'] = label
    stats['n_posts'] = int(len(df))
    stats['n_unique_posts'] = int(df['post_url'].nunique()) if 'post_url' in df.columns else None
    stats['n_profiles'] = int(df['profile_url'].nunique()) if 'profile_url' in df.columns else None

    for col in ['lm_net_sentiment', 'lm_positive_ratio', 'lm_negative_ratio',
                'lm_uncertainty_ratio', 'lm_polarity', 'lm_word_count']:
        if col in df.columns:
            s = df[col]
            stats[f'{col}_mean'] = float(s.mean())
            stats[f'{col}_median'] = float(s.median())
            stats[f'{col}_std'] = float(s.std())
            stats[f'{col}_p25'] = float(s.quantile(0.25))
            stats[f'{col}_p75'] = float(s.quantile(0.75))

    # Positive / negative / neutral breakdown
    if 'lm_net_sentiment' in df.columns:
        net = df['lm_net_sentiment']
        stats['pct_positive'] = float((net > 0).mean() * 100)
        stats['pct_negative'] = float((net < 0).mean() * 100)
        stats['pct_neutral'] = float((net == 0).mean() * 100)

    return stats


def print_summary(stats):
    """Pretty-print summary statistics."""
    label = stats.get('label', '')
    print(f"\n  [{label.upper()}]")
    print(f"    Posts: {stats['n_posts']:,}  |  Profiles: {stats.get('n_profiles', 'N/A')}")
    print(f"    Net sentiment  — mean: {stats.get('lm_net_sentiment_mean', 0):.2f}  "
          f"median: {stats.get('lm_net_sentiment_median', 0):.2f}  "
          f"std: {stats.get('lm_net_sentiment_std', 0):.2f}")
    print(f"    Positive: {stats.get('pct_positive', 0):.1f}%  "
          f"Negative: {stats.get('pct_negative', 0):.1f}%  "
          f"Neutral: {stats.get('pct_neutral', 0):.1f}%")
    print(f"    Polarity (pos/(pos+neg)) — mean: {stats.get('lm_polarity_mean', 0):.3f}")
    print(f"    Word count — mean: {stats.get('lm_word_count_mean', 0):.0f}  "
          f"median: {stats.get('lm_word_count_median', 0):.0f}")


# =======================
# Main
# =======================

def main():
    parser = argparse.ArgumentParser(
        description='L-M sentiment analysis on full LinkedIn posts dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--posts', type=str, help='Path to posts_combined.csv')
    parser.add_argument('--lm-dict', type=str, help='Path to L-M Master Dictionary CSV')
    parser.add_argument('--output-dir', type=str, help='Output directory')
    parser.add_argument('--chunk-size', type=int, default=100_000,
                        help='Rows per processing chunk (default: 100000)')
    parser.add_argument('--ai-only', action='store_true',
                        help='Only score AI-related posts (faster, less memory)')
    parser.add_argument('--no-save-full', action='store_true',
                        help='Skip saving the full scored CSV (save subsets only)')
    args = parser.parse_args()

    paths = default_paths()
    posts_path = Path(args.posts) if args.posts else paths['posts']
    lm_path = Path(args.lm_dict) if args.lm_dict else paths['lm_dict']
    output_dir = Path(args.output_dir) if args.output_dir else paths['output_dir']
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    print("=" * 80)
    print("  LOUGHRAN-MCDONALD SENTIMENT ANALYSIS — FULL SAMPLE")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # ---- Load dictionary ----
    pos, neg, unc, lit, con, strong, weak = load_lm_dictionary(lm_path)

    # ---- Process in chunks ----
    if not posts_path.exists():
        print(f"\nERROR: Posts file not found: {posts_path}")
        sys.exit(1)

    print(f"\nReading posts from: {posts_path}")
    print(f"Chunk size: {args.chunk_size:,}")

    t0 = time.time()

    # --- If ai-only, do a two-pass approach: tag first, then score ---
    if args.ai_only:
        print("\n--- AI-ONLY MODE ---")
        print("Pass 1: Identifying AI-related posts...")

        ai_indices = []
        chunk_reader = pd.read_csv(
            posts_path, engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
            chunksize=args.chunk_size,
        )
        total_rows = 0
        for i, chunk in enumerate(chunk_reader):
            total_rows += len(chunk)
            mask = chunk['post_text'].apply(lambda t: _has_keyword(t, AI_KEYWORDS))
            ai_indices.extend(chunk.index[mask].tolist())
            print(f"  Chunk {i + 1}: {total_rows:,} rows scanned, {len(ai_indices):,} AI posts found", end='\r')

        print(f"\n  Total: {total_rows:,} rows, {len(ai_indices):,} AI posts")
        print("\nPass 2: Scoring AI posts...")

        # Re-read and filter
        df_all = pd.read_csv(
            posts_path, engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
        )
        df_ai = df_all.iloc[ai_indices].copy()
        df_scored = score_chunk(df_ai, pos, neg, unc, lit, con)
        df_scored['is_ai_related'] = True

        # Save
        ai_path = output_dir / f'sentiment_ai_posts_{timestamp}.csv'
        df_scored.to_csv(ai_path, index=False)
        print(f"\n  Saved AI posts: {ai_path}")

        stats_ai = compute_summary(df_scored, 'ai_posts')
        print_summary(stats_ai)

        report_path = output_dir / f'sentiment_summary_{timestamp}.json'
        with open(report_path, 'w') as f:
            json.dump({'ai_posts': stats_ai}, f, indent=2, default=str)
        print(f"  Summary: {report_path}")

    else:
        # --- Full scoring with chunked append ---
        print("\n--- FULL SCORING MODE ---")

        # Output file for incremental writes
        full_output = output_dir / f'sentiment_all_posts_{timestamp}.csv'
        header_written = False
        total_scored = 0

        chunk_reader = pd.read_csv(
            posts_path, engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
            chunksize=args.chunk_size,
        )

        for i, chunk in enumerate(chunk_reader):
            t_chunk = time.time()
            scored = score_chunk(chunk, pos, neg, unc, lit, con)
            total_scored += len(scored)

            # Append to CSV (write header only once)
            if not args.no_save_full:
                scored.to_csv(
                    full_output,
                    mode='a',
                    header=not header_written,
                    index=False,
                )
                header_written = True

            elapsed = time.time() - t_chunk
            total_elapsed = time.time() - t0
            rate = total_scored / total_elapsed
            print(f"  Chunk {i + 1}: {total_scored:>10,} posts scored  "
                  f"({elapsed:.1f}s this chunk, {rate:,.0f} posts/sec overall)")

        if not args.no_save_full:
            print(f"\n  Full scored CSV: {full_output}")

        # --- Tag and extract subsets ---
        print("\nTagging keyword subsets (reading scored file)...")
        # Read back the scored file for tagging
        # (more memory efficient than holding everything in RAM)
        df_scored = pd.read_csv(
            full_output if not args.no_save_full else posts_path,
            engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
        )

        df_scored = tag_keywords(df_scored)

        # Save subsets
        ai_subset = df_scored[df_scored['is_ai_related']].copy()
        covid_subset = df_scored[df_scored['is_covid_related']].copy()

        ai_path = output_dir / f'sentiment_ai_posts_{timestamp}.csv'
        covid_path = output_dir / f'sentiment_covid_posts_{timestamp}.csv'

        ai_subset.to_csv(ai_path, index=False)
        covid_subset.to_csv(covid_path, index=False)

        print(f"  AI subset ({len(ai_subset):,} rows): {ai_path}")
        print(f"  COVID subset ({len(covid_subset):,} rows): {covid_path}")

        # If not saving full, re-save with tags
        if not args.no_save_full:
            # Overwrite with tagged version
            df_scored.to_csv(full_output, index=False)
            print(f"  Full CSV updated with keyword tags: {full_output}")

        # --- Summary statistics ---
        print("\n" + "=" * 80)
        print("  RESULTS SUMMARY")
        print("=" * 80)

        stats_all = compute_summary(df_scored, 'all_posts')
        stats_ai = compute_summary(ai_subset, 'ai_posts')
        stats_covid = compute_summary(covid_subset, 'covid_posts')

        # Non-AI posts for comparison
        non_ai = df_scored[~df_scored['is_ai_related']]
        stats_non_ai = compute_summary(non_ai, 'non_ai_posts')

        print_summary(stats_all)
        print_summary(stats_ai)
        print_summary(stats_non_ai)
        print_summary(stats_covid)

        # Difference
        if stats_ai['n_posts'] > 0 and stats_non_ai['n_posts'] > 0:
            diff = stats_ai.get('lm_net_sentiment_mean', 0) - stats_non_ai.get('lm_net_sentiment_mean', 0)
            print(f"\n  AI vs Non-AI net sentiment difference: {diff:+.2f}")

        # Save JSON report
        summary = {
            'timestamp': timestamp,
            'total_scored': total_scored,
            'all_posts': stats_all,
            'ai_posts': stats_ai,
            'non_ai_posts': stats_non_ai,
            'covid_posts': stats_covid,
        }
        report_path = output_dir / f'sentiment_summary_{timestamp}.json'
        with open(report_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n  Summary JSON: {report_path}")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time / 60:.1f} minutes")
    print("  Done.")


if __name__ == '__main__':
    main()
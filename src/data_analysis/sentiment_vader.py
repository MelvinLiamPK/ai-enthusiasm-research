#!/usr/bin/env python3
"""
VADER Sentiment Analysis — Full Sample
========================================
Adds VADER sentiment scores to the existing scored dataset.
Reads the most recent sentiment_all_posts CSV (with L-M + FinBERT columns)
and appends vader_* columns, producing a unified output.

VADER is deterministic and designed for social media text — it handles
capitalization, punctuation emphasis, emojis, and negation heuristics.
No GPU required.

Usage:
    python3 src/data_analysis/sentiment_vader.py
    python3 src/data_analysis/sentiment_vader.py --posts outputs/sentiment_results/sentiment_all_posts_20260331_182630.csv
    python3 src/data_analysis/sentiment_vader.py --prototype 1000
    python3 src/data_analysis/sentiment_vader.py --ai-only

On Sherlock (SLURM):
    sbatch scripts/slurm_sentiment_vader.sh

Outputs (in outputs/sentiment_results/):
    - sentiment_all_posts_YYYYMMDD_HHMMSS.csv      Unified dataset (L-M + FinBERT + VADER)
    - sentiment_ai_posts_YYYYMMDD_HHMMSS.csv       AI-related subset
    - sentiment_covid_posts_YYYYMMDD_HHMMSS.csv    COVID subset
    - vader_summary_YYYYMMDD_HHMMSS.json           VADER summary statistics
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

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
except ImportError:
    print("ERROR: Install vaderSentiment:")
    print("  pip install vaderSentiment")
    sys.exit(1)


# =======================
# Path Resolution
# =======================

def resolve_project_root():
    """Resolve project root from script location (2 levels up from src/data_analysis/)."""
    return Path(__file__).resolve().parent.parent.parent


def default_paths():
    """Return default file paths relative to project root."""
    root = resolve_project_root()
    output_dir = root / 'outputs' / 'sentiment_results'
    return {
        'posts': _find_latest_scored_output(output_dir),
        'output_dir': output_dir,
    }


def _find_latest_scored_output(output_dir):
    """Find the most recent sentiment_all_posts_*.csv that does NOT already have VADER columns."""
    output_dir = Path(output_dir)
    if not output_dir.exists():
        return None
    candidates = sorted(output_dir.glob('sentiment_all_posts_*.csv'))
    for path in reversed(candidates):
        sample = pd.read_csv(path, engine='c', lineterminator='\n',
                             on_bad_lines='skip', nrows=1)
        if 'vader_compound' not in sample.columns:
            return path
    if candidates:
        return candidates[-1]
    return None


# =======================
# Keyword Definitions (shared with L-M / FinBERT scripts)
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
# VADER Scoring
# =======================

def score_chunk(chunk, analyzer):
    """
    Score a DataFrame chunk with VADER.

    No text preprocessing — VADER uses capitalization, punctuation, and emojis
    as sentiment signals.

    Returns chunk with vader_compound, vader_positive, vader_negative,
    vader_neutral columns appended.
    """
    results = []
    for text in chunk['post_text']:
        if pd.isna(text) or str(text).strip() == '':
            results.append((0.0, 0.0, 0.0, 0.0))
            continue
        scores = analyzer.polarity_scores(str(text))
        results.append((
            scores['compound'],
            scores['pos'],
            scores['neg'],
            scores['neu'],
        ))

    cols = ['vader_compound', 'vader_positive', 'vader_negative', 'vader_neutral']
    scores_df = pd.DataFrame(results, columns=cols, index=chunk.index)

    return pd.concat([chunk, scores_df], axis=1)


# =======================
# Summary Statistics
# =======================

def compute_summary(df, label="all"):
    """Compute summary statistics for a (sub)set."""
    stats = {}
    stats['label'] = label
    stats['n_posts'] = int(len(df))
    stats['n_profiles'] = int(df['profile_url'].nunique()) if 'profile_url' in df.columns else None

    for col in ['vader_compound', 'vader_positive', 'vader_negative', 'vader_neutral']:
        if col in df.columns:
            s = df[col]
            stats[f'{col}_mean'] = float(s.mean())
            stats[f'{col}_median'] = float(s.median())
            stats[f'{col}_std'] = float(s.std())

    # Sentiment distribution (standard VADER thresholds)
    if 'vader_compound' in df.columns:
        c = df['vader_compound']
        stats['pct_positive'] = float((c > 0.05).mean() * 100)
        stats['pct_negative'] = float((c < -0.05).mean() * 100)
        stats['pct_neutral'] = float(((c >= -0.05) & (c <= 0.05)).mean() * 100)

    return stats


def print_summary(stats):
    """Pretty-print summary statistics."""
    label = stats.get('label', '')
    print(f"\n  [{label.upper()}]")
    print(f"    Posts: {stats['n_posts']:,}  |  Profiles: {stats.get('n_profiles', 'N/A')}")
    print(f"    Compound — mean: {stats.get('vader_compound_mean', 0):.3f}  "
          f"median: {stats.get('vader_compound_median', 0):.3f}  "
          f"std: {stats.get('vader_compound_std', 0):.3f}")
    print(f"    Positive prop — mean: {stats.get('vader_positive_mean', 0):.3f}")
    print(f"    Negative prop — mean: {stats.get('vader_negative_mean', 0):.3f}")
    print(f"    Neutral prop  — mean: {stats.get('vader_neutral_mean', 0):.3f}")
    print(f"    Distribution: Positive {stats.get('pct_positive', 0):.1f}%  "
          f"Negative {stats.get('pct_negative', 0):.1f}%  "
          f"Neutral {stats.get('pct_neutral', 0):.1f}%")


# =======================
# Main
# =======================

def main():
    parser = argparse.ArgumentParser(
        description='VADER sentiment analysis on LinkedIn posts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--posts', type=str,
                        help='Path to scored CSV (default: most recent sentiment_all_posts_*.csv without VADER)')
    parser.add_argument('--output-dir', type=str, help='Output directory')
    parser.add_argument('--chunk-size', type=int, default=100_000,
                        help='Rows per processing chunk (default: 100000)')
    parser.add_argument('--ai-only', action='store_true',
                        help='Only score AI-related posts')
    parser.add_argument('--no-save-full', action='store_true',
                        help='Skip saving the full scored CSV (save subsets only)')
    parser.add_argument('--prototype', type=int, default=0,
                        help='Score only first N posts (for testing)')
    args = parser.parse_args()

    paths = default_paths()
    posts_path = Path(args.posts) if args.posts else paths['posts']
    output_dir = Path(args.output_dir) if args.output_dir else paths['output_dir']
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    print("=" * 80)
    print("  VADER SENTIMENT ANALYSIS")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # ---- Initialize VADER ----
    analyzer = SentimentIntensityAnalyzer()
    print("VADER analyzer initialized")

    # ---- Verify input file ----
    if posts_path is None or not posts_path.exists():
        print(f"\nERROR: Input file not found: {posts_path}")
        print("Run sentiment_analysis_full.py and/or sentiment_finbert.py first,")
        print("or pass --posts explicitly.")
        sys.exit(1)

    # Check existing columns
    sample = pd.read_csv(posts_path, engine='c', lineterminator='\n',
                         on_bad_lines='skip', nrows=1)
    existing = []
    if 'lm_net_sentiment' in sample.columns:
        existing.append('L-M')
    if 'finbert_positive' in sample.columns:
        existing.append('FinBERT')
    print(f"\nReading scored file: {posts_path}")
    if existing:
        print(f"  (Existing scores: {', '.join(existing)} — VADER will be appended)")
    else:
        print("  (No existing scores found — scoring VADER only)")
    print(f"Chunk size: {args.chunk_size:,}")

    t0 = time.time()

    if args.ai_only:
        # --- AI-only mode ---
        print("\n--- AI-ONLY MODE ---")

        df_all = pd.read_csv(
            posts_path, engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
            nrows=args.prototype if args.prototype > 0 else None,
        )
        df_all = df_all[df_all['profile_url'].notna()].copy()

        if 'is_ai_related' in df_all.columns:
            df_ai = df_all[df_all['is_ai_related']].copy()
        else:
            mask = df_all['post_text'].apply(lambda t: _has_keyword(t, AI_KEYWORDS))
            df_ai = df_all[mask].copy()
        print(f"  {len(df_all):,} total posts -> {len(df_ai):,} AI posts")

        print("\nScoring AI posts with VADER...")
        df_scored = score_chunk(df_ai, analyzer)
        df_scored['is_ai_related'] = True

        ai_path = output_dir / f'sentiment_ai_posts_{timestamp}.csv'
        df_scored.to_csv(ai_path, index=False)
        print(f"\n  Saved: {ai_path}")

        stats_ai = compute_summary(df_scored, 'ai_posts')
        print_summary(stats_ai)

        report_path = output_dir / f'vader_summary_{timestamp}.json'
        with open(report_path, 'w') as f:
            json.dump({'ai_posts': stats_ai}, f, indent=2, default=str)

    else:
        # --- Full scoring with chunked append ---
        print("\n--- FULL SCORING MODE ---")

        full_output = output_dir / f'sentiment_all_posts_{timestamp}.csv'
        header_written = False
        total_scored = 0

        chunk_reader = pd.read_csv(
            posts_path, engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
            chunksize=args.chunk_size,
            nrows=args.prototype if args.prototype > 0 else None,
        )

        for i, chunk in enumerate(chunk_reader):
            chunk = chunk[chunk['profile_url'].notna()].copy()

            t_chunk = time.time()
            scored = score_chunk(chunk, analyzer)
            total_scored += len(scored)

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
            rate = total_scored / total_elapsed if total_elapsed > 0 else 0
            print(f"  Chunk {i + 1}: {total_scored:>10,} posts scored  "
                  f"({elapsed:.1f}s this chunk, {rate:,.0f} posts/sec overall)")

        if not args.no_save_full:
            print(f"\n  Full scored CSV: {full_output}")

        # --- Tag and extract subsets ---
        print("\nReading scored file for subsetting...")
        df_scored = pd.read_csv(
            full_output if not args.no_save_full else posts_path,
            engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
        )

        if 'is_ai_related' not in df_scored.columns:
            print("Tagging keyword subsets...")
            df_scored = tag_keywords(df_scored)
        else:
            print(f"  Using existing keyword tags (AI: {df_scored['is_ai_related'].sum():,}, "
                  f"COVID: {df_scored['is_covid_related'].sum():,})")

        ai_subset = df_scored[df_scored['is_ai_related']].copy()
        covid_subset = df_scored[df_scored['is_covid_related']].copy()

        ai_path = output_dir / f'sentiment_ai_posts_{timestamp}.csv'
        covid_path = output_dir / f'sentiment_covid_posts_{timestamp}.csv'

        ai_subset.to_csv(ai_path, index=False)
        covid_subset.to_csv(covid_path, index=False)

        print(f"  AI subset ({len(ai_subset):,} rows): {ai_path}")
        print(f"  COVID subset ({len(covid_subset):,} rows): {covid_path}")

        if not args.no_save_full:
            df_scored.to_csv(full_output, index=False)
            print(f"  Full CSV updated with keyword tags: {full_output}")

        # --- Summary ---
        print("\n" + "=" * 80)
        print("  RESULTS SUMMARY")
        print("=" * 80)

        stats_all = compute_summary(df_scored, 'all_posts')
        stats_ai = compute_summary(ai_subset, 'ai_posts')
        stats_covid = compute_summary(covid_subset, 'covid_posts')

        non_ai = df_scored[~df_scored['is_ai_related']]
        stats_non_ai = compute_summary(non_ai, 'non_ai_posts')

        print_summary(stats_all)
        print_summary(stats_ai)
        print_summary(stats_non_ai)
        print_summary(stats_covid)

        if stats_ai['n_posts'] > 0 and stats_non_ai['n_posts'] > 0:
            diff = stats_ai.get('vader_compound_mean', 0) - stats_non_ai.get('vader_compound_mean', 0)
            print(f"\n  AI vs Non-AI compound difference: {diff:+.3f}")

        summary = {
            'timestamp': timestamp,
            'total_scored': total_scored,
            'all_posts': stats_all,
            'ai_posts': stats_ai,
            'non_ai_posts': stats_non_ai,
            'covid_posts': stats_covid,
        }
        report_path = output_dir / f'vader_summary_{timestamp}.json'
        with open(report_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n  Summary JSON: {report_path}")

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time / 60:.1f} minutes")
    print("  Done.")


if __name__ == '__main__':
    main()

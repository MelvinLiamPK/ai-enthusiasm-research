#!/usr/bin/env python3
"""
FinBERT Sentiment Analysis — Full Sample
==========================================
Adds FinBERT (ProsusAI/finbert) sentiment scores to the L-M scored dataset.
Reads the existing sentiment_all_posts CSV and appends finbert_* columns,
producing a single unified output with both L-M and FinBERT scores.

Designed for GPU inference on Sherlock. Falls back to CPU if no GPU available.

Usage:
    # Score the L-M output (default: most recent sentiment_all_posts_*.csv)
    python3 src/data_analysis/sentiment_finbert.py
    python3 src/data_analysis/sentiment_finbert.py --posts outputs/sentiment_results/sentiment_all_posts_20260328_172937.csv
    python3 src/data_analysis/sentiment_finbert.py --prototype 1000
    python3 src/data_analysis/sentiment_finbert.py --ai-only

On Sherlock (SLURM):
    sbatch scripts/slurm_sentiment_finbert.sh

Outputs (in outputs/sentiment_results/):
    - sentiment_all_posts_YYYYMMDD_HHMMSS.csv      Unified dataset (L-M + FinBERT)
    - sentiment_ai_posts_YYYYMMDD_HHMMSS.csv       AI-related subset
    - sentiment_covid_posts_YYYYMMDD_HHMMSS.csv    COVID subset
    - finbert_summary_YYYYMMDD_HHMMSS.json         FinBERT summary statistics
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
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
except ImportError:
    print("ERROR: Install transformers and torch:")
    print("  pip install transformers torch")
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
        'posts': _find_latest_lm_output(output_dir),
        'output_dir': output_dir,
    }


def _find_latest_lm_output(output_dir):
    """Find the most recent sentiment_all_posts_*.csv that has L-M columns but NOT FinBERT columns."""
    output_dir = Path(output_dir)
    if not output_dir.exists():
        return None
    candidates = sorted(output_dir.glob('sentiment_all_posts_*.csv'))
    # Check from newest to oldest, skip files that already have FinBERT scores
    for path in reversed(candidates):
        sample = pd.read_csv(path, engine='c', lineterminator='\n',
                             on_bad_lines='skip', nrows=1)
        if 'lm_net_sentiment' in sample.columns and 'finbert_positive' not in sample.columns:
            return path
    # Fall back to most recent if all have FinBERT (re-run scenario)
    if candidates:
        return candidates[-1]
    return None


# =======================
# Keyword Definitions (shared with L-M script)
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
# Keyword Tagging (reused from L-M script)
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
# Text Preprocessing
# =======================

def clean_text(text):
    """Clean post text for FinBERT. Less aggressive than L-M — keep punctuation for context."""
    if pd.isna(text):
        return ""
    text = str(text)
    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    # Remove mentions
    text = re.sub(r'@\w+', '', text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# =======================
# FinBERT Model
# =======================

def load_finbert(device):
    """Load ProsusAI/finbert model and tokenizer."""
    model_name = "ProsusAI/finbert"
    print(f"Loading FinBERT from: {model_name}")
    print(f"Device: {device}")

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    model.to(device)
    model.eval()  # Disable dropout for near-deterministic inference

    print(f"  Model loaded. Labels: {model.config.id2label}")
    return tokenizer, model


def score_batch(texts, tokenizer, model, device, max_length=512):
    """
    Score a batch of texts with FinBERT.

    Returns:
        np.ndarray of shape (n, 3) with [positive, negative, neutral] probabilities.
    """
    # Tokenize with truncation (FinBERT max = 512 tokens)
    inputs = tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=max_length,
        return_tensors='pt',
    )
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.softmax(outputs.logits, dim=-1).cpu().numpy()

    return probs


# =======================
# Chunked Scoring
# =======================

FINBERT_LABELS = ['positive', 'negative', 'neutral']


def score_chunk(chunk, tokenizer, model, device, batch_size=64):
    """
    Score a DataFrame chunk with FinBERT.

    Returns chunk with finbert_positive, finbert_negative, finbert_neutral,
    finbert_sentiment, finbert_score columns appended.
    """
    texts = chunk['post_text'].apply(clean_text).tolist()

    all_probs = []
    for start in range(0, len(texts), batch_size):
        batch = texts[start:start + batch_size]
        # Replace empty strings with a space (tokenizer needs non-empty input)
        batch = [t if t.strip() else " " for t in batch]
        probs = score_batch(batch, tokenizer, model, device)
        all_probs.append(probs)

    all_probs = np.vstack(all_probs)

    # ProsusAI/finbert label order: positive=0, negative=1, neutral=2
    # Verify from model config and map accordingly
    label_map = model.config.id2label  # e.g. {0: 'positive', 1: 'negative', 2: 'neutral'}
    col_order = [label_map[i] for i in range(3)]

    scores_df = pd.DataFrame(all_probs, columns=[f'finbert_{c}' for c in col_order], index=chunk.index)

    # Predicted label and confidence
    pred_idx = all_probs.argmax(axis=1)
    scores_df['finbert_sentiment'] = [col_order[i] for i in pred_idx]
    scores_df['finbert_score'] = all_probs.max(axis=1)

    return pd.concat([chunk, scores_df], axis=1)


# =======================
# Checkpointing
# =======================

CHECKPOINT_FILE = '.finbert_checkpoint.json'


def load_checkpoint(output_dir):
    """Load checkpoint if it exists."""
    ckpt_path = output_dir / CHECKPOINT_FILE
    if ckpt_path.exists():
        with open(ckpt_path) as f:
            return json.load(f)
    return None


def save_checkpoint(output_dir, chunk_idx, total_scored, timestamp, output_file):
    """Save checkpoint after each chunk."""
    ckpt_path = output_dir / CHECKPOINT_FILE
    with open(ckpt_path, 'w') as f:
        json.dump({
            'chunk_idx': chunk_idx,
            'total_scored': total_scored,
            'timestamp': timestamp,
            'output_file': str(output_file),
        }, f)


def clear_checkpoint(output_dir):
    """Remove checkpoint file on successful completion."""
    ckpt_path = output_dir / CHECKPOINT_FILE
    if ckpt_path.exists():
        ckpt_path.unlink()


# =======================
# Summary Statistics
# =======================

def compute_summary(df, label="all"):
    """Compute summary statistics for a (sub)set."""
    stats = {}
    stats['label'] = label
    stats['n_posts'] = int(len(df))
    stats['n_profiles'] = int(df['profile_url'].nunique()) if 'profile_url' in df.columns else None

    for col in ['finbert_positive', 'finbert_negative', 'finbert_neutral', 'finbert_score']:
        if col in df.columns:
            s = df[col]
            stats[f'{col}_mean'] = float(s.mean())
            stats[f'{col}_median'] = float(s.median())
            stats[f'{col}_std'] = float(s.std())

    # Sentiment distribution
    if 'finbert_sentiment' in df.columns:
        vc = df['finbert_sentiment'].value_counts(normalize=True) * 100
        stats['pct_positive'] = float(vc.get('positive', 0))
        stats['pct_negative'] = float(vc.get('negative', 0))
        stats['pct_neutral'] = float(vc.get('neutral', 0))

    return stats


def print_summary(stats):
    """Pretty-print summary statistics."""
    label = stats.get('label', '')
    print(f"\n  [{label.upper()}]")
    print(f"    Posts: {stats['n_posts']:,}  |  Profiles: {stats.get('n_profiles', 'N/A')}")
    print(f"    Positive prob — mean: {stats.get('finbert_positive_mean', 0):.3f}  "
          f"median: {stats.get('finbert_positive_median', 0):.3f}")
    print(f"    Negative prob — mean: {stats.get('finbert_negative_mean', 0):.3f}  "
          f"median: {stats.get('finbert_negative_median', 0):.3f}")
    print(f"    Neutral prob  — mean: {stats.get('finbert_neutral_mean', 0):.3f}  "
          f"median: {stats.get('finbert_neutral_median', 0):.3f}")
    print(f"    Distribution: Positive {stats.get('pct_positive', 0):.1f}%  "
          f"Negative {stats.get('pct_negative', 0):.1f}%  "
          f"Neutral {stats.get('pct_neutral', 0):.1f}%")
    print(f"    Confidence — mean: {stats.get('finbert_score_mean', 0):.3f}")


# =======================
# Main
# =======================

def main():
    parser = argparse.ArgumentParser(
        description='FinBERT sentiment analysis on LinkedIn posts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--posts', type=str,
                        help='Path to L-M scored CSV (default: most recent sentiment_all_posts_*.csv)')
    parser.add_argument('--output-dir', type=str, help='Output directory')
    parser.add_argument('--chunk-size', type=int, default=50_000,
                        help='Rows per processing chunk (default: 50000)')
    parser.add_argument('--batch-size', type=int, default=64,
                        help='GPU inference batch size (default: 64)')
    parser.add_argument('--ai-only', action='store_true',
                        help='Only score AI-related posts')
    parser.add_argument('--no-save-full', action='store_true',
                        help='Skip saving the full scored CSV (save subsets only)')
    parser.add_argument('--prototype', type=int, default=0,
                        help='Score only first N posts (for testing)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from last checkpoint')
    args = parser.parse_args()

    paths = default_paths()
    posts_path = Path(args.posts) if args.posts else paths['posts']
    output_dir = Path(args.output_dir) if args.output_dir else paths['output_dir']
    output_dir.mkdir(parents=True, exist_ok=True)

    # Reproducibility
    torch.manual_seed(42)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(42)

    # Device selection
    if torch.cuda.is_available():
        device = torch.device('cuda')
    elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
        device = torch.device('mps')
    else:
        device = torch.device('cpu')

    print("=" * 80)
    print("  FINBERT SENTIMENT ANALYSIS")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Device: {device}")
    print("=" * 80)

    # ---- Check resume ----
    checkpoint = None
    if args.resume:
        checkpoint = load_checkpoint(output_dir)
        if checkpoint:
            print(f"\n  Resuming from chunk {checkpoint['chunk_idx'] + 1} "
                  f"({checkpoint['total_scored']:,} posts already scored)")

    timestamp = checkpoint['timestamp'] if checkpoint else datetime.now().strftime('%Y%m%d_%H%M%S')

    # ---- Load model ----
    tokenizer, model = load_finbert(device)

    # ---- Verify input file ----
    if posts_path is None or not posts_path.exists():
        print(f"\nERROR: Input file not found: {posts_path}")
        print("Run sentiment_analysis_full.py first to generate L-M scores,")
        print("or pass --posts explicitly.")
        sys.exit(1)

    # Check if input already has L-M columns
    has_lm = False
    sample = pd.read_csv(posts_path, engine='c', lineterminator='\n',
                         on_bad_lines='skip', nrows=1)
    has_lm = 'lm_net_sentiment' in sample.columns
    if has_lm:
        print(f"\nReading L-M scored file: {posts_path}")
        print("  (L-M columns detected — FinBERT scores will be appended)")
    else:
        print(f"\nReading raw posts from: {posts_path}")
        print("  (No L-M columns found — scoring FinBERT only)")
    print(f"Chunk size: {args.chunk_size:,}  |  Batch size: {args.batch_size}")

    t0 = time.time()

    if args.ai_only:
        # --- AI-only mode ---
        print("\n--- AI-ONLY MODE ---")
        print("Pass 1: Identifying AI-related posts...")

        df_all = pd.read_csv(
            posts_path, engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
            nrows=args.prototype if args.prototype > 0 else None,
        )
        df_all = df_all[df_all['profile_url'].notna()].copy()

        mask = df_all['post_text'].apply(lambda t: _has_keyword(t, AI_KEYWORDS))
        df_ai = df_all[mask].copy()
        print(f"  {len(df_all):,} total posts → {len(df_ai):,} AI posts")

        print("\nPass 2: Scoring AI posts with FinBERT...")
        df_scored = score_chunk(df_ai, tokenizer, model, device, args.batch_size)
        df_scored['is_ai_related'] = True

        ai_path = output_dir / f'sentiment_ai_posts_{timestamp}.csv'
        df_scored.to_csv(ai_path, index=False)
        print(f"\n  Saved: {ai_path}")

        stats_ai = compute_summary(df_scored, 'ai_posts')
        print_summary(stats_ai)

        report_path = output_dir / f'finbert_summary_{timestamp}.json'
        with open(report_path, 'w') as f:
            json.dump({'ai_posts': stats_ai}, f, indent=2, default=str)

    else:
        # --- Full scoring with chunked append ---
        print("\n--- FULL SCORING MODE ---")

        full_output = output_dir / f'sentiment_all_posts_{timestamp}.csv'

        # Resume state
        start_chunk = 0
        total_scored = 0
        header_written = False
        if checkpoint and args.resume:
            start_chunk = checkpoint['chunk_idx'] + 1
            total_scored = checkpoint['total_scored']
            header_written = True  # Header already written in previous run

        chunk_reader = pd.read_csv(
            posts_path, engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
            chunksize=args.chunk_size,
            nrows=args.prototype if args.prototype > 0 else None,
        )

        for i, chunk in enumerate(chunk_reader):
            if i < start_chunk:
                continue  # Skip already-processed chunks

            # Filter empty rows
            chunk = chunk[chunk['profile_url'].notna()].copy()

            t_chunk = time.time()
            scored = score_chunk(chunk, tokenizer, model, device, args.batch_size)
            total_scored += len(scored)

            # Append to CSV
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

            save_checkpoint(output_dir, i, total_scored, timestamp, str(full_output))

        if not args.no_save_full:
            print(f"\n  Full scored CSV: {full_output}")

        # --- Tag and extract subsets ---
        print("\nReading scored file for subsetting...")
        df_scored = pd.read_csv(
            full_output if not args.no_save_full else posts_path,
            engine='c', lineterminator='\n',
            on_bad_lines='skip', low_memory=False,
        )

        # Re-use keyword tags from L-M output if present, otherwise tag fresh
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

        # Difference
        if stats_ai['n_posts'] > 0 and stats_non_ai['n_posts'] > 0:
            diff = stats_ai.get('finbert_positive_mean', 0) - stats_non_ai.get('finbert_positive_mean', 0)
            print(f"\n  AI vs Non-AI positive prob difference: {diff:+.3f}")

        summary = {
            'timestamp': timestamp,
            'total_scored': total_scored,
            'device': str(device),
            'all_posts': stats_all,
            'ai_posts': stats_ai,
            'non_ai_posts': stats_non_ai,
            'covid_posts': stats_covid,
        }
        report_path = output_dir / f'finbert_summary_{timestamp}.json'
        with open(report_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n  Summary JSON: {report_path}")

        clear_checkpoint(output_dir)

    total_time = time.time() - t0
    print(f"\n  Total time: {total_time / 60:.1f} minutes")
    print("  Done.")


if __name__ == '__main__':
    main()

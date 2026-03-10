#!/usr/bin/env python3
"""
Data Quality Checks for Combined LinkedIn Posts
================================================
Comprehensive validation of posts_combined.csv before sentiment analysis.

Reports issues but does NOT modify the dataset — deduplication decisions
should be made at analysis time depending on unit of observation
(post-level vs. person-level vs. company-level).

Usage:
    python3 src/data_analysis/data_quality_checks.py
    python3 src/data_analysis/data_quality_checks.py --posts /path/to/posts_combined.csv
    python3 src/data_analysis/data_quality_checks.py --urls /path/to/all_linkedin_urls.csv
    python3 src/data_analysis/data_quality_checks.py --directors /path/to/directors.csv

On Sherlock (SLURM):
    sbatch slurm_quality_checks.sh
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime
import argparse
import sys
import warnings
import json
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
        'urls': root / 'data' / 'processed' / 'all_people_linkedin_urls' / 'all_linkedin_urls.csv',
        'directors': root / 'data' / 'raw' / 'directors.csv',
        'output_dir': root / 'outputs' / 'quality_checks',
    }


# =======================
# Helpers
# =======================

def section(title):
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")


def subsection(title):
    print(f"\n--- {title} ---")


def load_posts(filepath):
    """Load combined posts CSV with embedded-newline-safe reader."""
    print(f"Loading posts from: {filepath}")
    df = pd.read_csv(
        filepath,
        engine="c",
        lineterminator="\n",
        on_bad_lines="skip",
        low_memory=False,
    )
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


# =======================
# Check 1: Schema Validation
# =======================

EXPECTED_COLUMNS = [
    'company_name', 'person_name', 'position', 'source', 'gvkey', 'ticker',
    'execid', 'person_name_clean', 'company_name_clean', 'profile_url',
    'post_text', 'post_url', 'post_type', 'post_date', 'post_timestamp',
    'author_name', 'author_headline', 'reactions_total', 'likes', 'comments',
    'reposts', 'celebrates', 'supports', 'loves', 'insights', 'funnys',
    'media_type', 'article_url', 'article_title', 'reshared_text',
    'reshared_url', 'reshared_author',
]


def check_schema(df, report):
    """Validate column names and dtypes."""
    section("CHECK 1: SCHEMA VALIDATION")

    actual = set(df.columns)
    expected = set(EXPECTED_COLUMNS)

    missing = expected - actual
    extra = actual - expected

    print(f"  Expected columns: {len(expected)}")
    print(f"  Actual columns:   {len(actual)}")

    if missing:
        print(f"\n  MISSING columns ({len(missing)}):")
        for c in sorted(missing):
            print(f"    - {c}")
        report['schema_missing'] = sorted(missing)
    else:
        print("  All expected columns present.")

    if extra:
        print(f"\n  EXTRA columns ({len(extra)}):")
        for c in sorted(extra):
            print(f"    - {c}")
        report['schema_extra'] = sorted(extra)

    # Dtype summary
    subsection("Column dtypes")
    for col in df.columns:
        non_null = df[col].notna().sum()
        pct = non_null / len(df) * 100
        print(f"  {col:<25s}  dtype={str(df[col].dtype):<12s}  non-null={non_null:>10,} ({pct:5.1f}%)")

    report['total_rows'] = len(df)
    report['total_columns'] = len(df.columns)


# =======================
# Check 2: Null Analysis
# =======================

def check_nulls(df, report):
    """Identify null patterns including all-null placeholder rows."""
    section("CHECK 2: NULL ANALYSIS")

    # Per-column nulls
    subsection("Per-column null counts")
    null_counts = df.isnull().sum().sort_values(ascending=False)
    for col, n in null_counts.items():
        if n > 0:
            print(f"  {col:<25s}  {n:>10,} nulls ({n / len(df) * 100:5.1f}%)")

    # All-null rows (Apify placeholders)
    subsection("All-null placeholder rows")
    content_cols = ['post_text', 'post_url', 'post_type', 'post_date']
    available_content = [c for c in content_cols if c in df.columns]
    all_null_mask = df[available_content].isnull().all(axis=1)
    n_all_null = all_null_mask.sum()
    print(f"  Rows where ALL of {available_content} are null: {n_all_null:,}")
    report['all_null_rows'] = int(n_all_null)

    # Null post_text (expected for pure reshares)
    subsection("Null post_text breakdown")
    null_text = df['post_text'].isnull()
    n_null_text = null_text.sum()
    print(f"  Total null post_text: {n_null_text:,} ({n_null_text / len(df) * 100:.1f}%)")

    if 'post_type' in df.columns:
        print("\n  Null post_text by post_type:")
        for pt in df['post_type'].dropna().unique():
            mask = (df['post_type'] == pt) & null_text
            count = mask.sum()
            total_pt = (df['post_type'] == pt).sum()
            if total_pt > 0:
                print(f"    {pt:<15s}  {count:>10,} / {total_pt:>10,}  ({count / total_pt * 100:.1f}%)")

    report['null_post_text'] = int(n_null_text)


# =======================
# Check 3: Duplicate Detection (REPORT ONLY)
# =======================

def check_duplicates(df, report):
    """Report duplicates at different levels — does NOT remove any rows."""
    section("CHECK 3: DUPLICATE DETECTION (report only)")

    # 3a: Exact row duplicates
    subsection("Exact row duplicates (all columns)")
    n_exact = df.duplicated().sum()
    print(f"  Exact duplicate rows: {n_exact:,}")
    report['exact_dup_rows'] = int(n_exact)

    # 3b: Duplicates on (post_url, company_name) — the dedup key used in merge
    subsection("Duplicates on (post_url, company_name)")
    if 'post_url' in df.columns and 'company_name' in df.columns:
        dup_key = df.duplicated(subset=['post_url', 'company_name'], keep=False)
        n_dup_key = dup_key.sum()
        n_groups = df[dup_key].groupby(['post_url', 'company_name']).ngroups if n_dup_key > 0 else 0
        print(f"  Rows in duplicate groups: {n_dup_key:,}  ({n_groups:,} groups)")
        report['dup_post_url_company'] = int(n_dup_key)

        if n_dup_key > 0 and n_dup_key <= 200:
            print("\n  Sample duplicate groups:")
            sample_groups = df[dup_key].groupby(['post_url', 'company_name']).size().sort_values(ascending=False).head(5)
            for (url, comp), cnt in sample_groups.items():
                print(f"    {comp} | {str(url)[:60]}... | count={cnt}")
    else:
        print("  Skipped — post_url or company_name column missing.")

    # 3c: Duplicates on post_url alone (same post appearing for multiple companies = expected multi-board)
    subsection("Duplicates on post_url alone (expected: multi-board directors)")
    if 'post_url' in df.columns:
        dup_url = df.dropna(subset=['post_url']).duplicated(subset=['post_url'], keep=False)
        n_dup_url = dup_url.sum()
        n_unique_dup_urls = df.loc[df['post_url'].notna()][dup_url].groupby('post_url').ngroups if n_dup_url > 0 else 0
        print(f"  Rows sharing a post_url with another row: {n_dup_url:,}")
        print(f"  Unique post_urls that appear >1 time: {n_unique_dup_urls:,}")

        # Distinguish multi-board (expected) vs. same-company duplicates (unexpected)
        if n_dup_url > 0:
            dup_df = df.loc[df['post_url'].notna() & dup_url].copy()
            multi_company = dup_df.groupby('post_url')['company_name'].nunique()
            truly_multi_board = (multi_company > 1).sum()
            same_company_dup = (multi_company == 1).sum()
            print(f"\n  Multi-board (different company_name): {truly_multi_board:,} post_urls — EXPECTED")
            print(f"  Same-company duplicates:               {same_company_dup:,} post_urls — INVESTIGATE")
            report['multi_board_post_urls'] = int(truly_multi_board)
            report['same_company_dup_post_urls'] = int(same_company_dup)

            if same_company_dup > 0:
                print("\n  Sample same-company duplicate post_urls:")
                same_mask = multi_company[multi_company == 1].index
                sample_urls = list(same_mask[:5])
                for url in sample_urls:
                    rows = dup_df[dup_df['post_url'] == url]
                    comp = rows['company_name'].iloc[0]
                    print(f"    {comp} | {str(url)[:70]}... | {len(rows)} copies")

    # 3d: Profile-level post count distribution (detect re-scrape overlaps)
    subsection("Profile-level post counts (re-scrape overlap detection)")
    if 'profile_url' in df.columns:
        profile_counts = df.groupby('profile_url').size()
        print(f"  Profiles with posts: {len(profile_counts):,}")
        print(f"  Mean posts/profile:  {profile_counts.mean():.1f}")
        print(f"  Median:              {profile_counts.median():.0f}")
        print(f"  Max:                 {profile_counts.max():,}")
        print(f"  Profiles with >1000 posts: {(profile_counts > 1000).sum():,}")
        print(f"  Profiles with >5000 posts: {(profile_counts > 5000).sum():,}")

        # Flag profiles with suspiciously high counts (potential re-scrape overlap)
        if (profile_counts > 5000).sum() > 0:
            print("\n  Profiles with >5000 rows (may include multi-board expansion):")
            for url, cnt in profile_counts[profile_counts > 5000].sort_values(ascending=False).head(10).items():
                n_companies = df[df['profile_url'] == url]['company_name'].nunique()
                print(f"    {str(url)[:55]}...  rows={cnt:,}  companies={n_companies}")

        report['profiles_with_posts'] = int(len(profile_counts))


# =======================
# Check 4: Date Validation
# =======================

def check_dates(df, report):
    """Validate date ranges and temporal distribution."""
    section("CHECK 4: DATE VALIDATION")

    if 'post_date' not in df.columns:
        print("  Skipped — post_date column not found.")
        return

    # Parse dates
    dates = pd.to_datetime(df['post_date'], errors='coerce')
    valid_dates = dates.dropna()
    n_invalid = dates.isna().sum() - df['post_date'].isna().sum()  # subtract already-null

    print(f"  Total rows: {len(df):,}")
    print(f"  Valid dates: {len(valid_dates):,}")
    print(f"  Unparseable dates: {n_invalid:,}")
    report['unparseable_dates'] = int(n_invalid)

    if len(valid_dates) == 0:
        return

    print(f"\n  Date range: {valid_dates.min()} to {valid_dates.max()}")
    report['date_min'] = str(valid_dates.min())
    report['date_max'] = str(valid_dates.max())

    # Future dates (data quality flag)
    future = valid_dates > pd.Timestamp.now()
    print(f"  Future dates (> today): {future.sum():,}")
    report['future_dates'] = int(future.sum())

    # Very old dates (before LinkedIn existed, ~2003)
    old = valid_dates < pd.Timestamp('2003-01-01')
    print(f"  Pre-LinkedIn dates (< 2003): {old.sum():,}")

    # Year distribution
    subsection("Posts by year")
    years = valid_dates.dt.year.value_counts().sort_index()
    for yr, cnt in years.items():
        bar = '#' * min(int(cnt / years.max() * 40), 40)
        print(f"  {yr}  {cnt:>10,}  {bar}")


# =======================
# Check 5: Engagement Metrics
# =======================

ENGAGEMENT_COLS = [
    'reactions_total', 'likes', 'comments', 'reposts',
    'celebrates', 'supports', 'loves', 'insights', 'funnys',
]


def check_engagement(df, report):
    """Validate engagement metrics."""
    section("CHECK 5: ENGAGEMENT METRICS")

    available = [c for c in ENGAGEMENT_COLS if c in df.columns]
    if not available:
        print("  Skipped — no engagement columns found.")
        return

    subsection("Descriptive statistics")
    print(df[available].describe().round(1).to_string())

    # Negative values
    subsection("Negative values (should be zero)")
    for col in available:
        numeric = pd.to_numeric(df[col], errors='coerce')
        n_neg = (numeric < 0).sum()
        if n_neg > 0:
            print(f"  {col}: {n_neg:,} negative values — UNEXPECTED")
        else:
            print(f"  {col}: OK")

    # Consistency: reactions_total vs sum of reaction types
    if 'reactions_total' in df.columns:
        subsection("Reactions consistency (reactions_total vs. sum of types)")
        reaction_types = [c for c in ['likes', 'celebrates', 'supports', 'loves', 'insights', 'funnys'] if c in df.columns]
        if reaction_types:
            computed_sum = df[reaction_types].apply(pd.to_numeric, errors='coerce').sum(axis=1)
            reported = pd.to_numeric(df['reactions_total'], errors='coerce')
            diff = (reported - computed_sum).abs()
            n_mismatch = (diff > 0).sum()
            n_big_mismatch = (diff > 10).sum()
            print(f"  Any mismatch (|diff| > 0): {n_mismatch:,} rows")
            print(f"  Large mismatch (|diff| > 10): {n_big_mismatch:,} rows")
            if n_mismatch > 0:
                print(f"  Mean absolute difference: {diff.mean():.1f}")
                print(f"  Note: Small mismatches are normal (LinkedIn updates asynchronously)")
            report['engagement_mismatch'] = int(n_mismatch)

    # Engagement by post_type
    if 'post_type' in df.columns and 'reactions_total' in df.columns:
        subsection("Median engagement by post_type")
        numeric_reactions = pd.to_numeric(df['reactions_total'], errors='coerce')
        by_type = df.assign(_reactions=numeric_reactions).groupby('post_type')['_reactions'].agg(['median', 'mean', 'count'])
        print(by_type.round(1).to_string())

    # Outliers
    subsection("Engagement outliers")
    if 'reactions_total' in df.columns:
        numeric_reactions = pd.to_numeric(df['reactions_total'], errors='coerce')
        p99 = numeric_reactions.quantile(0.99)
        p999 = numeric_reactions.quantile(0.999)
        top = numeric_reactions.max()
        print(f"  99th percentile:  {p99:,.0f}")
        print(f"  99.9th percentile: {p999:,.0f}")
        print(f"  Maximum:           {top:,.0f}")


# =======================
# Check 6: Profile Coverage
# =======================

def check_profile_coverage(df, urls_path, report):
    """Compare scraped profiles against the full verified URL list."""
    section("CHECK 6: PROFILE COVERAGE")

    if urls_path is None or not Path(urls_path).exists():
        print("  Skipped — all_linkedin_urls.csv not provided or not found.")
        return

    urls_df = pd.read_csv(urls_path)
    verified = urls_df[urls_df['verified'] == 'True'] if 'verified' in urls_df.columns else urls_df[urls_df['verified'] == True]
    # Handle string vs bool
    if len(verified) == 0:
        verified = urls_df[urls_df['verified'].astype(str).str.lower() == 'true']
    n_verified = len(verified)
    n_unique_urls = verified['linkedin_url'].nunique() if 'linkedin_url' in verified.columns else 0

    print(f"  Verified URL records: {n_verified:,}")
    print(f"  Unique verified URLs: {n_unique_urls:,}")

    # Profiles in posts
    if 'profile_url' in df.columns:
        scraped_profiles = df['profile_url'].dropna().unique()
        n_scraped = len(scraped_profiles)
        print(f"  Profiles in posts_combined: {n_scraped:,}")

        # Match rate
        if 'linkedin_url' in verified.columns:
            # Normalize for comparison
            def norm_url(u):
                if pd.isna(u):
                    return None
                return str(u).strip().rstrip('/').lower()

            verified_set = set(verified['linkedin_url'].apply(norm_url).dropna())
            scraped_set = set(pd.Series(scraped_profiles).apply(norm_url).dropna())

            in_both = verified_set & scraped_set
            in_verified_only = verified_set - scraped_set
            in_scraped_only = scraped_set - verified_set

            print(f"\n  Profiles in both:         {len(in_both):,}")
            print(f"  Verified but not scraped: {len(in_verified_only):,}")
            print(f"  Scraped but not verified: {len(in_scraped_only):,}")

            report['profiles_scraped'] = n_scraped
            report['profiles_verified'] = n_unique_urls
            report['profiles_in_both'] = len(in_both)
            report['profiles_not_scraped'] = len(in_verified_only)

            # The ~16,161 zero-post profiles
            subsection("Zero-post profiles")
            print(f"  Verified URLs with no posts in dataset: {len(in_verified_only):,}")
            print(f"  (Includes inactive accounts + failed Apify batches)")


# =======================
# Check 7: Post Type Distribution
# =======================

def check_post_types(df, report):
    """Validate post type distribution."""
    section("CHECK 7: POST TYPE DISTRIBUTION")

    if 'post_type' not in df.columns:
        print("  Skipped — post_type column not found.")
        return

    dist = df['post_type'].value_counts(dropna=False)
    print("\n  Post type distribution:")
    for pt, cnt in dist.items():
        label = str(pt) if pd.notna(pt) else "<null>"
        print(f"    {label:<20s}  {cnt:>10,}  ({cnt / len(df) * 100:.1f}%)")
        report[f'post_type_{label}'] = int(cnt)


# =======================
# Check 8: gvkey / ticker consistency
# =======================

def check_identifiers(df, directors_path, report):
    """Check consistency of gvkey/ticker with the directors reference file."""
    section("CHECK 8: IDENTIFIER CONSISTENCY (gvkey/ticker)")

    if 'gvkey' not in df.columns or 'ticker' not in df.columns:
        print("  Skipped — gvkey or ticker not in posts.")
        return

    # Internal consistency: same gvkey should map to same ticker
    subsection("Internal gvkey-ticker consistency")
    gvkey_tickers = df.dropna(subset=['gvkey']).groupby('gvkey')['ticker'].nunique()
    multi_ticker = gvkey_tickers[gvkey_tickers > 1]
    print(f"  gvkeys with >1 ticker: {len(multi_ticker):,}")
    if len(multi_ticker) > 0:
        print("  (Can be legitimate — ticker changes over time)")
        for gv, n in multi_ticker.head(5).items():
            tickers = df[df['gvkey'] == gv]['ticker'].unique()
            print(f"    gvkey={gv}: {list(tickers)}")

    # Cross-reference with directors.csv
    if directors_path is not None and Path(directors_path).exists():
        subsection("Cross-reference with directors.csv")
        dir_df = pd.read_csv(directors_path)
        dir_gvkeys = set(dir_df['gvkey'].dropna().unique())
        post_gvkeys = set(df['gvkey'].dropna().unique())

        print(f"  gvkeys in directors.csv:  {len(dir_gvkeys):,}")
        print(f"  gvkeys in posts:          {len(post_gvkeys):,}")
        print(f"  Overlap:                  {len(dir_gvkeys & post_gvkeys):,}")
        print(f"  In posts but not directors: {len(post_gvkeys - dir_gvkeys):,}")
        report['gvkey_not_in_directors'] = int(len(post_gvkeys - dir_gvkeys))


# =======================
# Check 9: Text Quality & Edge Cases
# =======================

def check_text_quality(df, report):
    """Examine edge cases in post text."""
    section("CHECK 9: TEXT QUALITY & EDGE CASES")

    if 'post_text' not in df.columns:
        print("  Skipped — post_text not found.")
        return

    text = df['post_text'].dropna()
    lengths = text.str.len()

    subsection("Post text length distribution")
    print(f"  Posts with text: {len(text):,}")
    print(f"  Min length:   {lengths.min():,}")
    print(f"  Median:        {lengths.median():,.0f}")
    print(f"  Mean:          {lengths.mean():,.0f}")
    print(f"  Max:           {lengths.max():,}")
    print(f"  Very short (< 10 chars): {(lengths < 10).sum():,}")
    print(f"  Very long (> 5000 chars): {(lengths > 5000).sum():,}")

    # Word count
    words = text.str.split().str.len()
    subsection("Word count distribution")
    print(f"  Median words: {words.median():.0f}")
    print(f"  Mean words:   {words.mean():.0f}")
    print(f"  Max words:    {words.max():,}")

    # Empty-looking text (just whitespace or URLs)
    subsection("Suspicious text content")
    just_url = text.str.match(r'^https?://\S+$', na=False)
    just_whitespace = text.str.strip().eq('')
    print(f"  Text is only a URL: {just_url.sum():,}")
    print(f"  Text is only whitespace: {just_whitespace.sum():,}")


# =======================
# Check 10: AI Keyword Quick Scan
# =======================

AI_KEYWORDS = [
    'artificial intelligence', ' ai ', 'machine learning', ' ml ', 'deep learning',
    'neural network', 'llm', 'large language model', 'generative ai', 'gen ai',
    'chatgpt', 'gpt', 'claude', 'gemini', 'copilot', 'automation', 'algorithm',
    'data science', 'predictive analytics', 'nlp', 'natural language processing',
    'computer vision', 'robotics', 'autonomous',
]


def check_ai_keywords(df, report):
    """Quick AI keyword frequency check using word boundaries."""
    section("CHECK 10: AI KEYWORD QUICK SCAN")

    if 'post_text' not in df.columns:
        print("  Skipped.")
        return

    text = df['post_text'].fillna('')
    text_lower = text.str.lower()

    # Word-boundary matching (avoids "innovation" matching "automation" etc.)
    keyword_counts = {}
    for kw in AI_KEYWORDS:
        kw_stripped = kw.strip()
        if kw.startswith(' ') and kw.endswith(' '):
            # Space-padded keywords: use simple containment on padded text
            matches = (' ' + text_lower + ' ').str.contains(kw, regex=False).sum()
        else:
            pattern = r'\b' + re.escape(kw_stripped) + r'\b'
            matches = text_lower.str.contains(pattern, regex=True, na=False).sum()
        keyword_counts[kw_stripped] = int(matches)

    # Any AI keyword
    def has_ai(t):
        if pd.isna(t) or t == '':
            return False
        t = str(t).lower()
        for kw in AI_KEYWORDS:
            kw_stripped = kw.strip()
            if kw.startswith(' ') and kw.endswith(' '):
                if kw in ' ' + t + ' ':
                    return True
            else:
                if re.search(r'\b' + re.escape(kw_stripped) + r'\b', t):
                    return True
        return False

    ai_mask = df['post_text'].apply(has_ai)
    n_ai = ai_mask.sum()
    n_profiles_ai = df.loc[ai_mask, 'profile_url'].nunique() if 'profile_url' in df.columns else 0

    print(f"\n  AI-related posts: {n_ai:,} ({n_ai / len(df) * 100:.2f}%)")
    print(f"  Profiles with ≥1 AI post: {n_profiles_ai:,}")

    subsection("Keyword hit counts")
    for kw, cnt in sorted(keyword_counts.items(), key=lambda x: -x[1]):
        if cnt > 0:
            print(f"    {kw:<30s}  {cnt:>8,}")

    report['ai_posts'] = int(n_ai)
    report['ai_profiles'] = int(n_profiles_ai)


# =======================
# Main
# =======================

def main():
    parser = argparse.ArgumentParser(description='Data quality checks for posts_combined.csv')
    parser.add_argument('--posts', type=str, help='Path to posts_combined.csv')
    parser.add_argument('--urls', type=str, help='Path to all_linkedin_urls.csv')
    parser.add_argument('--directors', type=str, help='Path to directors.csv')
    parser.add_argument('--output-dir', type=str, help='Directory for report output')
    args = parser.parse_args()

    paths = default_paths()
    posts_path = Path(args.posts) if args.posts else paths['posts']
    urls_path = Path(args.urls) if args.urls else paths['urls']
    directors_path = Path(args.directors) if args.directors else paths['directors']
    output_dir = Path(args.output_dir) if args.output_dir else paths['output_dir']

    print("=" * 80)
    print("  DATA QUALITY CHECKS — posts_combined.csv")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    if not posts_path.exists():
        print(f"\nERROR: Posts file not found: {posts_path}")
        print("Use --posts /path/to/posts_combined.csv")
        sys.exit(1)

    df = load_posts(posts_path)
    report = {}

    # Run all checks
    check_schema(df, report)
    check_nulls(df, report)
    check_duplicates(df, report)
    check_dates(df, report)
    check_engagement(df, report)
    check_profile_coverage(df, urls_path, report)
    check_post_types(df, report)
    check_identifiers(df, directors_path, report)
    check_text_quality(df, report)
    check_ai_keywords(df, report)

    # Save JSON report
    section("SAVING REPORT")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = output_dir / f'quality_report_{timestamp}.json'
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    print(f"  JSON report saved to: {report_path}")

    # Summary
    section("SUMMARY")
    print(f"  Total rows:              {report.get('total_rows', 'N/A'):,}")
    print(f"  All-null placeholder rows: {report.get('all_null_rows', 'N/A'):,}")
    print(f"  Null post_text:          {report.get('null_post_text', 'N/A'):,}")
    print(f"  Exact duplicate rows:    {report.get('exact_dup_rows', 'N/A'):,}")
    print(f"  Same-company dup URLs:   {report.get('same_company_dup_post_urls', 'N/A')}")
    print(f"  Multi-board dup URLs:    {report.get('multi_board_post_urls', 'N/A')}")
    print(f"  AI-related posts:        {report.get('ai_posts', 'N/A'):,}")
    print(f"\n  Done.")


if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""
Scrape LinkedIn Posts via Apify
================================

Generic LinkedIn post scraper using Apify's linkedin-batch-profile-posts-scraper.
Accepts any CSV containing a LinkedIn URL column (typically the output of
find_urls.py), scrapes up to --max-posts posts per profile, and saves
structured output with checkpoint/resume for long runs on Sherlock / SLURM.

This script replaces the S&P 500-specific scrape_verified_directors_sp500.py
with a generic pipeline:
    CSV input → verified filtering → URL deduplication → Apify batch
    scraping → checkpoint/resume → structured output (posts CSV + raw JSON)

Filtering:
    If the input CSV has a 'verified' column (from find_urls.py), only rows
    with verified=True are scraped by default. Use --no-filter to override.

Auto-detected URL column (first match wins):
    linkedin_url, profile_url, url, LinkedIn URL

Metadata handling:
    All non-URL columns from the input are preserved and joined to each post
    via the LinkedIn URL. If a person appears on multiple rows (e.g. director
    on multiple boards), each post is duplicated once per row.

Output files (saved to --output directory):
    posts_YYYYMMDD_HHMMSS.csv            - structured posts with metadata
    profiles_YYYYMMDD_HHMMSS.csv         - profile-level summary
    no_posts_profiles_YYYYMMDD_HHMMSS.csv - profiles that returned zero posts
    posts_raw_YYYYMMDD_HHMMSS.json       - raw Apify response
    .scrape_checkpoint.json              - resumable state (auto-cleaned)

Usage:
    python3 scrape_posts.py --input urls.csv --stats               # Preview
    python3 scrape_posts.py --input urls.csv --prototype 5          # Test 5 profiles
    python3 scrape_posts.py --input urls.csv --run                  # Full run
    python3 scrape_posts.py --input urls.csv --run --yes            # SLURM (no prompt)
    python3 scrape_posts.py --input urls.csv --resume               # Resume
    python3 scrape_posts.py --input urls.csv --run --no-filter      # Include unverified
    python3 scrape_posts.py --input urls.csv --run --max-posts 200  # Override post limit

Prerequisites:
    pip install apify-client pandas python-dotenv
    APIFY_API_TOKEN in .env file (searched upward from CWD, or via --env)

Apify actor:
    apimaestro/linkedin-batch-profile-posts-scraper
    Cost: ~$5 per 1,000 profiles (check Apify pricing for current rates)
"""

import os
import sys
import json
import time
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------

from dotenv import load_dotenv

try:
    from apify_client import ApifyClient
except ImportError:
    print("=" * 60)
    print("ERROR: apify_client not installed!")
    print("=" * 60)
    print("\n  pip install apify-client")
    sys.exit(1)


# ===========================================================================
# Configuration defaults (overridable via CLI)
# ===========================================================================

APIFY_ACTOR = "apimaestro/linkedin-batch-profile-posts-scraper"
DEFAULT_MAX_POSTS = 1000
DEFAULT_BATCH_SIZE = 100   # profiles per Apify run
DELAY_BETWEEN_BATCHES = 10  # seconds

URL_COLUMN_CANDIDATES = ["linkedin_url", "profile_url", "url", "LinkedIn URL"]


# ===========================================================================
# Environment / credentials
# ===========================================================================

def _find_env_file(start_dir, explicit=None):
    """Search upward from *start_dir* for .env, or use an explicit path."""
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None

    d = Path(start_dir).resolve()
    for _ in range(10):
        candidate = d / ".env"
        if candidate.exists():
            return candidate
        parent = d.parent
        if parent == d:
            break
        d = parent
    return None


def _load_credentials(env_path=None):
    """Load APIFY_API_TOKEN and return an initialised client."""
    env_file = _find_env_file(Path.cwd(), env_path)
    if env_file:
        load_dotenv(env_file)
        print(f"  .env:   {env_file}")
    else:
        print("  .env:   (not found — relying on environment variables)")

    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("\n✗ APIFY_API_TOKEN not set.")
        print("  Add it to your .env or export it:")
        print("    export APIFY_API_TOKEN=your_token_here")
        print("  Get a token: https://console.apify.com/account/integrations")
        sys.exit(1)

    return ApifyClient(token)


# ===========================================================================
# Input loading & filtering
# ===========================================================================

def _detect_column(df, candidates, required=False, label="column"):
    """Return the first column name in *candidates* that exists in *df*."""
    for col in candidates:
        if col in df.columns:
            return col
    # Fallback: fuzzy match for linkedin+url
    for col in df.columns:
        if "linkedin" in col.lower() and "url" in col.lower():
            return col
    if required:
        print(f"\n✗ Could not find {label} column.")
        print(f"  Looked for: {candidates}")
        print(f"  Available:  {list(df.columns)}")
        sys.exit(1)
    return None


def load_input(filepath, filter_verified=True):
    """
    Load input CSV and apply verified filtering.

    Returns:
        (df, url_col)  where df has only scrapable rows.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        print(f"\n✗ Input file not found: {filepath}")
        sys.exit(1)

    print(f"  Input:  {filepath}")
    df = pd.read_csv(filepath)
    print(f"  Rows:   {len(df):,}   Columns: {len(df.columns)}")

    url_col = _detect_column(df, URL_COLUMN_CANDIDATES,
                             required=True, label="LinkedIn URL")
    print(f"  URL column: {url_col}")

    # --- Verified filtering ---
    if filter_verified and "verified" in df.columns:
        before = len(df)
        # Coerce to bool safely (handles string "True"/"False" from CSV)
        df["verified"] = df["verified"].astype(str).str.strip().str.lower() == "true"
        df = df[df["verified"]].copy()
        dropped = before - len(df)
        print(f"  Filtered to verified=True: {len(df):,} (dropped {dropped:,} unverified)")
    elif filter_verified:
        print(f"  No 'verified' column — scraping all rows with URLs")

    # Drop rows without a URL
    before = len(df)
    df = df[df[url_col].notna()].copy()
    if len(df) < before:
        print(f"  Dropped {before - len(df):,} rows with missing URL")

    return df, url_col


def _get_unique_urls(df, url_col):
    """Deduplicate URLs preserving order."""
    raw = df[url_col].tolist()
    unique = list(dict.fromkeys(raw))
    if len(unique) < len(raw):
        print(f"  Deduplicated: {len(raw):,} → {len(unique):,} unique URLs")
    return unique


def _build_url_metadata(df, url_col):
    """Build lookup:  url → list[dict] of metadata rows.

    Carries through ALL columns from the input except the URL itself and
    search/verification machinery.  One URL can map to multiple rows
    (e.g. director on multiple boards).
    """
    # Columns to skip (search machinery, not useful downstream)
    skip = {url_col, "search_query", "linkedin_title", "search_status",
            "verified", "match_type"}
    keep = [c for c in df.columns if c not in skip]

    lookup = {}
    for _, row in df.iterrows():
        url = row[url_col]
        meta = {c: row[c] for c in keep}
        lookup.setdefault(url, []).append(meta)
    return lookup


# ===========================================================================
# Statistics / preview
# ===========================================================================

def print_stats(df, url_col, max_posts, batch_size):
    """Print scraping preview without API calls."""
    unique = df[url_col].nunique()
    n_batches = (unique + batch_size - 1) // batch_size

    print(f"\n{'=' * 70}")
    print("SCRAPING PREVIEW")
    print("=" * 70)
    print(f"\n  Input rows (after filter): {len(df):,}")
    print(f"  Unique LinkedIn URLs:      {unique:,}")
    if unique < len(df):
        print(f"    (some people appear on multiple boards — posts duplicated per board)")
    print(f"  Max posts / profile:       {max_posts}")
    print(f"  Batch size:                {batch_size} profiles")
    print(f"  Batches:                   {n_batches}")

    # Metadata columns detected
    meta_cols = [c for c in df.columns
                 if c not in {url_col, "search_query", "linkedin_title",
                              "search_status", "verified", "match_type"}]
    if meta_cols:
        print(f"  Metadata columns:          {', '.join(meta_cols[:8])}")
        if len(meta_cols) > 8:
            print(f"                             ... and {len(meta_cols) - 8} more")

    # Match type distribution (if present and not filtered out)
    if "match_type" in df.columns:
        print(f"\n  Match type distribution:")
        for mt, cnt in df["match_type"].value_counts().items():
            print(f"    {str(mt):>12}: {cnt:>6,}")

    # Cost estimate
    cost = unique / 1000 * 5
    print(f"\n  Est. Apify cost:  ${cost:,.2f}  (at ~$5/1k profiles)")
    print(f"  Est. time:        {n_batches * 3}–{n_batches * 10} min")


# ===========================================================================
# Apify integration
# ===========================================================================

def _call_apify(client, profile_urls, max_posts):
    """Run the Apify actor for one batch. Returns list of items or None."""
    run_input = {
        "usernames": profile_urls,
        "maxPosts": max_posts,
    }
    try:
        run = client.actor(APIFY_ACTOR).call(run_input=run_input)
        run_id = run.get("id")
        status = run.get("status")
        print(f"      Run {run_id}: {status}")

        if status != "SUCCEEDED":
            print(f"      ✗ Run failed ({status})")
            return None

        dataset_id = run.get("defaultDatasetId")
        items = list(client.dataset(dataset_id).iterate_items())
        print(f"      ✓ {len(items):,} items retrieved")
        return items

    except Exception as e:
        print(f"      ✗ Apify error: {e}")
        return None


def _scrape_batches(client, urls, max_posts, batch_size,
                    checkpoint_cb=None):
    """Scrape *urls* in mini-batches with checkpointing."""
    all_results = []
    total = len(urls)
    n_batches = (total + batch_size - 1) // batch_size
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 3

    print(f"\n{'=' * 70}")
    print(f"SCRAPING {total:,} PROFILES IN {n_batches} BATCHES")
    print("=" * 70)

    for i in range(0, total, batch_size):
        batch_num = i // batch_size + 1
        batch_urls = urls[i : i + batch_size]
        print(f"\n  Batch {batch_num}/{n_batches}  ({len(batch_urls)} profiles)")

        items = _call_apify(client, batch_urls, max_posts)
        if items:
            all_results.extend(items)
            consecutive_failures = 0
            print(f"      Running total: {len(all_results):,} items")
        else:
            consecutive_failures += 1
            print(f"      ⚠  Batch failed ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES} consecutive)")
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n  ✗ {MAX_CONSECUTIVE_FAILURES} consecutive failures — aborting.")
                print(f"    Fix the issue and use --resume to continue.")
                break

        if checkpoint_cb:
            checkpoint_cb(all_results, i + len(batch_urls))

        if i + batch_size < total:
            print(f"      Waiting {DELAY_BETWEEN_BATCHES}s …")
            time.sleep(DELAY_BETWEEN_BATCHES)

    return all_results


# ===========================================================================
# Result parsing
# ===========================================================================
#
# The Apify actor 'apimaestro/linkedin-batch-profile-posts-scraper' returns
# a FLAT list where each item IS one post.  Key fields (from sample_raw_posts.json):
#
#   profile_input       — the URL we submitted (join key!)
#   text                — post content
#   url                 — post permalink
#   post_type           — "regular", "repost", etc.
#   posted_at.date      — "2024-03-15 10:22:33"
#   posted_at.timestamp — unix ms
#   author.first_name / last_name / headline / username / profile_url
#   stats.total_reactions / like / comments / reposts / celebrate / ...
#   media.type / media.url
#   article.url / article.title  (if shared article)
#   reshared_post.*  — original post for "quote" type posts
#     reshared_post.text / .author.first_name / .author.last_name / .url
#
# ===========================================================================

def _normalise_url(url):
    """Strip query params and trailing slash for consistent matching."""
    if not url:
        return ""
    return url.split("?")[0].rstrip("/")


def _parse_results(raw_data, url_metadata):
    """Parse flat Apify items into posts and profiles.

    Returns (posts_list, profiles_list).
    """
    posts_list = []
    profiles_seen = {}  # normalised_url → profile info

    # Pre-normalise the metadata lookup keys
    norm_meta = {}
    for url, metas in url_metadata.items():
        norm_meta[_normalise_url(url)] = metas

    for item in raw_data:
        # --- Identify which submitted profile this post belongs to ---
        submitted_url = _normalise_url(item.get("profile_input", ""))

        # --- Extract author info (nested under 'author') ---
        author = item.get("author") or {}
        author_name = f"{author.get('first_name', '')} {author.get('last_name', '')}".strip()
        author_headline = author.get("headline", "")
        author_username = author.get("username", "")

        # Track unique profiles
        if submitted_url and submitted_url not in profiles_seen:
            profiles_seen[submitted_url] = {
                "profile_url": submitted_url,
                "author_name": author_name,
                "headline": author_headline,
                "username": author_username,
                "profile_picture": author.get("profile_picture", ""),
            }

        # --- Extract post fields (all nested dicts) ---
        posted_at = item.get("posted_at") or {}
        stats = item.get("stats") or {}
        media = item.get("media") or {}
        article = item.get("article") or {}
        reshared = item.get("reshared_post") or {}
        reshared_author = reshared.get("author") or {}

        post_core = {
            "profile_url": submitted_url,
            "post_text": item.get("text", ""),
            "post_url": item.get("url", ""),
            "post_type": item.get("post_type", ""),
            "post_date": posted_at.get("date", ""),
            "post_timestamp": posted_at.get("timestamp", ""),
            "author_name": author_name,
            "author_headline": author_headline,
            "reactions_total": stats.get("total_reactions", 0),
            "likes": stats.get("like", 0),
            "comments": stats.get("comments", 0),
            "reposts": stats.get("reposts", 0),
            "celebrates": stats.get("celebrate", 0),
            "supports": stats.get("support", 0),
            "loves": stats.get("love", 0),
            "insights": stats.get("insight", 0),
            "funnys": stats.get("funny", 0),
            "media_type": media.get("type", ""),
            "article_url": article.get("url", ""),
            "article_title": article.get("title", ""),
            # Reshared post (for post_type="quote" — director endorsed this)
            "reshared_text": reshared.get("text", ""),
            "reshared_url": reshared.get("url", ""),
            "reshared_author": f"{reshared_author.get('first_name', '')} {reshared_author.get('last_name', '')}".strip(),
        }

        # --- Join metadata (one row per company/board membership) ---
        meta_rows = norm_meta.get(submitted_url, [{}])
        for meta in meta_rows:
            row = {**meta, **post_core}
            posts_list.append(row)

    profiles_list = list(profiles_seen.values())
    return posts_list, profiles_list


def _save_results(raw_data, output_dir, df, url_col, submitted_urls=None):
    """Parse Apify results and save as JSON + CSV.

    Returns dict with paths and counts.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Build metadata lookup
    url_metadata = _build_url_metadata(df, url_col)

    # 1. Raw JSON (always save first — safety net)
    raw_path = output_dir / f"posts_raw_{ts}.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2, ensure_ascii=False)
    print(f"\n  ✓ Raw JSON:     {raw_path}")

    # Debug: structure check
    if raw_data:
        first = raw_data[0]
        print(f"    First item keys: {sorted(first.keys())[:8]}")
        if "profile_input" in first:
            print(f"    profile_input ✓ (join key present)")
        else:
            print(f"    ⚠ No 'profile_input' — metadata join may fail")

    # 2. Parse
    posts_list, profiles_list = _parse_results(raw_data, url_metadata)

    # 3. Posts CSV
    posts_path = None
    if posts_list:
        posts_df = pd.DataFrame(posts_list)
        posts_path = output_dir / f"posts_{ts}.csv"
        posts_df.to_csv(posts_path, index=False, encoding="utf-8")
        print(f"  ✓ Posts CSV:    {posts_path}  ({len(posts_df):,} rows)")
    else:
        print("  ⚠ No posts parsed — inspect raw JSON for unexpected structure")

    # 4. Profiles CSV
    profiles_path = None
    if profiles_list:
        profiles_df = pd.DataFrame(profiles_list)
        profiles_path = output_dir / f"profiles_{ts}.csv"
        profiles_df.to_csv(profiles_path, index=False, encoding="utf-8")
        print(f"  ✓ Profiles CSV: {profiles_path}  ({len(profiles_df):,} rows)")

    # 5. No-posts profiles CSV
    #    Profiles that were submitted but returned zero posts.
    #    Useful for distinguishing "never posts" from "scrape failed".
    no_posts_path = None
    if submitted_urls:
        urls_with_posts = {_normalise_url(p["profile_url"]) for p in profiles_list}
        no_post_urls = [u for u in submitted_urls
                        if _normalise_url(u) not in urls_with_posts]
        if no_post_urls:
            # Look up metadata for these profiles
            norm_meta = {_normalise_url(u): m
                         for u, m in url_metadata.items()}
            no_posts_rows = []
            for url in no_post_urls:
                metas = norm_meta.get(_normalise_url(url), [{}])
                for meta in metas:
                    no_posts_rows.append({**meta, "profile_url": url})
            no_posts_df = pd.DataFrame(no_posts_rows)
            no_posts_path = output_dir / f"no_posts_profiles_{ts}.csv"
            no_posts_df.to_csv(no_posts_path, index=False, encoding="utf-8")
            print(f"  ✓ No-posts CSV: {no_posts_path}  ({len(no_post_urls):,} profiles)")

    return {
        "raw_path": str(raw_path),
        "posts_path": str(posts_path) if posts_path else None,
        "profiles_path": str(profiles_path) if profiles_path else None,
        "no_posts_path": str(no_posts_path) if no_posts_path else None,
        "posts_count": len(posts_list),
        "profiles_count": len(profiles_list),
        "no_posts_count": len(no_post_urls) if submitted_urls else 0,
        "timestamp": ts,
    }


# ===========================================================================
# Checkpoint management
# ===========================================================================

def _checkpoint_path(output_dir):
    return Path(output_dir) / ".scrape_checkpoint.json"


def _save_checkpoint(results, profiles_done, output_dir):
    """Persist current progress to disk."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    temp_file = output_dir / "temp_results.json"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False)

    cp_file = _checkpoint_path(output_dir)
    with open(cp_file, "w") as f:
        json.dump({
            "profiles_processed": profiles_done,
            "results_file": str(temp_file),
            "timestamp": datetime.now().isoformat(),
        }, f, indent=2)
    print(f"      💾 Checkpoint: {profiles_done} profiles done")


def _load_checkpoint(output_dir):
    """Load previous checkpoint if it exists."""
    cp_file = _checkpoint_path(output_dir)
    if not cp_file.exists():
        return None

    with open(cp_file) as f:
        cp = json.load(f)

    temp_file = cp.get("results_file")
    if temp_file and Path(temp_file).exists():
        with open(temp_file) as f:
            cp["previous_results"] = json.load(f)
    else:
        cp["previous_results"] = []

    return cp


def _clear_checkpoint(output_dir):
    """Remove checkpoint files after successful completion."""
    for name in [".scrape_checkpoint.json", "temp_results.json"]:
        p = Path(output_dir) / name
        if p.exists():
            p.unlink()


# ===========================================================================
# Main scraping orchestration
# ===========================================================================

def run_scraping(client, df, url_col, output_dir, max_posts, batch_size,
                 resume=True, prototype_limit=None):
    """Main entry point for scraping."""
    output_dir = Path(output_dir)

    if prototype_limit:
        # In prototype mode, take N unique URLs (not N rows)
        unique_urls = _get_unique_urls(df, url_col)[:prototype_limit]
        df = df[df[url_col].isin(unique_urls)].copy()
        print(f"  PROTOTYPE MODE: {len(unique_urls)} profiles, {len(df)} rows")
    else:
        unique_urls = _get_unique_urls(df, url_col)

    urls = unique_urls

    # Resume handling
    start_from = 0
    previous_results = []
    if resume:
        cp = _load_checkpoint(output_dir)
        if cp:
            start_from = cp.get("profiles_processed", 0)
            previous_results = cp.get("previous_results", [])
            print(f"\n  ✓ Resuming from checkpoint: {start_from} profiles already done")

    if start_from >= len(urls):
        print("\n  ✓ All profiles already scraped!")
        return

    # Scrape
    remaining = urls[start_from:]

    def checkpoint_cb(results, done):
        _save_checkpoint(results, start_from + done, output_dir)

    new_results = _scrape_batches(client, remaining, max_posts, batch_size,
                                  checkpoint_cb)

    all_results = previous_results + new_results

    # Save final output
    print(f"\n{'=' * 70}")
    print("SAVING FINAL RESULTS")
    print("=" * 70)

    info = _save_results(all_results, output_dir, df, url_col,
                         submitted_urls=urls)
    _clear_checkpoint(output_dir)

    # Summary
    print(f"\n{'=' * 70}")
    print("✅ SCRAPING COMPLETE")
    print("=" * 70)
    print(f"  Profiles scraped: {info['profiles_count']:,}")
    print(f"  Posts collected:  {info['posts_count']:,}")
    if info.get("no_posts_count"):
        print(f"  No-post profiles: {info['no_posts_count']:,}")
    if info["posts_path"]:
        print(f"  Posts CSV:        {info['posts_path']}")
    if info["profiles_path"]:
        print(f"  Profiles CSV:     {info['profiles_path']}")
    if info.get("no_posts_path"):
        print(f"  No-posts CSV:     {info['no_posts_path']}")
    print(f"  Raw JSON:         {info['raw_path']}")


# ===========================================================================
# SLURM job helper
# ===========================================================================

def generate_slurm_script(args, output_dir):
    """Write a SLURM job script for Sherlock."""
    input_path = Path(args.input).resolve()
    script_path = Path(__file__).resolve()
    output_dir = Path(output_dir)

    slurm = f"""#!/bin/bash
#SBATCH --job-name=linkedin-scrape-posts
#SBATCH --partition=normal
#SBATCH --time=48:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --output={output_dir}/slurm_%j.log

# LinkedIn Post Scraper — SLURM job
# Generated: {datetime.now().isoformat()}

module load python/3.12

cd $HOME/ai-enthusiasm-research

# Activate virtual environment
source venv/bin/activate

# Run scraper (--yes skips confirmation, auto-resumes on restart)
python3 {script_path} \\
    --input {input_path} \\
    --output {output_dir} \\
    --max-posts {args.max_posts} \\
    --batch-size {args.batch_size} \\
    --run --yes

echo "Done: $(date)"
"""
    output_dir.mkdir(parents=True, exist_ok=True)
    slurm_path = output_dir / "scrape_job.sh"
    with open(slurm_path, "w") as f:
        f.write(slurm)
    print(f"\n  ✓ SLURM script: {slurm_path}")
    print(f"    Submit with:  sbatch {slurm_path}")


# ===========================================================================
# CLI
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape LinkedIn posts via Apify from any CSV with profile URLs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview what will be scraped
  python3 scrape_posts.py --input all_linkedin_urls.csv --stats

  # Test with 5 profiles
  python3 scrape_posts.py --input all_linkedin_urls.csv --prototype 5

  # Full run (non-interactive for SLURM)
  python3 scrape_posts.py --input all_linkedin_urls.csv --run --yes

  # Resume after interruption
  python3 scrape_posts.py --input all_linkedin_urls.csv --resume

  # Include unverified URLs
  python3 scrape_posts.py --input all_linkedin_urls.csv --run --no-filter

  # Generate SLURM job script
  python3 scrape_posts.py --input all_linkedin_urls.csv --slurm
        """,
    )

    parser.add_argument("--input", "-i", required=True,
                        help="Path to CSV with LinkedIn URLs")
    parser.add_argument("--output", "-o", default=None,
                        help="Output directory (default: <input_dir>/scraped_posts/)")
    parser.add_argument("--env", default=None,
                        help="Path to .env file (default: search upward from CWD)")

    # Actions (mutually exclusive)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--stats", action="store_true",
                        help="Show scraping preview — no API calls")
    action.add_argument("--run", action="store_true",
                        help="Start scraping")
    action.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint")
    action.add_argument("--prototype", type=int, metavar="N",
                        help="Test with N profiles")
    action.add_argument("--slurm", action="store_true",
                        help="Generate a SLURM job script for Sherlock")

    # Options
    parser.add_argument("--max-posts", type=int, default=DEFAULT_MAX_POSTS,
                        help=f"Max posts per profile (default: {DEFAULT_MAX_POSTS})")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Profiles per Apify call (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--no-filter", action="store_true",
                        help="Scrape ALL rows with URLs (don't filter on verified)")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompts (for SLURM / automation)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore existing checkpoint, start fresh")

    args = parser.parse_args()

    # --- Resolve paths ---
    input_path = Path(args.input).resolve()
    if args.output:
        output_dir = Path(args.output).resolve()
    else:
        output_dir = input_path.parent / "scraped_posts"

    print("=" * 70)
    print("LinkedIn Post Scraper")
    print("=" * 70)

    # --- Load input ---
    filter_verified = not args.no_filter
    df, url_col = load_input(str(input_path), filter_verified=filter_verified)

    # --- SLURM script generation ---
    if args.slurm:
        print_stats(df, url_col, args.max_posts, args.batch_size)
        generate_slurm_script(args, output_dir)
        return

    # --- Stats only ---
    if args.stats or not (args.run or args.resume or args.prototype):
        print_stats(df, url_col, args.max_posts, args.batch_size)
        if not args.stats:
            parser.print_help()
        print("\n  Use --run to start, --prototype N to test, or --slurm for Sherlock.")
        return

    # --- Load credentials (only when actually calling the API) ---
    print(f"  Output: {output_dir}")
    client = _load_credentials(args.env)

    # --- Prototype ---
    if args.prototype:
        print(f"\n{'=' * 70}")
        print(f"PROTOTYPE MODE — {args.prototype} PROFILES")
        print("=" * 70)
        if not args.yes:
            confirm = input(f"\nScrape {args.prototype} profiles? (y/N): ").strip().lower()
            if confirm != "y":
                print("Cancelled.")
                return
        run_scraping(client, df, url_col, output_dir,
                     args.max_posts, args.batch_size,
                     resume=False, prototype_limit=args.prototype)
        return

    # --- Run / Resume ---
    print_stats(df, url_col, args.max_posts, args.batch_size)

    unique_count = df[url_col].nunique()
    if not args.yes and not args.resume:
        confirm = input(f"\nScrape {unique_count:,} profiles? (y/N): ").strip().lower()
        if confirm != "y":
            print("Cancelled.")
            return

    resume = args.resume or (args.run and not args.no_resume)
    run_scraping(client, df, url_col, output_dir,
                 args.max_posts, args.batch_size, resume=resume)


if __name__ == "__main__":
    main()
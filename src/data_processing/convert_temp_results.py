"""
Convert temp scrape results → proper CSVs.

Handles both formats:
  - temp_results.json  (JSON array from old checkpoint format)
  - temp_results.jsonl (JSONL from new incremental format)

Reuses the parsing logic from scrape_posts.py to produce:
  - posts_*.csv (with metadata join)
  - profiles_*.csv
  - no_posts_profiles_*.csv
  - posts_raw_*.json (raw data with proper naming)

Place in: src/data_processing/convert_temp_results.py

Usage:
    # Auto-detect temp file in default location
    python3 src/data_processing/convert_temp_results.py

    # Specify paths explicitly
    python3 src/data_processing/convert_temp_results.py \\
        --temp data/.../temp_results.jsonl \\
        --input data/.../all_linkedin_urls.csv \\
        --output data/.../scraped_posts_batch3/ \\
        --profiles-attempted 28600

SLURM (for large files):
    sbatch --partition=nbloom --mem=64G --time=01:00:00 \\
           --job-name=convert --output=logs/convert_%j.log \\
           --wrap='module load python/3.12; source venv/bin/activate; \\
                   python3 src/data_processing/convert_temp_results.py [args]'

Check results:
    cat "$(ls -t logs/convert_*.log | head -1)"
    ls -lh data/processed/all_people_linkedin_urls/scraped_posts_batch*/
"""

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve project root (works from any working directory)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # src/data_processing/../../

# Add src/data_collection so we can import scrape_posts
sys.path.insert(0, str(PROJECT_ROOT / "src" / "data_collection"))

from scrape_posts import (
    _build_url_metadata,
    _parse_results,
    _normalise_url,
)

import json
import pandas as pd
from datetime import datetime


def load_temp_results(path):
    """Load results from either JSON array or JSONL format."""
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix == ".jsonl":
        print(f"  Format: JSONL (line-delimited)", flush=True)
        data = []
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if line:
                    data.append(json.loads(line))
                if (i + 1) % 500_000 == 0:
                    print(f"    ... loaded {i + 1:,} items", flush=True)
        return data
    else:
        print(f"  Format: JSON array", flush=True)
        with open(path) as f:
            return json.load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Convert temp scrape results to proper CSVs")
    parser.add_argument("--temp", type=str, default=None,
                        help="Path to temp_results.json or .jsonl")
    parser.add_argument("--input", type=str, default=None,
                        help="Path to input CSV (for metadata join)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output directory for CSVs")
    parser.add_argument("--profiles-attempted", type=int, default=None,
                        help="Number of profiles attempted (overrides checkpoint)")
    args = parser.parse_args()

    # ---------------------------------------------------------------------------
    # Resolve paths (defaults for the standard project layout)
    # ---------------------------------------------------------------------------
    data_dir = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls"

    # Find temp file
    if args.temp:
        temp_path = Path(args.temp)
    else:
        # Auto-detect: prefer JSONL, fall back to JSON
        jsonl = data_dir / "scraped_posts" / "temp_results.jsonl"
        json_f = data_dir / "scraped_posts" / "temp_results.json"
        if jsonl.exists():
            temp_path = jsonl
        elif json_f.exists():
            temp_path = json_f
        else:
            print("ERROR: No temp file found. Specify with --temp", flush=True)
            sys.exit(1)

    input_csv = Path(args.input) if args.input else data_dir / "all_linkedin_urls.csv"
    output_dir = Path(args.output) if args.output else data_dir / "scraped_posts_converted"

    # Profiles attempted
    profiles_attempted = args.profiles_attempted
    if profiles_attempted is None:
        # Try to read from checkpoint
        cp_file = temp_path.parent / ".scrape_checkpoint.json"
        if cp_file.exists():
            with open(cp_file) as f:
                cp = json.load(f)
            profiles_attempted = cp.get("profiles_processed", 0)
        else:
            print("ERROR: Cannot determine profiles attempted.", flush=True)
            print("  Specify with --profiles-attempted N", flush=True)
            sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    print(f"{'=' * 60}", flush=True)
    print("Convert temp results → CSVs", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"  Temp file:          {temp_path}", flush=True)
    print(f"  Input CSV:          {input_csv}", flush=True)
    print(f"  Output dir:         {output_dir}", flush=True)
    print(f"  Profiles attempted: {profiles_attempted:,}", flush=True)

    # ---------------------------------------------------------------------------
    # Load input CSV for metadata
    # ---------------------------------------------------------------------------
    print("\nLoading input CSV for metadata...", flush=True)
    df = pd.read_csv(input_csv)
    df = df[df["verified"] == True]
    url_col = "linkedin_url"
    print(f"  Verified rows: {len(df):,}", flush=True)

    # Reconstruct which URLs were submitted (first N unique from input)
    seen = set()
    submitted_urls = []
    for url in df[url_col]:
        norm = str(url).split("?")[0].rstrip("/")
        if norm not in seen:
            seen.add(norm)
            submitted_urls.append(url)
        if len(submitted_urls) >= profiles_attempted:
            break
    print(f"  Submitted URLs (reconstructed): {len(submitted_urls):,}", flush=True)

    # Build metadata lookup
    url_metadata = _build_url_metadata(df, url_col)

    # ---------------------------------------------------------------------------
    # Load temp results
    # ---------------------------------------------------------------------------
    print(f"\nLoading {temp_path.name}...", flush=True)
    raw_data = load_temp_results(temp_path)
    print(f"  Loaded {len(raw_data):,} items", flush=True)

    # ---------------------------------------------------------------------------
    # 1. Save raw JSON with proper naming
    # ---------------------------------------------------------------------------
    raw_path = output_dir / f"posts_raw_{ts}.json"
    print(f"\nSaving raw JSON to {raw_path}...", flush=True)
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False)
    print(f"  ✓ Raw JSON: {raw_path}", flush=True)

    # ---------------------------------------------------------------------------
    # 2. Parse into posts + profiles
    # ---------------------------------------------------------------------------
    print("Parsing results...", flush=True)
    posts_list, profiles_list = _parse_results(raw_data, url_metadata)
    print(f"  Posts rows: {len(posts_list):,}", flush=True)
    print(f"  Profiles:   {len(profiles_list):,}", flush=True)

    # Free raw_data to save memory
    del raw_data

    # ---------------------------------------------------------------------------
    # 3. Posts CSV
    # ---------------------------------------------------------------------------
    posts_path = None
    if posts_list:
        posts_df = pd.DataFrame(posts_list)
        posts_path = output_dir / f"posts_{ts}.csv"
        posts_df.to_csv(posts_path, index=False, encoding="utf-8")
        print(f"  ✓ Posts CSV: {posts_path}  ({len(posts_df):,} rows)", flush=True)
        del posts_df
    del posts_list

    # ---------------------------------------------------------------------------
    # 4. Profiles CSV
    # ---------------------------------------------------------------------------
    profiles_path = None
    if profiles_list:
        profiles_df = pd.DataFrame(profiles_list)
        profiles_path = output_dir / f"profiles_{ts}.csv"
        profiles_df.to_csv(profiles_path, index=False, encoding="utf-8")
        print(f"  ✓ Profiles CSV: {profiles_path}  ({len(profiles_df):,} rows)", flush=True)

    # ---------------------------------------------------------------------------
    # 5. No-posts profiles
    # ---------------------------------------------------------------------------
    no_posts_path = None
    urls_with_posts = {_normalise_url(p["profile_url"]) for p in profiles_list}
    no_post_urls = [u for u in submitted_urls
                    if _normalise_url(u) not in urls_with_posts]

    if no_post_urls:
        norm_meta = {_normalise_url(u): m for u, m in url_metadata.items()}
        no_posts_rows = []
        for url in no_post_urls:
            metas = norm_meta.get(_normalise_url(url), [{}])
            for meta in metas:
                no_posts_rows.append({**meta, "profile_url": url})
        no_posts_df = pd.DataFrame(no_posts_rows)
        no_posts_path = output_dir / f"no_posts_profiles_{ts}.csv"
        no_posts_df.to_csv(no_posts_path, index=False, encoding="utf-8")
        print(f"  ✓ No-posts CSV: {no_posts_path}  ({len(no_post_urls):,} profiles)", flush=True)

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    print(f"\n{'=' * 60}", flush=True)
    print("CONVERSION COMPLETE", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"  Output dir:  {output_dir}", flush=True)
    if posts_path:
        print(f"  Posts CSV:   {posts_path}", flush=True)
    if profiles_path:
        print(f"  Profiles:    {profiles_path}", flush=True)
    if no_posts_path:
        print(f"  No-posts:    {no_posts_path}", flush=True)


if __name__ == "__main__":
    main()
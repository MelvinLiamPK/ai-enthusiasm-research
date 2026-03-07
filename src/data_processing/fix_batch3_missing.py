"""
Generate missing profiles and no_posts CSVs for batch 3.

The convert job OOM'd after writing the posts CSV but before
writing profiles and no_posts CSVs. This script generates them
from the JSONL file without loading the full dataset.

Place in: src/data_processing/fix_batch3_missing.py

Usage:
    sbatch --partition=nbloom --mem=16G --time=00:15:00 \
        --job-name=fix-b3 --output=logs/fix_b3_%j.log \
        --wrap='module load python/3.12; source venv/bin/activate; \
                python3 src/data_processing/fix_batch3_missing.py'

Check results:
    cat "$(ls -t logs/fix_b3_*.log | head -1)"
"""

import json
import sys
import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

sys.path.insert(0, str(PROJECT_ROOT / "src" / "data_collection"))
from scrape_posts import _build_url_metadata, _normalise_url

data_dir = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls"
output_dir = data_dir / "scraped_posts_batch3"
ts = "20260307_110447"

# Load input for metadata
print("Loading input CSV...", flush=True)
df = pd.read_csv(data_dir / "all_linkedin_urls.csv")
df = df[df["verified"] == True]
print(f"  Verified rows: {len(df):,}", flush=True)

# Reconstruct submitted URLs (first 14041 unique)
seen = set()
submitted_urls = []
for url in df["linkedin_url"]:
    n = str(url).split("?")[0].rstrip("/")
    if n not in seen:
        seen.add(n)
        submitted_urls.append(url)
    if len(submitted_urls) >= 14041:
        break
print(f"  Submitted URLs: {len(submitted_urls):,}", flush=True)

# Build profiles from JSONL (lightweight — just unique profile info)
print("Scanning JSONL for profiles...", flush=True)
profiles_seen = {}
with open(output_dir / "temp_results.jsonl") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)
        p = item.get("profile_input", "")
        if p and p not in profiles_seen:
            author = item.get("author") or {}
            profiles_seen[p] = {
                "profile_url": p,
                "author_name": (author.get("first_name", "") + " " + author.get("last_name", "")).strip(),
                "headline": author.get("headline", ""),
                "username": author.get("username", ""),
                "profile_picture": author.get("profile_picture", ""),
            }
print(f"  Profiles with posts: {len(profiles_seen):,}", flush=True)

# Save profiles CSV
profiles_df = pd.DataFrame(list(profiles_seen.values()))
profiles_path = output_dir / f"profiles_{ts}.csv"
profiles_df.to_csv(profiles_path, index=False)
print(f"  ✓ Profiles CSV: {profiles_path} ({len(profiles_df):,} rows)", flush=True)

# Save no-posts CSV
url_metadata = _build_url_metadata(df, "linkedin_url")
urls_with_posts = {_normalise_url(p) for p in profiles_seen}
no_post_urls = [u for u in submitted_urls if _normalise_url(u) not in urls_with_posts]
norm_meta = {_normalise_url(u): m for u, m in url_metadata.items()}
no_posts_rows = []
for url in no_post_urls:
    metas = norm_meta.get(_normalise_url(url), [{}])
    for meta in metas:
        no_posts_rows.append({**meta, "profile_url": url})
no_posts_df = pd.DataFrame(no_posts_rows)
no_posts_path = output_dir / f"no_posts_profiles_{ts}.csv"
no_posts_df.to_csv(no_posts_path, index=False)
print(f"  ✓ No-posts CSV: {no_posts_path} ({len(no_post_urls):,} profiles)", flush=True)

print("\nDone!", flush=True)

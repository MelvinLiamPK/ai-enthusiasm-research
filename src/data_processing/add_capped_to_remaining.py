"""
Add profiles capped at 1000 posts back to the remaining URLs file.

These profiles were scraped with --max-posts 1000 and need re-scraping
with the 10,000 limit to get their full post history.

Place in: src/data_processing/add_capped_to_remaining.py

Usage:
    python3 src/data_processing/add_capped_to_remaining.py

Check results:
    cat "$(ls -t logs/add_capped_*.log | head -1)"
"""

import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

data_dir = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls"
batch2_posts = data_dir / "scraped_posts_batch2" / "posts_20260304_221328.csv"
remaining_csv = data_dir / "remaining_urls_final.csv"
input_csv = data_dir / "all_linkedin_urls.csv"

# 1. Find profiles capped at exactly 1000 unique posts in batch 2
print("Loading batch 2 posts...", flush=True)
df = pd.read_csv(batch2_posts, usecols=["profile_url", "post_url"],
                 engine="c", lineterminator="\n", on_bad_lines="skip")
counts = df.drop_duplicates(subset=["post_url"]).groupby("profile_url").size()
capped = set(counts[counts == 1000].index)
print(f"  Profiles capped at 1000: {len(capped)}", flush=True)

# 2. Load full input for metadata
print("Loading input CSV...", flush=True)
all_df = pd.read_csv(input_csv)
all_df = all_df[all_df["verified"] == True]

# Normalize URLs for matching
norm = lambda u: str(u).split("?")[0].rstrip("/")
capped_norm = {norm(u) for u in capped}
all_df["_norm"] = all_df["linkedin_url"].apply(norm)

# Get rows for capped profiles
capped_rows = all_df[all_df["_norm"].isin(capped_norm)].drop(columns="_norm")
capped_unique = capped_rows["linkedin_url"].apply(norm).nunique()
print(f"  Capped profiles matched in input: {capped_unique}", flush=True)
print(f"  Capped rows (with multi-board): {len(capped_rows)}", flush=True)

# 3. Load existing remaining URLs
print("Loading remaining URLs...", flush=True)
remaining = pd.read_csv(remaining_csv)
remaining_before = remaining["linkedin_url"].apply(norm).nunique()
print(f"  Existing remaining unique URLs: {remaining_before}", flush=True)

# 4. Check overlap (some capped profiles might already be in remaining)
remaining_norms = set(remaining["linkedin_url"].apply(norm))
new_capped = capped_rows[~capped_rows["linkedin_url"].apply(norm).isin(remaining_norms)]
new_unique = new_capped["linkedin_url"].apply(norm).nunique()
print(f"  Already in remaining: {capped_unique - new_unique}", flush=True)
print(f"  New to add: {new_unique}", flush=True)

# 5. Combine and save
combined = pd.concat([remaining, new_capped], ignore_index=True).drop_duplicates()
combined_unique = combined["linkedin_url"].apply(norm).nunique()
print(f"\nCombined:", flush=True)
print(f"  Total rows: {len(combined):,}", flush=True)
print(f"  Unique URLs: {combined_unique:,}", flush=True)

combined.to_csv(remaining_csv, index=False)
print(f"\n  ✓ Updated: {remaining_csv}", flush=True)
print(f"  Was: {remaining_before:,} unique URLs", flush=True)
print(f"  Now: {combined_unique:,} unique URLs", flush=True)
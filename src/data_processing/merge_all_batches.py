"""
Merge scraped LinkedIn post batches into a single combined dataset.

Combines batch 2 (28,600 profiles attempted, pagination with 1000 cap)
and batch 3 (14,041 profiles, pagination with 10000 cap).

For the 114 overlapping profiles (re-scraped because they hit the 1000
cap in batch 2), batch 3 data is preferred (has full post history).

Produces:
  - posts_combined.csv          — all posts with metadata
  - profiles_combined.csv       — profile-level summary
  - no_posts_combined.csv       — profiles with zero posts
  - missing_profiles.csv        — profiles never successfully scraped (failed batches)
  - merge_report.txt            — summary statistics

Place in: src/data_processing/merge_all_batches.py

Usage (SLURM — needs ~40GB for 2.6GB of CSVs):
    sbatch --partition=nbloom --mem=64G --time=01:00:00 \
        --job-name=merge --output=logs/merge_%j.log \
        --wrap='module load python/3.12; source venv/bin/activate; \
                python3 src/data_processing/merge_all_batches.py'

Check results:
    cat "$(ls -t logs/merge_*.log | head -1)"
    ls -lh data/processed/all_people_linkedin_urls/scraped_posts_combined/
"""

import json
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Add src/data_collection for parsing functions
sys.path.insert(0, str(PROJECT_ROOT / "src" / "data_collection"))
from scrape_posts import _build_url_metadata, _parse_results, _normalise_url

DATA_DIR = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls"
BATCH2_DIR = DATA_DIR / "scraped_posts_batch2"
BATCH3_DIR = DATA_DIR / "scraped_posts_batch3"
OUTPUT_DIR = DATA_DIR / "scraped_posts_combined"
INPUT_CSV = DATA_DIR / "all_linkedin_urls.csv"

# Batch 2: CSV (already converted)
BATCH2_POSTS = BATCH2_DIR / "posts_20260304_221328.csv"

# Batch 3: read from JSONL (CSV was truncated due to disk full)
BATCH3_JSONL = BATCH3_DIR / "temp_results.jsonl"

norm = lambda u: str(u).split("?")[0].rstrip("/")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_lines = []

    def log(msg):
        print(msg, flush=True)
        report_lines.append(msg)

    log("=" * 70)
    log("MERGING SCRAPED BATCHES")
    log("=" * 70)
    log(f"  Batch 2: {BATCH2_POSTS}")
    log(f"  Batch 3: {BATCH3_POSTS}")
    log(f"  Output:  {OUTPUT_DIR}")
    log("")

    # ======================================================================
    # 1. Load both batches
    # ======================================================================
    log("Loading batch 2 posts...")
    b2 = pd.read_csv(BATCH2_POSTS, engine="c", lineterminator="\n",
                      on_bad_lines="skip", low_memory=False)
    log(f"  Rows: {len(b2):,}  Unique profiles: {b2['profile_url'].nunique():,}")

    log("Loading batch 3 from JSONL (parsing with metadata)...")
    # Load input CSV for metadata join
    all_df = pd.read_csv(INPUT_CSV)
    all_df = all_df[all_df["verified"] == True]
    url_metadata = _build_url_metadata(all_df, "linkedin_url")

    # Load JSONL
    raw_data = []
    with open(BATCH3_JSONL, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if line:
                raw_data.append(json.loads(line))
            if (i + 1) % 500_000 == 0:
                log(f"    ... loaded {i + 1:,} items")
    log(f"  Loaded {len(raw_data):,} items from JSONL")

    # Parse into posts + profiles
    b3_posts_list, _ = _parse_results(raw_data, url_metadata)
    del raw_data  # free memory
    b3 = pd.DataFrame(b3_posts_list)
    del b3_posts_list
    log(f"  Rows: {len(b3):,}  Unique profiles: {b3['profile_url'].nunique():,}")

    # ======================================================================
    # 2. Identify overlapping profiles (re-scraped in batch 3)
    # ======================================================================
    b2_profiles = set(b2["profile_url"].dropna().apply(norm))
    b3_profiles = set(b3["profile_url"].dropna().apply(norm))
    overlap = b2_profiles & b3_profiles
    log(f"\nOverlapping profiles: {len(overlap):,}")
    log(f"  Only in batch 2: {len(b2_profiles - b3_profiles):,}")
    log(f"  Only in batch 3: {len(b3_profiles - b2_profiles):,}")

    # ======================================================================
    # 3. For overlapping profiles, prefer batch 3 (has >1000 posts)
    # ======================================================================
    log("\nRemoving overlapping profiles from batch 2 (batch 3 has better data)...")
    b2["_norm"] = b2["profile_url"].apply(lambda u: norm(str(u)))
    b2_keep = b2[~b2["_norm"].isin(overlap)].drop(columns="_norm")
    b2_dropped = len(b2) - len(b2_keep)
    log(f"  Dropped {b2_dropped:,} rows from batch 2 (overlap)")
    log(f"  Keeping {len(b2_keep):,} rows from batch 2")

    # ======================================================================
    # 4. Combine
    # ======================================================================
    log("\nCombining...")
    combined = pd.concat([b2_keep, b3], ignore_index=True)
    log(f"  Combined rows: {len(combined):,}")

    # Free memory
    del b2, b3, b2_keep

    # ======================================================================
    # 5. Deduplicate by (post_url, company_name)
    # ======================================================================
    before_dedup = len(combined)
    combined = combined.drop_duplicates(subset=["post_url", "company_name"])
    after_dedup = len(combined)
    log(f"  After dedup by (post_url, company_name): {after_dedup:,}  (dropped {before_dedup - after_dedup:,} exact dupes)")

    # ======================================================================
    # 6. Drop completely empty rows
    # ======================================================================
    empty = combined.isna().all(axis=1)
    if empty.sum() > 0:
        combined = combined[~empty]
        log(f"  Dropped {empty.sum():,} completely empty rows")

    # Drop rows with null profile_url (junk Apify placeholders)
    null_profile = combined["profile_url"].isna()
    if null_profile.sum() > 0:
        combined = combined[~null_profile]
        log(f"  Dropped {null_profile.sum():,} rows with null profile_url")

    # ======================================================================
    # 7. Summary stats
    # ======================================================================
    unique_profiles = combined["profile_url"].nunique()
    unique_posts = combined["post_url"].nunique()
    date_min = combined["post_date"].min()
    date_max = combined["post_date"].max()

    log(f"\n{'=' * 70}")
    log("COMBINED DATASET")
    log(f"{'=' * 70}")
    log(f"  Total rows:       {len(combined):,}")
    log(f"  Unique posts:     {unique_posts:,}")
    log(f"  Unique profiles:  {unique_profiles:,}")
    log(f"  Date range:       {date_min} → {date_max}")
    log(f"  Duplication ratio: {len(combined)/unique_posts:.2f}x (multi-board)")

    # Posts per profile
    counts = combined.groupby("profile_url")["post_url"].nunique()
    log(f"\n  Posts per profile:")
    log(f"    Mean:   {counts.mean():.1f}")
    log(f"    Median: {counts.median():.0f}")
    log(f"    Max:    {counts.max():,}")
    log(f"    >100:   {(counts > 100).sum():,}")
    log(f"    >1000:  {(counts > 1000).sum():,}")

    # ======================================================================
    # 8. Save posts CSV
    # ======================================================================
    log(f"\nSaving...")
    posts_path = OUTPUT_DIR / "posts_combined.csv"
    combined.to_csv(posts_path, index=False, encoding="utf-8")
    log(f"  ✓ Posts: {posts_path}  ({len(combined):,} rows)")

    # ======================================================================
    # 9. Build and save profiles CSV
    # ======================================================================
    profile_cols = ["profile_url", "author_name", "author_headline"]
    if "author_name" in combined.columns:
        profiles = combined.drop_duplicates(subset=["profile_url"])[
            [c for c in profile_cols if c in combined.columns]
        ].copy()
    else:
        profiles = pd.DataFrame({"profile_url": combined["profile_url"].unique()})
    profiles_path = OUTPUT_DIR / "profiles_combined.csv"
    profiles.to_csv(profiles_path, index=False, encoding="utf-8")
    log(f"  ✓ Profiles: {profiles_path}  ({len(profiles):,} rows)")

    # ======================================================================
    # 10. No-posts and missing profiles
    # ======================================================================
    log("\nChecking for no-posts and missing profiles...")

    # Load all verified URLs (already loaded earlier for batch 3 parsing)
    all_df["_norm"] = all_df["linkedin_url"].apply(norm)
    all_unique_urls = set(all_df["_norm"].unique())
    log(f"  Total verified unique URLs: {len(all_unique_urls):,}")

    # Profiles with posts in combined dataset
    combined_profiles = set(combined["profile_url"].dropna().apply(norm))
    log(f"  Profiles with posts: {len(combined_profiles):,}")

    # No posts = verified URLs not in combined dataset
    # This includes genuinely zero-post profiles AND failed batch profiles
    no_posts_norms = all_unique_urls - combined_profiles
    no_posts_rows = all_df[all_df["_norm"].isin(no_posts_norms)].drop(columns="_norm")
    no_posts_unique = len(no_posts_norms)

    no_posts_path = OUTPUT_DIR / "no_posts_combined.csv"
    no_posts_rows.to_csv(no_posts_path, index=False, encoding="utf-8")
    log(f"  ✓ No-posts/missing: {no_posts_path}  ({no_posts_unique:,} unique profiles)")

    # ======================================================================
    # 11. Engagement summary
    # ======================================================================
    log(f"\n{'=' * 70}")
    log("ENGAGEMENT METRICS")
    log(f"{'=' * 70}")
    for col in ["likes", "comments", "reposts", "reactions_total"]:
        if col in combined.columns:
            log(f"  {col}: mean={combined[col].mean():.1f}, median={combined[col].median():.0f}")

    # ======================================================================
    # 12. Posts by year
    # ======================================================================
    log(f"\n{'=' * 70}")
    log("POSTS BY YEAR (unique posts)")
    log(f"{'=' * 70}")
    combined["_year"] = pd.to_datetime(combined["post_date"], errors="coerce").dt.year
    year_counts = combined.drop_duplicates(subset=["post_url"]).groupby(
        pd.to_datetime(combined.drop_duplicates(subset=["post_url"])["post_date"], errors="coerce").dt.year
    ).size()
    for year in sorted(year_counts.index.dropna()):
        count = year_counts[year]
        bar = "█" * int(count / year_counts.max() * 40)
        log(f"  {int(year)}: {count:>8,}  {bar}")
    combined.drop(columns="_year", inplace=True)

    # ======================================================================
    # 13. Save report
    # ======================================================================
    report_path = OUTPUT_DIR / "merge_report.txt"
    with open(report_path, "w") as f:
        f.write("\n".join(report_lines))
    log(f"\n  ✓ Report: {report_path}")

    log(f"\n{'=' * 70}")
    log("MERGE COMPLETE")
    log(f"{'=' * 70}")


if __name__ == "__main__":
    main()
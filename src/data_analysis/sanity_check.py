"""Sanity check on batch 2 scraped data."""
import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

csv_path = PROJECT_ROOT / "data/processed/all_people_linkedin_urls/scraped_posts_batch2/posts_20260304_221328.csv"

print("=" * 60)
print("BATCH 2 POSTS CSV")
print("=" * 60)
print(f"File: {csv_path}")
print(f"Size: {csv_path.stat().st_size / 1e9:.2f} GB")
print()

# Try python engine which handles malformed rows better
try:
    df = pd.read_csv(csv_path, low_memory=False, engine="python", on_bad_lines="skip")
    print(f"Loaded with python engine (on_bad_lines=skip)")
except Exception as e:
    print(f"Python engine also failed: {e}")
    print("Trying with c engine and error_bad_lines=False...")
    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip",
                     engine="c", lineterminator="\n")

print(f"Rows: {len(df):,}")
print(f"Columns: {len(df.columns)}")
print(f"Unique profiles: {df['profile_url'].nunique():,}")
print(f"Unique posts: {df['post_url'].nunique():,}")
print(f"Date range: {df['post_date'].min()} -> {df['post_date'].max()}")
print()

# Posts per profile distribution
counts = df.groupby("profile_url")["post_url"].nunique()
print("Posts per profile:")
print(f"  Mean: {counts.mean():.1f}")
print(f"  Median: {counts.median():.0f}")
print(f"  Max: {counts.max()}")
print(f"  >100: {(counts > 100).sum():,}")
print(f"  =100 (still capped?): {(counts == 100).sum():,}")
print()

# Null check on critical fields
print("Null counts (critical fields):")
for col in ["profile_url", "post_text", "post_url", "post_date",
            "likes", "company_name", "person_name", "ticker"]:
    if col in df.columns:
        n = df[col].isna().sum()
        pct = n / len(df) * 100
        print(f"  {col}: {n:,} ({pct:.1f}%)")
print()

# Empty rows (all NaN)
all_nan = df.isna().all(axis=1).sum()
print(f"Completely empty rows: {all_nan:,}")
print()

# Engagement metrics
print("Engagement metrics:")
for col in ["likes", "comments", "reposts", "reactions_total"]:
    if col in df.columns:
        print(f"  {col}: mean={df[col].mean():.1f}, median={df[col].median():.0f}, zeros={(df[col] == 0).sum():,}")
print()

# Pagination check
print("Pagination validation (profiles with >100 unique posts):")
over100 = counts[counts > 100]
print(f"  Profiles with >100 posts: {len(over100):,}")
if len(over100) > 0:
    top5 = over100.sort_values(ascending=False).head()
    for url, c in top5.items():
        print(f"    {c} posts: {url}")
print()

# Multi-board duplication check
print("Multi-board duplication:")
dedup = df.drop_duplicates(subset=["post_url"])
print(f"  Total rows: {len(df):,}")
print(f"  Unique posts: {len(dedup):,}")
print(f"  Duplication ratio: {len(df)/len(dedup):.2f}x")
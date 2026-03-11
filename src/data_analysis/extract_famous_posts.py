#!/usr/bin/env python3
"""
Extract posts for 10 well-known executives and save as Excel.
Run on Sherlock via SLURM (needs ~32GB for the CSV load).

Usage:
    python3 extract_famous_posts.py
    sbatch scripts/slurm_extract_famous.sh
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
POSTS_PATH = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "scraped_posts_combined" / "posts_combined.csv"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

# 10 well-known executives confirmed in the dataset
FAMOUS_PROFILES = {
    "https://www.linkedin.com/in/satyanadella": "Satya Nadella",
    "https://www.linkedin.com/in/sundarpichai": "Sundar Pichai",
    "https://www.linkedin.com/in/brianchesky": "Brian Chesky",
    "https://www.linkedin.com/in/reedhastings": "Reed Hastings",
    "https://www.linkedin.com/in/dara-khosrowshahi-70949862": "Dara Khosrowshahi",
    "https://www.linkedin.com/in/aravind-krishna-65b3b04": "Arvind Krishna",
    "https://www.linkedin.com/in/shantanu-narayen": "Shantanu Narayen",
    "https://www.linkedin.com/in/ryanroslansky": "Ryan Roslansky",
    "https://www.linkedin.com/in/nikesh-arora-02894670": "Nikesh Arora",
    "https://www.linkedin.com/in/frankslootman": "Frank Slootman",
}

def main():
    print("Loading posts_combined.csv...")
    df = pd.read_csv(POSTS_PATH, engine="c", lineterminator="\n",
                     on_bad_lines="skip", low_memory=False)
    print(f"  Loaded {len(df):,} rows")

    # Filter to famous profiles
    target_urls = set(FAMOUS_PROFILES.keys())
    mask = df["profile_url"].isin(target_urls)
    famous = df[mask].copy()

    # Deduplicate: keep one row per post (remove multi-board expansion for readability)
    famous = famous.drop_duplicates(subset=["post_url"], keep="first")
    famous = famous.sort_values(["profile_url", "post_date"], ascending=[True, False])

    # Add a readable name column
    famous.insert(0, "executive", famous["profile_url"].map(FAMOUS_PROFILES))

    # Select columns for readability
    cols = [
        "executive", "company_name", "post_date", "post_type", "post_text",
        "reactions_total", "likes", "comments", "reposts",
        "celebrates", "supports", "loves",
        "article_url", "article_title", "reshared_text", "post_url", "profile_url",
    ]
    cols = [c for c in cols if c in famous.columns]
    famous = famous[cols]

    print(f"\n  Posts extracted: {len(famous):,}")
    print(f"  Executives found:")
    for url, name in FAMOUS_PROFILES.items():
        n = (famous["profile_url"] == url).sum()
        print(f"    {name:25s}  {n:>5,} posts")

    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d")
    out_path = OUTPUT_DIR / f"famous_executive_posts_{ts}.xlsx"

    print(f"\n  Saving to: {out_path}")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        famous.to_excel(writer, sheet_name="All Posts", index=False)

        # Also add a summary sheet
        summary = famous.groupby("executive").agg(
            total_posts=("post_url", "count"),
            date_range_start=("post_date", "min"),
            date_range_end=("post_date", "max"),
            mean_reactions=("reactions_total", "mean"),
            median_reactions=("reactions_total", "median"),
            total_reactions=("reactions_total", "sum"),
            total_comments=("comments", "sum"),
        ).round(1).reset_index()
        summary = summary.sort_values("total_posts", ascending=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)

    print(f"  Done. File size: {out_path.stat().st_size / 1e6:.1f} MB")

if __name__ == "__main__":
    main()

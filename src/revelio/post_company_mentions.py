"""
Check scraped posts for company name mentions per person-company pair.

For each row in all_linkedin_urls.csv that has a scraped profile, counts how many
posts mention the expected company name. Adds two columns:

  - company_mention_count  (int):   number of posts mentioning the company
  - company_mention_ratio  (float): mention_count / total_posts (null if <5 posts)

Edge cases:
  - Short/ambiguous company names (<4 chars) → skipped, columns left null
  - Blockholders → signal is unreliable (passive investors rarely mention company)
  - Fewer than 5 posts → ratio left null (insufficient data)
  - Both post_text and reshared_text are checked

Usage:
    python3 src/revelio/post_company_mentions.py [--stats] [--dry-run] [--prototype N]
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
URLS_PATH = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls.csv"
POSTS_PATH = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "scraped_posts_combined" / "posts_combined.csv"

MIN_COMPANY_NAME_LEN = 4
MIN_POSTS_FOR_RATIO = 5


def clean_url(url):
    """Normalise to linkedin.com/in/<slug> for joining."""
    if pd.isna(url):
        return None
    url = str(url).strip().rstrip("/")
    url = re.sub(r"^https?://(www\.)?", "", url)
    return url if url.startswith("linkedin.com/in/") else None


def mentions_company(post_text, reshared_text, pattern):
    """True if either text field matches the company pattern."""
    for text in (post_text, reshared_text):
        if not pd.isna(text) and pattern.search(str(text)):
            return True
    return False


def make_pattern(company_name):
    """Word-boundary regex for company name. Returns None if name too short."""
    name = str(company_name).strip()
    if len(name) < MIN_COMPANY_NAME_LEN:
        return None
    return re.compile(r"\b" + re.escape(name) + r"\b", re.IGNORECASE)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--stats", action="store_true", help="Print summary statistics")
    parser.add_argument("--dry-run", action="store_true", help="Do not write output")
    parser.add_argument("--prototype", type=int, metavar="N", help="Run on first N scraped profiles only")
    args = parser.parse_args()

    # --- Load URLs ---
    print("Loading all_linkedin_urls.csv...")
    urls_df = pd.read_csv(URLS_PATH)
    print(f"  {len(urls_df):,} rows")

    urls_df["_clean_url"] = urls_df["linkedin_url"].apply(clean_url)

    # Only process rows with a found URL (no point checking posts for not_found)
    has_url = urls_df["_clean_url"].notna() & (urls_df["search_status"] == "found")
    print(f"  {has_url.sum():,} rows with a found URL")

    # --- Load posts ---
    print("Loading posts_combined.csv (this may take a moment)...")
    posts_df = pd.read_csv(
        POSTS_PATH,
        engine="c",
        lineterminator="\n",
        on_bad_lines="skip",
        usecols=["profile_url", "post_text", "reshared_text"],
    )
    posts_df = posts_df[posts_df["profile_url"].notna()].copy()
    posts_df["_clean_url"] = posts_df["profile_url"].apply(clean_url)
    print(f"  {len(posts_df):,} posts loaded")

    scraped_urls = set(posts_df["_clean_url"].dropna().unique())
    print(f"  {len(scraped_urls):,} unique scraped profiles")

    # --- Scope to scraped profiles ---
    work = urls_df[has_url & urls_df["_clean_url"].isin(scraped_urls)].copy()
    print(f"  {len(work):,} URL rows have scraped posts")

    if args.prototype:
        unique_profiles = work["_clean_url"].unique()[: args.prototype]
        work = work[work["_clean_url"].isin(unique_profiles)]
        posts_df = posts_df[posts_df["_clean_url"].isin(unique_profiles)]
        print(f"  [prototype] limited to {args.prototype} profiles ({len(work):,} URL rows)")

    # Group posts by profile URL for fast lookup
    posts_grouped = posts_df.groupby("_clean_url")

    # --- Compute mention counts ---
    print("Computing company mention counts...")

    mention_count = []
    mention_ratio = []

    for row in work.itertuples(index=False):
        company = row.company_name_clean
        pattern = make_pattern(company)

        # Null out for blockholders or short names
        if pattern is None or row.source == "blockholder":
            mention_count.append(None)
            mention_ratio.append(None)
            continue

        try:
            group = posts_grouped.get_group(row._clean_url)
        except KeyError:
            mention_count.append(0)
            mention_ratio.append(None)
            continue

        total = len(group)
        count = sum(
            mentions_company(r.post_text, r.reshared_text, pattern)
            for r in group.itertuples(index=False)
        )
        mention_count.append(count)
        mention_ratio.append(count / total if total >= MIN_POSTS_FOR_RATIO else None)

    work["company_mention_count"] = mention_count
    work["company_mention_ratio"] = mention_ratio

    # Merge back into full urls_df
    urls_df = urls_df.merge(
        work[["_clean_url", "company_name_clean", "company_mention_count", "company_mention_ratio"]],
        on=["_clean_url", "company_name_clean"],
        how="left",
    )
    urls_df.drop(columns=["_clean_url"], inplace=True)

    # --- Stats ---
    if args.stats:
        w = work.dropna(subset=["company_mention_count"])
        total_w = len(w)
        zero = (w["company_mention_count"] == 0).sum()
        any_mention = (w["company_mention_count"] > 0).sum()
        with_ratio = w["company_mention_ratio"].notna().sum()
        high = (w["company_mention_ratio"] >= 0.1).sum() if with_ratio else 0

        print(f"\n--- Stats ---")
        print(f"Profiles checked (non-blockholder, long name): {total_w:>8,}")
        print(f"  Zero mentions:                               {zero:>8,}  ({zero/total_w*100:.1f}%)")
        print(f"  Any mentions:                                {any_mention:>8,}  ({any_mention/total_w*100:.1f}%)")
        print(f"  With ratio (≥{MIN_POSTS_FOR_RATIO} posts):  {with_ratio:>8,}")
        print(f"  Ratio ≥ 10%:                                 {high:>8,}")

    # --- Write ---
    if not args.dry_run:
        urls_df.to_csv(URLS_PATH, index=False)
        print(f"\nSaved updated all_linkedin_urls.csv ({len(urls_df):,} rows)")
    else:
        print("\n[dry-run] No file written.")


if __name__ == "__main__":
    main()

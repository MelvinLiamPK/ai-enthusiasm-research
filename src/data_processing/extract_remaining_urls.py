"""
Extract remaining LinkedIn URLs that haven't been scraped yet.

Compares the full verified URL list against the 28,600 profiles
already attempted (based on checkpoint) and outputs the remainder.

Place in: src/data_processing/extract_remaining_urls.py

Usage:
    python3 src/data_processing/extract_remaining_urls.py
"""

from pathlib import Path
import pandas as pd
import json


def main():
    # ---------------------------------------------------------------------------
    # Paths (resolve from script location)
    # ---------------------------------------------------------------------------
    SCRIPT_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = SCRIPT_DIR.parent.parent

    data_dir = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls"
    input_csv = data_dir / "all_linkedin_urls.csv"
    checkpoint = data_dir / "scraped_posts" / ".scrape_checkpoint.json"
    output_csv = data_dir / "remaining_urls_final.csv"

    # ---------------------------------------------------------------------------
    # Load checkpoint to know how many profiles were attempted
    # ---------------------------------------------------------------------------
    print("Loading checkpoint...", flush=True)
    with open(checkpoint) as f:
        cp = json.load(f)
    profiles_attempted = cp.get("profiles_processed", 0)
    print(f"  Profiles attempted: {profiles_attempted:,}", flush=True)

    # ---------------------------------------------------------------------------
    # Load full input, filter to verified
    # ---------------------------------------------------------------------------
    print("Loading input CSV...", flush=True)
    df = pd.read_csv(input_csv)
    df = df[df["verified"] == True]
    print(f"  Verified rows: {len(df):,}", flush=True)

    # ---------------------------------------------------------------------------
    # Reconstruct which URLs were submitted
    # The scraper processes unique URLs in order, so the first N unique
    # from the input are the ones that were attempted.
    # ---------------------------------------------------------------------------
    norm = lambda u: str(u).split("?")[0].rstrip("/")

    seen = set()
    attempted_norms = set()
    for url in df["linkedin_url"]:
        n = norm(url)
        if n not in seen:
            seen.add(n)
            attempted_norms.add(n)
        if len(attempted_norms) >= profiles_attempted:
            break

    print(f"  Attempted URLs (reconstructed): {len(attempted_norms):,}", flush=True)

    # ---------------------------------------------------------------------------
    # Filter to remaining (not yet attempted)
    # ---------------------------------------------------------------------------
    df["_norm"] = df["linkedin_url"].apply(norm)
    remaining = df[~df["_norm"].isin(attempted_norms)].drop(columns="_norm")

    unique_remaining = remaining["linkedin_url"].apply(norm).nunique()
    print(f"  Remaining rows: {len(remaining):,}", flush=True)
    print(f"  Remaining unique URLs: {unique_remaining:,}", flush=True)

    # ---------------------------------------------------------------------------
    # Save
    # ---------------------------------------------------------------------------
    remaining.to_csv(output_csv, index=False)
    print(f"\n  ✓ Saved: {output_csv}", flush=True)


if __name__ == "__main__":
    main()
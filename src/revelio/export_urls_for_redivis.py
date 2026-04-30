"""
Export cleaned LinkedIn URLs for upload to Redivis.
Produces a CSV with one column (clean_linkedin_url) in the format
linkedin.com/in/<slug> — matching Revelio's profile_linkedin_url format.

Usage:
    python3 src/revelio/export_urls_for_redivis.py
"""

import re
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls.csv"
OUTPUT = PROJECT_ROOT / "data" / "revelio" / "urls_for_redivis.csv"


def clean_linkedin_url(url):
    """Strip protocol, www, trailing slash → linkedin.com/in/<slug>"""
    if pd.isna(url):
        return None
    url = str(url).strip()
    url = re.sub(r"^https?://(www\.)?", "", url)
    url = url.rstrip("/")
    if re.match(r"^linkedin\.com/in/[A-Za-z0-9\-%]+$", url):
        return url
    return None


def main():
    df = pd.read_csv(INPUT)
    print(f"Loaded: {len(df):,} rows")

    urls = df["linkedin_url"].dropna().unique()
    print(f"Unique raw URLs: {len(urls):,}")

    cleaned = pd.Series(urls).apply(clean_linkedin_url).dropna().unique()
    print(f"Valid cleaned URLs: {len(cleaned):,}")

    out = pd.DataFrame({"clean_linkedin_url": sorted(cleaned)})
    out.to_csv(OUTPUT, index=False)
    print(f"\nSaved: {OUTPUT}")
    print(f"Upload this file as a dataset table in Redivis.")


if __name__ == "__main__":
    main()

"""
Merge original (board-anchored) LinkedIn URLs with new DEF 14A (primary-anchored)
URLs into a single dataset with explicit board_company AND primary_company columns
per row, so the Revelio cross-check can evaluate both Tier 1 (board-only, legacy)
and Tier 2 (union: board OR primary) verification methods.

Output: data/processed/all_people_linkedin_urls/all_linkedin_urls_v2.csv

Each row is one URL hypothesis (we keep both original and DEF 14A rows when a
director has both; the Redivis notebook validates each row independently).

Usage:
    python3 src/data_collection/merge_def14a_urls.py
"""

import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

ALL_URLS_PATH = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls.csv"
BIOS_PATH = PROJECT_ROOT / "data" / "processed" / "def14a_extracted_bios.csv"
DEF14A_URLS_PATH = PROJECT_ROOT / "data" / "processed" / "def14a_serper_results_final.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls_v2.csv"

VALID_SEARCH_STATUSES = {"found", "not_found"}


def normalize_name(s):
    if pd.isna(s):
        return ""
    return str(s).lower().strip()


def main():
    print("=" * 70)
    print("Merging original + DEF 14A URLs into all_linkedin_urls_v2.csv")
    print("=" * 70)

    for path in [ALL_URLS_PATH, BIOS_PATH, DEF14A_URLS_PATH]:
        if not path.exists():
            print(f"ERROR: missing input {path}")
            return 1

    # 1. Load original URLs (board-anchored)
    all_urls = pd.read_csv(ALL_URLS_PATH)
    print(f"\nLoaded {len(all_urls):,} original URLs ({all_urls.shape[1]} cols)")

    # 2. Load DEF 14A bios and build (person_name_lower, ticker) -> primary_company lookup
    bios = pd.read_csv(BIOS_PATH)
    bios_dir = bios[bios["role_context"] == "director"].copy()
    bios_dir = bios_dir[bios_dir["full_name"].notna() & bios_dir["primary_company"].notna()]
    bios_dir["person_key"] = bios_dir["full_name"].map(normalize_name)
    # Keep most recent year per (person, ticker)
    bios_dir = bios_dir.sort_values("year", ascending=False).drop_duplicates(
        subset=["person_key", "ticker"]
    )
    primary_lookup = {
        (row.person_key, row.ticker): row.primary_company
        for row in bios_dir.itertuples(index=False)
    }
    print(f"Built primary_company lookup: {len(primary_lookup):,} (person, ticker) -> primary entries")

    # 3. Load DEF 14A Serper results, drop contamination rows
    def14a = pd.read_csv(DEF14A_URLS_PATH)
    before = len(def14a)
    def14a = def14a[def14a["search_status"].isin(VALID_SEARCH_STATUSES)].copy()
    print(f"\nLoaded {before:,} DEF 14A rows; {before - len(def14a):,} contamination rows filtered")
    print(f"  After filter: {len(def14a):,}")

    # 4. Build ticker -> board_company name lookup (from original URLs)
    ticker_to_board = (
        all_urls.dropna(subset=["ticker", "company_name"])
        .drop_duplicates("ticker")
        .set_index("ticker")["company_name"]
        .to_dict()
    )
    print(f"Built ticker -> board_company lookup: {len(ticker_to_board):,} tickers")

    # 5. Transform original URLs: rename company_name -> board_company; add primary_company column
    orig = all_urls.rename(columns={"company_name": "board_company"})
    orig["primary_company"] = orig.apply(
        lambda r: primary_lookup.get((normalize_name(r["person_name"]), r["ticker"])),
        axis=1,
    )
    orig["search_anchor_used"] = "board"
    n_orig_with_primary = orig["primary_company"].notna().sum()
    print(f"\nOriginal URLs: {len(orig):,} rows; {n_orig_with_primary:,} now have primary_company from DEF 14A lookup")

    # 6. Transform DEF 14A URLs into the same schema
    def14a_mapped = pd.DataFrame({
        "person_name": def14a["person_name"],
        "person_name_clean": def14a["person_name_clean"],
        "board_company": def14a["board_ticker"].map(ticker_to_board),
        "primary_company": def14a["company"],
        "company_name_clean": def14a["company_name_clean"],
        "position": def14a["role"],
        "source": "def14a_serper",
        "gvkey": pd.NA,
        "ticker": def14a["board_ticker"],
        "execid": pd.NA,
        "search_query": def14a["search_query"],
        "linkedin_url": def14a["linkedin_url"],
        "linkedin_title": def14a["linkedin_title"],
        "search_status": def14a["search_status"],
        "verified": def14a["verified"],
        "match_type": def14a["match_type"],
        "search_anchor_used": "primary",
    })
    n_def14a_missing_board = def14a_mapped["board_company"].isna().sum()
    print(f"DEF 14A URLs: {len(def14a_mapped):,} rows; {n_def14a_missing_board:,} have no board_company (ticker not in original URLs)")

    # 7. Combine - ensure same column order
    combined_cols = [
        "person_name", "person_name_clean",
        "board_company", "primary_company", "company_name_clean",
        "position", "source", "gvkey", "ticker", "execid",
        "search_query", "linkedin_url", "linkedin_title",
        "search_status", "verified", "match_type",
        "search_anchor_used",
    ]
    orig_subset = orig.reindex(columns=combined_cols)
    def14a_subset = def14a_mapped.reindex(columns=combined_cols)
    combined = pd.concat([orig_subset, def14a_subset], ignore_index=True)

    print(f"\nCombined: {len(combined):,} URL rows")
    print(f"\nBy search_anchor_used:")
    print(combined["search_anchor_used"].value_counts().to_string())
    print(f"\nBy source (top 10):")
    print(combined["source"].value_counts().head(10).to_string())
    print(f"\nCompany coverage:")
    print(f"  Has board_company:   {combined['board_company'].notna().sum():,}")
    print(f"  Has primary_company: {combined['primary_company'].notna().sum():,}")
    print(f"  Has BOTH:            {(combined['board_company'].notna() & combined['primary_company'].notna()).sum():,}")
    print(f"  Has NEITHER:         {(combined['board_company'].isna() & combined['primary_company'].isna()).sum():,}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\nWrote: {OUTPUT_PATH}")
    print(f"  {len(combined):,} rows, {combined.shape[1]} columns")

    return 0


if __name__ == "__main__":
    sys.exit(main())

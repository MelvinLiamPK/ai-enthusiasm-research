"""
Investigate URL Discovery Outliers — Companies with high verified profile counts.
==================================================================================
The URL discovery stats show max 54 verified profiles for one company.
This script identifies that company and breaks down WHY so many profiles exist.

Usage:
    python3 investigate_url_discovery_outliers.py [--top N] [--threshold T]

    --top N         Show top N companies by verified profile count (default: 10)
    --threshold T   Flag companies with more than T verified profiles (default: 30)

Expects:
    {PROJECT_ROOT}/data/processed/all_people_linkedin_urls/all_linkedin_urls.csv
"""

import pandas as pd
import argparse
from pathlib import Path

# ── Locate project root ──────────────────────────────────────────────
# Try env var first, then walk up from script location
import os

PROJECT_ROOT = Path(os.environ.get(
    "PROJECT_ROOT",
    Path(__file__).resolve().parent.parent.parent
))

URLS_FILE = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls.csv"
INPUT_FILE = PROJECT_ROOT / "data" / "extracted" / "combined" / "all_people.csv"


def load_urls():
    """Load the combined URL discovery results."""
    if not URLS_FILE.exists():
        print(f"✗ File not found: {URLS_FILE}")
        print(f"  Set PROJECT_ROOT env var or run from the project directory.")
        raise SystemExit(1)
    print(f"Loading: {URLS_FILE}")
    return pd.read_csv(URLS_FILE)


def investigate_company(df, company_name):
    """Deep-dive into a single company's verified profiles."""
    mask = (df["company_name_clean"] == company_name) & (df["verified"] == True)
    company_df = mask_df = df[mask].copy()

    print(f"\n{'='*70}")
    print(f"  DEEP DIVE: {company_name}")
    print(f"{'='*70}")

    # ── Basic counts ─────────────────────────────────────────────────
    all_rows = df[df["company_name_clean"] == company_name]
    print(f"\n  Total person-company pairs for this company: {len(all_rows)}")
    print(f"  URLs found:     {all_rows['linkedin_url'].notna().sum()}")
    print(f"  Verified:       {len(company_df)}")
    print(f"  Unique URLs:    {company_df['linkedin_url'].nunique()}")
    print(f"  Unique people:  {company_df['person_name_clean'].nunique()}")

    # ── Breakdown by source ──────────────────────────────────────────
    if "source" in company_df.columns:
        print(f"\n  By source:")
        for src, grp in company_df.groupby("source"):
            print(f"    {src}: {len(grp)} entries, {grp['person_name_clean'].nunique()} unique people")

    # ── Breakdown by position ────────────────────────────────────────
    if "position" in company_df.columns:
        print(f"\n  By position:")
        pos_counts = company_df["position"].value_counts()
        for pos, count in pos_counts.items():
            print(f"    {pos}: {count}")

    # ── Breakdown by match type ──────────────────────────────────────
    if "match_type" in company_df.columns:
        print(f"\n  By match type:")
        for mt, count in company_df["match_type"].value_counts().items():
            print(f"    {mt}: {count}")

    # ── Check for duplicate people (same person, different source) ───
    name_counts = company_df["person_name_clean"].value_counts()
    duplicates = name_counts[name_counts > 1]
    if len(duplicates) > 0:
        print(f"\n  ⚠ People appearing multiple times ({len(duplicates)} duplicates):")
        for name, count in duplicates.items():
            rows = company_df[company_df["person_name_clean"] == name]
            sources = rows["source"].tolist() if "source" in rows.columns else ["?"]
            urls = rows["linkedin_url"].unique().tolist()
            print(f"    {name} ({count}x): sources={sources}, unique URLs={len(urls)}")

    # ── Check for duplicate LinkedIn URLs ────────────────────────────
    url_counts = company_df["linkedin_url"].value_counts()
    dup_urls = url_counts[url_counts > 1]
    if len(dup_urls) > 0:
        print(f"\n  ⚠ LinkedIn URLs appearing multiple times ({len(dup_urls)}):")
        for url, count in dup_urls.head(10).items():
            people = company_df[company_df["linkedin_url"] == url]["person_name_clean"].tolist()
            print(f"    {url} ({count}x): {people}")

    # ── List all verified people ─────────────────────────────────────
    print(f"\n  All verified people ({len(company_df)}):")
    cols = ["person_name_clean"]
    if "position" in company_df.columns:
        cols.append("position")
    if "source" in company_df.columns:
        cols.append("source")
    if "match_type" in company_df.columns:
        cols.append("match_type")
    cols.append("linkedin_url")

    for i, (_, row) in enumerate(company_df[cols].iterrows(), 1):
        parts = [f"{row['person_name_clean']:<30}"]
        if "position" in cols:
            parts.append(f"pos={str(row.get('position', ''))[:30]:<30}")
        if "source" in cols:
            parts.append(f"src={str(row.get('source', ''))[:20]}")
        if "match_type" in cols:
            parts.append(f"match={row.get('match_type', '')}")
        print(f"    {i:>3}. {' | '.join(parts)}")


def main():
    parser = argparse.ArgumentParser(description="Investigate companies with high verified profile counts")
    parser.add_argument("--top", type=int, default=10, help="Show top N companies (default: 10)")
    parser.add_argument("--threshold", type=int, default=30, help="Flag companies above this count (default: 30)")
    parser.add_argument("--company", type=str, default=None, help="Investigate a specific company by name")
    args = parser.parse_args()

    df = load_urls()

    # Filter to verified only
    verified_df = df[df["verified"] == True]

    # ── Top companies by verified profile count ──────────────────────
    profiles_per_company = (
        verified_df.groupby("company_name_clean")
        .agg(
            verified_pairs=("linkedin_url", "size"),
            unique_urls=("linkedin_url", "nunique"),
            unique_people=("person_name_clean", "nunique"),
        )
        .sort_values("verified_pairs", ascending=False)
    )

    print(f"\n{'='*70}")
    print(f"  TOP {args.top} COMPANIES BY VERIFIED PROFILE COUNT")
    print(f"{'='*70}")
    print(f"\n  {'Company':<40} {'Pairs':>6} {'URLs':>6} {'People':>7}")
    print(f"  {'─'*40} {'─'*6} {'─'*6} {'─'*7}")

    for company, row in profiles_per_company.head(args.top).iterrows():
        label = str(company)[:40]
        print(f"  {label:<40} {row['verified_pairs']:>6} {row['unique_urls']:>6} {row['unique_people']:>7}")

    # ── Histogram of profile counts ──────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  DISTRIBUTION OF VERIFIED PROFILES PER COMPANY")
    print(f"{'='*70}")

    bins = [0, 1, 2, 5, 10, 15, 20, 30, 50, 100]
    labels = ["1", "2", "3-5", "6-10", "11-15", "16-20", "21-30", "31-50", "51+"]
    binned = pd.cut(profiles_per_company["unique_urls"], bins=bins, labels=labels, right=True)
    dist = binned.value_counts().sort_index()
    total_co = len(profiles_per_company)

    print(f"\n  {'Profiles':<12} {'Companies':>10} {'%':>7} {'Cumulative':>11}")
    print(f"  {'─'*12} {'─'*10} {'─'*7} {'─'*11}")
    cumulative = 0
    for label, count in dist.items():
        cumulative += count
        print(f"  {label:<12} {count:>10,} {100*count/total_co:>6.1f}% {100*cumulative/total_co:>10.1f}%")

    # ── Flag outliers ────────────────────────────────────────────────
    outliers = profiles_per_company[profiles_per_company["unique_urls"] > args.threshold]
    if len(outliers) > 0:
        print(f"\n{'='*70}")
        print(f"  ⚠ OUTLIERS: {len(outliers)} companies with >{args.threshold} unique verified URLs")
        print(f"{'='*70}")
        for company, row in outliers.iterrows():
            print(f"  {company}: {row['unique_urls']} URLs, {row['unique_people']} people, {row['verified_pairs']} pairs")

    # ── Deep dive into the max company (or specified company) ────────
    if args.company:
        investigate_company(df, args.company)
    else:
        # Auto-investigate the top company
        top_company = profiles_per_company.index[0]
        investigate_company(df, top_company)

    # ── Check: are blockholders driving the high counts? ─────────────
    if "source" in df.columns:
        print(f"\n{'='*70}")
        print(f"  SOURCE BREAKDOWN FOR TOP {min(5, args.top)} COMPANIES")
        print(f"{'='*70}")
        for company in profiles_per_company.head(5).index:
            company_verified = verified_df[verified_df["company_name_clean"] == company]
            source_counts = company_verified["source"].value_counts()
            print(f"\n  {company}:")
            for src, count in source_counts.items():
                print(f"    {src}: {count}")

    # ── Hypothesis: large companies have more ExecuComp entries ──────
    print(f"\n{'='*70}")
    print(f"  HYPOTHESIS CHECK: ARE HIGH COUNTS FROM MULTIPLE DATA SOURCES?")
    print(f"{'='*70}")

    top_company = profiles_per_company.index[0]
    top_all = df[df["company_name_clean"] == top_company]
    top_verified = verified_df[verified_df["company_name_clean"] == top_company]

    # Count unique people per source
    if "source" in df.columns:
        print(f"\n  For '{top_company}':")
        print(f"  Total rows in dataset:          {len(top_all)}")
        print(f"  Verified rows:                  {len(top_verified)}")
        print(f"  Unique verified people:         {top_verified['person_name_clean'].nunique()}")
        print(f"  Unique verified URLs:           {top_verified['linkedin_url'].nunique()}")

        # How many people appear in multiple sources?
        person_sources = top_verified.groupby("person_name_clean")["source"].nunique()
        multi_source = (person_sources > 1).sum()
        print(f"  People in multiple sources:     {multi_source}")
        print(f"  People in single source:        {(person_sources == 1).sum()}")


if __name__ == "__main__":
    main()
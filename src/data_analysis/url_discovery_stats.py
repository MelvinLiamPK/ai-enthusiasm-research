"""
LinkedIn URL Discovery — Descriptive Statistics
=================================================
Generates summary statistics and methodology notes for PI meeting.

Run after all batches complete and --combine has been run.

Usage:
    python3 src/data_analysis/url_discovery_stats.py

Outputs:
    data/processed/all_people_linkedin_urls/discovery_stats.txt
    data/processed/all_people_linkedin_urls/discovery_stats.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
from datetime import datetime

# Auto-detect project root
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

URLS_DIR = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls"
INPUT_FILE = PROJECT_ROOT / "data" / "extracted" / "combined" / "all_people.csv"


def load_data():
    """Load combined results. If not combined yet, combine from batches."""
    combined = URLS_DIR / "all_linkedin_urls.csv"

    if combined.exists():
        print(f"Loading: {combined}")
        return pd.read_csv(combined)

    # Try combining from batch files
    batch_files = sorted(URLS_DIR.glob("batch_*_urls.csv"))
    if not batch_files:
        print("✗ No results found. Run find_urls.py --combine first.")
        sys.exit(1)

    print(f"Combining {len(batch_files)} batch files...")
    dfs = [pd.read_csv(f) for f in batch_files]
    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(combined, index=False)
    print(f"  Saved: {combined}")
    return df


def print_section(title, file=None):
    """Print a section header."""
    line = "=" * 70
    print(f"\n{line}", file=file)
    print(f"  {title}", file=file)
    print(f"{line}", file=file)


def generate_stats(df):
    """Generate all descriptive statistics."""
    lines = []

    def p(text=""):
        print(text)
        lines.append(text)

    # ──────────────────────────────────────────
    # HEADER
    # ──────────────────────────────────────────
    p("=" * 70)
    p("  LINKEDIN URL DISCOVERY — DESCRIPTIVE STATISTICS")
    p(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    p("=" * 70)

    total = len(df)
    found = df["linkedin_url"].notna().sum()
    not_found = total - found
    verified = (df["verified"] == True).sum()
    unverified_with_url = found - verified

    # ──────────────────────────────────────────
    # OVERALL SUMMARY
    # ──────────────────────────────────────────
    p(f"\n{'─' * 70}")
    p("  1. OVERALL SUMMARY")
    p(f"{'─' * 70}")
    p(f"  Total person-company pairs searched:   {total:>8,}")
    p(f"  LinkedIn URL found:                    {found:>8,}  ({100*found/total:.1f}%)")
    p(f"    ├─ Verified (name matched):          {verified:>8,}  ({100*verified/total:.1f}% of total, {100*verified/found:.1f}% of found)")
    p(f"    └─ Unverified (URL but no match):    {unverified_with_url:>8,}  ({100*unverified_with_url/found:.1f}% of found)")
    p(f"  No LinkedIn URL found:                 {not_found:>8,}  ({100*not_found/total:.1f}%)")
    p(f"")
    p(f"  Unique people (by name):               {df['person_name_clean'].nunique():>8,}")
    p(f"  Unique companies:                      {df['company_name_clean'].nunique():>8,}")

    # ──────────────────────────────────────────
    # BREAKDOWN BY SOURCE
    # ──────────────────────────────────────────
    p(f"\n{'─' * 70}")
    p("  2. BREAKDOWN BY SOURCE")
    p(f"{'─' * 70}")

    if "source" in df.columns:
        # Simplify source labels
        def simplify_source(s):
            if pd.isna(s):
                return "unknown"
            s = str(s)
            if "executive" in s and "director" in s:
                return "director+executive"
            elif "executive" in s:
                return "executive"
            elif "director" in s:
                return "director"
            elif "blockholder" in s:
                return "blockholder"
            return s

        df["source_simple"] = df["source"].apply(simplify_source)

        p(f"\n  {'Source':<22} {'Total':>8} {'Found':>8} {'Rate':>7} {'Verified':>10} {'V.Rate':>7}")
        p(f"  {'─'*22} {'─'*8} {'─'*8} {'─'*7} {'─'*10} {'─'*7}")

        source_stats = []
        for source in ["executive", "director+executive", "director", "blockholder"]:
            mask = df["source_simple"] == source
            if mask.sum() == 0:
                continue
            sub = df[mask]
            n = len(sub)
            f_ = sub["linkedin_url"].notna().sum()
            v = (sub["verified"] == True).sum()
            rate = 100 * f_ / n if n > 0 else 0
            vrate = 100 * v / f_ if f_ > 0 else 0
            p(f"  {source:<22} {n:>8,} {f_:>8,} {rate:>6.1f}% {v:>10,} {vrate:>6.1f}%")
            source_stats.append({
                "source": source, "total": n, "found": f_,
                "found_rate": round(rate, 1), "verified": v,
                "verified_rate": round(vrate, 1)
            })

        p(f"  {'─'*22} {'─'*8} {'─'*8} {'─'*7} {'─'*10} {'─'*7}")
        p(f"  {'TOTAL':<22} {total:>8,} {found:>8,} {100*found/total:>6.1f}% {verified:>10,} {100*verified/found:>6.1f}%")
    else:
        p("  [source column not found in data]")
        source_stats = []

    # ──────────────────────────────────────────
    # VERIFICATION MATCH TYPES
    # ──────────────────────────────────────────
    p(f"\n{'─' * 70}")
    p("  3. VERIFICATION MATCH TYPES (among found URLs)")
    p(f"{'─' * 70}")

    if "match_type" in df.columns:
        found_df = df[df["linkedin_url"].notna()]
        match_counts = found_df["match_type"].value_counts()
        p(f"\n  {'Match Type':<20} {'Count':>8} {'% of Found':>12}")
        p(f"  {'─'*20} {'─'*8} {'─'*12}")
        for mt in ["both", "first_name", "last_name", "none"]:
            if mt in match_counts.index:
                c = match_counts[mt]
                p(f"  {mt:<20} {c:>8,} {100*c/found:>11.1f}%")
        p(f"\n  'both' = first AND last name found in LinkedIn title")
        p(f"  'first_name' or 'last_name' = partial match (includes nicknames)")
        p(f"  'none' = URL found but name not in title (unverified)")

    # ──────────────────────────────────────────
    # POSITION BREAKDOWN (if available)
    # ──────────────────────────────────────────
    if "position" in df.columns:
        p(f"\n{'─' * 70}")
        p("  4. BREAKDOWN BY POSITION")
        p(f"{'─' * 70}")

        # Top 15 positions
        pos_stats = df.groupby("position").agg(
            total=("linkedin_url", "size"),
            found=("linkedin_url", lambda x: x.notna().sum()),
            verified=("verified", lambda x: (x == True).sum())
        ).sort_values("total", ascending=False).head(15)

        pos_stats["found_rate"] = (100 * pos_stats["found"] / pos_stats["total"]).round(1)

        p(f"\n  {'Position':<40} {'Total':>7} {'Found':>7} {'Rate':>6}")
        p(f"  {'─'*40} {'─'*7} {'─'*7} {'─'*6}")
        for pos, row in pos_stats.iterrows():
            label = str(pos)[:40]
            p(f"  {label:<40} {row['total']:>7,} {row['found']:>7,} {row['found_rate']:>5.1f}%")

    # ──────────────────────────────────────────
    # UNIQUE PEOPLE WITH VERIFIED URLS
    # ──────────────────────────────────────────
    p(f"\n{'─' * 70}")
    p("  5. UNIQUE PEOPLE WITH VERIFIED LINKEDIN PROFILES")
    p(f"{'─' * 70}")

    verified_df = df[df["verified"] == True]
    unique_verified_urls = verified_df["linkedin_url"].nunique()
    unique_verified_people = verified_df["person_name_clean"].nunique()

    p(f"\n  Verified URL-person pairs:              {len(verified_df):>8,}")
    p(f"  Unique verified LinkedIn URLs:           {unique_verified_urls:>8,}")
    p(f"  Unique verified people (by name):        {unique_verified_people:>8,}")
    p(f"")
    p(f"  Note: Some people appear multiple times (multiple board seats)")
    p(f"  For scraping, deduplicate on LinkedIn URL to avoid redundant API calls")

    # Count how many posts we'd scrape
    p(f"\n  Estimated scraping scope:")
    p(f"    Unique URLs to scrape:               {unique_verified_urls:>8,}")
    p(f"    Posts per profile (target):                  200")
    p(f"    Maximum posts to collect:         {unique_verified_urls * 200:>10,}")

    # ──────────────────────────────────────────
    # COMPANY COVERAGE
    # ──────────────────────────────────────────
    p(f"\n{'─' * 70}")
    p("  6. COMPANY COVERAGE")
    p(f"{'─' * 70}")

    total_companies = df["company_name_clean"].nunique()
    companies_with_verified = verified_df["company_name_clean"].nunique()

    p(f"\n  Total companies in dataset:             {total_companies:>8,}")
    p(f"  Companies with ≥1 verified profile:     {companies_with_verified:>8,}  ({100*companies_with_verified/total_companies:.1f}%)")

    # Distribution of verified profiles per company
    profiles_per_company = verified_df.groupby("company_name_clean")["linkedin_url"].nunique()
    p(f"\n  Verified profiles per company:")
    p(f"    Mean:   {profiles_per_company.mean():.1f}")
    p(f"    Median: {profiles_per_company.median():.0f}")
    p(f"    Max:    {profiles_per_company.max()}")
    p(f"    Min:    {profiles_per_company.min()}")

    # Companies with gvkey (ExecuComp) vs without
    if "gvkey" in df.columns:
        has_gvkey = df["gvkey"].notna()
        p(f"\n  ExecuComp companies (have gvkey):       {df[has_gvkey]['company_name_clean'].nunique():>8,}")
        p(f"  Non-ExecuComp (blockholders only):      {df[~has_gvkey]['company_name_clean'].nunique():>8,}")

        execucomp_verified = verified_df[verified_df["gvkey"].notna()]["company_name_clean"].nunique()
        p(f"  ExecuComp with ≥1 verified profile:     {execucomp_verified:>8,}")

    # ──────────────────────────────────────────
    # SEARCH STATUS BREAKDOWN
    # ──────────────────────────────────────────
    p(f"\n{'─' * 70}")
    p("  7. SEARCH STATUS BREAKDOWN")
    p(f"{'─' * 70}")

    if "search_status" in df.columns:
        status_counts = df["search_status"].value_counts()
        p(f"\n  {'Status':<25} {'Count':>8} {'%':>7}")
        p(f"  {'─'*25} {'─'*8} {'─'*7}")
        for status, count in status_counts.items():
            p(f"  {str(status):<25} {count:>8,} {100*count/total:>6.1f}%")

    # ──────────────────────────────────────────
    # METHODOLOGY NOTES
    # ──────────────────────────────────────────
    p(f"\n{'─' * 70}")
    p("  8. METHODOLOGY NOTES")
    p(f"{'─' * 70}")
    p("""
  Data Sources:
    - Directors:    WRDS ExecuComp directorcomp (2010–2025)
    - Executives:   WRDS ExecuComp anncomp (2010–2025), top 5 compensated
    - Blockholders: Schwartz-Ziv/Volkova SEC 13D/13G/13F (2010–2023)

  LinkedIn URL Discovery:
    - Tool: Google Custom Search JSON API ($5 per 1,000 queries)
    - Query format: "{person_name} {company_name} site:linkedin.com/in/"
    - Top 5 Google results examined per query
    - Name cleaning: removed credentials (PhD, MBA, CFA, etc.),
      standardised SEC all-caps format, preserved suffixes (Jr, III)
    - Company cleaning: stripped Inc, Corp, LLC, etc.

  Name Verification:
    - Method: deterministic word-boundary regex matching
    - Matches person's first name (or nickname variant) and/or last name
      against the LinkedIn profile title returned by Google
    - Nickname dictionary covers 60+ common name variations
      (Timothy↔Tim, Robert↔Bob, William↔Bill, etc.)
    - A URL is "verified" if ANY name part matches (first OR last)
    - Match types: "both" (first+last), "first_name", "last_name", "none"
    - No machine learning or fuzzy matching — fully reproducible

  What "Verified" Means:
    - Verified = person's name appears in the LinkedIn result title
    - This is a NECESSARY but not SUFFICIENT condition for correctness
    - Common names (e.g. "John Smith") may verify against wrong person
    - Rare names (e.g. "Raghunandan Sagi") are near-certain matches
    - Downstream: can apply stricter filters (require "both" match type)

  What "Unverified" Means:
    - URL found but name not in Google's title snippet
    - Could be: wrong person, or correct person with abbreviated title
    - Conservative approach: exclude from scraping by default
    - ~26% of found URLs are unverified (likely mix of wrong + truncated)

  Limitations:
    - Google result quality varies by name uniqueness and company size
    - Directors are harder to find (LinkedIn shows primary job, not boards)
    - Blockholders at small companies have lowest discovery rates
    - ~28,000 potential duplicates from director-executive name mismatches
      (same person searched twice — wastes queries but doesn't corrupt data)
    - No ground truth for verification accuracy (would need manual audit)

  Robustness Checks Available:
    - Restrict to "both" match type only (stricter verification)
    - Analyse by source (executive vs director vs blockholder separately)
    - Compare ExecuComp vs non-ExecuComp companies
    - Manual spot-check of verified vs unverified samples
""")

    return lines, source_stats


def main():
    df = load_data()
    lines, source_stats = generate_stats(df)

    # Save text report
    output_txt = URLS_DIR / "discovery_stats.txt"
    with open(output_txt, "w") as f:
        f.write("\n".join(lines))
    print(f"\n✓ Saved report: {output_txt}")

    # Save source stats CSV for easy charting
    if source_stats:
        stats_df = pd.DataFrame(source_stats)
        output_csv = URLS_DIR / "discovery_stats_by_source.csv"
        stats_df.to_csv(output_csv, index=False)
        print(f"✓ Saved CSV:    {output_csv}")


if __name__ == "__main__":
    main()

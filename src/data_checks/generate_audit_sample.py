"""
Generate Stratified Audit Sample for Manual LinkedIn Verification
=================================================================

Draws a random stratified sample from verified LinkedIn URLs for manual
checking. Each sampled profile includes the LinkedIn URL to open, the
expected person name and company, and a blank column for the auditor to
mark correct/incorrect.

Strata:
    1. both-match executives   (first + last name matched, source = executive)
    2. both-match directors    (first + last name matched, source = director)
    3. first-name-only matches (any source)
    4. last-name-only matches  (any source)

Usage:
    python3 generate_audit_sample.py                     # 125 per stratum (500 total)
    python3 generate_audit_sample.py --per-stratum 50    # 50 per stratum (200 total)
    python3 generate_audit_sample.py --seed 123          # reproducible sample

Output:
    data/audit/audit_sample.csv
"""

import argparse
import sys
import pandas as pd
from pathlib import Path

# Resolve project root from script location (src/data_checks/../../)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Input: try data/urls/ first, then project root
URLS_CANDIDATES = [
    PROJECT_ROOT / "data" / "urls" / "all_linkedin_urls.csv",
    PROJECT_ROOT / "all_linkedin_urls.csv",
]


def find_input():
    for path in URLS_CANDIDATES:
        if path.exists():
            return path
    print("✗ Could not find all_linkedin_urls.csv")
    print(f"  Looked in:")
    for p in URLS_CANDIDATES:
        print(f"    {p}")
    sys.exit(1)


def classify_source(source_str):
    """Simplify source to executive / director / blockholder."""
    if pd.isna(source_str):
        return "unknown"
    s = str(source_str).lower()
    # director|executive → treat as executive (they have executive-level visibility)
    if "executive" in s:
        return "executive"
    if "director" in s:
        return "director"
    if "blockholder" in s:
        return "blockholder"
    return "other"


def main():
    parser = argparse.ArgumentParser(
        description="Generate stratified audit sample for manual LinkedIn verification"
    )
    parser.add_argument(
        "--per-stratum", type=int, default=125,
        help="Number of profiles to sample per stratum (default: 125)"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Path to all_linkedin_urls.csv (auto-detected if not specified)"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output path for audit sample CSV (default: data/audit/audit_sample.csv)"
    )
    args = parser.parse_args()

    # --- Load data ---
    input_path = Path(args.input) if args.input else find_input()
    print(f"Loading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"  {len(df):,} rows")

    # --- Filter to verified with URL ---
    df["verified"] = df["verified"].astype(str).str.strip().str.lower() == "true"
    df = df[df["verified"] & df["linkedin_url"].notna()].copy()
    print(f"  {len(df):,} verified with URL")

    # --- Deduplicate on LinkedIn URL (one check per profile) ---
    before = len(df)
    df = df.drop_duplicates(subset="linkedin_url", keep="first")
    print(f"  {len(df):,} unique URLs (dropped {before - len(df):,} duplicates)")

    # --- Classify source ---
    df["source_simple"] = df["source"].apply(classify_source)

    # --- Define strata ---
    strata = {
        "both_executive": df[
            (df["match_type"] == "both") & (df["source_simple"] == "executive")
        ],
        "both_director": df[
            (df["match_type"] == "both") & (df["source_simple"] == "director")
        ],
        "first_name_only": df[df["match_type"] == "first_name"],
        "last_name_only": df[df["match_type"] == "last_name"],
    }

    # --- Report stratum sizes ---
    print(f"\nStratum sizes:")
    for name, sdf in strata.items():
        print(f"  {name:<20} {len(sdf):>7,}")

    # --- Sample ---
    n = args.per_stratum
    samples = []

    for name, sdf in strata.items():
        available = len(sdf)
        take = min(n, available)
        if take < n:
            print(f"\n  ⚠ {name}: only {available} available, sampling all")
        sample = sdf.sample(n=take, random_state=args.seed)
        sample = sample.copy()
        sample["stratum"] = name
        samples.append(sample)

    combined = pd.concat(samples, ignore_index=True)

    # --- Build output ---
    output_cols = [
        "stratum",
        "linkedin_url",
        "person_name",
        "person_name_clean",
        "company_name",
        "source",
        "match_type",
        "linkedin_title",
        "correct",          # blank — for auditor to fill in
        "auditor_notes",    # blank — for auditor notes
    ]

    # Only include columns that exist
    available_cols = [c for c in output_cols if c in combined.columns or c in ("correct", "auditor_notes")]
    combined["correct"] = ""
    combined["auditor_notes"] = ""
    output_df = combined[[c for c in output_cols if c in combined.columns]].copy()

    # Shuffle so auditor doesn't see all one stratum at a time
    output_df = output_df.sample(frac=1, random_state=args.seed).reset_index(drop=True)

    # --- Save ---
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = PROJECT_ROOT / "data" / "audit" / "audit_sample.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_path, index=False)

    total = len(output_df)
    print(f"\n✓ Saved {total} profiles to: {output_path}")
    print(f"  ({args.per_stratum} per stratum × {len(strata)} strata)")
    print(f"\n  Columns for auditor to fill in:")
    print(f"    correct        — Y/N: is this the right person?")
    print(f"    auditor_notes  — optional notes (e.g. 'wrong company', 'different person')")


if __name__ == "__main__":
    main()

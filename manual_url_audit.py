#!/usr/bin/env python3
"""
manual_url_audit.py — Manually audit a sample of LinkedIn URLs.

Samples N verified and N unverified URLs, opens each in the browser,
and records whether the profile matches the expected person.

Usage:
    python manual_url_audit.py                          # defaults: 50 each
    python manual_url_audit.py --n 25                   # 25 each
    python manual_url_audit.py --only unverified        # only unverified
    python manual_url_audit.py --resume                 # resume interrupted session

Outputs:
    audit_results_YYYYMMDD_HHMMSS.csv  in the same directory as the input file
"""

import argparse
import os
import sys
import webbrowser
import pandas as pd
from datetime import datetime
from pathlib import Path

# ── defaults ────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls.csv"
DEFAULT_N = 50
SEED = 42


def load_and_sample(input_path: Path, n: int, group: str, seed: int) -> pd.DataFrame:
    """Load CSV and return a random sample for the requested group(s)."""
    df = pd.read_csv(input_path)

    # Only rows that actually have a LinkedIn URL
    df = df[df["linkedin_url"].notna() & (df["linkedin_url"] != "")]

    # Exclude corporate entities — their URLs are always wrong matches
    if "is_entity" in df.columns:
        before = len(df)
        df = df[df["is_entity"] != True]  # noqa: E712
        excluded = before - len(df)
        if excluded:
            print(f"Excluded {excluded} corporate entity rows (is_entity=True)")

    samples = []

    if group in ("verified", "both"):
        verified = df[df["verified"] == True]  # noqa: E712
        if len(verified) == 0:
            # Try string match in case the column is stored as string
            verified = df[df["verified"].astype(str).str.lower() == "true"]
        n_v = min(n, len(verified))
        samples.append(verified.sample(n=n_v, random_state=seed).assign(audit_group="verified"))
        print(f"Sampled {n_v} verified URLs (from {len(verified)} total)")

    if group in ("unverified", "both"):
        unverified = df[df["verified"].astype(str).str.lower() == "false"]
        if len(unverified) == 0:
            # Also try match_type == "none"
            unverified = df[df["match_type"] == "none"]
        n_u = min(n, len(unverified))
        samples.append(unverified.sample(n=n_u, random_state=seed).assign(audit_group="unverified"))
        print(f"Sampled {n_u} unverified URLs (from {len(unverified)} total)")

    if not samples:
        print("ERROR: No URLs found for the requested group(s).")
        sys.exit(1)

    return pd.concat(samples, ignore_index=True)


def run_audit(sample: pd.DataFrame, output_path: Path, start_idx: int = 0):
    """Interactive audit loop: show info, open browser, record judgment."""

    results = []
    total = len(sample)

    print("\n" + "=" * 70)
    print("MANUAL LINKEDIN URL AUDIT")
    print("=" * 70)
    print(f"Total URLs to review: {total} (starting from #{start_idx + 1})")
    print()
    print("For each URL, a browser tab will open. Then enter your judgment:")
    print("  y = correct match (profile is the right person)")
    print("  n = wrong person")
    print("  m = maybe / uncertain")
    print("  s = skip (couldn't load / login wall / etc.)")
    print("  q = quit and save progress")
    print("=" * 70)
    print()

    for i in range(start_idx, total):
        row = sample.iloc[i]

        print(f"\n── [{i + 1}/{total}] {'─' * 50}")
        print(f"  Group:       {row['audit_group']}")
        print(f"  Expected:    {row['person_name']}")
        print(f"  Company:     {row['company_name']}")
        print(f"  Position:    {row.get('position', 'N/A')}")
        print(f"  Match type:  {row.get('match_type', 'N/A')}")
        print(f"  LI title:    {row.get('linkedin_title', 'N/A')}")
        print(f"  URL:         {row['linkedin_url']}")

        # Open in default browser
        try:
            webbrowser.open(row["linkedin_url"])
        except Exception as e:
            print(f"  (Could not open browser: {e})")

        # Get judgment
        while True:
            judgment = input("  Judgment [y/n/m/s/q]: ").strip().lower()
            if judgment in ("y", "n", "m", "s", "q"):
                break
            print("  Invalid input. Enter y, n, m, s, or q.")

        if judgment == "q":
            print(f"\nSaving progress ({i} of {total} reviewed)...")
            # Save what we have so far
            _save_results(sample, results, output_path, completed=i)
            print(f"Saved to: {output_path}")
            print(f"To resume: python {__file__} --resume --output {output_path}")
            return

        label_map = {"y": "correct", "n": "wrong_person", "m": "uncertain", "s": "skipped"}
        results.append({
            "audit_index": i,
            "judgment": label_map[judgment],
        })

    # All done
    _save_results(sample, results, output_path, completed=total)
    _print_summary(output_path)


def _save_results(sample: pd.DataFrame, results: list, output_path: Path, completed: int):
    """Merge judgments into the sample dataframe and save."""
    out = sample.copy()
    out["judgment"] = ""
    out["audited"] = False

    for r in results:
        idx = r["audit_index"]
        out.loc[idx, "judgment"] = r["judgment"]
        out.loc[idx, "audited"] = True

    # Keep useful columns in a clean order
    keep_cols = [
        "audit_group", "person_name", "company_name", "position",
        "match_type", "linkedin_url", "linkedin_title",
        "judgment", "audited",
        # Preserve IDs for merging back
        "gvkey", "ticker", "execid", "source",
    ]
    keep_cols = [c for c in keep_cols if c in out.columns]
    out = out[keep_cols]

    out.to_csv(output_path, index=False)


def _print_summary(output_path: Path):
    """Print audit summary statistics."""
    df = pd.read_csv(output_path)
    audited = df[df["audited"] == True]  # noqa

    print("\n" + "=" * 70)
    print("AUDIT COMPLETE — SUMMARY")
    print("=" * 70)

    for group in audited["audit_group"].unique():
        g = audited[audited["audit_group"] == group]
        total = len(g)
        correct = (g["judgment"] == "correct").sum()
        wrong = (g["judgment"] == "wrong_person").sum()
        uncertain = (g["judgment"] == "uncertain").sum()
        skipped = (g["judgment"] == "skipped").sum()
        reviewed = total - skipped

        print(f"\n  {group.upper()} ({total} sampled, {reviewed} reviewed):")
        if reviewed > 0:
            print(f"    Correct:     {correct:3d}  ({100 * correct / reviewed:.1f}%)")
            print(f"    Wrong:       {wrong:3d}  ({100 * wrong / reviewed:.1f}%)")
            print(f"    Uncertain:   {uncertain:3d}  ({100 * uncertain / reviewed:.1f}%)")
        print(f"    Skipped:     {skipped:3d}")

    print(f"\nResults saved to: {output_path}")
    print("=" * 70)


def resume_audit(output_path: Path):
    """Resume an interrupted audit session."""
    if not output_path.exists():
        print(f"ERROR: Resume file not found: {output_path}")
        sys.exit(1)

    df = pd.read_csv(output_path)
    already_done = df["audited"].sum() if "audited" in df.columns else 0
    print(f"Resuming audit from #{already_done + 1} (of {len(df)} total)")

    # Rebuild the sample dataframe (full set including unaudited)
    # Re-run the audit starting from where we left off
    existing_results = []
    for i, row in df.iterrows():
        if row.get("audited", False):
            existing_results.append({
                "audit_index": i,
                "judgment": row["judgment"],
            })

    run_audit(df, output_path, start_idx=int(already_done))


def main():
    parser = argparse.ArgumentParser(description="Manually audit a sample of LinkedIn URLs")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT),
                        help="Path to all_linkedin_urls.csv")
    parser.add_argument("--n", type=int, default=DEFAULT_N,
                        help=f"Number of URLs to sample per group (default: {DEFAULT_N})")
    parser.add_argument("--only", choices=["verified", "unverified", "both"], default="both",
                        help="Which group(s) to audit (default: both)")
    parser.add_argument("--seed", type=int, default=SEED,
                        help=f"Random seed for reproducible sampling (default: {SEED})")
    parser.add_argument("--output", type=str, default=None,
                        help="Output CSV path (default: auto-generated in same dir as input)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume an interrupted audit (requires --output)")

    args = parser.parse_args()
    input_path = Path(args.input).resolve()

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        sys.exit(1)

    # Output path
    if args.output:
        output_path = Path(args.output).resolve()
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = input_path.parent / f"audit_results_{timestamp}.csv"

    if args.resume:
        resume_audit(output_path)
        return

    # Sample and run
    sample = load_and_sample(input_path, args.n, args.only, args.seed)
    print(f"\nOutput will be saved to: {output_path}")
    run_audit(sample, output_path)


if __name__ == "__main__":
    main()

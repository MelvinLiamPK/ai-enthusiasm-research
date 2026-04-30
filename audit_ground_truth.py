#!/usr/bin/env python3
"""
audit_ground_truth.py — Classify wrong-person results as truly missing vs algorithm failure.

Opens LinkedIn search for each case. Enter judgment: c/w/m/u/s/q.
Progress saves automatically. Use --resume to continue.

Usage:
    python audit_ground_truth.py
    python audit_ground_truth.py --resume
"""

import pandas as pd
import webbrowser
import sys
from pathlib import Path
from urllib.parse import quote_plus

FULL_RESULTS = Path("data/processed/test_serper_full_audit_linkedin_urls/batch_000_urls.csv")
SERPER_AUDIT = Path("data/serper_audit_results.csv")
ALL_PEOPLE = Path("data/extracted/combined/all_people.csv")
OUTPUT = Path("data/ground_truth_audit_results.csv")


def build_audit_df():
    """Build the 31 non-correct cases with all relevant info."""
    full = pd.read_csv(FULL_RESULTS)
    serper_audit = pd.read_csv(SERPER_AUDIT)
    people = pd.read_csv(ALL_PEOPLE, usecols=["person_name", "company_name", "source", "position"])

    # Deduplicate people (keep first match per person+company)
    people = people.drop_duplicates(subset=["person_name", "company_name"], keep="first")

    # Merge serper_judgment into full results
    serper_cols = serper_audit[["person_name", "serper_judgment"]].copy()
    df = full.merge(serper_cols, on="person_name", how="left")

    # For matching-URL cases (not in serper audit), use google_cse_judgment
    df["serper_judgment"] = df["serper_judgment"].fillna(df["google_cse_judgment"])

    # Filter to non-correct cases only
    df = df[df["serper_judgment"] != "correct"].copy()

    # Add source/position from all_people
    df = df.merge(people, on=["person_name", "company_name"], how="left")
    df["source"] = df["source"].fillna("unknown")
    df["position"] = df["position"].fillna("")

    # Add audit columns
    df["ground_truth"] = ""
    df["gt_audited"] = False

    df = df.reset_index(drop=True)
    return df


def main():
    resume = "--resume" in sys.argv

    if resume and OUTPUT.exists():
        df = pd.read_csv(OUTPUT)
        start = int(df["gt_audited"].sum())
        print(f"Resuming from #{start + 1} of {len(df)}")
    else:
        df = build_audit_df()
        start = 0
        print(f"{len(df)} cases to classify\n")

    print("Keys: [c] correct  [w] wrong person (real profile exists)")
    print("       [m] truly missing (no LinkedIn)  [u] uncertain  [s] skip  [q] quit\n")

    for i in range(start, len(df)):
        row = df.iloc[i]
        print(f"\n── [{i+1}/{len(df)}] ──────────────────────────────────────")
        print(f"  Person:   {row['person_name']}")
        print(f"  Company:  {row['company_name']}")
        print(f"  Source:   {row['source']}  |  Position: {row['position']}")
        print(f"  Serper:   {row.get('linkedin_url', 'N/A')}")
        print(f"  Title:    {row.get('linkedin_title', 'N/A')}  |  Score: {row.get('score', 'N/A')}")
        print(f"  Google:   {row['google_cse_url']} ({row['google_cse_judgment']})")
        print(f"  Previous: {row.get('serper_judgment', '')}")

        # Open Google search
        name_clean = row.get("person_name_clean", row["person_name"])
        position = row.get("position", "")
        search_query = quote_plus(f"{name_clean} {row['company_name']} {position} linkedin")
        search_url = f"https://www.google.com/search?q={search_query}"
        webbrowser.open(search_url)

        while True:
            j = input("  Ground truth [c/w/m/u/s/q]: ").strip().lower()
            if j in ("c", "w", "m", "u", "s", "q"):
                break
            print("  Invalid. Enter c, w, m, u, s, or q.")

        if j == "q":
            df.to_csv(OUTPUT, index=False)
            print(f"\nSaved progress ({i} of {len(df)} done) → {OUTPUT}")
            print("Run with --resume to continue.")
            return

        label = {"c": "correct", "w": "wrong_person", "m": "truly_missing", "u": "uncertain", "s": "skipped"}[j]
        df.at[i, "ground_truth"] = label
        df.at[i, "gt_audited"] = True

        # Auto-save every 5
        if (i + 1) % 5 == 0:
            df.to_csv(OUTPUT, index=False)

    df.to_csv(OUTPUT, index=False)

    # Summary
    audited = df[df["gt_audited"] == True]  # noqa
    total = len(audited)
    for label in ["correct", "wrong_person", "truly_missing", "uncertain", "skipped"]:
        count = (audited["ground_truth"] == label).sum()
        if count > 0:
            print(f"  {label:15s} {count:3d} ({100*count/total:.0f}%)")
    print(f"\nTotal: {total} classified → {OUTPUT}")


if __name__ == "__main__":
    main()

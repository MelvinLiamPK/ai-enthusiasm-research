#!/usr/bin/env python3
"""
audit_serper_urls.py — Manually verify Serper URLs that differ from Google CSE.

Opens each URL in browser. Enter judgment: y/n/m/s/q.
Progress saves automatically. Use --resume to continue.

Usage:
    python audit_serper_urls.py
    python audit_serper_urls.py --resume
"""

import pandas as pd
import webbrowser
import sys
from pathlib import Path

INPUT = Path("data/serper_audit_needed.csv")
OUTPUT = Path("data/serper_audit_results.csv")


def main():
    resume = "--resume" in sys.argv

    if resume and OUTPUT.exists():
        df = pd.read_csv(OUTPUT)
        start = int(df["audited"].sum())
        print(f"Resuming from #{start + 1} of {len(df)}")
    else:
        df = pd.read_csv(INPUT)
        df["serper_judgment"] = ""
        df["audited"] = False
        start = 0
        print(f"{len(df)} URLs to verify\n")

    print("Keys: [y] correct  [n] wrong  [m] maybe  [s] skip  [q] quit\n")

    for i in range(start, len(df)):
        row = df.iloc[i]
        print(f"\n── [{i+1}/{len(df)}] ──────────────────────────────────────")
        print(f"  Person:  {row['person_name']}")
        print(f"  Company: {row['company_name']}")
        print(f"  Score:   {row['score']} | Match: {row['match_type']}")
        print(f"  Title:   {row['linkedin_title']}")
        print(f"  Serper:  {row['linkedin_url']}")
        print(f"  Google:  {row['google_cse_url']} ({row['google_cse_judgment']})")

        webbrowser.open(row["linkedin_url"])

        while True:
            j = input("  Judgment [y/n/m/s/q]: ").strip().lower()
            if j in ("y", "n", "m", "s", "q"):
                break
            print("  Invalid. Enter y, n, m, s, or q.")

        if j == "q":
            df.to_csv(OUTPUT, index=False)
            print(f"\nSaved progress ({i} of {len(df)} done) → {OUTPUT}")
            print("Run with --resume to continue.")
            return

        label = {"y": "correct", "n": "wrong_person", "m": "uncertain", "s": "skipped"}[j]
        df.at[i, "serper_judgment"] = label
        df.at[i, "audited"] = True

        # Auto-save every 5
        if (i + 1) % 5 == 0:
            df.to_csv(OUTPUT, index=False)

    df.to_csv(OUTPUT, index=False)

    # Summary
    audited = df[df["audited"] == True]  # noqa
    correct = (audited["serper_judgment"] == "correct").sum()
    wrong = (audited["serper_judgment"] == "wrong_person").sum()
    maybe = (audited["serper_judgment"] == "uncertain").sum()
    total = len(audited)
    print(f"\n{'='*50}")
    print(f"DONE — {total} reviewed")
    print(f"  Correct:   {correct} ({100*correct/total:.0f}%)")
    print(f"  Wrong:     {wrong} ({100*wrong/total:.0f}%)")
    print(f"  Uncertain: {maybe} ({100*maybe/total:.0f}%)")
    print(f"Saved → {OUTPUT}")


if __name__ == "__main__":
    main()

"""
Build Serper input CSV for unmatched directors using DEF 14A primary employment.

Matches extracted DEF 14A bios to tracked persons, then creates input for
Serper re-search on unmatched directors using primary_company as anchor.

Usage:
    python3 src/data_collection/build_def14a_serper_input_full.py
"""

import pandas as pd
from pathlib import Path
import difflib

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

BIOS_PATH = PROJECT_ROOT / "data" / "processed" / "def14a_extracted_bios.csv"
PEOPLE_PATH = PROJECT_ROOT / "data" / "extracted" / "combined" / "all_people.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "def14a_serper_input_unmatched.csv"


def fuzzy_match_name(bio_name, person_names, threshold=0.85):
    """Find best matching person by name. Return (person_id, match_score) or (None, 0)."""
    if not bio_name or not isinstance(bio_name, str):
        return None, 0

    bio_name_lower = bio_name.lower().strip()
    best_match = None
    best_score = 0

    for person_id, pname in person_names.items():
        if not isinstance(pname, str):
            continue
        pname_lower = pname.lower().strip()
        score = difflib.SequenceMatcher(None, bio_name_lower, pname_lower).ratio()
        if score > best_score:
            best_score = score
            best_match = person_id

    if best_score >= threshold:
        return best_match, best_score
    return None, 0


def main():
    print("Building DEF 14A Serper input for unmatched directors...")

    if not BIOS_PATH.exists():
        print(f"ERROR: {BIOS_PATH} not found. Run parse_def14a_bios.py first.")
        return 1

    if not PEOPLE_PATH.exists():
        print(f"ERROR: {PEOPLE_PATH} not found.")
        return 1

    # Load data
    bios_df = pd.read_csv(BIOS_PATH)
    people_df = pd.read_csv(PEOPLE_PATH)

    print(f"  Loaded {len(bios_df):,} extracted bios")
    print(f"  Loaded {len(people_df):,} tracked persons")

    # Create name lookup from people
    person_names = dict(zip(people_df["person_id"], people_df["name"]))

    # Filter bios for directors only (exclude other roles)
    bios_df = bios_df[bios_df["role_context"] == "director"].copy()
    print(f"  Filtered to {len(bios_df):,} directors")

    # Match bios to persons
    matches = []
    for _, bio in bios_df.iterrows():
        person_id, score = fuzzy_match_name(bio["full_name"], person_names)
        matches.append({"bio_idx": _, "person_id": person_id, "match_score": score})

    bios_df["person_id"] = [m["person_id"] for m in matches]
    bios_df["match_score"] = [m["match_score"] for m in matches]

    # Filter for unmatched (no person_id found)
    unmatched = bios_df[bios_df["person_id"].isna()].copy()
    print(f"  Found {len(unmatched):,} unmatched directors")

    # Build Serper input: use primary_company, fall back to ticker if missing
    serper_input = unmatched[[
        "ticker", "full_name", "primary_company", "primary_role", "is_current"
    ]].copy()
    serper_input.columns = ["board_ticker", "person_name", "company", "role", "is_current"]

    # If primary_company is null, use board_company (ticker) as fallback
    serper_input["company"] = serper_input["company"].fillna(serper_input["board_ticker"])

    # Remove rows with null company
    serper_input = serper_input[serper_input["company"].notna()].copy()

    # Add metadata
    serper_input["match_source"] = "def14a_primary"
    serper_input = serper_input[["person_name", "company", "board_ticker", "role", "is_current", "match_source"]]

    serper_input.to_csv(OUTPUT_PATH, index=False)

    print(f"\nOutput: {OUTPUT_PATH}")
    print(f"  Rows: {len(serper_input):,}")
    print(f"  With primary_company: {(serper_input['company'] != serper_input['board_ticker']).sum():,}")
    print(f"\nNext step:")
    print(f"  python3 src/data_collection/find_urls_serper.py \\")
    print(f"    --input {OUTPUT_PATH} \\")
    print(f"    --run --yes")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

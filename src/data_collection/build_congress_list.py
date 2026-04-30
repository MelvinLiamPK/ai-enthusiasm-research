"""
Build Congress Member List for LinkedIn URL Discovery
======================================================

Fetches current US Congress members from the unitedstates/congress-legislators
dataset and outputs a CSV formatted for find_urls_serper.py.

Uses chamber name as company_name (e.g. "US Senate", "US House of Representatives").

Output columns:
    name            - full name (cleaned)
    company_name    - chamber ("US Senate" or "US House of Representatives")
    chamber         - senate or house
    state           - two-letter state code
    party           - D, R, I, etc.
    bioguide_id     - unique Congress identifier

Usage:
    python3 build_congress_list.py --stats
    python3 build_congress_list.py --run
    python3 build_congress_list.py --run --output data/extracted/congress/congress_members.csv
"""

import sys
import json
import argparse
import requests
import pandas as pd
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

LEGISLATORS_URL = (
    "https://unitedstates.github.io/congress-legislators/legislators-current.json"
)
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "extracted" / "congress" / "congress_members.csv"

CHAMBER_NAMES = {
    "sen": "US Senate",
    "rep": "US House of Representatives",
}


def fetch_legislators(url=LEGISLATORS_URL):
    print(f"Fetching legislators from {url} ...")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_legislators(data):
    rows = []
    for member in data:
        name_obj = member.get("name", {})
        # Prefer official_full, fall back to first + last
        full_name = name_obj.get("official_full") or (
            f"{name_obj.get('first', '')} {name_obj.get('last', '')}".strip()
        )

        bio = member.get("bio", {})
        bioguide_id = member.get("id", {}).get("bioguide", "")

        # Most recent term determines current chamber/state/party
        terms = member.get("terms", [])
        if not terms:
            continue
        current_term = terms[-1]

        chamber_code = current_term.get("type", "")  # "sen" or "rep"
        state = current_term.get("state", "")
        party = current_term.get("party", "")

        company_name = CHAMBER_NAMES.get(chamber_code, "US Congress")

        rows.append({
            "name": full_name,
            "company_name": company_name,
            "chamber": chamber_code,
            "state": state,
            "party": party,
            "bioguide_id": bioguide_id,
        })

    return pd.DataFrame(rows)


def print_stats(df):
    print(f"\nCongress member list stats:")
    print(f"  Total members:  {len(df)}")
    print(f"  Senate:         {(df['chamber'] == 'sen').sum()}")
    print(f"  House:          {(df['chamber'] == 'rep').sum()}")
    print(f"  Parties:        {df['party'].value_counts().to_dict()}")
    print(f"  States:         {df['state'].nunique()} unique")
    print(f"\nSample rows:")
    print(df.head(5).to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--run", action="store_true", help="Fetch and save the CSV")
    parser.add_argument("--stats", action="store_true", help="Show stats without saving")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    if not args.run and not args.stats:
        parser.print_help()
        sys.exit(0)

    data = fetch_legislators()
    df = parse_legislators(data)

    print_stats(df)

    if args.run:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.output, index=False)
        print(f"\nSaved {len(df)} members to {args.output}")


if __name__ == "__main__":
    main()

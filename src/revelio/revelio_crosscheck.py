"""
Cross-check our LinkedIn URLs against downloaded Revelio data.

Loads:
  - data/revelio/matched_users.csv   (Transform 1 output from Redivis)
  - data/revelio/matched_positions.csv (Transform 2 output from Redivis)
  - data/processed/all_people_linkedin_urls/all_linkedin_urls.csv

Adds new columns to all_linkedin_urls.csv:
  - revelio_url_match       (bool): URL found in Revelio
  - revelio_name_confirmed  (bool): Revelio fullname matches our person_name
  - revelio_company_confirmed (bool): any career position matches our company_name
  - revelio_user_id         (int): Revelio user_id
  - revelio_profile_title   (str): Revelio profile_title
  - company_in_title        (bool): company name appears in LinkedIn title

Usage:
    python3 src/revelio/revelio_crosscheck.py [--stats] [--dry-run]
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
URLS_PATH = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls.csv"
USERS_PATH = PROJECT_ROOT / "data" / "revelio" / "matched_users.csv"
POSITIONS_PATH = PROJECT_ROOT / "data" / "revelio" / "matched_positions.csv"

# Minimum company name length to use for matching (avoid short ambiguous names)
MIN_COMPANY_NAME_LEN = 4


def clean_url(url):
    """Normalise to linkedin.com/in/<slug> (no protocol, no trailing slash)."""
    if pd.isna(url):
        return None
    url = str(url).strip()
    url = re.sub(r"^https?://(www\.)?", "", url)
    url = url.rstrip("/")
    if re.match(r"^linkedin\.com/in/[A-Za-z0-9\-%]+$", url):
        return url
    return None


def name_matches(revelio_fullname, our_name):
    """True if last name from our_name appears in Revelio fullname (case-insensitive)."""
    if pd.isna(revelio_fullname) or pd.isna(our_name):
        return False
    revelio_fullname = str(revelio_fullname).lower()
    our_name = str(our_name).lower()
    # Use last token of our name as last name
    parts = our_name.split()
    if not parts:
        return False
    last = parts[-1]
    return last in revelio_fullname


def company_in_positions(user_id, positions_by_user, company_name):
    """True if any position's company_cleaned contains our company name."""
    if pd.isna(company_name) or len(str(company_name)) < MIN_COMPANY_NAME_LEN:
        return False
    positions = positions_by_user.get(user_id, [])
    company_lower = str(company_name).lower()
    for pos_company in positions:
        if pd.isna(pos_company):
            continue
        if company_lower in str(pos_company).lower() or str(pos_company).lower() in company_lower:
            return True
    return False


def company_in_title(linkedin_title, company_name):
    """True if company name appears (word-boundary) in the LinkedIn title string."""
    if pd.isna(linkedin_title) or pd.isna(company_name):
        return False
    if len(str(company_name)) < MIN_COMPANY_NAME_LEN:
        return False
    pattern = re.escape(str(company_name).strip())
    return bool(re.search(pattern, str(linkedin_title), re.IGNORECASE))


def load_positions_index(positions_df):
    """Build {user_id: [company_cleaned, ...]} for fast lookup."""
    col = "company_cleaned" if "company_cleaned" in positions_df.columns else positions_df.columns[1]
    index = {}
    for row in positions_df[["user_id", col]].itertuples(index=False):
        uid = row.user_id
        if uid not in index:
            index[uid] = []
        index[uid].append(row[1])
    return index


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--stats", action="store_true", help="Print summary statistics")
    parser.add_argument("--dry-run", action="store_true", help="Do not write output")
    args = parser.parse_args()

    # --- Load data ---
    print("Loading all_linkedin_urls.csv...")
    urls_df = pd.read_csv(URLS_PATH)
    print(f"  {len(urls_df):,} rows")

    if not USERS_PATH.exists():
        print(f"ERROR: {USERS_PATH} not found. Download from Redivis first.", file=sys.stderr)
        sys.exit(1)
    if not POSITIONS_PATH.exists():
        print(f"ERROR: {POSITIONS_PATH} not found. Download from Redivis first.", file=sys.stderr)
        sys.exit(1)

    print("Loading matched_users.csv...")
    users_df = pd.read_csv(USERS_PATH)
    print(f"  {len(users_df):,} rows")

    print("Loading matched_positions.csv...")
    positions_df = pd.read_csv(POSITIONS_PATH)
    print(f"  {len(positions_df):,} rows")

    # --- Normalise URLs for join ---
    urls_df["_clean_url"] = urls_df["linkedin_url"].apply(clean_url)
    users_df["_clean_url"] = users_df["clean_linkedin_url"].apply(clean_url) \
        if "clean_linkedin_url" in users_df.columns \
        else users_df["profile_linkedin_url"].apply(clean_url)

    # Build URL → Revelio user row lookup (one row per URL — take first if duplicates)
    revelio_by_url = users_df.drop_duplicates("_clean_url").set_index("_clean_url")

    # Build positions index
    positions_index = load_positions_index(positions_df)

    # --- Compute columns ---
    print("Computing Revelio match columns...")

    revelio_url_match = []
    revelio_name_confirmed = []
    revelio_company_confirmed = []
    revelio_user_id_col = []
    revelio_profile_title_col = []
    company_in_title_col = []

    for row in urls_df.itertuples(index=False):
        clean = row._clean_url
        rev = revelio_by_url.get(clean) if clean else None  # Series or None

        if rev is None or (hasattr(rev, 'empty') and rev.empty):
            revelio_url_match.append(False)
            revelio_name_confirmed.append(False)
            revelio_company_confirmed.append(False)
            revelio_user_id_col.append(None)
            revelio_profile_title_col.append(None)
        else:
            revelio_url_match.append(True)
            uid = rev.get("user_id") if hasattr(rev, "get") else getattr(rev, "user_id", None)
            fullname = rev.get("fullname") if hasattr(rev, "get") else getattr(rev, "fullname", None)
            prof_title = rev.get("profile_title") if hasattr(rev, "get") else getattr(rev, "profile_title", None)

            revelio_user_id_col.append(uid)
            revelio_profile_title_col.append(prof_title)
            revelio_name_confirmed.append(name_matches(fullname, row.person_name))
            revelio_company_confirmed.append(
                company_in_positions(uid, positions_index, row.company_name_clean)
            )

        # Company-in-title uses our scraped linkedin_title
        company_in_title_col.append(
            company_in_title(
                getattr(row, "linkedin_title", None),
                getattr(row, "company_name_clean", None)
            )
        )

    urls_df["revelio_url_match"] = revelio_url_match
    urls_df["revelio_name_confirmed"] = revelio_name_confirmed
    urls_df["revelio_company_confirmed"] = revelio_company_confirmed
    urls_df["revelio_user_id"] = revelio_user_id_col
    urls_df["revelio_profile_title"] = revelio_profile_title_col
    urls_df["company_in_title"] = company_in_title_col

    # Drop helper column
    urls_df.drop(columns=["_clean_url"], inplace=True)

    # --- Stats ---
    if args.stats:
        total = len(urls_df)
        found = len(urls_df[urls_df["search_status"] == "found"])
        matched = urls_df["revelio_url_match"].sum()
        name_conf = urls_df["revelio_name_confirmed"].sum()
        co_conf = urls_df["revelio_company_confirmed"].sum()
        both_conf = (urls_df["revelio_name_confirmed"] & urls_df["revelio_company_confirmed"]).sum()
        in_title = urls_df["company_in_title"].sum()

        print(f"\n--- Stats ---")
        print(f"Total rows:                    {total:>8,}")
        print(f"URLs found (search_status):    {found:>8,}")
        print(f"Revelio URL match:             {matched:>8,}  ({matched/found*100:.1f}% of found)")
        print(f"  Name confirmed:              {name_conf:>8,}  ({name_conf/matched*100:.1f}% of matched)")
        print(f"  Company confirmed:           {co_conf:>8,}  ({co_conf/matched*100:.1f}% of matched)")
        print(f"  Both confirmed:              {both_conf:>8,}  ({both_conf/matched*100:.1f}% of matched)")
        print(f"Company in LinkedIn title:     {in_title:>8,}  ({in_title/total*100:.1f}% of total)")

    # --- Write ---
    if not args.dry_run:
        urls_df.to_csv(URLS_PATH, index=False)
        print(f"\nSaved updated all_linkedin_urls.csv ({len(urls_df):,} rows)")
    else:
        print("\n[dry-run] No file written.")


if __name__ == "__main__":
    main()

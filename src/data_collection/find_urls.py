"""
Find LinkedIn Profile URLs via Google Custom Search
=====================================================

Searches Google for LinkedIn profile URLs for any list of company-person
pairs. Accepts any CSV containing person name and company name columns,
generates optimised search queries, and verifies results by matching the
person's name against the LinkedIn profile title returned by Google.

This script replaces the separate prepare_linkedin_queries.py and
find_linkedin_urls_sp500.py scripts with a single generic pipeline:
    CSV input → name cleaning → query generation → Google search →
    name verification → checkpoint/resume → combined output

Name verification uses deterministic word-boundary matching with a
built-in nickname dictionary (e.g. "Timothy" matches "Tim") to flag
profiles that likely belong to the right person. Unverified URLs are
preserved but flagged, prioritising recall over precision — downstream
filtering can apply stricter thresholds.

Data source:
    Any CSV with person name and company name columns. Designed to work
    with the output of combine_people.py (all_people.csv), but also
    accepts directors_current.csv, executives_current.csv, or any file
    with compatible columns.

Auto-detected input columns (first match wins):
    Person name:  person_name, director_name, executive_name, blockholder_name, name
    Company name: company_name, coname, company

Output columns (appended to input):
    search_query     - the query sent to Google
    linkedin_url     - discovered LinkedIn profile URL (or None)
    linkedin_title   - title of the LinkedIn search result
    search_status    - found, not_found, quota_exceeded, error, etc.
    verified         - True if person's name matched the LinkedIn title
    match_type       - both, first_name, last_name, or none

Output files (saved to --output directory):
    batch_NNN_urls.csv           - results per batch
    all_linkedin_urls.csv        - combined results from all batches
    all_linkedin_urls_verified.csv - combined with unverified URLs nulled

Usage:
    python3 find_urls.py --input all_people.csv --stats          # Preview, no API calls
    python3 find_urls.py --input all_people.csv --prototype 5    # Test with 5 people
    python3 find_urls.py --input all_people.csv --run            # Full run
    python3 find_urls.py --input all_people.csv --run --yes      # Non-interactive (SLURM)
    python3 find_urls.py --input all_people.csv --resume         # Resume after interruption
    python3 find_urls.py --input all_people.csv --status         # Check progress
    python3 find_urls.py --input all_people.csv --combine        # Combine batch results
    python3 find_urls.py --input all_people.csv --verify         # Verify combined file
    python3 find_urls.py --input all_people.csv --verify --apply # Verify + null bad URLs

Prerequisites:
    - Google Cloud project with Custom Search JSON API enabled + billing
    - Programmable Search Engine configured to search linkedin.com
    - API key and Search Engine ID in .env file:
        GOOGLE_API_KEY=...
        GOOGLE_CSE_ID=...

Requirements:
    pip install pandas requests python-dotenv
"""

import os
import sys
import re
import json
import time
import argparse
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


# =========================
# Configuration
# =========================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # src/data_collection/../../

# Load .env from project root
load_dotenv(PROJECT_ROOT / ".env")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_SEARCH_ENGINE_ID = os.getenv("GOOGLE_CSE_ID")
GOOGLE_SEARCH_URL = "https://www.googleapis.com/customsearch/v1"

DELAY_BETWEEN_REQUESTS = 1.5  # seconds
SAVE_EVERY_N_QUERIES = 25
BATCH_SIZE = 1000
MAX_RESULTS_PER_QUERY = 5


# =========================
# Column Detection
# =========================

PERSON_NAME_COLS = ["person_name", "director_name", "executive_name", "blockholder_name", "name"]
COMPANY_NAME_COLS = ["company_name", "coname", "company"]


def detect_column(df, candidates, label):
    """Find the first matching column from a list of candidates."""
    for col in candidates:
        if col in df.columns:
            return col
    available = ", ".join(df.columns.tolist())
    print(f"\n✗ Could not detect {label} column.")
    print(f"  Expected one of: {', '.join(candidates)}")
    print(f"  Available: {available}")
    raise SystemExit(1)


# =========================
# Name Cleaning
# =========================

def clean_person_name(name):
    """
    Clean person names: remove credentials, preserve generational suffixes.
    "Timothy D. Cook, MBA" → "Timothy D Cook"
    "CLOUES EDWARD B II"   → "Cloues Edward B II"
    """
    if pd.isna(name):
        return name

    name = str(name).strip()

    # Preserve generational suffixes
    gen_suffixes = r"\b(Jr\.?|Sr\.?|I{1,3}|IV|V|VI|VII|VIII|2nd|3rd|4th)\b"
    gen_match = re.search(gen_suffixes, name, re.IGNORECASE)
    gen_suffix = gen_match.group(0) if gen_match else ""

    # Remove credentials BEFORE stripping dots (so C.F.A. matches)
    credentials = [
        r"\bPh\.?D\.?\b", r"\bM\.?D\.?\b", r"\bMBA\b", r"\bM\.?B\.?A\.?\b",
        r"\bCPA\b", r"\bC\.?P\.?A\.?\b", r"\bC\.?F\.?A\.?\b", r"\bCFA\b",
        r"\bJ\.?D\.?\b", r"\bEsq\.?\b", r"\bP\.?E\.?\b", r"\bDr\.?\b",
        r"\bKBE\b", r"\bAC\b", r"\bOBE\b", r"\bCBE\b",
    ]
    for cred in credentials:
        name = re.sub(cred, "", name, flags=re.IGNORECASE)

    # Also catch spaced-out credentials (C F A, M B A, etc.)
    name = re.sub(r"\bC\s+F\s+A\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\bM\s+B\s+A\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\bC\s+P\s+A\b", "", name, flags=re.IGNORECASE)

    # Remove generational suffix temporarily
    if gen_suffix:
        name = re.sub(gen_suffixes, "", name, flags=re.IGNORECASE)

    # Clean up punctuation and whitespace
    name = re.sub(r"\.+", " ", name)
    name = re.sub(r"\s*,\s*", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    # Title case if all-uppercase (SEC format)
    if name == name.upper() and len(name) > 3:
        name = name.title()

    # Fix possessive mangling from title case (Carter'S → Carter's)
    name = re.sub(r"'S\b", "'s", name)

    # Restore suffix
    if gen_suffix:
        # Normalise suffix case
        suffix_fixes = {"Ii": "II", "Iii": "III", "Iv": "IV"}
        gen_clean = suffix_fixes.get(gen_suffix.title(), gen_suffix)
        name = f"{name} {gen_clean}"

    return name.strip()


def clean_company_name(name):
    """Strip common corporate suffixes for cleaner search queries."""
    if pd.isna(name):
        return name

    name = str(name).strip()

    suffixes = [
        r"\s*,?\s*Inc\.?\s*$", r"\s*,?\s*Corp\.?\s*$",
        r"\s*,?\s*Corporation\s*$", r"\s*,?\s*Ltd\.?\s*$",
        r"\s*,?\s*LLC\s*$", r"\s*,?\s*L\.L\.C\.?\s*$",
        r"\s*,?\s*PLC\s*$", r"\s*,?\s*Co\.?\s*$",
        r"\s*,?\s*Company\s*$",
    ]
    for suffix in suffixes:
        name = re.sub(suffix, "", name, flags=re.IGNORECASE)

    # Title case if all-uppercase
    if name == name.upper() and len(name) > 3:
        name = name.title()
        name = re.sub(r"'S\b", "'s", name)

    return re.sub(r"\s+", " ", name).strip()


# =========================
# Name Verification
# =========================

# Common nickname mappings for name matching
NICKNAMES = {
    "robert": ["bob", "rob", "bobby", "bert"],
    "william": ["bill", "will", "billy", "willy", "liam"],
    "richard": ["rick", "dick", "rich", "ricky"],
    "james": ["jim", "jimmy", "jamie"],
    "timothy": ["tim", "timmy"],
    "thomas": ["tom", "tommy"],
    "michael": ["mike", "mick", "mickey"],
    "joseph": ["joe", "joey"],
    "christopher": ["chris", "kit"],
    "anthony": ["tony", "ant"],
    "steven": ["steve", "stevie"],
    "stephen": ["steve", "stevie"],
    "edward": ["ed", "eddie", "ted", "teddy"],
    "charles": ["charlie", "chuck", "chas"],
    "daniel": ["dan", "danny"],
    "matthew": ["matt", "matty"],
    "andrew": ["andy", "drew"],
    "david": ["dave", "davey"],
    "kenneth": ["ken", "kenny"],
    "ronald": ["ron", "ronny", "ronnie"],
    "donald": ["don", "donny", "donnie"],
    "raymond": ["ray"],
    "lawrence": ["larry", "lars"],
    "nicholas": ["nick", "nicky"],
    "benjamin": ["ben", "benny", "benji"],
    "samuel": ["sam", "sammy"],
    "gregory": ["greg", "gregg"],
    "patrick": ["pat", "paddy"],
    "alexander": ["alex", "al", "xander"],
    "albert": ["al", "bert", "bertie"],
    "frederick": ["fred", "freddy", "freddie"],
    "gerald": ["jerry", "gerry"],
    "harold": ["harry", "hal"],
    "jeffrey": ["jeff", "geoff"],
    "jonathan": ["jon", "john", "jonny"],
    "peter": ["pete"],
    "phillip": ["phil"],
    "philip": ["phil"],
    "stanley": ["stan"],
    "theodore": ["ted", "teddy", "theo"],
    "walter": ["walt", "wally"],
    "elizabeth": ["liz", "lizzy", "beth", "betty", "eliza"],
    "margaret": ["maggie", "meg", "peggy", "marge"],
    "catherine": ["cathy", "kate", "katie", "cat"],
    "katherine": ["kathy", "kate", "katie", "kat"],
    "patricia": ["pat", "patty", "trish"],
    "jennifer": ["jen", "jenny"],
    "jessica": ["jess", "jessie"],
    "susan": ["sue", "susie", "suzy"],
    "rebecca": ["becky", "becca"],
    "barbara": ["barb", "barbie", "babs"],
    "dorothy": ["dot", "dotty", "dottie"],
    "deborah": ["deb", "debbie"],
    "nancy": ["nan"],
    "carolyn": ["carol", "carrie"],
    "christine": ["chris", "christy", "tina"],
    "virginia": ["ginny", "ginger"],
    "jacqueline": ["jackie", "jacqui"],
    "millard": ["mickey"],
}


def extract_name_parts(person_name):
    """Extract first/last name variations including nicknames."""
    if pd.isna(person_name):
        return {"first_names": [], "last_names": []}

    name = str(person_name).strip()
    name = re.sub(r"\b(Ph\.?D\.?|M\.?D\.?|MBA|CPA|J\.?D\.?|Esq\.?)\b", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\.+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    parts = [p.strip() for p in name.split() if p.strip()]
    if not parts:
        return {"first_names": [], "last_names": []}

    suffixes = {"jr", "sr", "ii", "iii", "iv", "v", "vi", "vii", "viii", "2nd", "3rd", "4th"}

    first_name = parts[0].lower()
    first_names = [first_name]

    # Add nicknames
    if first_name in NICKNAMES:
        first_names.extend(NICKNAMES[first_name])
    # Reverse lookup (nickname → formal)
    for formal, nicks in NICKNAMES.items():
        if first_name in nicks and formal not in first_names:
            first_names.append(formal)

    # Last name (skip suffixes, skip single initials)
    last_names = []
    for i in range(len(parts) - 1, 0, -1):
        part = parts[i].lower()
        if part not in suffixes and len(part) > 1:
            last_names.append(part)
            break

    # For compound last names, also try the very last word
    if len(parts) > 2:
        last_part = parts[-1].lower()
        if last_part not in suffixes and last_part not in last_names and len(last_part) > 1:
            last_names.append(last_part)

    return {"first_names": first_names, "last_names": last_names}


def verify_name_match(person_name, linkedin_title):
    """
    Check if the LinkedIn profile title matches the person's name.
    Returns dict with verified (bool), match_type, matched_first, matched_last.
    """
    if pd.isna(person_name) or pd.isna(linkedin_title):
        return {"verified": False, "match_type": "none"}

    parts = extract_name_parts(person_name)
    title_lower = str(linkedin_title).lower()

    matched_first = None
    for fn in parts["first_names"]:
        if re.search(r"\b" + re.escape(fn) + r"\b", title_lower):
            matched_first = fn
            break

    matched_last = None
    for ln in parts["last_names"]:
        if re.search(r"\b" + re.escape(ln) + r"\b", title_lower):
            matched_last = ln
            break

    if matched_first and matched_last:
        return {"verified": True, "match_type": "both"}
    elif matched_first:
        return {"verified": True, "match_type": "first_name"}
    elif matched_last:
        return {"verified": True, "match_type": "last_name"}
    else:
        return {"verified": False, "match_type": "none"}


# =========================
# Google Search
# =========================

def check_credentials():
    """Verify API credentials are loaded."""
    if not GOOGLE_API_KEY or not GOOGLE_SEARCH_ENGINE_ID:
        print("\n✗ Missing Google API credentials in .env file!")
        print(f"  Looked for .env at: {PROJECT_ROOT / '.env'}")
        print("\n  Required variables:")
        print("    GOOGLE_API_KEY=your_api_key")
        print("    GOOGLE_CSE_ID=your_search_engine_id")
        raise SystemExit(1)


def search_linkedin_profile(query, person_name=None, retries=3):
    """
    Search Google for a LinkedIn profile URL.

    Args:
        query:        search string (e.g. "Tim Cook Apple")
        person_name:  full name for verification against search results
        retries:      number of retry attempts on timeout

    Returns:
        dict with url, title, status, verified, match_type
    """
    search_query = f"{query} site:linkedin.com/in/"

    params = {
        "key": GOOGLE_API_KEY,
        "cx": GOOGLE_SEARCH_ENGINE_ID,
        "q": search_query,
        "num": MAX_RESULTS_PER_QUERY,
    }

    for attempt in range(retries):
        try:
            response = requests.get(GOOGLE_SEARCH_URL, params=params, timeout=10)

            if response.status_code == 200:
                items = response.json().get("items", [])

                # If we have a name, try verified match first
                if person_name:
                    for item in items:
                        link = item.get("link", "")
                        title = item.get("title", "")
                        if "linkedin.com/in/" in link:
                            result = verify_name_match(person_name, title)
                            if result["verified"]:
                                return {
                                    "url": link, "title": title,
                                    "status": "found", "verified": True,
                                    "match_type": result["match_type"],
                                }

                    # No verified match — return first LinkedIn result as unverified
                    for item in items:
                        link = item.get("link", "")
                        title = item.get("title", "")
                        if "linkedin.com/in/" in link:
                            return {
                                "url": link, "title": title,
                                "status": "found", "verified": False,
                                "match_type": "none",
                            }
                else:
                    # No name provided — return first LinkedIn result
                    for item in items:
                        link = item.get("link", "")
                        title = item.get("title", "")
                        if "linkedin.com/in/" in link:
                            return {
                                "url": link, "title": title,
                                "status": "found", "verified": None,
                                "match_type": None,
                            }

                return {"url": None, "title": None, "status": "not_found",
                        "verified": None, "match_type": None}

            elif response.status_code == 429:
                print(f"      ⚠ Rate limited, waiting 60s (attempt {attempt + 1}/{retries})...")
                time.sleep(60)
                continue

            elif response.status_code == 403:
                # Check if it's specifically a quota error
                error_body = response.json() if response.text else {}
                errors = error_body.get("error", {}).get("errors", [])
                reason = errors[0].get("reason", "") if errors else ""
                print(f"      ⚠ API 403: {reason or 'forbidden'}")
                return {"url": None, "title": None, "status": "quota_exceeded",
                        "verified": None, "match_type": None}

            else:
                print(f"      Error {response.status_code}")
                return {"url": None, "title": None,
                        "status": f"error_{response.status_code}",
                        "verified": None, "match_type": None}

        except requests.exceptions.Timeout:
            print(f"      Timeout on attempt {attempt + 1}")
            time.sleep(2)
        except Exception as e:
            print(f"      Error: {e}")
            return {"url": None, "title": None, "status": "exception",
                    "verified": None, "match_type": None}

    # If we exhausted retries, it's likely quota exceeded (repeated 429s)
    return {"url": None, "title": None, "status": "quota_exceeded",
            "verified": None, "match_type": None}


# =========================
# Input Preparation
# =========================

def prepare_input(input_path):
    """
    Load input CSV, detect columns, clean names, generate search queries.

    Returns:
        DataFrame with person_name_clean, company_name_clean, search_query columns
    """
    print(f"\nLoading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"  {len(df):,} rows, {df.shape[1]} columns")

    # Detect columns
    person_col = detect_column(df, PERSON_NAME_COLS, "person name")
    company_col = detect_column(df, COMPANY_NAME_COLS, "company name")
    print(f"  Person column:  {person_col}")
    print(f"  Company column: {company_col}")

    # Clean names
    print("  Cleaning names...")
    df["person_name_clean"] = df[person_col].apply(clean_person_name)
    df["company_name_clean"] = df[company_col].apply(clean_company_name)

    # Generate search queries
    df["search_query"] = df["person_name_clean"] + " " + df["company_name_clean"]

    # Drop rows with empty queries
    before = len(df)
    df = df.dropna(subset=["search_query"])
    df = df[df["search_query"].str.strip().ne("")]
    if len(df) < before:
        print(f"  Dropped {before - len(df)} rows with empty names")

    print(f"  Ready: {len(df):,} search queries")
    return df


# =========================
# Batch Processing
# =========================

def get_output_dir(input_path, output_arg):
    """Determine output directory from args or input filename."""
    if output_arg:
        return Path(output_arg)
    # Default: sibling directory named after input file
    input_stem = Path(input_path).stem
    return PROJECT_ROOT / "data" / "processed" / f"{input_stem}_linkedin_urls"


def get_checkpoint_dir(output_dir):
    return output_dir / "checkpoints"


def save_checkpoint(checkpoint_dir, batch_num, df, queries_processed):
    """Save progress for a batch."""
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    progress_file = checkpoint_dir / f"batch_{batch_num:03d}_progress.csv"
    df.to_csv(progress_file, index=False)
    checkpoint_file = checkpoint_dir / f"batch_{batch_num:03d}_checkpoint.json"
    with open(checkpoint_file, "w") as f:
        json.dump({
            "batch_num": batch_num,
            "queries_processed": queries_processed,
            "timestamp": datetime.now().isoformat(),
        }, f)


def load_checkpoint(checkpoint_dir, batch_num):
    """Load checkpoint for a batch, if it exists."""
    checkpoint_file = checkpoint_dir / f"batch_{batch_num:03d}_checkpoint.json"
    if not checkpoint_file.exists():
        return None
    with open(checkpoint_file) as f:
        return json.load(f)


def get_completed_batches(checkpoint_dir):
    """Get set of completed batch numbers."""
    completed_file = checkpoint_dir / "completed_batches.txt"
    if not completed_file.exists():
        return set()
    completed = set()
    with open(completed_file) as f:
        for line in f:
            if line.strip():
                completed.add(int(line.strip().split(",")[0]))
    return completed


def mark_batch_complete(checkpoint_dir, batch_num, found, total, verified):
    """Record a batch as completed."""
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    completed_file = checkpoint_dir / "completed_batches.txt"
    with open(completed_file, "a") as f:
        f.write(f"{batch_num},{datetime.now().isoformat()},{found},{total},{verified}\n")

    # Clean up checkpoint files
    for suffix in ["_checkpoint.json", "_progress.csv"]:
        p = checkpoint_dir / f"batch_{batch_num:03d}{suffix}"
        if p.exists():
            p.unlink()


def process_batch(batch_df, batch_num, output_dir, resume=True):
    """
    Process a single batch of search queries.

    Args:
        batch_df:    DataFrame for this batch
        batch_num:   batch number (1-indexed)
        output_dir:  where to save results
        resume:      whether to resume from checkpoint

    Returns:
        (DataFrame with results, quota_exceeded bool)
    """
    checkpoint_dir = get_checkpoint_dir(output_dir)
    total = len(batch_df)
    start_from = 0

    # Check for checkpoint
    if resume:
        cp = load_checkpoint(checkpoint_dir, batch_num)
        if cp:
            start_from = cp["queries_processed"]
            progress_file = checkpoint_dir / f"batch_{batch_num:03d}_progress.csv"
            if progress_file.exists():
                batch_df = pd.read_csv(progress_file)
                print(f"  Resuming from checkpoint: {start_from}/{total}")

    # Ensure result columns exist
    for col in ["linkedin_url", "linkedin_title", "search_status", "verified", "match_type"]:
        if col not in batch_df.columns:
            batch_df[col] = None

    print(f"\n  Batch {batch_num}: {total} queries" +
          (f" (starting from {start_from + 1})" if start_from > 0 else ""))
    print(f"  Estimated time: {(total - start_from) * DELAY_BETWEEN_REQUESTS / 60:.1f} minutes")

    found_count = batch_df["linkedin_url"].notna().sum()
    verified_count = (batch_df["verified"] == True).sum()
    quota_exceeded = False
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 5

    for i in range(start_from, total):
        row = batch_df.iloc[i]
        query = row["search_query"]
        person_name = row.get("person_name_clean", None)

        if (i + 1) % 10 == 0 or i == start_from:
            pct = 100 * (i + 1) / total
            print(f"    [{i + 1}/{total}] {pct:.0f}% ({found_count} found, {verified_count} verified)")

        result = search_linkedin_profile(query, person_name=person_name)

        batch_df.iloc[i, batch_df.columns.get_loc("linkedin_url")] = result["url"]
        batch_df.iloc[i, batch_df.columns.get_loc("linkedin_title")] = result["title"]
        batch_df.iloc[i, batch_df.columns.get_loc("search_status")] = result["status"]
        batch_df.iloc[i, batch_df.columns.get_loc("verified")] = result.get("verified")
        batch_df.iloc[i, batch_df.columns.get_loc("match_type")] = result.get("match_type")

        if result["url"]:
            found_count += 1
            consecutive_failures = 0
            if result.get("verified"):
                verified_count += 1

        if result["status"] == "quota_exceeded":
            quota_exceeded = True
            print(f"\n    ⚠ Quota exceeded at query {i + 1}")
            break

        # Track consecutive failures (not_found is OK, errors are not)
        if result["status"] in ("exception", "error_429", "error_403"):
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                quota_exceeded = True
                print(f"\n    ⚠ {MAX_CONSECUTIVE_FAILURES} consecutive failures — assuming quota exhausted")
                break
        elif result["status"] != "not_found":
            consecutive_failures = 0

        # Periodic checkpoint
        if (i + 1) % SAVE_EVERY_N_QUERIES == 0:
            save_checkpoint(checkpoint_dir, batch_num, batch_df, i + 1)

        if i < total - 1 and not quota_exceeded:
            time.sleep(DELAY_BETWEEN_REQUESTS)

    # Final counts
    found_count = batch_df["linkedin_url"].notna().sum()
    verified_count = (batch_df["verified"] == True).sum()
    print(f"\n  ✓ {found_count}/{total} URLs found ({100 * found_count / total:.1f}%)")
    if found_count > 0:
        print(f"  ✓ {verified_count}/{found_count} verified ({100 * verified_count / found_count:.1f}%)")

    # Save batch results
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_file = output_dir / f"batch_{batch_num:03d}_urls.csv"
    batch_df.to_csv(batch_file, index=False)
    print(f"  Saved: {batch_file}")

    # Mark complete or keep checkpoint
    if not quota_exceeded:
        mark_batch_complete(checkpoint_dir, batch_num, found_count, total, verified_count)
    else:
        save_checkpoint(checkpoint_dir, batch_num, batch_df, i + 1)

    return batch_df, quota_exceeded


# =========================
# Commands
# =========================

def cmd_stats(input_path):
    """Preview input data without making any API calls."""
    df = prepare_input(input_path)

    print(f"\n{'=' * 60}")
    print("INPUT STATISTICS")
    print(f"{'=' * 60}")
    print(f"\n  Total queries:      {len(df):,}")
    print(f"  Unique people:      {df['person_name_clean'].nunique():,}")
    print(f"  Unique companies:   {df['company_name_clean'].nunique():,}")

    n_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\n  Batch size:         {BATCH_SIZE}")
    print(f"  Number of batches:  {n_batches}")

    cost = len(df) * 5 / 1000
    hours = len(df) * DELAY_BETWEEN_REQUESTS / 3600
    days_10k = len(df) / 10_000
    print(f"\n  Estimated cost:     ${cost:.0f}")
    print(f"  Estimated time:     {hours:.1f} hours total")
    print(f"  At 10k/day limit:   {days_10k:.1f} days")

    print(f"\n  Sample queries:")
    for _, row in df.head(5).iterrows():
        print(f"    \"{row['search_query']}\"")


def cmd_prototype(input_path, n, output_dir):
    """Test with a small number of queries."""
    check_credentials()
    df = prepare_input(input_path)

    sample = df.head(n).copy()
    print(f"\nPrototype: searching {len(sample)} people")
    print(f"  Estimated time: {len(sample) * DELAY_BETWEEN_REQUESTS / 60:.1f} minutes")

    sample_df, _ = process_batch(sample, 0, output_dir, resume=False)

    print(f"\nResults:")
    for _, row in sample_df.iterrows():
        name = row.get("person_name_clean", "?")
        url = row.get("linkedin_url")
        verified = row.get("verified")
        if pd.notna(url):
            v = "✓" if verified else "⚠"
            print(f"  {v} {name}")
            print(f"    → {url}")
        else:
            print(f"  ✗ {name}")


def cmd_run(input_path, output_dir, resume=False, auto_yes=False):
    """Run the full search across all batches."""
    check_credentials()
    df = prepare_input(input_path)

    n_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
    cost = len(df) * 5 / 1000
    checkpoint_dir = get_checkpoint_dir(output_dir)
    completed = get_completed_batches(checkpoint_dir)

    print(f"\n{'=' * 60}")
    print(f"  Queries:    {len(df):,}")
    print(f"  Batches:    {n_batches} ({len(completed)} already done)")
    print(f"  Est. cost:  ${cost:.0f}")
    print(f"  Output:     {output_dir}")
    print(f"{'=' * 60}")

    if not auto_yes:
        confirm = input("\nProceed? (y/N): ").strip().lower()
        if confirm != "y":
            print("Cancelled.")
            return

    for batch_num in range(1, n_batches + 1):
        if batch_num in completed:
            print(f"\n  Skipping batch {batch_num} (already complete)")
            continue

        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = min(batch_num * BATCH_SIZE, len(df))
        batch_df = df.iloc[start_idx:end_idx].copy()

        _, quota_exceeded = process_batch(batch_df, batch_num, output_dir, resume=resume)

        if quota_exceeded:
            print(f"\n⚠ Quota exceeded. Resume tomorrow with --resume")
            break

        # Brief pause between batches
        if batch_num < n_batches:
            time.sleep(5)

    # Check if all done
    completed = get_completed_batches(checkpoint_dir)
    if len(completed) == n_batches:
        print("\n✓ All batches complete. Running --combine...")
        cmd_combine(output_dir)


def cmd_status(input_path, output_dir):
    """Show progress across all batches."""
    df = prepare_input(input_path)
    n_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
    checkpoint_dir = get_checkpoint_dir(output_dir)
    completed = get_completed_batches(checkpoint_dir)

    print(f"\n{'=' * 60}")
    print("SEARCH STATUS")
    print(f"{'=' * 60}")
    print(f"\n  Total queries:    {len(df):,}")
    print(f"  Batches:          {n_batches}")
    print(f"  Completed:        {len(completed)}")
    print()

    total_found = 0
    total_verified = 0
    total_queries = 0

    for batch_num in range(1, n_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = min(batch_num * BATCH_SIZE, len(df))
        batch_size = end_idx - start_idx
        total_queries += batch_size

        if batch_num in completed:
            batch_file = output_dir / f"batch_{batch_num:03d}_urls.csv"
            if batch_file.exists():
                bdf = pd.read_csv(batch_file)
                found = bdf["linkedin_url"].notna().sum()
                verified = (bdf["verified"] == True).sum()
                total_found += found
                total_verified += verified
                status = f"✓ {found}/{batch_size} found, {verified} verified"
            else:
                status = "✓ Complete"
        else:
            cp = load_checkpoint(checkpoint_dir, batch_num)
            if cp:
                status = f"⏸ {cp['queries_processed']}/{batch_size} done"
            else:
                status = "○ Not started"

        print(f"  Batch {batch_num:3d}: {status}")

    print(f"\n  Total: {total_found:,}/{total_queries:,} URLs found")
    if total_found > 0:
        print(f"  Verified: {total_verified:,}/{total_found:,}")


def cmd_combine(output_dir):
    """Combine all batch result files."""
    print(f"\n{'=' * 60}")
    print("Combining batch results...")
    print(f"{'=' * 60}")

    batch_files = sorted(output_dir.glob("batch_*_urls.csv"))
    if not batch_files:
        print("  ✗ No batch files found")
        return

    dfs = []
    for f in batch_files:
        bdf = pd.read_csv(f)
        dfs.append(bdf)
        print(f"  Loaded: {f.name} ({len(bdf)} rows)")

    combined = pd.concat(dfs, ignore_index=True)
    output_file = output_dir / "all_linkedin_urls.csv"
    combined.to_csv(output_file, index=False)

    total = len(combined)
    found = combined["linkedin_url"].notna().sum()
    verified = (combined["verified"] == True).sum() if "verified" in combined.columns else 0

    print(f"\n  ✓ Combined {len(batch_files)} files → {output_file}")
    print(f"    Total: {total:,}, URLs: {found:,} ({100 * found / total:.1f}%), Verified: {verified:,}")


def cmd_verify(output_dir, apply_filter=False):
    """Run verification on combined results."""
    combined_file = output_dir / "all_linkedin_urls.csv"
    if not combined_file.exists():
        print(f"  ✗ {combined_file} not found. Run --combine first.")
        return

    print(f"\nLoading: {combined_file}")
    df = pd.read_csv(combined_file)
    print(f"  {len(df):,} rows")

    # Re-verify all
    verified_count = 0
    unverified_count = 0
    no_url_count = 0

    for idx, row in df.iterrows():
        if pd.isna(row.get("linkedin_url")) or row.get("search_status") != "found":
            no_url_count += 1
            continue

        person_name = row.get("person_name_clean", row.get("person_name", ""))
        result = verify_name_match(person_name, row.get("linkedin_title", ""))

        df.at[idx, "verified"] = result["verified"]
        df.at[idx, "match_type"] = result["match_type"]

        if result["verified"]:
            verified_count += 1
        else:
            unverified_count += 1

    # Save with verification columns
    df.to_csv(combined_file, index=False)
    print(f"\n  ✓ Updated: {combined_file}")

    total_with_urls = verified_count + unverified_count
    if total_with_urls > 0:
        print(f"    Verified:   {verified_count:,} ({100 * verified_count / total_with_urls:.1f}%)")
        print(f"    Unverified: {unverified_count:,}")
        print(f"    No URL:     {no_url_count:,}")

    if apply_filter:
        df_filtered = df.copy()
        mask = (df_filtered["verified"] == False) & (df_filtered["linkedin_url"].notna())
        n_removed = mask.sum()
        df_filtered.loc[mask, "linkedin_url"] = None
        df_filtered.loc[mask, "search_status"] = "unverified"

        verified_file = output_dir / "all_linkedin_urls_verified.csv"
        df_filtered.to_csv(verified_file, index=False)
        print(f"\n  ✓ Filtered version: {verified_file}")
        print(f"    {n_removed} unverified URLs removed")
        print(f"    Use this file for scraping.")


# =========================
# Main
# =========================

def main():
    global BATCH_SIZE
    parser = argparse.ArgumentParser(
        description="Find LinkedIn profile URLs via Google Custom Search",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 find_urls.py --input all_people.csv --stats
  python3 find_urls.py --input all_people.csv --prototype 5
  python3 find_urls.py --input all_people.csv --run
  python3 find_urls.py --input all_people.csv --run --yes
  python3 find_urls.py --input all_people.csv --resume
  python3 find_urls.py --input all_people.csv --status
  python3 find_urls.py --input all_people.csv --combine
  python3 find_urls.py --input all_people.csv --verify
  python3 find_urls.py --input all_people.csv --verify --apply
        """,
    )

    parser.add_argument("--input", type=str, required=True,
                        help="Input CSV with person_name and company_name columns")
    parser.add_argument("--output", type=str, default=None,
                        help="Output directory (default: derived from input filename)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Queries per batch (default: {BATCH_SIZE})")

    # Commands (mutually exclusive)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--stats", action="store_true",
                       help="Preview input and cost estimate, no API calls")
    group.add_argument("--prototype", type=int, metavar="N",
                       help="Test with N queries")
    group.add_argument("--run", action="store_true",
                       help="Run full search")
    group.add_argument("--resume", action="store_true",
                       help="Resume from last checkpoint")
    group.add_argument("--status", action="store_true",
                       help="Show progress across batches")
    group.add_argument("--combine", action="store_true",
                       help="Combine all batch results")
    group.add_argument("--verify", action="store_true",
                       help="Run name verification on combined results")

    parser.add_argument("--yes", action="store_true",
                        help="Skip confirmation prompt (for SLURM)")
    parser.add_argument("--apply", action="store_true",
                        help="With --verify: null out unverified URLs")

    args = parser.parse_args()

    # Override global batch size if specified
    BATCH_SIZE = args.batch_size

    # Resolve paths
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"✗ Input file not found: {input_path}")
        raise SystemExit(1)

    output_dir = get_output_dir(input_path, args.output)

    print("=" * 60)
    print("LinkedIn URL Finder")
    print("=" * 60)
    print(f"  Input:  {input_path}")
    print(f"  Output: {output_dir}")

    # Dispatch
    if args.stats:
        cmd_stats(input_path)
    elif args.prototype is not None:
        cmd_prototype(input_path, args.prototype, output_dir)
    elif args.run:
        cmd_run(input_path, output_dir, resume=False, auto_yes=args.yes)
    elif args.resume:
        cmd_run(input_path, output_dir, resume=True, auto_yes=args.yes)
    elif args.status:
        cmd_status(input_path, output_dir)
    elif args.combine:
        cmd_combine(output_dir)
    elif args.verify:
        cmd_verify(output_dir, apply_filter=args.apply)


if __name__ == "__main__":
    main()
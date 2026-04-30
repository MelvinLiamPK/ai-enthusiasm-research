"""
Find LinkedIn Profile URLs via Serper.dev
==========================================

Searches Google (via Serper.dev API) for LinkedIn profile URLs for any list
of company-person pairs. Accepts any CSV containing person name and company
name columns, generates search queries, and scores results using name +
company matching to select the best LinkedIn profile.

Pipeline:
    CSV input -> name cleaning -> query generation -> Serper search ->
    scoring (name + company + position) -> checkpoint/resume -> combined output

Scoring algorithm:
    Each search result is scored on a 0-80 scale:
        - Both first + last name match in title:  50 pts
        - Last name only match:                   25 pts
        - First name only match:                  15 pts
        - Company name in title/snippet:          20 pts
        - Search position bonus:                  0-10 pts (10 - index)
    Results below --score-threshold (default 30) are marked not_found.

Data source:
    Any CSV with person name and company name columns. Designed to work
    with the output of combine_people.py (all_people.csv).

Auto-detected input columns (first match wins):
    Person name:  person_name, director_name, executive_name, blockholder_name, name
    Company name: company_name, coname, company

Output columns (appended to input):
    search_query     - the query sent to Serper
    linkedin_url     - discovered LinkedIn profile URL (or None)
    linkedin_title   - title of the LinkedIn search result
    search_status    - found, not_found, quota_exceeded, error, etc.
    verified         - True if person's name matched the LinkedIn title
    match_type       - both, first_name, last_name, or none
    score            - numeric match score (0-80)
    flags            - pipe-separated warning flags (e.g. "possible_staffer")

Output files (saved to --output directory):
    batch_NNN_urls.csv           - results per batch
    all_linkedin_urls.csv        - combined results from all batches

Usage:
    python3 find_urls_serper.py --input all_people.csv --stats
    python3 find_urls_serper.py --input all_people.csv --prototype 5
    python3 find_urls_serper.py --input all_people.csv --run
    python3 find_urls_serper.py --input all_people.csv --run --yes
    python3 find_urls_serper.py --input all_people.csv --resume
    python3 find_urls_serper.py --input all_people.csv --status
    python3 find_urls_serper.py --input all_people.csv --combine
    python3 find_urls_serper.py --input all_people.csv --verify
    python3 find_urls_serper.py --input all_people.csv --verify --apply

Prerequisites:
    - Serper.dev API key in .env file:
        SERPER_API_KEY=...

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

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SERPER_URL = "https://google.serper.dev/search"

DELAY_BETWEEN_REQUESTS = 0.5  # seconds (Serper allows 300 QPS)
SAVE_EVERY_N_QUERIES = 25
BATCH_SIZE = 1000
MAX_RESULTS_PER_QUERY = 10

# Scoring constants
SCORE_BOTH_NAMES = 50
SCORE_LAST_NAME = 25
SCORE_FIRST_NAME = 15
SCORE_COMPANY = 20
SCORE_POSITION_MAX = 10
SCORE_DUAL_QUERY_BONUS = 10
DEFAULT_SCORE_THRESHOLD = 35
SCORE_STAFFER_PENALTY = 30

STAFFER_PATTERNS = [
    r"\badvisor\s+to\b",
    r"\bstaff\s+(?:for|to)\b",
    r"\bcounsel\s+to\b",
    r"\baide\s+to\b",
    r"\bchief\s+of\s+staff\s+(?:for|to)\b",
    r"\bdeputy\s+(?:chief\s+of\s+staff|counsel)\b",
    r"\bpress\s+secretary\s+(?:for|to)\b",
    r"\blegislative\s+(?:director|assistant|aide)\s+(?:for|to)\b",
    r"\bdistrict\s+director\s+(?:for|to)\b",
    r"\bscheduler\s+(?:for|to)\b",
]


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
    "Timothy D. Cook, MBA" -> "Timothy D Cook"
    "CLOUES EDWARD B II"   -> "Cloues Edward B II"
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

    # Fix possessive mangling from title case (Carter'S -> Carter's)
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
    # Reverse lookup (nickname -> formal)
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
    Returns dict with verified (bool), match_type, flags (list).

    LinkedIn titles follow "Name - Job Title | LinkedIn". Name matching is
    restricted to the name segment (before the first " - " or " | ") so that
    profiles like "Jane Doe - Advisor to Senator Smith" don't falsely match
    Senator Smith. The full title is still checked for staffer patterns which
    are surfaced as flags rather than hard failures.
    """
    if pd.isna(person_name) or pd.isna(linkedin_title):
        return {"verified": False, "match_type": "none", "flags": []}

    parts = extract_name_parts(person_name)
    title_str = str(linkedin_title)
    title_lower = title_str.lower()

    # Restrict name matching to the name segment (before first " - " or " | ")
    name_segment = re.split(r"\s[-|]\s", title_str, maxsplit=1)[0].lower()

    matched_first = None
    for fn in parts["first_names"]:
        if re.search(r"\b" + re.escape(fn) + r"\b", name_segment):
            matched_first = fn
            break

    matched_last = None
    for ln in parts["last_names"]:
        if re.search(r"\b" + re.escape(ln) + r"\b", name_segment):
            matched_last = ln
            break

    if matched_first and matched_last:
        match_result = {"verified": True, "match_type": "both"}
    elif matched_first:
        match_result = {"verified": True, "match_type": "first_name"}
    elif matched_last:
        match_result = {"verified": True, "match_type": "last_name"}
    else:
        match_result = {"verified": False, "match_type": "none"}

    # Check full title for staffer patterns — flag but don't hard-reject
    flags = []
    for pattern in STAFFER_PATTERNS:
        if re.search(pattern, title_lower):
            flags.append("possible_staffer")
            break

    return {**match_result, "flags": flags}


# =========================
# Scoring
# =========================

def company_in_text(company_name, title, snippet):
    """Return True if company_name appears in the combined title+snippet text."""
    if not company_name or pd.isna(company_name):
        return False
    company_lower = company_name.lower()
    combined_text = f"{title} {snippet}".lower() if snippet else title.lower()
    if company_lower in combined_text:
        return True
    # Multi-word fallback: all significant words (>3 chars) must appear
    words = [w for w in company_lower.split() if len(w) > 3]
    return bool(words and all(w in combined_text for w in words))


def score_result(person_name, company_name, title, snippet, position):
    """
    Score a single search result on a 0-90 scale.

    Args:
        person_name:   cleaned person name
        company_name:  cleaned company name
        title:         LinkedIn result title
        snippet:       LinkedIn result snippet text
        position:      0-indexed position in search results

    Returns:
        (score, name_match_result)
    """
    score = 0
    name_result = verify_name_match(person_name, title)

    # Name scoring
    if name_result["match_type"] == "both":
        score += SCORE_BOTH_NAMES
    elif name_result["match_type"] == "last_name":
        score += SCORE_LAST_NAME
    elif name_result["match_type"] == "first_name":
        score += SCORE_FIRST_NAME

    # Company scoring — check both title and snippet
    if company_in_text(company_name, title, snippet):
        score += SCORE_COMPANY

    # Staffer penalty — possible_staffer flag already set in verify_name_match
    if "possible_staffer" in name_result.get("flags", []):
        score -= SCORE_STAFFER_PENALTY

    # Position bonus (result #1 gets 10, #10 gets 1)
    score += max(0, SCORE_POSITION_MAX - position)

    return score, name_result


# =========================
# Serper Search
# =========================

def check_credentials():
    """Verify API credentials are loaded."""
    if not SERPER_API_KEY:
        print("\n✗ Missing Serper API key in .env file!")
        print(f"  Looked for .env at: {PROJECT_ROOT / '.env'}")
        print("\n  Required variable:")
        print("    SERPER_API_KEY=your_api_key")
        raise SystemExit(1)


def _serper_query(search_query, retries=3):
    """Execute a single Serper API query. Returns (items_list, error_result_or_None)."""
    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {"q": search_query, "num": MAX_RESULTS_PER_QUERY}

    for attempt in range(retries):
        try:
            response = requests.post(SERPER_URL, json=payload, headers=headers, timeout=10)

            if response.status_code == 200:
                return response.json().get("organic", []), None
            elif response.status_code == 429:
                print(f"      Rate limited, waiting 60s (attempt {attempt + 1}/{retries})...")
                time.sleep(60)
                continue
            elif response.status_code == 403:
                print(f"      API 403: forbidden/quota exceeded")
                return [], {"status": "quota_exceeded"}
            else:
                print(f"      Error {response.status_code}")
                return [], {"status": f"error_{response.status_code}"}

        except requests.exceptions.Timeout:
            print(f"      Timeout on attempt {attempt + 1}")
            time.sleep(2)
        except Exception as e:
            print(f"      Error: {e}")
            return [], {"status": "exception"}

    return [], {"status": "quota_exceeded"}


def search_linkedin_profile(query, person_name=None, company_name=None,
                            score_threshold=DEFAULT_SCORE_THRESHOLD, retries=3,
                            require_full_name=False, require_company_match=False):
    """
    Search Serper.dev for a LinkedIn profile URL using a two-query strategy.

    Runs two queries:
        1. "{name} {company} site:linkedin.com/in/" — name + company
        2. "{name} site:linkedin.com/in/" — name only (catches cases where
           company name drowns out person name in Google ranking)

    All results are pooled, deduped by URL, and scored. URLs appearing in
    both queries get a +10 bonus.

    Args:
        query:                 search string (e.g. "Tim Cook Apple")
        person_name:           full name for verification against search results
        company_name:          company name for scoring against title/snippet
        score_threshold:       minimum score to accept a result (default 35)
        retries:               number of retry attempts on timeout
        require_full_name:     if True, reject results where first+last name
                               don't both appear in the title name segment
        require_company_match: if True, reject results where company name is
                               absent from title+snippet entirely

    Returns:
        dict with url, title, status, verified, match_type, score, flags
    """
    empty_result = {"url": None, "title": None, "status": None,
                    "verified": None, "match_type": None, "score": 0, "flags": ""}

    # Query 1: name + company
    q1 = f"{query} site:linkedin.com/in/"
    items1, err1 = _serper_query(q1, retries=retries)
    if err1 and err1["status"] == "quota_exceeded":
        return {**empty_result, "status": "quota_exceeded"}

    # Query 2: name only (skip if no company — queries would be identical)
    name_only_query = f"{person_name} site:linkedin.com/in/" if person_name else None
    items2 = []
    if name_only_query and company_name and not pd.isna(company_name):
        time.sleep(DELAY_BETWEEN_REQUESTS)
        items2, err2 = _serper_query(name_only_query, retries=retries)
        if err2 and err2["status"] == "quota_exceeded":
            return {**empty_result, "status": "quota_exceeded"}

    # Collect URLs from each query for overlap detection
    urls_q1 = set()
    for item in items1:
        link = item.get("link", "")
        if "linkedin.com/in/" in link:
            urls_q1.add(link)

    urls_q2 = set()
    for item in items2:
        link = item.get("link", "")
        if "linkedin.com/in/" in link:
            urls_q2.add(link)

    overlap_urls = urls_q1 & urls_q2

    # Score all results, deduped by URL (keep best score per URL)
    scored = {}  # url -> candidate dict

    for source_items in [items1, items2]:
        for pos, item in enumerate(source_items):
            link = item.get("link", "")
            if "linkedin.com/in/" not in link:
                continue
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            score, name_result = score_result(
                person_name, company_name, title, snippet, pos,
            )
            # Add overlap bonus
            if link in overlap_urls:
                score += SCORE_DUAL_QUERY_BONUS

            if link not in scored or score > scored[link]["score"]:
                scored[link] = {
                    "url": link, "title": title,
                    "score": score,
                    "verified": name_result["verified"],
                    "match_type": name_result["match_type"],
                    "flags": "|".join(name_result.get("flags", [])),
                }

    if not scored:
        return {**empty_result, "status": "not_found"}

    # Pick highest score
    best = max(scored.values(), key=lambda c: c["score"])

    if best["score"] < score_threshold:
        return {**empty_result, "status": "not_found",
                "verified": False, "match_type": best["match_type"],
                "score": best["score"], "flags": best.get("flags", "")}

    if require_full_name and best.get("match_type") != "both":
        return {**empty_result, "status": "not_found",
                "verified": False, "match_type": best["match_type"],
                "score": best["score"], "flags": best.get("flags", "")}

    if require_company_match and not company_in_text(company_name, best["title"], ""):
        return {**empty_result, "status": "not_found",
                "verified": False, "match_type": best["match_type"],
                "score": best["score"], "flags": best.get("flags", "")}

    return {
        "url": best["url"], "title": best["title"],
        "status": "found",
        "verified": best["verified"],
        "match_type": best["match_type"],
        "score": best["score"],
        "flags": best.get("flags", ""),
    }


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


def process_batch(batch_df, batch_num, output_dir, score_threshold=DEFAULT_SCORE_THRESHOLD,
                  resume=True, require_full_name=False, require_company_match=False):
    """
    Process a single batch of search queries.

    Args:
        batch_df:              DataFrame for this batch
        batch_num:             batch number (1-indexed)
        output_dir:            where to save results
        score_threshold:       minimum score to accept a result
        resume:                whether to resume from checkpoint
        require_full_name:     if True, reject results where first+last don't both match
        require_company_match: if True, reject results where company absent from result

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
    for col in ["linkedin_url", "linkedin_title", "search_status", "verified", "match_type", "score", "flags"]:
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
        company_name = row.get("company_name_clean", None)

        if (i + 1) % 10 == 0 or i == start_from:
            pct = 100 * (i + 1) / total
            print(f"    [{i + 1}/{total}] {pct:.0f}% ({found_count} found, {verified_count} verified)")

        result = search_linkedin_profile(
            query, person_name=person_name, company_name=company_name,
            score_threshold=score_threshold,
            require_full_name=require_full_name,
            require_company_match=require_company_match,
        )

        batch_df.iloc[i, batch_df.columns.get_loc("linkedin_url")] = result["url"]
        batch_df.iloc[i, batch_df.columns.get_loc("linkedin_title")] = result["title"]
        batch_df.iloc[i, batch_df.columns.get_loc("search_status")] = result["status"]
        batch_df.iloc[i, batch_df.columns.get_loc("verified")] = result.get("verified")
        batch_df.iloc[i, batch_df.columns.get_loc("match_type")] = result.get("match_type")
        batch_df.iloc[i, batch_df.columns.get_loc("score")] = result.get("score", 0)
        batch_df.iloc[i, batch_df.columns.get_loc("flags")] = result.get("flags", "")

        if result["url"]:
            found_count += 1
            consecutive_failures = 0
            if result.get("verified"):
                verified_count += 1

        if result["status"] == "quota_exceeded":
            quota_exceeded = True
            print(f"\n    Quota exceeded at query {i + 1}")
            break

        # Track consecutive failures (not_found is OK, errors are not)
        if result["status"] in ("exception", "error_429", "error_403"):
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                quota_exceeded = True
                print(f"\n    {MAX_CONSECUTIVE_FAILURES} consecutive failures -- assuming quota exhausted")
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
    print(f"\n  {found_count}/{total} URLs found ({100 * found_count / total:.1f}%)")
    if found_count > 0:
        print(f"  {verified_count}/{found_count} verified ({100 * verified_count / found_count:.1f}%)")

    # Score distribution
    scores = batch_df["score"].dropna()
    if len(scores) > 0:
        print(f"  Score stats: mean={scores.mean():.1f}, median={scores.median():.0f}, "
              f"min={scores.min():.0f}, max={scores.max():.0f}")

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

    cost = len(df) * 2 / 1000  # ~$1 per 1000 queries, 2 queries per person
    hours = len(df) * DELAY_BETWEEN_REQUESTS * 2 / 3600  # 2 queries per person
    print(f"\n  Estimated cost:     ${cost:.0f} (Serper.dev, 2 queries/person)")
    print(f"  Estimated time:     {hours:.1f} hours")

    print(f"\n  Sample queries:")
    for _, row in df.head(5).iterrows():
        print(f"    \"{row['search_query']}\"")


def cmd_prototype(input_path, n, output_dir, score_threshold=DEFAULT_SCORE_THRESHOLD,
                  require_full_name=False, require_company_match=False):
    """Test with a small number of queries."""
    check_credentials()
    df = prepare_input(input_path)

    sample = df.head(n).copy()
    print(f"\nPrototype: searching {len(sample)} people")
    print(f"  Estimated time: {len(sample) * DELAY_BETWEEN_REQUESTS / 60:.1f} minutes")
    print(f"  Score threshold: {score_threshold}")
    if require_full_name:
        print(f"  Require full name match: ON")
    if require_company_match:
        print(f"  Require company match: ON")

    sample_df, _ = process_batch(sample, 0, output_dir, score_threshold=score_threshold,
                                 resume=False, require_full_name=require_full_name,
                                 require_company_match=require_company_match)

    print(f"\nResults:")
    for _, row in sample_df.iterrows():
        name = row.get("person_name_clean", "?")
        company = row.get("company_name_clean", "?")
        url = row.get("linkedin_url")
        verified = row.get("verified")
        score = row.get("score", 0)
        if pd.notna(url):
            v = "+" if verified else "?"
            print(f"  {v} {name} ({company}) [score={score:.0f}]")
            print(f"    -> {url}")
        else:
            print(f"  - {name} ({company}) [score={score:.0f}] NOT FOUND")


def cmd_run(input_path, output_dir, score_threshold=DEFAULT_SCORE_THRESHOLD,
            resume=False, auto_yes=False, require_full_name=False, require_company_match=False):
    """Run the full search across all batches."""
    check_credentials()
    df = prepare_input(input_path)

    n_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
    cost = len(df) * 2 / 1000
    checkpoint_dir = get_checkpoint_dir(output_dir)
    completed = get_completed_batches(checkpoint_dir)

    print(f"\n{'=' * 60}")
    print(f"  Queries:    {len(df):,}")
    print(f"  Batches:    {n_batches} ({len(completed)} already done)")
    print(f"  Est. cost:  ${cost:.0f} (Serper.dev)")
    print(f"  Threshold:  {score_threshold}")
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

        _, quota_exceeded = process_batch(
            batch_df, batch_num, output_dir,
            score_threshold=score_threshold, resume=resume,
            require_full_name=require_full_name,
            require_company_match=require_company_match,
        )

        if quota_exceeded:
            print(f"\nQuota exceeded. Resume with --resume")
            break

        # Brief pause between batches
        if batch_num < n_batches:
            time.sleep(5)

    # Check if all done
    completed = get_completed_batches(checkpoint_dir)
    if len(completed) == n_batches:
        print("\nAll batches complete. Running --combine...")
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
                status = f"+ {found}/{batch_size} found, {verified} verified"
            else:
                status = "+ Complete"
        else:
            cp = load_checkpoint(checkpoint_dir, batch_num)
            if cp:
                status = f"~ {cp['queries_processed']}/{batch_size} done"
            else:
                status = "  Not started"

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
        print("  No batch files found")
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

    print(f"\n  Combined {len(batch_files)} files -> {output_file}")
    print(f"    Total: {total:,}, URLs: {found:,} ({100 * found / total:.1f}%), Verified: {verified:,}")

    # Score distribution
    if "score" in combined.columns:
        scores = combined["score"].dropna()
        if len(scores) > 0:
            print(f"    Score stats: mean={scores.mean():.1f}, median={scores.median():.0f}")


def cmd_verify(output_dir, apply_filter=False):
    """Run verification on combined results."""
    combined_file = output_dir / "all_linkedin_urls.csv"
    if not combined_file.exists():
        print(f"  {combined_file} not found. Run --combine first.")
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
    print(f"\n  Updated: {combined_file}")

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
        print(f"\n  Filtered version: {verified_file}")
        print(f"    {n_removed} unverified URLs removed")
        print(f"    Use this file for scraping.")


# =========================
# Main
# =========================

def main():
    global BATCH_SIZE
    parser = argparse.ArgumentParser(
        description="Find LinkedIn profile URLs via Serper.dev",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 find_urls_serper.py --input all_people.csv --stats
  python3 find_urls_serper.py --input all_people.csv --prototype 5
  python3 find_urls_serper.py --input all_people.csv --run
  python3 find_urls_serper.py --input all_people.csv --run --yes
  python3 find_urls_serper.py --input all_people.csv --resume
  python3 find_urls_serper.py --input all_people.csv --status
  python3 find_urls_serper.py --input all_people.csv --combine
  python3 find_urls_serper.py --input all_people.csv --verify
  python3 find_urls_serper.py --input all_people.csv --verify --apply
        """,
    )

    parser.add_argument("--input", type=str, required=True,
                        help="Input CSV with person_name and company_name columns")
    parser.add_argument("--output", type=str, default=None,
                        help="Output directory (default: derived from input filename)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Queries per batch (default: {BATCH_SIZE})")
    parser.add_argument("--score-threshold", type=int, default=DEFAULT_SCORE_THRESHOLD,
                        help=f"Minimum score to accept a result (default: {DEFAULT_SCORE_THRESHOLD})")

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
    parser.add_argument("--require-full-name", action="store_true",
                        help="Reject results where first+last name don't both appear in title")
    parser.add_argument("--require-company-match", action="store_true",
                        help="Reject results where company name is absent from title/snippet")

    args = parser.parse_args()

    # Override global batch size if specified
    BATCH_SIZE = args.batch_size

    # Resolve paths
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        raise SystemExit(1)

    output_dir = get_output_dir(input_path, args.output)

    print("=" * 60)
    print("LinkedIn URL Finder (Serper.dev)")
    print("=" * 60)
    print(f"  Input:  {input_path}")
    print(f"  Output: {output_dir}")

    # Dispatch
    if args.stats:
        cmd_stats(input_path)
    elif args.prototype is not None:
        cmd_prototype(input_path, args.prototype, output_dir, score_threshold=args.score_threshold,
                      require_full_name=args.require_full_name,
                      require_company_match=args.require_company_match)
    elif args.run:
        cmd_run(input_path, output_dir, score_threshold=args.score_threshold,
                resume=False, auto_yes=args.yes,
                require_full_name=args.require_full_name,
                require_company_match=args.require_company_match)
    elif args.resume:
        cmd_run(input_path, output_dir, score_threshold=args.score_threshold,
                resume=True, auto_yes=args.yes,
                require_full_name=args.require_full_name,
                require_company_match=args.require_company_match)
    elif args.status:
        cmd_status(input_path, output_dir)
    elif args.combine:
        cmd_combine(output_dir)
    elif args.verify:
        cmd_verify(output_dir, apply_filter=args.apply)


if __name__ == "__main__":
    main()

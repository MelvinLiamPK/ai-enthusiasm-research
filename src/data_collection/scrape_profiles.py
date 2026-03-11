#!/usr/bin/env python3
"""
Scrape LinkedIn Profiles via Apify
====================================

Generic LinkedIn profile scraper using Apify's linkedin-profile-batch-scraper.
Accepts any CSV containing a LinkedIn URL column (typically the output of
find_urls.py), scrapes profile details (experience, education, location, etc.),
and saves structured output with checkpoint/resume for long runs on Sherlock.

Companion to scrape_posts.py — same input file, different data:
    scrape_posts.py     → what people say  (post text, engagement)
    scrape_profiles.py  → who people are   (jobs, education, location)

Filtering:
    If the input CSV has a 'verified' column (from find_urls.py), only rows
    with verified=True are scraped by default. Use --no-filter to override.

Auto-detected URL column (first match wins):
    linkedin_url, profile_url, url, LinkedIn URL

Output files (saved to --output directory):
    profiles_YYYYMMDD_HHMMSS.csv         - one row per profile (flat fields)
    experience_YYYYMMDD_HHMMSS.csv       - one row per job (linked by profile_url)
    education_YYYYMMDD_HHMMSS.csv        - one row per degree (linked by profile_url)
    failed_profiles_YYYYMMDD_HHMMSS.csv  - profiles that could not be scraped
    profiles_raw_YYYYMMDD_HHMMSS.json    - raw Apify response
    .scrape_profiles_checkpoint.json     - resumable state (auto-cleaned)

Usage:
    python3 scrape_profiles.py --input urls.csv --stats           # Preview
    python3 scrape_profiles.py --input urls.csv --prototype 5      # Test 5 profiles
    python3 scrape_profiles.py --input urls.csv --run              # Full run
    python3 scrape_profiles.py --input urls.csv --run --yes        # SLURM (no prompt)
    python3 scrape_profiles.py --input urls.csv --resume           # Resume
    python3 scrape_profiles.py --input urls.csv --run --no-filter  # Include unverified

Prerequisites:
    pip install apify-client pandas python-dotenv
    APIFY_API_TOKEN in .env file (searched upward from CWD, or via --env)

Apify actor:
    apimaestro/linkedin-profile-batch-scraper-no-cookies-required
    Cost: ~$5 per 1,000 profiles
"""

import os
import sys
import json
import time
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------

from dotenv import load_dotenv

try:
    from apify_client import ApifyClient
except ImportError:
    print("=" * 60)
    print("ERROR: apify_client not installed!")
    print("=" * 60)
    print("\n  pip install apify-client")
    sys.exit(1)


# ===========================================================================
# Configuration defaults (overridable via CLI)
# ===========================================================================

APIFY_ACTOR = "apimaestro/linkedin-profile-batch-scraper-no-cookies-required"
DEFAULT_BATCH_SIZE = 100   # profiles per Apify run (actor max is 1000)
DELAY_BETWEEN_BATCHES = 10  # seconds

URL_COLUMN_CANDIDATES = ["linkedin_url", "profile_url", "url", "LinkedIn URL"]


# ===========================================================================
# Environment / credentials  (shared pattern with scrape_posts.py)
# ===========================================================================

def _find_env_file(start_dir, explicit=None):
    """Search upward from *start_dir* for .env, or use an explicit path."""
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None

    d = Path(start_dir).resolve()
    for _ in range(10):
        candidate = d / ".env"
        if candidate.exists():
            return candidate
        parent = d.parent
        if parent == d:
            break
        d = parent
    return None


def _load_credentials(env_path=None):
    """Load APIFY_API_TOKEN and return an initialised client."""
    env_file = _find_env_file(Path.cwd(), env_path)
    if env_file:
        load_dotenv(env_file)
        print(f"  .env:   {env_file}")
    else:
        print("  .env:   (not found — relying on environment variables)")

    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("\n✗ APIFY_API_TOKEN not set.")
        print("  Add it to your .env or export it:")
        print("    export APIFY_API_TOKEN=your_token_here")
        sys.exit(1)

    return ApifyClient(token)


# ===========================================================================
# Input loading & filtering  (shared pattern with scrape_posts.py)
# ===========================================================================

def _detect_column(df, candidates, required=False, label="column"):
    """Return the first column name in *candidates* that exists in *df*."""
    for col in candidates:
        if col in df.columns:
            return col
    for col in df.columns:
        if "linkedin" in col.lower() and "url" in col.lower():
            return col
    if required:
        print(f"\n✗ Could not find {label} column.")
        print(f"  Looked for: {candidates}")
        print(f"  Available:  {list(df.columns)}")
        sys.exit(1)
    return None


def load_input(filepath, filter_verified=True):
    """Load input CSV and apply verified filtering.

    Returns (df, url_col).
    """
    filepath = Path(filepath)
    if not filepath.exists():
        print(f"\n✗ Input file not found: {filepath}")
        sys.exit(1)

    print(f"  Input:  {filepath}")
    df = pd.read_csv(filepath)
    print(f"  Rows:   {len(df):,}   Columns: {len(df.columns)}")

    url_col = _detect_column(df, URL_COLUMN_CANDIDATES,
                             required=True, label="LinkedIn URL")
    print(f"  URL column: {url_col}")

    # Verified filtering
    if filter_verified and "verified" in df.columns:
        before = len(df)
        df["verified"] = df["verified"].astype(str).str.strip().str.lower() == "true"
        df = df[df["verified"]].copy()
        dropped = before - len(df)
        print(f"  Filtered to verified=True: {len(df):,} (dropped {dropped:,} unverified)")
    elif filter_verified:
        print(f"  No 'verified' column — scraping all rows with URLs")

    # Drop rows without a URL
    before = len(df)
    df = df[df[url_col].notna()].copy()
    if len(df) < before:
        print(f"  Dropped {before - len(df):,} rows with missing URL")

    return df, url_col


def _get_unique_urls(df, url_col):
    """Deduplicate URLs preserving order."""
    raw = df[url_col].tolist()
    unique = list(dict.fromkeys(raw))
    if len(unique) < len(raw):
        print(f"  Deduplicated: {len(raw):,} → {len(unique):,} unique URLs")
    return unique


def _normalise_url(url):
    """Strip query params and trailing slash for consistent matching."""
    if not url:
        return ""
    return url.split("?")[0].rstrip("/")


# ===========================================================================
# Statistics / preview
# ===========================================================================

def print_stats(df, url_col, batch_size):
    """Print scraping preview without API calls."""
    unique = df[url_col].nunique()
    n_batches = (unique + batch_size - 1) // batch_size

    print(f"\n{'=' * 70}")
    print("PROFILE SCRAPING PREVIEW")
    print("=" * 70)
    print(f"\n  Input rows (after filter): {len(df):,}")
    print(f"  Unique LinkedIn URLs:      {unique:,}")
    print(f"  Batch size:                {batch_size} profiles")
    print(f"  Batches:                   {n_batches}")

    # Cost estimate
    cost = unique / 1000 * 5
    print(f"\n  Est. Apify cost:  ${cost:,.2f}  (at ~$5/1k profiles)")
    print(f"  Est. time:        {n_batches * 2}–{n_batches * 5} min")


# ===========================================================================
# Apify integration
# ===========================================================================

def _call_apify(client, profile_urls, batch_num=0, n_batches=0):
    """Run the Apify actor for one batch. Returns list of dataset items or None."""
    run_input = {
        "usernames": profile_urls,
    }
    try:
        run = client.actor(APIFY_ACTOR).call(run_input=run_input)
        run_id = run.get("id")
        status = run.get("status")
        print(f"      Run {run_id}: {status}")

        if status != "SUCCEEDED":
            print(f"      ✗ Run failed ({status})")
            return None

        dataset_id = run.get("defaultDatasetId")
        items = list(client.dataset(dataset_id).iterate_items())
        print(f"      ✓ {len(items):,} items retrieved")
        return items

    except Exception as e:
        print(f"      ✗ Apify error: {e}")
        return None


def _scrape_batches(client, urls, batch_size, checkpoint_cb=None):
    """Scrape *urls* in mini-batches with checkpointing."""
    all_results = []
    total = len(urls)
    n_batches = (total + batch_size - 1) // batch_size

    print(f"\n{'=' * 70}")
    print(f"SCRAPING {total:,} PROFILES IN {n_batches} BATCHES")
    print("=" * 70)

    for i in range(0, total, batch_size):
        batch_num = i // batch_size + 1
        batch_urls = urls[i : i + batch_size]
        print(f"\n  Batch {batch_num}/{n_batches}  ({len(batch_urls)} profiles)")

        items = _call_apify(client, batch_urls, batch_num, n_batches)
        if items:
            all_results.extend(items)
            print(f"      Running total: {len(all_results):,} items")
        else:
            print(f"      ⚠  Batch failed — continuing")

        if checkpoint_cb:
            checkpoint_cb(all_results, i + len(batch_urls))

        if i + batch_size < total:
            print(f"      Waiting {DELAY_BETWEEN_BATCHES}s …")
            time.sleep(DELAY_BETWEEN_BATCHES)

    return all_results


# ===========================================================================
# Result parsing
# ===========================================================================
#
# The Apify actor returns dataset items. Based on the docs, each batch
# returns an item with structure:
#
#   {
#     "results": {
#       "username": {
#         "basic_info": { "location": { "country", "city", "full" } },
#         "experience": [{ "title", "company", "duration" }],
#         "education": [{ "school", "degree" }],
#         "certifications": [...],
#         "languages": [...]
#       }
#     },
#     "failedUsernames": [],
#     "totalProcessed": N,
#     "totalFailed": N
#   }
#
# However, actual field names may differ — the prototype run will confirm.
# The parser below is defensive and logs what it finds.
# ===========================================================================

def _extract_username_from_url(url):
    """Extract LinkedIn username from a URL for matching against results keys."""
    url = _normalise_url(url)
    # https://www.linkedin.com/in/username -> username
    parts = url.rstrip("/").split("/")
    if "in" in parts:
        idx = parts.index("in")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return parts[-1] if parts else ""


def _parse_results(raw_items, submitted_urls):
    """Parse Apify dataset items into structured lists.

    Returns (profiles_list, experience_list, education_list, failed_list).
    """
    profiles_list = []
    experience_list = []
    education_list = []
    failed_list = []

    # Build username → submitted_url lookup for joining
    username_to_url = {}
    for url in submitted_urls:
        uname = _extract_username_from_url(url)
        if uname:
            username_to_url[uname.lower()] = url

    for item in raw_items:
        # --- Case A: Batch wrapper with "results" dict ---
        if "results" in item and isinstance(item["results"], dict):
            results_dict = item["results"]
            failed_usernames = item.get("failedUsernames", [])

            for username, profile_data in results_dict.items():
                profile_url = username_to_url.get(username.lower(),
                              f"https://www.linkedin.com/in/{username}")

                _parse_single_profile(username, profile_url, profile_data,
                                      profiles_list, experience_list,
                                      education_list)

            for uname in failed_usernames:
                url = username_to_url.get(uname.lower(),
                      f"https://www.linkedin.com/in/{uname}")
                failed_list.append({
                    "profile_url": url,
                    "username": uname,
                    "reason": "failed",
                })

        # --- Case B: Each item IS a profile (flat structure) ---
        # Some actors return one item per profile instead of a batch wrapper.
        elif any(k in item for k in ("basic_info", "experience", "education",
                                      "firstName", "first_name", "headline")):
            # Try to identify the URL
            profile_url = (
                item.get("profileUrl")
                or item.get("profile_url")
                or item.get("linkedin_url")
                or item.get("url")
                or ""
            )
            username = (
                item.get("username")
                or _extract_username_from_url(profile_url)
                or f"unknown_{len(profiles_list)}"
            )
            if not profile_url:
                profile_url = username_to_url.get(username.lower(), "")

            _parse_single_profile(username, profile_url, item,
                                  profiles_list, experience_list,
                                  education_list)

        # --- Case C: Unknown structure — log for debugging ---
        else:
            print(f"    ⚠ Unknown item structure. Keys: {sorted(item.keys())[:10]}")

    return profiles_list, experience_list, education_list, failed_list


def _parse_single_profile(username, profile_url, data,
                           profiles_list, experience_list, education_list):
    """Parse one profile's data into the output lists."""

    # --- Basic info ---
    basic = data.get("basic_info") or {}
    location = basic.get("location") or {}

    # Some actors put fields at top level, some nest under basic_info
    profile_row = {
        "profile_url": profile_url,
        "username": username,
        "first_name": (basic.get("first_name") or data.get("first_name")
                       or data.get("firstName") or ""),
        "last_name": (basic.get("last_name") or data.get("last_name")
                      or data.get("lastName") or ""),
        "headline": (basic.get("headline") or data.get("headline") or ""),
        "summary": (basic.get("summary") or data.get("summary")
                    or data.get("about") or ""),
        "location": location.get("full", "") or data.get("location", ""),
        "location_city": location.get("city", ""),
        "location_country": location.get("country", ""),
        "industry": basic.get("industry") or data.get("industry") or "",
        "connections": (basic.get("connections") or data.get("connections")
                        or data.get("connectionsCount") or ""),
        "followers": (basic.get("followers") or data.get("followers")
                      or data.get("followersCount") or ""),
        "profile_picture": (basic.get("profile_picture")
                            or data.get("profilePicture")
                            or data.get("profile_picture") or ""),
    }
    profiles_list.append(profile_row)

    # --- Experience ---
    experiences = data.get("experience") or data.get("experiences") or []
    if isinstance(experiences, list):
        for i, exp in enumerate(experiences):
            exp_row = {
                "profile_url": profile_url,
                "username": username,
                "exp_order": i,
                "title": exp.get("title") or exp.get("jobTitle") or "",
                "company": exp.get("company") or exp.get("companyName") or "",
                "company_url": (exp.get("companyUrl") or exp.get("company_url")
                                or exp.get("companyLinkedinUrl") or ""),
                "duration": exp.get("duration") or "",
                "date_range": exp.get("dateRange") or exp.get("date_range") or "",
                "location": exp.get("location") or "",
                "description": exp.get("description") or "",
            }
            experience_list.append(exp_row)

    # --- Education ---
    educations = data.get("education") or data.get("educations") or []
    if isinstance(educations, list):
        for i, edu in enumerate(educations):
            edu_row = {
                "profile_url": profile_url,
                "username": username,
                "edu_order": i,
                "school": (edu.get("school") or edu.get("schoolName")
                           or edu.get("institution") or ""),
                "degree": edu.get("degree") or edu.get("degreeName") or "",
                "field_of_study": (edu.get("fieldOfStudy")
                                   or edu.get("field_of_study") or ""),
                "date_range": edu.get("dateRange") or edu.get("date_range") or "",
                "description": edu.get("description") or "",
            }
            education_list.append(edu_row)


def _save_results(raw_items, output_dir, submitted_urls):
    """Parse and save profile data as JSON + CSVs.

    Returns dict with paths and counts.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. Raw JSON (always save first — safety net)
    raw_path = output_dir / f"profiles_raw_{ts}.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(raw_items, f, indent=2, ensure_ascii=False)
    print(f"\n  ✓ Raw JSON:      {raw_path}")

    # Debug: structure check
    if raw_items:
        first = raw_items[0]
        print(f"    First item keys: {sorted(first.keys())[:10]}")
        if "results" in first:
            n_profiles = len(first["results"])
            n_failed = len(first.get("failedUsernames", []))
            print(f"    Batch wrapper: {n_profiles} profiles, {n_failed} failed")
        else:
            print(f"    Structure: flat (one item per profile)")

    # 2. Parse
    profiles, experience, education, failed = _parse_results(
        raw_items, submitted_urls)

    # 3. Profiles CSV
    profiles_path = None
    if profiles:
        profiles_df = pd.DataFrame(profiles)
        profiles_path = output_dir / f"profiles_{ts}.csv"
        profiles_df.to_csv(profiles_path, index=False, encoding="utf-8")
        print(f"  ✓ Profiles CSV:  {profiles_path}  ({len(profiles_df):,} rows)")

    # 4. Experience CSV
    experience_path = None
    if experience:
        exp_df = pd.DataFrame(experience)
        experience_path = output_dir / f"experience_{ts}.csv"
        exp_df.to_csv(experience_path, index=False, encoding="utf-8")
        print(f"  ✓ Experience CSV: {experience_path}  ({len(exp_df):,} rows)")

    # 5. Education CSV
    education_path = None
    if education:
        edu_df = pd.DataFrame(education)
        education_path = output_dir / f"education_{ts}.csv"
        edu_df.to_csv(education_path, index=False, encoding="utf-8")
        print(f"  ✓ Education CSV: {education_path}  ({len(edu_df):,} rows)")

    # 6. Failed profiles CSV
    failed_path = None
    if failed:
        failed_df = pd.DataFrame(failed)
        failed_path = output_dir / f"failed_profiles_{ts}.csv"
        failed_df.to_csv(failed_path, index=False, encoding="utf-8")
        print(f"  ✓ Failed CSV:    {failed_path}  ({len(failed_df):,} rows)")

    return {
        "raw_path": str(raw_path),
        "profiles_path": str(profiles_path) if profiles_path else None,
        "experience_path": str(experience_path) if experience_path else None,
        "education_path": str(education_path) if education_path else None,
        "failed_path": str(failed_path) if failed_path else None,
        "profiles_count": len(profiles),
        "experience_count": len(experience),
        "education_count": len(education),
        "failed_count": len(failed),
        "timestamp": ts,
    }


# ===========================================================================
# Checkpoint management
# ===========================================================================

def _checkpoint_path(output_dir):
    return Path(output_dir) / ".scrape_profiles_checkpoint.json"


def _save_checkpoint(results, profiles_done, output_dir):
    """Persist current progress to disk."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    temp_file = output_dir / "temp_profiles_results.json"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False)

    cp_file = _checkpoint_path(output_dir)
    with open(cp_file, "w") as f:
        json.dump({
            "profiles_processed": profiles_done,
            "results_file": str(temp_file),
            "timestamp": datetime.now().isoformat(),
        }, f, indent=2)
    print(f"      💾 Checkpoint: {profiles_done} profiles done")


def _load_checkpoint(output_dir):
    """Load previous checkpoint if it exists."""
    cp_file = _checkpoint_path(output_dir)
    if not cp_file.exists():
        return None

    with open(cp_file) as f:
        cp = json.load(f)

    temp_file = cp.get("results_file")
    if temp_file and Path(temp_file).exists():
        with open(temp_file) as f:
            cp["previous_results"] = json.load(f)
    else:
        cp["previous_results"] = []

    return cp


def _clear_checkpoint(output_dir):
    """Remove checkpoint files after successful completion."""
    for name in [".scrape_profiles_checkpoint.json",
                 "temp_profiles_results.json"]:
        p = Path(output_dir) / name
        if p.exists():
            p.unlink()


# ===========================================================================
# Main scraping orchestration
# ===========================================================================

def run_scraping(client, df, url_col, output_dir, batch_size,
                 resume=True, prototype_limit=None):
    """Main entry point for scraping."""
    output_dir = Path(output_dir)

    if prototype_limit:
        unique_urls = _get_unique_urls(df, url_col)[:prototype_limit]
        df = df[df[url_col].isin(unique_urls)].copy()
        print(f"  PROTOTYPE MODE: {len(unique_urls)} profiles")
    else:
        unique_urls = _get_unique_urls(df, url_col)

    urls = unique_urls

    # Resume handling
    start_from = 0
    previous_results = []
    if resume:
        cp = _load_checkpoint(output_dir)
        if cp:
            start_from = cp.get("profiles_processed", 0)
            previous_results = cp.get("previous_results", [])
            print(f"\n  ✓ Resuming from checkpoint: {start_from} profiles already done")

    if start_from >= len(urls):
        print("\n  ✓ All profiles already scraped!")
        return

    # Scrape
    remaining = urls[start_from:]

    def checkpoint_cb(results, done):
        _save_checkpoint(results, start_from + done, output_dir)

    new_results = _scrape_batches(client, remaining, batch_size,
                                  checkpoint_cb)

    all_results = previous_results + new_results

    # Save final output
    print(f"\n{'=' * 70}")
    print("SAVING FINAL RESULTS")
    print("=" * 70)

    info = _save_results(all_results, output_dir, submitted_urls=urls)
    _clear_checkpoint(output_dir)

    # Summary
    print(f"\n{'=' * 70}")
    print("✅ PROFILE SCRAPING COMPLETE")
    print("=" * 70)
    print(f"  Profiles:   {info['profiles_count']:,}")
    print(f"  Experience: {info['experience_count']:,} entries")
    print(f"  Education:  {info['education_count']:,} entries")
    if info["failed_count"]:
        print(f"  Failed:     {info['failed_count']:,}")
    if info["profiles_path"]:
        print(f"  Profiles CSV:  {info['profiles_path']}")
    if info["experience_path"]:
        print(f"  Experience CSV: {info['experience_path']}")
    if info["education_path"]:
        print(f"  Education CSV:  {info['education_path']}")
    if info.get("failed_path"):
        print(f"  Failed CSV:     {info['failed_path']}")
    print(f"  Raw JSON:       {info['raw_path']}")


# ===========================================================================
# SLURM job helper
# ===========================================================================

def generate_slurm_script(args, output_dir):
    """Write a SLURM job script for Sherlock."""
    input_path = Path(args.input).resolve()
    script_path = Path(__file__).resolve()
    output_dir = Path(output_dir)

    slurm = f"""#!/bin/bash
#SBATCH --job-name=linkedin-scrape-profiles
#SBATCH --partition=normal
#SBATCH --time=24:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --output={output_dir}/slurm_%j.log

# LinkedIn Profile Scraper — SLURM job
# Generated: {datetime.now().isoformat()}

module load python/3.12

cd $HOME/ai-enthusiasm-research

# Activate virtual environment
source venv/bin/activate

# Run scraper (--yes skips confirmation, auto-resumes on restart)
python3 {script_path} \\
    --input {input_path} \\
    --output {output_dir} \\
    --batch-size {args.batch_size} \\
    --run --yes

echo "Done: $(date)"
"""
    output_dir.mkdir(parents=True, exist_ok=True)
    slurm_path = output_dir / "scrape_profiles_job.sh"
    with open(slurm_path, "w") as f:
        f.write(slurm)
    print(f"\n  ✓ SLURM script: {slurm_path}")
    print(f"    Submit with:  sbatch {slurm_path}")


# ===========================================================================
# CLI
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape LinkedIn profile details via Apify",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview what will be scraped
  python3 scrape_profiles.py --input all_linkedin_urls.csv --stats

  # Test with 5 profiles
  python3 scrape_profiles.py --input all_linkedin_urls.csv --prototype 5

  # Full run (non-interactive for SLURM)
  python3 scrape_profiles.py --input all_linkedin_urls.csv --run --yes

  # Resume after interruption
  python3 scrape_profiles.py --input all_linkedin_urls.csv --resume

  # Include unverified URLs
  python3 scrape_profiles.py --input all_linkedin_urls.csv --run --no-filter

  # Generate SLURM job script
  python3 scrape_profiles.py --input all_linkedin_urls.csv --slurm
        """,
    )

    parser.add_argument("--input", "-i", required=True,
                        help="Path to CSV with LinkedIn URLs")
    parser.add_argument("--output", "-o", default=None,
                        help="Output directory (default: <input_dir>/scraped_profiles/)")
    parser.add_argument("--env", default=None,
                        help="Path to .env file (default: search upward from CWD)")

    # Actions (mutually exclusive)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--stats", action="store_true",
                        help="Show scraping preview — no API calls")
    action.add_argument("--run", action="store_true",
                        help="Start scraping")
    action.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint")
    action.add_argument("--prototype", type=int, metavar="N",
                        help="Test with N profiles")
    action.add_argument("--slurm", action="store_true",
                        help="Generate a SLURM job script for Sherlock")

    # Options
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Profiles per Apify call (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--no-filter", action="store_true",
                        help="Scrape ALL rows with URLs (don't filter on verified)")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompts (for SLURM / automation)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore existing checkpoint, start fresh")

    args = parser.parse_args()

    # --- Resolve paths ---
    input_path = Path(args.input).resolve()
    if args.output:
        output_dir = Path(args.output).resolve()
    else:
        output_dir = input_path.parent / "scraped_profiles"

    print("=" * 70)
    print("LinkedIn Profile Scraper")
    print("=" * 70)

    # --- Load input ---
    filter_verified = not args.no_filter
    df, url_col = load_input(str(input_path), filter_verified=filter_verified)

    # --- SLURM script generation ---
    if args.slurm:
        print_stats(df, url_col, args.batch_size)
        generate_slurm_script(args, output_dir)
        return

    # --- Stats only ---
    if args.stats or not (args.run or args.resume or args.prototype):
        print_stats(df, url_col, args.batch_size)
        if not args.stats:
            parser.print_help()
        print("\n  Use --run to start, --prototype N to test, or --slurm for Sherlock.")
        return

    # --- Load credentials (only when actually calling the API) ---
    print(f"  Output: {output_dir}")
    client = _load_credentials(args.env)

    # --- Prototype ---
    if args.prototype:
        print(f"\n{'=' * 70}")
        print(f"PROTOTYPE MODE — {args.prototype} PROFILES")
        print("=" * 70)
        if not args.yes:
            confirm = input(f"\nScrape {args.prototype} profiles? (y/N): ").strip().lower()
            if confirm != "y":
                print("Cancelled.")
                return
        run_scraping(client, df, url_col, output_dir,
                     args.batch_size,
                     resume=False, prototype_limit=args.prototype)
        return

    # --- Run / Resume ---
    print_stats(df, url_col, args.batch_size)

    unique_count = df[url_col].nunique()
    if not args.yes and not args.resume:
        confirm = input(f"\nScrape {unique_count:,} profiles? (y/N): ").strip().lower()
        if confirm != "y":
            print("Cancelled.")
            return

    resume = args.resume or (args.run and not args.no_resume)
    run_scraping(client, df, url_col, output_dir,
                 args.batch_size, resume=resume)


if __name__ == "__main__":
    main()

"""
Step 3: Scrape LinkedIn Posts using Apify
=========================================
This script reads LinkedIn URLs from Step 2 and scrapes posts using Apify.

Prerequisites:
    - Run Step 1: python prepare_linkedin_queries.py --prototype --company "Apple"
    - Run Step 2: python find_linkedin_urls.py --prototype --company "Apple"
    - Apify API token in .env file

Usage:
    python scrape_linkedin_posts.py --prototype --company "Apple"
    python scrape_linkedin_posts.py --batch 1
    python scrape_linkedin_posts.py --batch all

Input:
    Prototype: data/processed/linkedin_urls/prototype_{company}_urls.csv (from Step 2)
    Batch:     data/processed/linkedin_urls/batch_001_urls.csv, ...

Output:
    Prototype: data/processed/linkedin_posts/prototype_{company}/
    Batch:     data/processed/linkedin_posts/batch_001/
"""

import os
import sys
import json
import time
import argparse
import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# =========================
# Path Configuration
# =========================
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

# Load environment variables
load_dotenv(PROJECT_ROOT / ".env")

APIFY_API_TOKEN = os.getenv('APIFY_API_TOKEN')

if not APIFY_API_TOKEN:
    print("=" * 60)
    print("ERROR: APIFY_API_TOKEN not found in .env file!")
    print("=" * 60)
    print(f"\nLooking for .env at: {PROJECT_ROOT / '.env'}")
    print("\nPlease add to your .env file:")
    print("  APIFY_API_TOKEN=your_apify_token")
    print("\nGet your token at: https://console.apify.com/account/integrations")
    sys.exit(1)

# Paths
URL_RESULTS_PATH = PROJECT_ROOT / "data" / "processed" / "linkedin_urls"
POSTS_RESULTS_PATH = PROJECT_ROOT / "data" / "processed" / "linkedin_posts"
CHECKPOINT_PATH = PROJECT_ROOT / "data" / "processed" / "checkpoints"

# Apify Actor (batch version for multiple profiles)
ACTOR_POST_SCRAPER = "apimaestro~linkedin-batch-profile-posts-scraper"


# =========================
# Apify Functions
# =========================

def call_apify_actor(actor_id, input_data, timeout_secs=600):
    """Call an Apify actor and wait for results."""
    print(f"\n    [Apify] Starting actor: {actor_id}")
    print(f"    [Apify] Input: {json.dumps(input_data, indent=2)}")
    
    url = f"https://api.apify.com/v2/acts/{actor_id}/runs"
    headers = {
        "Authorization": f"Bearer {APIFY_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=input_data,
            params={"timeout": timeout_secs}
        )
        
        if response.status_code != 201:
            print(f"    [Apify] Error starting actor: {response.status_code}")
            print(f"    [Apify] Response: {response.text[:500]}")
            return None
        
        run_data = response.json()['data']
        run_id = run_data['id']
        print(f"    [Apify] Run started: {run_id}")
        
        # Poll for completion
        status_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
        start_time = time.time()
        
        while True:
            if time.time() - start_time > timeout_secs:
                print(f"    [Apify] Timeout after {timeout_secs}s")
                return None
            
            status_response = requests.get(status_url, headers=headers)
            status = status_response.json()['data']['status']
            
            if status == 'SUCCEEDED':
                print(f"    [Apify] ✓ Run completed successfully")
                break
            elif status in ['FAILED', 'ABORTED', 'TIMED-OUT']:
                print(f"    [Apify] ✗ Run failed with status: {status}")
                return None
            else:
                elapsed = int(time.time() - start_time)
                print(f"    [Apify] Status: {status}... ({elapsed}s elapsed)")
                time.sleep(10)
        
        # Get results
        dataset_id = status_response.json()['data']['defaultDatasetId']
        results_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
        
        results_response = requests.get(results_url, headers=headers)
        results = results_response.json()
        
        print(f"    [Apify] Retrieved {len(results)} results")
        return results
        
    except Exception as e:
        print(f"    [Apify] Error: {e}")
        return None


def scrape_posts(profile_urls, total_posts_per_profile=3):
    """Scrape posts from LinkedIn profiles using batch scraper."""
    # Input format for batch scraper
    input_data = {
        "usernames": profile_urls,  # Actor expects 'usernames' field
        "total_posts": total_posts_per_profile,  # Limit posts per profile to save credits
    }
    
    return call_apify_actor(ACTOR_POST_SCRAPER, input_data) or []


# =========================
# Save Results
# =========================

def save_results(data, output_dir, prefix, requested_urls=None):
    """Save scraped data in multiple formats."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Raw JSON
    raw_path = output_dir / f"{prefix}_raw_{timestamp}.json"
    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    print(f"    ✓ Raw JSON: {raw_path}")
    
    # 2. Parse posts - handle the actual Apify response format
    posts_list = []
    profiles_seen = set()
    
    # Normalize requested URLs for comparison
    requested_usernames = set()
    if requested_urls:
        for url in requested_urls:
            # Extract username from URL like "linkedin.com/in/username"
            if '/in/' in url:
                username = url.split('/in/')[-1].strip('/').lower()
                requested_usernames.add(username)
    
    for item in data:
        # Get author info
        author = item.get('author', {})
        author_username = author.get('username', '').lower()
        author_name = f"{author.get('first_name', '')} {author.get('last_name', '')}".strip()
        profile_url = author.get('profile_url', '')
        
        # Filter: only include posts from requested profiles
        if requested_usernames and author_username not in requested_usernames:
            continue
        
        # Track profiles
        if author_username:
            profiles_seen.add((author_username, author_name, author.get('headline', '')))
        
        # Parse post data
        posted_at = item.get('posted_at', {})
        stats = item.get('stats', {})
        
        posts_list.append({
            'profile_url': profile_url,
            'profile_username': author_username,
            'profile_name': author_name,
            'profile_headline': author.get('headline', ''),
            'post_url': item.get('url', ''),
            'post_date': posted_at.get('date', ''),
            'post_text': item.get('text', ''),
            'post_type': item.get('post_type', ''),
            'likes': stats.get('total_reactions', 0),
            'comments': stats.get('comments', 0),
            'reposts': stats.get('reposts', 0),
        })
    
    # 3. Save posts CSV
    if posts_list:
        posts_df = pd.DataFrame(posts_list)
        posts_path = output_dir / f"{prefix}_posts_{timestamp}.csv"
        posts_df.to_csv(posts_path, index=False, encoding='utf-8')
        print(f"    ✓ Posts CSV: {posts_path} ({len(posts_df)} posts)")
    else:
        print(f"    ⚠ No posts found from requested profiles")
        print(f"      (API returned {len(data)} posts total, but none matched requested URLs)")
    
    # 4. Save profiles CSV
    if profiles_seen:
        profiles_list = [{'username': u, 'name': n, 'headline': h} for u, n, h in profiles_seen]
        profiles_df = pd.DataFrame(profiles_list)
        profiles_path = output_dir / f"{prefix}_profiles_{timestamp}.csv"
        profiles_df.to_csv(profiles_path, index=False, encoding='utf-8')
        print(f"    ✓ Profiles CSV: {profiles_path}")
    
    return len(posts_list)


# =========================
# Prototype Mode
# =========================

def run_prototype(company_search):
    """Step 3 Prototype: Scrape posts for a single company."""
    print("=" * 60)
    print("🔬 STEP 3: Scrape LinkedIn Posts (Apify)")
    print("=" * 60)
    
    # Find the input file from Step 2
    input_file = None
    for f in URL_RESULTS_PATH.glob("prototype_*_urls.csv"):
        if company_search.lower().replace(' ', '_') in f.name.lower():
            input_file = f
            break
    
    # Also try listing all prototype files
    if not input_file:
        prototype_files = list(URL_RESULTS_PATH.glob("prototype_*_urls.csv"))
        if prototype_files:
            print(f"\n[!] Could not find exact match for '{company_search}'")
            print(f"    Available URL files:")
            for i, f in enumerate(prototype_files[:10], 1):
                print(f"      {i}. {f.name}")
            
            selection = input("\nEnter number to select (or Enter to cancel): ").strip()
            if selection.isdigit() and 1 <= int(selection) <= len(prototype_files):
                input_file = prototype_files[int(selection) - 1]
        else:
            print(f"\n✗ No URL files found in {URL_RESULTS_PATH}")
            print(f"\n  Run Steps 1 and 2 first:")
            print(f"  python prepare_linkedin_queries.py --prototype --company \"{company_search}\"")
            print(f"  python find_linkedin_urls.py --prototype --company \"{company_search}\"")
            return None
    
    if not input_file or not input_file.exists():
        print(f"\n✗ No URL file found for '{company_search}'")
        print(f"\n  Run Step 2 first:")
        print(f"  python find_linkedin_urls.py --prototype --company \"{company_search}\"")
        return None
    
    print(f"\n[1/3] Loading URLs from Step 2...")
    print(f"      Input: {input_file}")
    df = pd.read_csv(input_file)
    print(f"      Loaded {len(df)} directors")
    
    # Get company name
    company_name = df['company_name'].iloc[0] if 'company_name' in df.columns else company_search
    print(f"      Company: {company_name}")
    
    # Filter to profiles with URLs
    profiles_with_urls = df[df['linkedin_url'].notna()]['linkedin_url'].tolist()
    
    if not profiles_with_urls:
        print(f"\n✗ No LinkedIn URLs found in the input file!")
        print(f"  The Google search in Step 2 may not have found any profiles.")
        return None
    
    print(f"      Profiles with URLs: {len(profiles_with_urls)}")
    
    # Show which profiles we'll scrape
    print(f"\n[2/3] Profiles to scrape:")
    print("-" * 60)
    url_to_name = dict(zip(df['linkedin_url'], df.get('director_name_clean', df.get('director_name', ['Unknown']*len(df)))))
    for url in profiles_with_urls:
        name = url_to_name.get(url, 'Unknown')
        print(f"  • {name}")
        print(f"    {url}")
    
    # Scrape posts
    print(f"\n[3/3] Scraping posts from {len(profiles_with_urls)} profiles...")
    print("-" * 60)
    
    posts_data = scrape_posts(profiles_with_urls, total_posts_per_profile=3)
    
    # Save results
    print("\nSaving results...")
    safe_name = input_file.stem.replace('_urls', '')  # e.g., prototype_APPLE_INC
    output_dir = POSTS_RESULTS_PATH / safe_name
    
    # Also save the URL mapping
    url_mapping_path = output_dir / "url_mapping.csv"
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(url_mapping_path, index=False)
    print(f"    ✓ URL mapping: {url_mapping_path}")
    
    posts_count = 0
    if posts_data:
        posts_count = save_results(posts_data, output_dir, "posts", requested_urls=profiles_with_urls)
    else:
        print("    ⚠ No posts retrieved (profiles may have no public posts)")
    
    # Summary
    print(f"\n{'=' * 60}")
    print("✅ STEP 3 COMPLETE - PROTOTYPE FINISHED!")
    print("=" * 60)
    print(f"Company: {company_name}")
    print(f"Directors: {len(df)}")
    print(f"Profiles scraped: {len(profiles_with_urls)}")
    print(f"Posts retrieved: {posts_count}")
    print(f"Output directory: {output_dir}")
    
    return {
        'company': company_name,
        'directors': len(df),
        'profiles_scraped': len(profiles_with_urls),
        'posts_count': posts_count,
        'output_dir': output_dir,
    }


# =========================
# Batch Mode
# =========================

def get_completed_batches():
    """Get list of already completed batch numbers."""
    checkpoint_file = CHECKPOINT_PATH / "post_batches_completed.txt"
    if not checkpoint_file.exists():
        return set()
    
    completed = set()
    with open(checkpoint_file, 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            if parts:
                completed.add(int(parts[0]))
    return completed


def save_checkpoint(batch_num, posts_count):
    """Save batch completion checkpoint."""
    CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)
    checkpoint_file = CHECKPOINT_PATH / "post_batches_completed.txt"
    with open(checkpoint_file, 'a') as f:
        f.write(f"{batch_num},{datetime.now().isoformat()},{posts_count}\n")


def run_batch(batch_num):
    """Process a single batch file."""
    url_file = URL_RESULTS_PATH / f"batch_{batch_num:03d}_urls.csv"
    
    if not url_file.exists():
        print(f"✗ URL file not found: {url_file}")
        print("Run find_linkedin_urls.py --batch {batch_num} first.")
        return None
    
    print(f"\n{'=' * 60}")
    print(f"Processing Batch {batch_num}")
    print(f"{'=' * 60}")
    
    df = pd.read_csv(url_file)
    profiles_with_urls = df[df['linkedin_url'].notna()]['linkedin_url'].tolist()
    
    if not profiles_with_urls:
        print("No LinkedIn URLs found in batch.")
        return None
    
    print(f"Profiles to scrape: {len(profiles_with_urls)}")
    
    posts_data = scrape_posts(profiles_with_urls)
    
    posts_count = 0
    if posts_data:
        output_dir = POSTS_RESULTS_PATH / f"batch_{batch_num:03d}"
        posts_count = save_results(posts_data, output_dir, f"batch_{batch_num:03d}")
    
    save_checkpoint(batch_num, posts_count)
    
    print(f"\n✅ Batch {batch_num} complete! ({posts_count} posts)")
    return posts_count


def run_all_batches():
    """Process all batch files."""
    url_files = sorted(URL_RESULTS_PATH.glob("batch_*_urls.csv"))
    
    if not url_files:
        print("✗ No URL files found. Run find_linkedin_urls.py first.")
        return
    
    print(f"Found {len(url_files)} batch URL files")
    
    completed = get_completed_batches()
    print(f"Already completed: {len(completed)} batches")
    
    for url_file in url_files:
        batch_num = int(url_file.stem.split('_')[1])
        
        if batch_num in completed:
            print(f"\nSkipping batch {batch_num} (already completed)")
            continue
        
        run_batch(batch_num)
        
        print("\nWaiting 30s before next batch...")
        time.sleep(30)
    
    print("\n✅ All batches complete!")


# =========================
# Main
# =========================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Step 3: Scrape LinkedIn posts using Apify')
    parser.add_argument('--prototype', action='store_true', help='Run prototype mode')
    parser.add_argument('--company', type=str, default=None, help='Company name')
    parser.add_argument('--batch', type=str, default=None, help='Batch number or "all"')
    args = parser.parse_args()
    
    if args.prototype or args.company:
        if not args.company:
            args.company = input("Enter company name: ").strip()
        run_prototype(args.company)
    
    elif args.batch:
        if args.batch.lower() == 'all':
            run_all_batches()
        else:
            run_batch(int(args.batch))
    
    else:
        parser.print_help()
        print("\n" + "=" * 60)
        print("Full Pipeline:")
        print("  Step 1: python prepare_linkedin_queries.py --prototype --company \"Apple\"")
        print("  Step 2: python find_linkedin_urls.py --prototype --company \"Apple\"")
        print("  Step 3: python scrape_linkedin_posts.py --prototype --company \"Apple\"")
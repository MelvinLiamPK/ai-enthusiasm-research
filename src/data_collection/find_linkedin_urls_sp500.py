"""
LinkedIn Profile URL Finder for S&P 500 Directors
==================================================
Uses Google Custom Search API to find LinkedIn profile URLs
from name/company search queries for S&P 500 board directors.

VERIFICATION:
    This script includes name verification to filter out wrong profiles.
    A URL is verified if the director's first OR last name appears in the
    LinkedIn profile title returned by Google. See linkedin_verification.py.

Prerequisites:
    - Google Cloud project with Custom Search API enabled
    - Custom Search Engine configured to search linkedin.com
    - API key and Search Engine ID in .env file
    - Run prepare_linkedin_queries_sp500.py first to generate batch files

Usage:
    python3 find_linkedin_urls_sp500.py --status                       # Check progress
    python3 find_linkedin_urls_sp500.py --prototype --company "Apple"  # Test one company
    python3 find_linkedin_urls_sp500.py --batch 1                      # Process batch 1
    python3 find_linkedin_urls_sp500.py --batch all                    # Process all batches
    python3 find_linkedin_urls_sp500.py --combine                      # Combine all results
    python3 find_linkedin_urls_sp500.py --verify                       # Verify existing URLs
    python3 find_linkedin_urls_sp500.py --verify --apply               # Verify and null out bad URLs
"""

import os
import sys
import json
import time
import argparse
import pandas as pd
import requests
from datetime import datetime

# Load credentials from .env file
from dotenv import load_dotenv

# Look for .env in project root (two levels up from src/data_collection/)
env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(env_path)

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_SEARCH_ENGINE_ID = os.getenv('GOOGLE_CSE_ID')

if not GOOGLE_API_KEY or not GOOGLE_SEARCH_ENGINE_ID:
    print("=" * 60)
    print("ERROR: Missing Google credentials in .env file!")
    print("=" * 60)
    print(f"\nLooked for .env at: {os.path.abspath(env_path)}")
    print("\nPlease ensure your .env file contains:")
    print("  GOOGLE_API_KEY=your_api_key")
    print("  GOOGLE_CSE_ID=your_search_engine_id")
    sys.exit(1)

# Import verification functions
try:
    from linkedin_verification import verify_director_match, verify_url_data
except ImportError:
    # If running from different directory, try adding current dir to path
    sys.path.insert(0, os.path.dirname(__file__))
    from linkedin_verification import verify_director_match, verify_url_data

# =========================
# Configuration
# =========================
SP500_DATA_PATH = "../../data/sp500/sp500_current_directors.csv"
QUERIES_PATH = "../../outputs/sp500_batches/"
RESULTS_PATH = "../../data/processed/sp500_linkedin_urls/"
CHECKPOINT_PATH = "../../data/processed/sp500_checkpoints/"

GOOGLE_SEARCH_URL = "https://www.googleapis.com/customsearch/v1"
DELAY_BETWEEN_REQUESTS = 1.5
SAVE_EVERY_N_QUERIES = 25


# =========================
# Google Search with Verification
# =========================

def search_linkedin_profile(query, director_name=None, company_name=None, retries=3):
    """
    Search for LinkedIn profile URL with comprehensive name+company verification.
    
    Returns the first result that scores >= 70 (name + company match).
    If no verified match found, returns first result with lower score.
    """
    search_query = f"{query} site:linkedin.com/in/"
    
    params = {
        'key': GOOGLE_API_KEY,
        'cx': GOOGLE_SEARCH_ENGINE_ID,
        'q': search_query,
        'num': 5,
    }
    
    for attempt in range(retries):
        try:
            response = requests.get(GOOGLE_SEARCH_URL, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                
                # If we have director name and company, try to find verified match (score >= 70)
                if director_name and company_name:
                    best_result = None
                    best_score = 0
                    
                    for item in items:
                        link = item.get('link', '')
                        title = item.get('title', '')
                        if 'linkedin.com/in/' in link:
                            result = verify_director_match(director_name, company_name, title)
                            
                            # If we find a verified match (score >= 70), return it immediately
                            if result['verified']:
                                return {
                                    'url': link,
                                    'title': title,
                                    'status': 'found',
                                    'match_score': result['match_score'],
                                    'verified': True,
                                    'quality_flag': result['quality_flag'],
                                    'match_type': result['match_type'],
                                    'name_matched': result['name_matched'],
                                    'company_matched': result['company_matched']
                                }
                            
                            # Track best unverified result as fallback
                            if result['match_score'] > best_score:
                                best_score = result['match_score']
                                best_result = {
                                    'url': link,
                                    'title': title,
                                    'status': 'found',
                                    'match_score': result['match_score'],
                                    'verified': False,
                                    'quality_flag': result['quality_flag'],
                                    'match_type': result['match_type'],
                                    'name_matched': result['name_matched'],
                                    'company_matched': result['company_matched']
                                }
                    
                    # No verified match found, return best unverified result
                    if best_result:
                        return best_result
                else:
                    # No verification info - return first result
                    for item in items:
                        link = item.get('link', '')
                        title = item.get('title', '')
                        if 'linkedin.com/in/' in link:
                            return {
                                'url': link,
                                'title': title,
                                'status': 'found',
                                'match_score': None,
                                'verified': None,
                                'quality_flag': None,
                                'match_type': None,
                                'name_matched': None,
                                'company_matched': None
                            }
                
                return {
                    'url': None, 
                    'title': None, 
                    'status': 'not_found',
                    'match_score': 0,
                    'verified': None,
                    'quality_flag': 'NO_MATCH',
                    'match_type': 'no_match',
                    'name_matched': False,
                    'company_matched': False
                }
            
            elif response.status_code == 429:
                print(f"      âš  Rate limited, waiting 60s...")
                time.sleep(60)
                continue
            
            elif response.status_code == 403:
                print(f"      âš  API quota exceeded")
                return {
                    'url': None, 'title': None, 'status': 'quota_exceeded',
                    'match_score': 0, 'verified': None, 'quality_flag': None,
                    'match_type': None, 'name_matched': None, 'company_matched': None
                }
            
            else:
                print(f"      Error {response.status_code}")
                return {
                    'url': None, 'title': None, 'status': f'error_{response.status_code}',
                    'match_score': 0, 'verified': None, 'quality_flag': None,
                    'match_type': None, 'name_matched': None, 'company_matched': None
                }
                
        except requests.exceptions.Timeout:
            print(f"      Timeout on attempt {attempt + 1}")
            time.sleep(2)
        except Exception as e:
            print(f"      Error: {e}")
            return {
                'url': None, 'title': None, 'status': 'exception',
                'match_score': 0, 'verified': None, 'quality_flag': None,
                'match_type': None, 'name_matched': None, 'company_matched': None
            }
    
    return {
        'url': None, 'title': None, 'status': 'max_retries',
        'match_score': 0, 'verified': None, 'quality_flag': None,
        'match_type': None, 'name_matched': None, 'company_matched': None
    }


def find_linkedin_urls_batch(queries_df, batch_name, delay=DELAY_BETWEEN_REQUESTS, 
                              start_from=0, checkpoint_callback=None):
    """Find LinkedIn URLs for a batch with checkpoint support and comprehensive verification."""
    queries = queries_df['search_query'].tolist()
    total = len(queries)
    
    # Get director names for verification
    if 'director_name_clean' in queries_df.columns:
        director_names = queries_df['director_name_clean'].tolist()
    elif 'director_name' in queries_df.columns:
        director_names = queries_df['director_name'].tolist()
    else:
        director_names = [None] * total
    
    # Get company names for verification
    if 'company_name_clean' in queries_df.columns:
        company_names = queries_df['company_name_clean'].tolist()
    elif 'company_name' in queries_df.columns:
        company_names = queries_df['company_name'].tolist()
    else:
        company_names = [None] * total
    
    print(f"\n[â†’] Processing batch: {batch_name}")
    print(f"    Total queries: {total}")
    if start_from > 0:
        print(f"    Resuming from query {start_from + 1}")
    print(f"    Estimated time: {(total - start_from) * delay / 60:.1f} minutes")
    
    # Initialize columns (including new match_score columns)
    new_cols = ['linkedin_url', 'linkedin_title', 'search_status', 'match_score', 
                'verified', 'quality_flag', 'match_type', 'name_matched', 'company_matched', 
                'board_keyword_matched', 'matched_board_keywords']
    for col in new_cols:
        if col not in queries_df.columns:
            queries_df[col] = None
    
    found_count = queries_df['linkedin_url'].notna().sum()
    verified_count = (queries_df['verified'] == True).sum()
    quota_exceeded = False
    
    for i in range(start_from, total):
        query = queries[i]
        director_name = director_names[i]
        company_name = company_names[i]
        
        if (i + 1) % 10 == 0 or i == start_from:
            pct = 100 * (i + 1) / total
            print(f"    [{i + 1}/{total}] {pct:.0f}% complete ({found_count} URLs, {verified_count} verified)")
        
        # Call with both name and company for comprehensive verification
        result = search_linkedin_profile(query, director_name=director_name, company_name=company_name)
        
        # Store all result fields
        queries_df.iloc[i, queries_df.columns.get_loc('linkedin_url')] = result['url']
        queries_df.iloc[i, queries_df.columns.get_loc('linkedin_title')] = result['title']
        queries_df.iloc[i, queries_df.columns.get_loc('search_status')] = result['status']
        queries_df.iloc[i, queries_df.columns.get_loc('match_score')] = result.get('match_score')
        queries_df.iloc[i, queries_df.columns.get_loc('verified')] = result.get('verified')
        queries_df.iloc[i, queries_df.columns.get_loc('quality_flag')] = result.get('quality_flag')
        queries_df.iloc[i, queries_df.columns.get_loc('match_type')] = result.get('match_type')
        queries_df.iloc[i, queries_df.columns.get_loc('name_matched')] = result.get('name_matched')
        queries_df.iloc[i, queries_df.columns.get_loc('company_matched')] = result.get('company_matched')
        queries_df.iloc[i, queries_df.columns.get_loc('board_keyword_matched')] = result.get('board_keyword_matched', False)
        
        # Store board keywords if present
        if result.get('matched_board_keywords'):
            keywords_str = ', '.join(result['matched_board_keywords'])
            queries_df.iloc[i, queries_df.columns.get_loc('matched_board_keywords')] = keywords_str
        
        if result['url']:
            found_count += 1
            if result.get('verified'):
                verified_count += 1
        
        if result['status'] == 'quota_exceeded':
            quota_exceeded = True
            print(f"\n    âš  Quota exceeded at query {i + 1}")
            break
        
        if checkpoint_callback and (i + 1) % SAVE_EVERY_N_QUERIES == 0:
            checkpoint_callback(queries_df, i + 1)
            print(f"      ðŸ’¾ Checkpoint saved at query {i + 1}")
        
        if i < total - 1 and not quota_exceeded:
            time.sleep(delay)
    
    found_count = queries_df['linkedin_url'].notna().sum()
    verified_count = (queries_df['verified'] == True).sum()
    
    print(f"\n    âœ“ Completed: {found_count}/{total} URLs found ({100*found_count/total:.1f}%)")
    if found_count > 0:
        print(f"    âœ“ Verified: {verified_count}/{found_count} ({100*verified_count/found_count:.1f}%)")
    
    return queries_df, quota_exceeded

# =========================
# Batch Management
# =========================

def get_completed_batches():
    checkpoint_file = os.path.join(CHECKPOINT_PATH, "completed_batches.txt")
    if not os.path.exists(checkpoint_file):
        return set()
    completed = set()
    with open(checkpoint_file, 'r') as f:
        for line in f:
            if line.strip():
                completed.add(int(line.strip().split(',')[0]))
    return completed


def load_checkpoint(batch_num):
    checkpoint_file = os.path.join(CHECKPOINT_PATH, f"batch_{batch_num:03d}_checkpoint.json")
    if not os.path.exists(checkpoint_file):
        return None
    with open(checkpoint_file, 'r') as f:
        return json.load(f)


def save_batch_checkpoint(batch_num, df, queries_processed):
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)
    progress_file = os.path.join(CHECKPOINT_PATH, f"batch_{batch_num:03d}_progress.csv")
    df.to_csv(progress_file, index=False)
    checkpoint_file = os.path.join(CHECKPOINT_PATH, f"batch_{batch_num:03d}_checkpoint.json")
    with open(checkpoint_file, 'w') as f:
        json.dump({
            'batch_num': batch_num,
            'queries_processed': queries_processed,
            'timestamp': datetime.now().isoformat(),
            'progress_file': progress_file
        }, f)


def process_batch_file(batch_num, resume=True):
    batch_file = os.path.join(QUERIES_PATH, f"batch_{batch_num:03d}_queries.csv")
    
    if not os.path.exists(batch_file):
        print(f"Ã¢ÂÅ’ Batch file not found: {batch_file}")
        return None
    
    print(f"\n{'='*60}")
    print(f"Processing Batch {batch_num}")
    print(f"{'='*60}")
    
    start_from = 0
    checkpoint = load_checkpoint(batch_num)
    
    if resume and checkpoint:
        start_from = checkpoint['queries_processed']
        progress_file = checkpoint['progress_file']
        if os.path.exists(progress_file):
            df = pd.read_csv(progress_file)
            print(f"    Resuming from checkpoint: {start_from} queries done")
        else:
            df = pd.read_csv(batch_file)
            start_from = 0
    else:
        df = pd.read_csv(batch_file)
    
    def checkpoint_callback(current_df, queries_done):
        save_batch_checkpoint(batch_num, current_df, queries_done)
    
    df, quota_exceeded = find_linkedin_urls_batch(
        df, f"batch_{batch_num:03d}",
        start_from=start_from,
        checkpoint_callback=checkpoint_callback
    )
    
    os.makedirs(RESULTS_PATH, exist_ok=True)
    results_file = os.path.join(RESULTS_PATH, f"batch_{batch_num:03d}_urls.csv")
    df.to_csv(results_file, index=False)
    print(f"\n    Saved: {results_file}")
    
    if not quota_exceeded:
        completion_file = os.path.join(CHECKPOINT_PATH, "completed_batches.txt")
        with open(completion_file, 'a') as f:
            found = df['linkedin_url'].notna().sum()
            verified = (df['verified'] == True).sum()
            f.write(f"{batch_num},{datetime.now().isoformat()},{found},{len(df)},{verified}\n")
        
        # Clean up checkpoints
        for f_path in [
            os.path.join(CHECKPOINT_PATH, f"batch_{batch_num:03d}_checkpoint.json"),
            os.path.join(CHECKPOINT_PATH, f"batch_{batch_num:03d}_progress.csv")
        ]:
            if os.path.exists(f_path):
                os.remove(f_path)
    
    return df


def combine_all_results():
    print("\n" + "="*60)
    print("Combining all results...")
    print("="*60)
    
    if not os.path.exists(RESULTS_PATH):
        print("Ã¢ÂÅ’ Results directory not found!")
        return
    
    all_files = sorted([f for f in os.listdir(RESULTS_PATH) if f.startswith('batch_') and f.endswith('_urls.csv')])
    
    if not all_files:
        print("Ã¢ÂÅ’ No batch result files found!")
        return
    
    dfs = []
    for f in all_files:
        df = pd.read_csv(os.path.join(RESULTS_PATH, f))
        dfs.append(df)
        print(f"    Loaded: {f} ({len(df)} rows)")
    
    combined = pd.concat(dfs, ignore_index=True)
    output_path = os.path.join(RESULTS_PATH, "all_sp500_linkedin_urls.csv")
    combined.to_csv(output_path, index=False)
    
    total = len(combined)
    found = combined['linkedin_url'].notna().sum()
    verified = (combined['verified'] == True).sum() if 'verified' in combined.columns else 0
    
    print(f"\nÃ¢Å“â€œ Combined {len(all_files)} files Ã¢â€ â€™ {output_path}")
    print(f"  Total: {total:,}, URLs: {found:,} ({100*found/total:.1f}%), Verified: {verified:,}")


def print_status():
    print("\n" + "=" * 60)
    print("S&P 500 LinkedIn URL Search - Status")
    print("=" * 60)
    
    if not os.path.exists(QUERIES_PATH):
        print(f"\nÃ¢ÂÅ’ Run prepare_linkedin_queries_sp500.py first.")
        return
    
    batch_files = sorted([f for f in os.listdir(QUERIES_PATH) if f.startswith('batch_') and f.endswith('_queries.csv')])
    
    if not batch_files:
        print("\nÃ¢ÂÅ’ No batch files found!")
        return
    
    completed = get_completed_batches()
    print(f"\nBatches: {len(batch_files)} total, {len(completed)} completed")
    print("-" * 60)
    
    total_queries, total_found, total_verified = 0, 0, 0
    
    for batch_file in batch_files:
        batch_num = int(batch_file.split('_')[1])
        df = pd.read_csv(os.path.join(QUERIES_PATH, batch_file))
        queries = len(df)
        total_queries += queries
        
        if batch_num in completed:
            results_file = os.path.join(RESULTS_PATH, f"batch_{batch_num:03d}_urls.csv")
            if os.path.exists(results_file):
                results_df = pd.read_csv(results_file)
                found = results_df['linkedin_url'].notna().sum()
                verified = (results_df['verified'] == True).sum() if 'verified' in results_df.columns else 0
                total_found += found
                total_verified += verified
                status = f"Ã¢Å“â€œ {found}/{queries} found, {verified} verified"
            else:
                status = "Ã¢Å“â€œ Complete"
        else:
            checkpoint = load_checkpoint(batch_num)
            status = f"Ã¢ÂÂ¸ {checkpoint['queries_processed']}/{queries} done" if checkpoint else "Ã¢â€”â€¹ Not started"
        
        print(f"  Batch {batch_num}: {status}")
    
    print("-" * 60)
    print(f"\nTotal: {total_found:,}/{total_queries:,} URLs ({100*total_found/total_queries:.1f}%)")
    if total_found > 0:
        print(f"Verified: {total_verified:,}/{total_found:,} ({100*total_verified/total_found:.1f}%)")


def run_verification_cmd(apply_filter=False):
    """Run verification on combined results."""
    print("\n" + "=" * 60)
    print("LinkedIn URL Verification")
    print("=" * 60)
    
    combined_file = os.path.join(RESULTS_PATH, "all_sp500_linkedin_urls.csv")
    
    if not os.path.exists(combined_file):
        print(f"\nÃ¢ÂÅ’ File not found: {combined_file}")
        print("Run --combine first.")
        return
    
    print(f"\nLoading: {combined_file}")
    df = pd.read_csv(combined_file)
    print(f"Total records: {len(df):,}")
    
    # Run verification (adds columns, doesn't filter yet)
    df = verify_url_data(df, apply_filter=False)
    
    # Always save with verification columns (preserves URLs)
    df.to_csv(combined_file, index=False)
    print(f"\nÃ¢Å“â€œ Updated with verification columns: {combined_file}")
    
    if apply_filter:
        # Create a COPY for filtering, preserve original
        df_filtered = df.copy()
        
        # Null out unverified URLs in the copy
        unverified_mask = (df_filtered['verified'] == False) & (df_filtered['linkedin_url'].notna())
        unverified_count = unverified_mask.sum()
        
        df_filtered.loc[unverified_mask, 'linkedin_url'] = None
        df_filtered.loc[unverified_mask, 'search_status'] = 'unverified'
        
        # Save filtered version to NEW file (not overwriting original)
        verified_file = os.path.join(RESULTS_PATH, "all_sp500_linkedin_urls_verified.csv")
        df_filtered.to_csv(verified_file, index=False)
        print(f"Ã¢Å“â€œ Saved filtered data to: {verified_file}")
        print(f"  ({unverified_count} unverified URLs removed)")
        print(f"\n  NOTE: Original file preserved with all URLs intact.")
        print(f"  Use the _verified.csv file for scraping.")
    
    # Summary
    total = len(df)
    urls_found = df['linkedin_url'].notna().sum()
    verified_count = (df['verified'] == True).sum()
    
    print(f"\n{'='*60}")
    print(f"Total: {total:,}, URLs: {urls_found:,}, Verified: {verified_count:,}")
    if urls_found > 0:
        print(f"Verification rate: {100*verified_count/urls_found:.1f}% of found URLs")


def run_prototype(company_name):
    """Test with single company."""
    print("=" * 60)
    print("Ã°Å¸â€Â¬ PROTOTYPE MODE")
    print("=" * 60)
    print(f"\nCompany: {company_name}")
    
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data_processing'))
    from prepare_linkedin_queries_sp500 import clean_director_name, clean_company_name, generate_search_query
    
    if not os.path.exists(SP500_DATA_PATH):
        print(f"\nÃ¢ÂÅ’ Data not found: {SP500_DATA_PATH}")
        return
    
    df = pd.read_csv(SP500_DATA_PATH)
    df['director_name_clean'] = df['director_name'].apply(clean_director_name)
    df['company_name_clean'] = df['company_name'].apply(clean_company_name)
    
    companies = df['company_name'].unique()
    matches = [c for c in companies if company_name.lower() in c.lower()]
    
    if not matches:
        print(f"\nÃ¢ÂÅ’ No match for '{company_name}'")
        return
    
    selected = matches[0] if len(matches) == 1 else matches[0]
    if len(matches) > 1:
        print(f"\nFound {len(matches)} matches:")
        for i, m in enumerate(matches[:10], 1):
            print(f"  {i}. {m}")
        sel = input("Select (Enter for first): ").strip()
        if sel.isdigit() and 1 <= int(sel) <= len(matches):
            selected = matches[int(sel) - 1]
    
    print(f"\nÃ¢Å“â€œ Selected: {selected}")
    
    company_df = df[df['company_name'] == selected].copy()
    company_df['search_query'] = company_df.apply(generate_search_query, axis=1)
    
    print(f"\nDirectors ({len(company_df)}):")
    for _, row in company_df.iterrows():
        print(f"  Ã¢â‚¬Â¢ {row['director_name_clean']}")
    
    print(f"\nThis uses {len(company_df)} API queries.")
    if input("Proceed? (y/N): ").strip().lower() != 'y':
        return
    
    company_df, _ = find_linkedin_urls_batch(company_df, f"prototype_{company_name}")
    
    print(f"\nResults:")
    for _, row in company_df.iterrows():
        if pd.notna(row.get('linkedin_url')):
            v = "Ã¢Å“â€œ" if row.get('verified') else "Ã¢Å¡Â "
            print(f"  {v} {row['director_name_clean']}")
            print(f"    Ã¢â€ â€™ {row['linkedin_url']}")
        else:
            print(f"  Ã¢Å“â€” {row['director_name_clean']}")
    
    os.makedirs(RESULTS_PATH, exist_ok=True)
    safe_name = company_name.replace(' ', '_')[:30]
    output = os.path.join(RESULTS_PATH, f"prototype_{safe_name}_urls.csv")
    company_df.to_csv(output, index=False)
    
    found = company_df['linkedin_url'].notna().sum()
    verified = (company_df['verified'] == True).sum()
    print(f"\nÃ¢Å“â€¦ Done! {found}/{len(company_df)} found, {verified} verified")
    print(f"Saved: {output}")


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description='Find LinkedIn URLs for S&P 500 directors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 find_linkedin_urls_sp500.py --status
  python3 find_linkedin_urls_sp500.py --prototype --company "Apple"
  python3 find_linkedin_urls_sp500.py --batch 1
  python3 find_linkedin_urls_sp500.py --batch all
  python3 find_linkedin_urls_sp500.py --combine
  python3 find_linkedin_urls_sp500.py --verify
  python3 find_linkedin_urls_sp500.py --verify --apply
        """
    )
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--prototype', action='store_true')
    parser.add_argument('--company', type=str)
    parser.add_argument('--batch', type=str)
    parser.add_argument('--combine', action='store_true')
    parser.add_argument('--verify', action='store_true')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--no-resume', action='store_true')
    args = parser.parse_args()
    
    if args.status:
        print_status()
    elif args.verify:
        run_verification_cmd(apply_filter=args.apply)
    elif args.combine:
        combine_all_results()
    elif args.prototype or args.company:
        run_prototype(args.company or input("Company name: ").strip())
    elif args.batch:
        if not os.path.exists(QUERIES_PATH):
            print(f"Ã¢ÂÅ’ Run prepare_linkedin_queries_sp500.py first.")
            return
        
        batch_files = sorted([f for f in os.listdir(QUERIES_PATH) if f.startswith('batch_') and f.endswith('_queries.csv')])
        completed = get_completed_batches()
        resume = not args.no_resume
        
        if args.batch.lower() == 'all':
            for i in range(1, len(batch_files) + 1):
                if i in completed:
                    print(f"\nSkipping batch {i} (done)")
                    continue
                result = process_batch_file(i, resume=resume)
                if result is None:
                    break
                if 'quota_exceeded' in result.get('search_status', pd.Series()).values:
                    print("\nÃ¢Å¡Â  Quota exceeded. Continue tomorrow.")
                    break
                if i < len(batch_files):
                    time.sleep(5)
            print_status()
            if len(get_completed_batches()) == len(batch_files):
                combine_all_results()
        else:
            process_batch_file(int(args.batch), resume=resume)
    else:
        parser.print_help()
        print("\nQuick start:")
        print("  1. python3 find_linkedin_urls_sp500.py --status")
        print("  2. python3 find_linkedin_urls_sp500.py --verify --apply")


if __name__ == "__main__":
    main()
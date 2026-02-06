# LinkedIn Director Posts Scraper

A pipeline to scrape LinkedIn posts from corporate directors for AI enthusiasm research.

## Overview

This pipeline extracts LinkedIn posts from S&P 500 company directors in four steps:

| Step | Script | Purpose | API Used |
|------|--------|---------|----------|
| 0 | `build_sp500_directors.py` | Build S&P 500 directors dataset | WRDS |
| 1 | `prepare_linkedin_queries_sp500.py` | Clean names, generate search queries | None |
| 2 | `find_linkedin_urls_sp500.py` | Find LinkedIn profile URLs | Google Custom Search |
| 3 | `scrape_linkedin_posts_sp500.py` | Scrape posts from profiles | Apify |

> **Note:** There are two versions of each script:
> - **S&P 500 version** (`*_sp500.py`) - Pilot/prototype with ~9,400 directors (~$90)
> - **Full version** - All ~176,000 directors (~$1,600)
> 
> Run the S&P 500 pilot first to validate the pipeline before scaling to the full dataset.

## Project Structure

```
ai-enthusiasm-research/
├── .env                                    # API credentials (create this)
├── .gitignore
├── README.md
├── requirements.txt
├── config/
│   └── config_template.py
├── data/
│   ├── raw/
│   │   └── directors.csv                   # Input: WRDS director data (all companies)
│   ├── sp500/                              # Step 0 output: S&P 500 filtered data
│   │   ├── sp500_directors.csv             # All director-year records (~52k rows)
│   │   ├── sp500_current_directors.csv     # Most recent year per director (~9.4k rows)
│   │   ├── sp500_companies.csv             # S&P 500 company list (~500 companies)
│   │   └── sp500_constituents.csv          # Full S&P 500 membership info
│   ├── processed/
│   │   ├── sp500/                          # Step 1 processed output
│   │   ├── sp500_linkedin_urls/            # Step 2 output
│   │   ├── sp500_linkedin_posts/           # Step 3 output
│   │   ├── sp500_checkpoints/              # Progress tracking for S&P 500
│   │   ├── linkedin_urls/                  # Original workflow output
│   │   ├── linkedin_posts/                 # Original workflow output
│   │   └── checkpoints/                    # Original workflow checkpoints
│   └── samples/
├── docs/
├── notebooks/
├── outputs/
│   ├── sp500_batches/                      # Step 1 output: S&P 500 batch files
│   └── *.csv                               # Original workflow batch files
└── src/
    ├── analysis/
    ├── data_collection/
    │   ├── build_sp500_directors.py        # Step 0: Build S&P 500 dataset
    │   ├── find_linkedin_urls.py           # Step 2 (full)
    │   ├── find_linkedin_urls_sp500.py     # Step 2 (pilot)
    │   ├── scrape_linkedin_posts.py        # Step 3 (full)
    │   └── scrape_linkedin_posts_sp500.py  # Step 3 (pilot)
    └── data_processing/
        ├── prepare_linkedin_queries.py     # Step 1 (full)
        └── prepare_linkedin_queries_sp500.py # Step 1 (pilot)
```

---

## Setup

### 1. Install Dependencies

```bash
pip3 install pandas requests python-dotenv wrds
```

Or use the requirements file:
```bash
pip3 install -r requirements.txt
```

### 2. Configure WRDS Access (for Step 0)

You need a WRDS account with access to CRSP and ExecuComp.

Create a `~/.pgpass` file:
```
wrds-pgdata.wharton.upenn.edu:9737:wrds:your_username:your_password
```

Set permissions:
```bash
chmod 600 ~/.pgpass
```

### 3. Configure API Credentials (for Steps 2-3)

Create a `.env` file in the project root:

```
GOOGLE_API_KEY=your_google_api_key
GOOGLE_CSE_ID=your_custom_search_engine_id
APIFY_API_TOKEN=your_apify_token
```

### 4. Get API Keys

**Google Custom Search API:**
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project and enable "Custom Search API"
3. Create an API key at APIs & Services → Credentials
4. Go to [Programmable Search Engine](https://programmablesearchengine.google.com/)
5. Create a search engine for `linkedin.com`
6. Copy the Search Engine ID

**Apify:**
1. Sign up at [Apify.com](https://apify.com/)
2. Go to Account → Integrations
3. Copy your API token

---

## S&P 500 Pilot 

This pilot workflow processes S&P 500 company directors (~9,400 director-company pairs) to validate the pipeline before scaling to all companies.

### Cost & Time Summary

| Step | Queries | Cost | Time |
|------|---------|------|------|
| Step 0: Build dataset | N/A | Free (WRDS) | ~2 min |
| Step 1: Prepare queries | N/A | Free | ~30 sec |
| Step 2: Find URLs | 9,423 | **~$47** | ~4 hours |
| Step 3: Scrape posts | ~8,000* | **~$40** | ~2 hours |
| **Total** | | **~$87** | ~6 hours |

*Assuming ~85% URL match rate

---

### Step 0: Build S&P 500 Directors Dataset

```bash
cd /Users/melvinliam/Documents/Uni/RA-NB/Scraping/ai-enthusiasm-research

# Build the dataset (uses most recent S&P 500 constituents)
python3 src/data_collection/build_sp500_directors.py

# Or specify a date
python3 src/data_collection/build_sp500_directors.py --date 2024-12-31
```

**Output:** `data/sp500/`
| File | Description | Rows |
|------|-------------|------|
| `sp500_current_directors.csv` | Main input for pipeline | ~9,400 |
| `sp500_directors.csv` | All director-year records | ~52,000 |
| `sp500_companies.csv` | Company list | ~500 |
| `sp500_constituents.csv` | S&P 500 membership info | ~500 |

---

### Step 1: Prepare Search Queries

```bash
cd src/data_processing

# Generate all batch files
python3 prepare_linkedin_queries_sp500.py

# Or test with one company first
python3 prepare_linkedin_queries_sp500.py --prototype --company "Apple"
```

**Output:** `outputs/sp500_batches/`
- `batch_001_queries.csv` through `batch_010_queries.csv` (~1,000 queries each)
- `all_search_queries.csv` (combined)

---

### Step 2: Find LinkedIn URLs (Google API)

```bash
cd src/data_collection

# Check status
python3 find_linkedin_urls_sp500.py --status

# Test with one company (~10 API calls)
python3 find_linkedin_urls_sp500.py --prototype --company "Apple"

# Process batches
python3 find_linkedin_urls_sp500.py --batch 1      # Single batch
python3 find_linkedin_urls_sp500.py --batch all    # All batches

# Combine results when done
python3 find_linkedin_urls_sp500.py --combine
```

**Features:**
- ✅ Checkpoint system - saves every 25 queries, resume anytime
- ✅ Status command - see progress across all batches
- ✅ Quota detection - stops gracefully when daily limit hit
- ✅ Auto-resume - continues from last checkpoint

**Output:** `data/processed/sp500_linkedin_urls/`
| File | Description |
|------|-------------|
| `batch_001_urls.csv` | Results for batch 1 |
| `all_sp500_linkedin_urls.csv` | Combined results |

---

### Step 3: Scrape LinkedIn Posts (Apify)

```bash
cd src/data_collection

# Check status
python3 scrape_linkedin_posts_sp500.py --status

# Test with one company
python3 scrape_linkedin_posts_sp500.py --prototype --company "Apple"

# Process batches
python3 scrape_linkedin_posts_sp500.py --batch 1
python3 scrape_linkedin_posts_sp500.py --batch all

# Combine results
python3 scrape_linkedin_posts_sp500.py --combine
```

**Output:** `data/processed/sp500_linkedin_posts/`

---

## Full Workflow (All Companies)

After validating the S&P 500 pilot, run this workflow for the complete dataset (~176k director records).

### Step 1: Prepare Search Queries

```bash
cd src/data_processing
python3 prepare_linkedin_queries.py

# Or prototype mode
python3 prepare_linkedin_queries.py --prototype --company "Apple"
```

### Step 2: Find LinkedIn URLs

```bash
cd src/data_collection
python3 find_linkedin_urls.py --batch 1
python3 find_linkedin_urls.py --batch all
```

### Step 3: Scrape LinkedIn Posts

```bash
cd src/data_collection
python3 scrape_linkedin_posts.py --batch 1
python3 scrape_linkedin_posts.py --batch all
```

---

## API Limits & Costs

| Service | Free Tier | Paid Rate |
|---------|-----------|-----------|
| Google Custom Search | 100 queries/day | $5 per 1,000 queries |
| Apify | $5 free credit | ~$5 per 1,000 profiles |

### Cost Comparison

| Workflow | Directors | Google Cost | Apify Cost | Total |
|----------|-----------|-------------|------------|-------|
| **S&P 500 Pilot** | 9,423 | ~$47 | ~$40 | **~$87** |
| **Full Dataset** | 176,758 | ~$880 | ~$750 | **~$1,630** |

---

## Output Files

### Posts CSV (`*_posts_*.csv`)

The main output for analysis:

| Column | Description |
|--------|-------------|
| `profile_url` | LinkedIn profile URL |
| `profile_name` | Director's name |
| `post_url` | URL of the post |
| `post_date` | When the post was made |
| `post_text` | Full text content of the post |
| `likes` | Number of likes |
| `comments` | Number of comments |
| `reposts` | Number of reposts |
| `post_type` | Type of post |

### URL Mapping (`*_urls.csv`)

Links directors to their LinkedIn profiles:

| Column | Description |
|--------|-------------|
| `gvkey` | Company identifier |
| `ticker` | Stock ticker |
| `company_name_clean` | Company name |
| `director_name_clean` | Cleaned name |
| `search_query` | Query used for Google search |
| `linkedin_url` | Found LinkedIn URL (or empty) |
| `linkedin_title` | Google result title (for verification) |
| `search_status` | found / not_found / error |

---

## Checkpointing & Resume

### S&P 500 Workflow

Progress is saved to `data/processed/sp500_checkpoints/`:
- `batch_001_checkpoint.csv` - Partial results for in-progress batch
- `batch_001_progress.json` - Last processed index
- `completed_batches.txt` - List of completed batches

**To check progress:**
```bash
python3 find_linkedin_urls_sp500.py --status
```

**To resume after interruption:**
```bash
# Just run the same command - it auto-resumes
python3 find_linkedin_urls_sp500.py --batch 1
```

**To restart a batch from scratch:**
```bash
python3 find_linkedin_urls_sp500.py --batch 1 --no-resume
```

### Original Workflow

Progress is saved to `data/processed/checkpoints/`:
- `completed_batches.txt` - Completed URL search batches
- `scraping_progress.csv` - Completed post scraping batches

---

## Troubleshooting

### WRDS Connection Issues (Step 0)
- Ensure `~/.pgpass` is configured with correct credentials
- Check file permissions: `chmod 600 ~/.pgpass`
- Test connection: `python3 -c "import wrds; db = wrds.Connection(); print('Connected!')"`

### "S&P 500 data not found"
- Run Step 0 first: `python3 src/data_collection/build_sp500_directors.py`
- Check that `data/sp500/sp500_current_directors.csv` exists

### "No batch files found"
- Run Step 1 first: `python3 src/data_processing/prepare_linkedin_queries_sp500.py`
- Check that `outputs/sp500_batches/` contains CSV files

### "No LinkedIn URLs found"
- Check your Google Custom Search Engine is configured for `linkedin.com`
- Test API in browser: `https://www.googleapis.com/customsearch/v1?key=YOUR_KEY&cx=YOUR_CX&q=test`
- The director may not have a LinkedIn profile

### "API quota exceeded"
- Google: Wait until tomorrow (resets daily) or enable billing
- Apify: Add credits to your account
- The S&P 500 scripts save checkpoints, so just re-run to continue

### "No posts retrieved"
- The profile may have no public posts
- LinkedIn may have blocked the scraper temporarily

---

## Alternative Apify Actors

If `apimaestro/linkedin-profile-post-scraper` doesn't work, try:
- `anchor/linkedin-profile-scraper`
- `bebity/linkedin-profile-scraper`
- `harvest/linkedin-posts-scraper`

Update `ACTOR_POST_SCRAPER` in `scrape_linkedin_posts_sp500.py`.

---

## Data Sources (WRDS)

| WRDS Table | Purpose |
|------------|---------|
| `crsp.msp500list` | Monthly S&P 500 constituents (PERMNO, dates) |
| `crsp.ccmxpf_linktable` | Links CRSP PERMNO to Compustat GVKEY |
| `execcomp.directorcomp` | Director names and company info |

**Note:** CRSP S&P 500 data is updated periodically. If requesting future dates, the script automatically falls back to the most recent available data.
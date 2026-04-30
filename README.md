# AI Enthusiasm Among Corporate Leaders — LinkedIn Analysis Pipeline

Research pipeline measuring AI enthusiasm among corporate board directors, executives, and blockholders through sentiment analysis of their LinkedIn posts. Supervised by Nick Bloom (Stanford) and John Van Reenen (LSE).

## Research Question

Do corporate leaders who express genuine enthusiasm for AI on LinkedIn differ systematically from those who don't? This project builds a dataset linking individuals' public statements about AI to their corporate roles, enabling analysis of how AI sentiment varies across firms, industries, and governance positions.

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  1. DATA EXTRACTION (Mac, WRDS)                        COMPLETE │
│     build_directors.py  →  272,646 director-company-year rows   │
│     build_executives.py →  167,071 executive records            │
│     build_blockholders.py → 37,762 blockholder records          │
│                                                                 │
│  2. DEDUPLICATION (Mac)                                COMPLETE │
│     combine_people.py   →  96,968 unique person-company pairs   │
│                                                                 │
│  3. LINKEDIN URL DISCOVERY (Sherlock, Google API)      COMPLETE │
│     find_urls.py        →  42,527 verified LinkedIn URLs        │
│     ├── 96,971 queries, ~$485                                   │
│     └── ~10 days at 10,000 queries/day                          │
│                                                                 │
│  4. POST SCRAPING (Sherlock, Apify)                    COMPLETE │
│     scrape_posts.py     →  2.2M posts from 26,367 profiles      │
│     ├── Automatic pagination (up to 10,000 posts/profile)       │
│     ├── Checkpoint/resume for long runs                         │
│     └── Total cost: ~$5,500                                     │
│                                                                 │
│  5. SENTIMENT ANALYSIS                                 UPCOMING │
│     Loughran-McDonald dictionary + FinBERT                      │
│     AI keyword filtering vs baseline (e.g. COVID-19)            │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
ai-enthusiasm-research/
├── src/
│   ├── data_extraction/              # WRDS queries → CSV/DB files
│   │   ├── build_directors.py        #   ExecuComp directorcomp table
│   │   ├── build_executives.py       #   ExecuComp anncomp table
│   │   ├── build_blockholders.py     #   Schwartz-Ziv/Volkova SEC filings
│   │   └── combine_people.py         #   Merge + deduplicate all sources
│   ├── data_collection/              # LinkedIn URL discovery + scraping
│   │   ├── find_urls.py              #   Google Custom Search → LinkedIn URLs
│   │   ├── scrape_posts.py           #   Apify → LinkedIn posts (generic, SLURM-compatible)
│   │   └── linkedin_verification.py  #   Name-matching verification module
│   ├── data_processing/              # Data cleaning + merging
│   │   ├── merge_all_batches.py      #   Combine scrape batches into final dataset
│   │   ├── convert_temp_results.py   #   Convert JSONL temp files → CSV
│   │   ├── extract_remaining_urls.py #   Find unscraped profiles
│   │   ├── add_capped_to_remaining.py #  Add capped profiles to retry list
│   │   └── merge_posts_with_metadata.py # (legacy S&P 500 version)
│   └── data_analysis/                # Sentiment analysis
│       ├── analysisAI_LM.py          #   AI posts — L-M dictionary (S&P 500 pilot)
│       ├── covid_sentiment_LM.py     #   COVID baseline — L-M dictionary
│       ├── lm_dictionary_loader.py   #   L-M dictionary helper
│       ├── sanity_check.py           #   Data quality checks
│       └── inspect_posts.py          #   Post content inspection
├── data/
│   ├── raw/                          # Source data
│   │   └── blockholders.csv          #   Schwartz-Ziv/Volkova dataset
│   ├── extracted/                    # Output of build scripts
│   │   ├── directors/                #   directors_all.csv, directors_current.csv
│   │   ├── executives/               #   executives_all.csv, executives_current.csv
│   │   ├── blockholders/             #   blockholders_all.csv, blockholders_current.csv
│   │   └── combined/                 #   all_people.csv (96,968 rows)
│   ├── processed/                    # Output of collection + processing scripts
│   │   └── all_people_linkedin_urls/
│   │       ├── all_linkedin_urls.csv        # Full URL discovery results (96,971 rows)
│   │       ├── remaining_urls_final.csv     # Profiles not yet scraped / needing retry
│   │       ├── scraped_posts_batch2/        # Batch 2 output (28,600 profiles attempted)
│   │       ├── scraped_posts_batch3/        # Batch 3 output (14,041 profiles attempted)
│   │       └── scraped_posts_combined/      # ← FINAL MERGED DATASET
│   │           ├── posts_combined.csv       #   2,583,711 rows (2.8 GB)
│   │           ├── profiles_combined.csv    #   26,367 rows
│   │           ├── no_posts_combined.csv    #   16,161 profiles with zero posts
│   │           └── merge_report.txt         #   Merge statistics
│   └── Loughran-McDonald_MasterDictionary_1993-2024.csv  # L-M dictionary
├── jobs/                             # SLURM job scripts for Sherlock
├── logs/                             # SLURM log output
├── archive/                          # Superseded S&P-500-only scripts
├── .env                              # API credentials (not in git)
└── README.md
```

## Combined Dataset — `scraped_posts_combined/`

The final merged dataset from all scraping batches.

### posts_combined.csv (2.8 GB)

Each row is one post × one company board membership. Directors serving on multiple boards have posts duplicated per board.

| Column | Type | Description |
|--------|------|-------------|
| **Metadata (from input CSV)** | | |
| `company_name` | str | Company name (e.g. "APPLE INC") |
| `person_name` | str | Director/executive name |
| `position` | str | Title (e.g. "CEO, President & Director") |
| `source` | str | "director", "executive", or "blockholder" |
| `gvkey` | float | WRDS company identifier |
| `ticker` | str | Stock ticker |
| `execid` | float | ExecuComp person ID (executives only; null for directors) |
| `person_name_clean` | str | Cleaned name (credentials removed) |
| `company_name_clean` | str | Cleaned company name (Inc/Corp removed) |
| **Post data (from Apify)** | | |
| `profile_url` | str | LinkedIn profile URL |
| `post_text` | str | Full post text (null for pure reshares) |
| `post_url` | str | Permalink to the post |
| `post_type` | str | "regular", "repost", or "quote" |
| `post_date` | str | Datetime string "YYYY-MM-DD HH:MM:SS" |
| `post_timestamp` | float | Unix timestamp in milliseconds |
| `author_name` | str | Author name from LinkedIn |
| `author_headline` | str | Author's LinkedIn headline |
| **Engagement metrics** | | |
| `reactions_total` | float | Total reactions (sum of all types) |
| `likes` | float | Like count |
| `comments` | float | Comment count |
| `reposts` | float | Repost count |
| `celebrates` | float | Celebrate reaction count |
| `supports` | float | Support reaction count |
| `loves` | float | Love reaction count |
| `insights` | float | Insightful reaction count |
| `funnys` | float | Funny reaction count |
| **Media & articles** | | |
| `media_type` | str | "image", "video", or null |
| `article_url` | str | Shared article URL (null if none) |
| `article_title` | str | Shared article title |
| **Reshared content** | | |
| `reshared_text` | str | Original post text (for quote/repost types) |
| `reshared_url` | str | Original post URL |
| `reshared_author` | str | Original post author name |

### Key Statistics

| Metric | Value |
|--------|-------|
| Total rows | 2,583,711 |
| Unique posts | 2,194,784 |
| Unique profiles with posts | 26,367 |
| Profiles with zero posts | 16,161 |
| Total verified URLs | 42,527 |
| Date range | 2013 → 2026-03-07 |
| Multi-board duplication ratio | 1.18x |
| Mean posts per profile | 83.2 |
| Median posts per profile | 17 |
| Max posts per profile | 4,307 |
| Profiles with >100 posts | 5,201 |
| Profiles with >1000 posts | 124 |

### Post Type Distribution

| Type | Count | % | Description |
|------|-------|---|-------------|
| regular | ~1.4M | 54.5% | Original posts |
| repost | ~630K | 24.5% | Shared without commentary (post_text is null) |
| quote | ~540K | 20.9% | Shared with commentary |

### Posts by Year

| Year | Unique Posts |
|------|-------------|
| 2013 | 2,057 |
| 2014 | 10,753 |
| 2015 | 15,850 |
| 2016 | 45,521 |
| 2017 | 111,811 |
| 2018 | 150,972 |
| 2019 | 169,746 |
| 2020 | 211,447 |
| 2021 | 212,541 |
| 2022 | 228,078 |
| 2023 | 284,421 |
| 2024 | 316,275 |
| 2025 | 365,846 |
| 2026 | 69,465 |

### Known Data Quality Issues

1. **~280 empty rows** — Apify placeholder rows with all-null fields. Drop with `df = df[df['profile_url'].notna()]`.
2. **11.9% null post_text** — Pure reshares/reposts where the director shared content without adding their own text. The reshared content is in `reshared_text`.
3. **2.1% null ticker** — Companies without a ticker in the ExecuComp database.
4. **CSV parsing** — The 2.8GB CSV may trigger buffer overflow on pandas' default C parser due to embedded newlines in post text. Use: `pd.read_csv(path, engine="c", lineterminator="\n", on_bad_lines="skip")`.
5. **16,161 no-posts profiles** — Mix of genuinely inactive LinkedIn accounts and a small number from failed Apify batches (~500-1000 profiles from ~10 failed batches).

## Data Sources

### 1. Directors — WRDS ExecuComp `directorcomp`

Board directors of companies covered by ExecuComp (roughly S&P 1500 constituents).

| Metric | Value |
|--------|-------|
| Records | 272,646 |
| Unique directors | 31,530 |
| Unique companies | 2,784 |
| Year range | 2010–2025 |

### 2. Executives — WRDS ExecuComp `anncomp`

Top-5 compensated executives per firm.

| Metric | Value |
|--------|-------|
| Records | 167,071 |
| Unique executives | 32,961 |
| Unique companies | 2,796 |
| Year range | 2010–2025 |

### 3. Blockholders — Schwartz-Ziv/Volkova SEC Dataset

Individual investors holding ≥5% of a company's shares.

| Metric | Value |
|--------|-------|
| Records | 37,762 |
| Unique individuals | 14,149 |
| Year range | 2010–2023 |

### 4. Combined → URL Discovery → Post Scraping

| Stage | Records |
|-------|---------|
| Combined person-company pairs | 96,968 |
| Google search queries | 96,971 |
| LinkedIn URLs found | ~80,000 |
| Verified URLs (name matched) | 42,527 |
| Profiles with posts | 26,367 |
| Total posts scraped | 2,194,784 |

## Post Scraping — `scrape_posts.py`

Generic LinkedIn post scraper using Apify's `apimaestro/linkedin-batch-profile-posts-scraper` actor.

### Key Features

- **Automatic pagination**: Uses `total_posts` parameter (max 10,000) to get full post history
- **JSONL append-only checkpoints**: Results written to disk incrementally — no RAM accumulation. RAM usage bounded to one batch (~100 profiles) at a time.
- **Resume after interruption**: Checkpoint preserves progress on abort (spending limit, SLURM timeout, OOM). Resume with `--resume`.
- **1000-post cap detection**: Pauses scraping if a profile returns exactly 1000 posts without any profile exceeding 1000 (indicates pagination failure)
- **Fail-fast**: Aborts after 3 consecutive Apify failures
- **Metadata carry-through**: Joins company/person metadata to each post via LinkedIn URL

### Usage

```bash
python3 src/data_collection/scrape_posts.py --input urls.csv --stats       # Preview
python3 src/data_collection/scrape_posts.py --input urls.csv --prototype 5  # Test
python3 src/data_collection/scrape_posts.py --input urls.csv --run --yes    # Full run
python3 src/data_collection/scrape_posts.py --input urls.csv --resume       # Resume
python3 src/data_collection/scrape_posts.py --input urls.csv --slurm        # Generate SLURM script
```

### Architecture

```
Scraping:  scrape_posts.py → temp_results.jsonl (append-only, low RAM)
                            → .scrape_checkpoint.json (metadata only)
                            
Convert:   convert_temp_results.py → posts_*.csv + profiles_*.csv (high-memory SLURM job)

Merge:     merge_all_batches.py → posts_combined.csv (final dataset)
```

## Sentiment Analysis — Upcoming

### Loughran-McDonald Dictionary

The 2024 Master Dictionary (`Loughran-McDonald_MasterDictionary_1993-2024.csv`) is the primary sentiment method. File location: `data/Loughran-McDonald_MasterDictionary_1993-2024.csv`.

Existing pilot scripts (S&P 500 only):
- `analysisAI_LM.py` — AI post sentiment analysis
- `covid_sentiment_LM.py` — COVID baseline comparison
- `lm_dictionary_loader.py` — Dictionary loading helper

### Pilot Results (S&P 500 subset)

| Metric | AI Posts | COVID Posts |
|--------|----------|-------------|
| Mean net sentiment | +18.42 | +9.24 |
| % positive posts | 69.2% | 51.9% |

### Planned Approach

1. **AI keyword filtering** with word-boundary matching (`\b` regex) — avoids substring false positives
2. **L-M sentiment scoring** as primary method
3. **FinBERT** as robustness check
4. **Control groups** (Members of Congress, professional athletes) for platform-level positivity bias
5. **Aggregation**: post-level → director-level → company-level

## Running on Stanford Sherlock

### Setup

```bash
ssh sherlock
cd ~/ai-enthusiasm-research
module load python/3.12
source venv/bin/activate
```

### SLURM Partitions

| Partition | Use |
|-----------|-----|
| `nbloom` | Main compute (384GB RAM, 7-day limit) |
| `normal` | General (128GB RAM, 7-day limit) |
| `bigmem` | High-memory jobs (384GB+, 1-day limit) |

### Common Commands

```bash
squeue -u $USER                    # Job status
tail -30 "$(ls -t logs/*.log | head -1)"  # Latest log
scancel <JOB_ID>                   # Cancel job
```

## Configuration

### API Credentials (`.env`)

```bash
GOOGLE_API_KEY=your_api_key_here
GOOGLE_CSE_ID=your_search_engine_id_here
APIFY_API_TOKEN=your_apify_token_here
```

## Cost Summary

| Service | Cost | Status |
|---------|------|--------|
| Google Custom Search API | ~$485 | Complete |
| Apify LinkedIn scraper | ~$5,500 | Complete |
| WRDS | Free (Stanford) | Complete |
| Sherlock compute | Free (Stanford) | In use |
| **Total** | **~$6,000** | |

## Acknowledgments

- WRDS (Wharton Research Data Services) for corporate governance data
- Schwartz-Ziv and Volkova for the blockholder dataset
- Google Custom Search API for LinkedIn profile discovery
- Apify for LinkedIn post scraping infrastructure
- Stanford Research Computing (Sherlock cluster)
- Nick Bloom and John Van Reenen for project supervision and funding
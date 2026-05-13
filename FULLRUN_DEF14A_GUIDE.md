# Full DEF 14A Scrape & Parse — Overnight Run Guide

## TL;DR

To run the full DEF 14A extraction for all ~30k company-years:

```bash
# Step 1: Scrape SEC EDGAR for HTML (60 min, local machine)
python3 src/data_extraction/scrape_def14a.py --mode full --run --yes

# Step 2: Parse with Claude Haiku (5-10 hours, local machine, 10 parallel workers)
python3 src/data_extraction/parse_def14a_bios.py --mode full --workers 10

# Both should be safe to run offline (checkpointing + resumable)
```

Output: **~185k extracted director/officer primary employers** in `data/processed/def14a_extracted_bios.csv`

---

## What You Get

- `full_name`: each director/officer
- `primary_company`: their primary employer (not board company)
- `primary_role`: their job title
- `is_current`: true if current role, false if retired/past
- **Result:** primary-company anchor for LinkedIn re-search

---

## Runtime & Cost

| Phase | Time | Cost | Machine |
|-------|------|------|---------|
| **Scraping** | ~1 hour | $0 | Local (SEC doesn't charge) |
| **Parsing** | 5–10 hours | ~$1,388 | Local (Anthropic API) |
| **Total** | **~6–11 hours** | **~$1,388** | Can run overnight |

**Parsing breakdown:**
- 30,842 company-years
- ~185k bios estimated
- Haiku 4.5 @ 10 parallel workers
- With prompt caching, marginal cost per filing is ~$0.007

---

## Step-by-Step

### 1. Scrape SEC EDGAR (1 hour)

```bash
python3 src/data_extraction/scrape_def14a.py --mode full --run --yes
```

**What happens:**
- Downloads HTML from SEC EDGAR for all company-years in `directors_all.csv`
- Caches to `data/raw/def14a/{cik}_{year}.html`
- Rate-limited to 8 req/s (SEC fair-use)
- Checkpoints after each file; safe to kill & resume

**Output:** Manifest CSV with downloaded filing metadata

---

### 2. Parse with Haiku (5–10 hours)

```bash
python3 src/data_extraction/parse_def14a_bios.py --mode full --workers 10
```

**Flags:**
- `--workers 10` — run 10 parallel threads (default). Adjust if your connection is unstable (e.g., `--workers 5`)
- `--mode full` — process all filings in manifest
- `--dry-run` — preview text extraction without API calls (useful before committing budget)

**What happens:**
- Reads cached HTML files from step 1
- Narrows to director-bio sections (heuristic)
- Sends to Haiku 4.5 with structured output (Pydantic JSON schema)
- Appends results to `data/processed/def14a_extracted_bios.csv`
- Thread-safe: multiple workers write to same CSV without corruption

**Output:** `data/processed/def14a_extracted_bios.csv` with ~185k rows

**Resumable:** If connection drops, the script skips already-parsed company-years and resumes from the next one. No wasted API calls.

---

## Checkpointing & Resumption

### Scraping phase
```bash
# Kill at any time with Ctrl+C
# Resume (picks up where it left off):
python3 src/data_extraction/scrape_def14a.py --mode full --run --yes
```

### Parsing phase
```bash
# Kill at any time with Ctrl+C
# Resume (skip already-parsed, continue):
python3 src/data_extraction/parse_def14a_bios.py --mode full --workers 10
```

Both scripts use `(cik, year)` keys to detect completion. Killing mid-row is safe (no partial rows).

---

## Parallel Workers: Tuning

**Default: 10 workers.** Adjust for your connection stability:

| Workers | Parallel API calls | Best for |
|---------|-------------------|----------|
| 5 | ~5 concurrent | Unstable connection; slower but more reliable |
| 10 | ~10 concurrent | Stable connection; balanced speed/safety (default) |
| 20 | ~20 concurrent | Very stable connection; fastest but more resource-intense |

```bash
# Slow and steady (unstable WiFi)
python3 src/data_extraction/parse_def14a_bios.py --mode full --workers 5

# Fast (wired, stable)
python3 src/data_extraction/parse_def14a_bios.py --mode full --workers 20
```

---

## Cost Control

Before committing the full $1,388:

```bash
# Preview: parse just 10 filings to test
python3 src/data_extraction/parse_def14a_bios.py --mode full --limit 10 --workers 10

# Dry-run: extract text without API calls (free!)
python3 src/data_extraction/parse_def14a_bios.py --mode full --dry-run
```

---

## Expected Output

```
============================================================
DEF 14A Bio Parser (Haiku 4.5)
============================================================
  Mode:     full
  Manifest: data/raw/def14a/manifest.csv
  Output:   data/processed/def14a_extracted_bios.csv
  Filings:  30842 (downloaded/cached in manifest)
  To do:    30842 (after skipping already-parsed)
  Workers:  10

  [1/30842] AAPL 2025: 5 bios
  [2/30842] MSFT 2025: 8 bios
  [3/30842] GOOGL 2025: 9 bios
  ...
  [30842/30842] XYZ 2010: 6 bios

============================================================
Summary
============================================================
  Filings processed: 30842  (errors: 0)
  Output: data/processed/def14a_extracted_bios.csv
```

---

## Next: LinkedIn Re-Search

Once parsing is done, use the primary companies to re-search LinkedIn for ~15,692 unmatched directors:

```bash
# Build input CSV (primary_company or fallback to board_company)
python3 src/data_collection/build_def14a_serper_input_full.py

# Run Serper re-search on unmatched
python3 src/data_collection/find_urls_serper.py \
  --input data/processed/def14a_serper_input_unmatched.csv \
  --run --yes

# Cost: ~$31 for ~15,692 unmatched directors
```

---

## Troubleshooting

**Q: "ANTHROPIC_API_KEY not set"**
- Add to `.env` or: `export ANTHROPIC_API_KEY=sk-...`

**Q: "Manifest not found"**
- Run scraping step first: `python3 src/data_extraction/scrape_def14a.py --mode full --run --yes`

**Q: Connection drops during parsing**
- Just resume: `python3 src/data_extraction/parse_def14a_bios.py --mode full --workers 10`
- Script skips already-done company-years automatically

**Q: Running slow?**
- Check # of workers: `--workers 10` is default. Try `--workers 5` if unstable, `--workers 20` if stable.
- Monitor API latency: Haiku should respond in 1–2 sec per filing.

---

## Files

| File | Description |
|------|-------------|
| `src/data_extraction/scrape_def14a.py` | SEC EDGAR HTML scraper |
| `src/data_extraction/parse_def14a_bios.py` | Claude Haiku bio extractor (parallelized) |
| `data/raw/def14a/manifest.csv` | Downloaded filing metadata |
| `data/raw/def14a/{cik}_{year}.html` | Cached HTML files |
| `data/processed/def14a_extracted_bios.csv` | **Final output** |

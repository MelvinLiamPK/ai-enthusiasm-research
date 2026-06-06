# CODEBOOK — release `2026-06-05_def14a`

Base release: `2026-05-27`. This release **merges DEF 14A director data into the
panels on the posts we already have**. `current` is NOT flipped — owner reviews
the diffs, then flips. Only the columns/files that are NEW or CHANGED vs the base
release are documented here; everything else is carried forward unchanged (symlinks).

OUT OF SCOPE / deferred to Wave 4 (NOT in this release): 8-K election outcomes,
tenure-gating of firm-year sentiment, and the WRDS∪def14a tenure union. The
new-nominee/tenure table here is the **input** the Wave-4 union will consume.

---

## NEW FILE — `def14a_director_status.csv`  (replaces base symlink; adds `age`)
One row per (person × proxy-filing-year); 96,955 rows. All original columns from
`def14a_director_status_20260528.csv` PLUS:

| column | meaning |
|---|---|
| `age` | Director age, backfilled from the stored filing (no re-scrape). NaN if not recoverable. **95.5% coverage** of `is_director` rows (88,501 / 92,632). |
| `age_source` | How `age` was obtained: `table` (HTML `Name\|Age\|Director Since` column, 59,668), `llm` (Claude Haiku 4.5 read the filing window for rows the deterministic pass missed, 15,491), `regex` (parsed from `def14a_bio_text` prose, 12,916), `table(disagree_regex)` (table value kept but disagrees >1yr with the prose value — 426, ~0.5%). May carry a `\|qc_year_delta_off` suffix when the same person's age across filings did not track the calendar-year gap (kept, not dropped). |

**Age method:** two passes. (1) Deterministic HTML table-parse ∪ bio regex →
78.8% at $0. (2) Targeted LLM pass (`backfill_age_llm.py`, Haiku 4.5) on the
residual 3,486 filings → +15,312 ages, lifting coverage to **95.5%** for **$48.44**
(0 errors). Validated: deterministic methods agree 98.3% where both fire; the LLM
ages are 96.3% exact / 99.3% within ±1yr on the year-delta consistency check —
indistinguishable in quality from the deterministic ages. The residual ~4.5% omit
age from the filing entirely (special-meeting/merger proxies, stub rosters) or are
non-director NEO rows. Age distribution: mean 62.6, range 25–99.

## NEW FILE — `def14a_new_nominee_tenure.csv`  (Wave-4 union input)
One row per (person × firm); 24,410 rows. The clean per-(profile_url, ticker)
tenure + new-nominee deliverable.

| column | meaning |
|---|---|
| `profile_url` | LinkedIn URL via the name→URL bridge (clean form `linkedin.com/in/...`); blank if the (name, ticker) never bridged. 17,453 rows populated. |
| `full_name` | modal proxy spelling of the director's name |
| `ticker` | filing-company ticker |
| `director_since` | bio-stated start year (NaN if never stated) |
| `tenure_start` | `director_since` if present, else first proxy year the person appears |
| `tenure_end` | last proxy year the person appears for this firm |
| `right_censored` | True if `tenure_end` is the latest proxy we hold for the ticker (service likely continues) |
| `new_nominee` | True = genuine first-time nominee for THIS board (DEF14A classifier's `status==new_nominee`; cleared if `director_since` predates first proxy by >1yr, i.e. left-censored incumbent). **1,423 True.** Status is per-firm: a person can be `new_nominee` at one ticker and incumbent at another (361 such people). |
| `first_proxy_year`, `n_proxy_years` | first proxy-year appearance; count of distinct proxy years |

## NEW FILE — `def14a_board_composition.csv`  (keyed gvkey × year)
One row per (gvkey, year); 9,554 rows. Board snapshot from the proxy roster.
ticker→gvkey via `directors_all.csv` (0 tickers unmapped).

| column | meaning |
|---|---|
| `gvkey`, `year` | firm + proxy-filing year |
| `board_size` | # directors (incumbent + new_nominee + mid_year_appointee) |
| `n_incumbent`, `n_new_nominee`, `n_mid_year_appointee` | status counts |
| `share_new_nominee` | `n_new_nominee / board_size` |
| `mean_director_age` | mean over seats with a known age (see `age_coverage`) |
| `age_coverage` | fraction of the board's seats with a known age (mean ≈ 0.79) |

1,982 (gvkey, year) rows fall outside `firm_panel` (firm-years with no posts) — expected.

## NEW FILE — `def14a_corpus_intersection.csv`  (analyzable spine)
Tenure-panel rows restricted to corpus profiles (in `person_year`); 48,316 rows;
4,166 distinct profiles; 5,287 distinct (profile, ticker). Carries
`tenure_start`/`tenure_end`/`new_nominee` from the new-nominee table.

---

## CHANGED — `firm_panel_annual.dta`  (written in place; +7 columns)
Base firm panel LEFT-joined with `def14a_board_composition` on (gvkey, year).
7,572 of 19,858 firm-years gain board-composition columns (NaN elsewhere). New
columns: `board_size`, `n_incumbent`, `n_new_nominee`, `n_mid_year_appointee`,
`mean_director_age`, `share_new_nominee`, `age_coverage`. No existing column changed.

## CHANGED — `person_year.dta`  (written in place; +6 columns)
Joined on (clean profile_url, ticker). New columns:

| column | meaning |
|---|---|
| `def14a_director` | the (profile, firm) seat appears in the DEF14A tenure table (16,010 rows) |
| `def14a_new_nominee` | 1.0 / 0.0 / NaN — genuine first-time nominee for this seat (967 seat-years True) |
| `def14a_tenure_start`, `def14a_tenure_end` | service window for this (person, firm) |
| `pre_board_year` | True if this post-year is BEFORE the person's `tenure_start` at the firm — the pre-nomination-sentiment window (3,978 rows). This makes the instrument's pre-board sample directly queryable. |
| `is_established_elsewhere` | person is an incumbent director at a DIFFERENT firm (11,007 rows). A heterogeneity/robustness dimension, NOT a validity filter — pre-board exogeneity is firm-specific. |

## CHANGED — `person_lifetime.dta`  (written in place; +4 columns)
`def14a_director`, `def14a_ever_new_nominee` (317), `def14a_n_firms`,
`is_established_elsewhere`. Lifetime aggregation across firms (per profile_url).

---

### Notes / caveats
- Cross-firm person identity is **name-only** (no stable person-id) — status is
  kept per (person × firm); never collapsed across firms.
- `classifier_confidence` (carried from base) is uncalibrated LLM self-report.
- The new-nominee-with-posts sample is small/scrape-limited (a few hundred); N grows
  after the planned Apify expansion. `is_established_elsewhere` cases even have an
  observable pre-join posting history.
- Build script: `src/data_extraction/build_def14a_merge_release.py`.

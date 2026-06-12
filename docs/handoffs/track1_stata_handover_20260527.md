# Handover → Track 1 (Stata files for John)

**Date:** 2026-05-27 (revised late-day after discovering & fixing a major sentiment coverage gap)
**From:** Track 2 (DEF 14A continuation scrape, now complete + sentiment fix)
**To:** Track 1 (build the 4 `.dta` files per [post_tuesday_20260527.md](post_tuesday_20260527.md))

---

## ⚠️ REVISION LOG — read this if you started from an earlier version

This doc has been revised twice today. If your mental model came from a version earlier than this one, the following has changed:

### v3 (this version, ~16:50) — major sentiment architecture change

**What changed:** While building the combined sentiment file in v2, we discovered the existing Mar 30 sentiment file (`sentiment_all_posts_20260330_160559.csv`) covered only **52% of the corpus**, with directors 86% unscored and blockholders 92% unscored. Every prior regression and aggregation inherited this gap.

**Fix applied:**
1. Migrated sentiment storage from **row-level** (one row per post×person×company) to **post-level** (one row per unique post). Same post is no longer scored 1.3–3.3× because of board duplication.
2. Re-scored the ~1.11M unique unscored posts with L-M (4 min on CPU).
3. Re-aggregated company-year / company-quarterly with full coverage.
4. Re-ran all 4 regressions (Tobin's Q, ROA, sales growth, stock return). **N roughly doubled** across all of them.
5. Added explanatory comments to the FinBERT filter at [src/data_analysis/sentiment_finbert.py:417, 465](src/data_analysis/sentiment_finbert.py#L417) — it was not the root cause of the gap; just drops 2 fully-empty rows.

**Files that changed since v2:**

| If v2 told you to use this... | ...use this instead |
|---|---|
| `sentiment_all_posts_combined_20260527.csv` (1.5M rows, **deleted**) | `sentiment_posts_scored_unique_20260527.csv` (2.3M post-level) or `sentiment_all_posts_full_coverage_20260527.csv` (2.9M expanded) |
| (re-aggregate company-year yourself) | Already done: `company_sentiment_annual_20260527_164007.csv` (54K rows, 88% larger than the May 2 version) |
| (re-run regressions yourself) | Already done: `outputs/sanity_checks/regression_*_20260527_16460*/` |

### v2 (~13:00) — initial post-Track-2 handoff

What v2 said: scrape complete, merged into corpus, sentiment scored for def14a slice and combined with Mar 30 file. **All true but incomplete** — didn't catch the 52% coverage gap.

### v1 (pre-Track-2) — original [post_tuesday_20260527.md](post_tuesday_20260527.md)

The original Tuesday handoff. The 4-file Stata target hasn't changed; only the inputs to build them from.

---

## What Track 2 delivered (relevant to your work)

The DEF 14A scrape (pilot + continuation, 1,022 unique URLs) is done and merged into the corpus. Additionally — and not originally scoped — a sentiment-coverage gap was discovered and fixed; see revision log above.

**Use these as your inputs, not the originals:**

| New file | Old file (do NOT use for John) | Rows |
|---|---|---|
| `outputs/sentiment_results/sentiment_posts_scored_unique_20260527.csv` | `sentiment_all_posts_20260330_160559.csv` | 2,297,293 |
| `outputs/sentiment_results/sentiment_all_posts_full_coverage_20260527.csv` | (intermediate; ditto above) | 2,922,365 |
| `outputs/sentiment_results/company_sentiment_annual_20260527_164007.csv` | `company_sentiment_annual_20260502_155052.csv` | 54,466 |
| `outputs/sentiment_results/company_sentiment_quarterly_20260527_164007.csv` | `company_sentiment_quarterly_20260427_160543.csv` | 156,517 |
| `data/processed/all_people_linkedin_urls/scraped_posts_combined/posts_combined_v2_20260527.csv` | `posts_combined.csv` | 2,922,365 |
| `data/processed/all_people_linkedin_urls/scraped_posts_combined/profiles_combined_v2_20260527.csv` | `profiles_combined.csv` | 27,134 |
| `data/processed/all_people_linkedin_urls/def14a_scrape_outcomes_20260527.csv` | (new) | 1,022 |

The original master files are **untouched** for rollback.

## Sentiment architecture changed today (read before building anything)

The prior sentiment file (`sentiment_all_posts_20260330_160559.csv`) covered only **52% of the corpus** — concentrated in executives. Directors (86% unscored) and blockholders (92% unscored) were almost entirely missing. Every prior regression and aggregation inherited this gap. See "Coverage gap" section below for the audit.

The fix migrates to **post-level scoring**:

- **`sentiment_posts_scored_unique_20260527.csv`** — one row per unique `(profile_url, post_url)`. 2.3M rows. Has LM scores for all; FinBERT scores for the 1.19M rows that were in the Mar 30 file.
- **`sentiment_all_posts_full_coverage_20260527.csv`** — the corpus left-joined with the above on `(profile_url, post_url)`. 2.92M rows, drop-in replacement for the old `sentiment_all_posts_*.csv` schema. **Use this for any analysis that wants expanded (post × person × company) rows.**
- **`company_sentiment_annual_20260527_164007.csv` / quarterly** — re-aggregated with full coverage (`min_posts=1`, matches prior convention).

LM coverage is now **100%** (2,922,363 of 2,922,365 rows; the 2 missing are all-NaN edge cases). FinBERT remains at 41% coverage (1.19M rows scored); a Sherlock GPU re-run is needed to close that gap (separate task).

## Coverage gap discovered (and fixed) — read this

While building today's combined sentiment file, we discovered the Mar 30 sentiment run had scored only **52% of the corpus**, almost entirely concentrated in executives:

| Source | In corpus | Mar 30 scored | % missing |
|---|---|---|---|
| executive | 1,038,272 | 1,002,657 | 3.4% |
| director\|executive | 162,109 | 151,029 | 6.8% |
| director | 1,203,143 | 167,840 | **86%** |
| blockholder | 356,813 | 28,474 | **92%** |
| def14a_serper (new today) | 162,026 | 162,026 | 0% ✓ |

Root cause was **not** the FinBERT `profile_url.notna()` filter (it only drops 2 rows in current corpus — that's documented now). The actual cause: the Mar 30 run was on a partial input — 11,415 of 26,511 profile_urls in the current corpus were never scored. Plus, the def14a merge created 235K new (post × person × company) rows that didn't exist when Mar 30 ran.

**The fix is in.** All prior aggregations and regressions were re-run on the new full-coverage data. Headline N changes:

| Outcome | Prior N (firm-years) | New N (firm-years) | Firms (prior → new) |
|---|---|---|---|
| Tobin's Q | 1,307 | **3,101** (2.4×) | 524 → 1,092 |
| ROA | 7,125 | **14,038** (2.0×) | 1,408 → 2,011 |
| Sales growth | 7,125 | **14,032** (2.0×) | 1,408 → 2,011 |
| Stock return | 6,117 | **12,257** (2.0×) | 1,355 → 1,969 |

Coefficient deltas for Tobin's Q (others didn't have headline regressors in the prior file so only N comparison is meaningful):

| Regressor | Layer | Prior β (p) | New β (p) |
|---|---|---|---|
| `ai_post_share` | Pooled OLS | +0.666 (0.035*) | +0.695 (0.003***) |
| `ai_post_share` | + Year FE | +0.977 (0.006***) | +0.954 (<0.001***) |
| `ai_mean_net_sentiment` | + Year FE | +0.0014 (0.418) | +0.0024 (0.012**) |
| All regressors | + Firm FE (saturated) | null | null |

The cross-sectional signal stays directionally consistent and becomes more precise. The firm-FE-saturated null is unchanged — within-firm time effects don't survive saturation either way.

**Re-run outputs:** `outputs/sanity_checks/regression_{tobins_q,roa,sales_growth,stock_return}_20260527_16460*/`

## What this changes for the 4 .dta files

Sentiment scoring + aggregation are **done with full coverage**. Your job is mostly the .dta conversion + person-level aggregation.

### 1. `company_year.dta` and `company_quarterly.dta` — trivial

Use the **new pre-aggregated** files directly:
- `outputs/sentiment_results/company_sentiment_annual_20260527_164007.csv` (54,466 rows)
- `outputs/sentiment_results/company_sentiment_quarterly_20260527_164007.csv` (156,517 rows)

Just `.to_stata()` these. No re-aggregation needed.

### 2. `person_year.dta` and `person_lifetime.dta` — recommend post-level approach

**Recommended:** aggregate from `sentiment_posts_scored_unique_20260527.csv` (2.3M unique posts, **no board duplication**). For person-level rollups, post-level is correct and clean — each post is counted exactly once per person.

```python
scored = pd.read_csv("outputs/sentiment_results/sentiment_posts_scored_unique_20260527.csv",
                     engine="c", lineterminator="\n", on_bad_lines="skip", low_memory=False)
scored["year"] = pd.to_datetime(scored["post_date"], errors="coerce").dt.year
person_year = (scored.groupby(["profile_url","year"])
                     .agg(n_posts=("post_url","count"),
                          n_ai=("is_ai_related","sum"),
                          mean_lm=("lm_net_sentiment","mean"),
                          ...)
                     .reset_index())
# Then join in person metadata (name, primary company, source flags) from profiles_combined_v2_20260527.csv
```

**Alternative (if you specifically need (person × company) rows for some reason):** use `sentiment_all_posts_full_coverage_20260527.csv` (2.9M expanded rows, 100% LM-scored), but you'll then need `drop_duplicates(['post_url','profile_url'])` before person aggregation anyway. The post-level file skips that bookkeeping.

## Grain & gotchas — read before building

The merged dataset has **(post × person × company)** rows. Same physical post can appear multiple times if the author sits on multiple boards.

**Counts on the merged file:**
- 2,922,365 rows
- 2,297,285 unique posts (`post_url`)
- 26,511 unique people (`profile_url`)
- 3,376 unique companies (`gvkey` + `ticker` fallback)
- Average post → 1.27 rows (driven up by def14a directors, who have 1.63x expansion)

**Before person-level aggregation:** `drop_duplicates(['post_url', 'profile_url'])` — otherwise multi-board people get their posts double-counted.

**Before company-level aggregation:** `drop_duplicates(['post_url', 'profile_url', 'gvkey'])` — name-variant duplication in `all_linkedin_urls_v2.csv` can otherwise inflate counts (e.g., "Teri List" / "TERI LIST" / "Teri List-Stoll" all appear as separate rows for the same person on Danaher).

## def14a-specific NaN patterns

For rows where `source = 'def14a_serper'`:
- `gvkey`, `execid`, `position`, `person_name_clean`, `company_name_clean` → **all NaN**
- `ticker` and `company_name` → populated (from v2's `board_company`)

**Implication for `company_year.dta`:** if you key on `gvkey`, all def14a directors silently drop out of company aggregations. Either build a `ticker → gvkey` lookup first (Compustat join), or use `COALESCE(gvkey, ticker)` as the company key with a flag column distinguishing them.

## Tagging — "indicate that these are def14a added"

No new column was added. The discriminator is the existing `source` column: `source = 'def14a_serper'` isolates the def14a rows. Existing master rows use `director` / `executive` / `blockholder` / `director|executive`. Add this as a derived `cohort` column in the Stata files if John wants a cleaner one-shot filter:
```python
df['cohort'] = np.where(df['source'] == 'def14a_serper',
                        'def14a_continuation', 'initial_universe')
```

## Outcomes file for cross-referencing

`def14a_scrape_outcomes_20260527.csv` has 1,022 rows, one per def14a URL, with columns:
- `linkedin_url, in_pilot, in_continuation, scraped_in_pilot, scraped_in_continuation, has_posts_in_final_corpus, post_count`

Useful for the README "what to look at first" section — John can see per-URL which were scraped, which returned nothing, and which were in pilot vs continuation.

**Yield (final): 767/1,022 = 75.0%** for the def14a cohort.

## Known methodological issue (flag for John, not your problem to fix)

Current AI keyword filter (`src/data_analysis/sentiment_analysis_full.py:202`, `:320`) scans **`post_text` only, not `reshared_text`**. ~11.9% of posts are pure reshares with null `post_text` — these are entirely invisible to the AI filter. If person-year / person-lifetime aggregations show suspiciously low AI shares for prolific re-sharers, that's why. Decision pending on whether to broaden to `post_text | reshared_text`.

## Files NOT to touch

- `data/processed/all_people_linkedin_urls/scraped_posts_combined/posts_combined.csv` — original master, kept for rollback
- `data/processed/all_people_linkedin_urls/scraped_posts_combined/profiles_combined.csv` — same
- `outputs/sentiment_results/company_sentiment_annual_20260502_155052.csv` and `..._20260427_160543.csv` — old company-level aggregates from the 52% coverage era. Kept for comparison; don't use as input.
- `outputs/sentiment_results/sanity_checks/regression_*_20260503_*/` — old regression outputs on partial data. Superseded by `..._20260527_16460*/`.

## Files that no longer exist (referenced in earlier handoff versions)

- ~~`sentiment_all_posts_combined_20260527.csv`~~ — the 1.5M-row file v2 mentioned. **Deleted.** Replaced by the post-level scored file + the full-coverage expanded file.
- ~~`sentiment_all_posts_20260330_160559.csv`~~ — the canonical Mar 30 file. Kept on disk for reference but its 52% coverage makes it stale — don't use as a primary input.

## Open questions still pending from Tuesday

From the original handoff (`post_tuesday_20260527.md`):
1. AI keyword vocabulary — affects which sentiment file feeds person-year aggregations
2. Min-posts threshold for person-year — recommendation was keep all, add `is_active_poster` flag
3. No-post-year interpretation — recommendation was emit rows only when N ≥ 1
4. Continuation scope was resolved as `strong_match` (958) — Track 2 completed on this scope

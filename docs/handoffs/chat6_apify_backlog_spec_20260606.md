# Chat 6 spec — Coverage ledger + Apify backlog scrape (Wave 2 long-pole)

**Date:** 2026-06-06 · **From:** planning hub · **To:** execution chat (Chat 6)
**Read first (in order):** [`docs/PIPELINE_STATE.md`](../PIPELINE_STATE.md) ← the corrected
state, **read it before assuming anything needs doing**; `CLAUDE.md`;
`data/canonical/README.md` (release mechanism).

---

## 0. The reframing (why this spec is not what the master doc says)

The master sequencing describes Chat 6 as "Serper → crosscheck → Apify for ~5,647 new
def14a directors." **That is wrong.** Per `PIPELINE_STATE.md`:

- **URL discovery (Serper) and Revelio crosscheck are already DONE** for everyone — WRDS
  via the original run, def14a via the May-16 primary-anchor rescrape. Every discovered
  WRDS URL (58,956/58,956) is already in `revelio_validation_summary_v2.csv`.
- The genuinely-new def14a directors are essentially scraped already (`def14a_only`
  strong-match backlog = 73).
- What remains is a **post-scrape backlog**: of **25,221 distinct strong-match URLs**
  (`strong_match_either==True`), **15,670 have posts, 9,551 do not.** But how many of those
  9,551 were *ever submitted to Apify* is **not determinable from local files** — the only
  local submission list (`score70plus`, 3,269 URLs) is one early batch; the full batch2/
  batch3 inputs live on Sherlock. **True backlog is bounded [~hundreds, 9,551].**

So Chat 6 is **Apify-only**, and it must **resolve coverage before spending**. It also
builds the artifact whose absence has caused repeated confusion: a person-level coverage
ledger.

**Goal:** (0) build the coverage ledger + resolve the true backlog; (1) scrape the
never-submitted strong matches; (2) LM-re-score → new canonical release.

---

## 1. Environment — runs on SHERLOCK

```bash
ssh sherlock && cd ~/ai-enthusiasm-research
module load python/3.12 && source venv/bin/activate     # module BEFORE venv (see CLAUDE.md)
export HF_HOME=$SCRATCH/huggingface_cache
```
Sherlock is required because **Task 0 needs the original Apify submission logs** (batch2/
batch3 input lists under `/home/users/ml2068/.../scraped_posts_batch2|batch3/`), which are
**not on the local machine**. Large outputs → `$SCRATCH` (`outputs/` is symlinked there).

---

## 2. Task 0 — Build the person-level COVERAGE LEDGER (the gate; no spend before this)

**Why:** there is no single artifact stating, per person, what stages they cleared. Every
raw-file coverage check produces name/URL-normalization false gaps (see PIPELINE_STATE).
Build it once, with the project's own normalizer.

**Use `src/revelio/normalize_names.py`** for the person key and `norm(url)` (strip
scheme/`www.`/query/trailing-slash, lowercase) for URL joins. **Normalize both sides of
every join** — do not raw-string-match.

**Inputs (source-of-truth files only):**
| stage | file | flag it sets |
|---|---|---|
| person universe | `data/extracted/combined/all_people.csv` (WRDS) + `def14a_director_status_age_20260605.csv` (def14a) | identity, `source`, gvkey, ticker |
| Serper search | `all_linkedin_urls.csv` (WRDS) + `def14a_urls_for_revelio_validation.csv` (def14a, score≥60 subset) + the full def14a Serper output if a non-score-gated copy exists | `searched`, `search_status`, `url_found`, `linkedin_url` |
| Revelio crosscheck | `data/revelio/revelio_validation_summary_v2.csv` | `revelio_crosschecked`, `revelio_url_match`, `strong_match_either`, `strong_match_board` |
| Apify submission | batch2/batch3 input lists (Sherlock) + `outputs/apify_inputs/*score70plus*` + the def14a `apify_input.csv` files | `apify_submitted` |
| scrape result | `profiles_combined_v2_20260527.csv` (+ `posts_combined_v2`) | `has_posts`, `n_posts` |

**Output:** `data/processed/coverage_ledger_<ts>.csv` — one row per person (and a
person×url variant if a person has multiple URLs), columns:
`person_key, full_name, source, gvkey, ticker, searched, search_status, url_found,
linkedin_url_norm, revelio_crosschecked, revelio_url_match, strong_match_either,
apify_submitted, has_posts, n_posts`.

**Deliverable A — coverage certification** (answers the owner's standing question):
report, for WRDS and def14a separately: % searched, % with URL, % crosschecked,
% strong-match, % scraped. Residual "unsearched" rows must be inspected and labelled
**name-noise vs genuine** (the residual is expected to be ~all name-format mismatches).

**Deliverable B — the true backlog (3-way split of the 25,221 strong-match URLs):**
`has_posts` / `apify_submitted & ~has_posts` (returned empty/failed) / `~apify_submitted`
(**never submitted = the fundable backlog**). This replaces the bounded [hundreds, 9,551]
with a hard number.

---

## 3. Task 1 — Scrape the never-submitted strong matches (Apify)

- **Input:** ledger rows where `strong_match_either & ~has_posts & ~apify_submitted`
  → their `linkedin_url`. Strong-match-only (Meeting 11 discipline); no all-matches pool.
- **Reuse the proven harness:** `src/data_collection/scrape_posts.py` (or
  `scrape_def14a_pilot_and_continuation.py`), same pattern as the May continuation that
  scraped 767 profiles. Checkpoint (`.scrape_checkpoint.json` + JSONL append).
- **Cost:** report running spend; **no pilot, no hard cap** — owner tops up credits as it
  runs (decision 2026-06-02). At ~$0.72/profile the bill scales with the Task-0 number.
- **Task 1b (optional, cheap diagnostic — do BEFORE committing to anything large):**
  re-scrape a **~200-URL random sample of the `apify_submitted & ~has_posts` set** to
  measure recoverability (transient failures vs genuinely post-less). If recovery is high,
  widen scope to that set; if near-zero, drop it. This is the real lever on N and it's a
  ~$150 question — resolve it before any blanket re-scrape.

---

## 4. Task 2 — Re-score + new canonical release

- **LM-only re-score** (owner decision 2026-06-06; FinBERT NOT this round) of the new
  posts; merge into the corpus → rebuild `posts_scored_unique`, `company_sentiment_annual/
  quarterly`, and the firm panel aggregates.
- **Write a NEW dated canonical release** under `data/canonical/releases/<date>/` with a
  full `MANIFEST.json` (sources, row counts, code git SHA, build date) + `CODEBOOK.md`.
  **Do NOT flip `current`** — owner reviews and flips (canonical discipline).
- Keep **true missingness** (NaN where a firm-year posted but had no AI posts). Zero-
  imputation stays regression-time only (see PIPELINE_STATE imputation rule).

---

## 5. Decisions locked (do not relitigate)

1. **Apify-only.** Serper + Revelio already done; no new discovery/crosscheck for the
   strong-match set.
2. **Strong-match-only scrape** (no all-matches pool for the backlog).
3. **LM-only re-score** this round.
4. **No pilot / no hard cap**; checkpoint + cost report; owner tops up credits.
5. **New release, no auto-flip** of `current`.
6. **Normalize every join** (person key via `normalize_names.py`; URL via `norm`).

## 6. Out of scope (defer)

- FinBERT (next round). Serper top-up for non-strong / no-URL residual def14a directors
  (out of the strong-match sample). Tenure union + tenure-gating (Wave 4). Aliases 5b/5c
  (separate Wave-2 chat). The def14a→Revelio re-match with aliases (5c) may move some URLs
  False→True later — that's a future re-score, not this chat.

## 7. Deliverables back to planning

1. `coverage_ledger_<ts>.csv` + the coverage-certification report (Deliverable A).
2. The hard 3-way backlog number (Deliverable B) + Task-1b recoverability rate.
3. Apify cost actually spent + profiles/posts added.
4. The new canonical release path (un-flipped) for owner review.
5. A short handoff doc back here so PIPELINE_STATE can be updated (coverage now certified,
   backlog resolved, scrape done).

# PIPELINE_STATE — what is actually done (living state-of-the-world)

**Last verified:** 2026-06-05 (planning hub, from the real data files + Meeting Notes 10–11)
**Read this BEFORE assuming any pipeline stage needs doing.** Several chats have
re-derived state from scripts/outputs and got it wrong (including a "def14a was never
scraped" and a "~11.8k directors never searched" claim that the files disprove). This
doc is the corrected, file-backed picture. If you change state, update this doc.

---

## TL;DR

- **URL discovery (Serper/CSE): DONE for everyone** — all 96,968 WRDS people *and* the
  def14a directors (via the May-16 primary-anchor rescrape).
- **Revelio crosscheck: DONE for everyone** — two-tier validation in
  `revelio_validation_summary_v2.csv` (102,324 rows). 25,221 distinct strong-match URLs.
- **Post scraping: PARTIAL, exact backlog UNRESOLVED locally** — v2 corpus = 26,511
  profiles. Of 25,221 distinct strong-match URLs, **15,670 have posts; 9,551 do not.**
  But how many of those 9,551 were *ever submitted to Apify* (vs returned-empty vs
  never-sent) **cannot be determined from local files** — `no_posts_combined.csv` is a
  set-difference (verified − with-posts), not a scrape log, and the only local Apify
  input list (`score70plus`, 3,269 URLs) is one early batch, not the full submission
  history (batch2/batch3 inputs are on Sherlock). **True backlog is bounded
  [~hundreds, 9,551].** Resolving it = a scrape-coverage audit against the real Apify
  run logs — that is Chat 6's mandatory first task, before any spend.
- **Sentiment (LM): DONE** on the v2 corpus. FinBERT in progress.
- **The genuinely-new def14a directors are essentially scraped already** (73 strong-match
  URLs left). The backlog is mostly **executives + WRDS directors + blockholders.**

---

## SOURCE-OF-TRUTH FILE MAP (use these, not lookalikes)

| Need | Use THIS file | NOT this |
|---|---|---|
| URL discovery + Revelio match/strong-match, **all universes** | `data/revelio/revelio_validation_summary_v2.csv` (102,324 rows, two-tier) | `all_linkedin_urls.csv` is **WRDS-universe only** (Mar 28) — has none of the def14a primary-anchor URLs |
| Whose posts we have (scraped corpus) | `…/scraped_posts_combined/profiles_combined_v2_20260527.csv` (**26,511**) + `posts_combined_v2_20260527.csv` (3.39 GB) | `profiles_combined.csv` / `posts_combined.csv` are the **v1 (Mar, 26,366)** pre-def14a-scrape files |
| WRDS person universe | `data/extracted/combined/all_people.csv` (96,968) | — |
| def14a director classification | `data/processed/def14a_director_status_age_20260605.csv` (96,955 rows; +age) | `…_20260528.csv` is the **same director set**, just without age |
| Analysis inputs (regressions/summary) | `data/canonical/current/` symlink | any dated file under `outputs/` or `data/processed/` |

**Join gotcha:** LinkedIn URLs appear as both `linkedin.com/in/x` and
`https://www.linkedin.com/in/x/`. Normalize (strip scheme/`www.`/query/trailing-slash,
lowercase) before any URL join, or you will get false "0 overlap" results.

---

## STAGE STATUS (file-backed numbers)

### 1–3. Extraction / dedup / URL discovery — DONE
- WRDS universe: **96,968** people. `source` = role: director 41,591 · executive 32,260
  · blockholder 19,289 · director|executive 3,827 · (+1 triple).
- WRDS URL search: **100% submitted.** `all_linkedin_urls.csv` `search_status`:
  found 66,834 · not_found 30,111 · max_retries 16 · quota_exceeded 10.
  (The ~30k "not found" were **searched and not matched**, not skipped.)
- **def14a primary-anchor rescrape (Meeting Notes 10, 2026-05-16):** directors don't
  list board seats on LinkedIn, so `{name} {board_company}` was a weak anchor. Re-searched
  using each director's **primary employer** from their proxy bio: 23,084 searches /
  20,030 unique names / **94.2% URL match**. Results merged into the validation summary
  (tagged `source=def14a_serper`, 5,353 rows; `def14a_only=True`, 2,233 rows for
  non-WRDS directors). **This is why the def14a directors were already searched.**

### 4. Revelio crosscheck — DONE (two-tier)
`revelio_validation_summary_v2.csv` (102,324 rows; Redivis table `0wzc`):
- `revelio_url_match` True: 68,600 · `strong_match_board` (Tier 1) True: 29,780 ·
  **`strong_match_either` (Tier 2, headline) True: 30,832 rows → 25,221 distinct URLs.**
- Director-seat headline (Meeting 10): **8,809 → 13,401 strong-matched seats (+52%)**;
  ~84% of the lift was better methodology on existing WRDS directors, ~16% net-new
  def14a directors. Recency-conditioned: 45.2% (LinkedIn-era), 50.3% (currently active).
- **Caveat:** Revelio Workforce v6.0 is a frozen snapshot — URLs first-indexed after it
  read as unmatched even when correct. Structural floor, not a bug.

### 5. Post scraping — **PARTIAL (this is the open work)**
- Scraped corpus (v2, 2026-05-27): **26,511 profiles**, ~2.9M posts, 100% LM-scored.
- def14a-targeted Apify run (pilot 2026-05-24 + continuation 2026-05-27): **1,022 URLs →
  767 profiles with posts** (102,502 posts), merged into v2. Proves the
  Serper→crosscheck→Apify chain end-to-end. Ledger:
  `…/def14a_scrape_outcomes_20260527.csv`.
- **SCRAPE BACKLOG (the real expansion) — size UNRESOLVED:** of 25,221 strong-match URLs,
  15,670 have posts; **9,551 do not** (by source: executive 4,652 · director 2,713 ·
  blockholder 1,432 · director|executive 530 · def14a_serper 224; `def14a_only` only 73).
  The 9,551 is the UPPER bound — it is NOT known how many were ever submitted to Apify
  (see TL;DR caveat). Meeting 11 approved scraping ~9,423 strong matches (~$6,800) but only
  a 767-profile def14a pilot/continuation ran; whether the rest are unscraped-because-
  never-sent or unscraped-because-empty is the open question. **Audit submission logs first.**

### 6. Sentiment — LM DONE, FinBERT in progress
- Loughran-McDonald: 100% coverage on the v2 corpus.
- Canonical sentiment files (`company_sentiment_annual/quarterly`, `posts_scored_unique`)
  built on v2, 2026-05-27.

---

## WAVE-1 DELIVERABLES (2026-06-02 → 06-05) — built, `current` NOT flipped

| Chat | Status | Output |
|---|---|---|
| 1 · def14a merge | ✅ built | release `data/canonical/releases/2026-06-05_def14a/` — age 95.5% (no re-scrape; $48 Haiku top-up), board-composition (9,554 firm-yrs), new-nominee/tenure table (24,410; 1,423 new-nominees), person enrichment |
| 2 · Python parity (§3) | ✅ verified | `sentiment_q_regression.py` reproduces John's `jvrmel_06` headline (3/4 exact; 1 benign SE diff). Imputation rule locked (see below). |
| 4 · RA dataset release | ✅ packaged | `outputs/stata_handoff_20260527_release_20260530.zip` — same imputation rule as Chat 2 |
| 5a · data aliases | ✅ built | `data/revelio/company_aliases.csv` (18,130 rows; 2,052 firms w/ name change; Alphabet←Google, Meta←Facebook rescued) |
| Monthly-CRSP pull | ✅ **done + committed** (`c4345bb`; release `2026-06-07_crsp_monthly`) | monthly returns + market-equity from `crsp.msf` (387,652 rows, 2009-01→**2024-12**, no 2025 yet); fixed the `permno="ret"` bug (annual re-emit: returns byte-identical, only permno corrected) |

**Canonical `current` still points at `releases/2026-05-27`.** Owner flips after review.
**Flip target = `releases/2026-06-07_crsp_monthly`** — a complete, flippable 19-file snapshot
that symlink-inherits `2026-06-05_def14a` (which inherits `2026-05-27`) and adds
`crsp_monthly_returns.csv` + the fixed-permno `crsp_annual_returns.csv`. Flipping to it
activates def14a enrichment **and** the CRSP monthly panel in one move, so it **subsumes**
the earlier standalone "flip to def14a?" decision. Reproducible (MANIFEST `git_sha=c4345bb`).

**Locked imputation rule (identical in Chat 2 code + Chat 4 Stata/R, regression-time only,
data keeps NaN):** `ai_sent_new = 0 where (n_posts_strong≥1 & n_ai_posts_strong==0)`;
`= NaN where n_posts_strong==0`; else `= ai_mom_net_sentiment_strong`. ≡ John's
`replace ai_sent_new = 0 if ai_post_share_strong == 0`. 12,532 firm-years imputed.

---

## NOT DONE / OPEN

- **⚠️ No person-level COVERAGE LEDGER exists.** Every stage lives in its own file with its
  own keys and name/URL formatting (`all_people`, `all_linkedin_urls`, `def14a_urls_for_revelio_validation`,
  `revelio_validation_summary_v2`, `profiles_combined_v2`, `def14a_director_status`). There
  is **no single artifact** that says, per person: `searched / url_found / revelio_crosschecked
  / strong_match / has_posts`. Every coverage check run on the raw files produces an apparent
  gap that turns out to be **name/URL normalization noise** (e.g. def14a "unsearched" residual
  is prominent directors like Darius Adamczyk who Meeting 10 records as searched-but-not-found).
  **Coverage is NOT certifiable per-person until this ledger is built** (with `src/revelio/normalize_names.py`).
  This is Chat 6 Task 0. Best current estimate: WRDS Serper 100% (by construction); def14a
  Serper ~95%+ (residual is name-noise); Revelio crosscheck = every discovered WRDS URL (58,956/58,956).
- **Post-scrape backlog — true size unknown** (bounded [~hundreds, 9,551] strong-match URLs);
  resolved by the Task-0 ledger + Apify submission-log audit. Apify-only; LM re-score → new
  canonical release. (Wave 2 "Chat 6", reframed.)
- **Aliases 5b/5c** — LLM brand layer + apply-to-recompute-`strong_match`. Not started.
- **Tenure union (WRDS ∪ def14a) + tenure-gating** — Wave 4, after the backlog scrape.
- **Monthly CRSP pull** — status unconfirmed.
- **FinBERT sentiment** — planned.
- **Executive age** — not extracted (would need Execucomp `AGE`); def14a age is directors only.

---

## CORRECTIONS to the planning docs (read these against `master_sequencing_20260602.md`)

1. **Chat 6 is NOT "Serper → crosscheck → Apify for ~5,647 new def14a directors."**
   Search + crosscheck are already done for everyone. Chat 6 is **Apify-only**, and its
   FIRST task is a **scrape-coverage audit**: reconcile the 25,221 strong-match URLs
   against the real Apify submission logs (all batches, incl. Sherlock) to get the true
   split of have-posts / submitted-empty / never-submitted. Only the **never-submitted**
   get credits (plus optionally a sample of submitted-empty to test recoverability). The
   "9,551 backlog" is an upper bound, not a scrape target.
2. **"~5,647 new names needing the pipeline"** came from the tenure builder's name+ticker
   bridge and conflates "not in WRDS" with "not searched/scraped." The genuinely-new
   def14a directors with a strong match are basically scraped already (73 left).
3. **The two def14a classification files have no director delta** — `2026-06-05` only adds
   `age`. There is no "rescrape that added directors."

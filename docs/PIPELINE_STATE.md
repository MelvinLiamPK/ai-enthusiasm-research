# PIPELINE_STATE — what is actually done (living state-of-the-world)

**Last verified:** 2026-06-15 (planning hub — Chat 6 backlog resolved; Chat 3 v1 + age-join landed; D1–D5 locked)
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
- **Post scraping: BACKLOG RESOLVED (Chat 6, 2026-06-13→14).** Coverage ledger built;
  **never-submitted backlog = 0** — all 25,221 strong-match URLs were already submitted to
  Apify. The 9,551 "submitted-empty" were **~10.8% recoverable** transient failures from the
  original Feb–Mar scrape (not genuine empties) → recovered **+1,054 profiles / +136,959 posts**
  (~$750; Apify bills ~$0.005/post, so the old "$6,800" framing was ~140× high). New posts
  LM-scored, `company_sentiment` re-aggregated + cell-verified → un-flipped release
  `releases/2026-06-14_apify_backlog`. See the Chat 6 section below.
- **Sentiment (LM): DONE** on the v2 corpus + the +136,959 backlog posts. FinBERT = next round.
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

### 5. Post scraping — **BACKLOG RESOLVED (Chat 6, 2026-06-13→14)**
- Scraped corpus (v2, 2026-05-27): **26,511 profiles**, ~2.9M posts, 100% LM-scored.
- def14a-targeted Apify run (pilot 2026-05-24 + continuation 2026-05-27): **1,022 URLs →
  767 profiles with posts** (102,502 posts), merged into v2.
- **Coverage ledger built** (the long-missing artifact): `data/processed/coverage_ledger_20260613_*.csv`
  (102,324 rows, per-person `searched/url_found/crosschecked/strong_match/apify_submitted/has_posts`).
  **Deliverable A — coverage certified:** WRDS searched 100%; def14a 95.5% by name (residual 4.5%
  is name-noise, no genuinely-unsearched directors).
- **Deliverable B — backlog 3-way split of the 25,221 strong-match URLs:** has_posts 15,670 ·
  submitted-empty 9,551 · **never-submitted = 0.** The fundable backlog is **zero** — everything
  was already through Apify. The 9,551 "empties" proved **~10.8% recoverable** (transient API
  failures in the original scrape) → recovered **+1,054 profiles / +136,959 posts** (~$750;
  +6.7% profiles on the 15,670-profile strong-match corpus). Apify bills ~$0.005/post, so the
  Meeting-11 "$6,800" estimate was ~140× high.
- To grow N materially beyond this you must leave strong-match (out of scope this round).

### 6. Sentiment — LM DONE (incl. backlog expansion), FinBERT next round
- Loughran-McDonald: 100% coverage on the v2 corpus + the +136,959 backlog posts.
- Canonical sentiment files (`company_sentiment_annual/quarterly`, `posts_scored_unique`)
  built on v2 2026-05-27; **re-aggregated on the expanded corpus** in
  `releases/2026-06-14_apify_backlog` (cell-verified: every unchanged company-year identical).
  New posts carry `finbert_*`=NaN (LM-only this round; FinBERT next round should cover the whole
  expanded corpus).

---

## WAVE-1 DELIVERABLES (2026-06-02 → 06-05) — built; `current` flipped to `2026-06-07_crsp_monthly` (2026-06-12, `ec5d9f3`)

| Chat | Status | Output |
|---|---|---|
| 1 · def14a merge | ✅ built | release `data/canonical/releases/2026-06-05_def14a/` — age 95.5% (no re-scrape; $48 Haiku top-up), board-composition (9,554 firm-yrs), new-nominee/tenure table (24,410; 1,423 new-nominees), person enrichment |
| 2 · Python parity (§3) | ✅ verified | `sentiment_q_regression.py` reproduces John's `jvrmel_06` headline (3/4 exact; 1 benign SE diff). Imputation rule locked (see below). |
| 4 · RA dataset release | ✅ packaged | `outputs/stata_handoff_20260527_release_20260530.zip` — same imputation rule as Chat 2 |
| 5a · data aliases | ✅ built | `data/revelio/company_aliases.csv` (18,130 rows; 2,052 firms w/ name change; Alphabet←Google, Meta←Facebook rescued) |
| Monthly-CRSP pull | ✅ **done + committed** (`c4345bb`; release `2026-06-07_crsp_monthly`) | monthly returns + market-equity from `crsp.msf` (387,652 rows, 2009-01→**2024-12**, no 2025 yet); fixed the `permno="ret"` bug (annual re-emit: returns byte-identical, only permno corrected) |

**Canonical `current` → `releases/2026-06-07_crsp_monthly`** (flipped 2026-06-12, `ec5d9f3`).
def14a enrichment + CRSP monthly are **live** for all analysis. **Two un-flipped releases now sit
on top, each holding only PART of the new state** (both built on the crsp_monthly base, so neither
sees the other):
- `2026-06-12_age_attrs` — +director age (`def14a_birth_year`) + board-attribute block on the
  person panels.
- `2026-06-14_apify_backlog` — +re-scored `company_sentiment` + expanded posts (the +1,054-profile
  recovery), but inherits the OLD firm panel and has no age.

**Neither is the next flip target alone.** The clean target is the **UNIFIED RECONCILIATION
RELEASE** (spec [`handoffs/unified_reconciliation_release_spec_20260615.md`](handoffs/unified_reconciliation_release_spec_20260615.md);
**not yet built**): it folds age_attrs + apify_backlog + a re-scored firm/person panel into one
verified release via `build_stata_handoff` → `build_def14a_merge_release --skip-age` →
`augment_age_attributes` (reproduction-gate first). Owner flips that once built + verified;
`current` is NOT advanced past crsp_monthly until then.

**`releases/2026-06-12_age_attrs` (built 2026-06-13, join-only, no API/re-scrape):** puts
DEF 14A director **age** (as `def14a_birth_year`, time-invariant per `url_key` — derived
`round(median(year−age))`, carried exactly like `def14a_gender` so `age = year −
def14a_birth_year` is valid in every person-year incl. pre-/post-filing) and the
**board-attribute block** (`def14a_independent`, `def14a_n_committees`,
`def14a_committee_chair`, `def14a_audit_financial_expert`, `def14a_n_other_directorships`,
`def14a_board_leadership_role` — filing-year facts, joined year-specifically, NaN
elsewhere) onto `person_year.dta`/`person_lifetime.dta`. Reused the existing
name→URL bridge in `augment_age_attributes.py`. Verified: row counts unchanged
(person_year 143,850; person_lifetime 26,511); every base column byte-identical;
`def14a_birth_year` non-null on 24,176 person-years / 4,145 profiles; exec-also-directors
inherit age (2,747 `director|executive` rows vs 2,698 for gender — mirrors gender);
attributes non-null strictly in filing-years (7,487 rows); spot-check `year −
def14a_birth_year` reproduces disclosed proxy age (94.3% exact, 98.7% within ±1yr).

**Locked imputation rule (identical in Chat 2 code + Chat 4 Stata/R, regression-time only,
data keeps NaN):** `ai_sent_new = 0 where (n_posts_strong≥1 & n_ai_posts_strong==0)`;
`= NaN where n_posts_strong==0`; else `= ai_mom_net_sentiment_strong`. ≡ John's
`replace ai_sent_new = 0 if ai_post_share_strong == 0`. 12,532 firm-years imputed.

---

## CHAT 3 — FULL ANALYSIS v1 (2026-06-13) — DONE on the current corpus

Built on `releases/2026-06-07_crsp_monthly`; outputs in `outputs/chat3_v1/`; narrative in
[`handoffs/chat3_v1_done_20260613.md`](handoffs/chat3_v1_done_20260613.md). v1 build
artifacts (not a canonical release); re-runs in v2 after the backlog scrape (Wave 4).

- **§4 multi-frequency panel (UNGATED, true-missingness):** annual 31,807 / quarterly
  101,069 / monthly 248,157 firm-periods (2,731 firms). Strong-match metrics reproduce
  `firm_panel_annual.dta` **exactly** (max|Δ|=0); frequency-laddering verified (0 orphan
  cells). New scripts: `build_multifreq_panel.py`, `multifreq_regressions.py`,
  `multifreq_plots.py`, `descriptives_by_characteristics.py`.
- **§5:** sub-annual returns ~ general sentiment **null**; Δ5 long-difference AI-sentiment
  **0.00092\*\*\*** (> within-FE); size-weighting localizes the within-firm effect to
  smaller firms; AI-post share **×5** post-ChatGPT. **Length/intensity verdict: NOT
  incremental** after extensive margin + density (density stays the headline).
- **§6 descriptives:** AI-talk rises with size & (positive) R&D; SIC 73/35/36 top; younger
  directors more AI-forward (director-only ad-hoc age, 2020–2025); men modestly > women.
- Tenure-gating / WRDS∪def14a union stayed out of scope (Wave 4, by override). Age used the
  **ad-hoc** (name, ticker, year) join per override ③ — the formal `def14a_birth_year`
  panel column now exists in the unflipped `releases/2026-06-12_age_attrs`, so v2 can switch
  to it. One residual gap: Compustat `emp` is absent from the funda pull (size cut used total
  assets).

---

## CHAT 6 — BACKLOG RESOLVED + APIFY RE-SCORE (2026-06-13→14) — DONE

Narrative: [`handoffs/chat6_task0_coverage_report_20260613.md`](handoffs/chat6_task0_coverage_report_20260613.md);
decisions [`handoffs/planning_to_chat6_decisions_20260615.md`](handoffs/planning_to_chat6_decisions_20260615.md).

- **Coverage ledger built** (`data/processed/coverage_ledger_20260613_*.csv`) — coverage certified
  (Deliverable A); **never-submitted backlog = 0** (Deliverable B). The 9,551 "submitted-empty"
  were **~10.8% recoverable** transient failures → recovered **+1,054 profiles / +136,959 posts**
  (~$750). Strong-match posting corpus 15,670 → ~16,724.
- **Un-flipped release `releases/2026-06-14_apify_backlog`** (base `2026-06-07_crsp_monthly`):
  re-scored `company_sentiment_annual/quarterly` (cell-verified — every unchanged company-year
  identical) + expanded `posts_scored_unique`/`posts_full_coverage`. **Firm panel deliberately
  NOT re-scored** (left inherited → stale firm-level sentiment; that's why this isn't a flip
  target). v1 `strong_match` (strict) kept so unchanged companies stay byte-identical. LM-only.
- **D1–D5 locked** (planning): D1 don't flip this release; D2 firm-panel re-score → unified
  reconciliation release (dedicated chat); D3 keep v1 strict, Tier-2 panel-wide is a later release;
  D4 FinBERT next round; D5 committed on branch `chat6-apify-backlog-rescore` (`80e873e`, not
  pushed, `main` untouched at `bb60562`; scripts+docs only, GB data gitignored).
- **Builder note (D2 correction):** base firm/person panels are built by **`build_stata_handoff.py`**
  (`build_firm_panel`/`build_firm_quarterly` + person_year/lifetime from `company_sentiment` +
  `posts_scored_unique`); `build_def14a_merge_release.py` only merges board-composition on top. The
  firm-panel re-score must use the former.

---

## NOT DONE / OPEN

- **✅ COVERAGE LEDGER — DONE** (Chat 6). `data/processed/coverage_ledger_20260613_*.csv` is the
  per-person `searched/url_found/crosschecked/strong_match/apify_submitted/has_posts` artifact.
  Coverage certified; the historical "apparent gaps" confirmed as name/URL-normalization noise.
- **✅ Post-scrape backlog — RESOLVED** (Chat 6): never-submitted = 0; +1,054 profiles recovered.
- **◻ UNIFIED RECONCILIATION RELEASE — the next flip target, NOT yet built.** Folds
  `2026-06-12_age_attrs` + `2026-06-14_apify_backlog` + a re-scored firm/person panel into one
  verified release. Spec: `handoffs/unified_reconciliation_release_spec_20260615.md` (D2 follow-up).
- **◻ Tier-2 (`strong_match_either`) panel-wide migration** — a *separate, deliberate* methodology
  release AFTER the unified one (knowingly changes existing company-years; reconciles the
  `company_sentiment` v1-strict vs `firm_panel` definitions). Planning to spec after tracing usage. (D3)
- **◻ Aliases 5b/5c** — LLM brand layer + apply-to-recompute-`strong_match`. Not started.
- **◻ Tenure union (WRDS ∪ def14a) + tenure-gating** — Wave 4, after the backlog scrape.
- **✅ Monthly CRSP pull — DONE** (release `2026-06-07_crsp_monthly`, live in `current`).
- **◻ FinBERT sentiment** — next round; should cover the whole expanded corpus (incl. +136,959 new posts).
- **◻ Executive age** — not extracted (would need Execucomp `AGE`); def14a age is directors only.

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

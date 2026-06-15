# Chat 6 → planning hub — DECISIONS NEEDED (Apify backlog re-score)

**Date:** 2026-06-15 · **From:** Chat 6 (execution, Sherlock) · **To:** planning hub / owner
**Full record:** [`chat6_task0_coverage_report_20260613.md`](chat6_task0_coverage_report_20260613.md)
(the running blow-by-blow). This doc is just the **open decisions** + recommendations.

---

## What's done (one paragraph)

Task 0 ledger + Deliverables A/B **certified**: never-submitted backlog = **0**. The headline
correction — the 9,551 strong-match "submitted-empty" URLs are **~10.8% recoverable transient
failures**, not genuine empties; re-scraping all 9,169 recovered **1,054 profiles / 136,959
posts** (+6.7% profiles, ~$750 total; Apify bills **$0.005/post**, so the old "$6,800" framing
was ~140× high). New posts LM-scored, `company_sentiment` re-aggregated and **cell-level
verified** (every unchanged company-year identical), expanded `posts_*` corpus written →
new release **`data/canonical/releases/2026-06-14_apify_backlog/`** (built on
`2026-06-07_crsp_monthly`, **`current` NOT flipped**). One piece deliberately deferred: the
firm panel (see Decision 2).

---

## Decisions

### 1. Flip `current` to `2026-06-14_apify_backlog`? — *recommend: NOT yet*
The release has updated, verified `company_sentiment` + expanded corpus, but **inherits the OLD
firm panel** (Decision 2). Flipping now would give a panel where firm-level `firm_panel_annual.dta`
sentiment columns are stale relative to `company_sentiment` — internally inconsistent for
firm-level regressions. **Recommendation:** hold the flip until the firm panel is re-scored
(Decision 2), then flip both together. (If you only run company-level specs off
`company_sentiment_*`, flipping now is defensible — your call.)

### 2. Firm-panel re-score — who/when? — *recommend: schedule a short follow-up chat*
The firm panel re-score was **rejected by the verification gate** and left inherited-unchanged:
`build_multifreq_panel.py` is **not** the builder of the canonical `.dta` (it emits 32,049
unfiltered firm-years vs the `.dta`'s 19,858 filtered ones; 52 unchanged firm-years drifted).
The faithful path is **`build_def14a_merge_release.py`** (the blessed 2026-06-05 builder) pointed
at the new `company_sentiment`, then re-verify unchanged firm-years. This is well-scoped but
needs the builder's exact filter/merge logic. **Question:** assign this to a fresh chat (with the
new `company_sentiment` + the MANIFEST `firm_panel_note` as input)? It's the prerequisite for a
clean `current` flip.

### 3. Strong-match definition — keep v1 strict, or move panel-wide to v2 Tier-2? — *recommend: keep v1 for this release; Tier-2 as a separate methodology release*
For this release `company_sentiment` keeps the **existing v1 `strong_match`** (strict) so
unchanged companies stay byte-identical (that's what passes the gate). You'd asked to map
`strong_match_either` (Tier-2 headline). Applying Tier-2 panel-wide **changes existing companies'
`n_strong_match_posts`** → fails the unchanged-companies check, so it can't ride in a
"verified-superset" release. Concretely it matters for only **52 of 1,053** recovered profiles
(strong under Tier-2 but not v1 strict). **Question:** do you want a *separate* methodology-change
release that moves the whole `company_sentiment` panel to `strong_match_either` (knowingly
changing existing company-years)? If yes, it's a clean standalone job. (Note: the firm panel
already uses Tier-2, so the two artifacts differ today — worth reconciling in that same job.)

### 4. FinBERT on the new posts — *recommend: defer to the next FinBERT pass*
This round was **LM-only** (per the lock); the 136,959 new posts have `finbert_*` = NaN. When the
next FinBERT pass runs, it should cover the new posts (and ideally the whole corpus) so the
columns are populated consistently. **Question:** is FinBERT still "next round," and should it be
scoped to the expanded corpus?

### 5. Commit the new scripts/artifacts? — *recommend: yes, after you've eyeballed the release*
New untracked scripts (`extract_apify_submitted_sherlock.py`, `build_coverage_ledger.py`,
`build_backlog_corpus_additions.py`, `rescore_aggregate_backlog.py`, `verify_aggregation_repro.py`,
`rebuild_firm_panel_backlog.py`) + the release dir + handoffs are uncommitted. **Question:** want
Chat 6 to stage a commit (on a branch), or leave it to you?

---

## Pointers
- Release: `data/canonical/releases/2026-06-14_apify_backlog/` + its `MANIFEST.json` (documents
  every file's status, the strong_match rationale, and the firm_panel deferral).
- Ledger: `data/processed/coverage_ledger_20260613_*.csv`.
- Recovered+scored posts: `outputs/sentiment_results/backlog_20260613_183034/` and the expanded
  `sentiment_posts_scored_unique_20260614.csv` / `sentiment_all_posts_full_coverage_20260614.csv`.
- Verification scripts (re-runnable): `src/data_analysis/verify_aggregation_repro.py`,
  `rescore_aggregate_backlog.py`.

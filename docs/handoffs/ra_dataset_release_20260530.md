# Handoff → Build & ship the RA dataset release

**Date:** 2026-05-30
**From:** Melvin
**To:** New chat — finalize and package the dataset for other RAs
**One-liner:** This is **Chat A of [pre_meeting_plan_20260529.md](pre_meeting_plan_20260529.md)
(apply the ratified §3 defaults) + repackage and prep for upload.** Do not
open a separate chat for Chat A — this is it.

## Goal

Ship a clean, self-contained dataset snapshot to the other RAs that reflects
the changes validated this week. **Snapshot scope: posts corpus frozen
2026-05-27, pre-Apify-expansion** — a refresh is expected after the planned
LinkedIn expansion (see [../meeting_notes/20260530_decisions_and_next_scrape.md](../meeting_notes/20260530_decisions_and_next_scrape.md)).
Label the release as such so RAs know it's a snapshot.

## Starting point — most of this already exists

[`outputs/stata_handoff_20260527/`](../../outputs/stata_handoff_20260527/) is
already built by
[`src/data_analysis/build_stata_handoff.py`](../../src/data_analysis/build_stata_handoff.py)
from the **full-coverage** inputs (100%-LM-coverage sentiment, re-aggregated
`company_sentiment_annual_20260527_164007.csv`, mean-of-mean headline,
strong-match metrics, t and t+1 outcomes). It contains `firm_panel.dta`,
`firm_quarterly.dta`, `person_year.dta`, `person_lifetime.dta`, a README,
do-files, the `.Rmd`, outliers, and plots.

So this chat is mostly **applying the ratified defaults + documentation +
repackaging**, not rebuilding from scratch.

## Tasks

1. **§3.1 — drop default min-post filters; strong-match is the default.**
   In `outputs/stata_handoff_20260527/do/02_regressions.do` and
   `research_walkthrough.Rmd`, change the default sample to
   `keep if has_strong_match == 1` (drop `n_posts >= 10` and `n_ai_posts >= 3`).
   Keep the `meets_min_*` flag columns as optional toggles.

2. **§3.2 — regression-time zero-imputation (data stays honest).**
   In the do-file / `.Rmd`, build a derived regressor (`ai_sent_new`) set to 0
   where `n_posts >= 1 & n_ai_posts == 0` (mirror for `_strong`; leave NaN where
   `n_posts == 0`). **Do NOT bake this into `build_stata_handoff.py` or
   `firm_panel.dta` — the data files keep true missingness (NaN).** This mirrors
   John's `replace ai_sent_new = 0 if ai_post_share_strong == 0`. Document the
   rule in the README codebook and ship a short example snippet so RAs can
   reproduce it.

3. **Verify.** Headline regressions should reproduce John's `jvrmel_06` numbers:
   imputed AI sentiment on `ln_tobins_q` with year+firm FE ≈ β 0.0007, p 0.001,
   N ≈ 16,250; `ai_post_share_strong` year FE ≈ 1.0. Paste refreshed
   coefficients into the README and the do-file output block.

4. **README updates.** Add a "Decisions (2026-05-30)" section (regression-time
   imputation; strong-match default; min-post filters dropped) and a clear
   "Snapshot scope" banner (corpus frozen 2026-05-27, pre-Apify-expansion;
   refresh expected). Confirm the codebook matches the actual columns.

5. **Package & hand back.** Re-zip the folder (keep the `_20260527` data date in
   the folder name; you may add a release-date suffix on the zip) and return it
   to Melvin to upload for the RAs. Note in the README how to load the `.dta`
   files and apply the imputation snippet.

## Canonical inputs (already wired into `build_stata_handoff.py`)

- Sentiment annual / quarterly: `company_sentiment_{annual,quarterly}_20260527_164007.csv`
- Post-level scored: `sentiment_posts_scored_unique_20260527.csv`
- Person attrs / full-coverage: `sentiment_all_posts_full_coverage_20260527.csv`
- Funda: `data/extracted/compustat/funda_20260425_135322.csv`
- CRSP: `data/extracted/crsp/crsp_annual_returns_20260428_132604.csv`

## Do NOT

- Re-scrape or change the posts corpus (frozen snapshot).
- Bake imputation into any data file — regression-time only.
- Run the deferred analysis chats (internal Python parity, multi-frequency
  panel, new specs, descriptives, Revelio aliases). Those wait for the
  post-expansion re-sequencing.

## Verification

`02_regressions.do` runs end-to-end on the new defaults and reproduces John's
coefficients; the README "Decisions" + "Snapshot scope" sections match the code;
`firm_panel.dta` still shows NaN (not 0) for no-AI-post firm-years; a zip is
produced and ready to upload.

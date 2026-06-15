# Chat 6 — Task 0 report: Coverage ledger + true backlog (Deliverables A & B)

**Date:** 2026-06-13 · **From:** Chat 6 (execution, Sherlock) · **To:** planning hub
**Status:** Task 0 COMPLETE. **Hard gate reached — NO Apify spend yet, awaiting direction.**
**Headline:** the fundable **never-submitted backlog = 0.** Every one of the 25,221
strong-match URLs has already been submitted to Apify. The spec's Task 1 has **no targets**;
the only remaining lever is the Task-1b recoverability question on the 9,551 submitted-empty.

---

## How this was resolved (the Sherlock audit)

The gating unknown was `apify_submitted`, determinable only from the real Apify submission
logs on Sherlock. Reconstructed the authoritative submitted set from the run inputs:
- **batch1 input** = `batch_*_urls.csv` (98 chunks) → **58,956 distinct normalized URLs =
  exactly the full WRDS found-URL universe.** batch1 fed *every* found WRDS URL to Apify.
- **batch2/batch3** = `remaining_urls.csv` / `remaining_urls_final.csv` — re-try lists of the
  same URLs, not new ones.
- **Per-run outputs** `no_posts_profiles_*.csv` are genuine *submitted-and-empty* records;
  batch3 `temp_results.jsonl` (714k post rows) gives batch3 has-posts.
- **def14a runs** (local): 4 `apify_input.csv` (pilot+continuations) + score70plus early batch.

Join discipline: every URL join on `norm_url()` (strip scheme/www/query/fragment, /posts/→/in/,
trailing slash, lowercase), **both sides**; person key via `normalize_names.normalize_name`.

Artifacts:
- Ledger: `data/processed/coverage_ledger_20260613_*.csv` (102,324 rows = the unified
  pipeline ledger: WRDS 96,971 + def14a_serper 5,353, now with `apify_submitted`+`has_posts`).
- Sherlock extractor: `src/data_collection/extract_apify_submitted_sherlock.py`
- Local builder: `src/data_collection/build_coverage_ledger.py`
- Submitted/empty URL sets: `data/processed/coverage_audit_sherlock/`
- Task-1b list: `data/processed/task1b_submitted_empty_20260613_*.csv` (9,551 rows)
- Never-submitted target list: `…/scrape_target_never_submitted_*.csv` (**0 rows**)

---

## DELIVERABLE A — coverage certification

**WRDS universe (96,971 person-company rows):** searched **100%** · url_found 68.9% ·
crosschecked 68.9% (= every found URL) · strong_match 30.5% · **apify_submitted = url_found
(100% of found URLs)** · has_posts 32.6%.

**def14a director universe (25,460 distinct name|ticker seats):** matched a searched record
**95.5% by name** (93.7% by name+ticker). **Residual 1,148 (4.5%) is name-noise, not a gap** —
sample is nickname↔formal mismatches (Art/Arthur Levinson, Joe/Joseph Gebbia, Jeff/Jeffrey
Williams, Doug/Douglas Parker, Mike/Michael Embler, Kate/Katherine Adams), OCR/parse
artifacts ("Jim A lbaugh", "S. C. Scott III"), and named non-board executives the parser
captured (Kent Walker, Deirdre O'Brien). **No genuinely-unsearched board directors.**
Confirms PIPELINE_STATE's diagnosis exactly. **Coverage certified.**

---

## DELIVERABLE B — the true backlog (3-way split of 25,221 distinct strong-match URLs)

| bucket | count | share |
|---|---:|---:|
| (1) has_posts | **15,670** | 62.1% |
| (2) submitted & ~has_posts (returned empty) | **9,551** | 37.9% |
| (3) ~submitted & ~has_posts (**never submitted = fundable**) | **0** | 0.0% |

Reproduces PIPELINE_STATE's 15,670 / 9,551 exactly and **resolves the bounded
[hundreds, 9,551] to a hard 0** on the never-submitted bucket.

**Evidence quality on the 9,551 submitted-empty:**
- **9,369 have an explicit Apify `no_posts` output record** — Apify processed them and
  returned **zero posts** (strongest evidence of genuinely post-less profiles).
- **182 are input-list-only** (fed to Apify, no output record) — the only plausible
  transient-failure / re-scrape candidates.
- By source (reconciles with PIPELINE_STATE): executive 4,652 · director 2,713 ·
  blockholder 1,432 · director|executive 530 · def14a_serper 224 (def14a_only 69).

---

## What this means for the plan (decision needed)

- **Task 1 (scrape never-submitted strong matches): 0 targets. Nothing to fund.** The whole
  25,221 strong-match set is already through Apify.
- **Task 1b is now the *only* lever, and the finding makes its expected yield low:** 98%
  (9,369/9,551) of the empties have an explicit Apify "0 posts" result, i.e. the profile was
  scraped and the person simply doesn't post. Re-scraping those is unlikely to recover much.
  The 182 input-list-only URLs (~$130 at $0.72/profile) are the only cheap, plausibly-
  recoverable slice; a full 9,551 re-scrape (~$6,900) would mostly re-confirm empties.

**Cost correction:** the actual Apify cost in `scrape_posts.py` is **$5/1k profiles
(~$0.005/profile)**, not the spec's $0.72/profile (that was an average over post-heavy
profiles). Empty profiles cost ~nothing → a full 9,551 re-scrape ≈ **$48**, not $6,900.

---

## TASK 1b — recoverability probe (DONE, cost ~$1.91)

Re-scraped 382 strong-match empties on Sherlock (`task1b_probe_20260613_173140`):

| slice | recovered | rate | meaning |
|---|---:|---:|---|
| **input_only** (182; in batch input, no output record) | 59/182 | **32.4%** | genuine transient failures; median 13 posts |
| **explicit_empty** (200 of 9,369 w/ Apify "0 posts" record) | 3/200 | **1.5%** | genuinely post-less; 98.5% still empty |

**The 9,551 "submitted-empty" is two populations:** 182 transient-failures (~32%
recoverable — **probe already recovered all 59, 5,561 posts, in hand**) + 9,369 genuine
empties (~1.5% recoverable, ~140 profiles for ~$47). **Strong-match corpus (15,670) is at
its ceiling; max incremental ≈ +200 profiles (+1.3%).** To grow N materially you must leave
strong-match (out of scope this round). Artifacts: `data/processed/task1b_probe_*` +
`task1b_probe_results/`.

**Recommendation:** fold the 59 (already paid for) recoveries in regardless; the full
9,369 re-scrape is ~$47 for ~140 more — trivial cost but marginal yield, an owner call.
Task 2 (LM re-score → new release) fires once recoveries are folded in.

---

## FULL RE-SCRAPE — STARTED, then BLOCKED on Apify monthly limit (SLURM 29489552)

Submitted the 9,169 not-yet-rescraped explicit-empties. After batches 1–4 it hit
**"Apify error: Monthly usage hard limit exceeded"** and aborted (3 consecutive failures).
SLURM reports COMPLETED (the wrapper's `echo Done` masks the in-script abort), elapsed 7:38.

**Saved / in hand (checkpointed, `--resume`-able from profile 600):**
- Full-run partial: **40 profiles / 9,009 posts** (600 of 9,169 processed).
- Probe: **62 profiles / 5,714 posts**.
- **Combined: 102 distinct recovered profiles (0 overlap), ~14,723 posts.**
- Remaining **~8,569** explicit-empties untouched. (The run's `no_posts` CSV is a
  set-difference vs the *full* input, so it is NOT a reliable empty-record for the unprocessed.)

**BLOCKER → owner action:** top up Apify credits / wait for monthly reset, then `--resume`
job to finish the remaining ~8,569 (~$43, est. +~100–150 profiles at the random-probe 1.5%
rate). **Open decision:** finish-then-one-re-score vs. re-score now with the 102 vs. declare
the strong-match corpus done (102 ≈ +0.65% on 15,670 — marginal either way).

---

## UPDATE 2026-06-14 — resume run reveals recovery is FAR higher than the probe implied

Owner topped up to $499 headroom; re-scraped the 2 truncated profiles @10000 (3,043 posts)
then resumed. **The resume hit the $1,500 monthly cap AGAIN at 6,700/9,169 processed**, having
collected **95,791 posts → 726 recovered profiles = 10.8% recovery** (not 1.5%!).

**Key correction to Deliverable B:** the 9,551 "submitted-empty" are NOT mostly genuine
empties — **~10.8% are recoverable transient failures** from the original Feb–Mar scrape
(the original `no_posts` files conflated genuinely-post-less with API-failed-during-run). The
random 200-probe drew an anomalously low 1.5%; the 6,700-profile run is the reliable estimate.

**Recovered in hand (deduped across all runs): 788 distinct profiles, ~101,500 posts**
(726 full-run + 62 probe; 2 truncated re-scraped @10k). That's **~+5% on the 15,670-profile
corpus** — a real addition, not marginal.

**Cost correction (I under-estimated badly):** billing is $0.005/post; this session spent
**~$449** (resume 86,782 posts ≈ $434 + truncated 3,043 ≈ $15) vs my "$40–200" estimate.
Monthly usage now ~$1,450–1,500 (cap re-hit).

**Remaining: 2,469 profiles** (6,700→9,169), checkpointed, `--resume`-able. At 10.8% that's
~+267 profiles for ~$173 — needs another cap raise. **Open owner decision:** raise cap &
finish the last 2,469, or fold the 788 now → LM re-score → new release and finish later.

---

## UPDATE 2026-06-14 (later) — full re-scrape DONE + new posts LM-scored

Owner raised the cap; resume finished **all 9,169 explicit-empties**. Final tally:
- **991 profiles / 130,804 posts** from the full run (10.8% recovery), + probe 62 + 2 truncated.
- **Combined deduped: 1,054 distinct recovered profiles, 136,959 posts** = **+6.7% profiles /
  +4.7% posts** on the 15,670-profile / ~2.9M-post corpus. Assembled →
  `data/processed/backlog_newposts_<ts>.csv`.
- **LM-scored** (`sentiment_analysis_full.py`, 1.1 min): ~7,579 AI-related, 3,605 COVID →
  `outputs/sentiment_results/backlog_<ts>/sentiment_all_posts_*.csv` (lm_* + is_ai_related).
- **Total Apify spend this episode ≈ $0.005/post × ~150k billed posts ≈ ~$750** (incl. the
  failed-retry double-bills + the genuine-empty processing).

**Headline corrected finding:** the strong-match "submitted-empty" bucket is **~11%
recoverable** (transient failures in the original Feb–Mar scrape), not genuine empties — so
the corpus was under-counting ~1,000+ postable strong-match profiles.

**Remaining (Task 2 final step): fold into a NEW canonical release.** This requires
reproducing the hand-blessed 2026-05-27 aggregation (metadata multi-board join →
`posts_full_coverage`/`posts_scored_unique` append → `aggregate_sentiment.py` →
`company_sentiment_annual/quarterly` → firm panels). Known drift: `aggregate_sentiment.py`
expects a `strong_match` column but the v2 revelio file exposes `strong_match_either`
(headline tier) — adaptable, but the release must be consistency-verified (companies with no
new posts must aggregate identically) before it can be trusted. **Owner decision on depth.**

---

## TASK 2 COMPLETE 2026-06-14 — verified release `2026-06-14_apify_backlog` (NOT flipped)

Owner chose "full release, verified." Built with a **verification-gate-first** discipline:

- **Reproduction gate PASS:** `aggregate_sentiment.py` reproduces the canonical
  `company_sentiment_annual_20260527` byte-for-byte on all 54,466 company-years *before* any
  new data (`src/data_analysis/verify_aggregation_repro.py`). Memory-safe `usecols` read is
  faithful (pandas tokenizes the full row before applying usecols).
- **Strong-match decision:** company_sentiment keeps the existing panel's **v1 `strong_match`**
  (strict) so unchanged companies stay identical. All 1,053 recovered URLs are in v1 (979
  strong); only 52 are strong under v2 Tier-2 but not v1 — applying the Tier-2 lift panel-wide
  would change existing companies (fails the gate) → deferred. Documented in the MANIFEST.
- **company_sentiment re-aggregated + VERIFIED at cell level:** every (company, year/quarter)
  with zero new posts is identical (annual 50,687/50,687; quarterly 147,880/147,880; 0 drift).
  +2,051 annual company-years; 7,118 companies touched.
- **Expanded corpus:** `posts_scored_unique` (+136,959) and `posts_full_coverage` (+188,445
  per-board rows) written by header-stripped append.
- **Firm panel: STOPPED at the gate, INHERITED unchanged (deferred).** `build_multifreq_panel.py`
  is a separate unfiltered product (32,049 firm-years) and is NOT the canonical `.dta`'s builder
  (19,858 filtered firm-years); 52 unchanged firm-years drifted → rejected per stop-and-report.
  **FOLLOW-UP (planning 2026-06-15; off Chat 6 → dedicated unified-reconciliation chat):** the
  base firm/person panels are built by **`build_stata_handoff.py`** (`build_firm_panel`/
  `build_firm_quarterly` + person panels, from `company_sentiment` + `posts_scored_unique`) —
  re-run that on the new company_sentiment, then re-apply def14a board-comp via
  `build_def14a_merge_release.py` (merge-only) + the age join, with a reproduction gate.
- **Release:** `data/canonical/releases/2026-06-14_apify_backlog/` (base `2026-06-07_crsp_monthly`),
  MANIFEST documents every file's status; def14a/CRSP/funda/revelio + firm panels symlink-
  inherited; NaN missingness preserved (LM-only, FinBERT=NaN on new posts). **`current` NOT flipped.**

**Net:** company-level sentiment panel now reflects the +1,054-profile / +136,959-post
recovery and is owner-reviewable; firm panel re-score is the one documented follow-up.
Scripts: `verify_aggregation_repro.py`, `build_backlog_corpus_additions.py`,
`rescore_aggregate_backlog.py`, `rebuild_firm_panel_backlog.py` (gate-rejected), and the
scrape/coverage scripts under `src/data_collection` + `src/data_processing`.

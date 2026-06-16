# Spec — Unified reconciliation release (the clean flip target)

**Date:** 2026-06-15 · **From:** planning hub · **To:** execution chat (focused build)
**Type:** canonical-release build (new release, **no flip**). Local; no API, no re-scrape — all inputs on disk.

---

## 1. Why this exists
Two un-flipped releases each hold only *part* of the current state, built on the same base
(`2026-06-07_crsp_monthly`) so neither sees the other:
- **`2026-06-12_age_attrs`** — `person_year`/`person_lifetime` + age (`def14a_birth_year`) and the board-attribute block. (See [`age_attributes_panel_join_done_20260613.md`](age_attributes_panel_join_done_20260613.md).)
- **`2026-06-14_apify_backlog`** — re-scored `company_sentiment_annual/quarterly` + expanded `posts_scored_unique`/`posts_full_coverage` (the +1,054-profile / +136,959-post recovery), but it **inherits the OLD firm panel** (stale firm-level sentiment) and has **no age**.

Neither is flippable alone. **This release folds both + re-scores the firm and person panels on the expanded corpus, all verified, into one flip target.**

## 2. The builder chain (all confirmed — do not re-implement)
- **`src/data_analysis/build_stata_handoff.py`** — builds the **base** `firm_panel`/`firm_quarterly` **and** `person_year`/`person_lifetime` from `company_sentiment_*` + `sentiment_posts_scored_unique`. (Writes to its handoff dir; the release renames `firm_panel.dta`→`firm_panel_annual.dta`.) This is the sentiment→panel aggregator — **not** `build_def14a_merge_release.py`.
- **`src/data_extraction/build_def14a_merge_release.py`** — reads the base panels + def14a → adds board-composition to the firm panel and the def14a person fields (director/tenure/new-nominee/`pre_board_year`) to the person panels. **Run with `--skip-age`** to reuse the cached `def14a_ages_extracted.csv` (do not re-parse filings).
- **`src/data_extraction/augment_age_attributes.py`** — reads the def14a-enriched person panels → adds `def14a_birth_year` (mapped by `url_key`, person-level median) + the filing-year attribute block.

## 3. Build sequence

**Step A — Reproduction gate FIRST (faithfulness, on OLD data; same discipline Chat 6 used).**
Before touching new data, prove the chain reproduces the current canonical artifacts byte-/numeric-identical from the **OLD** `company_sentiment` (`releases/2026-05-27`'s sentiment + posts):
1. `build_stata_handoff` panel builders → reproduce the frozen `outputs/stata_handoff_20260527/data/` base panels (firm + person).
2. `build_def14a_merge_release.py --skip-age` → reproduce `releases/2026-06-05_def14a/{firm_panel_annual,person_year,person_lifetime}.dta`.
3. `augment_age_attributes.py` → reproduce `releases/2026-06-12_age_attrs/{person_year,person_lifetime}.dta`.
**If any stage does not reproduce, STOP and report** — do not proceed to new data. (This is what caught the `build_multifreq_panel` drift; expect to nail the exact `build_stata_handoff` join/filter that yields the canonical row counts, e.g. the firm panel's funda/CRSP-matched firm-years.)

**Step B0 — Re-aggregate sentiment with the alias-aware labels (NEW — folds in 5c).**
This release now incorporates **5c** ([`revelio_alias_recompute_spec_20260615.md`](revelio_alias_recompute_spec_20260615.md)): the strong-match sample changes (Pichai/Alphabet, Meta/Facebook, renamed firms enter). So `company_sentiment` is **NOT** carried from `apify_backlog` unchanged — it is **re-aggregated** with:
- **strong-match = alias-aware `strong_match_either`** from `revelio_validation_summary_v3` (Tier-2, the locked headline definition);
- the **full expanded corpus**: `apify_backlog` posts **+ the alias-mover scrape** (the small set of newly-strong URLs that were found-but-empty — most movers already have posts from the original broad scrape).
Re-run `aggregate_sentiment.py` (the blessed aggregator) on this label set + corpus → new `company_sentiment_annual/quarterly` + `posts_scored_unique`/`posts_full_coverage`.

**Step B — Rebuild base panels.**
Run the `build_stata_handoff` panel builders pointed at the **Step-B0** `company_sentiment` + `posts_scored_unique` + funda + CRSP (annual & monthly) → new base `firm_panel`, `firm_quarterly`, `person_year`, `person_lifetime`.

**Step C — Re-apply def14a enrichment.** `build_def14a_merge_release.py --skip-age` on the Step-B base panels → board-composition on the firm panel + def14a person fields.

**Step D — Re-apply age/attributes.** `augment_age_attributes.py` on the Step-C person panels → `def14a_birth_year` + attribute block (mirror the `url_key` person-level mapping from `age_attrs`).

**Step E — Verify (the gate; expected-change discipline now that the sample moved).**
- **Cells unaffected by BOTH backlog AND alias-movers stay byte-identical** to `current/` (firms with no new posts and no mover/label change). Cells that DID change must reconcile **1:1 against the explicit backlog + mover lists** — every changed company-year traces to an added post or a strong-match label flip; no unexplained drift. (This replaces the pure "unchanged cells identical" check, which only held before 5c moved the sample.)
- **Movers landed:** Pichai (`GOOGL`) and the other v3 movers are now in the strong-match sample; report the count of company-years gained.
- **Enrichment preserved:** def14a board-comp columns present; `def14a_birth_year` coverage ≈ `age_attrs` (≈24,176 person-years / 4,145 profiles; exec-also-directors ≈2,747 `director|executive` rows).
- **Discipline:** true NaN missingness preserved (no baked-in zero-imputation); LM-only (`finbert_*` = NaN on new posts).
- **Row counts sane:** person_year grows by recovered + mover rows; no base columns dropped.

**Step F — Package.** New dated release `data/canonical/releases/2026-06-1X_unified/` (base `2026-06-07_crsp_monthly`) carrying its **own** `firm_panel_annual.dta`, `firm_panel_quarterly.dta`, `person_year.dta`, `person_lifetime.dta`, `company_sentiment_*` + `posts_*` (from apify_backlog); **symlink-inherit** def14a side-files, CRSP, funda, revelio. Full `MANIFEST.json` (sources, row counts, git SHA, what-changed, the **alias-aware Tier-2 strong-match** note + the mover count) + `CODEBOOK.md`. **Do NOT flip `current`** — owner reviews and flips. This one move then activates def14a + CRSP monthly + age + the backlog re-score + the alias recovery together.

## 4. Decisions locked (do not relitigate)
1. **Strong-match = alias-aware `strong_match_either` (Tier-2)** from `revelio_validation_summary_v3`. This release folds in 5c, so it is a deliberate **methodology+data** release — NOT a byte-identical superset. (Supersedes the earlier "keep v1 strict" framing, which only made sense before 5c was in scope; this also resolves the D3 v1-vs-Tier-2 inconsistency in one move.)
2. **LM-only** (FinBERT next round, full corpus).
3. **Regression-time-only zero-imputation**; on-disk panels keep NaN.
4. **New release, no auto-flip.** Reproduction gate before new data; stop-and-report on any drift.
5. **Reuse the confirmed builders** (`build_stata_handoff` → `build_def14a_merge_release --skip-age` → `augment_age_attributes`); do not re-implement aggregation.

## 5. Out of scope
FinBERT (next round, full corpus). Tenure union / tenure-gating (Wave 4). **5b LLM brand aliases** (YouTube/Waymo — optional fast-follow; this release uses the 5a data-derived alias set). (The Tier-2 migration + company_sentiment re-aggregation are now IN scope here, via 5c — no longer deferred.)

## 6. Deliverables back to planning
The new un-flipped release path + MANIFEST · the Step-A reproduction-gate result + the Step-E verification table · a short handoff so `PIPELINE_STATE.md` records "unified flip target ready (backlog + def14a + CRSP monthly + age, verified)."

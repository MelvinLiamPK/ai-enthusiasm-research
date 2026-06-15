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

**Step B — Rebuild base panels on the EXPANDED corpus.**
Run the `build_stata_handoff` panel builders pointed at the **new** `company_sentiment_annual/quarterly` + **expanded** `sentiment_posts_scored_unique` (from `apify_backlog`) + funda + CRSP (annual & monthly) → new base `firm_panel`, `firm_quarterly`, `person_year`, `person_lifetime`. `person_year` grows by the recovered profiles' rows; `company_sentiment` + the `posts_*` files **carry over unchanged from `apify_backlog`** (already verified — do not rebuild them).

**Step C — Re-apply def14a enrichment.** `build_def14a_merge_release.py --skip-age` on the Step-B base panels → board-composition on the firm panel + def14a person fields.

**Step D — Re-apply age/attributes.** `augment_age_attributes.py` on the Step-C person panels → `def14a_birth_year` + attribute block (mirror the `url_key` person-level mapping from `age_attrs`).

**Step E — Verify (the gate).**
- **Unchanged cells byte-identical:** firm-years / company-years / person-years with **no new posts** must match `current/` exactly (firm_panel_annual, firm_quarterly, person_year). Report counts (expect ≈ `apify_backlog`'s 50,687/50,687 annual etc.).
- **Enrichment preserved:** def14a board-comp columns present + identical on unchanged firm-years; `def14a_birth_year` non-null coverage matches `age_attrs` (≈24,176 person-years / 4,145 profiles; exec-also-directors inherit age, ≈2,747 `director|executive` rows).
- **Carried files intact:** `company_sentiment_*` + `posts_*` identical to `apify_backlog`; def14a side-files / CRSP (annual+monthly) / funda / revelio symlink-inherited unchanged.
- **Discipline:** true NaN missingness preserved (no baked-in zero-imputation); LM-only (`finbert_*` = NaN on new posts).
- **Row counts sane:** person_year ≥ 143,850 (grows by recovered rows); no base columns dropped.

**Step F — Package.** New dated release `data/canonical/releases/2026-06-1X_unified/` (base `2026-06-07_crsp_monthly`) carrying its **own** `firm_panel_annual.dta`, `firm_panel_quarterly.dta`, `person_year.dta`, `person_lifetime.dta`, `company_sentiment_*` + `posts_*` (from apify_backlog); **symlink-inherit** def14a side-files, CRSP, funda, revelio. Full `MANIFEST.json` (sources, row counts, git SHA, what-changed, the strong-match=v1 note) + `CODEBOOK.md`. **Do NOT flip `current`** — owner reviews and flips. This one move then activates def14a + CRSP monthly + age + the backlog re-score together.

## 4. Decisions locked (do not relitigate)
1. **Strong-match stays v1 strict** this release (Tier-2 panel-wide is a *separate, later* methodology release).
2. **LM-only** (FinBERT next round, full corpus).
3. **Regression-time-only zero-imputation**; on-disk panels keep NaN.
4. **New release, no auto-flip.** Reproduction gate before new data; stop-and-report on any drift.
5. **Reuse the confirmed builders** (`build_stata_handoff` → `build_def14a_merge_release --skip-age` → `augment_age_attributes`); do not re-implement aggregation.

## 5. Out of scope
Tier-2 `strong_match_either` migration (separate methodology release). FinBERT (next round). Tenure union / tenure-gating (Wave 4). Any new scraping or company_sentiment re-aggregation (carry apify_backlog's, already verified).

## 6. Deliverables back to planning
The new un-flipped release path + MANIFEST · the Step-A reproduction-gate result + the Step-E verification table · a short handoff so `PIPELINE_STATE.md` records "unified flip target ready (backlog + def14a + CRSP monthly + age, verified)."

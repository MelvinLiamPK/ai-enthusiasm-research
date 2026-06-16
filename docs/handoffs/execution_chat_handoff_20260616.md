# Handoff → new EXECUTION chat (2026-06-16)

**From:** the planning chat that became an execution chat (got long) · **To:** a fresh execution chat.
**Role:** you RUN the pipeline (Redivis, Apify/Sherlock, builds, regressions). Keep spec/decision
changes in a separate planning chat — but you execute the specs below.

## Read first (in order)
1. [`docs/PIPELINE_STATE.md`](../PIPELINE_STATE.md) — file-backed state; read before assuming anything.
2. [`revelio_alias_recompute_spec_20260615.md`](revelio_alias_recompute_spec_20260615.md) — 5c (the v3 notebook is already built).
3. [`unified_reconciliation_release_spec_20260615.md`](unified_reconciliation_release_spec_20260615.md) — the merge (alias-aware Tier-2).
4. [`planning_to_chat6_decisions_20260615.md`](planning_to_chat6_decisions_20260615.md) — D1–D5.
5. `CLAUDE.md` (local-only) + [`data/canonical/README.md`](../../data/canonical/README.md).

## State (2026-06-16)
- `current` → `releases/2026-06-07_crsp_monthly` (def14a + CRSP monthly **live**).
- Un-flipped: `2026-06-12_age_attrs` (+age), `2026-06-14_apify_backlog` (+backlog re-score). **Neither is the flip target alone** — the unified merge combines them.
- **Chat 3 v1** analysis done (`outputs/chat3_v1/`), pre-backlog.
- **Chat 6 backlog**: never-submitted = 0; +1,054 profiles / +136,959 posts recovered; D1–D5 locked; committed on branch `chat6-apify-backlog-rescore` (pushed).
- **5c alias recompute**: v3 notebook **built + hardened** (`src/revelio/redivis_crosscheck_notebook_v3.ipynb` — alias-aware board leg keyed on gvkey, deterministic joins, within-run alias-OFF-vs-ON dual compute, movers export). A **first (noisy, non-deterministic) Redivis run** produced `data/revelio/revelio_validation_summary_v3.csv` — it **validated the approach** (Pichai rescued; +1,023 distinct strong URLs) but is **NOT merge-grade** (run-to-run noise). Re-run the hardened notebook for the clean version (it replaces that file).

## Pipeline ahead (ordered)
1. **Re-run the hardened v3 notebook on Redivis** (upload `data/revelio/company_aliases.csv`, set `<ver>` in Cell 6b) → clean `revelio_validation_summary_v3` + `alias_movers_v3`. **Use the within-run diff** (`strong_match_either & ~strong_match_either_noalias`) for movers — NOT v3-vs-v2. Sanity: Pichai `True`; `strong_match_either_noalias` ≈ v2's old strong-match.
2. **Movers without posts** → most are already scraped (the original Apify run submitted **all** found URLs); only the found-but-empty residual needs scraping → small Apify batch (Sherlock, strong-match-only) → LM-score.
3. **Unified merge** ([spec](unified_reconciliation_release_spec_20260615.md)): re-aggregate `company_sentiment` on v3 alias-aware Tier-2 labels + expanded corpus → `build_stata_handoff` panels → `build_def14a_merge_release --skip-age` → `augment_age_attributes` → verify (expected-change discipline) → package new release → **owner flips**.
4. **Re-run regressions** on the final panel (headline `jvrmel_06` + age).

## Wednesday-presentation track (INDEPENDENT — run now if not already)
Headline **backlog re-run** + **age regression** on EXISTING releases — does **not** need the merge. The dispatch (Task A: rebuild firm panel from `apify_backlog` sentiment via `build_stata_handoff::build_firm_panel`, run 4 jvrmel_06 specs, deltas; Task B: `ai_post_share ~ age` and `ai_mean_lm_sentiment ~ age` on `2026-06-12_age_attrs/person_year.dta`) is in the prior chat's notes. Present Wed AM off the stable corpus; flag the alias recovery as in-flight with **Pichai** as the concrete example.

## Locked decisions (do not relitigate)
Canonical: new release, no auto-flip (owner flips) · strong-match = **alias-aware `strong_match_either` (Tier-2)** in the final merge · zero-imputation is regression-time only (data keeps NaN) · aliases **lenient / non-date-gated** · **LM-only** this round (FinBERT next) · tenure = Wave 4.

## Gotchas
- v3 first-run noise = non-deterministic Revelio user pick (no `ORDER BY`); **fixed** in the hardened notebook. Movers come from the within-run off-vs-on diff.
- **`build_stata_handoff.py` builds the base firm+person panels** (not `build_def14a_merge_release.py`, which only adds board-comp).
- Most alias-movers already have posts (broad original scrape) → step 2's scrape is small.
- Normalize every join (`normalize_names.py` + `norm(url)`).

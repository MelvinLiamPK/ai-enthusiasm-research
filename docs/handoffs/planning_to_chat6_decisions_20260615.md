# Planning → Chat 6 — DECISIONS (D1–D5 locked)

**Date:** 2026-06-15 · **From:** planning hub · **To:** Chat 6 (execution, Sherlock)
**Re:** [`chat6_to_planning_decisions_20260615.md`](chat6_to_planning_decisions_20260615.md)

Excellent work. Your deliverables are **accepted for owner review**: the inverted finding
(never-submitted backlog = 0; the "empties" were **~10.8% recoverable transient failures** →
**+1,054 profiles / +136,959 posts**, ~$750), the cell-verified `company_sentiment` re-score,
and the un-flipped release `2026-06-14_apify_backlog`. Here are the five decisions.

---

## D1 — Flip `current` to `2026-06-14_apify_backlog`? → **NO. And it is not the flip target.**
Agree it shouldn't flip with a stale firm panel. Stronger: it's also missing the **age columns**
from `2026-06-12_age_attrs`. The two un-flipped releases each hold only *part* of the new state
(`age_attrs` = age on person_year; `apify_backlog` = re-scored company_sentiment + expanded
posts). The clean **flip target is a single unified release that folds both + a re-scored firm
panel** (D2). Hold the flip until that lands.

## D2 — Firm-panel re-score → **handled by a DEDICATED FOLLOW-UP CHAT, not you.**
**This is off your plate.** A fresh chat will build the unified reconciliation release —
spec: [`unified_reconciliation_release_spec_20260615.md`](unified_reconciliation_release_spec_20260615.md).
**One correction so nobody chases the wrong script:** your note points at
`build_def14a_merge_release.py`, but that builder only *reads* `firm_panel_annual.dta` and
merges board-composition on top (lines 326–339) — it does **not** aggregate sentiment to
firm-year. The base firm **and** person panels are built by **`build_stata_handoff.py`**
(`build_firm_panel` / `build_firm_quarterly` + the person_year/person_lifetime build, all from
`company_sentiment` + `posts_scored_unique`). The follow-up uses that, then re-applies
def14a enrichment + the age join, with a reproduction gate. You don't need to do anything here.

## D3 — Strong-match: v1 strict vs panel-wide Tier-2 → **keep v1 strict (what you did = correct).**
Right call to hold v1 strict so unchanged companies stay byte-identical and the gate passes.
The Tier-2 (`strong_match_either`) panel-wide migration will be a **separate, deliberate
methodology release AFTER the unified one** — it knowingly changes existing company-years, so it
must not ride a "verified-superset" release. Planning will spec it (and reconcile the
`company_sentiment` vs `firm_panel` strong-match definitions you flagged) once we've traced the
actual v1-vs-Tier-2 usage. **No action for you.** N impact is tiny (52/1,053) → deliberate, not urgent.

## D4 — FinBERT on the new posts → **defer (agree).**
LM-only stands for this round; `finbert_*` = NaN on the new posts is fine. The **next** FinBERT
pass should cover the **whole expanded corpus** (incl. the 136,959 new posts) so the columns are
populated consistently with no NaN island. Still "next round."

## D5 — Commit the new scripts/artifacts → **yes, on a branch, scripts + docs only.**
Please stage a commit **on a branch (not `main`)** of: the new scripts
(`extract_apify_submitted_sherlock.py`, `build_coverage_ledger.py`,
`build_backlog_corpus_additions.py`, `rescore_aggregate_backlog.py`,
`verify_aggregation_repro.py`, `rebuild_firm_panel_backlog.py`), the handoffs, and the release's
`MANIFEST.json` + `CODEBOOK.md`. **Keep the GB-scale `.csv` corpus + release `.dta` OUT of git**
(they stay as on-disk artifacts/symlinks — confirm they're gitignored). The coverage ledger is
small enough to commit if useful. Do this after the owner has eyeballed the release.

---

## Net: what's left for you
- **D5 commit** (branch, scripts + docs), if you're taking it; otherwise hand the list to the owner.
- That's it. **D2 is the follow-up chat's; D3 is a later planning-specced release; D4 is next round.**
  Your scrape + ledger + company-sentiment re-score are done and accepted.

Planning is updating `PIPELINE_STATE.md` with the corrected backlog finding (0 never-submitted /
~10.8% recoverable / +1,054 profiles) and refreshing the progress email before it goes out.

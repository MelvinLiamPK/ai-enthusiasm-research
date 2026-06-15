# Chat 3 — LAUNCH NOTE (overrides + dispatch)

**Date:** 2026-06-12 · **From:** planning hub · **To:** execution chat (Chat 3, full analysis v1)
**This wraps the spec — it does NOT replace it.** Your spec is
[`analysis_expansion_plan_20260530.md`](analysis_expansion_plan_20260530.md) **§4/§5/§6**
plus [`chat3_length_intensity_addendum_20260606.md`](chat3_length_intensity_addendum_20260606.md)
(John's post-length signal, folds into §5). **§3 is already done (Python parity, shipped).**
Read both, then apply the four overrides below — they reconcile the spec with decisions locked *after* it was written.

---

## Read first (in order)
1. [`docs/PIPELINE_STATE.md`](../PIPELINE_STATE.md) — corrected state. **Read before assuming a stage needs doing.**
2. [`analysis_expansion_plan_20260530.md`](analysis_expansion_plan_20260530.md) §4/§5/§6 + [`chat3_length_intensity_addendum_20260606.md`](chat3_length_intensity_addendum_20260606.md).
3. `CLAUDE.md` (local-only) + [`data/canonical/README.md`](../../data/canonical/README.md).

All inputs come from `data/canonical/current/` (now → `releases/2026-06-07_crsp_monthly`). Never read a dated file under `outputs/` or `data/processed/` directly.

---

## The four overrides (apply these to the spec as written)

**① §4 — build the panel UNGATED. Skip the tenure union / tenure-gating entirely.**
§4's "Tenure-gated attribution (corpus-wide correctness fix)" sub-bullet (the WRDS∪def14a tenure union, the (profile_url,ticker) tenure window, gating posts to in-tenure years) is **Wave 4, NOT this chat** (locked decision #6 — it depends on the backlog scrape landing first). Build the annual / quarterly / monthly firm panel attributing **every** post to its firm-year regardless of tenure window. Everything else in §4 (multi-frequency grain, calendar↔fiscal mapping, the frequency-laddering missing-data rule, true-missingness-only storage, carrying the metric set + raw `n_posts`/`n_ai_posts`) stands.

**② §5.1 — the monthly-CRSP blocker is CLEARED.** §4/§5.1 flag "needs a monthly CRSP pull — in scope or a blocker?" It is **done and live**: `current/crsp_monthly_returns.csv` (387,652 rows, 2009-01 → 2024-12; no 2025) + the fixed-permno `current/crsp_annual_returns.csv`. Build the monthly/quarterly return regressions against those. (Note the 2024-12 ceiling — don't expect 2025 monthly returns.)

**③ §6 age/gender — read age from the SIDE-FILE for v1; don't build a panel join.**
`person_year.dta` has `def14a_gender` but **no age** and none of the board attributes. For v1 descriptives:
- **Age:** join `current/def14a_director_status.csv` (cols `age`, `age_source`; keyed `ticker, year, full_name`) onto your person-firm-year frame ad-hoc on **(normalized full_name, ticker, year)**. Label these "ad-hoc, director-only (~95.5% of def14a director-rows / ~21% of the universe)." The **formal `def14a_birth_year`/attributes column on `person_year`** is being specced separately ([`age_attributes_panel_join_spec_20260612.md`](age_attributes_panel_join_spec_20260612.md)) for v2 / age-as-regressor — do **not** build it here; the ad-hoc read is sufficient for descriptives.
- **Gender:** use `executives_all.csv` `gender` directly for execs; `current/def14a_director_status.csv` has no gender but `current/def14a_director_attributes.csv` does (91%); `person_year.def14a_gender` is already populated. Name-inference only as a flagged fallback for blockholders.
- Age is **director-only** — execs/blockholders have none unless separately pulled (out of scope). State this in the writeup.

**④ Fold in the length/intensity addendum** ([`chat3_length_intensity_addendum_20260606.md`](chat3_length_intensity_addendum_20260606.md)) as part of §5: build the three intensity measures, run the four horse-race specs (a–d), report the one-line verdict (does length/intensity add signal *after* extensive margin + density?). Sentiment is a per-word **density**, so post length is genuinely orthogonal — this is the one margin nothing else captures. Restrict length/count measures to posts with `lm_word_count > 0` (drop pure reshares).

---

## Locked — carry, do not relitigate
- Zero-imputation is **regression-time only**, on an in-memory copy; on-disk panels keep **true NaN**. Exact rule (identical to Chat 2): `ai_sent_new = 0 where (n_posts_strong≥1 & n_ai_posts_strong==0)`, `= NaN where n_posts_strong==0`, else `= ai_mom_net_sentiment_strong`.
- **Strong-match (`strong_match_either`) is the default sample**; min-post filters dropped; mean-of-mean is the headline measure.
- Build everything **v1 on the current corpus** — corpus-agnostic, re-runs in v2 after the Chat 6 backlog scrape lands (Wave 4 re-runs Chats 2+3).

## Deliverables back to planning
The §4 multi-frequency panel (built, with a row-count + missingness sanity report) · the §5 spec tables (incl. the four length/intensity horse-race tables + verdict) · the §6 descriptives (AI sentiment by industry / employees / R&D / age / gender) · a short handoff so PIPELINE_STATE can record "Chat 3 v1 done."

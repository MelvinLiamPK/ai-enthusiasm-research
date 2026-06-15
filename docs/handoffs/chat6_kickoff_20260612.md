# Chat 6 — KICKOFF (dispatch message)

**Date:** 2026-06-12 · **From:** planning hub · **To:** execution chat (Chat 6, Sherlock)
**This is the launch wrapper. Your full spec is** [`chat6_apify_backlog_spec_20260606.md`](chat6_apify_backlog_spec_20260606.md) **— read it end-to-end; it is self-contained.**

---

## Read first (in order)
1. [`docs/PIPELINE_STATE.md`](../PIPELINE_STATE.md) — the corrected, file-backed state. **Read before assuming anything needs doing.** Several chats mis-derived state from raw files; this is the fix.
2. [`chat6_apify_backlog_spec_20260606.md`](chat6_apify_backlog_spec_20260606.md) — your spec (ledger → scrape → re-score).
3. `CLAUDE.md` (local-only, gitignored) + [`data/canonical/README.md`](../../data/canonical/README.md).

## You are GO — no dependency
- Canonical `current` is flipped to `releases/2026-06-07_crsp_monthly` (2026-06-12). Nothing you do is blocked by it; you write a **new** release at the end and do **not** flip.
- Search + Revelio crosscheck are **already done for everyone** — you are **Apify-only**. Do not re-run discovery or crosscheck.

## The one hard gate (do not skip)
**Task 0 — build the person-level coverage ledger and report Deliverable B BEFORE any Apify spend.**
- The "9,551 strong-match URLs without posts" is an **upper bound**, not a scrape target. The real fundable backlog = the `~apify_submitted` (never-submitted) slice, which is **only determinable from the Sherlock Apify submission logs** (batch2/batch3 input lists under `/home/users/ml2068/.../scraped_posts_batch2|batch3/`). That's why this runs on Sherlock.
- Use `src/revelio/normalize_names.py` for the person key and `norm(url)` for URL joins — **normalize both sides of every join.** Raw-string matching produces fake coverage gaps (see PIPELINE_STATE).
- **Report Deliverable A (coverage certification) + Deliverable B (the hard 3-way split: has_posts / submitted-empty / never-submitted) back here before spending a dollar.**

## Then
- **Task 1b first** (the ~$150 lever): re-scrape a ~200-URL random sample of `apify_submitted & ~has_posts` to measure recoverability. High recovery → widen scope; near-zero → drop that set. Resolve this before any blanket re-scrape.
- **Task 1:** scrape `strong_match_either & ~has_posts & ~apify_submitted`. Strong-match-only. No pilot, no hard cap; checkpoint + running cost report; owner tops up credits.
- **Task 2:** **LM-only** re-score (FinBERT is next round, not now) → new dated canonical release w/ MANIFEST + CODEBOOK. **Do not flip `current`.** Keep true missingness (NaN); zero-imputation stays regression-time only.

## Locked — do not relitigate
Apify-only · strong-match-only scrape · LM-only re-score · no pilot/no cap (owner tops up) · new release no auto-flip · normalize every join.

## Report back to planning (so PIPELINE_STATE can be updated)
1. `coverage_ledger_<ts>.csv` + Deliverable A report. 2. The hard 3-way backlog number (Deliverable B) + Task-1b recovery rate. 3. Apify spend + profiles/posts added. 4. The new (un-flipped) release path. 5. A short handoff doc back here.

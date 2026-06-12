# Master sequencing — chats, order, dependencies

**Date:** 2026-06-02
**Owner:** Melvin
**Purpose:** The single source of truth for *which chat does what, in what order*.
Each chat gets its detailed spec via the linked handoff doc; this file is the map.

**This SUPERSEDES the 3-chat workplan in `pre_meeting_plan_20260529.md`.** Do not
launch those old Chat A/B/C separately — they're folded in below.

---

## Cross-cutting rules (every chat must follow)

- **Canonical discipline.** Each chat that changes data writes a NEW dated release
  under `data/canonical/releases/` and **does NOT flip `current`**. The owner
  reviews diffs and flips. Prevents concurrent chats clobbering the source of truth.
  See `data/canonical/README.md`.
- **True missingness in data; imputation at regression time.** Data files keep NaN
  where a firm-year posted but had no AI posts. Zero-imputation (`ai_sent_new`) is
  applied in the regression/`.do` scripts only. Chats 2 and 4 must use the IDENTICAL rule.
- **v1 now, v2 after expansion.** The analysis machinery is corpus-agnostic. Build +
  run it now on the current corpus (v1: validation + interim numbers); re-run on the
  post-expansion corpus later (v2: final). This double-run is intended.

---

## The chats

### Chat 1 — DEF 14A merge
- **Spec:** `def14a_merge_build_spec_20260602.md`
- **Does:** age backfill from bio_text; clean per-(profile_url,ticker) `director_since`
  + `new_nominee` table (feeds the tenure union); firm-year board-composition
  features; person-level enrichment. **8-K dropped; tenure-gating NOT here.**
- **Depends on:** nothing (runs on existing def14a outputs + corpus).
- **Outputs feed:** Chat 2 (tenure union refinement), Chat 3 (age, board-composition).

### Chat 2 — Internal parity + headline regression refresh
- **Spec:** `analysis_expansion_plan_20260530.md` §3.
- **Does:** port the ratified defaults into `sentiment_q_regression.py` (drop
  min-post filters; **regression-time zero-imputation** — `ai_sent_new`); re-run the
  four headline regressions → the v1 corrected numbers (should match John's
  `jvrmel_06`).
- **TENURE UNION DEFERRED (decision 2026-06-02).** Do NOT build the WRDS∪def14a
  tenure panel or apply tenure-gating yet — we search for the def14a directors' URLs
  first (Chat 6) and merge tenure only once they have posts. Tenure-union + gating
  move to **Wave 4** (post-expansion).
- **Depends on:** nothing hard (current canonical). **Coordinate the imputation rule
  with Chat 4.**

### Chat 3 — Full analysis agenda (v1, current corpus)
- **Spec:** `analysis_expansion_plan_20260530.md` §4 (multi-frequency) + §5 + §6.
- **Does:** monthly/quarterly panel + calendar→fiscal + missing-data laddering;
  new specs (5-yr long-diff + outliers, size-weighted, monthly/quarterly stock
  returns, AI keyword time series, LOESS); descriptives by industry / employees /
  age / gender.
- **Build the §4 panel WITHOUT tenure-gating in v1** (tenure union is deferred to
  Wave 4). The panel + specs are corpus-agnostic machinery — they re-run in v2 with
  tenure-gating once the expansion lands.
- **Depends on:** Chat 1 (age for §6; gender from `executives_all.csv`); the monthly
  CRSP pull (Wave 1 task). **Does NOT depend on the tenure union.**

### Chat 4 — RA dataset release (external)
- **Spec:** `ra_dataset_release_20260530.md`
- **Does:** apply the ratified defaults in the external Stata/R files; package the
  current canonical snapshot; ship to the other RAs. Labeled pre-expansion snapshot.
- **Depends on:** nothing hard (current canonical). **Coordinate the imputation rule
  with Chat 2.**

### Chat 5 — Company alias workstream (Pichai fix)
- **Spec:** `pre_meeting_plan_20260529.md` §4 + the alias task in
  `meeting_notes/20260530_decisions_and_next_scrape.md`.
- **Does:** (a) **data-derived name-change list FIRST** — EDGAR `formerNames` + CRSP
  `stocknames` + Compustat (the small piece that must precede expansion discovery);
  (b) LLM brand-alias layer; merge → `company_aliases.csv`; (c) apply aliases to
  recompute Revelio `strong_match` (lenient/non-time-gated: match any alias
  regardless of date).
- **Depends on:** nothing. **Its step (a) is the prerequisite for Chat 6.**

### Chat 6 — Apify backlog scrape (long-pole; now a Wave-2 priority)
- **Spec:** [`chat6_apify_backlog_spec_20260606.md`](chat6_apify_backlog_spec_20260606.md). **REFRAMED 2026-06-06 — read it + `docs/PIPELINE_STATE.md`.**
- **CORRECTION:** the old framing below ("URL discovery for ~5,647 new directors →
  Apify") was **wrong**. Per `PIPELINE_STATE.md`, **Serper discovery AND the Revelio
  crosscheck are already DONE for everyone** (def14a via the May-16 primary-anchor
  rescrape); the new def14a directors are essentially scraped already. What remains is a
  **post-scrape backlog** of validated strong-match URLs — an **Apify-only** job.
- **Does:** (Task 0) build the person-level **coverage ledger** → certify Serper/Revelio
  coverage + resolve the true backlog from the Sherlock Apify submission logs (the 9,551
  strong-match-without-posts is an UPPER bound; unknown how many were ever submitted);
  (Task 1) Apify-scrape the **never-submitted** strong matches (+ a ~200-URL recoverability
  probe on the submitted-empty set); (Task 2) **LM-only** re-score → NEW canonical release.
  Checkpoint + cost report; no pilot/no cap; owner tops up credits. Runs on Sherlock.
- **Depends on:** nothing hard (NOT 5a — no discovery happens here). Still **the gate for
  the tenure union** — once the backlog is scored, Wave 4 builds WRDS∪def14a tenure + gating.

---

## Order (waves) — revised per 2026-06-02 decisions

| Wave | Chats (parallel within a wave) | Gate |
|---|---|---|
| **1** | Chat 1 (def14a merge) · Chat 2 (parity + headline refresh) · Chat 4 (RA release) · Chat 5a (data aliases) · **Monthly-CRSP pull** | none |
| **2** | **Chat 6 (Apify backlog scrape — reframed)** · Chat 3 (full analysis v1) · Chat 5b/c (LLM alias + apply) | Chat 6← none (Apify-only; 5a no longer required); Chat 3←Chat 1 + CRSP pull |
| **4** | **Build the tenure union (WRDS∪def14a) + tenure-gating**, then re-run Chats 2+3 pipeline on the post-expansion corpus (v2 final) + new-nominee instrument | ←Chat 6 + re-score |

Wave 1 is fully parallel — all start immediately. Chat 6 (the long-pole) can kick off
**immediately** (no 5a dependency — it's Apify-only on already-validated strong matches)
and runs in the background while Chat 3 does v1 on the current corpus. **Tenure work all
lands in Wave 4** (after the backlog scrape is scored).

**Monthly-CRSP pull (Wave 1 task):** extend `build_crsp_returns.py` (or a sibling)
to pull **monthly** CRSP returns from WRDS → `data/extracted/crsp/crsp_monthly_returns_*.csv`,
add it to the canonical release. Quick; unblocks Chat 3's sub-annual stock-return specs.

---

## Resolved decisions (2026-06-02)

1. **Monthly CRSP: YES, pull it** (Wave 1 task above).
2. **Zero-imputation:** regression-time, in Chats 2 & 4 (identical rule). Data keeps NaN.
3. **Tenure union DEFERRED to Wave 4.** Reason: tenure-gating joins tenure to the
   *posts*, which are keyed on `profile_url`. The ~5,647 new def14a directors have no
   `profile_url` (and no posts) until Chat 6 discovers + scrapes them, so a merge now
   would be incomplete and redone later. Search URLs first, then merge. (Owner's
   rationale: only the *final* data matters, so do tenure once, at the end.)
   NOT deferred — the def14a work that isn't url-keyed: board-composition
   `(gvkey, year)`, age, and the new-nominee table all proceed in Chat 1 now.
4. **Expansion:** no pilot; run in waves; owner increases credits as it goes (no hard cap).

## Flags

Old workplan superseded · canonical no-auto-flip discipline · imputation rule
identical across Chats 2 & 4 · v1→v2 rework intended · name-only identity in the
(deferred) tenure union.

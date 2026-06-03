# Handoff → New planning chat

**Date:** 2026-06-02
**From:** Prior planning chat (got long; starting fresh)
**To:** The next planning chat
**Your job:** Be the **planning hub** for the AI-enthusiasm project — write specs &
handoffs, sequence chats, make/record design decisions. **Do NOT execute** (no
scraping, no big builds, no kicking off other chats). The owner launches execution
chats from your specs.

## Read these first (in order)

1. **`docs/handoffs/master_sequencing_20260602.md`** — THE PLAN. Chats, waves,
   dependencies, resolved decisions. Everything below is context for it.
2. `CLAUDE.md` — project overview, pipeline, conventions, the source-of-truth layer.
3. `data/canonical/README.md` — how the source of truth works (releases, manifest,
   `current` pointer). All analysis reads `data/canonical/current/`.

## The plan in one paragraph

The corpus (2.9M LinkedIn posts, 100% LM-scored) is frozen at the 2026-05-27
canonical release. Work is split into **chats run in waves** (see master doc):
Wave 1 (def14a merge, internal parity, RA dataset release, data-derived company
aliases, monthly-CRSP pull) → Wave 2 (Apify expansion = the long pole; full
analysis v1; LLM aliases) → **Wave 4** (tenure union + tenure-gating + v2 final
re-run + the new-nominee instrument). Tenure work was deliberately pushed to Wave 4
because it must join to posts on `profile_url`, and the new def14a directors don't
have URLs/posts until the expansion scrapes them.

## Document map

| Doc | What it is | Status |
|---|---|---|
| `master_sequencing_20260602.md` | THE PLAN — chats/waves/decisions | **current** |
| `analysis_expansion_plan_20260530.md` | §3 parity · §4 multi-freq panel + tenure · §5 specs · §6 descriptives | current (Chats 2/3) |
| `def14a_merge_build_spec_20260602.md` | def14a merge: age, new-nominee table, board-composition | current (Chat 1) |
| `ra_dataset_release_20260530.md` | external Stata/R defaults + package for RAs | current (Chat 4) |
| `def14a_director_status_RESULTS_to_planning_20260601.md` | results of the def14a classification (input) | reference |
| `def14a_director_status_classification_20260528.md` | original def14a spec | done |
| `pre_meeting_plan_20260529.md` | OLD 3-chat workplan — **superseded** by master, but its §4 alias methodology is still referenced by Chat 5 | partial |
| `track1_stata_handover_20260527.md` | earlier Track-1 stata handover | reference |
| `meeting_notes/20260530_decisions_and_next_scrape.md` | decisions log + Apify-expansion plan + name-change task | current |

Also: `src/summary_stats/compute_summary_stats.py` (headline descriptives from
canonical). Memories: `project_def14a_director_status`, `project_sprint_sequencing_20260530`.

## Decisions locked — do NOT relitigate

1. **Source of truth = `data/canonical/current/`.** Dated immutable releases + a
   `current` symlink. Every chat writes a NEW release and does **not** auto-flip
   `current` (owner reviews + flips). Prevents concurrent chats clobbering data.
2. **Zero-imputation is regression-time only** (`ai_sent_new`: 0 where a firm-year
   posted but had no AI posts; NaN where it didn't post). **Data files keep true
   NaN.** Internal Python (Chat 2) and external Stata/R (Chat 4) must use the
   identical rule.
3. **Strong-match = default sample; `n_posts≥10`/`n_ai_posts≥3` filters dropped.**
   Mean-of-mean is the headline sentiment measure.
4. **Tenure = WRDS panels ∪ def14a**, keyed `(profile_url, ticker)`. WRDS
   (`directors_all`/`executives_all`/`blockholders_all`, person×firm×year, 2010–2025)
   is the primary source; **def14a is the SOLE source for the ~5,647 directors (30%)
   not in WRDS.** All tenure work is **deferred to Wave 4** (needs URLs first).
5. **Never-elected placebo group is empty** (>99% uncontested elections; 8-K pull
   DROPPED). The surviving identification angle is **new-nominees' pre-board
   sentiment**, which is scrape-limited (345 of 1,332 have posts) and needs the
   expansion to be credible. Exogeneity is **firm-specific** — "already a director
   elsewhere" is NOT a contamination; the real threat is endogenous director-firm
   matching (handled by timing + controls).
6. **Monthly CRSP: pull it** (Wave 1 task). **Expansion: no pilot**, run in waves,
   owner tops up credits.
7. **Company name/ownership aliases**: data-derived (SEC EDGAR `formerNames` + CRSP
   `stocknames` + Compustat) + LLM brand-alias layer; **comprehensive but LENIENT
   matching** (match any alias regardless of date — Sundar-says-Google → Alphabet).

## Open planning tasks (pick these up)

- **Write the Chat 6 (Apify expansion) spec** when the owner reaches Wave 2 — not
  yet written. Scope: URL discovery for ~5,647 new directors + ~987 unscraped
  new-nominees (using the data-derived alias list) → Apify scrape → re-score → new
  canonical release.
- *(Optional)* consolidate Chat 5's spec into one `company_alias_build_spec` (its
  scope is currently split across `pre_meeting_plan §4` + the meeting-notes task).
- As chats finish, reconcile their canonical releases and advise the owner when to
  flip `current`.
- Eventually: the Wave-4 spec (tenure union + gating + v2 re-run + instrument).

## Gotchas the prior chat hit (don't repeat)

- Tenure is **not** def14a-only — WRDS annual panels already have it corpus-wide;
  def14a only adds the non-WRDS directors + clean new-nominee flags + age. (The
  combined `all_people.csv` dropped `year` in dedup — rebuild tenure from the
  `*_all.csv` files.)
- Don't frame the 345 new-nominees-with-posts as "selection-biased wrong people" —
  exogeneity is firm-specific (see decision 5).
- Gender for executives is already in `executives_all.csv`; only directors/
  blockholders need name-inference.
- `age` was NOT extracted in the def14a run — backfill from `def14a_bio_text`
  (stored), don't re-scrape.

## Key numbers (so you don't recompute)

Corpus: 2,922,365 post rows · 26,511 profiles · 100% LM coverage. def14a: 96,955
director-rows, 9,588 filings, statuses {incumbent 87,779 · mid_year 2,829 ·
new_nominee 1,640 · not_director 4,323}. Tenure panel: 224,429 rows, name→url bridge
71%. Corpus overlap: 4,160 classified directors already have posts; 345 new-nominees
with posts; ~5,647 def14a directors (30%) are new names needing the pipeline.

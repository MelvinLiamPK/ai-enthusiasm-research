# Build spec → DEF 14A merge release (execution chat)

**Date:** 2026-06-02
**From:** Planning chat
**To:** Execution chat (build it)
**Reads:** `def14a_director_status_RESULTS_to_planning_20260601.md` (what was produced),
`def14a_director_status_classification_20260528.md` (classification rules + tenure logic),
`data/canonical/README.md` (the release mechanism).

Build a new canonical release that merges the DEF 14A director classification +
tenure into the analysis panels, **on the posts we already have**. Then leave it
for owner review (do NOT auto-flip `current`).

## Decisions locked (do not re-litigate)

1. **Proceed, reframed.** The never-elected placebo group is empty (YoY diagnostic
   ⇒ ~0 genuine election failures; >99% uncontested). The def14a value:
   **(a) tenure-gated attribution correctness** (firm-year sentiment counts only
   directors actually serving that year) and **(b) the new-nominee pre-board-sentiment
   instrument**. Exogeneity is **firm-specific**: a new nominee's pre-board sentiment
   need only be exogenous to the firm they're joining, NOT to corporate AI in general
   — so "already an incumbent elsewhere" is fine (those people even have an observable
   pre-join posting history). The genuine identification threat is **endogenous
   matching** (does the firm recruit AI-keen directors *because* it already plans AI?),
   addressed via timing + controls, not by filtering on insider status. The 345
   new-nominees with posts are a usable but **small/scrape-limited** starting sample
   (of 1,332 total); the Apify expansion grows N/power by reaching the ~987 not-yet-scraped.
2. **8-K Item 5.07 pull: dropped.** Treat all `new_nominee` as seated from year T.
   No elected/not-elected split.
3. **Age: backfill** from the stored `def14a_bio_text` (no re-scrape).
4. **Lead vs contemporaneous: build agnostic.** The merge only tenure-gates
   attribution; the lead structure is a regression-time choice (like the
   zero-imputation). Do not hard-code a lead.
5. **Calendar→fiscal: default** = map a post's calendar date to the firm's fiscal
   year that contains it (Compustat FY-end month per gvkey; Dec-FYE ⇒ calendar=fiscal).
   Document it; it's a methodological default, not a hard requirement.

## The analyzable set (already computed — no new scrape needed)

Matching tenure-panel bridged URLs against the existing corpus:

- **4,160** classified directors already have scraped posts (≈3,400 from the
  original WRDS universe scrape, 742 from the earlier def14a pilot).
- **5,280** distinct (director × firm) served-pairs with posts.
- **345** `new_nominee` person×firm cases with posts (the pre-board-sentiment sample).
- 45,238 tenure (director×firm×year) rows attach to a corpus profile.

This is a floor; the future Apify expansion grows it but is **not** a prerequisite.

## ⚠️ Tenure source: WRDS annual panels, NOT def14a (correction 2026-06-02)

Tenure for attribution-gating comes from the **WRDS annual panels**, which are
person × firm × **year** for all three roles and cover the whole corpus:
- `data/extracted/directors/directors_all.csv` (2010–2025; 272,646 rows)
- `data/extracted/executives/executives_all.csv` (2010–2025; has `gender`, `is_director`, `execid`)
- `data/extracted/blockholders/blockholders_all.csv` (2010–2023)

For each (person, firm), the years they appear = their service window (fill rare
mid-tenure gaps via min–max). This gates attribution **corpus-wide** (~26k
profiles, all roles), not just the 4,160 def14a directors. The combined
`all_people.csv` dropped `year` in dedup — rebuild a person × firm × year tenure
panel from the `*_all.csv` files.

**def14a's role narrows to a refinement layer:** (1) clean **new-nominee
classification** (`director_since` vs proxy year separates true first-time
nominees from WRDS left-censoring — this is what the instrument needs), (2)
`director_since` backfill for pre-2010 starts, (3) **age** + bio text. Tenure
gating does NOT depend on def14a.

## Inputs

WRDS panels above, plus from `data/canonical/current/`: `def14a_director_status.csv`,
`def14a_director_tenure.csv`, `posts_full_coverage.csv`, `posts_scored_unique.csv`,
`company_sentiment_annual.csv`, `firm_panel_annual.dta`, `firm_panel_quarterly.dta`,
`person_year.dta`, `person_lifetime.dta`, `revelio_validation_summary.csv`,
`funda_annual.csv`. Bridge file: `data/processed/def14a_urls_for_revelio_validation.csv`.

## Build steps

1. **Age backfill.** Parse `def14a_bio_text` → `age` (regex for `, NN,` / `age NN`;
   light LLM fallback for misses). Write `def14a_director_status` **as a new dated
   file** (do NOT append `age` to the existing CSV in place — see the OUTPUT_FIELDS
   note in `classify_def14a_director_status.py`; the old file's header has no age
   column). Report coverage (% of director rows with an age).

2. **Corpus-intersection table.** Persist the (profile_url, ticker, year,
   def14a_director_status, tenure_start/end, age) rows restricted to corpus
   profiles — the analyzable spine. ~45,238 rows.

3. **Tenure: this chat is a REQUIRED source, not just a refinement.** The merged
   `tenure_panel.csv` is assembled in the panel rebuild
   (`analysis_expansion_plan_20260530.md` §4) by **unioning WRDS panels with def14a
   tenure**. def14a is the **sole tenure source for the ~5,647 directors (30%) not
   in WRDS** — WRDS has nothing for them. **This chat's deliverable to the union:**
   a clean per-(profile_url, ticker) table with `tenure_start` (from `director_since`,
   else first proxy year), `tenure_end` (last proxy year), `right_censored`, and a
   `new_nominee` flag (genuine first-time, via director_since vs proxy year — more
   precise than WRDS first-appearance). Hand it over; the §4 builder does the union
   and the actual firm-period gating. Don't recompute firm-year sentiment here.

4. **Firm-year board-composition features** joined onto `firm_panel`: per (gvkey,
   year) counts of `incumbent` / `new_nominee` / `mid_year_appointee`, board size,
   `mean_director_age`, `share_new_nominee`.

5. **Person-level enrichment** on `person_year` / `person_lifetime`: attach
   `def14a_director_status` and tenure per (person × firm); flag each person's
   **pre-board years** (post years before their `tenure_start` at a firm) so the
   pre-nomination-sentiment sample is directly queryable. **Codebook note:** the
   current new-nominee-with-posts sample is small (345 of 1,332, scrape-limited) —
   usable as a starting instrument sample, with N growing after the Apify expansion.
   Add an `is_established_elsewhere` boolean (incumbent at another firm) as a
   heterogeneity/robustness dimension — NOT a validity filter (exogeneity is
   firm-specific, so insider-elsewhere status doesn't disqualify a case).

6. *(Lower priority)* merge `def14a_director_status` + `def14a_bio_text` as columns
   onto the corpus files keyed on (profile_url, ticker). The panels above are what
   analysis reads; do this only if cheap.

## Package as a new canonical release

- New folder `data/canonical/releases/2026-06-02_def14a/`. Carry forward the
  unchanged sentiment/corpus/funda/crsp files (symlinks, same stable names).
- Add/replace: the tenure-gated firm panels, board-composition features, enriched
  person panels, the new age-bearing `def14a_director_status`, and the
  corpus-intersection table.
- Write `MANIFEST.json` (source paths, rows, git SHA, build date, **what changed
  vs 2026-05-27**) and update `CODEBOOK.md` for the new columns.
- **Do NOT flip `current`.** Leave it on `2026-05-27` until the owner reviews the
  tenure-gating diffs, then flips. Note this in the manifest.

## Caveats to respect (from the results handoff)

- Cross-firm person identity is **name-only** (no stable person-id) — keep status
  per (person × firm); don't collapse across firms.
- `classifier_confidence` is uncalibrated LLM self-report — not a probability.
- 148,392/224,429 tenure rows are right-censored (current directors) — expected.
- `primary_company` was not extracted (recoverable from `bio_text` later; not needed here).
- Name→URL bridge is 71% of director-firm pairs; the 30%-with-posts is a floor.

## Verification

- Age coverage reported; new `def14a_director_status` keeps the old columns + `age`.
- Tenure-gating: report Δ firm-years and a before/after on one headline regression
  (e.g. ln_tobins_q ~ ai sentiment) so the correctness impact is visible.
- Spot-check a multi-board director (incumbent at one firm, recently-joined at
  another) — status and tenure differ correctly by firm.
- The 345-case pre-board-sentiment sample is queryable from the person panel.
- New release builds; `current` still points at `2026-05-27`; MANIFEST documents the diff.

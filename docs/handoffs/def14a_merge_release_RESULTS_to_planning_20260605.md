# Handoff → Planning: DEF 14A merge release BUILT (Wave 1 deliverable)

**Date:** 2026-06-05
**From:** Execution chat (built the merge release)
**To:** Planning chat / owner
**Reads:** `def14a_merge_build_spec_20260602.md` (the spec this fulfils),
`def14a_director_status_RESULTS_to_planning_20260601.md` (the classification it builds on),
`data/canonical/README.md` (release mechanism).

---

## 0. Status in one line

The DEF 14A merge release is **built and verified** at
[data/canonical/releases/2026-06-05_def14a/](../../data/canonical/releases/2026-06-05_def14a/).
`current` is **NOT flipped** — it still points at `2026-05-27`, awaiting owner review.
All four in-scope deliverables are done; director **age coverage = 95.5%**. A follow-up pass
(2026-06-06) also folded in **gender (91%), primary_company (current employer), and board
leadership** — all from data already on disk, $0 (see §1e). Out-of-scope items (8-K,
tenure-gating, WRDS∪def14a tenure union) were left for Wave 4 as instructed.

**To activate:** `ln -sfn releases/2026-06-05_def14a data/canonical/current`

---

## 1. What was built (the four in-scope deliverables)

Build script: [src/data_extraction/build_def14a_merge_release.py](../../src/data_extraction/build_def14a_merge_release.py)
(deterministic, re-runnable; `--skip-age` reuses the cached age extraction).

### (a) Director AGE — backfilled to 95.5%, no re-scrape
The original classification never persisted age (it's in the LLM model but not in
`OUTPUT_FIELDS`). We recovered it from data already on disk, in two passes:

1. **Deterministic** ([build script](../../src/data_extraction/build_def14a_merge_release.py)):
   HTML `Name|Age|Director Since` table-parse ∪ `def14a_bio_text` regex → **78.8%** at **$0**.
2. **Targeted LLM** ([src/data_extraction/backfill_age_llm.py](../../src/data_extraction/backfill_age_llm.py)):
   Haiku 4.5 read the roster window for the residual 3,486 filings (name→age only, tiny
   output) → +15,312 ages → **95.5%** for **$48.44**, 0 errors.

| metric | value |
|---|---|
| Age coverage | **88,501 / 92,632 director rows = 95.5%** |
| Source mix | `table` 59,668 · `llm` 15,491 · `regex` 12,916 · `table(disagree_regex)` 426 |
| Distribution | mean 62.6, range 25–99 |

**Validation (year-delta consistency — age should increment ~1/calendar-year):**
- Of all ages, **95.3% are testable** (person×firm in ≥2 years); 4.7% appear once (untestable, not failures).
- Of testable ages, **98.45% pass ±1**; over the whole sample **93.9% pass, 4.7% untestable, 1.5% fail**.
- Per-pair: exact 96.4% · within ±1 98.8% · within ±2 98.9% · off>2 1.07%. Errors are
  **bimodal** (right, or large misalignment) — ±2 barely beats ±1, so the ~1% failures are
  genuine misalignments, flagged with a `|qc_year_delta_off` suffix in `age_source`.
- By source the LLM ages are **as clean as deterministic** (within ±2: regex 99.7% · llm 99.3% · table 99.0%).

**Remaining ~4.5%** omit age from the filing entirely (special-meeting/merger proxies, stub
rosters) or are non-director NEO rows. Not recoverable without a different source.
**Revelio was checked and ruled out** — its `individual_user`/`individual_position` tables
carry no age/birth field (only `highest_degree` + predicted demographics).

### (b) Clean per-(profile_url, ticker) new-nominee / tenure table — Wave-4 union input
[def14a_new_nominee_tenure.csv](../../data/canonical/releases/2026-06-05_def14a/def14a_new_nominee_tenure.csv) — **24,410** rows (person × firm).
Columns: `profile_url` (bridged, 17,453 populated), `full_name`, `ticker`, `director_since`,
`tenure_start`, `tenure_end`, `right_censored`, `new_nominee`, `first_proxy_year`, `n_proxy_years`.
- **`new_nominee` = 1,423 True.** Defined as the classifier's `status==new_nominee` (cleared
  when `director_since` predates first proxy by >1yr → left-censored incumbent). NOT broadened
  on `director_since` (an earlier draft did, inflating to 2,677 — corrected).
- **811** new-nominees carry a bridged `profile_url`; the in-corpus (with-posts) subset is the
  scrape-limited instrument sample (a few hundred), growing after the Apify expansion.
- Status is strictly **per (person × firm)** — 361 people are new_nominee at one firm AND
  incumbent at another (e.g. Adriana Cisneros: new at Ford 2025, incumbent at Mattel since 2018).

### (c) Firm-year board-composition features keyed (gvkey, year)
[def14a_board_composition.csv](../../data/canonical/releases/2026-06-05_def14a/def14a_board_composition.csv) — **9,554** rows. Joined onto `firm_panel_annual.dta`.
Columns: `board_size`, `n_incumbent`, `n_new_nominee`, `n_mid_year_appointee`,
`share_new_nominee`, `mean_director_age`, `age_coverage`. ticker→gvkey via `directors_all.csv`.
- mean board size **9.7**, mean director age **62.3**, **mean board age_coverage 0.955**
  (7,781 of 9,554 firm-years have FULL board-age; `mean_director_age` populated for 98.2%).

### (d) Person-level enrichment
- `person_year.dta` (+6 cols): `def14a_director` (16,010), `def14a_new_nominee` (967 seat-years),
  `def14a_tenure_start/end`, **`pre_board_year`** (3,978 — the pre-nomination-sentiment window,
  directly queryable for the instrument), **`is_established_elsewhere`** (11,007 — heterogeneity
  dimension, NOT a validity filter; exogeneity is firm-specific).
- `person_lifetime.dta` (+4 cols): `def14a_director` (4,170), `def14a_ever_new_nominee` (317),
  `def14a_n_firms`, `is_established_elsewhere`.
- Plus the analyzable spine [def14a_corpus_intersection.csv](../../data/canonical/releases/2026-06-05_def14a/def14a_corpus_intersection.csv) — **48,316** tenure rows on corpus
  profiles (4,166 distinct profiles, 5,287 distinct profile×firm).

### (e) Director ATTRIBUTES — added 2026-06-06 (no API, all from data on disk)
A second pass folded in director-level attributes recoverable from data we already hold
([src/data_extraction/augment_release_attributes.py](../../src/data_extraction/augment_release_attributes.py),
[parse_def14a_bio_features.py](../../src/data_extraction/parse_def14a_bio_features.py)):

- **gender** — parsed from `bio_text` (honorific/pronoun). **91.1%** of director rows;
  **99.79% agreement** vs Execucomp on 1,951 overlapping directors. Directors had no gender before.
- **primary_company** (current employer) — **important correction:** a full-scale DEF 14A
  primary-employer extraction *was already run* (it survives embedded in
  `revelio_validation_rows_v2.csv` / `all_linkedin_urls_v2.csv`, NOT in the stale 128-row
  `def14a_extracted_bios.csv` that the classification handoff cited). Recovered it as a clean
  (person × firm) map: **14,075 real employers** (13,850 people, 1,627 tickers) after dropping
  ticker-fallback rows. **`primary_title` is NOT available at scale** — it did not survive the
  URL-pipeline merge (only in the 128-row pilot). Gives the "is this director a tech-company
  exec" signal and re-anchors LinkedIn URL discovery for unmatched directors.
- **board_leadership_role** (board_chair / lead_independent_director / lead_director) — 23%
  (positive-flag; absence ≈ not a leader).
- **committees / independent / audit_financial_expert / n_other_public_directorships** — parsed
  from `bio_text` but **table-bound, so low (10–22%)**; same LLM-on-filing lift that took age
  79%→95% would raise these if a governance-controls spec needs them.

Landed as: NEW [def14a_director_attributes.csv](../../data/canonical/releases/2026-06-05_def14a/def14a_director_attributes.csv)
(92,632 rows, per filing-year, joins to status); `def14a_new_nominee_tenure.csv` gains
`gender` / `board_leadership_role` / `primary_company`; `person_year.dta` & `person_lifetime.dta`
gain `def14a_gender` + `def14a_primary_company`.

---

## 2. Release contents

New/changed (the rest carried forward as symlinks, unchanged from `2026-05-27`):

| file | rows | note |
|---|---|---|
| `def14a_director_status.csv` | 96,955 | + `age`, `age_source` |
| `def14a_new_nominee_tenure.csv` | 24,410 | NEW (Wave-4 union input) |
| `def14a_board_composition.csv` | 9,554 | NEW (gvkey × year) |
| `def14a_corpus_intersection.csv` | 48,316 | NEW (analyzable spine) |
| `def14a_director_attributes.csv` | 92,632 | NEW (gender/leadership/committees, per filing-year — §1e) |
| `firm_panel_annual.dta` | 19,858 | + 7 board-comp cols (7,572 firm-years populated) |
| `person_year.dta` | 143,850 | + 6 def14a cols + `def14a_gender`/`def14a_primary_company` |
| `person_lifetime.dta` | 26,511 | + 4 def14a cols + `def14a_gender`/`def14a_primary_company` |

`MANIFEST.json` + `CODEBOOK.md` document every new column, the age method, cost, and the diff.

---

## 3. Key / join failures (full disclosure)

- **(profile_url, ticker):** 6,957 of 24,410 new-nominee rows have **no `profile_url`** — these
  directors never had a LinkedIn URL discovered (not a join bug). They're non-corpus; Wave-4's
  union takes their tenure from def14a directly. The 30%-with-posts figure is a floor.
- **(gvkey, year):** **0** tickers failed ticker→gvkey mapping. **1,982** board-comp (gvkey,year)
  rows fall outside `firm_panel` — firm-years with no posts (expected, not errors).

---

## 4. OUT OF SCOPE — deferred to Wave 4 (untouched)

Per the spec and owner instruction, this chat did NOT do:
1. **8-K Item 5.07 election outcomes** — dropped (YoY diagnostic ⇒ ~0 genuine failures).
   All `new_nominee` treated as seated from year T.
2. **Tenure-gating of firm-year sentiment** — the merge is attribution-agnostic. The
   "before/after tenure-gated regression" the spec's verification mentioned was therefore
   NOT run (gating itself is Wave 4).
3. **WRDS ∪ def14a tenure union** — this chat produced the clean def14a side
   (`def14a_new_nominee_tenure.csv`); the union with the WRDS annual panels and the actual
   firm-period gating is the Wave-4 builder's job (`analysis_expansion_plan_20260530.md §4`).
   def14a is the sole tenure source for the ~30% of directors not in WRDS.

---

## 5. Verification done

- Age coverage + validation reported above; multi-board spot-check passes (status/tenure differ
  by firm). All release symlinks resolve. New release builds; `current` still `2026-05-27`.
- Provenance: deterministic 78.8% age cache backed up to
  `data/processed/def14a_ages_extracted.deterministic_backup.csv`.

---

## 6. Decisions needed from planning / owner

1. **Flip `current`?** Review the board-comp + person-enrichment columns; flip when satisfied.
   (Reversible — old release stays frozen.)
2. **Commission Wave 4?** The WRDS∪def14a tenure union + tenure-gating of firm-year sentiment
   is the next build; this release hands it the def14a side. Confirm scope/timing (and whether
   it waits on the Apify corpus expansion).
3. **Extend age to executives/blockholders?** Age here covers def14a *directors* only. If a
   retirement-hazard / age-control model needs exec age, the cheapest source is re-pulling
   Execucomp with its `AGE` field (executives only) — not in any current extract.
4. **New-nominee instrument N.** 811 new-nominees have URLs, far fewer have posts (scrape-limited).
   This is the pre-board-sentiment sample's current ceiling; it grows with the Apify expansion.
5. **`primary_company` → board AI/tech-orientation control?** We now have current employer for
   ~14k director×firm pairs. Classifying employers as tech/AI-intensive gives a board-level
   tech-exposure measure independent of the LinkedIn-sentiment instrument — a candidate control
   or cross-check. Decide if planning wants it built (needs an employer→industry mapping).
6. **LLM-lift committees/independence?** Currently bio-only (10–22%). Same ~$50 Haiku-on-filing
   pass that took age to 95% would lift them, IF a governance-controls spec needs them.
7. **Board skills/technology-expertise matrix** (NOT yet extracted) — the highest-research-value
   untapped def14a item: many proxies include a director skills grid marking "Technology"/
   "Cybersecurity"/sometimes "AI" expertise. Direct board-level AI-orientation signal. Worth a
   focused parse of the filings we already hold (decision: commission it?).

---

## 7. Reproduction

```bash
module load python/3.12 && source venv/bin/activate   # (Sherlock) / local venv
# age: deterministic pass is inside the build; LLM top-up is separate:
python3 src/data_extraction/backfill_age_llm.py            # $48, fills age cache
python3 src/data_extraction/build_def14a_merge_release.py --skip-age   # builds release
# attributes (gender / primary_company / leadership / committees) — $0, augments release in place:
python3 src/data_extraction/parse_def14a_bio_features.py
python3 src/data_extraction/augment_release_attributes.py
```
Scripts (committed on branch `def14a-merge-release-wave1`): [build_def14a_merge_release.py](../../src/data_extraction/build_def14a_merge_release.py),
[backfill_age_llm.py](../../src/data_extraction/backfill_age_llm.py),
[parse_def14a_bio_features.py](../../src/data_extraction/parse_def14a_bio_features.py),
[augment_release_attributes.py](../../src/data_extraction/augment_release_attributes.py).
The release `MANIFEST.json` + `CODEBOOK.md` are committed too; the large `.dta`/`.csv` data
files are gitignored-by-convention (canonical layer is local/scratch — see canonical README).

> **Note for planner:** `primary_company` was *already extracted at full scale* in an earlier
> wave (lives in `revelio_validation_rows_v2.csv`); the 2026-06-01 classification handoff's claim
> that the primary-employer parse "only ran on 10 filings" referred to a stale 128-row remnant
> file and is **superseded** — see §1e.

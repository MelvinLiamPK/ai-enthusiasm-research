# Spec — Age + director-attributes → `person_year` panel join

**Date:** 2026-06-12 · **From:** planning hub · **To:** execution chat (small build)
**Type:** canonical-release build (new release, no flip). Runs locally; no API, no re-scrape — everything is already on disk.

---

## 1. The gap (what this fixes)
`person_year.dta` (143,850 rows) carries the def14a person fields `def14a_director`, `def14a_new_nominee`, `def14a_tenure_start/end`, `pre_board_year`, `is_established_elsewhere`, `def14a_gender`, `def14a_primary_company` — but **no age** and **none of the board attributes** (`independent`, `n_committees`, `n_other_public_directorships`, `board_leadership_role`, `committee_chair`, `audit_financial_expert`). Those live only in side-files:
- **Age:** `current/def14a_director_status.csv` (= `def14a_director_status_age_20260605.csv`), cols `age`, `age_source`, keyed `(ticker, cik, year, full_name)`. Coverage 95.5% of def14a director-rows.
- **Attributes:** `current/def14a_director_attributes.csv` (= `def14a_director_attributes_20260606.csv`), keyed `(ticker, cik, year, full_name)`. Gender 91%, leadership ~23%, committees/independence bio-only ~10–22%.

Because they're side-files, any regression or descriptive wanting age/attributes as a **panel column** has to re-join by hand. This spec puts them on `person_year` (and `person_lifetime`) once, correctly.

## 2. Extend the existing bridge — do NOT invent a new join
The script that already put `def14a_gender` + `def14a_primary_company` onto `person_year` is
[`src/data_extraction/augment_release_attributes.py`](../../src/data_extraction/augment_release_attributes.py).
It builds a **normalized name→profile_url bridge** (`norm_name` + `clean_url`, via the `nn` new-nominee table which carries `full_name, ticker, profile_url`) and maps onto `person_year`'s `url_key = clean_url(profile_url)`. **Reuse that exact bridge** — same `norm_name`, same `clean_url`, same `(url_key, ticker)` keying. Do not write a fresh fuzzy matcher.

`build_def14a_merge_release.py` already **computes age** (`build_ages`, table-parse ∪ regex, with a year-delta QC) into `status_age`; it just never propagated it to the panel. So no age extraction is needed — only the join.

## 3. Design — age via `def14a_birth_year` (time-invariant), attributes year-specific

**Age is time-varying** (a director ages one year per year); gender/primary_company are time-invariant. Do **not** join raw `age` on `(name, ticker, year)` only — that leaves age NaN in every non-filing person-year. Instead:

1. From `def14a_director_status.csv`, take director rows with non-null `age`. Compute per row `birth_year = year − age`.
2. Attach `url_key` via the §2 bridge (`norm_name(full_name)` + `ticker` → `profile_url` → `clean_url`).
3. Collapse to one **`def14a_birth_year` per (url_key, ticker)** = `round(median(birth_year))` across that person-firm's filing rows (median absorbs the off-by-one QC noise flagged in `age_source`). Optionally keep `def14a_birth_year_n` (rows behind it) and propagate any `qc_year_delta_off` flag as `def14a_age_qc`.
4. Map `def14a_birth_year` onto `person_year` on `(url_key, ticker)` — **time-invariant, exactly like `def14a_gender`**. Consumers derive `age = year − def14a_birth_year`, valid in **every** person-year of that director (incl. pre-/post-filing years), not just filing years.
5. On `person_lifetime`, carry `def14a_birth_year` per `url_key` (median across firms).

**Attributes are filing-year board facts** → join **year-specifically** on `(url_key, ticker, year)` from `def14a_director_attributes.csv`, leaving NaN in non-filing years (do **not** forward-fill — "independent in 2021" ≠ "independent in 2024"). Carry: `def14a_independent`, `def14a_n_committees`, `def14a_committee_chair`, `def14a_audit_financial_expert`, `def14a_n_other_public_directorships`, `def14a_board_leadership_role`. Prefix all with `def14a_` for namespace consistency.

**Coverage caveat to document:** age is **director-only** (~95.5% of def14a director-rows; ~21% of the full person universe). Execs/blockholders get age **only** where they are also a def14a director at some firm (the firm-keyed join hands the ~2,421 exec-also-directors their age for free — same mechanism that gave them `def14a_gender`). Committees/independence are bio-only and low-coverage — descriptive-grade, not strong regressors. Age (95.5%) is the must-have; the attributes block is the nice-to-have.

## 4. Output — a NEW release (do not edit 2026-06-05 in place, do not flip)
The prior `augment_release_attributes.py` edited `2026-06-05_def14a` **in place** — a discipline departure. Don't repeat it: write a **new dated release** (e.g. `releases/2026-06-12_age_attrs/` or fold into the next planned release) that carries its **own** `person_year.dta` + `person_lifetime.dta` with the new columns and **symlink-inherits everything else** from `2026-06-07_crsp_monthly` (CRSP monthly + def14a + 2026-05-27 base). Write a full `MANIFEST.json` (sources, row counts, code git SHA, what-changed) and update `CODEBOOK.md` with the new columns + the birth_year-derivation note. **Do NOT flip `current`** — owner reviews and flips.

## 5. Verification
- `person_year` row count unchanged (143,850); new columns added, none dropped.
- `def14a_birth_year` non-null count ≈ the distinct (url_key, ticker) director count with age; spot-check 5 directors that `year − def14a_birth_year` matches their disclosed proxy age in the filing year.
- The ~2,421 exec-also-directors show non-null `def14a_birth_year` (proves the firm-keyed join carried age across roles, mirroring `def14a_gender`).
- Attributes non-null only in filing-years; coverage matches the side-file (gender 91% / leadership 23% / committees-independence 10–22%).

## 6. Out of scope
Executive age from Execucomp `AGE` (separate pull, not this chat). Tenure union / tenure-gating (Wave 4). Any re-score or panel-metric change — this is a **join-only** enrichment of existing panels.

## 7. Sequencing note
Chat 3 v1 (running now) reads age **ad-hoc from the side-file** for §6 descriptives (per its launch note) and does **not** wait on this. This release matters for **v2 / Wave-4** regressions that want `age` (or `def14a_birth_year`) as a panel regressor. Low urgency relative to Chat 6 and Chat 3; can run any time before the Wave-4 re-runs.

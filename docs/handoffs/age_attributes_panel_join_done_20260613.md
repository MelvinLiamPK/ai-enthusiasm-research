# Handoff — Age + board-attributes panel join DONE

**Date:** 2026-06-13 · **From:** execution chat (small build) · **To:** planning hub
**Re:** [`age_attributes_panel_join_spec_20260612.md`](age_attributes_panel_join_spec_20260612.md)
**Type:** completion report — new canonical release built, `current` NOT flipped.

---

## What was done
Executed the spec. Joined DEF 14A director **age** (as a birth year) and the **board-attribute
block** onto the person panels. Join-only — no API, no re-scrape; every input was on disk.

- **New release:** `data/canonical/releases/2026-06-12_age_attrs/`
- Writes its own `person_year.dta` + `person_lifetime.dta`; **symlink-inherits the other 16
  files unchanged** from `2026-06-07_crsp_monthly`. 20 entries total.
- Did **not** edit `2026-06-05` (or any prior release) in place. Did **not** flip `current`.
- Build script: `src/data_extraction/augment_age_attributes.py` (extends the existing
  name→URL bridge from `augment_release_attributes.py` — same `norm_name`/`clean_url`).
- Full `MANIFEST.json` + `CODEBOOK.md` written.

## Columns added
`person_year.dta` (+9): `def14a_birth_year`, `def14a_birth_year_n`, `def14a_age_qc`, and the
filing-year block `def14a_independent`, `def14a_n_committees`, `def14a_committee_chair`,
`def14a_audit_financial_expert`, `def14a_n_other_directorships`, `def14a_board_leadership_role`.
`person_lifetime.dta` (+3): the three birth-year columns only (attributes are filing-year facts,
no place on a year-less panel).

Consumers derive **`age = year − def14a_birth_year`** — valid in *every* person-year of a
director (incl. pre-/post-filing), not just filing years.

## One decision the planner should note (spec had an internal conflict)
The spec's §3 step 4 says map birth_year on `(url_key, ticker)`, but §5's verification requires
exec-also-directors to **inherit age across roles, mirroring `def14a_gender`** — which only holds
if birth_year is mapped by **`url_key` alone**. A person has one birth year regardless of board,
and `(url_key, ticker)` reached only 353 dual-role people vs gender-parity. **I mapped by
`url_key` (person-level median across firms).** The `(url_key, ticker)` collapse is an
intermediate step. This is the conceptually-correct reading and satisfies the §5 test. Documented
in MANIFEST/CODEBOOK.

## Verification (all pass)
- Row counts unchanged: `person_year` 143,850; `person_lifetime` 26,511. Every base column
  byte-identical; only new columns added.
- `def14a_birth_year` non-null on 24,176 person-years / 4,145 corpus profiles.
- Exec-also-directors inherit age: 2,747 `director|executive` rows non-null vs 2,698 for gender —
  tracks gender (slightly above, as expected from age's higher coverage).
- Spot-check `year − def14a_birth_year` vs disclosed proxy age: **94.3% exact, 98.7% within ±1yr**
  (median absorbs the `age_source` off-by-one QC noise).
- Attributes non-null **strictly in filing-years** (7,487 rows); 0 leak into non-filing years; not
  forward-filled.

## Two `.dta` quirks handled (for whoever reads the panel)
- 32-char Stata var limit → `n_other_public_directorships` shipped as `def14a_n_other_directorships`.
- Stata stores missing **strings** as `""` not NaN, so `def14a_board_leadership_role` reads non-null
  everywhere (same as the existing `def14a_gender`). Use a numeric attr like `def14a_committee_chair`
  as the clean filing-year indicator.

## State changes / what's needed
- `docs/PIPELINE_STATE.md` updated: **flip target is now `releases/2026-06-12_age_attrs`** (it wraps
  the crsp_monthly snapshot, so it subsumes the prior flip target). `current` still NOT flipped.
- **Owner action:** review the new release and flip `current` when ready (one move activates def14a
  enrichment + CRSP monthly + age/board-attributes).
- This release feeds **v2 / Wave-4 regressions** wanting `age`/`def14a_birth_year` as a panel
  regressor. Chat 3 v1 reads age ad-hoc from the side-file and did not depend on this.

## Still out of scope (unchanged)
Executive age (needs Execucomp `AGE` — separate pull); def14a age is directors-only. Tenure
union / tenure-gating is Wave 4.

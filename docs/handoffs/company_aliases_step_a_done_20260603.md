# Handoff — alias task step (a) is DONE

**Date:** 2026-06-03 · **From:** execution chat · **To:** the planning doc /
Wave-2 sequencing owner

This closes the **data-derived name-change list** — the prerequisite that had to
precede Wave-2 expansion URL discovery. Pointers below so the larger plan can
mark it complete and pick up the remaining (Wave-2) pieces.

## What was asked vs. what is done

The alias task ([meeting notes 2026-05-30](../meeting_notes/20260530_decisions_and_next_scrape.md),
[pre_meeting_plan_20260529 §4](pre_meeting_plan_20260529.md)) has three parts:

| ref | piece | status |
|---|---|---|
| **(a) / 5a** | data-derived name-change list (EDGAR + CRSP + Compustat → `company_aliases.csv`) | ✅ **DONE (this chat)** |
| 5b | LLM brand/subsidiary alias layer (ALPHABET → "Google", "YouTube") | ⬜ Wave 2 — not started |
| 5c | apply aliases → recompute Revelio `strong_match`; quantify movers | ⬜ Wave 2 — not started |

## Deliverables (step a)

- **Output:** [`data/revelio/company_aliases.csv`](../../data/revelio/company_aliases.csv)
  (long format: `gvkey, cik, permno, alias_name, alias_name_clean, source, valid_from, valid_to`)
- **Script:** [`src/revelio/build_company_aliases.py`](../../src/revelio/build_company_aliases.py)
  (WRDS pulls + EDGAR responses cached & resumable under `data/revelio/_alias_build_cache/`)
- **Methodology note:** [`docs/research_notes/company_aliases.md`](../research_notes/company_aliases.md)

## Headline numbers

- **18,130 rows**, **2,770/2,770 gvkeys covered (100%)**, **7,382 distinct
  (gvkey, alias_name_clean) pairs**, **2,052 firms (74%) with a real name
  change/variant**.
- Sources: crsp_stocknames 4,912 · compustat_conm 2,770 · compustat_conml 2,770
  · compustat_names_hist 2,770 · edgar_current 2,747 · edgar_former 2,161.
- Known false-negatives confirmed rescued: **Alphabet ← GOOGLE INC**,
  **Meta ← FACEBOOK INC**.

## Decisions baked in (so the plan doesn't re-litigate)

- **Lenient / non-time-gated matching** (owner 2026-06-02): `valid_from`/`valid_to`
  are audit-only; downstream must **not** gate on dates.
- Universe = the 2,770 gvkeys in `funda_20260425_135322.csv`.
- gvkey→permno via WRDS `crsp.ccmxpf_lnkhist`, gvkey→CIK via `comp.company`.
  (The local `crsp_annual_returns` file's permno column is an unusable `"ret"`
  placeholder — flagged for separate cleanup, did not block this.)

## Canonical discipline

`company_aliases.csv` sits in `data/revelio/`, **not** `data/canonical/`. It is
not yet joined into any panel, so **no new canonical release was cut and
`current` is unchanged.** The new release happens at **5c**, when aliases are
applied to recompute `strong_match` (new dated release + MANIFEST then).

## What the plan should do next (Wave 2)

1. **5b — LLM brand layer:** enumerate operating-brand/subsidiary names per
   gvkey (ALPHABET → Google/YouTube/Waymo …) and append rows with a
   `source = llm_brand` value into the same long schema. Spot-check ≥30
   high-impact firms; keep the model's justification for audit.
2. **5c — apply + measure:** union the data-derived + LLM layers, resolve a
   LinkedIn/Revelio-stated company against the alias set (lenient), recompute
   `strong_match`, and quantify how many people/firm-years move False→True and
   whether the Tobin's Q coefficient shifts. Then cut the new canonical release.
3. **Sequencing reminder:** this all sits *after* / alongside the planned Apify
   expansion, which will re-grow the posts corpus and force a re-score →
   re-aggregate. Don't double-run analysis before the expansion lands.

---

## End-to-end methodology (full walkthrough)

### 1. Where & how we got the name-change list

**Problem.** The Revelio crosscheck validates a LinkedIn profile on three tests:
URL match, name match, and **company match** (does the person's Revelio job
history contain the firm?). The company test compares *one* Compustat name (e.g.
`ALPHABET INC`) against what Revelio records (`Google`). When a firm's legal name
≠ the name on LinkedIn/Revelio, the test fails and the person is wrongly dropped
(`strong_match = False`) — this is the Pichai / Walker / Schindler problem.

**Fix.** Give each firm a *set* of acceptable names instead of one, harvested
from three authoritative online sources and keyed to `gvkey` (universe = the
2,770 firms in `funda`):

| Source | How pulled | Contributes |
|---|---|---|
| Compustat (`comp.company`, `comp.names`) | WRDS SQL | current legal name (`conm`/`conml`) + name history; the join key |
| CRSP (`crsp.stocknames`) | WRDS, gvkey→permno via CCM link | full historical name list w/ date ranges — catches `GOOGLE INC` (Alphabet's pre-2015 legal name) |
| SEC EDGAR (`formerNames`) | public `data.sec.gov/submissions/CIK*.json`, one call per CIK | legally-recorded former names w/ from/to dates — catches `Facebook Inc` under Meta |

Build script pulls all three, normalizes, dedupes, writes one file. WRDS pulls +
EDGAR responses cached for cheap re-runs. All 2,770 firms covered; 2,747 CIKs
resolved with zero errors. This is the **data-derived layer only** — it catches
*former legal names*, not pure brands that were never a legal name (YouTube,
Waymo); those are the Wave-2 LLM layer (5b).

### 2. How the dataset is structured

`data/revelio/company_aliases.csv` — **long format, one row per
(firm, name spelling, source)**, 18,130 rows:

```
gvkey,  cik,        permno, alias_name,    alias_name_clean, source,           valid_from, valid_to
160329,           ,       , GOOGLE INC,    GOOGLE INC,       crsp_stocknames,  2004-08-19, 2014-04-02
160329, ...,       ,         Alphabet Inc., ALPHABET INC,    edgar_current,    ,
170617,           ,       , Facebook Inc,  FACEBOOK INC,     edgar_former,     2005-05-06, 2021-10-27
```

- `gvkey` — join key back to every firm panel and to the Revelio validation rows.
- `alias_name` — raw name as the source spelled it; `alias_name_clean` —
  normalized for matching (uppercase, `&`→`AND`, punctuation stripped, whitespace
  collapsed; corporate suffixes **kept** to avoid over-merging distinct firms).
- `source` — provenance, so every alias is auditable.
- `valid_from`/`valid_to` — date range the name was in use; **kept for audit
  only, matching ignores them** (lenient / non-time-gated rule).

A firm's alias set = `aliases[aliases.gvkey == g].alias_name_clean.unique()`.

### 3. How this gets wired into the Revelio crosscheck (DESIGN — not yet built)

**Status:** the alias list (steps 1–2) is built. Applying it to the crosscheck is
Wave-2 work (5c). Mechanism:

Today the crosscheck sets `revelio_company_confirmed_*` by asking "does the single
Compustat name appear in this person's Revelio job history?" → false negative when
names diverge. The change swaps that single-name test for an **alias-set
membership test**, joined on `gvkey`:

1. For each validation row, look up its `gvkey`, pull the firm's
   `alias_name_clean` set from `company_aliases.csv`.
2. Normalize the Revelio-side company names the same way.
3. `revelio_company_confirmed = True` if Revelio's company matches **any** alias
   (lenient, date-ignored).
4. Recompute `strong_match = revelio_url_match AND revelio_name_confirmed AND
   revelio_company_confirmed`.

Because `revelio_company_confirmed_*` and `strong_match` already exist in
`revelio_validation_rows_v2.csv`, this is a **recomputation of existing fields,
not a schema change** — `firm_panel.dta`, `company_sentiment_annual`, and all
`_strong` regression variants pick it up once a new canonical release is cut.

**Validation gate:** Pichai / Walker / Schindler must flip `strong_match`
False→True (Alphabet's alias set now contains `GOOGLE INC`) without a flood of new
false positives; then quantify people/firm-years moved and any Tobin's Q shift.

**Caveats for the wiring:**
- Data-derived layer rescues firms whose brand *was once a legal name* (Google
  Inc, Facebook Inc). Pure never-legal brands need the LLM layer (5b) first.
- Recompute placement (on Redivis where Revelio job-history lives, vs. a
  downstream post-hoc fix-up) is still **open** — part of Wave 2.

### Aside — "true name-change rate" sanity check

Counting *distinct raw spellings* per firm gives 74%, but that is inflated by
cosmetic variants (`A T AND T` vs `AT AND T`; `CATO CORP` vs `CATO CORP CL A`).
Collapsing each name to a normalized "core" (un-space acronyms, fold
abbreviations like `SYS→SYSTEM`, strip suffix/share-class tokens) and clustering
near-duplicate cores per firm (union-find; merge on token-subset or Jaccard ≥0.6)
yields **~47% (1,309/2,770)** of firms with ≥2 genuinely distinct names —
validated at ~96% precision on a 25-firm random sample. Note this is "ever changed
name in CRSP/EDGAR history," **not** "changed within the 2010–2025 window" (CRSP
`stocknames` carries each security's full history). The 74% figure should be read
as *match-recall coverage*, not a name-change rate. (This analysis was a
diagnostic; no `is_namechange` flag is persisted yet.)

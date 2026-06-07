# Handoff — Monthly CRSP returns pull (DONE)

**Date:** 2026-06-07 · **From:** execution chat (WRDS, ml2068) · **To:** planning hub
**Spec:** [monthly_crsp_pull_spec_20260607.md](monthly_crsp_pull_spec_20260607.md)
**Status:** ✅ built and verified · `current` NOT flipped · awaiting planning sign-off

---

## What was asked vs done

| Spec task | Status |
|---|---|
| 1. Emit monthly panel, one row per `(gvkey,permno,month)`, calendar months | ✅ |
| 2. Fix `permno="ret"` bug in **both** monthly & annual paths; re-emit annual | ✅ |
| 3. Add `retx, prc, shrout`; emit `me = |prc|*shrout` ($000s) | ✅ |
| 4. De-dupe to one row per `(gvkey,month)`, keep `linkprim='P'` over `'C'`; report residual dupes | ✅ (0 residual) |
| 5. Output `data/extracted/crsp/crsp_monthly_returns_<ts>.csv` | ✅ |
| 6. Add as `crsp_monthly_returns.csv` to a NEW dated release + MANIFEST; do NOT flip `current` | ✅ |

---

## Code change

[`src/data_extraction/build_crsp_returns.py`](../../src/data_extraction/build_crsp_returns.py)

- Added `--frequency {annual,monthly}` (default `annual`, preserves prior behavior).
- New `attach_gvkey()` helper, shared by both paths: link-window join
  (`date BETWEEN linkdt AND linkenddt`), then collapse to one row per
  `(gvkey,month)` preferring `linkprim='P'`. Prints a `[dedupe]` line if any
  `(gvkey,month)` cell had >1 linked permno.
- `get_monthly_returns()` now selects `ret, retx, prc, shrout` (was `ret` only)
  and no longer drops `ret`-NaN at fetch time (annual path drops them itself;
  monthly keeps the month so price/`me` survive).
- **`permno="ret"` bug fixed.** The annual loop previously wrote `sub.name`
  (the ret-Series name = literal `"ret"`). It now records the real PERMNO
  (last month in the window). Monthly path carries permno straight from the join.
- `build_monthly_panel()` emits `gvkey, permno, date(month-end), year, month,
  ret, retx, prc, shrout, me`, with an in-code assert of one row per `(gvkey,month)`.

---

## Outputs

**Monthly** — `data/extracted/crsp/crsp_monthly_returns_20260607_150204.csv`
- Rows: **387,652** · gvkeys: **2,739** · permnos: **2,823**
- Date range: **2009-01 → 2024-12**
- `permno` real (int64; 0 == `"ret"`; all numeric) · **0 duplicate `(gvkey,month)`**
- `ret` nulls 0.4% (kept on purpose) · `me` non-null on 387,021 rows

**Annual (re-emitted, bug fixed)** — `data/extracted/crsp/crsp_annual_returns_20260607_150439.csv`
- Rows: **29,667** · gvkeys: **2,715** · fyear 2010–2025 · `permno` now real

---

## Canonical placement (un-flipped)

New release: **`data/canonical/releases/2026-06-07_crsp_monthly/`** — a **complete,
flippable snapshot** (re-cut 2026-06-07 per planning request; was initially a 2-file
folder, which `current` could not have pointed at).

- **Symlink-inherits the full `2026-06-05_def14a` fileset** (which itself inherits
  `2026-05-27`) — every sentiment / def14a / panel / funda / revelio file, plus
  `CODEBOOK.md`. def14a's symlinks are copied verbatim (sibling dirs, same relative
  targets); its in-place `.dta` panels are symlinked → `../2026-06-05_def14a/<name>`.
- **`crsp_annual_returns.csv`** → re-emitted file, **replacing the broken `2026-04-28`
  symlink** (`permno="ret"`).
- **`crsp_monthly_returns.csv`** → new stable name.
- **Final fileset = 19 data files = def14a's 18 + `crsp_monthly_returns.csv`.** All 19
  symlinks resolve (zero dangling); MANIFEST lists exactly the 19 on-disk files.
- `MANIFEST.json` — full fileset with sources/row counts/coverage, git SHA `874f1d5`,
  `base_release: 2026-06-05_def14a`, `inheritance` note → def14a manifest for upstream
  provenance.
- **`current` still → `releases/2026-05-27`** (unchanged).

Ready to flip directly, or fold into a later release (e.g. Chat 6).

---

## Two caveats for planning

1. **Table vintage = SIZ (legacy `crsp.msf`), still live.** The spec warned to
   switch to the CIZ monthly file (`crsp.stkmth`/`crsp_m_stock`, renamed columns)
   only if this WRDS instance had migrated and dropped `msf`. It hasn't — `crsp.msf`
   queried fine, so columns/schema match the existing annual file and no switch was
   needed. No action required; just recording which vintage the panel is built on.
2. **Data ends 2024-12.** Requested window was 2010–2025 (script pulls a leading
   year, hence 2009 start). `crsp.msf` on this instance has **no 2025 monthly rows
   yet** (CRSP update lag, not a format issue). 2025 will appear on a re-run once
   CRSP posts it; annual `fyear=2025` exists only via partial windows.

---

## Open question for planning

Approve flipping `current` to (or folding these files into) the next release, and
confirm Chat 3 wants both `ret` and `retx` plus `me` as shipped. Working-tree changes
to the script are **not yet committed** — say the word and I'll commit.

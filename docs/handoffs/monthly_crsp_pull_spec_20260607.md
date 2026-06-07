# Spec — Monthly CRSP returns pull (Wave 1 task)

**Date:** 2026-06-07 · **From:** planning hub · **For:** execution chat (WRDS access)
**Goal:** emit a **monthly** CRSP returns panel for the sample, to unblock Chat 3's
sub-annual stock-return specs (monthly/quarterly returns, size-weighting). Quick job —
the monthly data is already pulled; it's just being aggregated away today.

---

## Key insight: don't rewrite the pull, just stop compounding

[`src/data_extraction/build_crsp_returns.py`](../../src/data_extraction/build_crsp_returns.py)
already: (1) loads the gvkey universe from `all_people.csv`, (2) links gvkey↔permno via
`crsp.ccmxpf_lnkhist` (linktype LU/LC, linkprim P/C), (3) pulls **monthly** `ret` from
`crsp.msf` ([`get_monthly_returns`, L152](../../src/data_extraction/build_crsp_returns.py#L152)),
(4) attaches gvkey with a link-window filter ([L224-230](../../src/data_extraction/build_crsp_returns.py#L224-L230)),
then compounds to annual. **We just emit the monthly panel at step (4), before compounding.**

---

## Tasks

1. **Emit the monthly panel.** Add `--frequency monthly` (or a sibling emitter) that writes,
   after the link-window join, one row per **(gvkey, permno, month)**:
   `gvkey, permno, date (month-end), year, month, ret`. Calendar months — **do NOT** map to
   fiscal year here (Chat 3's panel builder does calendar→fiscal laddering downstream).
2. **FIX the `permno = "ret"` bug.** [L252](../../src/data_extraction/build_crsp_returns.py#L252)
   writes the Series name instead of permno (this is why
   `crsp_annual_returns_20260428.csv` has `permno = "ret"`). The monthly file **must carry
   the real permno + gvkey**; fix the annual path in the same edit and re-emit annual.
3. **Pull market-equity inputs (for Chat 3's size-weighting specs).** Extend the `crsp.msf`
   query to also select `retx, prc, shrout`; emit `me = abs(prc) * shrout` (market cap, in
   $000s). Cheap to add now, saves a second WRDS round-trip later.
4. **De-dupe.** A gvkey can map to multiple permnos over time; the link-window filter handles
   it, but assert **one row per (gvkey, month)** — if a month has >1 linked permno, keep
   `linkprim='P'` over `'C'`. Report any residual dupes.
5. **Output:** `data/extracted/crsp/crsp_monthly_returns_<ts>.csv`.
6. **Canonical:** add as stable name **`crsp_monthly_returns.csv`** to a NEW dated release
   (+ MANIFEST entry), or hand it to whoever cuts the next release (e.g. the Chat 6 release).
   **Do NOT flip `current`.**

---

## Notes / gotchas

- **Universe & window:** same gvkey universe (`all_people.csv`), 2010–2025; the script
  already pulls one extra leading year for alignment — keep that (useful for any lookback).
- **`ret` = total return** (incl. dividends) — the right default; `retx` is ex-dividend,
  emit both so Chat 3 can choose.
- **Table vintage:** `crsp.msf` is the legacy (SIZ) monthly file. If the WRDS instance has
  migrated to the CIZ format and `msf` is unavailable/deprecated, use the CIZ monthly
  equivalent (`crsp.stkmth` / `crsp_m_stock`) with the analogous `mthret`/`mthprc` fields —
  flag if you have to switch, since column names differ.
- **Quarterly** is not a separate pull — Chat 3 compounds 3 monthly returns. Monthly is the
  atomic extract.

## Environment
WRDS access required (`--wrds-username ml2068`, password from `~/.pgpass`; CRSP + CCM
entitlements). Runs locally or on Sherlock.

```bash
python3 src/data_extraction/build_crsp_returns.py --frequency monthly --wrds-username ml2068 --stats
```

## Deliverable back to planning
File path + row count, distinct gvkey/permno coverage, date range, confirmation that
**`permno` is real (not `"ret"`)** and one-row-per-(gvkey,month), and where it landed in the
canonical layer (un-flipped) so it joins the next release for Chat 3.

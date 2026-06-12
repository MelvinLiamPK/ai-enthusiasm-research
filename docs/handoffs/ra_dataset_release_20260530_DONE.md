# Handoff ← RA dataset release: DONE

**Date:** 2026-06-03
**By:** Execution chat (Chat A of [pre_meeting_plan_20260529.md](pre_meeting_plan_20260529.md))
**Spec executed:** [ra_dataset_release_20260530.md](ra_dataset_release_20260530.md)
**Status:** ✅ Complete — package built and verified, ready for Melvin to upload to the other RAs.

## One-liner

Applied the ratified §3 defaults to the do-files / `.Rmd`, documented them, and
repackaged `stata_handoff_20260527/` as a clearly-labeled **pre-Apify-expansion**
snapshot. No data file changed; canonical `current` untouched.

## Deliverable

- **Zip:** `outputs/stata_handoff_20260527_release_20260530.zip` (21 MB, 22 files)
- Internal folder keeps the `_20260527` data date; `20260530` is the release suffix.
- Stripped macOS/`.Rhistory` junk and the now-stale `README.html` (predated the
  edits; no pandoc to regenerate — RAs read `README.md` / knit the `.Rmd`).

## What changed (code/docs only — data files byte-untouched)

1. **§3.1 Strong-match default, min-post filters dropped.**
   `do/02_regressions.do` + `research_walkthrough.Rmd` now default to
   `keep if has_strong_match == 1`. The `meets_min_*` filters survive as
   commented optional toggles.
2. **§3.2 Regression-time zero-imputation of AI sentiment.** Built `ai_sent_new`
   / `ai_sent_new_strong` inside the do-file / `.Rmd` only. **NOT** baked into
   any `.dta` — shipped data keeps true NaN (13,713 NaN rows confirmed; no
   `ai_sent_new` column in the panel).
3. Added a year+firm FE spec on `ai_sent_new_strong` (John's `jvrmel_06`).
4. **README:** added "Snapshot scope" banner + "Decisions (2026-05-30)" section
   (with exact rule, Stata + R snippets, refreshed headline table); reconciled
   the codebook/regression tables with the new defaults.

## Imputation rule (must stay byte-identical to Chat 2)

> **Column `ai_sent_new`** (mirror `ai_sent_new_strong`): `= 0` where the
> firm-year **posted but had no AI posts** (`n_posts >= 1 & n_ai_posts == 0`);
> **NaN** where it **did not post** (`n_posts == 0`); otherwise
> `= ai_mom_net_sentiment[_strong]`. Equivalent to John's
> `replace ai_sent_new = 0 if ai_post_share_strong == 0`.

The panel has zero `n_posts == 0` rows, so the NaN branch is naturally absent;
13,713 firm-years (12,532 strong) receive the imputed 0.

## Refreshed headline (R `fixest`; Stata not on the build machine)

| Regressor | Spec | β | SE | N |
|---|---|---|---|---|
| `ai_sent_new_strong` (imputed) | Year + Firm FE | 0.00058 | 0.00022 | 16,501 |
| `ai_post_share_strong` | Year FE | 1.561 | 0.219 | 16,623 |

In John's `jvrmel_06` neighbourhood (β≈0.0007, N≈16,250; share≈1.0); small β gap
is winsorization/control-handling, direction + magnitude + N match.

## Canonical discipline

- Shipped `firm_panel.dta` is the **same physical file** `data/canonical/current/`
  (release `2026-05-27`) points at — verified via `readlink -f`. Not a loose copy.
- **No new release cut, `current` not flipped** — only do-files/Rmd/README changed
  (not part of the release's symlinked data set); `2026-05-27` stays valid.

## Open / next

- Code edits are in the working tree, **uncommitted** (no commit requested).
- `README.html` not regenerated (no pandoc); knit `research_walkthrough.Rmd` or
  pandoc `README.md` if an HTML copy is wanted.
- Refresh expected post-Apify-expansion: re-score → re-aggregate → new canonical
  release → rebuild this package.

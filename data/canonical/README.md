# `data/canonical/` — the source of truth

Every regression and summary statistic should read its inputs from
**`data/canonical/current/`** — never from a dated file under `outputs/` or
`data/processed/` directly. This directory is the single declared source of
truth.

## Plain-English glossary

- **Release** — a dated, frozen folder (e.g. `releases/2026-05-27/`) holding one
  complete, self-consistent snapshot of the analysis data. Think of it like a
  numbered edition of a book: once published it never changes; a correction means
  a *new* edition, not edits to the old one.
- **Manifest** (`MANIFEST.json`) — the snapshot's packing slip: it lists every
  file in the release, where it came from, how many rows it has, and which code
  version built it. It exists so anyone can see exactly what a release contains
  and reproduce it.
- **`current`** — a shortcut (symlink) that always points at the release you're
  currently using. Scripts read `current/…`; when you publish a new release you
  just re-point this shortcut, and every script picks up the new data with no
  code change.

## The rule

- **The date lives in the folder name, not the filename.** Inside a release,
  files have stable names (`company_sentiment_annual.csv`, `firm_panel_annual.dta`, …).
  `current/company_sentiment_annual.csv` always means "the canonical one."
- **`current` is a symlink** to the active release under `releases/`. All
  analysis code (Stata, R, Python) reads through `current`.
- **Releases are immutable.** You never edit a release in place. A new scrape or
  methodology change → build a **new** dated release and flip `current`. Old
  releases stay frozen so past results remain reproducible.
- **Entries are symlinks** to immutable dated source files (no duplication —
  the GB-scale files are not copied). Do not delete/overwrite the targets.

```
data/canonical/
  releases/
    2026-05-27/            # blessed snapshot (corpus frozen 2026-05-27)
      MANIFEST.json        # provenance: source path, rows, git SHA, build date
      CODEBOOK.md
      <stable-named files / symlinks>
  current -> releases/2026-05-27
```

## How to cut a new release

1. `mkdir data/canonical/releases/<YYYY-MM-DD[_tag]>/`
2. Symlink (or write) the new files with the **same stable names**.
3. Write a `MANIFEST.json` (copy the previous one, update sources/rows/git SHA/
   what-changed) and update `CODEBOOK.md` if columns changed.
4. Flip the pointer: `ln -sfn releases/<new> data/canonical/current`.
5. Analysis code is unchanged — it reads `current/` and picks up the new data.

A `src/build_canonical_release.py` entrypoint that automates steps 1–4 by
composing the existing build scripts is the recommended next step (not built
yet — this release was blessed by hand).

## Known next releases (deferred — see docs/handoffs/ and meeting_notes/)

- **`2026-05-31_def14a`** — join the def14a director status + tenure into the
  person/firm panels (person enrichment, firm board-composition features,
  tenure-gated sentiment attribution). The status/tenure CSVs are present in the
  current release as inputs but are **not yet joined**.
- **Post-Apify-expansion** — the planned LinkedIn expansion will grow the posts
  corpus, requiring a full re-score → re-aggregate → new release.

## Sherlock note

The two `posts_*` files are GB-scale. On Sherlock, put `data/canonical/` on
`$SCRATCH` (as `outputs/` already is) so it doesn't hit the home quota; the
relative symlinks resolve as long as the whole tree moves together.

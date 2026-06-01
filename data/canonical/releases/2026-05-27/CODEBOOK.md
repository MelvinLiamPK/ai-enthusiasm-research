# Codebook — canonical release 2026-05-27

Concise reference. Full panel column docs live in
`outputs/stata_handoff_20260527/README.md`; this file covers what each canonical
file is, its grain, and its join keys.

## Primary analysis inputs (CSV)

| File | Grain | Key columns / join keys |
|---|---|---|
| `company_sentiment_annual.csv` | firm × year | `gvkey`, `year`; `(ai_)mean_of_mean_net_sentiment`, `ai_post_share`, `*_strong`, `n_posts`, `n_ai_posts`. Python regressions read this. |
| `company_sentiment_quarterly.csv` | firm × quarter | `gvkey`, `quarter` (e.g. `2022Q3`); same metric set. |
| `posts_scored_unique.csv` | unique post | `(profile_url, post_url)`; LM scores (`lm_net_sentiment`, `lm_polarity`, …), `is_ai_related`, `is_covid_related`. One row per post — person-level aggregation source. |
| `posts_full_coverage.csv` | post × person × company | `(profile_url, post_url, ticker)`; same scores + `gvkey`/`ticker`/`source`/`position`. 100% LM-scored. FinBERT cols may be NaN (41% coverage). |
| `revelio_validation_summary.csv` | profile × board | `linkedin_url`; `strong_match_either`, `strong_match_either_fuzzy`, `source`. Strong-match validation. |
| `funda_annual.csv` | firm × fiscal year | `gvkey`, `fyear`; Compustat fundamentals (`at`, `ni`, `sale`, `xrd`, leverage inputs). |
| `crsp_annual_returns.csv` | firm × year | `gvkey`/`permno`, `year`; annual stock return. |

## DEF 14A director metadata (CSV) — present as INPUTS, not yet joined

| File | Grain | Key columns |
|---|---|---|
| `def14a_director_status.csv` | director × filing | `(cik, year, full_name)`; `def14a_director_status` ∈ {incumbent, new_nominee, mid_year_appointee, not_director, unknown}, `served_on_this_board_before`, `director_since_year`, `is_nominee_this_cycle`, `def14a_bio_text`. **No `age` column** in this run (backfill from `def14a_bio_text`). To attach to people, bridge `full_name`→`profile_url` via `data/processed/def14a_urls_for_revelio_validation.csv` (resolves name variants); do not re-fuzzy-match. |
| `def14a_director_tenure.csv` | director × firm × year | `(profile_url or full_name, ticker, year)`; tenure window (`tenure_start`/`tenure_end`, `is_serving`). Use to gate sentiment attribution to in-tenure firm-years. |

## Built panels (.dta)

`firm_panel_annual.dta`, `firm_panel_quarterly.dta`, `person_year.dta`,
`person_lifetime.dta` — regression-ready, built by `build_stata_handoff.py`.
Full column docs in `outputs/stata_handoff_20260527/README.md`.

**Note:** these panels keep **true missingness**. AI sentiment is NaN for
firm-years that posted but had no AI posts; the 0-imputation
(`ai_sent_new`-style) is applied **at regression time**, not stored here.

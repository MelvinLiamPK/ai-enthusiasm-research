# Meeting notes & decisions — 2026-05-30

Running log of decisions and planned next steps. Permanent home for things that
would otherwise get lost in chat history.

## Decisions made today

1. **Zero-imputation lives at regression time, not in the data.** AI sentiment
   is imputed to 0 for firm-years that posted but had no AI posts
   (`n_posts >= 1 & n_ai_posts == 0`; mirror for `_strong`; NaN left where
   `n_posts == 0`). This happens **in the regression scripts**
   (`02_regressions.do`, `.Rmd`, and the internal `sentiment_q_regression.py`)
   on a derived `ai_sent_new`-style variable. **All data files
   (`firm_panel.dta`, `company_sentiment_annual_*.csv`) keep true missingness
   (NaN).** Matches what John already does in his `.do`. Captured in
   [pre_meeting_plan_20260529.md](../handoffs/pre_meeting_plan_20260529.md) §3.2
   and [analysis_expansion_plan_20260530.md](../handoffs/analysis_expansion_plan_20260530.md) §3.2.

2. **Director age** added to the DEF 14A classifier schema/prompt
   (`classify_def14a_director_status.py`) for future runs. NOT added to
   `OUTPUT_FIELDS` yet — doing so mid-run would corrupt the CSV on the next
   relaunch. Age for the current run will be backfilled from the saved
   `def14a_bio_text` in a post-hoc pass.

3. **Headline result (John, `jvrmel_06`):** with regression-time zero-imputation,
   AI sentiment ~ Tobin's Q is significant **even with firm FE** (β≈0.0007,
   p≈0.001, N≈16,250). AI enthusiasm is stronger for large firms and
   R&D-intensive firms.

## Planned next scrape — Apify LinkedIn expansion (NOT yet run)

**Intent:** feed the newly DEF-14A-classified directors' **primary employers**
(extracted from proxy bios) into the Serper → Apify LinkedIn pipeline to
discover and scrape **new** profiles/posts — the same re-anchoring strategy that
produced the original `def14a_serper` cohort, now at full scale.

**Implication for sequencing (important):** this will **grow the posts corpus**.
Everything downstream of the corpus — sentiment scoring, the full-coverage file,
`company_sentiment_annual`, `firm_panel`, all regressions and descriptives —
would need to be **re-run** afterward. So:

- The RA dataset release being built now (see
  [ra_dataset_release_20260530.md](../handoffs/ra_dataset_release_20260530.md))
  is a **snapshot: corpus frozen 2026-05-27, pre-Apify-expansion.** Label it as
  such so RAs know a refresh is coming.
- The analysis chats (external Stata defaults/aliases; internal
  parity/multi-frequency panel/specs/descriptives) are **deferred** until after
  the expansion, to avoid running them twice. Re-sequence once the Apify scrape
  + re-scoring complete.
- The DEF 14A classifier run currently in progress (PID 13315, ETA ~7–8h) is
  about director *metadata* and is independent of the posts corpus — it does not
  need to wait on the Apify expansion.

**Open question to resolve before launching the expansion:** how large is the
expansion (how many new primary-employer-anchored searches), and what's the
Apify budget? Determines whether a single re-scoring pass is cheap or needs
batching.

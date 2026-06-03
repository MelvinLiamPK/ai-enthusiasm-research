# Meeting notes & decisions — 2026-05-30

Running log of decisions and planned next steps. Permanent home for things that
would otherwise get lost in chat history.

## TASK (added 2026-06-02) — S&P 1500 name / ownership-change list

Build a per-firm **alias set** for S&P 1500 firms (name + ownership-structure
changes over 2010–2025) to fix the "Sundar Pichai work-history problem"
(Compustat says ALPHABET INC, LinkedIn says Google → strong-match fails).

**Authoritative online sources (don't hand-compile):**
1. **SEC EDGAR `formerNames`** — the company-submissions JSON
   (`data.sec.gov/submissions/CIK##########.json`) lists former names with dates,
   per CIK. Free, authoritative for legal name changes.
2. **CRSP `stocknames` / `dsenames`** — full name + ticker history with date
   ranges per PERMNO (gold standard; catches Google Inc → Alphabet Inc, ticker
   changes, etc.).
3. **Compustat** `conm`/`conml` + names history; `dlrsn` for merger/deletion events.
4. **LLM brand-alias layer** (existing Section 4 / Chat 2) — brand/subsidiary
   names that aren't legal-name changes (ALPHABET → Google, YouTube …).

Per firm: alias_set = {current legal name} ∪ {all former names (EDGAR/CRSP)} ∪
{brand/subsidiary names (LLM)}.

**Matching philosophy: COMPREHENSIVE but LENIENT (high recall) — owner's call
2026-06-02.** A LinkedIn-stated company matches the firm if it hits **any** alias,
**regardless of date** (NOT time-gated). Example: Sundar still lists "Google" →
match to Alphabet even though the legal name changed in 2015. This resolves the
prior Section 4 open question "should aliases be time-aware?" → **no**, prioritize
avoiding false negatives (dropping real matches like Pichai) over false positives.
Merge all sources into one `company_aliases.csv`.

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

**Update 2026-06-02 — expansion grows the instrument's power.**
The def14a never-elected placebo is empty, so the surviving identification angle
is new-nominees' pre-board sentiment (exogenous to the *joined* firm). Of 1,332
new-nominees only 345 (26%) currently have posts — the rest are just not-yet-scraped
— so the Apify expansion raises N/power for this instrument. (Note: "already an
incumbent elsewhere," true of most of the 345, is NOT a contamination — exogeneity
is firm-specific, and those people conveniently have a pre-join posting history.
The real threat is endogenous director-firm matching, handled by timing + controls.)
Tenure-gated attribution correctness is available now on existing posts regardless.

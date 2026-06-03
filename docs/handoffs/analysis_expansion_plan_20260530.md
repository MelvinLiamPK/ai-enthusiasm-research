# Analysis-expansion planning brief

**Date:** 2026-05-30
**Owner:** Melvin Liam
**Audience:** the planning agent that will turn this into a concrete workplan

This is a handoff brief — not the workplan itself. It is the **second**
planning brief in this sprint. The first
([`pre_meeting_plan_20260529.md`](pre_meeting_plan_20260529.md)) covered the
**external** Stata/R handoff defaults (Section 3) and the Revelio alias fix
(Section 4); its three-chat workplan is still valid and should run as-is.

This brief covers two things that brief did **not**:

1. **Python-pipeline parity (Section 3 here).** The defaults John validated
   live only in the external Stata/R files and in his ad-hoc `.do`. Our
   *internal* Python analysis path never received them and has silently
   diverged. The repo, not the Stata handoff, is our source of truth — it
   needs to be brought into parity first, because everything below builds on
   it.
2. **A new analysis agenda (Sections 4–6 here).** Multi-frequency panel,
   new regression specs, and new descriptive cuts that came out of the
   2026-05-30 regression session (see John's `Notes from Melvin work.docx`).

The planning agent should read this end-to-end and produce a concrete
implementation plan for Sections **3–6**. Sections 1–2 are context only.

---

## 1. Where things stand today

**The headline finding from the 2026-05-30 session (John's `jvrmel_06.do/log`):**
once you impute 0 sentiment for firm-years that posted but had no AI posts
(call it `ai_sent_new`), AI sentiment and Tobin's Q are significantly linked
**even with firm fixed effects**:

| Spec (LHS `ln_tobins_q`) | β | SE | p | N |
|---|---:|---:|---:|---:|
| `ai_sent_new`, year FE | 0.0034 | 0.0005 | 0.000 | 16,385 |
| `ai_sent_new`, year + firm FE | 0.0007 | 0.0002 | 0.001 | 16,250 |
| `ai_post_share_strong`, year FE | 1.005 | 0.172 | 0.000 | 16,385 |
| `ai_post_share_strong`, year + firm FE | 0.162 | 0.076 | 0.033 | 16,250 |

This is a materially stronger result than the strong-match-filtered numbers in
the prior brief — the zero-imputation keeps the firm-years that post but don't
talk about AI, which were being silently dropped.

**The parity problem.** John's changes (zero-imputation, dropped filters, the
`ai_sent_new` index) were made in `outputs/stata_handoff_20260527/` and in his
local `.do` — i.e. the **external** deliverable for John & Nick. Our internal
Python regression path is now out of sync:

| Logic | `build_stata_handoff.py` (external bridge) | `sentiment_q_regression.py` (internal) |
|---|---|---|
| Mean-of-mean headline | ✅ `mom_net_sentiment`, `ai_mom_net_sentiment` | ✅ reads `(ai_)mean_of_mean_net_sentiment` |
| Strong-match as **default** | ✅ `has_strong_match` flag baked | ❌ opt-in via `--strong-match-only` |
| Drop `n_posts≥10` / `n_ai_posts≥3` | ✅ no default filter | ❌ defaults still `--min-posts 10`, `--min-ai-posts 3` |
| Zero-impute AI sentiment | ❌ not yet (John did it in `.do`) | ❌ not at all |
| t / t+1 outcomes | ✅ | ✅ |

So neither Python file fully reflects the ratified defaults, and the internal
regression script (`sentiment_q_regression.py`, lines 619–621, 686–714) is the
furthest behind.

---

## 2. What's already done or planned elsewhere — do not redo

Already shipped (per prior brief Section 2): mean-of-mean swap, t/t+1
outcomes, per-profile `strong_match`, pre-baked outlier + `dropped_by_revelio`
CSVs, documented Revelio false-negatives.

Owned by other chats — **do not absorb into this plan:**
- **External Stata/R defaults (§3.1/3.2/3.3) + Revelio aliases (§4)** →
  the three-chat workplan from [`pre_meeting_plan_20260529.md`](pre_meeting_plan_20260529.md).
  This brief's Section 3 is the *internal-Python mirror* of that work, not a
  duplicate — see the boundary note in Section 3.
- **DEF 14A incumbent/nominee classification + tenure panel + director age** →
  [`def14a_director_status_classification_20260528.md`](def14a_director_status_classification_20260528.md).
  Director **age** extraction folds in there (proxies disclose age under
  Reg S-K Item 401); this plan only *consumes* age once produced.
- **Revelio "don't screen out people with no work history" refinement** →
  fold into Chat 2 of the prior workplan (it's a refinement of the same
  `strong_match` logic), not here.
- FinBERT GPU re-run, Tesla `author_name` mismatch, Elon's LinkedIn absence,
  any new data collection — out of scope everywhere (prior brief Section 6).

---

## 3. Python-pipeline parity — bring the internal path up to the ratified defaults

**Goal:** the internal Python analysis path should reproduce John's headline
numbers without him having to hand-edit a `.do`. This is the prerequisite for
Sections 4–6, which extend that same path.

**Boundary with the prior workplan:** Chat 1 of the prior workplan edits the
*external* artifacts (`02_regressions.do`, `research_walkthrough.Rmd`). **This
section edits the *internal* analysis script** ([`sentiment_q_regression.py`](../../src/data_analysis/sentiment_q_regression.py)).
The zero-imputation lives **in the regression script**, applied at point-of-use
— the on-disk data files (`company_sentiment_annual_*.csv`, the firm panels)
**must keep true missingness** (NaN where a firm-year posted but had no AI
posts). Do **not** bake the imputation into `aggregate_sentiment.py` or the
saved panels.

### 3.1 Drop the default min-post filters; make strong-match the default
- In [`sentiment_q_regression.py`](../../src/data_analysis/sentiment_q_regression.py):
  change `--min-posts` default from `10` to `0` (or `1`) and `--min-ai-posts`
  default from `3` to `0`; keep the flags so old numbers are reproducible.
- Make the strong-match sample the **default**, not opt-in: either flip
  `--strong-match-only` to default-true with an `--all-matches` escape hatch,
  or key the default sample on `has_strong_match == 1` to mirror
  `build_stata_handoff.py`.
- **Verification:** rerunning the four outcomes with no flags should now
  produce John's N (~16k for the imputed Tobin's Q spec), not the prior ~3k.

### 3.2 Impute 0 AI sentiment in firm-years with posts but no AI posts
- **The imputation happens inside `sentiment_q_regression.py`, on the
  in-memory regression sample only.** The data files on disk keep true NaN.
- Mirror John's `ai_sent_new`: after loading the panel, on a working copy,
  where the firm-year has `n_posts >= 1` and `n_ai_posts == 0`, set
  `ai_mom_net_sentiment` (and the `_strong` variant where `n_posts_strong >= 1`
  and `n_ai_posts_strong == 0`) to **0**. Leave NaN where the firm didn't post
  at all (`n_posts == 0`) — those rows still drop out, correctly.
- Build the imputed regressor as a derived column (e.g. `ai_sent_new`) rather
  than overwriting the source column, and add an `ai_sentiment_imputed`
  boolean on the regression frame so imputed cells are inspectable/droppable
  within the run.
- Gate it behind a flag (e.g. `--impute-zero-ai-sentiment`, default **on** to
  match the ratified behavior, with the off-switch for reproducing pre-
  imputation numbers).
- **Verification:** the imputed-sample coefficient on the imputed regressor
  with year+firm FE should match John's β≈0.0007, p≈0.001 (within rounding /
  winsorization differences). Confirm the on-disk `company_sentiment_annual_*`
  still shows NaN (not 0) for the no-AI-post firm-years after the run.
  Document the rule in the codebook.

### 3.3 Strong-match stays the default
No-op for completeness — consistent with §3.1's strong-match default and with
the prior brief. The Revelio alias fix (prior brief §4) will later expand which
profiles qualify; this section should not pre-empt it.

**Section 3 verification (overall):** a single no-flag invocation of the
internal regression path reproduces the four headline specs from John's
`jvrmel_06.log` within rounding. Capture the output under
`outputs/sanity_checks/regression_*_parity_<timestamp>/` and diff against the
log.

---

## 4. Multi-frequency firm panel — the dependency spine for Sections 5–6

Everything in Sections 5–6 needs a firm panel at **monthly and quarterly**
grain, not just annual. The current `firm_panel.dta` / `company_sentiment_annual`
is annual only (quarterly *sentiment* exists via `aggregate_sentiment.py` but
there is no quarterly/monthly *firm panel* joined to outcomes).

The planner should design a panel builder that:
- Produces firm × period panels at **annual, quarterly, and monthly** grain
  from `sentiment_all_posts_full_coverage_20260527.csv` (post timestamps) joined
  to the right-frequency outcome series.
- **Tenure-gated attribution (corpus-wide correctness fix).** Build a canonical
  `tenure_panel.csv` keyed `(profile_url, ticker)` → `[tenure_start, tenure_end,
  tenure_source]` by **unioning two sources**:
  1. **WRDS** annual panels (`data/extracted/{directors,executives,blockholders}/*_all.csv`,
     person × firm × year, 2010–2025): map name+company → `profile_url` via
     `all_linkedin_urls`; collapse to (profile_url, ticker) → [min_year, max_year].
  2. **def14a** `def14a_director_tenure.csv`: already (profile_url, ticker, year)
     with `director_since` backfill.
  Merge per (profile_url, ticker): `tenure_start = min(WRDS_min, def14a_start)`,
  `tenure_end = max(WRDS_max, def14a_end)`, `tenure_source ∈
  {wrds_only, def14a_only, both}`. **def14a is the SOLE tenure source for the ~5,647
  directors (30%) not in WRDS** — not a refinement; without it their posts can't be
  gated. Then count a post `(profile_url, ticker, year)` toward a (firm, period)
  **only if the year ∈ the merged tenure window**. Covers all roles/all ~26k
  profiles. (The combined `all_people.csv` dropped `year` in dedup — that's why
  this wasn't happening; rebuild from the `*_all.csv` files + def14a.)
- Aligns **calendar** (post timestamps) to **fiscal** (Compustat) periods —
  define and document the mapping rule (which fiscal period a post falls in
  given each firm's fiscal-year-end; don't assume December).
- Implements the **missing-data frequency-laddering** rule from the notes:
  *"for missing, go up in frequency, label as missing, then go down in
  frequency."* Concretely: aggregate up to the coarsest frequency, mark cells
  with no posts as explicitly missing, then distribute/forward down to finer
  frequencies with the missing label preserved. The panel stores **true
  missingness** (NaN) throughout — it never zero-imputes. Zero-imputation is a
  separate, regression-time transform (§3.2) applied to an in-memory copy, so
  the two rules **cannot collide**: the panel is the honest source, and any
  imputation is a downstream choice made at the point of regression. The
  planner should still state the laddering rule precisely, but the §3.2
  interaction is resolved by construction.
- Carries the same metric set as the annual panel (`mom_net_sentiment`,
  `ai_post_share`, `_strong` variants) plus the raw `n_posts` / `n_ai_posts`
  counts the regression script needs to derive the §3.2 imputation. The panel
  does **not** store `ai_sentiment_imputed` — that flag is computed on the
  regression frame at run time.

**Inputs available:** `sentiment_all_posts_full_coverage_20260527.csv` (2.92M
rows, 100% LM-scored, has post dates), `funda_*` (annual fundamentals),
`crsp_annual_returns_*` (annual). **Note:** monthly/quarterly stock returns
(§5.1) need a **monthly CRSP returns pull** — the current CRSP file is annual
only. Flag whether that pull is in scope or a blocker.

**Verification:** annual panel rebuilt by the new multi-frequency builder
matches the existing annual `firm_panel` numbers (regression-test against §3
parity output). Quarterly/monthly panels have sane row counts and the
frequency-laddering missing/zero labels are internally consistent.

---

## 5. New regression specs (build on the Section 4 panel)

Each is a distinct spec the planner should scope as a discrete deliverable:

### 5.1 Monthly + quarterly regressions, esp. stock returns on general sentiment
- General (non-AI) `mom_net_sentiment` → stock returns, at monthly and
  quarterly frequency. Needs the monthly CRSP pull (§4 note).

### 5.2 Five-year long-difference instead of year-to-year FE
- Δ₅ outcome on Δ₅ sentiment (long-difference estimator) as an alternative to
  the within-firm FE that currently absorbs most of the signal.
- **Outlier handling on the 5-year difference** is called out specifically in
  the notes — long-differences amplify outliers, so the planner should specify
  winsorization / trimming on the differenced variables (not just the levels).

### 5.3 Size-weighted regressions
- Weight regressions by company size (assets or market cap) so the estimates
  reflect economically larger firms. Specify the weight variable and whether
  it's contemporaneous or lagged.

### 5.4 AI keyword time series
- Time series of post frequency on `"AI"` / `"artificial intelligence"`
  (keyword set already finalized — the "llm" item in the notes is already
  handled and is **not** a new ask). Plot volume over time; this is descriptive
  context for the panel.

### 5.5 LOESS / binscatter visualizations
- `binscatter` is already done (John ran size and R&D-intensity binscatters).
  **Add LOESS** smoothing for the key sentiment-outcome relationships as a
  flexible-form check on the linear specs.

**Verification:** each spec runs end-to-end on the §4 panel and produces a
saved figure/table under `outputs/`. Sign and rough magnitude of headline
relationships are consistent with the annual results (or, where they differ,
the planner flags *why* — e.g. long-difference vs FE).

---

## 6. New descriptive cuts — AI sentiment by firm characteristics

Descriptive stats of AI sentiment, broken out by:
- **Industry** (SIC/NAICS from Compustat — already available).
- **Number of employees** (Compustat `emp` — already available).
- **R&D intensity** — *already done* by John (`binscatter av_senti lrnd_int`,
  with `log(rnd_int)` and the "lots of zeroes" handled). Include for
  completeness; don't redo unless extending.
- **Age and gender** — age from the DEF 14A bios (backfill from `def14a_bio_text`;
  the "over age 30?" note reads as a sanity-check threshold). **Gender for
  executives is already in `data/extracted/executives/executives_all.csv`
  (`gender` column)** — use it directly; only fall back to name-inference for
  directors/blockholders, flagging accuracy limits.

**Verification:** a descriptives table/notebook that reproduces John's
"large firms and R&D-intensive firms are more AI-positive" finding and extends
it to industry / employees / age / gender, saved under `outputs/`.

---

## 7. What the planner is being asked for

- **Section 3:** concrete edits to bring `sentiment_q_regression.py` into
  parity with the ratified defaults, plus the verification that reproduces
  John's `jvrmel_06` numbers. The zero-imputation lives in the regression
  script (point-of-use); the on-disk data files keep true NaN.
- **Section 4:** a design for the multi-frequency panel builder — grain,
  calendar↔fiscal mapping, and the exact missing-data frequency-laddering rule.
  The panel stores true missingness only; it does not zero-impute (that's §3.2,
  downstream). This section still needs real design thought on grain and the
  calendar↔fiscal mapping.
- **Sections 5–6:** scope each spec/descriptive as a discrete deliverable with
  named inputs and a saved output, with explicit flags for the two known
  blockers (monthly CRSP pull; age/gender data dependencies).
- **An ordering:** Section 3 first (everything depends on it), then Section 4
  (the panel spine), then 5–6 in parallel. Identify what can fan out.

Section 3 is mechanical but load-bearing. Section 4 is the one that needs
careful design. Sections 5–6 are mostly straightforward once the panel exists.

---

## 8. Non-asks (out of scope here)

- External Stata/R default edits + Revelio alias discovery — owned by the
  prior brief's three-chat workplan.
- DEF 14A age/gender-from-age extraction and the incumbent/nominee tenure
  panel — owned by the director-status chat.
- Revelio "no work history" screening refinement — fold into prior Chat 2.
- Monthly CRSP data *collection* if it turns out to require a new WRDS pull —
  flag it as a blocker for §5.1 rather than scoping the pull here.
- FinBERT coverage, Tesla name mismatch, Elon's absence — parked, as before.

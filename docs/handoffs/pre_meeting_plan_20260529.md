# Pre-meeting planning brief

**Date:** 2026-05-29
**Owner:** Melvin Liam
**Audience:** the planning agent that will turn this into a concrete workplan

This is a handoff brief — not the workplan itself. It captures (a) where the
dataset stands after today's changes, (b) the methodology defaults I want
ratified at the next meeting and made permanent in the build pipeline, and
(c) the big open identification problem (Revelio alias mismatches) that
needs an actual design pass before the next sprint.

The planning agent should read this end-to-end, then produce a concrete
implementation plan covering Sections **3** and **4**. Sections 1–2 are
context only.

---

## 1. Where the dataset stands today

The handoff to John & Nick (Stata files + Rmarkdown walkthrough) lives at
[`outputs/stata_handoff_20260527/`](../../outputs/stata_handoff_20260527/).
It was built by [`src/data_analysis/build_stata_handoff.py`](../../src/data_analysis/build_stata_handoff.py).

Headline numbers under current defaults (strong-match sample, year FE,
Tobin's Q at t+1):

| Regressor | β | SE | N | p |
|---|---:|---:|---:|---:|
| `ai_post_share_strong` | 0.717 | 0.258 | 2,159 | 0.006 |
| `ai_mom_net_sentiment_strong` | 0.0014 | 0.0011 | 2,159 | 0.21 |

For comparison, the unfiltered sample (matching the prior Python
`summary.txt`) gives β = 0.954, p < 0.001, N = 3,101 on `ai_post_share`.
The decline under strong-match is partly genuine (less noise), partly
attenuation (real big-tech AI voices like Pichai/Walker/Schindler are
incorrectly dropped — see Section 4).

What's already on `firm_panel.dta` and matters for everything below:
- Both unfiltered and `_strong` versions of every metric (`n_posts`,
  `n_ai_posts`, `ai_post_share`, `mom_net_sentiment`, `ai_mom_net_sentiment`)
- Toggle flags: `meets_min_posts_10`, `meets_min_ai_posts_3`,
  `meets_min_posts_strong_10`, `meets_min_ai_posts_strong_3`,
  `has_strong_match`
- Outcomes at both year *t* and year *t+1*
- Mean-of-mean (person-equal-weighting) is the headline sentiment measure

---

## 2. Today's changes — context for the planner

These are already shipped; mentioning so the planner doesn't try to redo
them:

- Swapped the headline sentiment measure from post-level mean to
  **mean-of-mean** (`mom_net_sentiment` / `ai_mom_net_sentiment`) — one
  prolific poster can't dominate a firm-year.
- Added contemporaneous outcomes (year *t*) alongside the t+1 lead
  outcomes.
- Added Revelio `strong_match` as a per-profile column on the person
  files; firm-year metrics get parallel `_strong` variants.
- Pre-baked outlier CSVs and a `dropped_by_revelio.csv` so the
  Revelio false positives are visible (Nathan May → David W Grant etc.).
- Documented the known Revelio false-negative problem (Pichai et al.)
  in the README of the handoff folder. See "Known data-quality
  limitations" section.

---

## 3. Defaults to ratify at the meeting and bake into the build

These are the changes I want the meeting to bless and then make permanent
in `build_stata_handoff.py` / `02_regressions.do` / the .Rmd. The planner
should produce concrete edits for each.

### 3.1 No `n_posts` minimum, default to strong-match

**Current state (do-file default):**
```stata
keep if meets_min_posts_strong_10 == 1 & meets_min_ai_posts_strong_3 == 1
```

**Desired default going forward:**
```stata
keep if has_strong_match == 1
```

- Drop the `n_posts >= 10` filter entirely from the default regression
  sample. Same for `n_ai_posts >= 3`.
- Keep the `meets_min_*` flag columns on the panel as togglable filters
  (someone can still apply them if they want to reproduce older numbers).
- The strong-match requirement stays — `has_strong_match == 1` keeps
  the firm-year if Revelio confirmed at least one of its profiles.
- All four regressions (Tobin's Q, ROA, sales growth, stock return)
  should adopt this default — currently only the Tobin's Q block uses
  the AI-share filter; the general regressors use just `n_posts >= 10`.
  Both go away.

The planner should:
- Update [`02_regressions.do`](../../outputs/stata_handoff_20260527/do/02_regressions.do)
  default blocks.
- Update [`research_walkthrough.Rmd`](../../outputs/stata_handoff_20260527/research_walkthrough.Rmd)
  default filter in `run_q_regs` and `run_general_regs`.
- Update the README's "Key decisions baked in" section.
- Re-verify the new headline coefficients on the no-filter / strong-match
  sample and write them into the README.

### 3.2 Impute 0 sentiment in years with posts but no AI posts

**Current state:** `ai_mom_net_sentiment` (and `_strong`) is NaN for any
firm-year where the firm has posts but no AI-related posts. That firm-year
is silently dropped from any regression on that LHS.

**Desired:** in those cells, impute **0** so the firm-year stays in the
sample.

Rationale: NaN drops the firm-year, which biases the sample toward
firms that *talk about* AI. Imputing 0 says "this firm posted but
expressed no AI sentiment in either direction this year," which is the
honest read. The same logic applies to `ai_mean_net_sentiment` if we
ever switch back to post-level.

Edge cases the planner needs to specify:
- Imputation applies only to firm-years where `n_posts >= 1` (the firm
  *was* on LinkedIn that year). If `n_posts == 0` and `n_ai_posts == 0`,
  leave the AI sentiment NaN — the firm didn't post at all.
- For the `_strong` variant: impute 0 only where `n_posts_strong >= 1`
  and `n_ai_posts_strong == 0`. Mirror the same logic.
- Also impute for `ai_frac_positive_strong` if we surface it (currently
  NaN by the same logic).
- Document the imputation rule in the README codebook so it's not silent.

**IMPORTANT — where the imputation lives (decision 2026-05-30):** the imputation
happens **in `02_regressions.do` (and the `.Rmd`), at regression time** — NOT in
`build_stata_handoff.py`. `firm_panel.dta` must **keep true missingness** (NaN
where a firm-year posted but had no AI posts). This matches what John already
does (`replace ai_sent_new = 0 if ai_post_share_strong == 0` inside his `.do`,
on a derived variable, leaving the panel honest) and keeps the data file and the
internal Python path ([analysis_expansion_plan_20260530.md](analysis_expansion_plan_20260530.md) §3.2)
consistent.

The planner should:
- In `02_regressions.do` / `research_walkthrough.Rmd`: build a derived
  regressor (e.g. `ai_sent_new`) that sets AI sentiment to 0 where
  `n_posts >= 1 & n_ai_posts == 0` (mirror for `_strong` with the
  `_strong` counts), leaving NaN where `n_posts == 0`. Do **not** overwrite the
  source column.
- Do **NOT** add the imputation to `build_stata_handoff.py`; the panel keeps
  NaN. (The only `build_stata_handoff.py` change in this chat is dropping the
  default min-post filters per §3.1.)
- Document the imputation rule + the `ai_sent_new` derivation in the README
  codebook so John & Nick see it's a regression-time choice, not baked data.
- Re-verify the headline regressions with the imputed regressor. The coefficient
  on the imputed AI-sentiment variable will change — possibly meaningfully,
  because the previously-dropped sample was selected on having any AI posts.

### 3.3 Strong-match stays the default

No change — strong-match is already the default. But the planner should
make sure the meeting understands the cost (Section 4 below) before
ratifying.

---

## 4. The big one — fix the Revelio crosscheck

**The problem:** Revelio's company-name matching has systematic false
negatives at firms where Compustat's corporate name differs from the
operating brand. Documented cases:

| Person | Compustat searches against | Revelio actually has them at | Result |
|---|---|---|---|
| Sundar Pichai (Google CEO) | ALPHABET INC | "Google" / "Google LLC" | dropped |
| Kent Walker (Google President) | ALPHABET INC | "Google" / "Google LLC" | dropped |
| Philipp Schindler (Google SVP) | ALPHABET INC | "Google" / "Google LLC" | dropped |

Even Revelio's fuzzy company-match (SequenceMatcher) doesn't bridge the
"Alphabet" ↔ "Google" gap. The same pattern almost certainly affects
Meta/Facebook, X/Twitter, and any other firm with a known brand vs.
corporate-name divergence.

**The cost is large.** For Google alone, ~600+ posts and the firm's
loudest AI voice (Pichai at 81% AI share) are silently filtered out of
the default regression sample. We have no way to estimate the magnitude
across the full universe without going firm-by-firm.

**Desired fix:** algorithmically discover company-name aliases at scale
and apply them as a manual alias map inside the Revelio crosscheck
logic.

### Sketch of the approach (the planner should pressure-test this)

Two paths I see, not mutually exclusive:

**A. LLM-driven alias discovery from Compustat**
- Input: the unique `conm` / `board_company` / `primary_company` names
  in `funda_*.csv` for the firms in our universe.
- Process: for each Compustat name, ask Claude (or a comparable model)
  to enumerate likely operating brand names, subsidiaries, and DBAs
  (e.g. ALPHABET INC → Google, Google LLC, YouTube LLC, Waymo LLC, etc.).
- Output: a CSV `data/revelio/company_aliases.csv` keyed on `gvkey`
  with one row per alias.
- Verification: spot-check at least 30 high-impact firms manually. Save
  the LLM's source/justification per alias so we can audit.

**B. Workforce-data-driven alias discovery on Redivis**
- For each `revelio_user_id` whose Compustat-side person is one of our
  scraped profiles, pull their actual Revelio role history (company names
  + start/end dates).
- For each unmatched (person, gvkey) pair, look at what Revelio company
  name they have during the right time window. The most-frequent
  unmatched-but-near-miss name is a candidate alias.
- This requires Redivis access — the existing notebook at
  [`src/revelio/redivis_crosscheck_notebook_v2.ipynb`](../../src/revelio/redivis_crosscheck_notebook_v2.ipynb)
  has the right environment.

Path A is faster to prototype locally and produces an interpretable
artifact. Path B is more principled because it discovers aliases from
the data Revelio actually has — but requires Redivis compute.

**Once the alias map exists**, the planner needs to:
- Decide where it gets applied — in Hall's R validation script on
  Redivis (changes the output of the cross-check), or downstream as a
  post-hoc fix-up (cheaper, doesn't require re-running cross-check).
- Add the `aliases.csv` as an input to `build_stata_handoff.py` and
  recompute `strong_match` accordingly.
- Quantify the impact: how many people and firm-years move from
  `strong_match=False` to `True`? Does the Tobin's Q coefficient shift?
- Document the alias map in the README so John & Nick can see which
  names were aliased to which.

### Open methodology questions for the meeting

1. **Confidence threshold for aliases.** "Google" → "Alphabet" is
   unambiguous. "Snap Inc" → "Snapchat" is clear. But what about edge
   cases like "Meta Platforms" → "Instagram" / "WhatsApp" (these are
   subsidiaries; should role records at subsidiaries count toward
   parent strong-match)?
2. **Should aliases be symmetric?** If "Alphabet" → "Google", do we
   also accept Revelio records that explicitly say "Alphabet" as
   validating someone whose Compustat record is "Google"?
3. **Historical name changes.** "Facebook" → "Meta" happened in 2021.
   Role records pre-2021 say Facebook. Should the alias be time-aware?
4. **Subsidiaries.** If someone's role history at Revelio is at
   "YouTube" and Compustat searches them against "ALPHABET INC", does
   that pass? Probably yes for our purposes (they're really at the
   firm), but it's a judgment call.

The planner should propose answers to each before implementation
begins.

---

## 5. What the planner is being asked for

- A concrete implementation plan for the three Section 3 default changes
  (3.1, 3.2, 3.3). Each should name the files to edit, the function
  signatures to change, and the verification steps. Section 3.2 is the
  one that requires real care about edge cases.
- A design proposal for Section 4 covering: which path (A, B, or both),
  proposed schema for `company_aliases.csv`, where in the pipeline the
  alias resolution lives, and how we'll validate that the fix actually
  rescues the known cases (Pichai, Walker, Schindler) without
  introducing new false positives.
- An ordering: which goes first, what depends on what, and what can
  be parallelized.

Section 3 is the easier, mostly-mechanical work. Section 4 is the one
that needs careful thought before we touch code — please don't
collapse it into "add an aliases CSV" without thinking through the
methodology questions above.

---

## 6. Non-asks (deliberately out of scope here)

- The big-tech name-vs-URL mismatch on Tesla ("Lewis Hamilton" = Greg
  Reichow; "Tesla" = Vaibhav Taneja). The URLs are right; the
  `author_name` is wrong. This is annoying but cosmetic — Revelio's
  strong_match works URL-side, so it doesn't change any numbers. Worth
  fixing but later.
- Elon Musk being absent from the Tesla data. He's on X, not LinkedIn.
  Nothing to do.
- FinBERT coverage (41% of corpus). Separate task, Sherlock GPU re-run.
- Any further data-collection scope expansion. The current corpus is
  fine; the open work is about analyzing what we have, not collecting
  more.

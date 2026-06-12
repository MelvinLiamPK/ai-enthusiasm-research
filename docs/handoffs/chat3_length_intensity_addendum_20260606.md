# Chat 3 addendum — post-length / intensity signal (John's suggestion)

**Date:** 2026-06-06 · **From:** planning hub · **For:** Chat 3 (full analysis v1)
**Extends:** [`analysis_expansion_plan_20260530.md`](analysis_expansion_plan_20260530.md) **§5** (new specs).
Slot these into the v1 run on the current corpus; they re-run in v2 post-expansion.

---

## 1. Why (and why it's NOT already captured)

John's idea: AI **enthusiasts may write *more* about AI**, so post length is a signal.
This is **valid and orthogonal to the current measures**, because our per-post score is a
**length-normalized density**, not a count:

[`analysisAI_LM.py:142-147`](../../src/data_analysis/analysisAI_LM.py#L142-L147):
`lm_net_sentiment = (positive_count/word_count − negative_count/word_count) × 1000`
→ net positive LM words **per 1,000 words**. A 30-word "AI is amazing!" and a 600-word
enthusiastic essay with the same density score **identically**. Length is divided out.

The signal map — length is the one margin nothing touches:

| Margin | Captured by |
|---|---|
| Extensive (posts about AI?) | `ai_post_share`, `n_ai_posts` |
| Valence per word (density) | `ai_mom_net_sentiment_strong` (headline) — **length removed** |
| Volume (how many AI posts) | `sum` / `mean_of_sum` variants |
| **Intensity (how much per AI post)** | **— nothing —** ← this addendum |

---

## 2. Fields available (already persisted, per post)

In `data/canonical/current/posts_scored_unique.csv`: `lm_word_count`,
`lm_positive_count`, `lm_negative_count`, `lm_net_sentiment`, `is_ai_related`.
Everything below is buildable with no re-scoring.
**Reshare caveat:** reshares have null `post_text` (~11.9%); their `lm_*` counts are 0/NaN.
Restrict all length/count measures to posts with `lm_word_count > 0` (drop empty/pure
reshares) so length isn't mechanically deflated.

---

## 3. Measures to build (AI posts only; strong-match sample; person→firm-year)

Aggregate person→firm-year with the **same person-equal-weight (mean-of-...) discipline**
as the headline, keep **true missingness**, apply **regression-time zero-imputation** to
match Chat 2 (see PIPELINE_STATE imputation rule).

1. **Total enthusiasm output (the cleanest form of John's idea) — a LEVEL measure.**
   Per person-year: `ai_net_pos_words = Σ_{AI posts} (lm_positive_count − lm_negative_count)`.
   Rises with density × length × volume jointly = "wrote a lot of net-positive AI content."
   To firm-year: `mean_of_sum` (person-average of their totals; parallels the existing
   `mean_of_sum` variant). Also keep a `sum_over_people` version for a size-weighted view.
2. **Mean AI-post length.** Per person: `mean(lm_word_count)` over AI posts; firm-year =
   person-mean. The direct "do they write more per AI post" control.
3. **Within-person relative length (style-confound control).**
   `ai_mean_post_len / all_mean_post_len` for the same person — nets out baseline verbosity
   (consultants/academics write long regardless of topic).

---

## 4. Specs to run (horse-race against the existing headline)

LHS = `ln_tobins_q`, strong-match, year & year+firm FE, clustered by gvkey — same frame as
Chat 2's headline, just adding regressors:

- **(a) Length as control:** headline + `ai_mean_post_len`. Does it enter, and does it move
  the density coefficient?
- **(b) Interaction (the real test of "long *and* positive"):** headline +
  `ai_mom_net_sentiment × ai_mean_post_len`. A positive interaction = enthusiasm intensity
  predicts beyond density.
- **(c) Level measure horse-race:** replace/augment the headline with `ai_net_pos_words`
  (the §3.1 measure). Does it add incremental fit **conditional on** `ai_post_share` +
  density? Report ΔR²/ΔAIC and whether its sign holds under firm FE.
- **(d) Robustness:** rerun (a)–(c) with the within-person relative length (§3.3) to confirm
  any length effect isn't just baseline verbosity / person composition.

**Verdict to report:** does length/intensity carry predictive signal *after* conditioning on
extensive margin + density? If yes → John's intuition is real, promote it to the headline
set for v2. If it's absorbed by share + density → document as "tested, not incremental."

---

## 5. Caveats to surface in the writeup

- **Length is valence-agnostic** — a long AI post can be a skeptic's critique. Length only
  reads as enthusiasm when combined with positive valence; hence the interaction (spec b)
  and the net-positive-words level measure (spec c), not raw length alone.
- **Short-post density noise:** very short posts give unstable density (1 pos word in 5 →
  huge); the length control may also stabilize the density estimate — note if so.
- **Style/role confound:** handled by the within-person relative measure + firm/person FE.
- Keep it **v1 on the current corpus**; corpus-agnostic, re-runs in v2.

## 6. Deliverable back to planning
The four specs' tables + the one-line verdict (incremental or not), so we can decide whether
`ai_net_pos_words` / length joins the headline measure set for the v2 final run.

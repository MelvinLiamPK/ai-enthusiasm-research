# Handoff — Section 3 (Python-pipeline parity) COMPLETE

**Date:** 2026-06-03
**Owner:** Melvin Liam (execution chat)
**Closes:** [`analysis_expansion_plan_20260530.md`](analysis_expansion_plan_20260530.md) **§3** (§3.1, §3.2, §3.3)
**Status:** Done & verified against John's `jvrmel_06`. Unblocks §4 (the panel spine).

---

## What was done

The internal Python regression path (`src/data_analysis/sentiment_q_regression.py`)
now reproduces John's ratified headline numbers with **no hand-edited `.do`**.
John's `jvrmel_06.do` headline (its live block; everything past line 102 `stop`
is dead code) is now the script's **default mode**; the legacy staggered+controls
ladder is preserved behind `--ladder`.

### The four headline regressions (default, no flags)

LHS = **contemporaneous** `ln_tobins_q` · no controls · no winsorization ·
SEs clustered by gvkey · strong-match sample by construction.

| Spec | β | SE | p | N | vs jvrmel_06 |
|---|---:|---:|---:|---:|---|
| `ai_sent_new`, year FE | 0.0034 | 0.0005 | 0.000 | 16,385 | ✅ exact |
| `ai_sent_new`, year+firm FE | 0.0007 | 0.0002 | 0.001 | 16,250 | ✅ exact |
| `ai_post_share_strong`, year FE | 1.0052 | 0.1719 | 0.000 | 16,385 | ✅ exact |
| `ai_post_share_strong`, year+firm FE | 0.1623 | 0.0812 | 0.046 | 16,250 | β/N exact; SE 0.081 vs 0.076 |

The one non-match is benign: `reghdfe` vs `linearmodels` count absorbed firm
dummies differently in the clustered-SE small-sample correction, so the
`ai_post_share_strong` firm-FE SE differs (p .046 vs .033). **β and N are exact
and both p<0.05.** Singleton firms are dropped under firm FE to match `reghdfe`
(N 16,385→16,250).

### §3.1 — filters dropped, strong-match default
- `--min-posts` / `--min-ai-posts` defaults → **0** (old 10/3 filters gone; flags
  retained for reproducibility).
- Strong-match is now the **default** sample; `--all-matches` opts out.
- `--lead` default → **0** (contemporaneous, matching jvrmel_06; `--lead 1` for t+1).

### §3.2 — zero-imputation (regression-time only)
Exact rule, on the in-memory regression frame only:
```
ai_sent_new = ai_mom_net_sentiment_strong
            = 0   where (n_posts_strong >= 1 & n_ai_posts_strong == 0)   # ≡ ai_post_share_strong == 0
            = NaN where (n_posts_strong == 0)
```
- 12,532 firm-years imputed to 0. Toggle: `--no-impute-zero-ai-sentiment`
  (default ON). Disabling it collapses `ai_sent_new` to N≈4,374 and kills the
  firm-FE result — confirming the imputation is load-bearing.
- **On-disk honesty verified:** all 12,532 `posted-strong-but-no-AI` cells remain
  **NaN** (0 baked) in `data/canonical/current/firm_panel_annual.dta`. Nothing
  was written to the data layer.
- **Identical** to the rule in `build_ra_dataset.py` (lines 299–301) and
  `compute_summary_stats._imputed_ai_sentiment` → **Chat 4 (RA release) is
  already aligned.**

### §3.3 — strong-match default
Satisfied by §3.1 above. The Revelio alias expansion (prior brief §4) is **not**
pre-empted here.

---

## Where the numbers live
- Code: [`src/data_analysis/sentiment_q_regression.py`](../../src/data_analysis/sentiment_q_regression.py)
- Run output: `outputs/sanity_checks/regression_tobins_q_parity_20260603_001525/`
  (`coefficients.csv`, `summary.txt` — the latter carries the imputation-rule string).

## Notes for the planner (§4 onward)
- §3 is the parity prerequisite; **§4 (multi-frequency panel) is unblocked.**
- The headline path reads the canonical **`.dta`** (not the CSV aggregate) because
  the mean-of-mean `_strong` columns and `ln_tobins_q` live only in the panel.
  §4's panel builder should carry those same columns + raw `n_posts(_strong)` /
  `n_ai_posts(_strong)` counts so the §3.2 imputation stays derivable downstream.
- Canonical discipline respected: only sanity-check tables emitted; **`current`
  not flipped**, no new release cut.

## Not done (out of this chat's scope)
- WRDS∪def14a tenure union / tenure-gating (**Wave 4 — deferred**).
- Writing the rule into a release `CODEBOOK.md` (release is immutable; fold into
  the next cut). The rule is documented here and in the run's `summary.txt`.

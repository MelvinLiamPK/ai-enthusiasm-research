# Progress email — to Nick / John / Daron (ready to send)

**Note:** drafted for the missed meeting; copy into your mail client to send.

---

**Subject:** AI-enthusiasm project — progress update (apologies for missing this week)

Hi Nick, John, Daron,

Sorry to have missed this week's meeting. Here's where things stand — I'll bring slides next time.

**1. John's regressions reproduced in the Python pipeline.** The internal path now reproduces John's ratified `jvrmel_06` headline numbers with no hand-edited do-file. Three of four headline specs match exactly; the fourth matches on coefficient and N with a benign standard-error difference (a known reghdfe-vs-linearmodels small-sample detail). AI-sentiment → Tobin's Q stays positive and significant throughout:

| Spec | β | SE | N |
|---|---:|---:|---:|
| AI sentiment, year FE | 0.0034 | 0.0005 | 16,385 |
| AI sentiment, year + firm FE | 0.0007 | 0.0002 | 16,250 |
| AI-post share, year FE | 1.005 | 0.172 | 16,385 |
| AI-post share, year + firm FE | 0.162 | 0.081 | 16,250 |

Zero-imputation of AI sentiment is applied only at regression time — the data keeps true missingness — and it's load-bearing (removing it collapses the firm-FE result).

**2. DEF 14A enrichment of the director panel.** Backfilled director **age to 95.5%** coverage (mean 62.6) from data already on disk — no re-scraping — and validated it with a year-over-year consistency check. Also derived firm-year **board-composition** features (board size, share of new nominees, mean board age across 9,554 firm-years) and a clean **tenure / new-nominee** table (1,423 genuine new nominees) to support the identification work. (Age covers directors; executive age would need an Execucomp pull, which we can add if useful.)

**3. Company name / alias problem (the "Google vs Alphabet" issue).** Built a data-derived alias list from SEC EDGAR former-names, CRSP stocknames, and Compustat — 18,130 entries covering **all of our firms** — so the Revelio company-match test can check a *set* of acceptable names per firm instead of one legal name. Roughly **half of firms have had a genuinely distinct former name**, and the list rescues the cases that were silently dropped before, e.g. Alphabet ← Google Inc and Meta ← Facebook Inc. Two follow-ups are queued: an LLM layer for pure brands that were never a legal name (YouTube, Waymo), and then applying the alias set to recompute the profile matches.

Separately, an RA-facing dataset snapshot is packaged and ready to share with the other RAs, labeled as pre-expansion.

Happy to discuss async or in next week's slot.

Best,
Melvin

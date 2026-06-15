# Progress email — to Nick / John / Daron (ready to send)

**Note:** drafted for the missed meeting; **refreshed 2026-06-15** with the corpus-recovery and extended-analysis results. Copy into your mail client to send.

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

**4. Corpus coverage audit + recovery.** I audited our LinkedIn scraping against the original run logs and found the first scrape had silently dropped ~1,050 strong-match profiles to transient API failures — they read as "post-less" but weren't. Recovering them added **~1,050 profiles and ~137,000 posts (about +7% of the strong-match posting corpus)** for ~$750. A useful side-finding: the real scraping cost is roughly **half a cent per post** (pennies per profile), so an earlier ~$6,800 estimate for a backlog scrape was ~140× too high — expanding beyond the current sample is far cheaper than we'd assumed.

**5. Extended analysis (preliminary, on the pre-expansion corpus).** Picking up the threads from the last session:
- **Five-year long differences:** the AI-sentiment → Tobin's Q effect is *larger* over 5-year differences than year-to-year (0.00092 vs ~0.0007) — the within-year fixed effects are attenuating a relationship that strengthens over longer horizons.
- **Firm size:** weighting by assets, the within-firm effect concentrates in **smaller** firms (it washes out once large firms dominate the estimate).
- **Sub-annual returns:** no detectable monthly/quarterly stock-return predictability from general sentiment.
- **John's post-length idea:** tested three ways. Once you condition on whether and how positively people post about AI, raw post length adds no incremental signal — our score is already a per-word density, so length is divided out. Worth revisiting on the expanded corpus given the still-modest AI-posting sample.
- **Descriptive cuts:** AI enthusiasm rises with firm size and R&D intensity, is higher among **younger** directors, and is modestly higher for men than women. AI-post share rose ~5× after ChatGPT.

These are build artifacts for discussion, not yet a frozen dataset; they'll re-run on the expanded corpus.

Separately, an RA-facing dataset snapshot is packaged and ready to share with the other RAs, labeled as pre-expansion.

Happy to discuss async or in next week's slot.

Best,
Melvin

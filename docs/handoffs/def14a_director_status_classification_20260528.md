# Handoff → Director Status Classification (incumbent vs. nominee)

**Date:** 2026-05-28
**From:** Track 2 (DEF 14A pipeline)
**To:** New chat — classify DEF 14A directors by board-service status
**Purpose:** Enable a separate identification strategy that distinguishes sitting directors from director-nominees, and (if N is large enough) isolates **nominees who were never elected** as a potential instrument/control group.

---

## Why this matters (read first)

The DEF 14A proxy statement is a legal SEC filing. Under Schedule 14A (Rule 14a-101, Item 7), a company **must disclose all directors** — both:
- **Continuing directors** (not up for election this cycle, e.g. staggered boards), and
- **Director-nominees** standing for election — which includes *both* incumbents seeking re-election *and* genuinely new first-time nominees.

So everyone in a DEF 14A is either a sitting director or a nominee. We currently treat them all as "directors," which conflates three economically distinct groups:

1. **Incumbents** — already on the board, demonstrated influence over firm decisions (including AI investment). Endogeneity concern: their AI sentiment may be *caused by* the firm's existing AI strategy.
2. **New nominees who were elected** — joined the board fresh. Their pre-nomination AI sentiment is plausibly exogenous to the firm's prior AI strategy → cleaner instrument.
3. **Nominees who were NOT elected** — stood for election but lost / withdrew. They never influenced the firm. Their sentiment is measured but they had **zero treatment effect on the firm**, making them a natural placebo/control — valuable for a *separate* identification strategy (if N is large enough).

**Scope note for the project at hand:** the main research question is *"does AI sentiment predict AI investment?"* — for that, the **only valid group is people who were actually serving directors during the sentiment-measurement window** (incumbents, and elected nominees *once they are seated*). A person's sentiment can only be attributed to a firm during the years they were genuinely on that firm's board. Getting this right depends entirely on DEF 14A filing timing — see next section.

## DEF 14A filing timing & sentiment attribution (read carefully — easy to get backwards)

A DEF 14A is filed ahead of the company's **annual shareholder meeting**, not at fiscal year-end. Typical cadence for a December fiscal-year-end firm:

| Event | Timing |
|---|---|
| Fiscal year ends | Dec 31, year **Y−1** |
| DEF 14A filed | ~March–April, year **Y** |
| Annual meeting held | ~May–June, year **Y** |
| Elected directors serve | ~mid-**Y** → mid-**Y+1** |

So a proxy filed in **spring 2025** reports on **fiscal 2024** and nominates/confirms the board for the term running mid-2025 → mid-2026. (FY-end varies by firm; key on each filing's actual `filing_date` and the firm's fiscal-year-end, don't assume December.)

**The proxy is a snapshot at filing time that looks both backward and forward:**
- **Incumbents** listed in the spring-Y proxy were *already serving* — they were directors during fiscal Y−1 (that's *why* they're incumbents) and continue forward. Their bio gives an explicit **"director since [year]"** anchor.
- **New nominees** are forward-only — they become directors at the year-Y meeting; they were **not** on the board during Y−1.

**Past year or next year? → For incumbents, both, anchored by "director since".** A proxy filed in year Y does *not* mean the incumbent "becomes" a director in Y; it confirms an **already-running tenure**. Attribute their sentiment to the firm across their **confirmed tenure window** `[director_since, last_year_they_appear_in_a_proxy]` — not a single year.

**Validity trap (this is the whole point of the "only one valid group" instinct):** *never attribute a person's sentiment to a firm for years before they joined that board.* A new nominee's influence starts at the meeting — their pre-nomination posts do **not** belong to that firm yet for this project. (Those same pre-nomination posts are what the separate never-elected placebo strategy wants — opposite use case.)

**Build a director × firm × year tenure panel** (new deliverable — see Target output):
- Each proxy filed in spring Y establishes the **full** board for that snapshot. Note: even with **staggered/classified boards**, *all* directors are listed every year (not just the ~1/3 up for election), so one proxy per firm-year gives the complete board.
- Use `director_since` to backfill the start of tenure; use disappearance from a later proxy to bound the end.
- Attribute a director's posts in calendar year *t* to the firm **only for years inside that tenure window.**

**Two alignment wrinkles the consumer must handle:**
1. **Calendar vs fiscal year** — posts are timestamped by calendar date; Compustat AI-investment outcomes are fiscal-year. Need a rule mapping each post to the fiscal year it falls in given the firm's FY-end.
2. **Design fork (contemporaneous vs lead)** — the project thesis is *"AI sentiment **prior to** AI investment as an instrument,"* which points to a **lead** specification: sentiment in year *t* → investment in year *t+1* (or longer lead). That means the cleanest sample is directors whose tenure covers **both** the sentiment year *and* the later investment year. Confirm the lead structure with the project owner before finalizing the panel.

## The classification rule (important — don't get this wrong)

❌ **WRONG approach:** "bio text contains the word 'nominee'." Incumbents standing for re-election are *also* called nominees in proxy language, so this over-counts.

✅ **RIGHT approach:** classify by **absence of prior/current board-service signal on THIS board**:
- If the bio indicates the person has **already served on this company's board** ("director since 2018", "has served as a director since", "joined the Board in [past year]", "re-nominated", "incumbent") → **INCUMBENT**.
- If the bio shows **no prior service on this board** and they are being put forward for the first time ("nominated for election", "first-time nominee", "standing for election to the Board", no past-service language) → **NEW NOMINEE**.
- **Edge case — mid-year appointees:** appointed by the Board to fill a vacancy and now standing for their first *shareholder* election ("appointed to the Board in [year]", "elected by the Board to fill a vacancy"). They are technically incumbents but behaved like nominees until appointment. **Keep these as a separate third flag** (`mid_year_appointee`) so they're visible — they can be bucketed with incumbents downstream if needed.

The decision hinges on **"does the bio show prior or current service on this specific board?"** — not on the word "nominee".

## Target output

### Primary: per-(person × company) status flag merged into the corpus
Add a column keyed by `(profile_url, ticker)` — call it `def14a_director_status` — to the corpus files. Values:
- `incumbent`
- `new_nominee_elected`
- `new_nominee_not_elected`  ← the identification-critical group
- `mid_year_appointee`
- `unknown` (bio ambiguous / not extractable)

**Also store the identifying bio text itself** as a column (e.g. `def14a_bio_text` and/or `def14a_status_evidence` = the specific sentence(s) the classification was based on). The user explicitly wants this — the scraped bio text is valuable data in its own right, independent of the classification, and it makes the LLM's decision auditable.

Files to add the column to (left-join on `(profile_url, ticker)`; def14a rows only, NaN elsewhere):
- `data/processed/all_people_linkedin_urls/scraped_posts_combined/posts_combined_v2_20260527.csv`
- `outputs/sentiment_results/sentiment_posts_scored_unique_20260527.csv`
- `outputs/sentiment_results/sentiment_all_posts_full_coverage_20260527.csv`

### Also produce: standalone classification CSV
`data/processed/def14a_director_status_20260528.csv` with columns:
```
ticker, cik, year, filing_date, filing_url, full_name, profile_url (if matched),
def14a_director_status, def14a_status_evidence, def14a_bio_text, classifier_confidence
```
Produce this **first** and let the user review classification quality before merging into the 3 corpus files above.

### Also produce: director × firm × year tenure panel
`data/processed/def14a_director_tenure_panel_20260528.csv` — the artifact that makes valid sentiment attribution possible (see "filing timing" section). One row per `(profile_url, ticker, year)` for every year inside each director's confirmed tenure window:
```
profile_url, ticker, cik, year, director_since, tenure_start, tenure_end,
def14a_director_status, source_proxy_filing_dates, is_serving (bool)
```
Construction:
- `tenure_start` = `director_since` (from bio), falling back to the earliest proxy the person appears in for that ticker.
- `tenure_end` = the last proxy year they appear in for that ticker (open-ended / right-censored if they appear in the most recent available proxy).
- Emit a row for every `year` in `[tenure_start, tenure_end]`, with `is_serving = True`.
- This is what downstream sentiment aggregation should join against to decide *which firm-years a person's posts may be attributed to.*

## How to classify — use the existing LLM extractor

There is already a working Claude-based bio extractor: **`src/data_extraction/parse_def14a_bios.py`** (uses Claude Haiku 4.5, structured JSON output, system-prompt caching, BeautifulSoup HTML→text, bio-section narrowing). It currently extracts `{full_name, primary_company, primary_role, is_current, role_context}` and already has an `is_current` boolean — **that is a closely-related signal but NOT sufficient** (it doesn't distinguish elected vs. not-elected nominees, and `is_current` semantics need verifying against the rule above).

**Recommended approach:** extend `parse_def14a_bios.py` (or write a sibling script `classify_def14a_director_status.py`) to:
1. Reuse the cached HTML filings from `scrape_def14a.py` and the bio-narrowing logic.
2. Add to the structured output schema:
   - `served_on_this_board_before` (bool) — the core signal
   - `director_since_year` (int or null) — **critical for the tenure panel**; this anchors `tenure_start`. Extract whenever the bio states it ("director since 2018", "has served on the Board since 2018").
   - `first_appointment_year` (int or null) — for mid-year appointees ("appointed to the Board in [year]").
   - `is_nominee_this_cycle` (bool)
   - `election_outcome` (`elected` / `not_elected` / `withdrawn` / `unknown`) — **note:** the *current-year* DEF 14A states nominations but NOT outcomes. Outcomes appear in the **subsequent 8-K (Item 5.07, voting results)** or the next year's proxy. See "Determining election outcome" below.
   - `status_evidence` (string) — verbatim sentence(s) supporting the call
   - `bio_text` (string) — the full extracted bio
3. Derive `def14a_director_status` from these fields in post-processing (deterministic rules, not the LLM) so the logic is auditable and adjustable without re-running the API.

**Cost:** Haiku 4.5 at ~$0.005/filing → ~$50 for the full ~10K filing universe (within Nick's <$100 cap, per the script's own header). Prototype on `--mode prototype` first, spot-check ~30 bios by hand against the rule, then scale.

## Determining election outcome (the hard part — flag for user)

The `not_elected` group is the most valuable but also the hardest to identify, because **a single DEF 14A does not report whether a nominee won.** Options, in rough order of reliability:

1. **8-K Item 5.07 (Submission of Matters to a Vote):** filed within 4 business days after the annual meeting, reports per-director vote tallies. This is the authoritative source for who was elected. Requires a separate EDGAR pull keyed by `(cik, meeting_date)`.
2. **Next-year DEF 14A:** if a nominee appears in year T's proxy but is absent from year T+1's board list (and didn't resign for unrelated reasons), they likely weren't elected. Noisier — confounded by resignations, deaths, term expiry.
3. **Within-proxy "withdrawn nominee" language:** occasionally a proxy is amended to note a withdrawn nominee. Rare.

**Decision needed from user:** whether to invest in the 8-K pull (cleanest, more scraping work) or approximate via year-over-year proxy diffing (cheaper, noisier). Recommend scoping the 8-K pull only if step 1's prototype shows the elected/not-elected split has usable N. **First just count how many `new_nominee` directors exist at all** — if that's small, the `not_elected` subset won't be large enough to power a separate identification strategy, and this whole branch may not be worth the 8-K work.

## Suggested execution order

1. **Sanity-count first.** Run the extended classifier on the existing cached filings (`--mode prototype`, then `--mode full` if cheap). Report counts: incumbents / new nominees / mid-year appointees / unknown. **Gate:** if `new_nominee` N is too small (say < a few hundred), surface that to the user before building the 8-K outcome pipeline.
2. Build `def14a_director_status_20260528.csv` (without election outcome). Hand to user for spot-check.
3. **Build the director × firm × year tenure panel** (`def14a_director_tenure_panel_20260528.csv`) from `director_since` + proxy-appearance bounds. This is the core artifact for valid sentiment attribution — it determines which firm-years each director's posts may be attributed to. (Requires stacked multi-year proxies per firm; if only single-year proxies are available, tenure end is right-censored and `director_since` is the only start anchor — note this limitation in the output.)
4. Only if N warrants: build the 8-K Item 5.07 outcome pull → populate `election_outcome` → split `new_nominee` into `_elected` / `_not_elected`.
5. Merge the final status column + bio text into the 3 corpus files (left-join on `(profile_url, ticker)`). Sentiment aggregation then joins posts → tenure panel on `(profile_url, ticker, year)` so only in-tenure firm-years are attributed.

## Key existing files & infrastructure

| Resource | Path | Notes |
|---|---|---|
| Bio extractor (Claude Haiku) | `src/data_extraction/parse_def14a_bios.py` | **Extend this.** Already has `is_current`, `role_context`, structured output, prompt caching. |
| Existing extracted bios (pilot) | `data/processed/def14a_extracted_bios.csv` | 128 rows. Schema: `ticker, cik, year, filing_date, filing_url, full_name, primary_company, primary_role, is_current, role_context`. |
| Filing scraper | `src/data_extraction/scrape_def14a.py` | Produces cached HTML filings the extractor reads. |
| Batch-by-year runner | `src/data_extraction/batch_def14a_by_year.py` | For scaling extraction. |
| def14a → corpus join keys | `data/processed/def14a_urls_for_revelio_validation.csv` | 19,284 def14a-discovered URLs with `(person_name, company, linkedin_url, board_ticker, score, match_type)`. Use to map bios → `profile_url`. |
| Scrape outcomes | `data/processed/all_people_linkedin_urls/def14a_scrape_outcomes_20260527.csv` | 1,022 URLs with scrape status. |

## Join-key caveats (carry over from the Stata handoff)

- For def14a rows, `gvkey` is NaN — only `ticker` is populated. Join on `(profile_url, ticker)`, not gvkey.
- Name-variant duplication exists ("Teri List" / "TERI LIST" / "Teri List-Stoll"). Match bios → profile_url via the `def14a_urls_for_revelio_validation.csv` mapping (which already resolved these), not by re-fuzzy-matching names.
- A person can sit on multiple boards. The status is **per (person × company)** — someone can be an incumbent at firm X and a new nominee at firm Y in the same year. Do not collapse to person-level.

## Context on scale (for setting expectations)

- DEF 14A Serper search processed **23,060 directors**, found URLs for 21,735, with 19,284 high-confidence.
- 724 unique def14a people have scraped posts in the current corpus (889 director-seats; 178 net-new seats vs. WRDS).
- The classification universe is the bio-extraction set (~10K+ filings → potentially tens of thousands of director-bios), which is *larger* than the scraped-posts subset. Classify broadly; the posts-matched subset is what feeds sentiment analyses, but the full classification is independently useful and the `not_elected` group may only have usable N at the full scale.

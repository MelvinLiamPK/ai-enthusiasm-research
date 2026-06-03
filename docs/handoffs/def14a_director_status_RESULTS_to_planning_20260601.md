# Handoff → Planning chat: DEF 14A director-status classification RESULTS

**Date:** 2026-06-01
**From:** Track 2 execution chat (ran the classification)
**To:** Planning chat that authored `def14a_director_status_classification_20260528.md`
**Purpose:** Report exactly what was produced so planning can decide the open design questions (esp. the lead-vs-contemporaneous fork and whether to merge into the corpus).

---

## 0. Status in one line

Classification is **complete** for all 1,654 high-confidence tickers × 2020–2025. The standalone status CSV and the tenure panel exist. The merge into the 3 corpus files is **not done** (handoff gated it on user review). Cost ≈ $296 (Haiku 4.5). Raw filings now stored **gzipped** (`data/raw/def14a/{cik}_{year}.html.gz`); both reader scripts handle the `.gz` fallback.

---

## 1. Exact output files + schemas

### `data/processed/def14a_director_status_20260528.csv` — one row per (person × filing-year)
96,955 rows · 92,632 with `is_director=True` · 1,654 firms · 9,588 filings with a roster.

| column | meaning |
|---|---|
| `ticker`, `cik`, `year`, `filing_date`, `filing_url` | filing identity (year = proxy filing calendar year) |
| `full_name` | name as written in the proxy |
| `def14a_director_status` | derived label: `incumbent` / `new_nominee` / `mid_year_appointee` / `not_director` / `unknown` |
| `served_on_this_board_before` (bool) | THE core signal |
| `director_since_year` (int/blank) | from bio; anchors tenure_start. Populated for 86,615/92,632 director rows (**93.5%**) |
| `appointed_by_board_to_fill_vacancy` (bool) | mid-year-appointee signal |
| `first_appointment_year` (int/blank) | vacancy-fill appointment year |
| `is_nominee_this_cycle` (bool) | standing for election in this proxy |
| `is_director` (bool) | board director/nominee of filing co (False = named officer / ownership row) |
| `def14a_status_evidence` (str) | verbatim sentence(s) the call rests on |
| `def14a_bio_text` (str) | full bio paragraph(s), verbatim (~1,230 chars median) |
| `classifier_confidence` | `high`/`medium`/`low`/`unknown` — **LLM self-report, NOT a statistical CI; uncalibrated**. 95.7% high. |

**Status counts:** incumbent 87,779 · mid_year_appointee 2,829 · new_nominee 1,640 · not_director 4,323 · unknown 384.

**⚠️ `age` was NOT added or back-filled.** There is no age column in this run. If the panel needs director age (e.g. for retirement-hazard or controls), it must be extracted in a follow-up pass (the bio text is stored, so it's recoverable without re-scraping).

### `data/processed/def14a_director_tenure_panel_20260528.csv` — one row per (director × firm × served-year)
224,429 rows.

| column | meaning |
|---|---|
| `profile_url` | LinkedIn URL via the name bridge (blank if unmatched) |
| `ticker`, `cik`, `year` | firm + calendar year of service |
| `director_since` | bio-stated start year (blank if never stated) |
| `tenure_start`, `tenure_end` | confirmed service window (see §3) |
| `def14a_director_status` | status in that year's proxy; back-filled years = `incumbent` |
| `source_proxy_filing_dates` | filing dates the person appears in for this firm |
| `is_serving` (bool) | always True in current build (panel only emits served-years) |
| `right_censored` (bool) | True if tenure_end = latest proxy we hold → end unknown, extends past data |

---

## 2. Name → profile_url bridge — BUILT

- **Where:** `src/data_extraction/build_def14a_tenure_panel.py` (`load_mapping` / `resolve_url`).
- **Against:** `data/processed/def14a_urls_for_revelio_validation.csv` (19,284 discovered URLs; the same file that already resolved name variants during URL discovery).
- **Join key:** `(normalized-name, ticker)` with a fallback to `(first-initial + last-name, ticker)`. Normalization drops punctuation, suffixes (Jr/III/etc.), and parenthetical nicknames.
- **Match rate:** **16,483 of 23,079 distinct (director, firm) pairs = 71%.** The unmatched 29% are overwhelmingly directors for whom **no LinkedIn URL was ever discovered** (not a join failure) — they simply aren't in the corpus.
- **Caveat:** this is the *director ↔ URL* bridge. It does not yet confirm those URLs are in the *scraped-posts* corpus — that intersection (the actual "do we already have them / does the merge add posts" question) is **not yet computed**.

---

## 3. Tenure-panel construction

Per (normalized-name, ticker), after a **canonicalization** pass that merges name-variant fragments of the same person on the same board (e.g. "Sue"/"Susan Rataj", "Robert"/"Robert D Isom") — merged only when the variants never co-occur in the same year (true distinct people would co-occur). 137 fragment groups merged at full scale.

- `tenure_start` = `director_since` (bio) if present, else earliest proxy-year the person appears; clamped to ≤ earliest appearance.
- `tenure_end` = latest proxy-year the person appears for that firm.
- `right_censored` = True iff `tenure_end` equals the most recent proxy-year we hold for that ticker (148,392 of 224,429 rows are right-censored — most current directors, expected).
- A row is emitted for **every** year in `[tenure_start, tenure_end]`; years where the person isn't in that year's proxy but falls inside the window are back-filled as serving (a director missing from one annual proxy almost always still served). ~99% of tenures are contiguous; back-fill touches ~1%.

**Left-side limitation:** `tenure_start` can predate our 2020 window via `director_since` (good — 2,701+ tenures extended backward in the pilot), but if `director_since` is blank, start is censored at first appearance.

---

## 4. Election outcome (8-K Item 5.07) — **PENDING / not done**

- The 8-K vote-tally pull was **not** run. Therefore `new_nominee` **cannot currently be split** into `new_nominee_elected` / `new_nominee_not_elected`. All 1,640 first-time nominees sit in the single `new_nominee` bucket.
- **Diagnostic we ran instead (year-over-year proxy diffing):** a new_nominee in year T is "present" if they reappear as a director in T+1. new_nominee disappearance rate = **8.4%**, which is *below* the incumbent disappearance rate (**9.8%**, the false-absence + ordinary-churn floor). Net excess ≈ −1.4 pts → **implied genuine election failures ≈ 0.**
- **Implication for planning:** the `not_elected` placebo group — the identification-critical group the original handoff flagged — looks **essentially empty**, consistent with >99% uncontested board elections. Even an 8-K pull would likely return near-zero. **Decision needed:** is the 8-K pull still worth it, or do we treat all elected new_nominees as seated-from-T and drop the never-elected strategy?

---

## 5. Known issues / fragile bits

1. **Unmatched directors (29% of pairs):** mostly no LinkedIn URL ever found; a minority are name-bridge misses. Not yet reconciled against the *posts* corpus.
2. **Multi-board handling:** status is correctly **per (person × firm)** — a person can be incumbent at firm X and new_nominee at firm Y. Panel keys on (name, ticker); the same person across firms is intentionally separate rows. Cross-firm person identity relies on name only (no stable person-id).
3. **17 filings have no roster** (special-meeting/merger proxies + 4 tiny stub/notice docs) — correctly empty, not failures. So 9,588 of 9,605 filings carry directors.
4. **`classifier_confidence` is LLM self-report**, uncalibrated. Don't use it as a probability.
5. **Calendar vs fiscal year not yet applied.** `year` is the proxy *filing* calendar year. Mapping posts (calendar-dated) → Compustat fiscal years, and the snapshot-looks-backward logic from the original handoff, are **not** baked into the panel yet.
6. **Mega-filing recall** required a whole-document fallback for ~46 oversized proxies; resolved, but very large/idiosyncratic layouts remain the most fragile.
7. **`primary_company` was NOT extracted** in this run (that was the separate `parse_def14a_bios.py`, only ever run on 10 filings). "Did we find a different primary employer?" is unanswered — recoverable from stored `bio_text` via a parse pass, no re-scrape.

---

## Decisions we need from planning / the owner

1. **Design fork (the big one):** contemporaneous (sentiment_t → investment_t) vs **lead** (sentiment_t → investment_{t+1+}). The original handoff deferred this to the owner; it determines which served-years a post attaches to and whether we require tenure to span both years.
2. **Calendar→fiscal mapping rule** for attributing a calendar-dated post to a fiscal year given each firm's FY-end.
3. **8-K Item 5.07 pull:** proceed (given ≈0 expected failures) or drop?
4. **Merge go/no-go:** should we merge `def14a_director_status` + bio into the 3 corpus files now, and is the value-add (a cleaner served-vs-nominee panel that we can defend as ground truth over Execucomp) worth it for the posts subset specifically — pending the corpus-intersection count we haven't run.

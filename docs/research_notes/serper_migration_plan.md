# Plan: LinkedIn URL Discovery via Serper.dev with Scoring

## Context
The project needs to re-run LinkedIn URL discovery using Serper.dev instead of Google Custom Search API. Key motivations:
- **Cost**: Serper.dev is ~$29-97 for 97K queries vs $485+ for alternatives
- **Better matching**: Current algorithm picks the first verified name match with no company verification or scoring. When the correct profile is the 3rd result, it can pick the wrong one.
- **Low-score flagging**: Instead of returning a wrong profile as "unverified", low-scoring results should be marked `not_found` (person likely has no LinkedIn).

## Approach: New file `src/data_collection/find_urls_serper.py`

Copy `find_urls.py` and modify. Keeps all existing helpers (name cleaning, nickname dictionary, `verify_name_match`, checkpoint system, CLI structure).

### Changes from `find_urls.py`

**1. API swap** — Replace Google CSE config with Serper.dev:
- `POST https://google.serper.dev/search` with `X-API-KEY` header, JSON body `{"q": ..., "num": 10}`
- Response uses `organic[]` instead of `items[]`, and includes `snippet` field
- `SERPER_API_KEY` from `.env`
- Reduce delay to 0.5s (Serper allows 300 QPS)

**2. Scoring algorithm** — New `score_result()` function:

| Signal | Points |
|--------|--------|
| Both first + last name match | 50 |
| Last name only | 25 |
| First name only | 15 |
| Company name in title/snippet | 20 |
| Position bonus (10 - index) | 0-10 |

- Max score: 80
- **Threshold: 30** (configurable via `--score-threshold`)
- Below threshold → `status="not_found"`, no URL returned
- Company matching: full name substring, fallback to all words >3 chars

**3. Output** — One new column: `score` (numeric, 0-80)

**4. CLI** — Add `--score-threshold` argument (default 30)

### Files to modify/create
- **Create**: [find_urls_serper.py](src/data_collection/find_urls_serper.py) (copy + modify from [find_urls.py](src/data_collection/find_urls.py))
- **Update**: [CLAUDE.md](CLAUDE.md) — add `SERPER_API_KEY` to credentials list

### Progress

**Script created:** `find_urls_serper.py` is implemented and working.

**Prototype test (10 cases from audit data) — 2026-04-01:**

| Person | Google CSE result | Serper + Scoring |
|--------|------------------|-----------------|
| Bhavesh Patel @ Union Pacific | Wrong Patel at IBM | Correct: bhavesh-bob-patel (score=60) |
| Troy Alstead @ Starbucks | Random: Marci Rosine | Correct: troyalstead (score=80) |
| Jacqueline Reses @ Block | Random: Ashley Sutton | Correct: jacqueline-reses (score=80) |
| Douglas Morris @ Activision | Random: Jacob M. | Correctly marked NOT FOUND (score=10) |
| Naomi Kelman @ Nat'l Vision | Random: Lynae Millette | Correctly marked NOT FOUND (score=10) |
| Joseph Levin @ LendingTree | Random: Josh Eldridge | Correctly marked NOT FOUND (score=29) |

**Borderline cases (score ~30) need review:**
- Enrique Hernandez @ Chevron (score=33) — returned jackhernandez, likely wrong
- James Judge @ Eversource (score=30) — returned duncan-mackay, wrong
- Amy Schulman @ Pfizer (score=30) — returned heather-sheehan, wrong

**Open question:** Consider raising threshold from 30 to 35 to reject these borderline false positives.

**Expanded test (15 cases) — 2026-04-02:**

Added: Steven Nielsen (Univar), Jeffrey Siegel (Univar), Frank Fusco (Astoria), Robert Blalock (United Community Banks), D. Eugene Ewing (Darling Ingredients)

| Person | Google CSE | Serper + Scoring | Real LinkedIn exists? |
|--------|-----------|-----------------|----------------------|
| Steven M. Nielsen @ Univar | Michelle Brown | Michelle Brown (score=30) — same wrong result | YES: stevenmnielsen |
| Jeffrey H. Siegel @ Univar | Paul Cyr | NOT FOUND (score=0) | Unknown |
| Frank E. Fusco @ Astoria Financial | Frank Sandoval | NOT FOUND (score=0) | Unknown |
| Robert H. Blalock @ United Community Banks | Kim Blalock | jamie-blalock (score=35) — still wrong | Unknown |
| D. Eugene Ewing @ Darling Ingredients | Chad Darling | NOT FOUND (score=21) | Unknown |

**Key finding:** For Steven Nielsen, the real profile (`linkedin.com/in/stevenmnielsen`) doesn't appear in ANY of the 10 Serper results. Raw results are all Univar employees but none named Nielsen. This is a Google ranking issue, not an API issue.

**Investigating:** Why does Google rank Michelle Brown above Steven Nielsen for query "Steven M Nielsen Univar Solutions site:linkedin.com/in/"?

**Root cause — Steven Nielsen case:**
Query `"Steven M Nielsen Univar Solutions site:linkedin.com/in/"` returns 10 results — ALL are random Univar employees, none contain "Steven" or "Nielsen". Google ignores the person name entirely and matches only on company. But searching WITHOUT the company (`"Steven M Nielsen site:linkedin.com/in/"`) returns the correct profile at `linkedin.com/in/stevenmnielsen/`. The company name drowns out the person name in Google's ranking.

### Proposed fix: Two-query hybrid strategy

For each person, run **two queries**:
1. `"{name} {company} site:linkedin.com/in/"` — current approach
2. `"{name} site:linkedin.com/in/"` — name-only fallback

Score all results from both queries (deduped by URL). If the same URL appears in both, +10 bonus. This costs 2x API calls (~$60-194 for 97K people) but catches cases where company name hurts ranking.

**Updated scoring (0-90 scale):**

| Signal | Points |
|--------|--------|
| Both first + last name match | 50 |
| Last name only | 25 |
| First name only | 15 |
| Company name in title/snippet | 20 |
| Position bonus (10 - index) | 0-10 |
| **Appears in both queries** | **+10** |

Max score: 90. Threshold: **35** (raised from 30).

### Next steps
1. Implement two-query strategy in `find_urls_serper.py`
2. Raise threshold from 30 to 35 (all score=30 cases were wrong)
3. Re-test expanded cases with two-query approach
4. Add Steven Nielsen case to `Meeting_Notes_7.md` (outside project dir: `/Users/melvinliam/Documents/Uni/RA-NB/Meeting Notes/Meeting_Notes_7.md`)
5. Test file: `data/test_serper_cases.csv` (15 rows from audit data)

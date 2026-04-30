# Revelio Cross-Check: LinkedIn URL Validation

## Overview

Cross-validated our LinkedIn URLs against the Revelio Labs Workforce Data (v6.0) dataset hosted on Redivis. The goal was to identify which URLs are correctly matched to the right person and company. Analysis restricted to people only (entity blockholders excluded, `is_entity=False`).

## Method

### Step 1: URL Export
Cleaned all LinkedIn URLs to `linkedin.com/in/<slug>` format and exported 58,562 unique valid URLs (`data/revelio/urls_for_redivis.csv`).

### Step 2: Redivis Workflow (SQL Transforms)
- **Transform 1:** Joined our URLs against Revelio's `individual_user` table (708M rows) on `profile_linkedin_url` → 56,768 matched users
- **Transform 2:** Joined matched `user_id`s against `individual_position` (1.66B rows, full table) → 483,471 career positions for 56,500 users

### Step 3: Notebook Cross-Check (`src/revelio/redivis_crosscheck_notebook.ipynb`)

Three confirmation signals computed per URL:

| Signal | Logic |
|---|---|
| `revelio_url_match` | URL found in Revelio's `individual_user` table |
| `revelio_name_confirmed` | Revelio `fullname` matches our person name (4-stage cascade) |
| `revelio_company_confirmed` | Any career position matches our company name |

**Strong match** = (`revelio_name_confirmed` OR `verified`) AND `revelio_company_confirmed`

`verified` = Google CSE found the person's name in the LinkedIn title (independent name signal).

### Name Matching — 4-Stage Cascade
1. **Last name substring** — ASCII normalized (accents stripped), credentials stripped (CPA, MBA, J.D., etc.), comma-split
2. **Hyphenated name** — check each part of hyphenated last names separately
3. **First name + last initial** — catches "John S." LinkedIn abbreviations
4. **difflib similarity ≥ 0.85** — catches typos and name format variants

Tries `person_name_clean` (pre-cleaned) first, then falls back to `person_name`.

### Company Matching
- Strip legal suffixes (Inc, Corp, Ltd, LLC, Group, Holdings, International, etc.) before comparison
- Bidirectional substring match after normalization
- Token overlap: ≥50% of our company name tokens appear in Revelio position string
- Checks both `company_name` (original) and `company_name_clean` (truncated) to handle abbreviation mismatches

---

## Results (people only, entity blockholders excluded)

| Metric | Count | Rate |
|---|---|---|
| Total people | 91,748 | — |
| Has URL | 63,636 | 69.4% of people |
| Verified (name in LinkedIn title) | 48,077 | 52.4% of people |
| Revelio URL match | 61,361 | 96.4% of URLs found |
| Name confirmed (Revelio) | 41,487 | 64.4% of Revelio matched |
| Company confirmed (Revelio) | 33,872 | 52.6% of Revelio matched |
| Both confirmed (Revelio only) | 26,469 | 41.1% of Revelio matched |
| **Strong match** | **28,290** | **46.1% of Revelio matched** |

### Strong Match Breakdown

| Component | Count |
|---|---|
| All three signals (Revelio name + Google verify + company) | 26,197 |
| Google-verified + company (Revelio name failed) | 2,342 |
| Revelio name + company only (no Google verify) | 272 |

### By Source

| Source | n | Strong matches | Rate |
|---|---|---|---|
| Executive | 32,263 | 15,049 | 61.2% |
| Director\|Executive (dual) | 3,827 | 1,858 | 60.5% |
| Director | 41,591 | 8,813 | 34.2% |
| Blockholder (individual) | 14,066 | 2,569 | 32.4% |

Executives have ~27pp higher strong match rates than directors. Directors often hold multiple board seats and don't list them as career positions in Revelio or on LinkedIn.

---

## S&P 500 vs Non-S&P 500

| Group | People | Has URL | Verified | Strong match |
|---|---|---|---|---|
| S&P 500 (498 cos) | 18,212 | 14,344 (78.8%) | 11,344 (62.3%) | 6,748 (49.1%) |
| Non-S&P 500 (2,298 cos) | 73,536 | 49,292 (67.0%) | 36,733 (50.0%) | 21,542 (45.2%) |
| All (2,796 cos) | 91,748 | 63,636 (69.4%) | 48,077 (52.4%) | 28,290 (46.1%) |

S&P 500 membership based on current constituents. Some non-S&P 500 companies were constituents at time of data collection (e.g. Hertz post-bankruptcy), so the true S&P 500 quality gap is likely larger.

### Company-Level Distribution

| | S&P 500 (498) | Non-S&P 500 (2,298) | All (2,796) |
|---|---|---|---|
| 0 strong matches | 11 (2.2%) | 156 (6.8%) | 167 (6.0%) |
| ≥1 strong match | 487 (97.8%) | 2,142 (93.2%) | 2,629 (94.0%) |
| ≥2 strong matches | 480 (96.4%) | 2,034 (88.5%) | 2,514 (89.9%) |
| ≥5 strong matches | 452 (90.8%) | 1,629 (70.9%) | 2,081 (74.4%) |
| Median per company | 14.0 | 7.5 | 8.0 |
| Mean per company | 13.6 | 8.3 | 9.2 |

---

## Confidence Tiers

| Tier | Condition |
|---|---|
| **High** | `revelio_name_confirmed` AND `revelio_company_confirmed` |
| **High (alt)** | `verified=True` AND `revelio_company_confirmed` |
| **Medium** | `revelio_name_confirmed` only (company link uncertain) |
| **Low** | `revelio_url_match` only |
| **None** | No Revelio match |

---

## Output

`revelio_validation_summary.csv` — columns:
- `linkedin_url`, `person_name`, `company_name_clean`, `source`, `verified`
- `revelio_url_match`, `revelio_name_confirmed`, `revelio_company_confirmed`, `revelio_user_id`

---

## Technical Notes

- Revelio's `individual_position` must be queried at **full table** (not 1% sample) — the default Redivis workflow node is sampled; use qualified ID `individual_position:8xgp`
- The `IN` clause approach for fetching positions hit query length limits at ~50k IDs; replaced with a server-side JOIN against Transform 1 output (`matched_users:kh5e`)
- Redivis notebook username: `ml2068`; dataset IDs: `urls_for_redivis:5n0r`, `all_linkedin_urls`
- `rapidfuzz` is not available in Redivis notebooks; name similarity uses stdlib `difflib.SequenceMatcher`
- Entity blockholders (`is_entity=True`, n=5,223) excluded from people-level analysis

# Post-Tuesday handoff — two parallel tracks

**Date:** 2026-05-27 (post-meeting)
**Owner:** Melvin Liam

The two tracks below are independent and can be run in parallel: Track 1 happens locally / on Sherlock CPU; Track 2 runs on Apify (external service) for 6–8 hours. Track 2 doesn't need your attention while it runs.

---

## TRACK 1 — Prepare Stata datasets for John

**Goal:** give John self-contained, exploration-friendly Stata files so he can poke at the sentiment data by **company, by person, and over time** to find outliers and sanity-check the signal.

### Don't import the 2.8 GB CSV into Stata

The raw `posts_combined.csv` is the wrong unit for what John wants:

- Stata loads the whole file into memory. The CSV is 2.8 GiB; the corresponding `.dta` is roughly 1.5–2 GB. Loadable, but interactive exploration grinds.
- Post text itself isn't useful for outlier-hunting at the firm/person/time aggregation John is after. He wants summary statistics, not natural-language reading.
- Stata strings are bounded (`str244` for normal vars, `strL` for unlimited but expensive). Post bodies in raw form make the file unwieldy.

Instead, give him **four pre-aggregated `.dta` files** at the right grain — small enough to be interactive, rich enough to support outlier detection at every level he asked for.

### Files to produce

Save all to a single directory: `outputs/stata_handoff_20260527/`.

#### 1. `company_year.dta` (~29 K rows, primary file)

The headline panel. Already exists as CSV: [outputs/sentiment_results/company_sentiment_annual_20260502_155052.csv](outputs/sentiment_results/company_sentiment_annual_20260502_155052.csv). Just needs `.dta` conversion.

**Keys:** `gvkey`, `ticker`, `year`
**Sentiment columns:** `n_posts`, `n_persons`, `n_ai_posts`, `ai_post_share`, `mean_net_sentiment` (LM), `ai_mean_net_sentiment`, FinBERT variants, mean-of-mean variants.
**Quality flags:** `n_strong_match_posts`, `strong_match_share`, `n_unverified_posts`.

This is where John can plot AI-share by ticker × year, spot the firms with implausibly extreme values, or sort by `n_persons` to find under-covered firms.

#### 2. `company_quarterly.dta` (~120 K rows)

Same as #1 but at quarterly resolution. Source: [outputs/sentiment_results/company_sentiment_quarterly_20260427_160543.csv](outputs/sentiment_results/company_sentiment_quarterly_20260427_160543.csv).

For time-series outlier detection — quarters where a firm suddenly spikes or goes silent.

#### 3. `person_year.dta` (~400 K rows, NEW — needs a generation script)

Per-(person, year): a director or executive or blockholder's annual sentiment summary. Doesn't exist yet; needs to be aggregated from the raw posts + sentiment outputs.

**Keys:** `profile_url` (or a stable person ID), `year`
**Suggested columns:**
- `person_name`, `source` (director / executive / blockholder / mixed)
- `n_posts`, `n_ai_posts`, `ai_post_share`
- `mean_lm_sentiment`, `mean_lm_pos`, `mean_lm_neg`
- `mean_ai_lm_sentiment` (LM applied to AI-subset)
- `strong_match` (T/F), `revelio_name_confirmed`, `verified`
- Link cols: `gvkey`, `ticker`, `company_name` (their primary board/company)

**Generation:** join `posts_combined.csv` to the sentiment-scored file (`outputs/sentiment_results/sentiment_all_posts_*.csv`), group by `(profile_url, year)`, aggregate.

#### 4. `person_lifetime.dta` (~26 K rows, NEW)

One row per person — lifetime sentiment summary. Easiest file to scan for outliers ("show me the directors who post about AI most/least").

**Keys:** `profile_url`
**Columns:** same as `person_year` but aggregated across all years; add `first_post_date`, `last_post_date`, `n_years_active`.

### Generation script outline

Put it at `src/data_analysis/build_stata_handoff.py`. Rough structure:

```python
import pandas as pd

# 1. Company-year (existing) -- just to_stata()
df = pd.read_csv("outputs/sentiment_results/company_sentiment_annual_20260502_155052.csv")
df.to_stata("outputs/stata_handoff_20260527/company_year.dta",
            write_index=False, version=118)  # version 118 = Stata 15+

# 2. Company-quarterly -- same
# 3. Person-year -- new aggregation from raw posts + sentiment
posts = pd.read_csv("outputs/sentiment_results/sentiment_all_posts_<latest>.csv",
                    engine="c", lineterminator="\n", on_bad_lines="skip")
# expects columns: profile_url, post_date, is_ai_related, lm_net_sentiment, ...
posts["year"] = pd.to_datetime(posts["post_date"]).dt.year
person_year = (posts.groupby(["profile_url", "year"])
                    .agg(n_posts=("post_text","size"),
                         n_ai=("is_ai_related","sum"),
                         mean_lm=("lm_net_sentiment","mean"))
                    .reset_index())
# join in person attrs from profiles_combined.csv + validation file
person_year.to_stata(".../person_year.dta", write_index=False, version=118)

# 4. Person-lifetime -- groupby profile_url only
```

Stata write tips:
- `version=118` ⇒ Stata 15+ (handles UTF-8 properly); use `version=117` if John is on Stata 13–14.
- Variable names get auto-cleaned for Stata compatibility (≤32 chars, no special chars). Worth running `df.columns = [c.replace(".", "_")[:32] for c in df.columns]` before write.
- For text columns longer than 244 chars use `strL` — but for these aggregated files, no column should hit that.
- Add a brief codebook: write a `README.md` in the same directory listing every column with one-line description.

### Deliverable to John

A zip / shared-drive folder containing:

```
stata_handoff_20260527/
├── README.md                      # codebook + how to load
├── company_year.dta               # primary firm panel
├── company_quarterly.dta          # quarterly firm panel
├── person_year.dta                # per-person annual summary
└── person_lifetime.dta            # per-person lifetime summary
```

Plus a one-page "what to look at first" note: e.g.

> Start with `company_year.dta`. Sort by `ai_post_share` to see who's most AI-loud. Drop rows where `n_posts < 10` for cleaner outlier hunting. Cross-reference against `n_persons` — firms with very few leaders represented are inherently noisier.

---

## TRACK 2 — Continue the DEF 14A strong-match scrape

**Goal:** scrape the remaining 958 DEF 14A strong-match director LinkedIn profiles, validated and prepared per the May 23 workplan.

### Status check (run before kicking off)

```bash
# Confirm the input file is still where we expect
ls -la data/processed/all_people_linkedin_urls/scraped_posts_def14a_continuation_strong_match_clean/apify_input.csv
# Should be ~ 958 rows + header
wc -l data/processed/all_people_linkedin_urls/scraped_posts_def14a_continuation_strong_match_clean/apify_input.csv
```

If both check out, you're good.

### Command

Lifted directly from [WORKPLAN_STATUS_20260523.md:63–68](../../WORKPLAN_STATUS_20260523.md#L63):

```bash
python3 src/data_collection/scrape_posts.py \
  --input  data/processed/all_people_linkedin_urls/scraped_posts_def14a_continuation_strong_match_clean/apify_input.csv \
  --output data/processed/all_people_linkedin_urls/scraped_posts_def14a_continuation_strong_match_clean \
  --max-posts 10000 \
  --run --yes
```

### Expected outputs

After 6–8 hours of Apify processing, the output directory should contain:

```
scraped_posts_def14a_continuation_strong_match_clean/
├── apify_input.csv                  # (already there)
├── posts_<timestamp>.csv            # all scraped posts (~70 K rows expected)
├── profiles_<timestamp>.csv         # per-profile metadata
├── no_posts_profiles_<timestamp>.csv # profiles that returned nothing
└── posts_raw_<timestamp>.json       # raw Apify response
```

Working from the pilot's 73 posts / billable profile and a ~69% yield rate, expect roughly **650–700 profiles to return posts** and **~50–70 K total posts**.

### After the scrape finishes

Three sequential steps, in order:

1. **Merge into the master corpus.** Append the new posts to `posts_combined.csv` (or keep separate and join at analysis time — your choice).
2. **Re-run Revelio validation** on the new URLs to confirm they still strong-match (Tier 2). Per workplan: run the Redivis notebook on the expanded URL set.
3. **Re-run sentiment** on the combined corpus under whatever AI keyword list Tuesday's meeting blessed. Output goes to `outputs/sentiment_results/` with the standard timestamped naming.

Then re-run the regressions (Tobin's Q, ROA, sales growth, stock returns) on the expanded sample — this is the bet from Slide 18: more validated N → less attenuation → cleaner signal under firm FE.

### What you do NOT need to do during the scrape

- Don't watch it. Apify has its own queue and resume logic. The `--yes` flag means it won't pause for confirmation.
- Don't kick off any other Apify job in parallel against the same actor (rate limit / billing entanglement).
- Don't modify `apify_input.csv` mid-run.

### If the scrape fails or stalls

The checkpoint logic in `scrape_posts.py` writes a `.scrape_checkpoint.json` and appends to a JSONL file as it goes (per CLAUDE.md gotchas). Re-running the same command with the same `--input` / `--output` will pick up where it left off.

---

## How the two tracks intersect

They mostly don't — Track 1 is about presenting the *current* (pre-DEF14A-continuation) dataset to John for exploratory analysis. Track 2 is about *adding to* the dataset.

But: once Track 2 finishes and Track 1 has been delivered to John, you'll probably want to **regenerate the Stata handoff** with the expanded data. The build script in Track 1 should be parameterized to take input paths so this is a one-line rerun, not a rebuild.

### Decision points coming back from John

Whatever John flags in his exploration of the Stata files (anomalous firms, suspicious time-series jumps, person-level outliers) feeds back into validation-criteria decisions. Keep a running notes file at `docs/handoffs/john_followups.md` for whatever he sends back.

---

## Open questions still on the table

From Slide 27 of the deck — these remain unresolved until the meeting decisions land:

1. **Continuation scope** — we're proceeding with `strong_match` (958) per default. If meeting greenlit a wider scope (`revelio_matched` = 3,313 or `all` = 4,016), Track 2 needs to swap the input file before launching.
2. **AI keyword vocabulary** — affects which sentiment file we use for Track 1's person-year / person-lifetime aggregations.
3. **Min-posts threshold** — when building person-year, decide whether to drop sparse posters or keep them with a flag. Recommendation: keep all, add `is_active_poster` column (≥10 posts), let John filter himself.
4. **No-post interpretation** — for person-year file, do we emit rows for years a person posted zero times? Recommendation: only emit rows where N ≥ 1 post in that year; missing rows = no posting activity. Document this clearly in the README.

Resolve those four before Track 1 generation actually runs, otherwise we ship John a file shaped by defaults that the team hasn't ratified.

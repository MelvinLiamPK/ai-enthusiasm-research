#!/usr/bin/env python3
"""Task 2: build the corpus ADDITIONS from the backlog-recovered + LM-scored posts, aligned
to the exact existing canonical schemas, ready to append.

Produces two files (dated):
  1. backlog_scored_unique_<ts>.csv  -- one row per recovered post, posts_scored_unique schema
     (finbert columns = NaN; LM-only this round).
  2. backlog_full_coverage_<ts>.csv  -- per-board-duplicated, full_coverage schema, via a
     normalized-URL join to all_linkedin_urls (same multi-board attribution as the corpus).

No re-scoring; the lm_* / is_ai_related come straight from sentiment_analysis_full output.
Strong_match is NOT added here -- aggregate_sentiment merges it (v1 revelio) at aggregation
time, identically for old + new posts (see verify_aggregation_repro gate).
"""
import re
import sys
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
SCORED = sorted((ROOT / "data/processed/backlog_scored").glob("sentiment_all_posts_*.csv"))[-1]
ALU = ROOT / "data/processed/all_people_linkedin_urls/all_linkedin_urls.csv"

# exact existing schemas
SCORED_UNIQUE_COLS = ["profile_url","post_text","post_url","post_type","post_date","post_timestamp","author_name","author_headline","reactions_total","likes","comments","reposts","celebrates","supports","loves","insights","funnys","media_type","article_url","article_title","reshared_text","reshared_url","reshared_author","lm_word_count","lm_positive_count","lm_negative_count","lm_uncertainty_count","lm_litigious_count","lm_constraining_count","lm_positive_ratio","lm_negative_ratio","lm_uncertainty_ratio","lm_net_sentiment","lm_net_ratio","lm_polarity","is_ai_related","is_covid_related","finbert_positive","finbert_negative","finbert_neutral","finbert_sentiment","finbert_score","finbert_positive.1","finbert_negative.1","finbert_neutral.1","finbert_sentiment.1","finbert_score.1"]
FULL_COVERAGE_COLS = ["company_name","person_name","position","source","gvkey","ticker","execid","person_name_clean","company_name_clean","profile_url","post_text","post_url","post_type","post_date","post_timestamp","author_name","author_headline","reactions_total","likes","comments","reposts","celebrates","supports","loves","insights","funnys","media_type","article_url","article_title","reshared_text","reshared_url","reshared_author","lm_word_count","lm_positive_count","lm_negative_count","lm_uncertainty_count","lm_litigious_count","lm_constraining_count","lm_positive_ratio","lm_negative_ratio","lm_uncertainty_ratio","lm_net_sentiment","lm_net_ratio","lm_polarity","is_ai_related","is_covid_related","finbert_positive","finbert_negative","finbert_neutral","finbert_sentiment","finbert_score"]
ALU_META = ["company_name","person_name","position","source","gvkey","ticker","execid","person_name_clean","company_name_clean","linkedin_url"]


def norm(u):
    s = str(u).strip().lower()
    s = re.sub(r"^https?://", "", s); s = re.sub(r"^www\.", "", s)
    return s.split("?")[0].split("#")[0].rstrip("/")


def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"[load] scored new posts: {SCORED.name}", flush=True)
    posts = pd.read_csv(SCORED, low_memory=False, engine="c", lineterminator="\n", on_bad_lines="skip")
    print(f"       {len(posts):,} posts", flush=True)
    posts["_u"] = posts["profile_url"].map(norm)

    # ---- 1. scored_unique additions (per post; finbert NaN) ----
    su = pd.DataFrame(index=posts.index)
    for c in SCORED_UNIQUE_COLS:
        su[c] = posts[c] if c in posts.columns else np.nan
    # dedup to one row per unique post (profile + post_url), index-aligned with posts
    dup_key = posts["_u"].fillna("") + "|" + posts["post_url"].astype(str)
    su = su.loc[~dup_key.duplicated()]
    out_su = ROOT / f"data/processed/backlog_scored_unique_{ts}.csv"
    su.to_csv(out_su, index=False)
    print(f"[scored_unique] {len(su):,} unique posts -> {out_su.name}", flush=True)

    # ---- 2. full_coverage additions (per-board via all_linkedin_urls join) ----
    alu = pd.read_csv(ALU, usecols=[c for c in ALU_META], low_memory=False)
    alu["_u"] = alu["linkedin_url"].map(norm)
    alu = alu.dropna(subset=["_u"])
    # post-content + lm columns to carry from posts (everything except leading new-meta)
    carry = [c for c in FULL_COVERAGE_COLS if c in posts.columns and c not in ALU_META]
    merged = posts[["_u"] + carry].merge(
        alu[["_u","company_name","person_name","position","source","gvkey","ticker","execid","person_name_clean","company_name_clean"]],
        on="_u", how="inner")
    fc = pd.DataFrame(index=merged.index)
    for c in FULL_COVERAGE_COLS:
        fc[c] = merged[c] if c in merged.columns else np.nan
    out_fc = ROOT / f"data/processed/backlog_full_coverage_{ts}.csv"
    fc.to_csv(out_fc, index=False)
    n_match = merged["_u"].nunique()
    print(f"[full_coverage] {len(fc):,} rows (per-board) from {n_match:,} matched profiles -> {out_fc.name}", flush=True)
    print(f"  board-multiplier: {len(fc)/max(len(su),1):.2f}x")
    # report any recovered profiles with NO alu match (would be dropped)
    unmatched = set(posts["_u"].dropna()) - set(alu["_u"])
    print(f"  recovered profiles with NO all_linkedin_urls seat (dropped from full_coverage): {len(unmatched)}")
    open(ROOT / "data/processed/.backlog_additions_ts", "w").write(ts)


if __name__ == "__main__":
    main()

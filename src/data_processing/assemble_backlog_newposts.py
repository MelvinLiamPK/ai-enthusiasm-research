#!/usr/bin/env python3
"""Task 2 step 1 (Sherlock): assemble the combined set of newly-recovered posts from the
Apify backlog re-scrape, deduped, into one CSV ready for LM scoring + metadata join.

Sources (all under data/processed/all_people_linkedin_urls/):
  - full-run  : task1b_full_explicit_empty_<ts>/posts_20260614_220616.csv   (130,804; 991 profiles)
  - probe     : task1b_probe_20260613_173140/posts_*.csv                      (5,715; 62 profiles, cap 1000)
  - truncated : task1b_truncated2/posts_*.csv                                 (3,043; 2 profiles, cap 10000)

Dedup rule: the 2 truncated profiles (jpmello, robert-greaney) appear in the probe at the
1000 cap; use their 10000-cap versions instead. Everything else is disjoint by construction
(probe input_only & probe's random-200 were excluded from the full-run input).
Output: one posts CSV keyed by profile_url with post content (post_text etc.).
"""
import re
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
BASE = ROOT / "data/processed/all_people_linkedin_urls"
TS = open(ROOT / "data/processed/.task1b_full_ts").read().strip()


def norm(u):
    s = str(u).strip().lower()
    s = re.sub(r"^https?://", "", s)
    s = re.sub(r"^www\.", "", s)
    return s.split("?")[0].split("#")[0].rstrip("/")


def newest(globpat, base):
    fs = sorted(Path(base).glob(globpat))
    return fs[-1] if fs else None


def main():
    full = pd.read_csv(BASE / f"task1b_full_explicit_empty_{TS}" / "posts_20260614_220616.csv",
                       engine="c", lineterminator="\n", on_bad_lines="skip", low_memory=False)
    probe = pd.read_csv(newest("posts_2*.csv", BASE / "task1b_probe_20260613_173140"),
                        engine="c", lineterminator="\n", on_bad_lines="skip", low_memory=False)
    trunc = pd.read_csv(newest("posts_2*.csv", BASE / "task1b_truncated2"),
                        engine="c", lineterminator="\n", on_bad_lines="skip", low_memory=False)

    # identify the post-content columns common to all (profile_url onward)
    content_cols = [c for c in full.columns if c in probe.columns]
    # the 2 truncated profile_urls
    trunc_urls = {norm(u) for u in trunc["profile_url"].dropna().unique()}
    # drop truncated profiles from probe (use 10k version instead)
    probe_keep = probe[~probe["profile_url"].map(norm).isin(trunc_urls)]

    def keep(df):
        cols = [c for c in df.columns if c in content_cols or c == "profile_url" or c.startswith(("post_", "reshared_", "article_", "media_", "author_", "reactions", "likes", "comments", "reposts", "celebrates", "supports", "loves", "insights", "funnys"))]
        return df[[c for c in df.columns if c in set(cols)]]

    # align to full's content schema
    base_cols = [c for c in full.columns]
    def align(df):
        for c in base_cols:
            if c not in df.columns:
                df[c] = pd.NA
        return df[base_cols]

    combined = pd.concat([align(full), align(probe_keep), align(trunc)], ignore_index=True)
    # final dedup on (profile_url, post_url) keeping first
    key = combined["profile_url"].map(norm).fillna("") + "|" + combined.get("post_url", pd.Series([""] * len(combined))).astype(str)
    combined = combined[~key.duplicated()]

    out = ROOT / f"data/processed/backlog_newposts_{TS}.csv"
    combined.to_csv(out, index=False)
    n_prof = combined["profile_url"].map(norm).nunique()
    print(f"combined new posts: {len(combined):,} rows | distinct profiles: {n_prof:,}")
    print(f"  full={len(full):,}  probe_kept={len(probe_keep):,}  trunc={len(trunc):,}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()

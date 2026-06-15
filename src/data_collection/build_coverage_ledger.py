#!/usr/bin/env python3
"""
Task 0 (Chat 6): build the person-level COVERAGE LEDGER and resolve the true Apify
backlog (Deliverables A + B). Runs LOCALLY; consumes the authoritative WRDS Apify
submission sets extracted on Sherlock (data/processed/coverage_audit_sherlock/).

Backbone: revelio_validation_summary_v2.csv — this file already unifies the WRDS
universe (96,971) + the def14a_serper rows (5,353) with person/source/url/crosscheck/
strong_match columns. We add the two missing stages: apify_submitted and has_posts.

Join discipline: every URL join is on norm_url() applied to BOTH sides. The person key
is normalize_name() (project normalizer). No raw-string matching.
"""
from __future__ import annotations
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src" / "revelio"))
from normalize_names import normalize_name  # noqa: E402

BASE = ROOT / "data/processed/all_people_linkedin_urls"
AUDIT = ROOT / "data/processed/coverage_audit_sherlock"

_SCHEME = re.compile(r"^https?://", re.I)
_WWW = re.compile(r"^www\.", re.I)


def norm_url(u) -> str | None:
    if u is None or (isinstance(u, float) and np.isnan(u)):
        return None
    s = str(u).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    s = _SCHEME.sub("", s)
    s = _WWW.sub("", s)
    s = s.split("?")[0].split("#")[0]
    if "/posts/" in s:
        parts = s.split("/posts/")
        if len(parts) > 1 and parts[1]:
            dom = parts[0].split("/")[0]
            username = parts[1].split("/")[0].split("-")[0]
            s = f"{dom}/in/{username}"
    s = s.rstrip("/").lower()
    return s or None


def load_url_set_from_txt(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with open(path, encoding="utf-8", errors="replace") as fh:
        return {n for n in (norm_url(line) for line in fh) if n}


def load_url_set_from_csv(path: Path, col: str) -> set[str]:
    if not path.exists():
        return set()
    df = pd.read_csv(path, usecols=lambda c: c == col, low_memory=False)
    if col not in df.columns:
        return set()
    return {n for n in (norm_url(v) for v in df[col]) if n}


def main() -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ---------- the two sets we add ----------
    # SUBMITTED = WRDS (Sherlock) ∪ def14a apify inputs (local) ∪ score70plus (local)
    submitted = load_url_set_from_txt(AUDIT / "wrds_submitted_urls.txt")
    n_wrds_sub = len(submitted)
    def14a_dirs = [
        "scraped_posts_def14a_pilot_clean",
        "scraped_posts_def14a_continuation_revelio_matched",
        "scraped_posts_def14a_continuation_strong_match_clean",
        "scraped_posts_def14a_continuation_all",
    ]
    def14a_submitted: set[str] = set()
    for d in def14a_dirs:
        def14a_submitted |= load_url_set_from_csv(BASE / d / "apify_input.csv", "linkedin_url")
    score70 = load_url_set_from_txt(ROOT / "outputs/apify_inputs/linkedin_urls_score70plus_20260128_221236.txt")
    submitted |= def14a_submitted | score70

    # explicit submitted-and-empty (no_posts evidence): WRDS (Sherlock) + def14a no_posts
    no_posts_evidence = load_url_set_from_txt(AUDIT / "wrds_no_posts_urls.txt")
    for d in def14a_dirs:
        for f in (BASE / d).glob("no_posts_profiles_*.csv"):
            no_posts_evidence |= load_url_set_from_csv(f, "profile_url")
    submitted |= no_posts_evidence  # any empty-evidence URL was, by definition, submitted

    # HAS_POSTS authority = merged v2 corpus
    prof = pd.read_csv(BASE / "scraped_posts_combined/profiles_combined_v2_20260527.csv", low_memory=False)
    has_posts = {n for n in (norm_url(v) for v in prof["profile_url"]) if n}

    print(f"[sets] submitted={len(submitted):,} (wrds={n_wrds_sub:,}, def14a={len(def14a_submitted):,}, "
          f"score70={len(score70):,}) | no_posts_evidence={len(no_posts_evidence):,} | has_posts={len(has_posts):,}")

    # ---------- backbone ledger ----------
    rev = pd.read_csv(ROOT / "data/revelio/revelio_validation_summary_v2.csv", low_memory=False)
    rev["person_key"] = rev["person_name"].apply(normalize_name)
    rev["linkedin_url_norm"] = rev["linkedin_url"].apply(norm_url)
    rev["url_found"] = rev["linkedin_url_norm"].notna()
    rev["searched"] = True  # every summary row went through a search
    rev["revelio_crosschecked"] = rev["url_found"]  # crosscheck needs a URL
    for c in ("revelio_url_match", "strong_match_either", "strong_match_board", "def14a_only"):
        rev[c] = rev[c].fillna(False).astype(bool)
    rev["apify_submitted"] = rev["linkedin_url_norm"].isin(submitted) & rev["url_found"]
    rev["has_posts"] = rev["linkedin_url_norm"].isin(has_posts) & rev["url_found"]
    rev["is_wrds"] = rev["source"] != "def14a_serper"

    ledger_cols = [
        "person_key", "person_name", "source", "is_wrds", "def14a_only", "gvkey", "ticker",
        "searched", "url_found", "linkedin_url_norm", "revelio_crosschecked",
        "revelio_url_match", "strong_match_board", "strong_match_either",
        "apify_submitted", "has_posts",
    ]
    ledger = rev[ledger_cols].copy()
    out_csv = ROOT / f"data/processed/coverage_ledger_{ts}.csv"
    ledger.to_csv(out_csv, index=False)
    print(f"[ledger] wrote {out_csv}  rows={len(ledger):,}")

    # ===================== DELIVERABLE A — coverage certification =====================
    def pct(n, d):
        return f"{n:,}/{d:,} ({100*n/d:5.1f}%)" if d else "n/a"

    print("\n" + "=" * 78 + "\nDELIVERABLE A — COVERAGE CERTIFICATION\n" + "=" * 78)
    for label, sub in (("WRDS universe", rev[rev["is_wrds"]]),
                       ("def14a_serper rows (net-new to summary)", rev[~rev["is_wrds"]])):
        d = len(sub)
        print(f"\n[{label}]  n_rows={d:,}")
        print(f"  searched          : {pct(int(sub['searched'].sum()), d)}")
        print(f"  url_found         : {pct(int(sub['url_found'].sum()), d)}")
        print(f"  revelio_crosschckd: {pct(int(sub['revelio_crosschecked'].sum()), d)}")
        print(f"  revelio_url_match : {pct(int(sub['revelio_url_match'].sum()), d)}")
        print(f"  strong_match      : {pct(int(sub['strong_match_either'].sum()), d)}")
        print(f"  apify_submitted   : {pct(int(sub['apify_submitted'].sum()), d)}")
        print(f"  has_posts(scraped): {pct(int(sub['has_posts'].sum()), d)}")

    # ===================== DELIVERABLE B — 3-way split of strong-match URLs ============
    sm = rev[rev["strong_match_either"]].copy()
    sm_urls = sm.dropna(subset=["linkedin_url_norm"]).drop_duplicates("linkedin_url_norm")
    n_sm = len(sm_urls)
    hp = sm_urls["has_posts"].sum()
    sub_empty = (sm_urls["apify_submitted"] & ~sm_urls["has_posts"]).sum()
    never = (~sm_urls["apify_submitted"] & ~sm_urls["has_posts"]).sum()
    print("\n" + "=" * 78 + "\nDELIVERABLE B — TRUE BACKLOG (3-way split of distinct strong-match URLs)\n" + "=" * 78)
    print(f"  distinct strong-match URLs         : {n_sm:,}")
    print(f"  (1) has_posts                      : {pct(int(hp), n_sm)}")
    print(f"  (2) submitted & ~has_posts (empty) : {pct(int(sub_empty), n_sm)}")
    print(f"  (3) ~submitted & ~has_posts (NEVER): {pct(int(never), n_sm)}   <-- fundable backlog")

    # never-submitted breakdown by source
    nv = sm_urls[~sm_urls["apify_submitted"] & ~sm_urls["has_posts"]]
    print("\n  never-submitted by source:")
    for s, c in nv["source"].value_counts().items():
        print(f"    {s:32s} {c:,}")
    print(f"    def14a_only=True among never-submitted: {int(nv['def14a_only'].sum()):,}")

    # also report the submitted-empty set size (Task-1b universe)
    se = sm_urls[sm_urls["apify_submitted"] & ~sm_urls["has_posts"]]
    print(f"\n  Task-1b universe (submitted-empty strong matches): {len(se):,}")

    # write the never-submitted scrape target list
    target = nv[["person_key", "person_name", "source", "ticker", "gvkey", "linkedin_url_norm"]].copy()
    target_csv = ROOT / f"data/processed/scrape_target_never_submitted_{ts}.csv"
    target.to_csv(target_csv, index=False)
    print(f"\n[target] never-submitted strong-match scrape list -> {target_csv} ({len(target):,} rows)")
    se_csv = ROOT / f"data/processed/task1b_submitted_empty_{ts}.csv"
    se[["person_key", "person_name", "source", "ticker", "gvkey", "linkedin_url_norm"]].to_csv(se_csv, index=False)
    print(f"[task1b] submitted-empty list -> {se_csv} ({len(se):,} rows)")


if __name__ == "__main__":
    main()

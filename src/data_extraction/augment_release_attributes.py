#!/usr/bin/env python3
"""
Augment the 2026-06-05_def14a release IN PLACE with director attributes already
recoverable from data on disk (no API, no re-scrape):

  - gender + board_leadership_role  (from def14a_bio_features_<stamp>.csv, this chat)
  - primary_company                 (the full-scale DEF 14A primary-employer extraction
                                      that survives in revelio_validation_rows_v2.csv;
                                      ticker-fallback rows where primary==ticker dropped)
  - committees / independent / financial-expert flags (bio-only, low coverage — carried
    as the new def14a_director_attributes.csv keyed (ticker,cik,year,full_name))

primary_TITLE is NOT available at scale (it did not survive the URL-pipeline merge; only
the 128-row pilot bios file has it). Only primary_company is delivered broadly.

Edits the existing release; does NOT touch `current`. Run once.
"""
from __future__ import annotations
import json, os, re, subprocess
from datetime import date
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
PROC = ROOT / "data" / "processed"
RELEASE = ROOT / "data" / "canonical" / "releases" / "2026-06-05_def14a"
STAMP = date.today().isoformat().replace("-", "")

BIO_FEATURES = PROC / "def14a_bio_features_20260605.csv"
REVELIO = ROOT / "data" / "revelio" / "revelio_validation_rows_v2.csv"
NN_CSV = PROC / "def14a_new_nominee_tenure_20260605.csv"

_SUFFIX = re.compile(r"\b(jr|sr|ii|iii|iv|v|dr|mr|mrs|ms|phd|md|esq)\b")
_PAREN = re.compile(r"\(.*?\)")
def norm_name(s):
    s = _PAREN.sub(" ", str(s).lower()); s = re.sub(r"[.,'\"]", " ", s)
    s = _SUFFIX.sub(" ", s); return re.sub(r"\s+", " ", s).strip()
def clean_url(u):
    if u is None or (isinstance(u, float) and pd.isna(u)): return None
    s = str(u).lower().strip(); s = re.sub(r"^https?://", "", s)
    m = re.search(r"linkedin\.com/in/[^/?#\s]+", s); return m.group(0) if m else None

def modal(s):
    s = s.dropna()
    return s.mode().iloc[0] if len(s.mode()) else (s.iloc[0] if len(s) else np.nan)

def main():
    print("Augmenting", RELEASE.name)

    # ---- 1. primary_company map (drop ticker-fallbacks) ----
    rv = pd.read_csv(REVELIO, low_memory=False)
    p = rv[rv["primary_company"].notna()].copy()
    p = p[p["primary_company"].astype(str).str.upper() != p["ticker"].astype(str).str.upper()]
    p["nk"] = p["person_name"].map(norm_name) + "|" + p["ticker"].astype(str).str.upper()
    prim = p.groupby("nk")["primary_company"].agg(modal)
    print(f"  primary_company: {len(prim):,} (person×firm) real employers "
          f"({p['person_name'].nunique():,} people, {p['ticker'].nunique():,} tickers)")

    # ---- 2. gender + leadership per (norm_name, ticker) ----
    bf = pd.read_csv(BIO_FEATURES)
    bf["nk"] = bf["full_name"].map(norm_name) + "|" + bf["ticker"].astype(str).str.upper()
    gmap = bf.groupby("nk")["gender"].agg(modal)
    lmap = bf.groupby("nk")["board_leadership_role"].agg(lambda s: s.dropna().iloc[0] if s.notna().any() else np.nan)

    # ---- 3. add to new_nominee_tenure (person×firm) ----
    nn = pd.read_csv(NN_CSV)
    nn["nk"] = nn["full_name"].map(norm_name) + "|" + nn["ticker"].astype(str).str.upper()
    nn["gender"] = nn["nk"].map(gmap)
    nn["board_leadership_role"] = nn["nk"].map(lmap)
    nn["primary_company"] = nn["nk"].map(prim)
    nn = nn.drop(columns=["nk"])
    nn.to_csv(NN_CSV, index=False)
    print(f"  new_nominee_tenure: +gender ({nn['gender'].notna().sum():,}) "
          f"+primary_company ({nn['primary_company'].notna().sum():,}) "
          f"+leadership ({nn['board_leadership_role'].notna().sum():,})")

    # person/(profile_url,ticker) and person/url lookups via the bridge in nn
    nn["uk"] = nn["profile_url"].map(clean_url)
    nn_u = nn.dropna(subset=["uk"])
    g_by_url = nn_u.groupby("uk")["gender"].agg(modal)
    pc_by_urltk = nn_u.dropna(subset=["primary_company"]).set_index(
        [nn_u.dropna(subset=["primary_company"])["uk"], nn_u.dropna(subset=["primary_company"])["ticker"].str.upper()]
    )["primary_company"]
    pc_by_urltk = pc_by_urltk[~pc_by_urltk.index.duplicated()]

    # ---- 4. enrich person panels ----
    py = pd.read_stata(RELEASE / "person_year.dta")
    py["uk"] = py["profile_url"].map(clean_url)
    py["tk"] = py["ticker"].astype(str).str.upper()
    py["def14a_gender"] = py["uk"].map(g_by_url)
    idx = list(zip(py["uk"], py["tk"]))
    py["def14a_primary_company"] = [pc_by_urltk.get(k, np.nan) if k in pc_by_urltk.index else np.nan for k in idx]
    py = py.drop(columns=["uk", "tk"])
    py.to_stata(RELEASE / "person_year.dta", write_index=False, version=118)
    print(f"  person_year: +def14a_gender ({py['def14a_gender'].notna().sum():,}) "
          f"+def14a_primary_company ({py['def14a_primary_company'].notna().sum():,})")

    pl = pd.read_stata(RELEASE / "person_lifetime.dta")
    pl["uk"] = pl["profile_url"].map(clean_url)
    pl["def14a_gender"] = pl["uk"].map(g_by_url)
    pc_by_url = nn_u.dropna(subset=["primary_company"]).groupby("uk")["primary_company"].agg(modal)
    pl["def14a_primary_company"] = pl["uk"].map(pc_by_url)
    pl = pl.drop(columns=["uk"])
    pl.to_stata(RELEASE / "person_lifetime.dta", write_index=False, version=118)
    print(f"  person_lifetime: +def14a_gender ({pl['def14a_gender'].notna().sum():,}) "
          f"+def14a_primary_company ({pl['def14a_primary_company'].notna().sum():,})")

    # ---- 5. new attributes file (per filing-year) -> processed + symlink in release ----
    attr_path = PROC / f"def14a_director_attributes_{STAMP}.csv"
    bf.drop(columns=["nk"]).to_csv(attr_path, index=False)
    link = RELEASE / "def14a_director_attributes.csv"
    if link.is_symlink() or link.exists():
        link.unlink()
    link.symlink_to(os.path.relpath(attr_path.resolve(), RELEASE))
    print(f"  def14a_director_attributes.csv: {len(bf):,} rows (gender/leadership/committees/independence)")

    # ---- 6. update MANIFEST ----
    mpath = RELEASE / "MANIFEST.json"
    m = json.load(open(mpath))
    git_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    m["augmented_" + date.today().isoformat()] = {
        "git_sha": git_sha,
        "added": {
            "def14a_director_attributes.csv": {"rows": len(bf),
                "cols": "gender, gender_source, board_leadership_role, independent, n_committees, committees, committee_chair, audit_financial_expert, n_other_public_directorships",
                "coverage": "gender 91% (99.8% vs Execucomp); leadership 23%; committees/independence bio-only ~10-22% (table-bound, LLM-liftable)"},
            "new_nominee_tenure +cols": "gender, board_leadership_role, primary_company",
            "person_year/person_lifetime +cols": "def14a_gender, def14a_primary_company",
        },
        "primary_company_source": "Recovered from revelio_validation_rows_v2.csv (the full-scale DEF14A primary-employer extraction, embedded in the URL/Revelio datasets); ticker-fallback rows dropped. primary_TITLE not available at scale.",
    }
    json.dump(m, open(mpath, "w"), indent=2)
    print("  MANIFEST updated. current still ->", os.readlink(ROOT / "data/canonical/current"))

if __name__ == "__main__":
    main()

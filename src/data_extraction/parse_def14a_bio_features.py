#!/usr/bin/env python3
"""
Parse extra director features from the stored `def14a_bio_text` (no API, no re-scrape).

Deterministic extraction of fields that sit in the bio PROSE (high coverage) vs the
summary TABLE (low bio coverage — flagged; an LLM pass would lift these like it did age):

  PROSE-reliable:  gender, board_leadership_role
  TABLE-bound:     independent, committee_chair / n_committees, audit_financial_expert,
                   n_other_public_directorships

Output: data/processed/def14a_bio_features_<stamp>.csv keyed (ticker, cik, year, full_name)
— joins 1:1 onto def14a_director_status.csv. NOT folded into a release here; owner decides.

Run: python3 src/data_extraction/parse_def14a_bio_features.py
"""
from __future__ import annotations
import re
from datetime import date
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
STATUS = ROOT / "data" / "processed" / "def14a_director_status_20260528.csv"
OUT = ROOT / "data" / "processed" / f"def14a_bio_features_{date.today().isoformat().replace('-','')}.csv"

# --- gender (honorific first, pronoun fallback) -------------------------------
def gender(bio: str):
    if re.search(r"\bMrs?\b\.?\s+[A-Z]|\bMr\.\s+[A-Z]", bio):  # Mr. / Mr
        if re.search(r"\bMr\.\s+[A-Z]", bio):
            return "M", "honorific"
    if re.search(r"\bM(?:s|rs|iss)\.?\s+[A-Z]", bio):
        return "F", "honorific"
    if re.search(r"\bMr\.?\s+[A-Z]", bio):
        return "M", "honorific"
    he = len(re.findall(r"\b(he|his|him)\b", bio, re.I))
    she = len(re.findall(r"\b(she|her|hers)\b", bio, re.I))
    if he or she:
        if he >= 2 * max(she, 1) and he > she:
            return "M", "pronoun"
        if she >= 2 * max(he, 1) and she > he:
            return "F", "pronoun"
    return np.nan, ""

# --- board leadership role ----------------------------------------------------
def leadership(bio: str):
    b = bio
    if re.search(r"lead\s+independent\s+director", b, re.I):
        return "lead_independent_director"
    if re.search(r"(chair(man|person|woman)?)\s+(and\s+\w+\s+)?of\s+(the|our)\s+board|board\s+chair|chair(man|person|woman)?\s+of\s+the\s+board\s+of\s+directors", b, re.I):
        return "board_chair"
    if re.search(r"vice\s+chair(man|person|woman)?\s+of\s+(the|our)\s+board", b, re.I):
        return "vice_chair"
    if re.search(r"\blead\s+director\b", b, re.I):
        return "lead_director"
    return np.nan

# --- independence (TABLE-bound, conservative) ---------------------------------
def independent(bio: str):
    if re.search(r"\|\s*independent\b|is\s+(?:an?\s+)?independent\s+director|independent\s+director\b|deemed\s+independent|qualifies?\s+as\s+independent|determined\s+to\s+be\s+independent", bio, re.I):
        return True
    if re.search(r"\bnot\s+(?:deemed\s+)?independent|non-?independent", bio, re.I):
        return False
    return np.nan

# --- committees ---------------------------------------------------------------
COMMITTEES = ["audit", "compensation", "nominating", "governance", "finance",
              "risk", "executive", "technology", "science", "sustainability",
              "compliance", "investment", "human resources", "cybersecurity"]
def committees(bio: str):
    # restrict to a committees section if present, else whole bio
    m = re.search(r"committees?\s*:?\s*(.{0,400})", bio, re.I)
    region = m.group(1) if m else bio
    found = set()
    chair = False
    for c in COMMITTEES:
        if re.search(rf"\b{re.escape(c)}\b", region, re.I) and re.search(rf"\b{re.escape(c)}\b.{{0,20}}committee|committee.{{0,20}}\b{re.escape(c)}\b", bio, re.I):
            found.add(c)
    if re.search(r"committee\s*\(?\s*chair|chair\s+of\s+(?:the\s+)?\w[\w\s]{0,30}?committee|\(chair\)", bio, re.I):
        chair = True
    is_fin_expert = bool(re.search(r"financial\s+expert", bio, re.I))
    return len(found), (";".join(sorted(found)) if found else np.nan), chair, is_fin_expert

# --- other public directorships ----------------------------------------------
def n_other_directorships(bio: str):
    m = re.search(r"other\s+(?:public\s+)?(?:company\s+)?directorships?[^:]{0,20}:\s*(.{0,400})", bio, re.I)
    if not m:
        return np.nan
    seg = m.group(1)
    seg = re.split(r"former|past\s+five\s+years|qualification|committee", seg, 1, re.I)[0]
    if re.search(r"\bnone\b", seg, re.I):
        return 0
    items = [x for x in re.split(r"[•·•\n;]|,\s+(?=[A-Z])", seg) if len(x.strip()) > 3]
    return min(len(items), 12) if items else np.nan

def main():
    ds = pd.read_csv(STATUS)
    d = ds[ds["is_director"] == True].copy()
    bio = d["def14a_bio_text"].fillna("")
    N = len(d)

    g = bio.map(gender)
    d["gender"] = g.map(lambda x: x[0]); d["gender_source"] = g.map(lambda x: x[1])
    d["board_leadership_role"] = bio.map(leadership)
    d["independent"] = bio.map(independent)
    com = bio.map(committees)
    d["n_committees"] = com.map(lambda x: x[0]).replace(0, np.nan)
    d["committees"] = com.map(lambda x: x[1])
    d["committee_chair"] = com.map(lambda x: x[2])
    d["audit_financial_expert"] = com.map(lambda x: x[3])
    d["n_other_public_directorships"] = bio.map(n_other_directorships)

    cols = ["ticker", "cik", "year", "full_name", "gender", "gender_source",
            "board_leadership_role", "independent", "n_committees", "committees",
            "committee_chair", "audit_financial_expert", "n_other_public_directorships"]
    out = d[cols]
    out.to_csv(OUT, index=False)

    def cov(c):
        s = out[c]
        nn = s.notna() if s.dtype != bool else s
        return f"{int(nn.sum()):>6,} ({nn.mean()*100:4.1f}%)"
    print(f"director rows: {N:,}   ->  {OUT.name}\n")
    print("PROSE-reliable:")
    print(f"  gender                        : {cov('gender')}   "
          f"(M {int((out['gender']=='M').sum()):,} / F {int((out['gender']=='F').sum()):,}; "
          f"{(out['gender_source']=='honorific').mean()*100:.0f}% via honorific)")
    print(f"  board_leadership_role         : {cov('board_leadership_role')}   "
          f"{out['board_leadership_role'].value_counts().to_dict()}")
    print("\nTABLE-bound (bio-only coverage; LLM-liftable like age):")
    for c in ["independent", "n_committees", "committee_chair",
              "audit_financial_expert", "n_other_public_directorships"]:
        print(f"  {c:30s}: {cov(c)}")

if __name__ == "__main__":
    main()

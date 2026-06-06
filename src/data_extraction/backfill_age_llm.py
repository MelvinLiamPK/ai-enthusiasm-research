#!/usr/bin/env python3
"""
Targeted LLM age backfill — recover director age for the ~21% of DEF 14A director
rows that the HTML table-parse ∪ regex pass missed (build_def14a_merge_release.py).

Reads the age cache (data/processed/def14a_ages_extracted.csv), finds is_director
rows with NO age, and for each involved filing asks Claude Haiku 4.5 for ONLY
name->age (tiny output = cheap). Fills age with age_source='llm', writes the cache
back. Then re-run:
    python3 src/data_extraction/build_def14a_merge_release.py --skip-age
to rebuild the release at the higher coverage.

Reuses the proven roster-window selection from classify_def14a_director_status.py.

Run:
    python3 src/data_extraction/backfill_age_llm.py --limit 50    # validation batch
    python3 src/data_extraction/backfill_age_llm.py               # full residual
"""
from __future__ import annotations
import argparse, glob, gzip, os, re, threading, time
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
import anthropic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(ROOT / ".env")
PROC = ROOT / "data" / "processed"
RAW = ROOT / "data" / "raw" / "def14a"
STATUS = PROC / "def14a_director_status_20260528.csv"
AGE_CACHE = PROC / "def14a_ages_extracted.csv"

MODEL = "claude-haiku-4-5"
BIO_WINDOW_CHARS = 48_000
BIO_WINDOW_STEP = 8_000
MIN_COMPLETE_DIRECTOR_SIGNAL = 8
MIN_SECOND_WINDOW_DIRECTOR_SIGNAL = 4
PRICE_IN, PRICE_OUT = 1.0 / 1e6, 5.0 / 1e6
PRICE_CW, PRICE_CR = 1.25 / 1e6, 0.10 / 1e6

# --- roster windowing (copied from classify_def14a_director_status.py) ---------
DIRECTOR_SIGNAL = re.compile(
    r"director\s+since|nominee[s]?\s+for\s+(?:election\s+as\s+)?director|"
    r"for\s+election\s+as\s+(?:a\s+)?director|first\s+nominated|"
    r"not\s+currently\s+serving|standing\s+for\s+(?:re-?)?election|"
    r"has\s+served\s+as\s+a\s+director|joined\s+(?:our|the)\s+board|"
    r"nominees?\s+for\s+the\s+board", re.IGNORECASE)
BIO_SIGNAL = re.compile(
    r"\bdirector\s+since\s+\d{4}\b|\bsince\s+\d{4}\b|\bage[:\s]+\d{2}\b|"
    r"\bcommittee\s+(?:chair|member)s?\b|\bpreviously\s+(?:served|was|held)\b|"
    r"\bcurrently\s+(?:serves?|is)\b|\bjoined\s+(?:our|the)\s+board\b|"
    r"\b(?:Mr|Ms|Mrs|Dr)\.\s+[A-Z]", re.IGNORECASE)

def _score(w): return 3 * len(DIRECTOR_SIGNAL.findall(w)) + len(BIO_SIGNAL.findall(w))

def narrow_to_bios(text: str) -> str:
    if len(text) <= BIO_WINDOW_CHARS:
        return text
    last = len(text) - BIO_WINDOW_CHARS
    scored = []
    for s in range(0, last + BIO_WINDOW_STEP, BIO_WINDOW_STEP):
        s = min(s, last); scored.append((_score(text[s:s + BIO_WINDOW_CHARS]), s))
    scored.sort(reverse=True)
    best = scored[0][1]; bw = text[best:best + BIO_WINDOW_CHARS]
    if len(DIRECTOR_SIGNAL.findall(bw)) >= MIN_COMPLETE_DIRECTOR_SIGNAL:
        return bw
    second = None
    for _, s in scored[1:]:
        if abs(s - best) < BIO_WINDOW_CHARS:
            continue
        if len(DIRECTOR_SIGNAL.findall(text[s:s + BIO_WINDOW_CHARS])) >= MIN_SECOND_WINDOW_DIRECTOR_SIGNAL:
            second = s; break
    if second is None:
        return bw
    a, b = sorted([best, second])
    return text[a:a + BIO_WINDOW_CHARS] + "\n...\n" + text[b:b + BIO_WINDOW_CHARS]

def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style"]):
        t.decompose()
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in soup.get_text(separator="\n").splitlines()]
    return "\n".join(ln for ln in lines if ln)

def read_html(cik, year):
    paths = glob.glob(str(RAW / f"{cik}_{year}.html*"))
    if not paths:
        return None
    p = paths[0]
    return gzip.open(p, "rt", errors="replace").read() if p.endswith(".gz") else open(p, errors="replace").read()

# --- name normalization (mirrors the build) -----------------------------------
_SUFFIX = re.compile(r"\b(jr|sr|ii|iii|iv|v|dr|mr|mrs|ms|phd|md|esq)\b")
_PAREN = re.compile(r"\(.*?\)")
def norm_name(s):
    s = _PAREN.sub(" ", str(s).lower()); s = re.sub(r"[.,'\"]", " ", s)
    s = _SUFFIX.sub(" ", s); return re.sub(r"\s+", " ", s).strip()
def keys(name):
    n = norm_name(name); k = [n]; t = n.split()
    if len(t) >= 2: k.append(f"{t[0][0]}|{t[-1]}")
    return k

# --- LLM schema + call --------------------------------------------------------
class Person(BaseModel):
    full_name: str = Field(description="person's full name exactly as written in the filing")
    age: Optional[int] = Field(default=None, description="age as an integer if the filing states it, else null")
class AgeList(BaseModel):
    people: list[Person]

SYS = (
    "You extract director and officer AGES from a SEC DEF 14A proxy excerpt. For "
    "EVERY person whose biography or the director-summary table appears in the text, "
    "return their full_name exactly as written and their age as an integer if the "
    "filing states it — e.g. 'Mr. Smith, 54,', 'Age: 54', or an 'Age' column in the "
    "director roster table. Use null when the age is not stated. NEVER infer, "
    "estimate, or guess an age; only copy an explicitly stated number."
)

def call(client, text):
    r = client.messages.parse(
        model=MODEL, max_tokens=4000,
        system=[{"type": "text", "text": SYS, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content":
            "Extract every person's name and stated age from this DEF 14A excerpt. "
            "Return JSON with a 'people' array.\n\n=== FILING TEXT ===\n" + text}],
        output_format=AgeList)
    return r.parsed_output, r

_lock = threading.Lock()
_usage = {"in": 0, "out": 0, "cw": 0, "cr": 0, "n": 0, "err": 0}

def process_filing(client, cik, year):
    """Return {full_key: age} for ages found, or None on failure."""
    html = read_html(cik, year)
    if html is None:
        return None
    text = narrow_to_bios(html_to_text(html))
    for attempt in range(4):
        try:
            res, resp = call(client, text)
            u = resp.usage
            with _lock:
                _usage["in"] += u.input_tokens or 0
                _usage["out"] += u.output_tokens or 0
                _usage["cw"] += getattr(u, "cache_creation_input_tokens", 0) or 0
                _usage["cr"] += getattr(u, "cache_read_input_tokens", 0) or 0
                _usage["n"] += 1
            out = {}
            for p in (res.people if res else []):
                if p.age is not None and 25 <= p.age <= 99:
                    for k in keys(p.full_name):
                        out.setdefault(k, p.age)
            return out
        except Exception as e:
            if attempt == 3:
                with _lock:
                    _usage["err"] += 1
                return None
            time.sleep(2 ** attempt)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="process only N filings (validation)")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    status = pd.read_csv(STATUS)
    cache = pd.read_csv(AGE_CACHE, index_col=0)
    dirs = status[status["is_director"] == True]
    n_dir = len(dirs)
    before = int(cache.loc[cache.index.isin(dirs.index), "age"].notna().sum())
    print(f"director rows: {n_dir:,} | age before: {before:,} ({before/n_dir*100:.1f}%)")

    residual = cache[cache["age"].isna() & cache.index.isin(dirs.index)]
    meta = status.loc[residual.index, ["cik", "year", "full_name"]]
    # map (cik,year) -> {full_key/initial_key: [status_index,...]}
    filings = {}
    for idx, r in meta.iterrows():
        key = (int(r["cik"]), int(r["year"]))
        filings.setdefault(key, []).append((idx, r["full_name"]))
    print(f"residual rows: {len(residual):,} across {len(filings):,} filings")

    items = list(filings.items())
    if args.limit:
        items = items[:args.limit]
        print(f"LIMIT: processing first {len(items)} filings")
    if args.dry_run:
        print("dry-run: no API calls"); return

    client = anthropic.Anthropic()
    filled = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(process_filing, client, cik, yr): (cik, yr)
                for (cik, yr) in [k for k, _ in items]}
        done = 0
        for fut in as_completed(futs):
            cik, yr = futs[fut]
            ages = fut.result() or {}
            for idx, full_name in filings[(cik, yr)]:
                age = None
                for k in keys(full_name):
                    if k in ages:
                        age = ages[k]; break
                if age is not None:
                    cache.loc[idx, "age"] = float(age)
                    cache.loc[idx, "age_source"] = "llm"
                    filled += 1
            done += 1
            if done % 200 == 0:
                u = _usage
                cost = (u["in"]*PRICE_IN + u["out"]*PRICE_OUT + u["cw"]*PRICE_CW + u["cr"]*PRICE_CR)
                print(f"  {done}/{len(items)} filings | filled {filled} | ${cost:.2f} | {time.time()-t0:.0f}s")

    cache.to_csv(AGE_CACHE)
    u = _usage
    cost = (u["in"]*PRICE_IN + u["out"]*PRICE_OUT + u["cw"]*PRICE_CW + u["cr"]*PRICE_CR)
    after = int(cache.loc[cache.index.isin(dirs.index), "age"].notna().sum())
    print(f"\nDONE. filled {filled:,} new ages | errors {u['err']}")
    print(f"age now: {after:,}/{n_dir:,} = {after/n_dir*100:.1f}%  (was {before/n_dir*100:.1f}%)")
    print(f"tokens in={u['in']:,} out={u['out']:,} cache_r={u['cr']:,} | COST ${cost:.2f}")
    print(f"cache written -> {AGE_CACHE.name}.  Next: rebuild with --skip-age.")

if __name__ == "__main__":
    main()

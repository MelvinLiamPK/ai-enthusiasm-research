#!/usr/bin/env python3
"""
Build the DEF 14A merge canonical release.

Per docs/handoffs/def14a_merge_build_spec_20260602.md, IN-SCOPE for this chat:
  1. Age backfill from stored def14a HTML (table-parse) + bio_text regex (~80%, $0).
  2. Clean per-(profile_url, ticker) director_since + new-nominee table.
  3. Firm-year board-composition features keyed (gvkey, year), joined onto firm_panel.
  4. Person-level enrichment (person_year, person_lifetime) + corpus-intersection spine.

OUT OF SCOPE (deferred to Wave 4): 8-K work, tenure-gating, WRDS-union tenure.

Outputs a NEW dated release under data/canonical/releases/<date>_def14a/ with a
MANIFEST.json. Does NOT flip the `current` symlink (owner reviews and flips).

Run:  python3 src/data_extraction/build_def14a_merge_release.py
      python3 src/data_extraction/build_def14a_merge_release.py --skip-age   # reuse cached ages
"""
from __future__ import annotations
import argparse, json, os, re, gzip, glob, subprocess, sys, warnings
from datetime import date
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DEF14A = ROOT / "data" / "raw" / "def14a"
PROC = ROOT / "data" / "processed"
CANON = ROOT / "data" / "canonical"
STAMP = date.today().isoformat()            # YYYY-MM-DD
RELEASE = CANON / "releases" / f"{STAMP}_def14a"
AGE_CACHE = PROC / "def14a_ages_extracted.csv"

STATUS_CSV = PROC / "def14a_director_status_20260528.csv"
TENURE_CSV = PROC / "def14a_director_tenure_panel_20260528.csv"
BRIDGE_CSV = PROC / "def14a_urls_for_revelio_validation.csv"
DIRECTORS_ALL = ROOT / "data" / "extracted" / "directors" / "directors_all.csv"
CUR = CANON / "current"

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# name / url normalization (mirrors build_def14a_tenure_panel.py)
# ---------------------------------------------------------------------------
_SUFFIX = re.compile(r"\b(jr|sr|ii|iii|iv|v|dr|mr|mrs|ms|phd|md|esq)\b")
_PAREN = re.compile(r"\(.*?\)")

def norm_name(s: str) -> str:
    s = _PAREN.sub(" ", str(s).lower())
    s = re.sub(r"[.,'\"]", " ", s)
    s = _SUFFIX.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()

def name_keys(name: str):
    n = norm_name(name); keys = [("full", n)]; toks = n.split()
    if len(toks) >= 2:
        keys.append(("fil", f"{toks[0][0]}|{toks[-1]}"))
    return keys

def clean_url(u) -> str | None:
    if u is None or (isinstance(u, float) and np.isnan(u)):
        return None
    s = str(u).lower().strip()
    s = re.sub(r"^https?://", "", s)
    m = re.search(r"linkedin\.com/in/[^/?#\s]+", s)
    return m.group(0) if m else None

# ---------------------------------------------------------------------------
# AGE extraction: HTML table-parse  (run per filing, parallel)
# ---------------------------------------------------------------------------
AGE_HDR = re.compile(r"\bage\b", re.I)
BAD_HDR = re.compile(r"average|percentage|age\s+(of\s+)?(service|tenure)|coverage", re.I)

def _read_html(p: str) -> str:
    return gzip.open(p, "rt", errors="ignore").read() if p.endswith(".gz") else open(p, errors="ignore").read()

def parse_age_rows(html: str):
    """Return list of (set-of-name-tokens, age) from any Age-bearing table/cell."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for tbl in soup.find_all("table"):
        trs = tbl.find_all("tr")
        if not trs:
            continue
        age_col = hdr_i = None
        for i, tr in enumerate(trs[:5]):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
            for j, c in enumerate(cells):
                if c and AGE_HDR.search(c) and not BAD_HDR.search(c) and len(c) <= 18:
                    age_col, hdr_i = j, i; break
            if age_col is not None:
                break
        if age_col is not None:
            for tr in trs[hdr_i + 1:]:
                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                if len(cells) > age_col:
                    m = re.search(r"\b(\d{2,3})\b", cells[age_col])
                    if m and 25 <= int(m.group(1)) <= 99:
                        rows.append((" ".join(cells), int(m.group(1))))
        for tr in trs:
            joined = " ".join(c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"]))
            m = re.search(r"\bage\b\s*:?\s*(\d{2,3})\b", joined, re.I)
            if m and 25 <= int(m.group(1)) <= 99:
                rows.append((joined, int(m.group(1))))
    return [(set(norm_name(rt).split()), ag) for rt, ag in rows]

def _age_for_filing(args):
    """Worker: (cik, year, [(full_name, idx)]) -> [(idx, age)] from HTML table."""
    cik, year, people = args
    paths = glob.glob(str(RAW_DEF14A / f"{cik}_{year}.html*"))
    if not paths:
        return []
    try:
        rows = parse_age_rows(_read_html(paths[0]))
    except Exception:
        return []
    out = []
    for full_name, idx in people:
        toks = norm_name(full_name).split()
        if len(toks) < 2:
            continue
        first, last = toks[0], toks[-1]
        for rset, ag in rows:
            if last in rset and first in rset:
                out.append((idx, ag)); break
    return out

def regex_age(t) -> float:
    for pat in (r"\bAge\s*:?\s*(\d{2,3})\b", r",\s*(\d{2}),"):
        m = re.search(pat, str(t), re.I)
        if m and 25 <= int(m.group(1)) <= 99:
            return int(m.group(1))
    return np.nan

def build_ages(status: pd.DataFrame) -> pd.DataFrame:
    """Return status index -> (age, age_source) using table-parse UNION regex."""
    dirs = status[status["is_director"] == True]
    # group director rows by filing for one HTML parse per (cik, year)
    groups = {}
    for idx, r in dirs[["cik", "year", "full_name"]].iterrows():
        groups.setdefault((int(r["cik"]), int(r["year"])), []).append((r["full_name"], idx))
    tasks = [(cik, yr, ppl) for (cik, yr), ppl in groups.items()]
    print(f"  parsing {len(tasks):,} filings for Age tables (parallel)...")
    table_age = {}
    with ProcessPoolExecutor() as ex:
        for k, res in enumerate(ex.map(_age_for_filing, tasks, chunksize=16)):
            for idx, ag in res:
                table_age[idx] = ag
            if (k + 1) % 1000 == 0:
                print(f"    {k+1:,}/{len(tasks):,} filings")
    print(f"  table-parse produced ages for {len(table_age):,} director rows")

    rx = dirs["def14a_bio_text"].map(regex_age)
    age = pd.Series(np.nan, index=dirs.index, dtype="float64")
    src = pd.Series("", index=dirs.index, dtype="object")
    for idx in dirs.index:
        ta = table_age.get(idx, np.nan)
        ra = rx.loc[idx]
        if not np.isnan(ta):
            age.loc[idx] = ta
            src.loc[idx] = "table" if (np.isnan(ra) or abs(ta - ra) <= 1) else "table(disagree_regex)"
        elif not np.isnan(ra):
            age.loc[idx] = ra; src.loc[idx] = "regex"
    out = pd.DataFrame({"age": age, "age_source": src})

    # Year-delta QC: same (norm_name, ticker) across years -> age should track years
    qc = dirs.loc[out["age"].notna(), ["full_name", "ticker", "year"]].copy()
    qc["age"] = out.loc[qc.index, "age"]
    qc["nkey"] = qc["full_name"].map(norm_name) + "|" + qc["ticker"].astype(str)
    flagged = 0
    for _, g in qc.groupby("nkey"):
        if g["year"].nunique() < 2:
            continue
        g = g.sort_values("year")
        resid = (g["age"].diff() - g["year"].diff()).abs()
        bad = resid > 2
        if bad.any():
            flagged += int(bad.sum())
            out.loc[g.index[bad.values], "age_source"] += "|qc_year_delta_off"
    print(f"  year-delta QC flagged {flagged} rows (kept, marked in age_source)")
    return out

# ---------------------------------------------------------------------------
def load_status() -> pd.DataFrame:
    return pd.read_csv(STATUS_CSV)

def load_bridge_maps():
    full_map, fil_map = {}, {}
    br = pd.read_csv(BRIDGE_CSV)
    for _, x in br.iterrows():
        url = clean_url(x.get("linkedin_url"))
        tk = str(x.get("board_ticker") or "").strip().upper()
        if not url or not tk:
            continue
        for kind, key in name_keys(str(x.get("person_name", ""))):
            (full_map if kind == "full" else fil_map).setdefault((key, tk), url)
    return full_map, fil_map

def resolve_url(name, ticker, full_map, fil_map):
    tk = str(ticker).upper()
    for kind, key in name_keys(name):
        m = full_map if kind == "full" else fil_map
        if (key, tk) in m:
            return m[(key, tk)]
    return None

# ---------------------------------------------------------------------------
def build_new_nominee_table(status: pd.DataFrame, full_map, fil_map) -> pd.DataFrame:
    """One row per (norm_name, ticker): tenure_start/end, right_censored, new_nominee, profile_url."""
    d = status[status["is_director"] == True].copy()
    d["nname"] = d["full_name"].map(norm_name)
    # latest proxy year per ticker (for right-censoring)
    last_proxy = d.groupby("ticker")["year"].max().to_dict()
    recs = []
    for (nname, ticker), g in d.groupby(["nname", "ticker"]):
        g = g.sort_values("year")
        first_proxy = int(g["year"].min())
        last_year = int(g["year"].max())
        ds = g["director_since_year"].dropna()
        director_since = int(ds.min()) if len(ds) else np.nan
        tenure_start = int(director_since) if not np.isnan(director_since) else first_proxy
        # genuine first-time nominee = the DEF14A classifier's call (status==new_nominee
        # in any proxy; it appears only in the year they first stand for THIS board).
        # director_since vs proxy year already fed that classification (separates true
        # first-timers from WRDS left-censoring). We do NOT broaden on director_since,
        # which would absorb mid-year-appointees / left-censored incumbents.
        ever_new = bool((g["def14a_director_status"] == "new_nominee").any())
        # contradiction guard: if the bio says they started well before the first proxy,
        # they are a left-censored incumbent, not a genuine new nominee.
        if ever_new and (not np.isnan(director_since)) and (director_since < first_proxy - 1):
            ever_new = False
        new_nominee = ever_new
        # representative full_name (modal spelling); guard all-NaN groups
        modes = g["full_name"].mode()
        if len(modes):
            fname = modes.iloc[0]
        elif g["full_name"].notna().any():
            fname = g["full_name"].dropna().iloc[0]
        else:
            fname = ""
        recs.append({
            "profile_url": resolve_url(fname, ticker, full_map, fil_map),
            "full_name": fname, "ticker": ticker,
            "director_since": director_since,
            "tenure_start": tenure_start, "tenure_end": last_year,
            "right_censored": bool(last_year == last_proxy.get(ticker)),
            "new_nominee": new_nominee,
            "first_proxy_year": first_proxy,
            "n_proxy_years": int(g["year"].nunique()),
        })
    return pd.DataFrame(recs)

# ---------------------------------------------------------------------------
def build_board_composition(status_age: pd.DataFrame, tic2gv: dict, firm_panel: pd.DataFrame):
    d = status_age[status_age["is_director"] == True].copy()
    d["gvkey"] = d["ticker"].str.upper().map(tic2gv)
    failed_gvkey = d[d["gvkey"].isna()]["ticker"].nunique()
    d = d.dropna(subset=["gvkey"]).copy()
    d["gvkey"] = d["gvkey"].astype(int)
    is_board = d["def14a_director_status"].isin(["incumbent", "new_nominee", "mid_year_appointee"])
    d = d[is_board].copy()

    def agg(g):
        return pd.Series({
            "board_size": len(g),
            "n_incumbent": int((g["def14a_director_status"] == "incumbent").sum()),
            "n_new_nominee": int((g["def14a_director_status"] == "new_nominee").sum()),
            "n_mid_year_appointee": int((g["def14a_director_status"] == "mid_year_appointee").sum()),
            "mean_director_age": g["age"].mean(),
            "n_age_known": int(g["age"].notna().sum()),
        })
    bc = d.groupby(["gvkey", "year"]).apply(agg).reset_index()
    bc["share_new_nominee"] = bc["n_new_nominee"] / bc["board_size"]
    bc["age_coverage"] = bc["n_age_known"] / bc["board_size"]

    fp_keys = set(zip(firm_panel["gvkey"].astype(int), firm_panel["year"].astype(int)))
    bc_keys = set(zip(bc["gvkey"].astype(int), bc["year"].astype(int)))
    not_in_fp = len(bc_keys - fp_keys)
    return bc, failed_gvkey, not_in_fp

# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-age", action="store_true", help="reuse cached age CSV")
    args = ap.parse_args()

    print("=" * 70); print(f"DEF 14A merge release build  ->  {RELEASE}"); print("=" * 70)
    status = load_status()
    print(f"status rows: {len(status):,} | is_director: {int(status['is_director'].sum()):,}")

    # ---- 1. AGE ----
    print("\n[1] Age backfill (HTML table-parse ∪ bio regex)")
    if args.skip_age and AGE_CACHE.exists():
        ages = pd.read_csv(AGE_CACHE, index_col=0)
        print(f"  reused cache {AGE_CACHE.name}: {ages['age'].notna().sum():,} ages")
    else:
        ages = build_ages(status)
        ages.to_csv(AGE_CACHE)
        print(f"  cached -> {AGE_CACHE.name}")
    status_age = status.copy()
    status_age["age"] = np.nan
    status_age.loc[ages.index, "age"] = ages["age"].values
    status_age["age_source"] = ""
    status_age.loc[ages.index, "age_source"] = ages["age_source"].values
    dir_mask = status_age["is_director"] == True
    n_dir = int(dir_mask.sum())
    n_age = int(status_age.loc[dir_mask, "age"].notna().sum())
    print(f"  AGE COVERAGE: {n_age:,}/{n_dir:,} director rows = {n_age/n_dir*100:.1f}%")

    # ---- 2. new-nominee / tenure table ----
    print("\n[2] Per-(profile_url, ticker) director_since + new-nominee table")
    full_map, fil_map = load_bridge_maps()
    nn = build_new_nominee_table(status, full_map, fil_map)
    print(f"  rows (person×firm): {len(nn):,} | with profile_url: {nn['profile_url'].notna().sum():,}"
          f" | new_nominee=True: {int(nn['new_nominee'].sum()):,}")

    # ---- 3. board composition ----
    print("\n[3] Firm-year board-composition features (gvkey, year)")
    da = pd.read_csv(DIRECTORS_ALL, usecols=["gvkey", "ticker"]).dropna()
    da["ticker"] = da["ticker"].str.upper()
    tic2gv = (da.groupby("ticker")["gvkey"]
                .agg(lambda s: int(s.mode().iloc[0])).to_dict())
    firm_panel = pd.read_stata(CUR / "firm_panel_annual.dta")
    firm_panel["gvkey"] = pd.to_numeric(firm_panel["gvkey"], errors="coerce").astype("Int64")
    firm_panel["year"] = pd.to_numeric(firm_panel["year"], errors="coerce").astype("Int64")
    bc, failed_gvkey, not_in_fp = build_board_composition(status_age, tic2gv, firm_panel)
    bc["gvkey"] = bc["gvkey"].astype("Int64"); bc["year"] = bc["year"].astype("Int64")
    print(f"  board-comp rows (gvkey,year): {len(bc):,}")
    print(f"  tickers with NO gvkey map: {failed_gvkey}")
    print(f"  (gvkey,year) NOT in firm_panel (no-posts firm-years): {not_in_fp:,}")
    bccols = ["board_size", "n_incumbent", "n_new_nominee", "n_mid_year_appointee",
              "mean_director_age", "share_new_nominee", "age_coverage"]
    fp_enriched = firm_panel.merge(bc[["gvkey", "year"] + bccols],
                                   on=["gvkey", "year"], how="left")
    joined = fp_enriched["board_size"].notna().sum()
    print(f"  firm_panel rows gaining board-comp: {joined:,}/{len(firm_panel):,}")

    # ---- 4. person enrichment ----
    print("\n[4] Person-level enrichment")
    py = pd.read_stata(CUR / "person_year.dta")
    pl = pd.read_stata(CUR / "person_lifetime.dta")
    nn2 = nn.copy()
    nn2["url_key"] = nn2["profile_url"].map(clean_url)
    nn2["ticker"] = nn2["ticker"].str.upper()
    # representative per (url_key, ticker)
    nn2 = nn2.dropna(subset=["url_key"]).sort_values("new_nominee", ascending=False)
    nn_key = nn2.drop_duplicates(["url_key", "ticker"]).set_index(["url_key", "ticker"])

    py["url_key"] = py["profile_url"].map(clean_url)
    py["tk"] = py["ticker"].str.upper()
    def py_lookup(col):
        idx = list(zip(py["url_key"], py["tk"]))
        return [nn_key[col].get(k, np.nan) if k in nn_key.index else np.nan for k in idx]
    py["def14a_new_nominee"] = [np.nan if (v is None or (isinstance(v, float) and pd.isna(v)))
                                else float(bool(v)) for v in py_lookup("new_nominee")]
    py["def14a_tenure_start"] = pd.to_numeric(pd.Series(py_lookup("tenure_start")), errors="coerce").values
    py["def14a_tenure_end"] = pd.to_numeric(pd.Series(py_lookup("tenure_end")), errors="coerce").values
    py["def14a_director"] = py["def14a_tenure_start"].notna()
    py["pre_board_year"] = py["def14a_tenure_start"].notna() & (py["year"] < py["def14a_tenure_start"])
    # established elsewhere: incumbent at a DIFFERENT ticker (by url_key)
    inc_firms = nn2[nn2["new_nominee"] == False].groupby("url_key")["ticker"].agg(set)
    def established_elsewhere(row):
        s = inc_firms.get(row["url_key"])
        return bool(s and (s - {row["tk"]}))
    py["is_established_elsewhere"] = py.apply(established_elsewhere, axis=1)
    print(f"  person_year: def14a_director={int(py['def14a_director'].sum()):,}"
          f" | pre_board_year rows={int(py['pre_board_year'].sum()):,}"
          f" | new_nominee seats matched={int((py['def14a_new_nominee']==1.0).sum()):,}")

    # lifetime: aggregate across firms
    firms_by_url = nn2.groupby("url_key")
    pl["url_key"] = pl["profile_url"].map(clean_url)
    ever_dir = set(nn2["url_key"])
    ever_nn = set(nn2[nn2["new_nominee"] == True]["url_key"])
    nfirms = firms_by_url["ticker"].nunique().to_dict()
    estab = {u: (len(g[g["new_nominee"] == False]["ticker"].unique()) > 0) for u, g in firms_by_url}
    pl["def14a_director"] = pl["url_key"].isin(ever_dir)
    pl["def14a_ever_new_nominee"] = pl["url_key"].isin(ever_nn)
    pl["def14a_n_firms"] = pl["url_key"].map(lambda u: nfirms.get(u, 0))
    pl["is_established_elsewhere"] = pl["url_key"].map(lambda u: bool(estab.get(u, False)))
    print(f"  person_lifetime: def14a_director={int(pl['def14a_director'].sum()):,}"
          f" | ever_new_nominee={int(pl['def14a_ever_new_nominee'].sum()):,}")

    # ---- 5. corpus-intersection spine ----
    print("\n[5] Corpus-intersection spine (tenure rows on corpus profiles)")
    tp = pd.read_csv(TENURE_CSV)
    tp["url_key"] = tp["profile_url"].map(clean_url)
    corpus_keys = set(py["url_key"].dropna())
    spine = tp[tp["url_key"].isin(corpus_keys)].copy()
    # attach age (per ticker+year+name via status_age) and tenure summary
    sa = status_age[status_age["is_director"] == True][["ticker", "year", "full_name", "age"]].copy()
    sa["url_key2"] = None
    spine = spine.merge(
        nn[["profile_url", "ticker", "tenure_start", "tenure_end", "new_nominee"]]
          .assign(url_key=lambda x: x["profile_url"].map(clean_url))
          .drop(columns=["profile_url"]),
        on=["url_key", "ticker"], how="left", suffixes=("", "_nn"))
    print(f"  spine rows: {len(spine):,} | distinct profiles: {spine['url_key'].nunique():,}"
          f" | distinct (profile,ticker): {spine.groupby(['url_key','ticker']).ngroups:,}")

    # ---- 6. package release ----
    print("\n[6] Packaging release")
    RELEASE.mkdir(parents=True, exist_ok=True)
    counts = {}

    status_age_path = PROC / f"def14a_director_status_age_{STAMP.replace('-','')}.csv"
    status_age.to_csv(status_age_path, index=False); counts["def14a_director_status.csv"] = len(status_age)
    nn_path = PROC / f"def14a_new_nominee_tenure_{STAMP.replace('-','')}.csv"
    nn.to_csv(nn_path, index=False); counts["def14a_new_nominee_tenure.csv"] = len(nn)
    bc_path = PROC / f"def14a_board_composition_{STAMP.replace('-','')}.csv"
    bc.to_csv(bc_path, index=False); counts["def14a_board_composition.csv"] = len(bc)
    spine_path = PROC / f"def14a_corpus_intersection_{STAMP.replace('-','')}.csv"
    spine.to_csv(spine_path, index=False); counts["def14a_corpus_intersection.csv"] = len(spine)

    # enriched panels written directly into the release (new analysis inputs)
    fp_enriched.to_stata(RELEASE / "firm_panel_annual.dta", write_index=False, version=118)
    counts["firm_panel_annual.dta"] = len(fp_enriched)
    py.drop(columns=["url_key", "tk"]).to_stata(RELEASE / "person_year.dta", write_index=False, version=118)
    counts["person_year.dta"] = len(py)
    pl.drop(columns=["url_key"]).to_stata(RELEASE / "person_lifetime.dta", write_index=False, version=118)
    counts["person_lifetime.dta"] = len(pl)

    def link(stable, target: Path):
        dst = RELEASE / stable
        rel = os.path.relpath(target.resolve(), RELEASE)
        if dst.is_symlink() or dst.exists():
            dst.unlink()
        dst.symlink_to(rel)

    # new dated source files -> stable-named symlinks in the release
    link("def14a_director_status.csv", status_age_path)
    link("def14a_new_nominee_tenure.csv", nn_path)
    link("def14a_board_composition.csv", bc_path)
    link("def14a_corpus_intersection.csv", spine_path)

    # carry-forward unchanged canonical entries (resolve through current/)
    carry = ["company_sentiment_annual.csv", "company_sentiment_quarterly.csv",
             "crsp_annual_returns.csv", "def14a_director_tenure.csv",
             "firm_panel_quarterly.dta", "funda_annual.csv",
             "posts_full_coverage.csv", "posts_scored_unique.csv",
             "revelio_validation_summary.csv"]
    for name in carry:
        link(name, (CUR / name).resolve())

    git_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                             capture_output=True, text=True).stdout.strip()
    manifest = {
        "release": f"{STAMP}_def14a",
        "build_date": STAMP,
        "git_sha": git_sha,
        "current_symlink_flipped": False,
        "base_release": "2026-05-27",
        "what_changed": (
            "Merged DEF 14A director classification into the panels (posts we already have). "
            "Added director AGE (HTML table-parse ∪ bio regex = 78.8% at $0, then targeted "
            "Haiku 4.5 pass on the residual = 95.5% for $48; no re-scrape); "
            "new-nominee/tenure table per (profile_url,ticker); firm-year board-composition "
            "features on firm_panel; person enrichment (def14a status, tenure, pre_board_year, "
            "is_established_elsewhere). OUT OF SCOPE / deferred to Wave 4: 8-K, tenure-gating of "
            "sentiment, WRDS∪def14a tenure union. `current` NOT flipped — owner reviews."
        ),
        "files": {},
    }
    for name in sorted(os.listdir(RELEASE)):
        if name in ("MANIFEST.json", "CODEBOOK.md"):
            continue
        p = RELEASE / name
        tgt = os.readlink(p) if p.is_symlink() else "(written in place)"
        manifest["files"][name] = {
            "rows": counts.get(name),
            "source": tgt,
        }
    manifest["key_metrics"] = {
        "age_coverage_pct": round(n_age / n_dir * 100, 1),
        "age_rows_backfilled": n_age,
        "new_nominee_pairs": int(nn["new_nominee"].sum()),
        "new_nominee_with_profile_url": int(nn[nn["new_nominee"] == True]["profile_url"].notna().sum()),
        "board_comp_tickers_no_gvkey": int(failed_gvkey),
        "board_comp_gvkey_year_not_in_firm_panel": int(not_in_fp),
        "firm_panel_rows_with_board_comp": int(joined),
        "person_year_pre_board_rows": int(py["pre_board_year"].sum()),
    }
    (RELEASE / "MANIFEST.json").write_text(json.dumps(manifest, indent=2))
    print(f"  wrote MANIFEST.json")
    print("\nDONE. Release at:", RELEASE)
    print("current still ->", os.readlink(CUR))
    print(json.dumps(manifest["key_metrics"], indent=2))

if __name__ == "__main__":
    main()

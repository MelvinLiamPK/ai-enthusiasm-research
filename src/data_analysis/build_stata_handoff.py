"""Build the Stata handoff environment for John.

Produces outputs/stata_handoff_20260527/:
  data/
    firm_panel.dta        — firm × year, regression-ready (sentiment + funda + CRSP lead outcomes)
    firm_quarterly.dta    — firm × quarter (direct convert of company_sentiment_quarterly)
    person_year.dta       — person × year aggregation from sentiment_posts_scored_unique
    person_lifetime.dta   — one row per person, lifetime sentiment summary
  do/
    master.do, 01_load.do, 02_regressions.do, 03_time_series.do,
    04_outliers.do, 05_person_level.do
  outliers/
    top_firms_by_ai_share.csv, biggest_yoy_ai_jumps.csv,
    top_persons_lifetime_ai.csv, firms_with_few_persons.csv
  plots/        (empty; populated when Stata runs the do-files)
  README.md

Design notes:
  - Strict AI keyword set is already baked into the upstream sentiment files'
    `is_ai_related` column. We use it as-is, no re-flagging.
  - Default: NO n_posts filter. firm_panel.dta has all firm-years; the
    `meets_min_posts_10` column is provided so John can toggle.
  - Person files dedupe at (profile_url, post_url) — multi-board people
    are not double-counted.
  - Person attrs (source / gvkey / ticker) come from a modal lookup over
    sentiment_all_posts_full_coverage, with def14a rows deferred for the
    gvkey tiebreaker (def14a rows have gvkey=NaN).

Usage:
    python3 src/data_analysis/build_stata_handoff.py --run
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# Inputs are read through the canonical source-of-truth pointer
# (data/canonical/current -> the active dated release). Stable names there
# decouple this script from dated filenames; cut a new release + flip `current`
# to update the data (see data/canonical/README.md). Files not yet in the
# canonical layer keep their direct paths.
CANONICAL = PROJECT_ROOT / "data" / "canonical" / "current"

DEFAULTS = {
    "company_annual": CANONICAL / "company_sentiment_annual.csv",
    "company_quarterly": CANONICAL / "company_sentiment_quarterly.csv",
    "posts_scored": CANONICAL / "posts_scored_unique.csv",
    "full_coverage": CANONICAL / "posts_full_coverage.csv",
    "profiles_v2": PROJECT_ROOT / "data/processed/all_people_linkedin_urls/scraped_posts_combined/profiles_combined_v2_20260527.csv",
    "funda": CANONICAL / "funda_annual.csv",
    "crsp": CANONICAL / "crsp_annual_returns.csv",
    "def14a_outcomes": PROJECT_ROOT / "data/processed/all_people_linkedin_urls/def14a_scrape_outcomes_20260527.csv",
    "revelio": CANONICAL / "revelio_validation_summary.csv",
    "out_dir": PROJECT_ROOT / "outputs/stata_handoff_20260527",
}


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def norm_gvkey(s):
    """gvkey to string (drop trailing .0); preserve NaN."""
    return s.astype("object").where(
        s.isna(),
        s.astype("Float64").astype("Int64").astype(str),
    )


def winsorize(df, cols, p=0.01):
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            continue
        s = out[c]
        if not pd.api.types.is_numeric_dtype(s):
            continue
        lo, hi = s.quantile(p), s.quantile(1 - p)
        if pd.isna(lo) or pd.isna(hi):
            continue
        out[c] = s.clip(lo, hi)
    return out


# ──────────────────────────────────────────────
# Mean-of-mean sentiment per (gvkey, year)
# ──────────────────────────────────────────────

def _compute_mean_of_mean_block(df):
    """Two-stage aggregation: person-mean within firm-year, then mean across persons.
    Returns DataFrame keyed on (gvkey, year) with `_mom_all` / `_mom_ai` columns.
    """
    person_all = (df.groupby(["gvkey", "year", "profile_url"])["lm_net_sentiment"]
                    .mean().reset_index()
                    .rename(columns={"lm_net_sentiment": "_pmean_all"}))
    ai_df = df[df["is_ai_related"]]
    person_ai = (ai_df.groupby(["gvkey", "year", "profile_url"])["lm_net_sentiment"]
                      .mean().reset_index()
                      .rename(columns={"lm_net_sentiment": "_pmean_ai"}))
    mom_all = (person_all.groupby(["gvkey", "year"])["_pmean_all"]
                          .mean().reset_index()
                          .rename(columns={"_pmean_all": "_mom_all"}))
    mom_ai = (person_ai.groupby(["gvkey", "year"])["_pmean_ai"]
                        .mean().reset_index()
                        .rename(columns={"_pmean_ai": "_mom_ai"}))
    return mom_all.merge(mom_ai, on=["gvkey", "year"], how="left")


def compute_firm_year_metrics(full_coverage_path, revelio_path, chunksize=500_000):
    """Compute all firm-year sentiment + volume metrics, both unfiltered AND
    restricted to Revelio strong-match profiles.

    Returns one DataFrame keyed on (gvkey, year) with columns:
      Unfiltered (all profiles):
        - mom_net_sentiment, ai_mom_net_sentiment
      Strong-match only (Revelio strong_match_either == True):
        - n_posts_strong, n_ai_posts_strong, n_persons_strong
        - ai_post_share_strong
        - mom_net_sentiment_strong, ai_mom_net_sentiment_strong

    Source: sentiment_all_posts_full_coverage_20260527.csv (post × person × company)
    Dedup: (post_url, profile_url, gvkey) — matches the upstream handoff caveat.
    """
    print(f"[firm-y] loading Revelio strong_match map …")
    rv = pd.read_csv(revelio_path, usecols=["linkedin_url", "strong_match_either"],
                     low_memory=False)
    rv = rv.drop_duplicates("linkedin_url")
    rv["strong_match"] = (rv["strong_match_either"].astype("boolean")
                            .fillna(False).astype(bool))
    sm_map = dict(zip(rv["linkedin_url"], rv["strong_match"]))
    n_sm = sum(sm_map.values())
    print(f"        {len(sm_map):,} Revelio rows; {n_sm:,} strong-matched")

    print(f"[firm-y] streaming full-coverage (chunked) …")
    cols = ["profile_url", "gvkey", "post_url", "post_date",
            "is_ai_related", "lm_net_sentiment"]
    parts = []
    for chunk in pd.read_csv(
        full_coverage_path, usecols=cols, engine="c", lineterminator="\n",
        on_bad_lines="skip", chunksize=chunksize, low_memory=False,
    ):
        chunk = chunk.dropna(subset=["gvkey", "post_date", "lm_net_sentiment"])
        chunk["gvkey"] = norm_gvkey(chunk["gvkey"])
        chunk["year"] = pd.to_datetime(chunk["post_date"], errors="coerce").dt.year
        chunk = chunk.dropna(subset=["year"])
        chunk["year"] = chunk["year"].astype(int)
        chunk["is_ai_related"] = (chunk["is_ai_related"].astype("boolean")
                                  .fillna(False).astype(bool))
        chunk["strong_match"] = chunk["profile_url"].map(sm_map).fillna(False)
        parts.append(chunk[["gvkey", "year", "profile_url", "post_url",
                            "is_ai_related", "lm_net_sentiment", "strong_match"]])
    df = pd.concat(parts, ignore_index=True)
    before = len(df)
    df = df.drop_duplicates(subset=["post_url", "profile_url", "gvkey"])
    print(f"        after dedup: {len(df):,} unique (post, person, company) rows "
          f"(dropped {before - len(df):,})")

    # ── unfiltered mean-of-mean ──
    mom_all = (_compute_mean_of_mean_block(df)
               .rename(columns={"_mom_all": "mom_net_sentiment",
                                "_mom_ai":  "ai_mom_net_sentiment"}))

    # ── strong-match-only mean-of-mean + volume counts ──
    sm = df[df["strong_match"]]
    print(f"        strong-match subset: {len(sm):,} rows "
          f"({sm['profile_url'].nunique():,} unique profiles)")

    mom_sm = (_compute_mean_of_mean_block(sm)
              .rename(columns={"_mom_all": "mom_net_sentiment_strong",
                               "_mom_ai":  "ai_mom_net_sentiment_strong"}))

    counts_sm = (sm.groupby(["gvkey", "year"])
                   .agg(n_posts_strong=("post_url", "count"),
                        n_ai_posts_strong=("is_ai_related", "sum"),
                        n_persons_strong=("profile_url", "nunique"))
                   .reset_index())
    counts_sm["n_ai_posts_strong"] = counts_sm["n_ai_posts_strong"].astype(int)
    counts_sm["ai_post_share_strong"] = (counts_sm["n_ai_posts_strong"] /
                                          counts_sm["n_posts_strong"])

    out = (mom_all
           .merge(counts_sm, on=["gvkey", "year"], how="left")
           .merge(mom_sm, on=["gvkey", "year"], how="left"))
    print(f"        produced {len(out):,} (gvkey, year) cells; "
          f"{out['n_posts_strong'].notna().sum():,} have any strong-match data")
    return out


def compute_strong_match_map(revelio_path):
    """Per-profile strong_match flag for joining onto person files."""
    rv = pd.read_csv(revelio_path, usecols=["linkedin_url", "strong_match_either"],
                     low_memory=False)
    rv = rv.drop_duplicates("linkedin_url")
    rv["strong_match"] = (rv["strong_match_either"].astype("boolean")
                            .fillna(False).astype(bool))
    return rv[["linkedin_url", "strong_match"]].rename(
        columns={"linkedin_url": "profile_url"})


# ──────────────────────────────────────────────
# 1. firm_panel.dta — sentiment + funda + CRSP, with t+1 outcomes
# ──────────────────────────────────────────────

def build_firm_panel(company_annual_path, funda_path, crsp_path, fym_df):
    print("[firm] loading sentiment, funda, crsp …")
    sent = pd.read_csv(company_annual_path, low_memory=False)
    sent["gvkey"] = norm_gvkey(sent["gvkey"])
    sent = sent[sent["gvkey"].notna()].copy()
    sent["year"] = sent["year"].astype(int)
    print(f"       sentiment rows: {len(sent):,} ({sent['gvkey'].nunique():,} gvkeys)")

    f = pd.read_csv(funda_path, low_memory=False)
    f["gvkey"] = norm_gvkey(f["gvkey"])
    f = f[f["gvkey"].notna() & f["fyear"].notna()].copy()
    f["year"] = f["fyear"].astype(int)

    # Derived outcomes + controls
    f["ln_at"] = np.log(f["at"].where(f["at"] > 0))
    f["leverage"] = f["lt"] / f["at"]
    f["rnd_int"] = f["xrd"].fillna(0) / f["at"]
    f["profit_marg"] = f["ni"] / f["sale"].where(f["sale"] > 0)
    f["roa"] = f["ni"] / f["at"].where(f["at"] > 0)
    f = f.sort_values(["gvkey", "year"])
    sale_pos = f["sale"].where(f["sale"] > 0)
    f["sales_growth"] = np.log(sale_pos) - np.log(sale_pos.groupby(f["gvkey"]).shift(1))

    # CRSP merge
    c = pd.read_csv(crsp_path, low_memory=False)
    c["gvkey"] = norm_gvkey(c["gvkey"])
    c = c[c["gvkey"].notna() & c["fyear"].notna()].copy()
    c["year"] = c["fyear"].astype(int)
    f = f.merge(c[["gvkey", "year", "stock_return"]], on=["gvkey", "year"], how="left")
    print(f"       funda rows: {len(f):,} after CRSP merge")

    # Lead frame: outcomes at year t+1
    lead = f[["gvkey", "year", "tobins_q", "roa", "sales_growth", "stock_return"]].copy()
    lead["year"] = lead["year"] - 1  # shift backward so it joins to sent year t
    lead = lead.rename(columns={
        "tobins_q": "tobins_q_lead",
        "roa": "roa_lead",
        "sales_growth": "sales_growth_lead",
        "stock_return": "stock_return_lead",
    })
    lead["ln_tobins_q_lead"] = np.log(lead["tobins_q_lead"].where(lead["tobins_q_lead"] > 0))

    # Controls + same-year (t) outcomes
    f["ln_tobins_q"] = np.log(f["tobins_q"].where(f["tobins_q"] > 0))
    ctrl_cols = ["gvkey", "year", "ln_at", "leverage", "rnd_int", "profit_marg", "sich",
                 "tobins_q", "ln_tobins_q", "roa", "sales_growth", "stock_return"]
    controls = f[ctrl_cols]

    # Sentiment columns we want.
    # Note: post-level `mean_net_sentiment` / `ai_mean_net_sentiment` are NOT
    # carried — they're replaced by mean-of-mean variants computed separately
    # in compute_mean_of_mean(). Each person within a firm-year gets equal
    # weight regardless of post count, so one prolific poster can't dominate
    # the firm-year sentiment.
    sent_cols = ["company_name_clean", "gvkey", "ticker", "year",
                 "n_posts", "n_ai_posts", "n_persons",
                 "n_strong_match_posts", "strong_match_share",
                 "ai_post_share",
                 "engagement_wtd_sentiment", "role_wtd_sentiment",
                 "mean_polarity", "frac_positive_posts", "frac_negative_posts"]
    sent = sent[[c for c in sent_cols if c in sent.columns]]
    # Merge in unfiltered mean-of-mean + strong-match parallel metrics
    sent = sent.merge(fym_df, on=["gvkey", "year"], how="left")
    # Strong-match toggle flags (parallel to meets_min_posts_10 / meets_min_ai_posts_3)
    sent["has_strong_match"] = (sent["n_posts_strong"].fillna(0) > 0).astype("Int8")
    sent["meets_min_posts_strong_10"] = (sent["n_posts_strong"].fillna(0) >= 10).astype("Int8")
    sent["meets_min_ai_posts_strong_3"] = (sent["n_ai_posts_strong"].fillna(0) >= 3).astype("Int8")

    panel = sent.merge(controls, on=["gvkey", "year"], how="inner")
    panel = panel.merge(
        lead[["gvkey", "year", "ln_tobins_q_lead", "roa_lead",
              "sales_growth_lead", "stock_return_lead"]],
        on=["gvkey", "year"], how="inner",
    )
    print(f"       after inner-merge: {len(panel):,} firm-years")

    # NOTE: no pre-winsorization. The Python regression winsorizes at 1%/99%
    # over its filtered sample (n_posts>=10, n_ai_posts>=3 for AI regressors,
    # plus non-NaN on controls). Pre-winsorizing here over the unfiltered
    # panel would compute different thresholds and break replication.
    # `02_regressions.do` applies the winsorize *after* the filter.

    panel["meets_min_posts_10"] = (panel["n_posts"] >= 10).astype("Int8")
    panel["meets_min_ai_posts_3"] = (panel["n_ai_posts"] >= 3).astype("Int8")
    panel = panel.sort_values(["gvkey", "year"]).reset_index(drop=True)
    return panel


# ──────────────────────────────────────────────
# 2. firm_quarterly.dta — direct convert
# ──────────────────────────────────────────────

def build_firm_quarterly(company_quarterly_path):
    print("[firm] loading quarterly aggregates …")
    df = pd.read_csv(company_quarterly_path, low_memory=False)
    df["gvkey"] = norm_gvkey(df["gvkey"])
    # NOTE: quarterly stays on POST-level mean_net_sentiment / ai_mean_net_sentiment
    # (the upstream file's columns). Mean-of-mean at the quarterly grain is awkward
    # because persons don't post every quarter. For annual regressions use firm_panel.
    keep = ["company_name_clean", "gvkey", "ticker", "quarter",
            "n_posts", "n_ai_posts", "n_persons",
            "ai_post_share", "ai_mean_net_sentiment",
            "mean_net_sentiment", "engagement_wtd_sentiment", "role_wtd_sentiment",
            "mean_polarity", "frac_positive_posts", "frac_negative_posts"]
    df = df[[c for c in keep if c in df.columns]].copy()
    df["meets_min_posts_10"] = (df["n_posts"] >= 10).astype("Int8")
    print(f"       quarterly rows: {len(df):,}")
    return df


# ──────────────────────────────────────────────
# 3. Person modal lookup — source/gvkey/ticker per profile_url
# ──────────────────────────────────────────────

def build_person_modal_lookup(full_coverage_path, def14a_outcomes_path, chunksize=300_000):
    """Modal (source, gvkey, ticker) per profile_url + cohort tag.

    For gvkey: drop NaN before taking mode (so def14a rows don't pull a
    person's modal gvkey to NaN). Fall back to def14a-only modal ticker
    when the person has no non-def14a rows.

    `cohort`:
      - 'def14a_continuation' if the person's URL is in def14a_scrape_outcomes
        (i.e. they were in the def14a continuation roster)
      - 'initial_universe' otherwise

    Note: many def14a-cohort people are also in the initial universe (the
    def14a continuation re-scraped people already in the corpus). The
    cohort tag just identifies origin in the def14a roster, not exclusivity.
    """
    print(f"[modal] scanning full-coverage for person attrs (chunked) …")
    use = ["profile_url", "source", "gvkey", "ticker"]
    parts = []
    for i, chunk in enumerate(pd.read_csv(
        full_coverage_path, usecols=use, engine="c", lineterminator="\n",
        on_bad_lines="skip", chunksize=chunksize, low_memory=False,
        dtype={"source": "string", "ticker": "string"},
    )):
        chunk["gvkey"] = norm_gvkey(chunk["gvkey"])
        parts.append(chunk)
        if (i + 1) % 10 == 0:
            print(f"        chunked {sum(len(p) for p in parts):,} rows …")
    df = pd.concat(parts, ignore_index=True)
    print(f"        total: {len(df):,} rows, {df['profile_url'].nunique():,} unique profiles")

    df["is_def14a"] = df["source"].eq("def14a_serper")

    def _mode_or_na(s):
        m = s.dropna()
        if len(m) == 0:
            return pd.NA
        vc = m.value_counts()
        return vc.index[0]

    print("        computing modes per profile_url …")
    # gvkey: prefer non-def14a rows
    non_def_gvkey = (df.loc[~df["is_def14a"]]
                       .groupby("profile_url")["gvkey"].agg(_mode_or_na))
    # For everyone else, fall back to overall modal gvkey (will be NaN if only def14a)
    all_gvkey = df.groupby("profile_url")["gvkey"].agg(_mode_or_na)
    gvkey = non_def_gvkey.reindex(all_gvkey.index).fillna(all_gvkey)

    # ticker: COALESCE non-def14a modal → def14a modal
    non_def_tk = (df.loc[~df["is_def14a"]]
                    .groupby("profile_url")["ticker"].agg(_mode_or_na))
    all_tk = df.groupby("profile_url")["ticker"].agg(_mode_or_na)
    ticker = non_def_tk.reindex(all_tk.index).fillna(all_tk)

    source = df.groupby("profile_url")["source"].agg(_mode_or_na)

    # cohort — driven by def14a_scrape_outcomes roster (canonical def14a URL list)
    def14a_urls = pd.read_csv(def14a_outcomes_path, usecols=["linkedin_url"])
    def14a_set = set(def14a_urls["linkedin_url"].dropna())
    cohort = pd.Series(gvkey.index, index=gvkey.index).map(
        lambda u: "def14a_continuation" if u in def14a_set else "initial_universe"
    )

    out = pd.DataFrame({
        "profile_url": gvkey.index,
        "gvkey": gvkey.values,
        "ticker": ticker.values,
        "source": source.reindex(gvkey.index).values,
        "cohort": cohort.reindex(gvkey.index).values,
    })
    print(f"        modal lookup: {len(out):,} profiles, "
          f"cohorts={out['cohort'].value_counts().to_dict()}")
    return out


# ──────────────────────────────────────────────
# 4. Person × year and person × lifetime
# ──────────────────────────────────────────────

PERSON_USECOLS = [
    "profile_url", "post_url", "post_date",
    "is_ai_related", "lm_net_sentiment", "lm_polarity",
    "lm_positive_ratio", "lm_negative_ratio", "lm_word_count",
    "finbert_score", "reactions_total",
]


def load_unique_posts(path, chunksize=300_000):
    print(f"[person] loading post-level scored file (chunked) …")
    # Header has duplicated finbert_* columns (".1" suffix) — we only want the first set
    header = pd.read_csv(path, nrows=0).columns.tolist()
    use = [c for c in PERSON_USECOLS if c in header]
    chunks = []
    n = 0
    for chunk in pd.read_csv(
        path, usecols=use, engine="c", lineterminator="\n",
        on_bad_lines="skip", chunksize=chunksize, low_memory=False,
    ):
        chunks.append(chunk)
        n += len(chunk)
    df = pd.concat(chunks, ignore_index=True)
    df["post_date"] = pd.to_datetime(df["post_date"], errors="coerce")
    df["year"] = df["post_date"].dt.year.astype("Int64")
    df["is_ai_related"] = df["is_ai_related"].astype("boolean").fillna(False).astype(bool)
    print(f"        loaded {len(df):,} unique posts")
    # Deduplicate by (profile_url, post_url) — defensive
    if "post_url" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["profile_url", "post_url"], keep="first")
        if len(df) < before:
            print(f"        dropped {before - len(df):,} duplicate (profile_url, post_url) rows")
    return df


def _person_agg_block(g):
    n_posts = len(g)
    n_ai = int(g["is_ai_related"].sum())
    out = {
        "n_posts": n_posts,
        "n_ai_posts": n_ai,
        "ai_post_share": n_ai / n_posts if n_posts else np.nan,
        "mean_lm_sentiment": g["lm_net_sentiment"].mean(),
        "median_lm_sentiment": g["lm_net_sentiment"].median(),
        "mean_lm_positive_ratio": g["lm_positive_ratio"].mean()
            if "lm_positive_ratio" in g.columns else np.nan,
        "mean_lm_negative_ratio": g["lm_negative_ratio"].mean()
            if "lm_negative_ratio" in g.columns else np.nan,
        "mean_lm_polarity": g["lm_polarity"].mean() if "lm_polarity" in g.columns else np.nan,
        "frac_positive_posts": (g["lm_net_sentiment"] > 0).mean(),
        "frac_negative_posts": (g["lm_net_sentiment"] < 0).mean(),
        "total_lm_words": int(g["lm_word_count"].sum()) if "lm_word_count" in g.columns else 0,
    }
    ai_sub = g[g["is_ai_related"]]
    if len(ai_sub):
        out["ai_mean_lm_sentiment"] = ai_sub["lm_net_sentiment"].mean()
        out["ai_frac_positive"] = (ai_sub["lm_net_sentiment"] > 0).mean()
    else:
        out["ai_mean_lm_sentiment"] = np.nan
        out["ai_frac_positive"] = np.nan
    if "finbert_score" in g.columns:
        out["mean_finbert_score"] = g["finbert_score"].mean()
        out["ai_mean_finbert_score"] = ai_sub["finbert_score"].mean() if len(ai_sub) else np.nan
    if "reactions_total" in g.columns:
        w = g["reactions_total"].fillna(0)
        tot = w.sum()
        out["engagement_wtd_lm_sentiment"] = (
            float((g["lm_net_sentiment"] * w).sum() / tot) if tot > 0
            else g["lm_net_sentiment"].mean()
        )
    return pd.Series(out)


def build_person_year(posts, modal, profiles, sm_map):
    print("[person] aggregating person × year …")
    p = posts.dropna(subset=["year"]).copy()
    p["year"] = p["year"].astype(int)
    rows = []
    for (purl, yr), sub in p.groupby(["profile_url", "year"]):
        rec = {"profile_url": purl, "year": int(yr)}
        rec.update(_person_agg_block(sub).to_dict())
        rows.append(rec)
    py = pd.DataFrame(rows)
    py = py.merge(modal, on="profile_url", how="left")
    py = py.merge(profiles, on="profile_url", how="left")
    py = py.merge(sm_map, on="profile_url", how="left")
    py["strong_match"] = py["strong_match"].fillna(False).astype("Int8")
    print(f"        {len(py):,} person-year rows  "
          f"(strong_match=1: {int(py['strong_match'].sum()):,})")
    return py


def build_person_lifetime(posts, modal, profiles, sm_map):
    print("[person] aggregating person × lifetime …")
    rows = []
    for purl, sub in posts.groupby("profile_url"):
        rec = {"profile_url": purl}
        rec.update(_person_agg_block(sub).to_dict())
        d = sub["post_date"].dropna()
        rec["first_post_date"] = d.min() if len(d) else pd.NaT
        rec["last_post_date"] = d.max() if len(d) else pd.NaT
        rec["n_years_active"] = int(sub["year"].dropna().nunique()) if "year" in sub.columns else 0
        rows.append(rec)
    pl = pd.DataFrame(rows)
    pl = pl.merge(modal, on="profile_url", how="left")
    pl = pl.merge(profiles, on="profile_url", how="left")
    pl = pl.merge(sm_map, on="profile_url", how="left")
    pl["strong_match"] = pl["strong_match"].fillna(False).astype("Int8")
    print(f"        {len(pl):,} person-lifetime rows  "
          f"(strong_match=1: {int(pl['strong_match'].sum()):,})")
    return pl


# ──────────────────────────────────────────────
# 5. Pre-baked outlier CSVs
# ──────────────────────────────────────────────

def write_outlier_csvs(out_dir, firm_panel, person_lifetime):
    """Pre-baked CSVs. Default to strong-match filter where applicable."""
    odir = out_dir / "outliers"
    odir.mkdir(parents=True, exist_ok=True)

    # top_firms_by_ai_share — strong-match sample (n_posts_strong >= 10)
    fp_strong = firm_panel[firm_panel["n_posts_strong"].fillna(0) >= 10]
    top_f = (fp_strong.nlargest(50, "ai_post_share_strong")
             [["gvkey", "ticker", "company_name_clean", "year",
               "n_posts_strong", "n_ai_posts_strong", "ai_post_share_strong",
               "ai_mom_net_sentiment_strong", "mom_net_sentiment_strong"]])
    top_f.to_csv(odir / "top_firms_by_ai_share.csv", index=False)
    print(f"        wrote outliers/top_firms_by_ai_share.csv ({len(top_f)} rows; strong-match sample)")

    # biggest_yoy_ai_jumps — strong-match sample
    fp = fp_strong.sort_values(["gvkey", "year"]).copy()
    fp["d_ai_share_strong"] = fp.groupby("gvkey")["ai_post_share_strong"].diff()
    jumps = (fp.dropna(subset=["d_ai_share_strong"])
               .assign(abs_jump=lambda d: d["d_ai_share_strong"].abs())
               .nlargest(30, "abs_jump")
               [["gvkey", "ticker", "company_name_clean", "year",
                 "ai_post_share_strong", "d_ai_share_strong", "n_posts_strong"]])
    jumps.to_csv(odir / "biggest_yoy_ai_jumps.csv", index=False)
    print(f"        wrote outliers/biggest_yoy_ai_jumps.csv ({len(jumps)} rows; strong-match sample)")

    # top_persons_lifetime_ai — strong-match only (the headline list)
    pl_strong = person_lifetime[(person_lifetime["strong_match"] == 1) &
                                (person_lifetime["n_posts"] >= 10)]
    top_p = (pl_strong.nlargest(50, "ai_post_share")
             [["profile_url", "author_name", "source", "cohort",
               "ticker", "n_posts", "n_ai_posts", "ai_post_share",
               "mean_lm_sentiment", "ai_mean_lm_sentiment"]])
    top_p.to_csv(odir / "top_persons_lifetime_ai.csv", index=False)
    print(f"        wrote outliers/top_persons_lifetime_ai.csv ({len(top_p)} rows; strong-match only)")

    # firms_with_few_persons — strong-match sample
    sparse = (firm_panel[firm_panel["n_persons_strong"].fillna(0) <= 2]
              .nlargest(30, "n_posts_strong")
              [["gvkey", "ticker", "company_name_clean", "year",
                "n_persons_strong", "n_posts_strong", "ai_post_share_strong"]])
    sparse.to_csv(odir / "firms_with_few_persons.csv", index=False)
    print(f"        wrote outliers/firms_with_few_persons.csv ({len(sparse)} rows; strong-match sample)")

    # NEW: dropped_by_revelio — visibility into who Revelio FILTERED OUT
    # These are the entries that look like Nathan May / David W Grant: scraped but
    # not confirmed by Revelio. Top 50 by raw n_posts so you see the heaviest false
    # matches first.
    dropped = (person_lifetime[person_lifetime["strong_match"] == 0]
               .nlargest(50, "n_posts")
               [["profile_url", "author_name", "source", "cohort",
                 "ticker", "n_posts", "n_ai_posts", "ai_post_share"]])
    dropped.to_csv(odir / "dropped_by_revelio.csv", index=False)
    print(f"        wrote outliers/dropped_by_revelio.csv ({len(dropped)} rows; strong_match=False)")


# ──────────────────────────────────────────────
# Stata writers
# ──────────────────────────────────────────────

def _stata_clean_names(df):
    import re
    rename = {}
    for c in df.columns:
        n = re.sub(r"[^A-Za-z0-9_]", "_", str(c))[:32]
        if n and n[0].isdigit():
            n = ("v_" + n)[:32]
        rename[c] = n
    seen = {}
    final = {}
    for k, v in rename.items():
        if v in seen:
            seen[v] += 1
            v = (v[:30] + f"_{seen[v]}")[:32]
        else:
            seen[v] = 1
        final[k] = v
    return df.rename(columns=final)


def _prep_for_stata(df):
    df = df.copy()
    # Nullable ints → float (NaN-safe)
    for c in df.select_dtypes(include=["Int64", "Int32", "Int16", "Int8"]).columns:
        df[c] = df[c].astype("float64")
    for c in df.select_dtypes(include=["boolean"]).columns:
        df[c] = df[c].astype("float64")
    # Datetimes — Stata accepts datetime64[ns]
    for c in df.select_dtypes(include=["object", "string"]).columns:
        df[c] = df[c].astype("object").where(df[c].notna(), "")
    return _stata_clean_names(df)


def write_dta(df, path):
    df = _prep_for_stata(df)
    df.to_stata(path, write_index=False, version=118)
    print(f"        {path.name}: {len(df):,} rows × {len(df.columns)} cols "
          f"({path.stat().st_size / 1024 / 1024:.1f} MB)")


# ──────────────────────────────────────────────
# Do-files & README
# ──────────────────────────────────────────────

MASTER_DO = '''\
* master.do — top-level runner for the Stata handoff
*
* This file runs the whole walkthrough end-to-end. Each step is also a
* standalone do-file in this folder; feel free to step through interactively
* by running them one at a time.
*
* Prerequisites:
*   ssc install reghdfe
*   ssc install ftools
*
* Adjust the `pwd` path below to wherever you extracted this folder.

clear all
set more off

* Change this to the absolute path of the handoff folder on your machine
cd "."

do do/01_load.do
do do/02_regressions.do
do do/03_time_series.do
do do/04_outliers.do
do do/05_person_level.do
'''

LOAD_DO = '''\
* 01_load.do — load the master firm-year panel, declare xtset, label vars

use "data/firm_panel.dta", clear

xtset gvkey year

label var n_posts                  "Total LinkedIn posts in firm-year"
label var n_ai_posts               "Posts matching AI keyword list"
label var n_persons                "Distinct individuals contributing posts"
label var ai_post_share            "Share of posts mentioning AI"
label var ai_mom_net_sentiment    "Mean LM net sentiment on AI posts"
label var mom_net_sentiment "Mean of person-mean LM sentiment (person-equal-weighting)"
label var engagement_wtd_sentiment "Reaction-weighted mean sentiment"
label var role_wtd_sentiment       "Role-hierarchy-weighted mean sentiment"
label var ln_tobins_q_lead         "ln(Tobin's Q) at t+1"
label var roa_lead                 "ROA at t+1"
label var sales_growth_lead        "Sales growth at t+1"
label var stock_return_lead        "Stock return at t+1"
label var meets_min_posts_10       "1 if n_posts >= 10 (toggle filter)"

di as txt "Panel loaded: " _N " firm-year rows"
summarize n_posts ai_post_share ai_mom_net_sentiment, detail
'''

REG_DO = '''\
* 02_regressions.do — 4 regressions (no controls, year FE optional)
*
* Specs run per outcome:
*   (1) Pooled OLS                  reg  y x, cluster(gvkey)
*   (2) + Year fixed effects        reghdfe y x, absorb(year) cluster(gvkey)
*
* SAMPLE & VARIANT (default = Revelio strong-match):
*   - All metrics with `_strong` suffix are computed on profiles that Revelio's
*     Workforce Data has confirmed by URL + name + company (board OR primary).
*     This drops false-positive URL matches (e.g. a person scraped under
*     "Nathan May / Meta" who is actually David W Grant, a B2B newsletter founder).
*   - The default keeps firm-years where n_posts_strong >= 10 AND, for AI regressors,
*     n_ai_posts_strong >= 3, and uses ai_post_share_strong / *_of_mean_*_strong
*     on the RHS.
*   - To run on the UNFILTERED-by-Revelio sample (matching the Python summary.txt),
*     swap to the non-_strong variants below. Each block has both written; comment
*     out whichever you don't want.
*   - 1%/99% winsorization is applied AFTER filtering.

capture program drop _winsor_inplace
program define _winsor_inplace
    syntax varlist, p(real)
    foreach v of varlist `varlist' {
        quietly summarize `v', detail
        local lo = r(p`=`p'*100')
        local hi = r(p`=100-`p'*100')
        quietly replace `v' = `lo' if `v' < `lo' & !missing(`v')
        quietly replace `v' = `hi' if `v' > `hi' & !missing(`v')
    }
end

* ════════ ln(Tobin's Q) at t+1 ════════
use "data/firm_panel.dta", clear
* DEFAULT: Revelio strong-match sample
keep if meets_min_posts_strong_10 == 1 & meets_min_ai_posts_strong_3 == 1
drop if missing(ln_at) | missing(leverage) | missing(rnd_int) | missing(profit_marg)
_winsor_inplace ln_tobins_q_lead ln_at leverage rnd_int profit_marg ai_post_share_strong ai_mom_net_sentiment_strong, p(0.01)

foreach x in ai_post_share_strong ai_mom_net_sentiment_strong {
    di _newline as result "===== ln_tobins_q_lead on `x' (strong-match sample) ====="
    reg ln_tobins_q_lead `x', cluster(gvkey)
    reghdfe ln_tobins_q_lead `x', absorb(year) cluster(gvkey)
}

* To replicate the Python summary.txt numbers (unfiltered sample), uncomment:
* use "data/firm_panel.dta", clear
* keep if meets_min_posts_10 == 1 & meets_min_ai_posts_3 == 1
* drop if missing(ln_at) | missing(leverage) | missing(rnd_int) | missing(profit_marg)
* _winsor_inplace ln_tobins_q_lead ln_at leverage rnd_int profit_marg ai_post_share ai_mom_net_sentiment, p(0.01)
* foreach x in ai_post_share ai_mom_net_sentiment {
*     di _newline as result "===== ln_tobins_q_lead on `x' (unfiltered) ====="
*     reg ln_tobins_q_lead `x', cluster(gvkey)
*     reghdfe ln_tobins_q_lead `x', absorb(year) cluster(gvkey)
* }

* ════════ ROA at t+1 ════════
use "data/firm_panel.dta", clear
keep if meets_min_posts_strong_10 == 1
drop if missing(ln_at) | missing(leverage) | missing(rnd_int) | missing(profit_marg)
_winsor_inplace roa_lead ln_at leverage rnd_int profit_marg mom_net_sentiment_strong engagement_wtd_sentiment role_wtd_sentiment, p(0.01)

foreach x in mom_net_sentiment_strong engagement_wtd_sentiment role_wtd_sentiment {
    di _newline as result "===== roa_lead on `x' (strong-match sample) ====="
    reg roa_lead `x', cluster(gvkey)
    reghdfe roa_lead `x', absorb(year) cluster(gvkey)
}

* ════════ Sales growth at t+1 ════════
use "data/firm_panel.dta", clear
keep if meets_min_posts_strong_10 == 1
drop if missing(ln_at) | missing(leverage) | missing(rnd_int) | missing(profit_marg)
_winsor_inplace sales_growth_lead ln_at leverage rnd_int profit_marg mom_net_sentiment_strong engagement_wtd_sentiment role_wtd_sentiment, p(0.01)

foreach x in mom_net_sentiment_strong engagement_wtd_sentiment role_wtd_sentiment {
    di _newline as result "===== sales_growth_lead on `x' (strong-match sample) ====="
    reg sales_growth_lead `x', cluster(gvkey)
    reghdfe sales_growth_lead `x', absorb(year) cluster(gvkey)
}

* ════════ Stock return at t+1 ════════
use "data/firm_panel.dta", clear
keep if meets_min_posts_strong_10 == 1
drop if missing(ln_at) | missing(leverage) | missing(rnd_int) | missing(profit_marg)
_winsor_inplace stock_return_lead ln_at leverage rnd_int profit_marg mom_net_sentiment_strong engagement_wtd_sentiment role_wtd_sentiment, p(0.01)

foreach x in mom_net_sentiment_strong engagement_wtd_sentiment role_wtd_sentiment {
    di _newline as result "===== stock_return_lead on `x' (strong-match sample) ====="
    reg stock_return_lead `x', cluster(gvkey)
    reghdfe stock_return_lead `x', absorb(year) cluster(gvkey)
}
'''

TS_DO = '''\
* 03_time_series.do — time series of sentiment, AI share, post volume
*
* Two views:
*   A) Aggregate (one line per metric, across all firms / all sources)
*   B) Source split (director / executive / blockholder) — from person_year

capture mkdir plots

* ──── A) Aggregate — annual ────
use "data/firm_panel.dta", clear

preserve
    collapse (mean) ai_post_share ai_mom_net_sentiment mom_net_sentiment ///
             (sum)  n_posts n_ai_posts, by(year)
    tsset year

    twoway tsline ai_post_share, ///
        title("AI post share — annual mean across firms") ///
        ytitle("Mean ai_post_share") xtitle("Year") ///
        name(ts_ai_share, replace)
    graph export "plots/ts_ai_share_annual.png", replace width(1200)

    twoway tsline ai_mom_net_sentiment, ///
        title("AI-post LM sentiment — annual mean across firms") ///
        ytitle("Mean ai_mom_net_sentiment") xtitle("Year") ///
        name(ts_ai_sent, replace)
    graph export "plots/ts_ai_sentiment_annual.png", replace width(1200)

    twoway tsline n_posts, ///
        title("Total LinkedIn posts in corpus — annual") ///
        ytitle("Total posts") xtitle("Year") ///
        name(ts_volume, replace)
    graph export "plots/ts_post_volume_annual.png", replace width(1200)

    twoway tsline n_ai_posts, ///
        title("Total AI posts in corpus — annual") ///
        ytitle("Total AI posts") xtitle("Year") ///
        name(ts_ai_volume, replace)
    graph export "plots/ts_ai_post_volume_annual.png", replace width(1200)
restore

* ──── A2) Aggregate — quarterly (higher resolution) ────
preserve
    use "data/firm_quarterly.dta", clear
    * quarter is a string like "2022Q3" — convert to Stata quarterly date
    gen q = quarterly(quarter, "YQ")
    format q %tq
    * NOTE: firm_quarterly uses POST-level mean_net_sentiment / ai_mean_net_sentiment
    * (mean-of-mean isn't well-defined at quarter grain since people don't post every quarter)
    collapse (mean) ai_post_share ai_mean_net_sentiment mean_net_sentiment ///
             (sum)  n_posts n_ai_posts, by(q)
    tsset q

    twoway tsline ai_post_share, ///
        title("AI post share — quarterly mean across firms") ///
        ytitle("Mean ai_post_share") xtitle("Quarter") ///
        name(ts_ai_share_q, replace)
    graph export "plots/ts_ai_share_quarterly.png", replace width(1200)
restore

* ──── B) Source split — annual ────
use "data/person_year.dta", clear
keep if !missing(source)

preserve
    collapse (mean) ai_post_share mean_lm_sentiment ///
             (sum)  n_posts n_ai_posts, by(year source)
    encode source, gen(source_id)

    twoway (line ai_post_share year if source=="director", lcolor(navy)) ///
           (line ai_post_share year if source=="executive", lcolor(maroon)) ///
           (line ai_post_share year if source=="blockholder", lcolor(forest_green)), ///
        legend(order(1 "Director" 2 "Executive" 3 "Blockholder")) ///
        title("AI post share by source role") ///
        ytitle("Mean ai_post_share") xtitle("Year") ///
        name(ts_ai_share_src, replace)
    graph export "plots/ts_ai_share_by_source.png", replace width(1200)
restore
'''

OUT_DO = '''\
* 04_outliers.do — outlier scanning (mirrors the pre-baked CSVs in outliers/)
*
* Pre-baked snapshots are in outliers/*.csv. The blocks below regenerate them
* inside Stata so you can tweak filters and re-cut.

use "data/firm_panel.dta", clear

* ──── Top firms by ai_post_share (n_posts >= 10) ────
preserve
    keep if n_posts >= 10
    gsort -ai_post_share
    list gvkey ticker company_name_clean year n_posts n_ai_posts ai_post_share ///
         in 1/20, noobs abbreviate(20)
restore

* ──── Biggest YoY jumps in ai_post_share ────
preserve
    sort gvkey year
    by gvkey: gen d_ai_share = ai_post_share - ai_post_share[_n-1]
    gen abs_jump = abs(d_ai_share)
    gsort -abs_jump
    list gvkey ticker company_name_clean year ai_post_share d_ai_share n_posts ///
         in 1/20, noobs abbreviate(20)
restore

* ──── Scatter with labels for prominent firms ────
twoway (scatter ai_post_share year if n_posts < 50, mcolor(gs12%30) msymbol(o)) ///
       (scatter ai_post_share year if n_posts >= 50, ///
            mlabel(ticker) mlabsize(vsmall) msize(small) mcolor(navy)), ///
    legend(off) title("AI post share — firms with >= 50 posts labeled") ///
    name(scatter_ai_share, replace)
graph export "plots/scatter_ai_share.png", replace width(1200)

* ──── Distribution across time ────
graph box ai_post_share, over(year) ///
    title("AI post share distribution by year") ///
    name(box_ai_share, replace)
graph export "plots/box_ai_share_by_year.png", replace width(1200)

* ──── Person-level: top lifetime AI-posters ────
use "data/person_lifetime.dta", clear
keep if n_posts >= 10
gsort -ai_post_share
list author_name source cohort ticker n_posts n_ai_posts ai_post_share ///
     in 1/20, noobs abbreviate(20)
'''

PERSON_DO = '''\
* 05_person_level.do — person-level exploration

use "data/person_lifetime.dta", clear

di as txt "person_lifetime: " _N " people"
tab source
tab cohort

* Distribution of ai_post_share by source role (active posters only)
preserve
    keep if n_posts >= 10
    graph box ai_post_share, over(source) ///
        title("Lifetime AI post share by source role (n_posts>=10)") ///
        name(box_ai_by_src, replace)
    graph export "plots/box_ai_share_by_source.png", replace width(1200)
restore

* Top posters by raw AI-post count
gsort -n_ai_posts
list author_name source cohort ticker n_posts n_ai_posts ai_post_share ///
     in 1/20, noobs abbreviate(20)

* def14a cohort only — top by ai_post_share
preserve
    keep if cohort == "def14a_continuation" & n_posts >= 5
    gsort -ai_post_share
    list author_name source ticker n_posts n_ai_posts ai_post_share ///
         in 1/20, noobs abbreviate(20)
restore

* Person × year — annual trajectories of the top 5 lifetime AI-posters
use "data/person_lifetime.dta", clear
keep if n_posts >= 10
gsort -ai_post_share
keep in 1/5
keep profile_url
tempfile top5
save `top5'

use "data/person_year.dta", clear
merge m:1 profile_url using `top5', keep(match) nogen
encode profile_url, gen(pid)
xtset pid year
twoway (line ai_post_share year, by(profile_url)), ///
    title("Annual AI share — top 5 lifetime AI posters") ///
    name(top5_trajectories, replace)
graph export "plots/top5_person_trajectories.png", replace width(1200)
'''


README_TEMPLATE = """# Stata handoff — AI sentiment exploration

Generated: {date}
Source: LinkedIn posts by directors / executives / blockholders of US public firms.

## What's here

```
stata_handoff_20260527/
├── data/
│   ├── firm_panel.dta            ({n_fp:,} firm-years — regression panel)
│   ├── firm_quarterly.dta        ({n_fq:,} firm-quarters)
│   ├── person_year.dta           ({n_py:,} person-years)
│   └── person_lifetime.dta       ({n_pl:,} people)
├── do/
│   ├── master.do                 ← run this first
│   ├── 01_load.do                load & label firm_panel
│   ├── 02_regressions.do         4 outcomes × 2 specs (OLS, +year FE)
│   ├── 03_time_series.do         time-series plots (aggregate + by source)
│   ├── 04_outliers.do            outlier scanning
│   └── 05_person_level.do        person-level helpers
├── outliers/
│   ├── top_firms_by_ai_share.csv
│   ├── biggest_yoy_ai_jumps.csv
│   ├── top_persons_lifetime_ai.csv
│   └── firms_with_few_persons.csv
├── plots/                        (populated when do-files run)
└── README.md                     (this file)
```

## Setup

1. Open Stata, `cd` to this folder.
2. Install dependencies:
   ```
   ssc install reghdfe
   ssc install ftools
   ```
3. Run `do do/master.do`.

## Key decisions baked in

- **AI keyword set:** strict 7-term list (artificial intelligence, ai, large
  language model, llm, generative ai, gen ai, chatgpt), word-boundary regex,
  case-insensitive. Applied in upstream sentiment scoring; `is_ai_related`
  carried through to all four files.
- **Sentiment aggregation: mean-of-mean (person-equal-weighting).**
  `mom_net_sentiment` and `ai_mom_net_sentiment` on
  `firm_panel.dta` are computed as: per-person mean within the firm-year,
  then mean across persons. Each individual gets one vote regardless of
  how prolific they are. This is the conservative measure — one heavy
  poster can't dominate a firm-year's sentiment value. (Note:
  `firm_quarterly.dta` still uses the post-level `mean_net_sentiment` /
  `ai_mean_net_sentiment` because mean-of-mean is awkward at quarter
  grain — people don't post every quarter.)
- **Revelio strong-match is the default regression sample.**
  Firm-year metrics computed on Revelio-confirmed profiles (URL + name + company
  match) are suffixed `_strong`:
  - `n_posts_strong`, `n_ai_posts_strong`, `n_persons_strong`,
    `ai_post_share_strong`,
  - `mom_net_sentiment_strong`, `ai_mom_net_sentiment_strong`.

  The unfiltered versions are also on the panel (no suffix) — Revelio's
  weakness is that a false negative loses real signal. Toggle flags:
  - `has_strong_match` (= 1 if the firm-year has *any* strong-matched posts)
  - `meets_min_posts_strong_10` / `meets_min_ai_posts_strong_3` — strong-match
    versions of the volume filters.
  - `meets_min_posts_10` / `meets_min_ai_posts_3` — unfiltered-sample versions
    that match the Python `summary.txt` filter logic.

  `02_regressions.do` defaults to `meets_min_posts_strong_*` filters and the
  `_strong` regressors. To replicate Python `summary.txt` exactly, swap to the
  non-`_strong` versions (commented blocks already in the do-file).

  Person files (`person_year.dta`, `person_lifetime.dta`) carry a
  `strong_match` column directly so you can `keep if strong_match == 1`.
- **No n_posts filter applied in the data file.** All firm-years are present
  in `firm_panel.dta`. The `meets_min_*` flags above are toggles, not
  pre-applied filters.
- **No pre-winsorization.** Raw values are in the .dta file so outlier
  hunting surfaces real extremes. Winsorization at 1% / 99% happens
  inside `02_regressions.do` after the filter is applied — matches Python.
- **No-post person-years dropped.** A person who didn't post in year *t* has
  no row for that year — `person_year.dta` is unbalanced.
- **Multi-board people:** posts deduplicated by (profile_url, post_url)
  before person-level aggregation.
- **Outcomes lead by 1 year.** All `*_lead` columns are at t+1 relative to
  the sentiment year — matches the Python regression structure.
- **Winsorized at 1%** on outcomes, controls, and sentiment regressors.

## Known data-quality limitations (read before interpreting)

The strong-match filter has both **false positives** (caught) and **false
negatives** (NOT caught). Spot-checks while building this handoff:

### Revelio caught these false positives (good)
URLs that scraped real LinkedIn data but the wrong person:

- "Nathan May" → URL `/in/davidwgrant` (B2B newsletter founder, not at Meta).
  Eliminated 514 posts from Meta's pre-2017 sample — the dominant pre-ChatGPT
  poster was an unrelated person.
- "Joni Klippert" → URL `/in/dana-costello-white`. Wrong person.
- See `outliers/dropped_by_revelio.csv` for the top 50 by post count.

### Revelio missed these false negatives (bad)

Real senior employees that the strong-match filter incorrectly drops because
Revelio's role records for them list one employer name and Compustat's
records use another:

| Person | Compustat name | Why Revelio drops them |
|---|---|---|
| **Sundar Pichai** (Google CEO) | ALPHABET INC | Revelio role records likely say "Google" / "Google LLC". Even fuzzy company match fails. |
| **Kent Walker** (Google President of Global Affairs) | ALPHABET INC | Same pattern. 502 posts, 50% AI share — silently dropped. |
| **Philipp Schindler** (Google SVP/CBO) | ALPHABET INC | Same pattern. |

By contrast, **Eric Schmidt** matches because he held the Chairman role at
"Alphabet" (the parent) directly, so his Revelio record contains the right
employer name. **Ruth Porat** matches on her *previous* employer (Morgan
Stanley) because Compustat's lookup happened to use that anchor — the
strong-match is coincidental, not a real validation that she's at Alphabet now.

### Implications

- **For firm-level signal at any company whose corporate vs. operating name
  differs (Alphabet/Google, Meta/Facebook, X/Twitter, etc.), expect
  systematic under-coverage in the strong-match sample.** The unfiltered
  metrics are likely closer to truth for these firms.
- **The Tobin's Q effect in the strong-match sample is conservative — likely
  attenuated by missing the most AI-vocal voices at the biggest tech firms.**
- The right long-term fix is in the Revelio match logic (alias Alphabet ↔
  Google, etc.). See the validation notebook at
  `src/revelio/redivis_crosscheck_notebook_v2.ipynb`.

## Regressions (`do/02_regressions.do`)

Each outcome × regressor combination runs two specs:

| Spec | Stata command | Notes |
|------|---------------|-------|
| Pooled OLS | `reg y x, cluster(gvkey)` | Cluster SEs on firm |
| + Year FE  | `reghdfe y x, absorb(year) cluster(gvkey)` | Year fixed effects, cluster SEs on firm |

Outcome × regressor pairs:

| Outcome (LHS) | Regressors (RHS) |
|---------------|-------------------|
| `ln_tobins_q_lead` | `ai_post_share`, `ai_mom_net_sentiment` |
| `roa_lead` | `mom_net_sentiment`, `engagement_wtd_sentiment`, `role_wtd_sentiment` |
| `sales_growth_lead` | same as ROA |
| `stock_return_lead` | same as ROA |

To reproduce the Python summary.txt numbers exactly, uncomment
`keep if meets_min_posts_10 == 1` at the top of `02_regressions.do` —
the Python regression filtered to n_posts >= 10 by default.

## What to look at first

1. Run `do do/master.do`. It produces all the regression tables (in the
   results window) plus PNGs in `plots/`.
2. Scan `outliers/top_firms_by_ai_share.csv` and `top_persons_lifetime_ai.csv`
   for suspicious entries (e.g., 100% AI share with n_posts ≤ 5 — noisy).
3. Look at `plots/ts_ai_share_annual.png` — the post-Nov-2022 ChatGPT jump
   should be visible.
4. `plots/ts_ai_share_by_source.png` — does the AI signal sit more in
   executives, directors, or blockholders?

## Column glossary

### `firm_panel.dta` (firm × year)

| Column | Description |
|--------|-------------|
| `gvkey`, `ticker`, `company_name_clean`, `year` | Keys |
| `n_posts`, `n_ai_posts`, `n_persons` | Volume / coverage |
| `ai_post_share` | n_ai_posts / n_posts |
| `mom_net_sentiment` | Mean of person-mean LM sentiment within firm-year (person-equal-weighting) |
| `ai_mom_net_sentiment` | Same, restricted to AI-related posts |
| `engagement_wtd_sentiment` | Reaction-weighted mean LM sentiment |
| `role_wtd_sentiment` | Role-hierarchy-weighted mean LM sentiment |
| `mean_polarity` | Mean (pos−neg)/(pos+neg) |
| `frac_positive_posts`, `frac_negative_posts` | Share of posts with positive / negative LM net sentiment |
| `tobins_q`, `ln_tobins_q`, `roa`, `sales_growth`, `stock_return` | Outcomes at year t (the sentiment year) — contemporaneous |
| `ln_tobins_q_lead`, `roa_lead`, `sales_growth_lead`, `stock_return_lead` | Outcomes at t+1 — predictive |
| `ln_at`, `leverage`, `rnd_int`, `profit_marg` | Controls (not used by default) |
| `sich` | SIC industry code |
| `meets_min_posts_10` | 1 if n_posts >= 10 |

### `firm_quarterly.dta` (firm × quarter)

Same columns minus controls / lead outcomes. `quarter` is a string like
`"2022Q3"`; convert to Stata quarterly date with `gen q = quarterly(quarter, "YQ")`.

### `person_year.dta` / `person_lifetime.dta`

| Column | Description |
|--------|-------------|
| `profile_url` | LinkedIn profile URL — person ID |
| `author_name`, `author_headline` | Display name / headline from LinkedIn |
| `source` | Modal: director / executive / blockholder / director\\|executive / def14a_serper |
| `cohort` | `def14a_continuation` if all rows are def14a; else `initial_universe` |
| `gvkey`, `ticker` | Modal firm affiliation (def14a rows deprioritized for gvkey since they have gvkey=NaN) |
| `year` (person-year only) | Calendar year |
| `n_posts`, `n_ai_posts`, `ai_post_share` | Volume + AI share |
| `mean_lm_sentiment` | Mean LM net sentiment across the person's posts |
| `ai_mean_lm_sentiment` | Mean LM sentiment on AI posts only |
| `mean_finbert_score` | Mean FinBERT score = P(pos) − P(neg) |
| `engagement_wtd_lm_sentiment` | Reaction-weighted mean LM sentiment |
| `first_post_date`, `last_post_date`, `n_years_active` (lifetime only) | Tenure metrics |

## Source files (for the record)

- Sentiment annual: `outputs/sentiment_results/company_sentiment_annual_20260527_164007.csv`
- Sentiment quarterly: `outputs/sentiment_results/company_sentiment_quarterly_20260527_164007.csv`
- Post-level scored: `outputs/sentiment_results/sentiment_posts_scored_unique_20260527.csv`
- Person attrs lookup: `outputs/sentiment_results/sentiment_all_posts_full_coverage_20260527.csv`
- Profiles: `data/processed/all_people_linkedin_urls/scraped_posts_combined/profiles_combined_v2_20260527.csv`
- Funda: `data/extracted/compustat/funda_20260425_135322.csv`
- CRSP: `data/extracted/crsp/crsp_annual_returns_20260428_132604.csv`

## Replication target

The Python regression outputs live at
`outputs/sanity_checks/regression_{{tobins_q,roa,sales_growth,stock_return}}_20260527_16460*/summary.txt`.
With `keep if meets_min_posts_10 == 1` and a year FE spec, the Stata
coefficients should match the "(2) + Year FE" rows in those files to
3 decimals.
"""


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    for k, v in DEFAULTS.items():
        ap.add_argument(f"--{k.replace('_','-')}", type=Path, default=v)
    ap.add_argument("--run", action="store_true", help="Actually write outputs")
    ap.add_argument("--stats", action="store_true")
    args = ap.parse_args()

    out_dir = args.out_dir
    (out_dir / "data").mkdir(parents=True, exist_ok=True)
    (out_dir / "do").mkdir(parents=True, exist_ok=True)
    (out_dir / "outliers").mkdir(parents=True, exist_ok=True)
    (out_dir / "plots").mkdir(parents=True, exist_ok=True)

    print(f"Output dir: {out_dir}\n")

    # ── 0. firm-year metrics (unfiltered AND strong-match parallel) ──
    fym = compute_firm_year_metrics(args.full_coverage, args.revelio)

    # Per-profile strong_match map for person files
    sm_map = compute_strong_match_map(args.revelio)

    # ── 1. firm_panel ──
    firm_panel = build_firm_panel(args.company_annual, args.funda, args.crsp, fym)

    # ── 2. firm_quarterly ──
    firm_quarterly = build_firm_quarterly(args.company_quarterly)

    # ── 3. person modal lookup ──
    modal = build_person_modal_lookup(args.full_coverage, args.def14a_outcomes)

    # ── 4. person files ──
    profiles = pd.read_csv(args.profiles_v2, low_memory=False)
    profiles = profiles.drop_duplicates(subset=["profile_url"], keep="first")
    print(f"[profiles] {len(profiles):,} profile metadata rows")

    posts = load_unique_posts(args.posts_scored)
    person_year = build_person_year(posts, modal, profiles, sm_map)
    person_lifetime = build_person_lifetime(posts, modal, profiles, sm_map)

    if args.stats:
        for name, df in (("firm_panel", firm_panel),
                         ("firm_quarterly", firm_quarterly),
                         ("person_year", person_year),
                         ("person_lifetime", person_lifetime)):
            print(f"\n=== {name} ({len(df):,} rows × {len(df.columns)} cols) ===")
            print(df.head(3).to_string())

    if not args.run:
        print("\nDry run — pass --run to write files.")
        return

    # ── 5. pre-baked outlier CSVs ──
    print("\n[outliers]")
    write_outlier_csvs(out_dir, firm_panel, person_lifetime)

    # ── 6. write .dta files ──
    print("\n[write] .dta files →")
    write_dta(firm_panel, out_dir / "data" / "firm_panel.dta")
    write_dta(firm_quarterly, out_dir / "data" / "firm_quarterly.dta")
    write_dta(person_year, out_dir / "data" / "person_year.dta")
    write_dta(person_lifetime, out_dir / "data" / "person_lifetime.dta")

    # ── 7. do-files ──
    print("\n[write] do-files →")
    (out_dir / "do" / "master.do").write_text(MASTER_DO)
    (out_dir / "do" / "01_load.do").write_text(LOAD_DO)
    (out_dir / "do" / "02_regressions.do").write_text(REG_DO)
    (out_dir / "do" / "03_time_series.do").write_text(TS_DO)
    (out_dir / "do" / "04_outliers.do").write_text(OUT_DO)
    (out_dir / "do" / "05_person_level.do").write_text(PERSON_DO)
    print(f"        wrote do/*.do (6 files)")

    # ── 8. README ──
    readme = README_TEMPLATE.format(
        date=datetime.now().strftime("%Y-%m-%d %H:%M"),
        n_fp=len(firm_panel), n_fq=len(firm_quarterly),
        n_py=len(person_year), n_pl=len(person_lifetime),
    )
    (out_dir / "README.md").write_text(readme)
    print(f"        wrote README.md")

    print(f"\nDone. Files in {out_dir}/")


if __name__ == "__main__":
    main()

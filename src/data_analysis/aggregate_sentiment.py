#!/usr/bin/env python3
"""
Company-Level Sentiment Aggregation
=====================================
Aggregates post-level L-M sentiment scores to company × year and
company × quarter, with multiple weighting schemes.

Prerequisites:
    - outputs/sentiment_results/sentiment_all_posts_*.csv  (auto-detected: newest)
    - data/extracted/combined/all_people.csv               (gvkey, ticker, source, position)
    - data/revelio/revelio_validation_summary.csv          (Revelio validation flags;
      download Cell 12 output from src/revelio/redivis_crosscheck_notebook.ipynb on Redivis)

Usage:
    python3 src/data_analysis/aggregate_sentiment.py --stats
    python3 src/data_analysis/aggregate_sentiment.py --run
    python3 src/data_analysis/aggregate_sentiment.py --run --stats
    python3 src/data_analysis/aggregate_sentiment.py --prototype 50000 --stats
    python3 src/data_analysis/aggregate_sentiment.py --input path/to/sentiment.csv --run

Outputs (in outputs/sentiment_results/):
    company_sentiment_annual_YYYYMMDD_HHMMSS.csv           all posts, company × year
    company_sentiment_quarterly_YYYYMMDD_HHMMSS.csv        all posts, company × quarter
    company_sentiment_annual_ai_only_YYYYMMDD_HHMMSS.csv   AI posts only, company × year
    company_sentiment_quarterly_ai_only_YYYYMMDD_HHMMSS.csv
    aggregation_summary_YYYYMMDD_HHMMSS.json
"""

import argparse
import json
import sys
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def default_paths():
    return {
        "sentiment_dir": PROJECT_ROOT / "outputs" / "sentiment_results",
        # all_linkedin_urls has clean names + gvkey/ticker/source/position + linkedin_url
        "all_people":    PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls.csv",
        "revelio":       PROJECT_ROOT / "data" / "revelio" / "revelio_validation_summary.csv",
        "output_dir":    PROJECT_ROOT / "outputs" / "sentiment_results",
    }


def find_latest_sentiment(sentiment_dir: Path) -> Path:
    """Return the newest sentiment_all_posts_*.csv in the directory."""
    candidates = sorted(sentiment_dir.glob("sentiment_all_posts_*.csv"))
    if not candidates:
        sys.exit(f"[ERROR] No sentiment_all_posts_*.csv found in {sentiment_dir}")
    return candidates[-1]


# ──────────────────────────────────────────────
# Role weighting
# ──────────────────────────────────────────────

ROLE_WEIGHTS = [
    ("chief executive", 3), ("ceo",              3), ("president",        3),
    ("chief financial",  2), ("chief operating",  2), ("chief technology", 2),
    ("chief information",2), ("chief marketing",  2), ("chief",            2),
    ("director",         1), ("blockholder",      1),
]


def role_weight(position: str) -> int:
    if pd.isna(position):
        return 1
    pos = str(position).lower()
    for key, w in ROLE_WEIGHTS:
        if key in pos:
            return w
    return 1


# ──────────────────────────────────────────────
# Load & merge
# ──────────────────────────────────────────────

def load_sentiment(path: Path, nrows=None) -> pd.DataFrame:
    print(f"[load] Reading sentiment file: {path.name}")
    # post_text contains embedded newlines that overflow the default C parser
    # buffer; the explicit lineterminator + on_bad_lines='skip' is the
    # standard workaround used elsewhere in the project (see CLAUDE.md).
    df = pd.read_csv(
        path,
        nrows=nrows,
        low_memory=False,
        engine="c",
        lineterminator="\n",
        on_bad_lines="skip",
    )
    print(f"       {len(df):,} rows, {df.shape[1]} columns")
    return df


def merge_all_people(df: pd.DataFrame, all_people_path: Path) -> pd.DataFrame:
    """Merge gvkey, ticker, source, position from all_linkedin_urls.csv.

    The corporate sentiment file already carries gvkey/ticker/source/position
    inline (from the raw posts file), while the Congress variant does not.
    Drop overlapping columns from the right side so we don't get _x/_y
    suffixes — df's existing values win.
    """
    print(f"[merge] Loading all_linkedin_urls.csv …")
    ap = pd.read_csv(
        all_people_path,
        usecols=["company_name_clean", "person_name_clean", "gvkey", "ticker", "source", "position",
                 "linkedin_url", "verified"],
        low_memory=False,
    )
    ap = ap.drop_duplicates(subset=["company_name_clean", "person_name_clean"])

    join_keys = ["company_name_clean", "person_name_clean"]
    overlap = [c for c in ap.columns if c in df.columns and c not in join_keys]
    if overlap:
        print(f"       Right-side already present on left, dropping from merge: {overlap}")
        ap = ap.drop(columns=overlap)

    before = len(df)
    df = df.merge(ap, on=join_keys, how="left")
    if "gvkey" in df.columns:
        print(f"       Matched {df['gvkey'].notna().sum():,} / {before:,} rows to a gvkey")
    else:
        print(f"       No gvkey column on either side — downstream firm-level aggregations will lack gvkey.")
    return df


def merge_revelio(df: pd.DataFrame, revelio_path: Path) -> pd.DataFrame:
    """Merge Revelio validation flags.

    Uses pre-computed `strong_match` (strict company-name match) and, if
    available, `strong_match_fuzzy` (looser SequenceMatcher-based company
    match — see src/revelio/redivis_crosscheck_notebook.ipynb Cell 6).
    """
    if not revelio_path.exists():
        print(f"[warn] Revelio validation file not found: {revelio_path}")
        print("       match_tier will be 'unverified' for all rows.")
        print("       To add validation: run Cell 12 in src/revelio/redivis_crosscheck_notebook.ipynb")
        print("       and save the output to data/revelio/revelio_validation_summary.csv")
        df["revelio_url_match"]    = False
        df["strong_match"]         = False
        df["strong_match_fuzzy"]   = False
        df["match_tier"]           = "unverified"
        df["match_tier_fuzzy"]     = "unverified"
        return df

    print(f"[merge] Loading Revelio validation summary …")
    # Probe the header to know whether the fuzzy column is present
    available = pd.read_csv(revelio_path, nrows=0).columns.tolist()
    has_fuzzy = "strong_match_fuzzy" in available
    cols = ["linkedin_url", "revelio_url_match", "strong_match"]
    if has_fuzzy:
        cols.append("strong_match_fuzzy")
    else:
        print("       (no strong_match_fuzzy column — running strict-only)")

    rv = pd.read_csv(revelio_path, usecols=cols, low_memory=False)
    rv = rv.drop_duplicates(subset=["linkedin_url"])
    rv = rv.rename(columns={"linkedin_url": "profile_url"})

    df = df.merge(rv, on="profile_url", how="left")

    df["revelio_url_match"]  = df["revelio_url_match"].fillna(False).astype(bool)
    df["strong_match"]       = df["strong_match"].fillna(False).astype(bool)
    if has_fuzzy:
        df["strong_match_fuzzy"] = df["strong_match_fuzzy"].fillna(False).astype(bool)
    else:
        df["strong_match_fuzzy"] = df["strong_match"]   # falls back to strict

    df["match_tier"] = "unverified"
    df.loc[df["revelio_url_match"] & ~df["strong_match"], "match_tier"] = "url_only"
    df.loc[df["strong_match"], "match_tier"] = "strong"

    df["match_tier_fuzzy"] = "unverified"
    df.loc[df["revelio_url_match"] & ~df["strong_match_fuzzy"], "match_tier_fuzzy"] = "url_only"
    df.loc[df["strong_match_fuzzy"], "match_tier_fuzzy"] = "strong"

    n_strong = df["strong_match"].sum()
    n_strong_f = df["strong_match_fuzzy"].sum()
    print(f"       Strong matches (strict): {n_strong:,} posts ({n_strong/len(df)*100:.1f}%)")
    print(f"       Strong matches (fuzzy):  {n_strong_f:,} posts "
          f"({n_strong_f/len(df)*100:.1f}%)  [+{n_strong_f - n_strong:,} via fuzzy]")
    return df


# ──────────────────────────────────────────────
# Time keys
# ──────────────────────────────────────────────

def add_time_keys(df: pd.DataFrame) -> pd.DataFrame:
    df["post_date"] = pd.to_datetime(df["post_date"], errors="coerce")
    df["year"]      = df["post_date"].dt.year
    df["quarter"]   = df["post_date"].dt.to_period("Q").astype(str)  # e.g. "2022Q3"
    n_null = df["year"].isna().sum()
    if n_null:
        print(f"[warn] {n_null:,} rows have unparseable post_date; excluded from aggregation")
    return df


# ──────────────────────────────────────────────
# Aggregation
# ──────────────────────────────────────────────

GROUP_KEYS_ANNUAL    = ["company_name_clean", "gvkey", "ticker", "year"]
GROUP_KEYS_QUARTERLY = ["company_name_clean", "gvkey", "ticker", "quarter"]


def _safe_engagement_wtd(sub: pd.DataFrame) -> float:
    """Engagement-weighted mean sentiment; falls back to simple mean if total reactions = 0."""
    w = sub["reactions_total"].fillna(0)
    total = w.sum()
    if total == 0:
        return sub["lm_net_sentiment"].mean()
    return (sub["lm_net_sentiment"] * w).sum() / total


def _role_wtd(sub: pd.DataFrame) -> float:
    """Role-hierarchy weighted mean sentiment."""
    w = sub["_role_weight"]
    total = w.sum()
    if total == 0:
        return sub["lm_net_sentiment"].mean()
    return (sub["lm_net_sentiment"] * w).sum() / total


def aggregate(df: pd.DataFrame, group_keys: list) -> pd.DataFrame:
    """Core aggregation — returns one row per group."""
    df = df.copy()
    df["_role_weight"] = df.get("position", pd.Series(dtype=str)).apply(role_weight)

    records = []
    for keys, sub in df.groupby(group_keys, dropna=False):
        key_dict = dict(zip(group_keys, keys if isinstance(keys, tuple) else (keys,)))

        n_posts   = len(sub)
        n_persons = sub["person_name_clean"].nunique() if "person_name_clean" in sub.columns else np.nan
        n_ai      = int(sub["is_ai_related"].sum()) if "is_ai_related" in sub.columns else 0

        net = sub["lm_net_sentiment"]
        pos = sub["lm_positive_ratio"]   if "lm_positive_ratio"   in sub.columns else pd.Series(dtype=float)
        neg = sub["lm_negative_ratio"]   if "lm_negative_ratio"   in sub.columns else pd.Series(dtype=float)
        unc = sub["lm_uncertainty_ratio"] if "lm_uncertainty_ratio" in sub.columns else pd.Series(dtype=float)
        pol = sub["lm_polarity"]          if "lm_polarity"          in sub.columns else pd.Series(dtype=float)

        n_strong         = int(sub["strong_match"].sum())       if "strong_match"       in sub.columns else 0
        n_strong_fuzzy   = int(sub["strong_match_fuzzy"].sum()) if "strong_match_fuzzy" in sub.columns else n_strong
        n_url_only       = int((sub.get("match_tier", "") == "url_only").sum())

        row = {
            **key_dict,
            # Coverage
            "n_posts":               n_posts,
            "n_persons":             n_persons,
            "mean_posts_per_person": (n_posts / n_persons) if n_persons else np.nan,
            "n_ai_posts":            n_ai,
            "n_strong_match_posts":       n_strong,
            "n_strong_match_fuzzy_posts": n_strong_fuzzy,
            "n_url_only_posts":      n_url_only,
            "n_unverified_posts":    n_posts - n_strong - n_url_only,
            "ai_post_share":         n_ai / n_posts if n_posts else np.nan,
            "strong_match_share":         n_strong / n_posts       if n_posts else np.nan,
            "strong_match_fuzzy_share":   n_strong_fuzzy / n_posts if n_posts else np.nan,
            "sparse_flag":           int(n_posts < 5),
            # Equal-weight (standard L-M approach)
            "mean_net_sentiment":    net.mean(),
            "median_net_sentiment":  net.median(),
            "sum_net_sentiment":     net.sum(),
            # Polarity / directional
            "mean_polarity":         pol.mean() if len(pol) else np.nan,
            "mean_positive_ratio":   pos.mean() if len(pos) else np.nan,
            "mean_negative_ratio":   neg.mean() if len(neg) else np.nan,
            "mean_uncertainty_ratio":unc.mean() if len(unc) else np.nan,
            # Density measures (Baker-Wurgler style)
            "frac_positive_posts":   (net > 0).mean(),
            "frac_negative_posts":   (net < 0).mean(),
            # Weighted averages
            "engagement_wtd_sentiment": _safe_engagement_wtd(sub),
            "role_wtd_sentiment":       _role_wtd(sub),
            # Volume
            "total_words": int(sub["lm_word_count"].sum()) if "lm_word_count" in sub.columns else 0,
        }

        # AI-post density within this group
        if "is_ai_related" in sub.columns and n_ai > 0:
            ai_sub = sub[sub["is_ai_related"]]
            row["ai_mean_net_sentiment"] = ai_sub["lm_net_sentiment"].mean()
            row["ai_sum_net_sentiment"]  = ai_sub["lm_net_sentiment"].sum()
            row["ai_frac_positive"]      = (ai_sub["lm_net_sentiment"] > 0).mean()
        else:
            row["ai_mean_net_sentiment"] = np.nan
            row["ai_sum_net_sentiment"]  = np.nan
            row["ai_frac_positive"]      = np.nan

        records.append(row)

    result = pd.DataFrame(records)
    return result


# ──────────────────────────────────────────────
# Stats printer
# ──────────────────────────────────────────────

def print_stats(label: str, df: pd.DataFrame):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"  Rows:            {len(df):,}")
    if "company_name_clean" in df.columns:
        print(f"  Companies:       {df['company_name_clean'].nunique():,}")
    if "gvkey" in df.columns:
        print(f"  With gvkey:      {df['gvkey'].notna().sum():,}")
    if "n_posts" in df.columns:
        print(f"  Total posts:     {df['n_posts'].sum():,.0f}")
        print(f"  Median posts/co: {df['n_posts'].median():.1f}")
        print(f"  Sparse (n<5):    {df['sparse_flag'].sum():,}  ({df['sparse_flag'].mean()*100:.1f}%)")
    if "mean_net_sentiment" in df.columns:
        print(f"  Mean sentiment (equal-wt):      {df['mean_net_sentiment'].mean():.4f}")
        print(f"  Mean sentiment (eng-wt):        {df['engagement_wtd_sentiment'].mean():.4f}")
        print(f"  Mean sentiment (role-wt):       {df['role_wtd_sentiment'].mean():.4f}")
        print(f"  Mean frac positive posts:       {df['frac_positive_posts'].mean():.3f}")
    if "ai_post_share" in df.columns:
        print(f"  Mean AI post share:             {df['ai_post_share'].mean():.4f}")
    if "strong_match_share" in df.columns:
        print(f"  Mean strong-match share (strict): {df['strong_match_share'].mean():.4f}")
    if "strong_match_fuzzy_share" in df.columns:
        print(f"  Mean strong-match share (fuzzy):  {df['strong_match_fuzzy_share'].mean():.4f}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Aggregate post-level sentiment to company × year / quarter.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input",      type=str, help="Override auto-detected sentiment CSV")
    parser.add_argument("--out-dir",    type=str, help="Override output directory")
    parser.add_argument("--min-posts",  type=int, default=1,
                        help="Minimum posts per company-period to include (default: 1)")
    parser.add_argument("--prototype",  type=int, metavar="N",
                        help="Run on first N rows only (for testing)")
    parser.add_argument("--stats",      action="store_true", help="Print summary stats")
    parser.add_argument("--run",        action="store_true", help="Write output CSVs (dry-run otherwise)")
    args = parser.parse_args()

    paths = default_paths()
    sentiment_path = Path(args.input) if args.input else find_latest_sentiment(paths["sentiment_dir"])
    out_dir        = Path(args.out_dir) if args.out_dir else paths["output_dir"]
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── Load ──────────────────────────────────────────────────────────
    df = load_sentiment(sentiment_path, nrows=args.prototype)
    df = merge_all_people(df, paths["all_people"])
    df = merge_revelio(df, paths["revelio"])
    df = add_time_keys(df)

    # Drop rows with no date (can't assign to period)
    df = df[df["year"].notna()].copy()

    # ── Build subsets ─────────────────────────────────────────────────
    df_ai = df[df["is_ai_related"]].copy() if "is_ai_related" in df.columns else pd.DataFrame()

    subsets = {
        "annual":             (df,    GROUP_KEYS_ANNUAL),
        "quarterly":          (df,    GROUP_KEYS_QUARTERLY),
        "annual_ai_only":     (df_ai, GROUP_KEYS_ANNUAL),
        "quarterly_ai_only":  (df_ai, GROUP_KEYS_QUARTERLY),
    }

    results = {}
    for name, (data, keys) in subsets.items():
        if data.empty:
            print(f"[skip] {name} — no rows")
            results[name] = pd.DataFrame()
            continue
        print(f"\n[aggregate] {name} ({len(data):,} posts) …")
        agg = aggregate(data, keys)
        if args.min_posts > 1:
            agg = agg[agg["n_posts"] >= args.min_posts]
        results[name] = agg
        if args.stats:
            print_stats(name, agg)

    # ── Summary JSON ──────────────────────────────────────────────────
    summary = {
        "timestamp": ts,
        "input_file": sentiment_path.name,
        "prototype_n": args.prototype,
        "min_posts_filter": args.min_posts,
        "revelio_available": paths["revelio"].exists(),
        "tables": {
            name: {
                "rows": len(df_),
                "companies": int(df_["company_name_clean"].nunique()) if not df_.empty else 0,
            }
            for name, df_ in results.items()
        },
    }

    # ── Write ─────────────────────────────────────────────────────────
    if not args.run:
        print("\n[dry-run] Pass --run to write output files.")
        print("Summary:")
        for name, info in summary["tables"].items():
            print(f"  {name:<35} {info['rows']:>6,} rows  {info['companies']:>5,} companies")
        return

    for name, df_ in results.items():
        if df_.empty:
            continue
        out_path = out_dir / f"company_sentiment_{name}_{ts}.csv"
        df_.to_csv(out_path, index=False)
        print(f"[write] {out_path.name}  ({len(df_):,} rows)")

    summary_path = out_dir / f"aggregation_summary_{ts}.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"[write] {summary_path.name}")


if __name__ == "__main__":
    main()

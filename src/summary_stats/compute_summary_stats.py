"""
Summary statistics — the headline descriptives, in one place.
==============================================================

Computes the project's important summary statistics from the canonical source
of truth (data/canonical/current/) and writes them to
outputs/summary_stats/<timestamp>/ as CSV tables + a summary.md.

What it produces:
  1. Panel coverage          — firm-years, firms, strong-match coverage, year range
  2. Sentiment descriptives  — distribution of AI post-share & AI sentiment,
                               including the mass point at zero
  3. AI sentiment by size    — mean AI sentiment across firm-size (ln_at) deciles
  4. AI sentiment by R&D     — across R&D-intensity (rnd_int) deciles
  5. AI sentiment by industry— across SIC divisions
  6. Revelio match rate      — strong-match rate by source (executive/director/...)
  7. DEF 14A director status — incumbent / new_nominee / mid_year_appointee / ...

Sentiment convention: AI sentiment is imputed to 0 for firm-years that posted
but had no AI posts (n_posts>=1 & n_ai_posts==0), left NaN where the firm did
not post at all. This is the regression-time imputation (see CLAUDE.md / the
2026-05-30 decision); the canonical data files themselves keep true NaN.

Usage:
    python3 src/summary_stats/compute_summary_stats.py
    python3 src/summary_stats/compute_summary_stats.py --strong   # use _strong vars
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

try:
    import numpy as np
    import pandas as pd
except ImportError:
    print("Missing dependency: pip install pandas numpy", file=sys.stderr)
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CANONICAL = PROJECT_ROOT / "data" / "canonical" / "current"
OUTPUT_BASE = PROJECT_ROOT / "outputs" / "summary_stats"


def _imputed_ai_sentiment(df, sent_col, share_col, nposts_col):
    """ai_sent_new: AI sentiment, 0 where the firm posted but said nothing about
    AI, NaN where it didn't post at all. Mirrors John's `replace ai_sent_new=0`."""
    out = df[sent_col].copy()
    posted_no_ai = (df[nposts_col].fillna(0) >= 1) & (df[share_col].fillna(0) == 0)
    out = out.where(~posted_no_ai, 0.0)
    return out


def _sic_division(sich):
    """4-digit SIC -> broad division label."""
    if pd.isna(sich):
        return "Unknown"
    s = int(sich)
    if s < 1000:   return "Agriculture/Forestry/Fishing"
    if s < 1500:   return "Mining"
    if s < 1800:   return "Construction"
    if s < 4000:   return "Manufacturing"
    if s < 5000:   return "Transportation/Utilities"
    if s < 5200:   return "Wholesale Trade"
    if s < 6000:   return "Retail Trade"
    if s < 6800:   return "Finance/Insurance/RealEstate"
    if s < 9000:   return "Services"
    return "Public Admin/Other"


def panel_coverage(fp):
    rows = {
        "firm_years": len(fp),
        "unique_firms": fp["gvkey"].nunique(),
        "year_min": int(fp["year"].min()),
        "year_max": int(fp["year"].max()),
        "firm_years_with_strong_match": int((fp["has_strong_match"].fillna(0) == 1).sum()),
        "firm_years_with_any_ai_post": int((fp["n_ai_posts"].fillna(0) > 0).sum()),
        "firm_years_posted_no_ai": int(((fp["n_posts"].fillna(0) >= 1) &
                                        (fp["n_ai_posts"].fillna(0) == 0)).sum()),
    }
    return pd.DataFrame([rows]).T.rename(columns={0: "value"})


def sentiment_descriptives(fp, sent_col, share_col):
    sub = fp[[share_col, sent_col]].copy()
    desc = sub.describe(percentiles=[.1, .25, .5, .75, .9]).T
    desc["pct_zero_share"] = [(fp[share_col].fillna(0) == 0).mean(), np.nan]
    return desc


def by_bin(fp, group_label, group_series, value_series, n_bins=10):
    d = pd.DataFrame({"g": group_series, "v": value_series}).dropna()
    if d.empty:
        return pd.DataFrame()
    try:
        d["bin"] = pd.qcut(d["g"], n_bins, labels=False, duplicates="drop")
    except ValueError:
        d["bin"] = pd.cut(d["g"], n_bins, labels=False)
    out = (d.groupby("bin")
             .agg(n=("v", "size"), g_mean=("g", "mean"), v_mean=("v", "mean"),
                  v_median=("v", "median"))
             .reset_index())
    out.insert(0, "dimension", group_label)
    return out


def by_industry(fp, value_series):
    d = pd.DataFrame({"div": fp["sich"].map(_sic_division), "v": value_series}).dropna(subset=["v"])
    out = (d.groupby("div")
             .agg(n_firm_years=("v", "size"), ai_sent_mean=("v", "mean"),
                  ai_sent_median=("v", "median"))
             .sort_values("ai_sent_mean", ascending=False)
             .reset_index())
    return out


def revelio_match_by_source(rev_path):
    rv = pd.read_csv(rev_path, low_memory=False)
    for c in ["revelio_url_match", "strong_match_either", "strong_match_either_fuzzy"]:
        rv[c] = rv[c].fillna(False).astype(bool)
    # person-level (collapse multi-board): strong-match on ANY board
    ppl = (rv.dropna(subset=["linkedin_url"])
             .groupby("linkedin_url")
             .agg(source=("source", "first"),
                  url_any=("revelio_url_match", "max"),
                  strong=("strong_match_either", "max"),
                  strong_fuzzy=("strong_match_either_fuzzy", "max")))
    out = (ppl.groupby("source")
              .agg(n_people=("strong", "size"),
                   url_match_rate=("url_any", "mean"),
                   strong_match_rate=("strong", "mean"),
                   strong_match_fuzzy_rate=("strong_fuzzy", "mean"))
              .sort_values("strong_match_rate", ascending=False)
              .reset_index())
    total = pd.DataFrame([{
        "source": "ALL", "n_people": len(ppl),
        "url_match_rate": ppl["url_any"].mean(),
        "strong_match_rate": ppl["strong"].mean(),
        "strong_match_fuzzy_rate": ppl["strong_fuzzy"].mean(),
    }])
    return pd.concat([out, total], ignore_index=True)


def def14a_status_breakdown(status_path):
    df = pd.read_csv(status_path, usecols=["def14a_director_status"], low_memory=False)
    vc = df["def14a_director_status"].value_counts(dropna=False)
    out = vc.rename_axis("status").reset_index(name="n_director_filings")
    out["pct"] = (out["n_director_filings"] / out["n_director_filings"].sum() * 100).round(1)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--strong", action="store_true",
                    help="Use Revelio strong-match-only sentiment vars (*_strong).")
    ap.add_argument("--out-dir", type=str, help="Override output directory.")
    args = ap.parse_args()

    if not CANONICAL.exists():
        sys.exit(f"[error] canonical pointer missing: {CANONICAL}\n"
                 f"        See data/canonical/README.md")

    suffix = "_strong" if args.strong else ""
    sent_col = f"ai_mom_net_sentiment{suffix}"
    share_col = f"ai_post_share{suffix}"
    nposts_col = f"n_posts{suffix}"

    print(f"[load] {CANONICAL}/firm_panel_annual.dta")
    fp = pd.read_stata(CANONICAL / "firm_panel_annual.dta")
    fp["ai_sent"] = _imputed_ai_sentiment(fp, sent_col, share_col, nposts_col)
    fp["lrnd_int"] = np.log(fp["rnd_int"].where(fp["rnd_int"] > 0))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    odir = Path(args.out_dir) if args.out_dir else OUTPUT_BASE / ts
    odir.mkdir(parents=True, exist_ok=True)

    tables = {
        "panel_coverage": panel_coverage(fp),
        "sentiment_descriptives": sentiment_descriptives(fp, sent_col, share_col),
        "ai_sentiment_by_size": by_bin(fp, "ln_at (size) decile", fp["ln_at"], fp["ai_sent"]),
        "ai_sentiment_by_rnd": by_bin(fp, "log R&D-intensity decile", fp["lrnd_int"], fp["ai_sent"]),
        "ai_sentiment_by_industry": by_industry(fp, fp["ai_sent"]),
        "revelio_match_by_source": revelio_match_by_source(CANONICAL / "revelio_validation_summary.csv"),
        "def14a_status_breakdown": def14a_status_breakdown(CANONICAL / "def14a_director_status.csv"),
    }

    md = [f"# Summary statistics — {ts}",
          f"\nSource: `data/canonical/current/` · sentiment vars: "
          f"`{sent_col}` (imputed 0 where posted-but-no-AI)\n"]
    for name, tbl in tables.items():
        tbl.to_csv(odir / f"{name}.csv", index=name in ("sentiment_descriptives",))
        md.append(f"\n## {name}\n\n```\n{tbl.to_string(index=name == 'sentiment_descriptives')}\n```")
        print(f"\n===== {name} =====")
        print(tbl.to_string(index=name == "sentiment_descriptives"))

    (odir / "summary.md").write_text("\n".join(md))
    print(f"\n[done] {len(tables)} tables -> {odir}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Per-firm scatters for Diagnostic 3 — visualises the bracket-pattern story.

For each outcome (ROA, sales growth, stock return), produces a scatter of
firm-pooled mean_net_sentiment vs the firm-pooled outcome, with points
colored by match tier (all firms in light gray; strong-match firms
overlaid in blue). The strong-match overlay is what makes Nick's
"match-quality metric" story visually obvious.

Inputs (auto-detected, newest):
    outputs/sentiment_results/company_sentiment_annual_*.csv
    data/extracted/compustat/funda_*.csv
    data/extracted/crsp/crsp_annual_returns_*.csv

Outputs:
    outputs/sanity_checks/performance_scatters_{ts}/
        roa_scatter.png
        sales_growth_scatter.png
        stock_return_scatter.png
        merged_panel.csv
"""

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SENT_DIR  = PROJECT_ROOT / "outputs" / "sentiment_results"
FUNDA_DIR = PROJECT_ROOT / "data" / "extracted" / "compustat"
CRSP_DIR  = PROJECT_ROOT / "data" / "extracted" / "crsp"
OUT_BASE  = PROJECT_ROOT / "outputs" / "sanity_checks"


def latest(glob_pat: str, directory: Path, exclude: str | None = None) -> Path | None:
    cands = sorted(p for p in directory.glob(glob_pat)
                   if exclude is None or exclude not in p.name)
    return cands[-1] if cands else None


def norm_gvkey(s: pd.Series) -> pd.Series:
    return (s.astype(str).str.replace(r"\.0$", "", regex=True)
             .str.strip().str.zfill(6))


def main():
    sent_path  = latest("company_sentiment_annual_*.csv", SENT_DIR, exclude="_ai_only_")
    funda_path = latest("funda_*.csv", FUNDA_DIR)
    crsp_path  = latest("crsp_annual_returns_*.csv", CRSP_DIR)
    if not all([sent_path, funda_path, crsp_path]):
        sys.exit("[error] Missing one of: sentiment / funda / crsp")

    print(f"[load] {sent_path.name}")
    sent = pd.read_csv(sent_path, low_memory=False)
    sent = sent[sent["gvkey"].notna() & (sent["n_posts"] >= 10)].copy()
    sent["gvkey"] = norm_gvkey(sent["gvkey"])

    print(f"[load] {funda_path.name}")
    funda = pd.read_csv(funda_path, low_memory=False)
    funda = funda[funda["gvkey"].notna() & funda["fyear"].notna()].copy()
    funda["gvkey"] = norm_gvkey(funda["gvkey"])
    funda["year"] = funda["fyear"].astype(int)

    # Build outcomes per firm-year
    funda = funda.sort_values(["gvkey", "year"])
    funda["roa"] = funda["ni"] / funda["at"].where(funda["at"] > 0)
    sale_pos = funda["sale"].where(funda["sale"] > 0)
    funda["sales_growth"] = (np.log(sale_pos)
                             - np.log(sale_pos.groupby(funda["gvkey"]).shift(1)))

    print(f"[load] {crsp_path.name}")
    crsp = pd.read_csv(crsp_path, low_memory=False)
    crsp["gvkey"] = norm_gvkey(crsp["gvkey"])
    crsp["year"]  = crsp["fyear"].astype(int)

    # Firm-pooled aggregates
    sent_firm = (sent.groupby("gvkey")
                     .agg(mean_net_sentiment=("mean_net_sentiment", "mean"),
                          n_posts=("n_posts", "sum"),
                          strong_match_share=("strong_match_share", "mean"),
                          strong_match_fuzzy_share=("strong_match_fuzzy_share", "mean"),
                          company_name_clean=("company_name_clean", "first"))
                     .reset_index())

    roa_firm = (funda.groupby("gvkey")["roa"].mean().reset_index()
                     .rename(columns={"roa": "mean_roa"}))
    sg_firm  = (funda.groupby("gvkey")["sales_growth"].mean().reset_index()
                     .rename(columns={"sales_growth": "mean_sales_growth"}))
    ret_firm = (crsp.groupby("gvkey")["stock_return"].mean().reset_index()
                    .rename(columns={"stock_return": "mean_stock_return"}))

    panel = (sent_firm
             .merge(roa_firm, on="gvkey", how="left")
             .merge(sg_firm,  on="gvkey", how="left")
             .merge(ret_firm, on="gvkey", how="left"))

    # Light winsorize for plot readability
    for c in ["mean_roa", "mean_sales_growth", "mean_stock_return", "mean_net_sentiment"]:
        lo, hi = panel[c].quantile([0.01, 0.99])
        panel[c] = panel[c].clip(lo, hi)

    panel["is_strong"] = panel["strong_match_share"].fillna(0) >= 0.5

    print(f"\n[panel] {len(panel):,} firms; "
          f"strong-match firms: {panel['is_strong'].sum():,}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = OUT_BASE / f"performance_scatters_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    panel.to_csv(out_dir / "merged_panel.csv", index=False)

    import matplotlib.pyplot as plt
    from scipy.stats import pearsonr, spearmanr

    specs = [
        ("mean_roa",          "Mean ROA  (ni / at)",                "roa_scatter.png"),
        ("mean_sales_growth", "Mean sales growth  (Δlog sale)",     "sales_growth_scatter.png"),
        ("mean_stock_return", "Mean annual stock return  (CRSP)",   "stock_return_scatter.png"),
    ]

    for col, ylabel, fname in specs:
        full = panel[["mean_net_sentiment", col]].dropna()
        strong = panel[panel["is_strong"]][["mean_net_sentiment", col]].dropna()

        pr_full, _ = pearsonr(full["mean_net_sentiment"], full[col])
        sp_full, _ = spearmanr(full["mean_net_sentiment"], full[col])
        pr_strong, _ = pearsonr(strong["mean_net_sentiment"], strong[col])
        sp_strong, _ = spearmanr(strong["mean_net_sentiment"], strong[col])

        fig, ax = plt.subplots(figsize=(8, 6))
        ax.scatter(full["mean_net_sentiment"], full[col], s=12, alpha=0.30,
                   color="lightgray", label=f"All firms (n={len(full):,})")
        ax.scatter(strong["mean_net_sentiment"], strong[col], s=12, alpha=0.55,
                   color="tab:blue", label=f"Strong-match (n={len(strong):,})")

        # OLS lines for both samples (visual only — no inference)
        for sub, color in [(full, "gray"), (strong, "tab:blue")]:
            if len(sub) < 5:
                continue
            x = sub["mean_net_sentiment"].values
            y = sub[col].values
            m, b = np.polyfit(x, y, 1)
            xs = np.linspace(x.min(), x.max(), 50)
            ax.plot(xs, m * xs + b, color=color, alpha=0.7, linewidth=1.5)

        ax.set_xlabel("Firm mean LinkedIn sentiment  (L-M net per 1K words)")
        ax.set_ylabel(ylabel)
        ax.set_title(
            f"{ylabel}  vs  LinkedIn sentiment\n"
            f"All: Pearson={pr_full:.3f}, Spearman={sp_full:.3f}   "
            f"·   Strong: Pearson={pr_strong:.3f}, Spearman={sp_strong:.3f}"
        )
        ax.axhline(0, color="black", alpha=0.3, linewidth=0.5)
        ax.grid(alpha=0.3)
        ax.legend(loc="upper right")
        fig.tight_layout()
        fig.savefig(out_dir / fname, dpi=140)
        plt.close(fig)
        print(f"  [plot] {out_dir}/{fname}  "
              f"all ρ={pr_full:.3f} → strong ρ={pr_strong:.3f}")

    print(f"\n[done] {out_dir}")


if __name__ == "__main__":
    main()

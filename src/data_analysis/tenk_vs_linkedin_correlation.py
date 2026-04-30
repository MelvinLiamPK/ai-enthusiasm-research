#!/usr/bin/env python3
"""
10-K AI Mentions vs LinkedIn AI Sentiment — Convergent Validity
================================================================

Per-firm correlation between:
  - 10-K AI mention rate (per 1K words) — from build_tenk_ai_mentions.py
  - LinkedIn AI post share — from aggregate_sentiment.py

Pearson + Spearman, plus a deck-ready scatter. The hypothesis: firms whose
executives talk about AI on LinkedIn should also be the firms whose 10-Ks
mention AI more — measuring the same underlying "AI awareness" via two
independent corpora.

Inputs (auto-detected newest):
    data/extracted/tenk/tenk_ai_mentions_*.csv
    outputs/sentiment_results/company_sentiment_annual_*.csv

Outputs (under outputs/sanity_checks/tenk_pilot_{ts}/):
    correlation.csv     — pearson, spearman, n
    scatter.png         — labeled scatter for the deck
    merged_panel.csv    — audit trail

Usage:
    python3 src/data_analysis/tenk_vs_linkedin_correlation.py
    python3 src/data_analysis/tenk_vs_linkedin_correlation.py --tenk path/to/file.csv
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TENK_DIR     = PROJECT_ROOT / "data" / "extracted" / "tenk"
SENT_DIR     = PROJECT_ROOT / "outputs" / "sentiment_results"
OUT_BASE     = PROJECT_ROOT / "outputs" / "sanity_checks"
DIAG_MD_PATH = PROJECT_ROOT / "docs" / "research_notes" / "sentiment_diagnostics.md"


def replace_marker_block(start: str, end: str, content: str) -> None:
    """Replace text between matching <!-- {start} --> and <!-- {end} --> markers."""
    if not DIAG_MD_PATH.exists():
        return
    md = DIAG_MD_PATH.read_text()
    if start not in md or end not in md:
        return
    pre, rest = md.split(start, 1)
    _, post = rest.split(end, 1)
    md = f"{pre}{start}\n{content}\n{end}{post}"
    DIAG_MD_PATH.write_text(md)
    print(f"[write] {DIAG_MD_PATH.relative_to(PROJECT_ROOT)} "
          f"(updated {start.strip('<!- >')})")


def latest(glob_pat: str, directory: Path, exclude: str | None = None) -> Path | None:
    cands = sorted(p for p in directory.glob(glob_pat)
                   if exclude is None or exclude not in p.name)
    return cands[-1] if cands else None


def main():
    parser = argparse.ArgumentParser(
        description="Correlate 10-K AI mentions with LinkedIn AI post share",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--tenk", type=str,
                        help="Override auto-detected 10-K mentions CSV")
    parser.add_argument("--sentiment", type=str,
                        help="Override auto-detected company_sentiment_annual CSV")
    parser.add_argument("--label-top", type=int, default=15,
                        help="Number of firms to label on the scatter (default: 15)")
    args = parser.parse_args()

    tenk_path = Path(args.tenk) if args.tenk else latest(
        "tenk_ai_mentions_*.csv", TENK_DIR)
    sent_path = Path(args.sentiment) if args.sentiment else latest(
        "company_sentiment_annual_*.csv", SENT_DIR, exclude="_ai_only_")

    if tenk_path is None or not tenk_path.exists():
        sys.exit(f"[error] No tenk_ai_mentions_*.csv in {TENK_DIR}\n"
                 f"        Run: python3 src/data_extraction/build_tenk_ai_mentions.py")
    if sent_path is None or not sent_path.exists():
        sys.exit(f"[error] No company_sentiment_annual_*.csv in {SENT_DIR}")

    print(f"[load] {tenk_path.name}")
    tenk = pd.read_csv(tenk_path, low_memory=False)
    tenk["gvkey"] = tenk["gvkey"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    print(f"       {len(tenk):,} 10-K filings, {tenk['gvkey'].nunique():,} firms")

    print(f"[load] {sent_path.name}")
    sent = pd.read_csv(sent_path, low_memory=False)
    sent["gvkey"] = sent["gvkey"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)

    # Aggregate the LinkedIn AI post share to the firm level (most-recent year
    # in our sentiment panel, or pooled across years — pooled is more robust
    # for a small pilot).
    li_firm = (sent.groupby("gvkey")
                   .agg(li_n_posts=("n_posts", "sum"),
                        li_n_ai_posts=("n_ai_posts", "sum"))
                   .reset_index())
    li_firm["li_ai_post_share"] = li_firm["li_n_ai_posts"] / li_firm["li_n_posts"]

    # Aggregate the 10-K side too (mean across the firm's filings in the
    # window — ratio-of-means is the right summary for AI awareness).
    tk_firm = (tenk.groupby("gvkey")
                   .agg(tenk_n_filings=("accession", "nunique"),
                        tenk_total_words=("n_words", "sum"),
                        tenk_total_ai_mentions=("n_ai_mentions", "sum"),
                        company_name_clean=("company_name_clean", "first"),
                        ticker=("ticker", "first"))
                   .reset_index())
    tk_firm["tenk_ai_mention_per_1k"] = (
        tk_firm["tenk_total_ai_mentions"] / tk_firm["tenk_total_words"] * 1000
    )

    panel = tk_firm.merge(li_firm, on="gvkey", how="inner")
    print(f"\n[panel] {len(panel):,} firms with both LinkedIn and 10-K data")

    # Drop firms with too little 10-K text or no LinkedIn AI activity flag
    panel = panel[(panel["tenk_total_words"] >= 1000) &
                  (panel["li_n_posts"] >= 10)].copy()
    print(f"        after coverage filter: {len(panel):,} firms")

    if len(panel) < 5:
        sys.exit("[error] Not enough firms to correlate.")

    pearson = panel[["tenk_ai_mention_per_1k", "li_ai_post_share"]].corr(method="pearson").iloc[0, 1]
    spearman = panel[["tenk_ai_mention_per_1k", "li_ai_post_share"]].corr(method="spearman").iloc[0, 1]
    n = len(panel)
    print(f"\n  Pearson  ρ = {pearson:.3f}  (n={n})")
    print(f"  Spearman ρ = {spearman:.3f}  (n={n})")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = OUT_BASE / f"tenk_pilot_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame([{
        "n_firms": n,
        "pearson": pearson,
        "spearman": spearman,
        "tenk_file": tenk_path.name,
        "sentiment_file": sent_path.name,
    }]).to_csv(out_dir / "correlation.csv", index=False)

    panel.to_csv(out_dir / "merged_panel.csv", index=False)

    # Update Diagnostic 4 markdown blocks
    corr_block = (
        f"_Run: {ts} · 10-K: `{tenk_path.name}` · LI: `{sent_path.name}`_\n\n"
        f"| Metric | Value |\n"
        f"|---|---:|\n"
        f"| Firms (n) | {n:,} |\n"
        f"| Pearson ρ  | {pearson:.3f} |\n"
        f"| Spearman ρ | {spearman:.3f} |\n"
    )
    replace_marker_block("<!-- DIAG4_CORR_START -->",
                         "<!-- DIAG4_CORR_END -->", corr_block)

    top_n = panel.nlargest(15, "tenk_ai_mention_per_1k")[
        ["company_name_clean", "ticker", "tenk_n_filings", "tenk_total_words",
         "tenk_total_ai_mentions", "tenk_ai_mention_per_1k", "li_ai_post_share"]
    ]
    top_md_lines = [
        "| Firm | Ticker | 10-Ks | Total words | AI mentions | Per 1K words | LI AI share |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for _, r in top_n.iterrows():
        top_md_lines.append(
            f"| {str(r['company_name_clean'])[:30]} | {r['ticker']} | "
            f"{int(r['tenk_n_filings'])} | {int(r['tenk_total_words']):,} | "
            f"{int(r['tenk_total_ai_mentions']):,} | "
            f"{r['tenk_ai_mention_per_1k']:.2f} | "
            f"{r['li_ai_post_share']:.3f} |"
        )
    replace_marker_block("<!-- DIAG4_TOP10K_START -->",
                         "<!-- DIAG4_TOP10K_END -->",
                         "\n".join(top_md_lines))

    # Scatter plot
    try:
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(8, 6))
        # Log-log axes since both distributions are right-skewed; add small
        # epsilons so zero values don't blow up the log
        x = panel["tenk_ai_mention_per_1k"] + 1e-3
        y = panel["li_ai_post_share"] + 1e-4
        ax.scatter(x, y, s=20, alpha=0.5, color="tab:blue")

        # OLS fit in log-log space (a line in log-log = power-law fit;
        # appropriate for two right-skewed variables). Slope is the elasticity:
        # a 10x increase in 10-K AI rate predicts ~10**slope-fold increase in
        # LinkedIn AI share.
        log_x = np.log10(x.values)
        log_y = np.log10(y.values)
        slope, intercept = np.polyfit(log_x, log_y, 1)
        xs_log = np.linspace(log_x.min(), log_x.max(), 50)
        ax.plot(10**xs_log, 10**(slope * xs_log + intercept),
                color="tab:red", linewidth=1.5, alpha=0.85,
                label=f"OLS (log-log)  slope = {slope:.2f}")

        # Label the most prominent firms by either dimension
        prominent = pd.concat([
            panel.nlargest(args.label_top, "tenk_ai_mention_per_1k"),
            panel.nlargest(args.label_top, "li_ai_post_share"),
        ]).drop_duplicates(subset=["gvkey"])
        for _, r in prominent.iterrows():
            ax.annotate(
                str(r["company_name_clean"])[:20],
                xy=(r["tenk_ai_mention_per_1k"] + 1e-3, r["li_ai_post_share"] + 1e-4),
                fontsize=7, alpha=0.8,
            )

        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("10-K AI mentions per 1K words (log)")
        ax.set_ylabel("LinkedIn AI post share (log)")
        ax.set_title(f"10-K AI awareness vs. LinkedIn AI sentiment (n={n})\n"
                     f"Pearson ρ={pearson:.3f}, Spearman ρ={spearman:.3f}")
        ax.grid(alpha=0.3)
        ax.legend(loc="lower right", fontsize=8)
        fig.tight_layout()
        fig.savefig(out_dir / "scatter.png", dpi=140)
        plt.close(fig)
        print(f"  [plot] {out_dir}/scatter.png  "
              f"(OLS log-log slope = {slope:.3f})")
    except ImportError:
        print("  [warn] matplotlib not available — skipping plot")

    print(f"\n[done] Outputs in {out_dir}")


if __name__ == "__main__":
    main()

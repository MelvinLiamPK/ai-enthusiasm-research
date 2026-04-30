#!/usr/bin/env python3
"""
Face-Validity Checks on the AI Sentiment Measure
=================================================

Sanity-check the LinkedIn AI sentiment measure before using it as a
regressor in downstream analysis. Three checks:

  1. Temporal    — AI-post share and AI sentiment over time.
                    Expect a visible jump around Nov 2022 (ChatGPT).
  2. Cross-sec.  — Rank firms by AI-post share and mean AI sentiment.
                    Expect tech/software at the top, utilities at the
                    bottom.
  3. Within-ind. — Same ranking within 2-digit SIC 73 (business
                    services / software), if a Compustat funda file is
                    available to supply `sich`.

Inputs (auto-detected from newest timestamp):
    outputs/sentiment_results/sentiment_all_posts_*.csv
    data/processed/all_people_linkedin_urls/all_linkedin_urls.csv
    data/extracted/compustat/funda_*.csv                (optional, for SIC)

Outputs:
    outputs/sanity_checks/face_validity_{ts}/
        monthly_ai_trends.csv
        monthly_ai_trends.png
        firm_rank_ai_share.csv
        firm_rank_ai_sentiment.csv
        within_sic73_rank.csv        (if Compustat file present)
        chatgpt_ttest.txt

Usage:
    python3 src/data_analysis/face_validity_checks.py
    python3 src/data_analysis/face_validity_checks.py --min-posts 20
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

SENTIMENT_DIR  = PROJECT_ROOT / "outputs" / "sentiment_results"
ALL_PEOPLE_CSV = PROJECT_ROOT / "data" / "processed" / "all_people_linkedin_urls" / "all_linkedin_urls.csv"
FUNDA_DIR      = PROJECT_ROOT / "data" / "extracted" / "compustat"
OUTPUT_BASE    = PROJECT_ROOT / "outputs" / "sanity_checks"

CHATGPT_LAUNCH = pd.Timestamp("2022-11-30")


# ──────────────────────────────────────────────
# Inputs
# ──────────────────────────────────────────────

def latest(glob_pat: str, directory: Path) -> Path | None:
    candidates = sorted(directory.glob(glob_pat))
    return candidates[-1] if candidates else None


def load_sentiment(path: Path, nrows: int | None) -> pd.DataFrame:
    usecols = [
        "company_name_clean", "person_name_clean",
        "post_date", "lm_net_sentiment", "is_ai_related",
    ]
    print(f"[load] {path.name}")
    # Same embedded-newline workaround as the rest of the pipeline.
    df = pd.read_csv(
        path, usecols=usecols, nrows=nrows, low_memory=False,
        engine="c", lineterminator="\n", on_bad_lines="skip",
    )
    df["post_date"]     = pd.to_datetime(df["post_date"], errors="coerce")
    df["is_ai_related"] = df["is_ai_related"].fillna(False).astype(bool)
    df = df[df["post_date"].notna() & df["company_name_clean"].notna()]
    print(f"       {len(df):,} dated posts, "
          f"{df['is_ai_related'].sum():,} AI posts "
          f"({df['is_ai_related'].mean()*100:.2f}%)")
    return df


def attach_gvkey(df: pd.DataFrame) -> pd.DataFrame:
    if not ALL_PEOPLE_CSV.exists():
        print(f"[warn] {ALL_PEOPLE_CSV.name} not found — gvkey will be missing.")
        df["gvkey"] = np.nan
        return df
    ap = pd.read_csv(
        ALL_PEOPLE_CSV,
        usecols=["company_name_clean", "person_name_clean", "gvkey"],
        low_memory=False,
    ).drop_duplicates(subset=["company_name_clean", "person_name_clean"])
    df = df.merge(ap, on=["company_name_clean", "person_name_clean"], how="left")
    print(f"[merge] gvkey attached to {df['gvkey'].notna().sum():,} / {len(df):,} posts")
    return df


def load_sich() -> pd.DataFrame | None:
    """Return gvkey→sich lookup from the newest Compustat funda file, if any."""
    path = latest("funda_*.csv", FUNDA_DIR)
    if path is None:
        print(f"[info] no Compustat funda_*.csv in {FUNDA_DIR} — skipping within-industry check")
        return None
    print(f"[load] {path.name} (for SIC codes)")
    df = pd.read_csv(path, usecols=["gvkey", "sich"], low_memory=False)
    df["gvkey"] = df["gvkey"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    # Take the modal sich per gvkey (codes are mostly stable over time)
    df = df.dropna(subset=["sich"])
    df["sich"] = df["sich"].astype(int)
    lookup = df.groupby("gvkey")["sich"].agg(lambda s: s.mode().iloc[0]).reset_index()
    print(f"       {len(lookup):,} gvkeys with SIC code")
    return lookup


# ──────────────────────────────────────────────
# Checks
# ──────────────────────────────────────────────

def temporal_check(df: pd.DataFrame, out_dir: Path) -> dict:
    """Monthly time series of AI-post share and AI mean sentiment."""
    monthly = (
        df.assign(month=df["post_date"].values.astype("datetime64[M]"))
          .groupby("month")
          .agg(n_posts=("lm_net_sentiment", "size"),
               n_ai=("is_ai_related", "sum"),
               ai_mean_sent=("lm_net_sentiment",
                             lambda s: s[df.loc[s.index, "is_ai_related"]].mean()))
          .reset_index()
    )
    monthly["ai_post_share"] = monthly["n_ai"] / monthly["n_posts"]

    monthly.to_csv(out_dir / "monthly_ai_trends.csv", index=False)

    # t-test pre vs post ChatGPT (at post level)
    df["post_chatgpt"] = df["post_date"] >= CHATGPT_LAUNCH
    pre  = df.loc[~df["post_chatgpt"], "is_ai_related"]
    post = df.loc[ df["post_chatgpt"], "is_ai_related"]
    share_pre, share_post = pre.mean(), post.mean()
    # Two-proportion z-test (simple)
    p = pd.concat([pre, post]).mean()
    n1, n2 = len(pre), len(post)
    se = np.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
    z = (share_post - share_pre) / se if se > 0 else np.nan

    lines = [
        "ChatGPT (Nov 2022) break test — AI post share",
        "=" * 50,
        f"  Pre-ChatGPT:   n={n1:>10,}   share={share_pre*100:6.3f}%",
        f"  Post-ChatGPT:  n={n2:>10,}   share={share_post*100:6.3f}%",
        f"  Ratio:         {share_post/share_pre:.2f}x" if share_pre > 0 else "  Ratio:         n/a",
        f"  z-stat:        {z:.2f}",
        "",
        "Interpretation:",
        "  - Ratio should be > 2x if the AI filter captures the post-ChatGPT surge.",
        "  - If ratio < 1.5x, investigate the AI keyword list before trusting downstream work.",
    ]
    (out_dir / "chatgpt_ttest.txt").write_text("\n".join(lines) + "\n")
    for line in lines:
        print("  " + line)

    # Plot
    try:
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
        axes[0].plot(monthly["month"], monthly["ai_post_share"] * 100, color="tab:blue")
        axes[0].axvline(CHATGPT_LAUNCH, color="red", linestyle="--", alpha=0.7,
                        label="ChatGPT launch")
        axes[0].set_ylabel("AI post share (%)")
        axes[0].legend()
        axes[0].grid(alpha=0.3)

        axes[1].plot(monthly["month"], monthly["ai_mean_sent"], color="tab:orange")
        axes[1].axvline(CHATGPT_LAUNCH, color="red", linestyle="--", alpha=0.7)
        axes[1].set_ylabel("Mean AI post sentiment (L-M)")
        axes[1].set_xlabel("Month")
        axes[1].grid(alpha=0.3)

        fig.suptitle("AI-related LinkedIn posts over time")
        fig.tight_layout()
        fig.savefig(out_dir / "monthly_ai_trends.png", dpi=120)
        plt.close(fig)
        print(f"  [plot] monthly_ai_trends.png")
    except ImportError:
        print("  [warn] matplotlib not available — skipping plot")

    return {
        "share_pre": float(share_pre),
        "share_post": float(share_post),
        "ratio": float(share_post / share_pre) if share_pre > 0 else None,
        "z": float(z) if not np.isnan(z) else None,
    }


def cross_sectional_check(df: pd.DataFrame, out_dir: Path, min_posts: int) -> None:
    """Rank firms by AI-post share and mean AI sentiment."""
    firm = (
        df.groupby("company_name_clean")
          .agg(n_posts=("lm_net_sentiment", "size"),
               n_ai=("is_ai_related", "sum"),
               ai_mean_sent=("lm_net_sentiment",
                             lambda s: s[df.loc[s.index, "is_ai_related"]].mean()),
               gvkey=("gvkey", "first"))
          .reset_index()
    )
    firm["ai_post_share"] = firm["n_ai"] / firm["n_posts"]
    firm = firm[firm["n_posts"] >= min_posts].copy()
    print(f"\n[firm] {len(firm):,} firms with ≥ {min_posts} posts")

    # AI-post share
    by_share = firm.sort_values("ai_post_share", ascending=False)
    by_share.to_csv(out_dir / "firm_rank_ai_share.csv", index=False)

    # AI mean sentiment (restrict to firms with ≥5 AI posts to avoid noise)
    with_ai = firm[firm["n_ai"] >= 5].sort_values("ai_mean_sent", ascending=False)
    with_ai.to_csv(out_dir / "firm_rank_ai_sentiment.csv", index=False)

    def _show(label, dfx, value_col, n=15):
        print(f"\n  Top {n} — {label}")
        print(dfx.head(n)[["company_name_clean", "n_posts", "n_ai", value_col]]
              .to_string(index=False, float_format=lambda v: f"{v:.3f}"))
        print(f"\n  Bottom {n} — {label}")
        print(dfx.tail(n)[["company_name_clean", "n_posts", "n_ai", value_col]]
              .to_string(index=False, float_format=lambda v: f"{v:.3f}"))

    _show("AI-post share", by_share, "ai_post_share")
    _show("AI mean sentiment (≥5 AI posts)", with_ai, "ai_mean_sent")


def within_industry_check(df: pd.DataFrame, sich_lookup: pd.DataFrame,
                          out_dir: Path, min_posts: int) -> None:
    """Rank firms within SIC 2-digit 73 (business services / software)."""
    df = df.copy()
    df["gvkey"] = df["gvkey"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    merged = df.merge(sich_lookup, on="gvkey", how="left")
    merged["sic2"] = (merged["sich"] // 100).astype("Int64")

    sic73 = merged[merged["sic2"] == 73]
    if sic73.empty:
        print("[info] no SIC-73 posts after merge — skipping within-industry check")
        return

    firm = (
        sic73.groupby("company_name_clean")
             .agg(n_posts=("lm_net_sentiment", "size"),
                  n_ai=("is_ai_related", "sum"),
                  ai_mean_sent=("lm_net_sentiment",
                                lambda s: s[sic73.loc[s.index, "is_ai_related"]].mean()))
             .reset_index()
    )
    firm["ai_post_share"] = firm["n_ai"] / firm["n_posts"]
    firm = firm[firm["n_posts"] >= min_posts].sort_values("ai_post_share", ascending=False)
    firm.to_csv(out_dir / "within_sic73_rank.csv", index=False)
    print(f"\n  SIC 73 (business services) — top 15 by AI post share")
    print(firm.head(15).to_string(index=False,
          float_format=lambda v: f"{v:.3f}"))


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Face-validity checks on the AI sentiment measure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", type=str,
                        help="Override auto-detected sentiment_all_posts_*.csv")
    parser.add_argument("--prototype", type=int, metavar="N",
                        help="Run on first N post rows only")
    parser.add_argument("--min-posts", type=int, default=10,
                        help="Min posts per firm for ranking (default: 10)")
    args = parser.parse_args()

    sentiment_path = Path(args.input) if args.input else latest(
        "sentiment_all_posts_*.csv", SENTIMENT_DIR)
    if sentiment_path is None or not sentiment_path.exists():
        sys.exit(f"[error] No sentiment_all_posts_*.csv found in {SENTIMENT_DIR}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = OUTPUT_BASE / f"face_validity_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[out]  {out_dir}\n")

    df = load_sentiment(sentiment_path, nrows=args.prototype)
    df = attach_gvkey(df)

    print("\n" + "=" * 60)
    print("1. TEMPORAL CHECK")
    print("=" * 60)
    temporal_check(df, out_dir)

    print("\n" + "=" * 60)
    print("2. CROSS-SECTIONAL CHECK")
    print("=" * 60)
    cross_sectional_check(df, out_dir, args.min_posts)

    sich_lookup = load_sich()
    if sich_lookup is not None:
        print("\n" + "=" * 60)
        print("3. WITHIN-INDUSTRY CHECK (SIC 73)")
        print("=" * 60)
        within_industry_check(df, sich_lookup, out_dir, args.min_posts)

    print(f"\n[done] Outputs in {out_dir}")


if __name__ == "__main__":
    main()

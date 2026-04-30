#!/usr/bin/env python3
"""
Tobin's Q ~ AI Sentiment Panel Regression
==========================================

Sanity-check regression: does firm-level AI sentiment (from LinkedIn posts
of executives, directors, and blockholders) predict firm value (Tobin's Q)
one year ahead, after controlling for standard firm characteristics?

Specification (primary):
    ln(Q)_{i, t+1} ~ ai_sentiment_{i, t}
                     + ln(at)_{i,t} + leverage_{i,t} + rnd_{i,t}
                     + firm FE + year FE
    SEs clustered by firm.

Also runs a share-based variant (volume of AI talk vs. tone) and, if a
quarterly aggregate exists, a firm-quarter version.

Inputs (auto-detected, newest):
    outputs/sentiment_results/company_sentiment_annual_*.csv
    outputs/sentiment_results/company_sentiment_quarterly_*.csv    (optional)
    data/extracted/compustat/funda_*.csv

Prerequisites:
    Run src/data_analysis/aggregate_sentiment.py --run first to produce
    the company_sentiment_annual_*.csv file. Run
    src/data_extraction/build_compustat_funda.py first to produce the
    funda_*.csv file.

Outputs:
    outputs/sanity_checks/q_regression_{ts}/
        coefficients.csv       (one row per spec × variable)
        summary.txt            (human-readable results)
        merged_panel.csv       (the regression panel itself, for audit)

Usage:
    python3 src/data_analysis/sentiment_q_regression.py
    python3 src/data_analysis/sentiment_q_regression.py --min-ai-posts 5
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from linearmodels.panel import PanelOLS
    import statsmodels.api as sm
except ImportError:
    sys.exit("[error] linearmodels/statsmodels not installed — "
             "`pip install linearmodels`")


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

SENTIMENT_DIR = PROJECT_ROOT / "outputs" / "sentiment_results"
FUNDA_DIR     = PROJECT_ROOT / "data" / "extracted" / "compustat"
CRSP_DIR      = PROJECT_ROOT / "data" / "extracted" / "crsp"
OUTPUT_BASE   = PROJECT_ROOT / "outputs" / "sanity_checks"


# ──────────────────────────────────────────────
# I/O helpers
# ──────────────────────────────────────────────

def latest(glob_pat: str, directory: Path) -> Path | None:
    candidates = sorted(directory.glob(glob_pat))
    return candidates[-1] if candidates else None


def norm_gvkey(s: pd.Series) -> pd.Series:
    return (s.astype(str)
             .str.replace(r"\.0$", "", regex=True)
             .str.strip()
             .str.zfill(6))


def load_sentiment_annual(path: Path) -> pd.DataFrame:
    print(f"[load] {path.name}")
    df = pd.read_csv(path, low_memory=False)
    df = df[df["gvkey"].notna() & df["year"].notna()].copy()
    df["gvkey"] = norm_gvkey(df["gvkey"])
    df["year"]  = df["year"].astype(int)
    print(f"       {len(df):,} firm-year sentiment rows, "
          f"{df['gvkey'].nunique():,} firms")
    return df


def load_funda(path: Path) -> pd.DataFrame:
    print(f"[load] {path.name}")
    df = pd.read_csv(path, low_memory=False)
    df = df[df["gvkey"].notna() & df["fyear"].notna()].copy()
    df["gvkey"] = norm_gvkey(df["gvkey"])
    df["year"]  = df["fyear"].astype(int)
    print(f"       {len(df):,} firm-year funda rows, "
          f"{df['gvkey'].nunique():,} firms")
    return df


def load_crsp_returns(path: Path) -> pd.DataFrame:
    print(f"[load] {path.name}")
    df = pd.read_csv(path, low_memory=False)
    df = df[df["gvkey"].notna() & df["fyear"].notna()].copy()
    df["gvkey"] = norm_gvkey(df["gvkey"])
    df["year"]  = df["fyear"].astype(int)
    print(f"       {len(df):,} firm-year return rows, "
          f"{df['gvkey'].nunique():,} firms")
    return df


def merge_crsp_returns(funda: pd.DataFrame, crsp_path: Path) -> pd.DataFrame:
    """Add a `stock_return` column to funda from a CRSP annual-returns CSV."""
    crsp = load_crsp_returns(crsp_path)
    return funda.merge(
        crsp[["gvkey", "year", "stock_return"]],
        on=["gvkey", "year"], how="left",
    )


# ──────────────────────────────────────────────
# Panel construction
# ──────────────────────────────────────────────

def build_controls(funda: pd.DataFrame) -> pd.DataFrame:
    """
    Compute controls + outcome columns from raw Compustat funda.

    Outcomes added:
      - tobins_q    : (mkt_cap + at - ceq) / at   (already in raw funda)
      - sales_growth: ln(sale_t / sale_{t-1}) per gvkey
      - roa         : ni / at
    """
    f = funda.copy()
    # Controls
    f["ln_at"]       = np.log(f["at"].where(f["at"] > 0))
    f["leverage"]    = f["lt"]  / f["at"]
    f["rnd_int"]     = f["xrd"].fillna(0) / f["at"]
    f["profit_marg"] = f["ni"]  / f["sale"].where(f["sale"] > 0)

    # Outcomes (in addition to tobins_q which is already on funda from the puller)
    f["roa"] = f["ni"] / f["at"].where(f["at"] > 0)

    # sales_growth requires lag within firm — sort then shift
    f = f.sort_values(["gvkey", "year"])
    sale_pos = f["sale"].where(f["sale"] > 0)
    f["sales_growth"] = (np.log(sale_pos)
                         - np.log(sale_pos.groupby(f["gvkey"]).shift(1)))
    return f


# Outcome registry — maps the CLI choice to the column on the panel and
# how to transform it for the LHS of the regression.
#
#   col          : column on the (controlled) funda dataframe at year t+lead
#   panel_col    : final column name on the regression panel (LHS)
#   transform    : called on the column before it becomes the LHS
#   require_pos  : if True, drop rows where the outcome is non-positive
#                  (necessary for log transforms)
OUTCOMES = {
    "tobins_q": {
        "col":         "tobins_q",
        "panel_col":   "ln_q_lead",
        "transform":   np.log,
        "require_pos": True,
        "label":       "ln(Tobin's Q)",
    },
    "sales_growth": {
        "col":         "sales_growth",
        "panel_col":   "sales_growth_lead",
        "transform":   None,  # already in log-difference form
        "require_pos": False,
        "label":       "Sales growth (Δlog sale)",
    },
    "roa": {
        "col":         "roa",
        "panel_col":   "roa_lead",
        "transform":   None,
        "require_pos": False,
        "label":       "ROA (ni / at)",
    },
    "stock_return": {
        "col":         "stock_return",
        "panel_col":   "stock_return_lead",
        "transform":   None,
        "require_pos": False,
        "label":       "Annual stock return",
    },
}


def winsorize(df: pd.DataFrame, cols: list[str], p: float = 0.01) -> pd.DataFrame:
    """Winsorize each column at the (p, 1-p) quantiles."""
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            continue
        lo, hi = out[c].quantile(p), out[c].quantile(1 - p)
        out[c] = out[c].clip(lo, hi)
    return out


def build_panel(sent: pd.DataFrame, funda: pd.DataFrame,
                sent_cols: list[str], outcome: str = "tobins_q",
                lead: int = 1) -> pd.DataFrame:
    """
    Merge sentiment_t with controls_t and outcome at t+lead.

    Structure:
        index = (gvkey, year)  -- the year of the sentiment observation
        <outcome>_lead is the chosen outcome at year t+lead.
    """
    if outcome not in OUTCOMES:
        raise ValueError(f"Unknown outcome '{outcome}'. "
                         f"Choices: {sorted(OUTCOMES)}")
    spec = OUTCOMES[outcome]
    src_col = spec["col"]
    panel_col = spec["panel_col"]

    controls = ["ln_at", "leverage", "rnd_int", "profit_marg"]
    f = build_controls(funda)

    if src_col not in f.columns:
        raise ValueError(
            f"Outcome '{outcome}' requires column '{src_col}' which is not "
            f"in the funda dataframe. For stock_return, run "
            f"build_crsp_returns.py and merge before invoking the regression."
        )

    lhs = (f[["gvkey", "year", src_col]]
           .rename(columns={"year": "year_lead", src_col: panel_col}))
    lhs["year"] = lhs["year_lead"] - lead

    keep_sent = ["gvkey", "year"] + sent_cols
    keep_ctrl = ["gvkey", "year"] + controls + ["sich"]

    panel = (sent[keep_sent]
             .merge(f[keep_ctrl], on=["gvkey", "year"], how="inner")
             .merge(lhs[["gvkey", "year", panel_col]],
                    on=["gvkey", "year"], how="inner"))

    if spec["require_pos"]:
        panel = panel[panel[panel_col].gt(0)].copy()
    if spec["transform"] is not None:
        panel[panel_col] = spec["transform"](panel[panel_col])

    panel = panel.dropna(subset=[panel_col]).copy()

    panel = winsorize(
        panel,
        [panel_col, "ln_at", "leverage", "rnd_int", "profit_marg"] + sent_cols,
        p=0.01,
    )
    return panel


# ──────────────────────────────────────────────
# Regression — staggered build-up
# ──────────────────────────────────────────────

RHS_CONTROLS = ["ln_at", "leverage", "rnd_int", "profit_marg"]

# Four nested specs. All four are estimated on the SAME sample (the panel
# after dropping NaN on the most-restrictive variable set), so coefficient
# changes across layers reflect what each layer absorbs, not sample drift.
LAYERS = [
    # (label,                    add_controls, entity_fx, time_fx)
    ("(1) Pooled OLS",                 False,    False,     False),
    ("(2) + Year FE",                  False,    False,     True),
    ("(3) + Controls",                 True,     False,     True),
    ("(4) + Firm FE (saturated)",      True,     True,      True),
]


def _common_sample(panel: pd.DataFrame, y: str, x: str) -> pd.DataFrame:
    """Drop NaN on (y, x, all controls) so every layer runs on the same rows."""
    cols = [y, x] + RHS_CONTROLS
    d = panel.dropna(subset=cols).copy()
    return d.set_index(["gvkey", "year"])


def run_layer(d: pd.DataFrame, y: str, x: str,
              add_controls: bool, entity_fx: bool, time_fx: bool,
              label: str) -> dict:
    """Run one layer of the staggered build-up. Cluster SEs by firm throughout."""
    rhs_cols = [x] + (RHS_CONTROLS if add_controls else [])
    exog = sm.add_constant(d[rhs_cols], has_constant="add")

    model = PanelOLS(
        dependent=d[y],
        exog=exog,
        entity_effects=entity_fx,
        time_effects=time_fx,
        drop_absorbed=True,
    )
    res = model.fit(cov_type="clustered", cluster_entity=True)

    n_obs   = int(res.nobs)
    n_firms = d.index.get_level_values("gvkey").nunique()
    # Use within-R² when entity effects are on (the conventional FE summary);
    # use overall R² for pooled / time-only specs.
    r2 = float(res.rsquared_within) if entity_fx else float(res.rsquared)

    return {
        "regressor": x,
        "layer":     label,
        "beta":      float(res.params[x]),
        "se":        float(res.std_errors[x]),
        "t":         float(res.tstats[x]),
        "p":         float(res.pvalues[x]),
        "n_obs":     n_obs,
        "n_firms":   n_firms,
        "r2":        r2,
    }


def run_staggered(panel: pd.DataFrame, y: str, regressors: list[str]) -> pd.DataFrame:
    """For each regressor, run all four nested specs on the same sample."""
    rows = []
    for x in regressors:
        d = _common_sample(panel, y, x)
        if len(d) < 50:
            print(f"  [skip] {x}: only {len(d)} common-sample obs")
            continue
        # Constant regressors are uninformative — flag and skip
        if d[x].nunique() <= 1:
            print(f"  [skip] {x}: constant within common sample")
            continue
        for label, ctrl, ef, tf in LAYERS:
            try:
                rows.append(run_layer(d, y, x, ctrl, ef, tf, label))
            except Exception as e:
                print(f"  [error] {x} / {label}: {e}")
    return pd.DataFrame(rows)


def print_staggered_table(df: pd.DataFrame, regressor: str) -> None:
    """Render one regressor's 4-layer build-up as a clean text table."""
    sub = df[df["regressor"] == regressor]
    if sub.empty:
        return
    print(f"\n  {regressor}")
    print(f"    {'Layer':<32} {'β':>10} {'SE':>8} {'t':>7} {'p':>7} "
          f"{'N':>7} {'R²':>7}")
    print(f"    {'-'*32} {'-'*10} {'-'*8} {'-'*7} {'-'*7} {'-'*7} {'-'*7}")
    for _, r in sub.iterrows():
        star = "*" * sum(r["p"] < thr for thr in (0.10, 0.05, 0.01))
        print(f"    {r['layer']:<32} {r['beta']:>10.4f} {r['se']:>8.4f} "
              f"{r['t']:>7.2f} {r['p']:>7.3f} {r['n_obs']:>7,} "
              f"{r['r2']:>7.4f} {star}")


# ──────────────────────────────────────────────
# Markdown report
# ──────────────────────────────────────────────

DIAG_MD_PATH = PROJECT_ROOT / "docs" / "research_notes" / "sentiment_diagnostics.md"

DIAG_MD_TEMPLATE = """\
# Sentiment Measurement Diagnostics

Sanity checks on the LinkedIn AI sentiment measure prior to using it in
downstream causal work. Each diagnostic answers a different question about
whether the measure tracks something real.

## Diagnostic 1 — Face validity

See `outputs/sanity_checks/face_validity_*` for the latest run. Headline:
post-ChatGPT (Nov 2022) AI-post share rose ~5.7× vs. the pre-ChatGPT
baseline; firm rankings within SIC 73 (software/business services)
recover the recognizable AI-forward names.

## Diagnostic 2 — Tobin's Q on AI sentiment (staggered build-up)

Question: does firm-level AI sentiment predict next-year firm value? We
build up the specification one layer at a time so we can see what each
layer absorbs, instead of reporting only the FE-saturated version.

Spec ladder (all on the same sample, SEs clustered by firm):

1. Bivariate pooled OLS: `ln(Q)_{t+1} ~ AI sentiment`
2. + Year FE
3. + Standard controls (`ln_at`, `leverage`, `rnd_int`, `profit_marg`)
4. + Firm FE (saturated)

Run as a three-way bracket to gauge measurement-error attenuation:
- **Full sample** — every LinkedIn URL we have a post for, including
  unvalidated ones
- **Strong-match (strict)** — Revelio confirms name *and* exact/legal-suffix
  company match
- **Strong-match (fuzzy)** — same, but the company match also accepts
  hyphen/space/abbreviation variants (SequenceMatcher ratio ≥ 0.80)

If the AI-sentiment coefficient grows as we tighten validation, the full
sample was attenuated by wrong-person noise — the IV's first-stage signal
is real.

### Full sample

<!-- DIAG2_FULL_START -->
*(run `python3 src/data_analysis/sentiment_q_regression.py` to populate)*
<!-- DIAG2_FULL_END -->

### Strong-match (strict)

<!-- DIAG2_STRONG_START -->
*(run `python3 src/data_analysis/sentiment_q_regression.py --strong-match-only` to populate)*
<!-- DIAG2_STRONG_END -->

### Strong-match (fuzzy)

<!-- DIAG2_STRONG_FUZZY_START -->
*(run `python3 src/data_analysis/sentiment_q_regression.py --strong-match-only --strong-match-variant fuzzy` to populate)*
<!-- DIAG2_STRONG_FUZZY_END -->

## Diagnostic 3 — Firm performance on general LinkedIn sentiment (match-quality metric)

Per Nick Bloom: *"when firms are doing well their execs are positive on
LinkedIn"*. The strength of this correlation is a match-quality metric —
if the strong-match-only coefficient is meaningfully larger than the full-
sample coefficient, wrong-person noise was attenuating the signal and our
matches are doing real work.

Same staggered ladder, same 3-way sample bracket, but with general
sentiment regressors (`mean_net_sentiment`, `engagement_wtd_sentiment`,
`role_wtd_sentiment`) and firm-performance outcomes.

### Sales growth — Δlog(sale)_{t+1}

#### Full sample
<!-- DIAG3_SALES_FULL_START -->
*(run `python3 src/data_analysis/sentiment_q_regression.py --outcome sales_growth`)*
<!-- DIAG3_SALES_FULL_END -->

#### Strong-match (strict)
<!-- DIAG3_SALES_STRONG_START -->
*(run with `--outcome sales_growth --strong-match-only`)*
<!-- DIAG3_SALES_STRONG_END -->

#### Strong-match (fuzzy)
<!-- DIAG3_SALES_STRONG_FUZZY_START -->
*(run with `--outcome sales_growth --strong-match-only --strong-match-variant fuzzy`)*
<!-- DIAG3_SALES_STRONG_FUZZY_END -->

### ROA — ni / at_{t+1}

#### Full sample
<!-- DIAG3_ROA_FULL_START -->
*(run `python3 src/data_analysis/sentiment_q_regression.py --outcome roa`)*
<!-- DIAG3_ROA_FULL_END -->

#### Strong-match (strict)
<!-- DIAG3_ROA_STRONG_START -->
*(run with `--outcome roa --strong-match-only`)*
<!-- DIAG3_ROA_STRONG_END -->

#### Strong-match (fuzzy)
<!-- DIAG3_ROA_STRONG_FUZZY_START -->
*(run with `--outcome roa --strong-match-only --strong-match-variant fuzzy`)*
<!-- DIAG3_ROA_STRONG_FUZZY_END -->

### Stock return — annual buy-and-hold_{t+1}

#### Full sample
<!-- DIAG3_RETURN_FULL_START -->
*(requires CRSP pull; run `python3 src/data_analysis/sentiment_q_regression.py --outcome stock_return`)*
<!-- DIAG3_RETURN_FULL_END -->

#### Strong-match (strict)
<!-- DIAG3_RETURN_STRONG_START -->
*(run with `--outcome stock_return --strong-match-only`)*
<!-- DIAG3_RETURN_STRONG_END -->

#### Strong-match (fuzzy)
<!-- DIAG3_RETURN_STRONG_FUZZY_START -->
*(run with `--outcome stock_return --strong-match-only --strong-match-variant fuzzy`)*
<!-- DIAG3_RETURN_STRONG_FUZZY_END -->
"""


def render_md_table(df: pd.DataFrame, run_meta: dict) -> str:
    """Markdown table for one sample's staggered results."""
    if df.empty:
        return "_No results — see run log._"

    filters = f"min_posts={run_meta['min_posts']}"
    if run_meta.get("min_ai_posts") is not None:
        filters += f", min_ai_posts={run_meta['min_ai_posts']}"
    lines = [
        f"_Run: {run_meta['ts']} · outcome: `{run_meta['outcome']}` · "
        f"panel: `{run_meta['sentiment_file']}` · "
        f"funda: `{run_meta['funda_file']}` · "
        f"{filters} · lead={run_meta['lead']}_",
        "",
        "| Regressor | Layer | β | SE | t | p | N | R² |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in df.iterrows():
        stars = "*" * sum(r["p"] < thr for thr in (0.10, 0.05, 0.01))
        lines.append(
            f"| `{r['regressor']}` | {r['layer']} | "
            f"{r['beta']:.4f}{stars} | {r['se']:.4f} | {r['t']:.2f} | "
            f"{r['p']:.3f} | {r['n_obs']:,} | {r['r2']:.4f} |"
        )
    lines.append("")
    lines.append("Significance: * p<0.10, ** p<0.05, *** p<0.01.")
    return "\n".join(lines)


SAMPLE_KIND_TO_MARKER = {
    "full":         "FULL",
    "strong":       "STRONG",
    "strong_fuzzy": "STRONG_FUZZY",
}

# Map outcome → (diagnostic number, outcome marker stem). Diagnostic 2 is
# specifically Tobin's Q with no outcome stem; Diagnostic 3 covers the
# performance bracket.
OUTCOME_TO_DIAG = {
    "tobins_q":     ("DIAG2", None),
    "sales_growth": ("DIAG3", "SALES"),
    "roa":          ("DIAG3", "ROA"),
    "stock_return": ("DIAG3", "RETURN"),
}


def _marker_for(outcome: str, sample_kind: str) -> str:
    """Build the marker stem like 'DIAG2_FULL' or 'DIAG3_SALES_STRONG_FUZZY'."""
    diag, stem = OUTCOME_TO_DIAG[outcome]
    sample_mk  = SAMPLE_KIND_TO_MARKER[sample_kind]
    return f"{diag}_{stem + '_' if stem else ''}{sample_mk}"


def _all_known_markers() -> list[str]:
    return [_marker_for(o, s) for o in OUTCOME_TO_DIAG for s in SAMPLE_KIND_TO_MARKER]


def update_diagnostics_md(new_block: str, outcome: str, sample_kind: str) -> None:
    """Replace the diagnostic table block for a given (outcome, sample)."""
    DIAG_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not DIAG_MD_PATH.exists():
        DIAG_MD_PATH.write_text(DIAG_MD_TEMPLATE)

    marker = _marker_for(outcome, sample_kind)
    start = f"<!-- {marker}_START -->"
    end   = f"<!-- {marker}_END -->"
    md = DIAG_MD_PATH.read_text()

    if start not in md or end not in md:
        # File predates this marker — rewrite from template, preserving
        # any blocks that already exist.
        existing_blocks = {}
        for mk in _all_known_markers():
            s = f"<!-- {mk}_START -->"
            e = f"<!-- {mk}_END -->"
            if s in md and e in md:
                existing_blocks[mk] = md.split(s, 1)[1].split(e, 1)[0]
        md = DIAG_MD_TEMPLATE
        for mk, content in existing_blocks.items():
            s = f"<!-- {mk}_START -->"
            e = f"<!-- {mk}_END -->"
            if s in md and e in md:
                pre, rest = md.split(s, 1)
                _, post = rest.split(e, 1)
                md = f"{pre}{s}{content}{e}{post}"

    pre, rest = md.split(start, 1)
    _, post = rest.split(end, 1)
    md = f"{pre}{start}\n{new_block}\n{end}{post}"
    DIAG_MD_PATH.write_text(md)
    print(f"[write] {DIAG_MD_PATH.relative_to(PROJECT_ROOT)} "
          f"(updated {marker} block)")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

DEFAULT_REGRESSORS_BY_OUTCOME = {
    "tobins_q":     ["ai_mean_net_sentiment", "ai_post_share"],
    "sales_growth": ["mean_net_sentiment", "engagement_wtd_sentiment",
                     "role_wtd_sentiment"],
    "roa":          ["mean_net_sentiment", "engagement_wtd_sentiment",
                     "role_wtd_sentiment"],
    "stock_return": ["mean_net_sentiment", "engagement_wtd_sentiment",
                     "role_wtd_sentiment"],
}

ALL_REGRESSORS = ["ai_mean_net_sentiment", "ai_post_share",
                  "mean_net_sentiment", "engagement_wtd_sentiment",
                  "role_wtd_sentiment"]


def main():
    parser = argparse.ArgumentParser(
        description="Firm-performance ~ LinkedIn sentiment staggered panel regression",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--outcome", choices=list(OUTCOMES.keys()),
                        default="tobins_q",
                        help="LHS variable. tobins_q (default) is the AI-sentiment "
                             "Q regression; sales_growth/roa/stock_return are the "
                             "general-sentiment match-quality checks (Diagnostic 3).")
    parser.add_argument("--regressor", action="append", choices=ALL_REGRESSORS,
                        help="RHS sentiment column(s). Repeat to add multiple. "
                             "Defaults: AI columns for tobins_q, general sentiment "
                             "columns for sales_growth/roa/stock_return.")
    parser.add_argument("--sentiment", type=str,
                        help="Override auto-detected annual sentiment CSV")
    parser.add_argument("--funda", type=str,
                        help="Override auto-detected Compustat funda CSV")
    parser.add_argument("--crsp", type=str,
                        help="Override auto-detected CRSP returns CSV (only used "
                             "when --outcome stock_return).")
    parser.add_argument("--min-posts", type=int, default=10,
                        help="Drop firm-years with fewer than N total posts (default: 10)")
    parser.add_argument("--min-ai-posts", type=int, default=3,
                        help="Drop firm-years with fewer than N AI posts. "
                             "Applied only when an AI regressor is selected (default: 3)")
    parser.add_argument("--lead", type=int, default=1,
                        help="Years ahead to measure the outcome (default: 1)")
    parser.add_argument("--strong-match-only", action="store_true",
                        help="Restrict to Revelio strong-match firm-years.")
    parser.add_argument("--strong-match-variant", choices=["strict", "fuzzy"],
                        default="strict",
                        help="Which strong-match definition to use when "
                             "--strong-match-only is set: 'strict' (default) or "
                             "'fuzzy' (SequenceMatcher >= 0.80).")
    parser.add_argument("--min-strong-match-share", type=float, default=0.5,
                        help="With --strong-match-only, drop firm-years below this "
                             "share of strong-match posts (default: 0.5).")
    args = parser.parse_args()

    if not args.regressor:
        args.regressor = DEFAULT_REGRESSORS_BY_OUTCOME[args.outcome]

    if args.sentiment:
        sent_path = Path(args.sentiment)
    else:
        # Pick the latest *full* annual aggregate, not the *_ai_only_* variant
        # (where ai_post_share is constant 1.0 by construction).
        candidates = sorted(p for p in SENTIMENT_DIR.glob("company_sentiment_annual_*.csv")
                            if "_ai_only_" not in p.name)
        sent_path = candidates[-1] if candidates else None
    funda_path = Path(args.funda) if args.funda else latest(
        "funda_*.csv", FUNDA_DIR)

    if sent_path is None or not sent_path.exists():
        sys.exit(f"[error] No company_sentiment_annual_*.csv found in {SENTIMENT_DIR}\n"
                 f"        Run: python3 src/data_analysis/aggregate_sentiment.py --run")
    if funda_path is None or not funda_path.exists():
        sys.exit(f"[error] No funda_*.csv found in {FUNDA_DIR}\n"
                 f"        Run: python3 src/data_extraction/build_compustat_funda.py --stats")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = OUTPUT_BASE / f"regression_{args.outcome}_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[out] {out_dir}")
    print(f"[outcome] {args.outcome} ({OUTCOMES[args.outcome]['label']})")
    print(f"[regressors] {args.regressor}\n")

    sent  = load_sentiment_annual(sent_path)
    funda = load_funda(funda_path)

    # If we're regressing stock returns, merge in the CRSP file
    if args.outcome == "stock_return":
        crsp_path = Path(args.crsp) if args.crsp else latest(
            "crsp_annual_returns_*.csv", CRSP_DIR)
        if crsp_path is None or not crsp_path.exists():
            sys.exit(f"[error] --outcome stock_return requires a CRSP file in "
                     f"{CRSP_DIR}\n        Run: python3 src/data_extraction/build_crsp_returns.py "
                     f"--wrds-username ml2068 --start-year 2010 --stats")
        funda = merge_crsp_returns(funda, crsp_path)

    # Filter sparse firm-years (always require N total posts so sentiment
    # isn't being driven by a single noisy post)
    if "n_posts" in sent.columns:
        before = len(sent)
        sent = sent[sent["n_posts"] >= args.min_posts].copy()
        print(f"[filter] n_posts ≥ {args.min_posts}: "
              f"{len(sent):,} / {before:,} firm-years kept")

    # Only enforce the AI-post floor when at least one chosen regressor is
    # AI-specific (otherwise we'd unnecessarily exclude general-sentiment firms).
    needs_ai_floor = any(r.startswith("ai_") for r in args.regressor)
    if needs_ai_floor and "n_ai_posts" in sent.columns:
        before = len(sent)
        sent = sent[sent["n_ai_posts"] >= args.min_ai_posts].copy()
        print(f"[filter] n_ai_posts ≥ {args.min_ai_posts}: "
              f"{len(sent):,} / {before:,} firm-years kept")

    # Optional Revelio strong-match gate
    if args.strong_match_only:
        share_col = ("strong_match_fuzzy_share" if args.strong_match_variant == "fuzzy"
                     else "strong_match_share")
        if share_col not in sent.columns:
            sys.exit(f"[error] --strong-match-only --strong-match-variant {args.strong_match_variant} "
                     f"requires '{share_col}' in the sentiment file. Re-run "
                     "aggregate_sentiment.py with data/revelio/revelio_validation_summary.csv present.")
        if sent[share_col].max() == 0:
            sys.exit(f"[error] {share_col} is 0 everywhere — Revelio "
                     "validation file was not loaded during aggregation. "
                     "Confirm data/revelio/revelio_validation_summary.csv "
                     "exists and re-run aggregate_sentiment.py.")
        before = len(sent)
        sent = sent[sent[share_col] >= args.min_strong_match_share].copy()
        print(f"[filter] {share_col} ≥ {args.min_strong_match_share}: "
              f"{len(sent):,} / {before:,} firm-years kept")

    # Sentiment columns we need on the panel: every chosen regressor.
    sent_cols = [c for c in args.regressor if c in sent.columns]
    missing = [c for c in args.regressor if c not in sent.columns]
    if missing:
        sys.exit(f"[error] Regressors not found in sentiment file: {missing}")
    if not sent_cols:
        sys.exit("[error] No regressor columns available on the sentiment file.")

    panel = build_panel(sent, funda, sent_cols, outcome=args.outcome, lead=args.lead)
    panel.to_csv(out_dir / "merged_panel.csv", index=False)
    panel_col = OUTCOMES[args.outcome]["panel_col"]
    print(f"\n[panel] {len(panel):,} firm-year observations with "
          f"{args.outcome}_{{t+{args.lead}}}, {panel['gvkey'].nunique():,} firms")

    # Staggered build-up across 4 nested specs, for each chosen regressor
    regressors = [r for r in args.regressor if r in panel.columns]
    if not regressors:
        sys.exit("[error] No regressor columns survived panel construction.")

    print("\n" + "=" * 60)
    print(f"Staggered regression: {OUTCOMES[args.outcome]['label']}_{{t+{args.lead}}} ~ X")
    print("All 4 layers run on the same sample; SEs clustered by firm.")
    print("=" * 60)

    results = run_staggered(panel, panel_col, regressors)
    if results.empty:
        sys.exit("[error] No layers produced results.")

    for r in regressors:
        print_staggered_table(results, r)

    # Persist machine-readable + text outputs
    results.to_csv(out_dir / "coefficients.csv", index=False)

    txt_lines = [
        f"Staggered Panel Regression — {OUTCOMES[args.outcome]['label']}",
        "=" * 60,
        f"Outcome: {args.outcome}",
        f"Sample: {'strong-match only ('+args.strong_match_variant+')' if args.strong_match_only else 'full'}",
        f"Min posts filter: n_posts >= {args.min_posts}"
        + (f", n_ai_posts >= {args.min_ai_posts}" if needs_ai_floor else ""),
        f"Lead: t+{args.lead}",
        "",
    ]
    for r in regressors:
        sub = results[results["regressor"] == r]
        txt_lines.append(f"[{r}]")
        txt_lines.append(
            f"  {'Layer':<32} {'β':>10} {'SE':>8} {'t':>7} {'p':>7} "
            f"{'N':>7} {'R²':>7}"
        )
        for _, row in sub.iterrows():
            stars = "*" * sum(row["p"] < thr for thr in (0.10, 0.05, 0.01))
            txt_lines.append(
                f"  {row['layer']:<32} {row['beta']:>10.4f} "
                f"{row['se']:>8.4f} {row['t']:>7.2f} {row['p']:>7.3f} "
                f"{row['n_obs']:>7,} {row['r2']:>7.4f} {stars}"
            )
        txt_lines.append("")
    (out_dir / "summary.txt").write_text("\n".join(txt_lines) + "\n")
    print(f"\n[write] {out_dir}/coefficients.csv")
    print(f"[write] {out_dir}/summary.txt")

    # Update the canonical research-notes table
    run_meta = {
        "ts":              ts,
        "outcome":         args.outcome,
        "sentiment_file":  sent_path.name,
        "funda_file":      funda_path.name,
        "min_posts":       args.min_posts,
        "min_ai_posts":    args.min_ai_posts if needs_ai_floor else None,
        "lead":            args.lead,
    }
    md_block = render_md_table(results, run_meta)
    if args.strong_match_only:
        sample_kind = "strong_fuzzy" if args.strong_match_variant == "fuzzy" else "strong"
    else:
        sample_kind = "full"
    update_diagnostics_md(md_block, args.outcome, sample_kind)

    # Quarterly robustness if file exists
    q_path = latest("company_sentiment_quarterly_*.csv", SENTIMENT_DIR)
    if q_path is not None:
        print(f"\n[info] Quarterly aggregate found ({q_path.name}); "
              f"not run automatically. Re-invoke with --sentiment {q_path.name} and "
              f"modify the panel build for quarter-level keys.")

    print(f"\n[done] Outputs in {out_dir}")


if __name__ == "__main__":
    main()

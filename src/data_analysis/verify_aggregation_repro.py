#!/usr/bin/env python3
"""Task 2 VERIFICATION GATE: prove aggregate_sentiment.py reproduces the EXISTING canonical
company_sentiment_annual from the EXISTING scored corpus, before any new data is folded in.

Memory-safe: reads full_coverage with usecols (drops the giant post_text/reshared_text);
pandas tokenizes the whole row before selecting usecols, so on_bad_lines skipping is identical
to a full read — the reproduction stays faithful. Uses the project's own functions.

PASS  => the pipeline is faithful; safe to add new posts.
FAIL  => stop and report (do not ship a drifted panel).
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src" / "data_analysis"))
import aggregate_sentiment as AGG  # noqa: E402

FULL_COVERAGE = ROOT / "outputs/sentiment_results/sentiment_all_posts_full_coverage_20260527.csv"
EXISTING_ANNUAL = ROOT / "outputs/sentiment_results/company_sentiment_annual_20260527_164007.csv"

USECOLS = [
    "company_name_clean", "person_name_clean", "gvkey", "ticker", "source", "position",
    "post_date", "profile_url", "is_ai_related", "reactions_total", "lm_word_count",
    "lm_net_sentiment", "lm_positive_ratio", "lm_negative_ratio", "lm_uncertainty_ratio",
    "lm_polarity",
]

KEYS = ["company_name_clean", "gvkey", "ticker", "year"]


def reproduce_annual(full_coverage_path: Path) -> pd.DataFrame:
    print(f"[load] {full_coverage_path.name} (usecols, memory-safe)…", flush=True)
    df = pd.read_csv(full_coverage_path, usecols=lambda c: c in set(USECOLS),
                     low_memory=False, engine="c", lineterminator="\n", on_bad_lines="skip")
    print(f"       {len(df):,} rows", flush=True)
    df = AGG.merge_all_people(df, AGG.default_paths()["all_people"])
    df = AGG.merge_revelio(df, AGG.default_paths()["revelio"])
    df = AGG.add_time_keys(df)
    df = df[df["year"].notna()].copy()
    print(f"[aggregate] annual ({len(df):,} posts)…", flush=True)
    return AGG.aggregate(df, AGG.GROUP_KEYS_ANNUAL)


def compare(repro: pd.DataFrame, existing_path: Path) -> bool:
    exist = pd.read_csv(existing_path, low_memory=False)
    print(f"\n[compare] repro rows={len(repro):,}  existing rows={len(exist):,}")
    # normalize key dtypes + sort
    for d in (repro, exist):
        d["gvkey"] = pd.to_numeric(d["gvkey"], errors="coerce")
        d["year"] = pd.to_numeric(d["year"], errors="coerce")
    common_cols = [c for c in exist.columns if c in repro.columns]
    r = repro[common_cols].sort_values(KEYS).reset_index(drop=True)
    e = exist[common_cols].sort_values(KEYS).reset_index(drop=True)

    if len(r) != len(e):
        print(f"  ✗ ROW COUNT MISMATCH: repro {len(r):,} vs existing {len(e):,}")
        # show key-set diff
        rk = set(map(tuple, r[KEYS].fillna(-1).values))
        ek = set(map(tuple, e[KEYS].fillna(-1).values))
        print(f"    keys only in repro: {len(rk - ek):,} | only in existing: {len(ek - rk):,}")
        return False

    ok = True
    num_cols = [c for c in common_cols if c not in KEYS and pd.api.types.is_numeric_dtype(e[c])]
    for c in num_cols:
        a = pd.to_numeric(r[c], errors="coerce")
        b = pd.to_numeric(e[c], errors="coerce")
        both_nan = a.isna() & b.isna()
        close = np.isclose(a, b, rtol=1e-6, atol=1e-6, equal_nan=True) | both_nan
        nbad = (~close).sum()
        if nbad:
            ok = False
            mx = (a - b).abs().max()
            print(f"  ✗ {c}: {nbad:,} rows differ (max abs diff {mx:.6g})")
    if ok:
        print("  ✓ ALL numeric columns match within tol on ALL company-years.")
    return ok


if __name__ == "__main__":
    repro = reproduce_annual(FULL_COVERAGE)
    passed = compare(repro, EXISTING_ANNUAL)
    print("\n" + ("GATE PASS ✅ — pipeline reproduces the canonical panel." if passed
                   else "GATE FAIL ❌ — STOP. Do not ship a drifted panel; investigate."))
    sys.exit(0 if passed else 1)

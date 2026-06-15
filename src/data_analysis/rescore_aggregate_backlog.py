"""Task 2: re-aggregate company_sentiment on the EXPANDED corpus (existing full_coverage +
backlog additions), then VERIFY that companies untouched by the new posts aggregate
identically to the existing canonical panel. Stop-and-report discipline: if any untouched
company-year drifts, we do NOT proceed.

Uses the project's own aggregate_sentiment functions (faithful, gate-proven) with a
memory-safe usecols load.
"""
import sys
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src" / "data_analysis"))
import aggregate_sentiment as AGG  # noqa: E402
from verify_aggregation_repro import USECOLS  # noqa: E402

EXIST_FC = ROOT / "outputs/sentiment_results/sentiment_all_posts_full_coverage_20260527.csv"
NEW_FC = sorted((ROOT / "data/processed").glob("backlog_full_coverage_*.csv"))[-1]
EXIST = {
    "annual": ROOT / "outputs/sentiment_results/company_sentiment_annual_20260527_164007.csv",
    "quarterly": ROOT / "outputs/sentiment_results/company_sentiment_quarterly_20260527_164007.csv",
    "annual_ai_only": ROOT / "outputs/sentiment_results/company_sentiment_annual_ai_only_20260527_164007.csv",
    "quarterly_ai_only": ROOT / "outputs/sentiment_results/company_sentiment_quarterly_ai_only_20260527_164007.csv",
}
KEYS = {"annual": AGG.GROUP_KEYS_ANNUAL, "quarterly": AGG.GROUP_KEYS_QUARTERLY,
        "annual_ai_only": AGG.GROUP_KEYS_ANNUAL, "quarterly_ai_only": AGG.GROUP_KEYS_QUARTERLY}


def load_fc(path, label):
    print(f"[load] {label}: {path.name}", flush=True)
    df = pd.read_csv(path, usecols=lambda c: c in set(USECOLS), low_memory=False,
                     engine="c", lineterminator="\n", on_bad_lines="skip")
    print(f"       {len(df):,} rows", flush=True)
    return df


def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    exist_fc = load_fc(EXIST_FC, "existing full_coverage")
    new_fc = load_fc(NEW_FC, "backlog full_coverage")
    affected = set(new_fc["company_name_clean"].dropna().unique())
    print(f"[scope] new posts touch {len(affected):,} distinct company_name_clean", flush=True)

    df = pd.concat([exist_fc, new_fc], ignore_index=True)
    del exist_fc, new_fc
    df = AGG.merge_all_people(df, AGG.default_paths()["all_people"])
    df = AGG.merge_revelio(df, AGG.default_paths()["revelio"])
    df = AGG.add_time_keys(df)
    df = df[df["year"].notna()].copy()
    df_ai = df[df["is_ai_related"]].copy()

    subsets = {"annual": (df, AGG.GROUP_KEYS_ANNUAL), "quarterly": (df, AGG.GROUP_KEYS_QUARTERLY),
               "annual_ai_only": (df_ai, AGG.GROUP_KEYS_ANNUAL), "quarterly_ai_only": (df_ai, AGG.GROUP_KEYS_QUARTERLY)}

    all_ok = True
    out_dir = ROOT / "outputs/sentiment_results"
    for name, (data, keys) in subsets.items():
        print(f"\n[aggregate] {name} ({len(data):,} posts)…", flush=True)
        agg = AGG.aggregate(data, keys)
        out = out_dir / f"company_sentiment_{name}_backlog_{ts}.csv"
        agg.to_csv(out, index=False)
        print(f"  wrote {out.name}  rows={len(agg):,}")

        # ---- VERIFY: untouched companies identical to existing ----
        exist = pd.read_csv(EXIST[name], low_memory=False)
        kcols = keys
        for d in (agg, exist):
            d["gvkey"] = pd.to_numeric(d["gvkey"], errors="coerce")
            if "year" in d.columns:
                d["year"] = pd.to_numeric(d["year"], errors="coerce")
        a = agg[~agg["company_name_clean"].isin(affected)].sort_values(kcols).reset_index(drop=True)
        e = exist[~exist["company_name_clean"].isin(affected)].sort_values(kcols).reset_index(drop=True)
        common = [c for c in exist.columns if c in agg.columns]
        if len(a) != len(e):
            print(f"  ✗ {name}: untouched row count {len(a):,} vs existing {len(e):,}")
            all_ok = False
            continue
        bad_cols = []
        for c in common:
            if c in kcols or not pd.api.types.is_numeric_dtype(e[c]):
                continue
            x = pd.to_numeric(a[c], errors="coerce"); y = pd.to_numeric(e[c], errors="coerce")
            if not (np.isclose(x, y, rtol=1e-6, atol=1e-6, equal_nan=True) | (x.isna() & y.isna())).all():
                bad_cols.append(c)
        if bad_cols:
            print(f"  ✗ {name}: untouched companies DRIFT in {bad_cols}")
            all_ok = False
        else:
            print(f"  ✓ {name}: {len(a):,} untouched company-rows IDENTICAL to canonical; "
                  f"{len(agg)-len(a):,} affected rows updated")

    print("\n" + ("VERIFY PASS ✅ — untouched companies identical; expanded panels written."
                  if all_ok else "VERIFY FAIL ❌ — STOP. untouched companies drifted."))
    if all_ok:
        open(ROOT / "data/processed/.backlog_aggregate_ts", "w").write(ts)
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()

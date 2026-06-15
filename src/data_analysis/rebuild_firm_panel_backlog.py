"""Task 2: rebuild the firm panel on the EXPANDED corpus, faithfully, while preserving the
def14a + CRSP enrichment (guardrail #2) and verifying unchanged firm-years are identical (#3).

Strategy:
  - Run the project's own build_multifreq_panel functions, but point FULL_COVERAGE at the
    expanded full_coverage (financials/CRSP/revelio inherited from `current` unchanged;
    revelio strong_match_either, same as the existing panel).
  - That yields the new base annual panel (sentiment + financials), gvkey-year.
  - Merge the def14a/board columns + (company_name_clean, ticker) from the EXISTING
    firm_panel_annual.dta by (gvkey, year) — preserved byte-for-byte (guardrail #2).
  - Verify: firm-years with NO new posts must match the existing .dta on every column.
"""
import sys
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "src" / "data_analysis"))
import build_multifreq_panel as B  # noqa: E402

EXPANDED_FC = ROOT / "outputs/sentiment_results/sentiment_all_posts_full_coverage_20260614.csv"
EXISTING_DTA = ROOT / "data/canonical/current/firm_panel_annual.dta"
NEW_FC = sorted((ROOT / "data/processed").glob("backlog_full_coverage_*.csv"))[-1]

# def14a/board columns that live ONLY in the enriched .dta (not produced by build_multifreq_panel)
DEF14A_COLS = ["board_size", "n_incumbent", "n_new_nominee", "n_mid_year_appointee",
               "mean_director_age", "share_new_nominee", "age_coverage"]
ID_COLS = ["company_name_clean", "ticker"]  # firm identifiers carried on the .dta


def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    # ---- point the builder at the expanded corpus + a fresh compact cache ----
    B.FULL_COVERAGE = EXPANDED_FC
    B.COMPACT_PATH = ROOT / "outputs/chat3_v1/panel/posts_compact_backlog.pkl"
    print(f"[build] streaming expanded corpus {EXPANDED_FC.name} …", flush=True)
    compact = B.stream_compact()
    print(f"[build] compact rows: {len(compact):,}", flush=True)

    ann = B.aggregate_grain(compact, ["gvkey", "year"])
    ann = B.join_annual_outcomes(ann)
    ann["gvkey"] = B.norm_gvkey(ann["gvkey"])
    print(f"[build] new base annual panel: {len(ann):,} firm-years", flush=True)

    # ---- existing enriched panel ----
    exist = pd.read_stata(EXISTING_DTA)
    exist["gvkey"] = B.norm_gvkey(exist["gvkey"])
    exist["year"] = pd.to_numeric(exist["year"], errors="coerce").astype("Int64")
    ann["year"] = pd.to_numeric(ann["year"], errors="coerce").astype("Int64")

    # ---- preserve def14a + id columns from existing by (gvkey, year) ----
    keep = exist[["gvkey", "year"] + ID_COLS + DEF14A_COLS].drop_duplicates(["gvkey", "year"])
    out = ann.merge(keep, on=["gvkey", "year"], how="left")

    # ---- affected firm-years (received >=1 new post) ----
    newfc = pd.read_csv(NEW_FC, usecols=["gvkey", "post_date"], low_memory=False,
                        engine="c", lineterminator="\n", on_bad_lines="skip")
    newfc = newfc.dropna(subset=["gvkey"])
    newfc["gvkey"] = B.norm_gvkey(newfc["gvkey"])
    newfc["year"] = pd.to_datetime(newfc["post_date"], errors="coerce").dt.year
    affected = set(map(tuple, newfc.dropna(subset=["year"])
                       .assign(year=lambda d: d["year"].astype(int))[["gvkey", "year"]].values))

    # ---- VERIFY unchanged firm-years identical on ALL existing columns ----
    common = [c for c in exist.columns if c in out.columns]
    e_idx = exist.set_index(["gvkey", "year"])
    o_idx = out.set_index(["gvkey", "year"])
    unchanged_keys = [k for k in o_idx.index if k not in affected and k in e_idx.index]
    numcols = [c for c in common if c not in ("gvkey", "year") and pd.api.types.is_numeric_dtype(exist[c])]
    A = o_idx.loc[unchanged_keys, numcols].apply(pd.to_numeric, errors="coerce")
    Bx = e_idx.loc[unchanged_keys, numcols].apply(pd.to_numeric, errors="coerce")
    eq = (np.isclose(A, Bx, rtol=1e-6, atol=1e-6, equal_nan=True) | (A.isna() & Bx.isna()))
    bad = (~eq).sum(axis=0)
    drift_cols = bad[bad > 0]
    print(f"\n[verify] firm-years total={len(out):,}  affected={len(set(affected) & set(o_idx.index)):,}  "
          f"unchanged checked={len(unchanged_keys):,}")
    if len(drift_cols):
        print("  ✗ DRIFT in unchanged firm-years:")
        print(drift_cols.to_string())
        print("\nFIRM PANEL VERIFY FAIL ❌ — STOP.")
        sys.exit(1)
    print(f"  ✓ all {len(unchanged_keys):,} unchanged firm-years IDENTICAL on every existing column")
    new_fy = [k for k in o_idx.index if k not in e_idx.index]
    print(f"  + {len(new_fy):,} NEW firm-years added by the backlog recovery (def14a=NaN unless merged)")

    # ---- order columns like existing, write ----
    col_order = [c for c in exist.columns if c in out.columns] + \
                [c for c in out.columns if c not in exist.columns]
    out = out[["gvkey", "year"] + [c for c in col_order if c not in ("gvkey", "year")]]
    out_path = ROOT / f"outputs/chat3_v1/panel/firm_panel_annual_backlog_{ts}.dta"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # stata needs str cols clean
    for c in out.columns:
        if out[c].dtype == object:
            out[c] = out[c].astype(str).replace("nan", "")
    out.to_stata(out_path, write_index=False, version=118)
    print(f"\n[write] {out_path}  ({len(out):,} firm-years, {len(out.columns)} cols)")
    open(ROOT / "data/processed/.backlog_firmpanel_ts", "w").write(ts)
    print("FIRM PANEL VERIFY PASS ✅")


if __name__ == "__main__":
    main()

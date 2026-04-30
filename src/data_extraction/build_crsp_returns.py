"""
Build CRSP Annual Stock Returns for Firms in the AI-Enthusiasm Sample
=====================================================================

Pulls monthly returns from WRDS CRSP (`crsp.msf`) for the GVKEYs in our
sample, links GVKEY ↔ PERMNO via the Compustat-CRSP link table
(`crsp.ccmxpf_lnkhist`), then aggregates monthly returns into annual
buy-and-hold returns aligned to fiscal year end.

Annual return formula (compounding monthly):
    annual_ret_{i,fyear} = ∏_{m=1..12} (1 + ret_{i,m}) - 1
where the 12-month window ends at the firm's fiscal-year-end month
(`comp.funda.fyr`).

Output: `data/extracted/crsp/crsp_annual_returns_{ts}.csv`
Columns: gvkey, permno, fyear, fyear_end_date, n_months, stock_return,
         compound_log_return

Usage:
    python3 src/data_extraction/build_crsp_returns.py --wrds-username ml2068
    python3 src/data_extraction/build_crsp_returns.py --wrds-username ml2068 --start-year 2010 --stats
    python3 src/data_extraction/build_crsp_returns.py --explore --wrds-username ml2068

Requirements:
    pip install wrds pandas
    WRDS account with CRSP + CCM access (credentials in ~/.pgpass)
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import wrds
except ImportError:
    import sys
    sys.exit("[error] wrds not installed — `pip install wrds`")


# =========================
# Configuration
# =========================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "extracted" / "crsp"
ALL_PEOPLE_CSV = PROJECT_ROOT / "data" / "extracted" / "combined" / "all_people.csv"

START_YEAR = 2010
END_YEAR = 2025


# =========================
# Helpers
# =========================

def load_gvkey_universe(path: Path) -> list[str]:
    if not path.exists():
        raise SystemExit(f"[error] GVKEY source not found: {path}")
    df = pd.read_csv(path, usecols=["gvkey"], low_memory=False)
    gvkeys = (
        df["gvkey"]
        .dropna()
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(6)
        .unique()
        .tolist()
    )
    gvkeys.sort()
    print(f"[load] {len(gvkeys):,} unique GVKEYs in sample universe ({path.name})")
    return gvkeys


def get_pgpass_password(username: str) -> str | None:
    pgpass = Path.home() / ".pgpass"
    if not pgpass.exists():
        return None
    for line in pgpass.read_text().splitlines():
        parts = line.strip().split(":", 4)
        if len(parts) == 5 and parts[3] == username:
            return parts[4]
    return None


# =========================
# WRDS queries
# =========================

def explore_tables(db):
    print("=" * 60)
    print("EXPLORING crsp.msf and ccmxpf_lnkhist")
    print("=" * 60)

    print("\n[1] crsp.msf columns:")
    cols = db.raw_sql("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'crsp' AND table_name = 'msf'
          AND column_name IN ('permno','date','ret','retx','prc','shrout','vol','cusip')
        ORDER BY column_name
    """)
    print(cols.to_string(index=False))

    print("\n[2] ccmxpf_lnkhist columns (Compustat ↔ CRSP link):")
    cols = db.raw_sql("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'crsp' AND table_name = 'ccmxpf_lnkhist'
          AND column_name IN ('gvkey','lpermno','linktype','linkprim','linkdt','linkenddt')
        ORDER BY column_name
    """)
    print(cols.to_string(index=False))

    print("\n[3] Sample link rows (first 5 active links):")
    sample = db.raw_sql("""
        SELECT gvkey, lpermno, linktype, linkprim, linkdt, linkenddt
        FROM crsp.ccmxpf_lnkhist
        WHERE linktype IN ('LU', 'LC') AND linkprim IN ('P', 'C')
          AND (linkenddt IS NULL OR linkenddt >= '2020-01-01')
        ORDER BY gvkey
        LIMIT 5
    """)
    print(sample.to_string(index=False))


def get_gvkey_permno_links(db, gvkeys: list[str]) -> pd.DataFrame:
    """Return current/historical primary GVKEY↔PERMNO links for our universe."""
    CHUNK = 1000
    frames = []
    print(f"\nFetching GVKEY↔PERMNO links for {len(gvkeys):,} GVKEYs …")
    for i in range(0, len(gvkeys), CHUNK):
        batch = gvkeys[i:i + CHUNK]
        in_clause = ",".join(f"'{g}'" for g in batch)
        q = f"""
            SELECT gvkey, lpermno AS permno, linkdt, linkenddt, linktype, linkprim
            FROM crsp.ccmxpf_lnkhist
            WHERE linktype IN ('LU', 'LC')
              AND linkprim IN ('P', 'C')
              AND gvkey IN ({in_clause})
        """
        frames.append(db.raw_sql(q))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"  retrieved {len(df):,} link rows for {df['gvkey'].nunique():,} GVKEYs")
    return df


def get_monthly_returns(db, permnos: list[int],
                        start_year: int, end_year: int) -> pd.DataFrame:
    """Pull monthly returns for the linked PERMNOs."""
    CHUNK = 2000
    frames = []
    print(f"\nFetching crsp.msf for {len(permnos):,} PERMNOs, "
          f"{start_year}–{end_year} …")
    start_date = f"{start_year - 1}-01-01"   # one extra year for fyear alignment
    end_date   = f"{end_year + 1}-12-31"
    for i in range(0, len(permnos), CHUNK):
        batch = permnos[i:i + CHUNK]
        in_clause = ",".join(str(p) for p in batch)
        q = f"""
            SELECT permno, date, ret
            FROM crsp.msf
            WHERE permno IN ({in_clause})
              AND date BETWEEN '{start_date}' AND '{end_date}'
        """
        chunk = db.raw_sql(q)
        chunk["date"] = pd.to_datetime(chunk["date"])
        frames.append(chunk)
        print(f"  batch {i // CHUNK + 1}: {len(chunk):,} monthly rows")
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    df = df.dropna(subset=["ret"])
    print(f"  total {len(df):,} monthly observations")
    return df


def get_fyear_ends(db, gvkeys: list[str],
                   start_year: int, end_year: int) -> pd.DataFrame:
    """Get fiscal-year-end month per GVKEY × fyear (for return windowing)."""
    CHUNK = 1000
    frames = []
    print(f"\nFetching fyear-end dates for {len(gvkeys):,} GVKEYs …")
    for i in range(0, len(gvkeys), CHUNK):
        batch = gvkeys[i:i + CHUNK]
        in_clause = ",".join(f"'{g}'" for g in batch)
        q = f"""
            SELECT gvkey, fyear, datadate
            FROM comp.funda
            WHERE indfmt='INDL' AND datafmt='STD'
              AND consol='C' AND popsrc='D'
              AND fyear BETWEEN {start_year} AND {end_year}
              AND gvkey IN ({in_clause})
        """
        frames.append(db.raw_sql(q))
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    df["datadate"] = pd.to_datetime(df["datadate"])
    df["fyear"] = df["fyear"].astype(int)
    print(f"  {len(df):,} firm-year-ends")
    return df


# =========================
# Aggregation
# =========================

def annual_returns_from_monthly(monthly: pd.DataFrame,
                                links: pd.DataFrame,
                                fyear_ends: pd.DataFrame) -> pd.DataFrame:
    """
    Build firm-year buy-and-hold returns: compound the 12 monthly returns
    ending at the fiscal-year-end month.
    """
    # Pick a single primary link per (permno, date) by taking the row whose
    # validity window covers the date. Use a vectorized join.
    links = links.copy()
    links["linkdt"]    = pd.to_datetime(links["linkdt"], errors="coerce")
    links["linkenddt"] = pd.to_datetime(links["linkenddt"], errors="coerce")
    # Some link rows have NaT linkenddt meaning still active — set to far future
    links["linkenddt"] = links["linkenddt"].fillna(pd.Timestamp("2099-12-31"))

    # Merge monthly returns to get gvkey
    monthly = monthly.merge(
        links[["permno", "gvkey", "linkdt", "linkenddt"]],
        on="permno", how="inner"
    )
    monthly = monthly[(monthly["date"] >= monthly["linkdt"]) &
                      (monthly["date"] <= monthly["linkenddt"])].copy()

    # For each (gvkey, fyear) we need the 12 months ending at datadate
    fyear_ends = fyear_ends.rename(columns={"datadate": "fyear_end"})
    fyear_ends["fyear_end_period"] = fyear_ends["fyear_end"].dt.to_period("M")
    monthly["month_period"] = monthly["date"].dt.to_period("M")

    rows = []
    # Build a (gvkey, month_period) → ret lookup
    mret_by_gvkey = monthly.groupby("gvkey")
    for gvkey, sub in mret_by_gvkey:
        sub = sub.set_index("month_period")["ret"].sort_index()
        firm_fy = fyear_ends[fyear_ends["gvkey"] == gvkey]
        for _, fy in firm_fy.iterrows():
            end_p = fy["fyear_end_period"]
            start_p = end_p - 11  # 12 months inclusive
            window = sub.loc[start_p:end_p]
            if len(window) >= 6:  # need at least half a year
                stock_return = float((1 + window).prod() - 1)
                clr = float(np.log1p(window).sum())
                rows.append({
                    "gvkey": gvkey,
                    "permno": sub.name if hasattr(sub, "name") else None,
                    "fyear": int(fy["fyear"]),
                    "fyear_end_date": fy["fyear_end"],
                    "n_months": int(len(window)),
                    "stock_return": stock_return,
                    "compound_log_return": clr,
                })
    out = pd.DataFrame(rows)
    return out


def print_stats(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Rows:              {len(df):,}")
    print(f"  Unique GVKEYs:     {df['gvkey'].nunique():,}")
    if len(df):
        print(f"  Fyear range:       {int(df['fyear'].min())}–{int(df['fyear'].max())}")
    if df["stock_return"].notna().any():
        r = df["stock_return"].dropna()
        print(f"  stock_return       mean={r.mean():.3f}  median={r.median():.3f}  "
              f"p10={r.quantile(0.1):.3f}  p90={r.quantile(0.9):.3f}")
    print(f"  Median months/window: {df['n_months'].median():.0f}")


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Pull annual buy-and-hold stock returns from CRSP for the AI-sample GVKEYs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--explore", action="store_true",
                        help="Inspect WRDS schemas and exit")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--wrds-username", type=str,
                        default=os.environ.get("WRDS_USERNAME"),
                        help="WRDS username (default: $WRDS_USERNAME). "
                             "Required for non-interactive runs; password read from ~/.pgpass.")
    args = parser.parse_args()

    print("=" * 60)
    print("CRSP Annual Returns Builder")
    print("=" * 60)
    print(f"  Years:  {args.start_year}–{args.end_year}")
    print(f"  Output: {args.output}")

    print("\nConnecting to WRDS …")
    conn_kwargs = {}
    if args.wrds_username:
        conn_kwargs["wrds_username"] = args.wrds_username
        pwd = get_pgpass_password(args.wrds_username)
        if pwd:
            conn_kwargs["wrds_password"] = pwd
    db = wrds.Connection(**conn_kwargs)
    print("  Connected")

    try:
        if args.explore:
            explore_tables(db)
            return

        gvkeys = load_gvkey_universe(ALL_PEOPLE_CSV)
        links = get_gvkey_permno_links(db, gvkeys)

        if links.empty:
            print("[error] No GVKEY↔PERMNO links found.")
            return

        permnos = sorted(set(int(p) for p in links["permno"].dropna().tolist()))
        monthly = get_monthly_returns(db, permnos, args.start_year, args.end_year)
        fyear_ends = get_fyear_ends(db, gvkeys, args.start_year, args.end_year)

        df = annual_returns_from_monthly(monthly, links, fyear_ends)
        if df.empty:
            print("[error] No annual returns built.")
            return

        if args.stats:
            print_stats(df)

        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = output_dir / f"crsp_annual_returns_{ts}.csv"
        df.to_csv(out_path, index=False)
        print(f"\n[write] {out_path}  ({len(df):,} rows)")

    finally:
        db.close()
        print("\nWRDS connection closed.")


if __name__ == "__main__":
    main()

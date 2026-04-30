"""
Build Compustat Annual Fundamentals for Firms in the AI-Enthusiasm Sample
=========================================================================

Pulls firm-year accounting data from WRDS Compustat (comp.funda) for the
set of GVKEYs appearing in data/extracted/combined/all_people.csv. Used
downstream to construct Tobin's Q and firm-level controls for the
sentiment sanity-check regression.

Data source:
    WRDS Compustat → comp.funda (standard filter: INDL / STD / C / D)

Output columns:
    gvkey, datadate, fyear, conm, sich
    at, lt, ceq, csho, prcc_f            # balance sheet + price
    sale, ni, xrd, capx, ppent           # flow / investment
    mkvalt                               # reported market value (if present)
    # Derived:
    market_cap      = csho * prcc_f
    tobins_q        = (market_cap + at - ceq) / at

Output file:
    data/extracted/compustat/funda_{YYYYMMDD_HHMMSS}.csv

Usage:
    python3 src/data_extraction/build_compustat_funda.py
    python3 src/data_extraction/build_compustat_funda.py --start-year 2010 --stats
    python3 src/data_extraction/build_compustat_funda.py --explore

Requirements:
    pip install wrds pandas
    WRDS account with Compustat access (credentials in ~/.pgpass)
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

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
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "extracted" / "compustat"
ALL_PEOPLE_CSV = PROJECT_ROOT / "data" / "extracted" / "combined" / "all_people.csv"

START_YEAR = 2010
END_YEAR = 2025


# =========================
# Helpers
# =========================

def load_gvkey_universe(path: Path) -> list[str]:
    """Return the sorted list of unique GVKEYs in the sample."""
    if not path.exists():
        raise SystemExit(f"[error] GVKEY source not found: {path}")
    df = pd.read_csv(path, usecols=["gvkey"], low_memory=False)
    # gvkey is a 6-digit string in Compustat; normalize
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


# =========================
# WRDS queries
# =========================

def explore_tables(db):
    print("=" * 60)
    print("EXPLORING comp.funda")
    print("=" * 60)

    print("\n[1] Column sample:")
    cols = db.raw_sql("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'comp' AND table_name = 'funda'
          AND column_name IN
              ('gvkey','datadate','fyear','conm','sich','at','lt','ceq',
               'csho','prcc_f','sale','ni','xrd','capx','ppent','mkvalt')
        ORDER BY column_name
    """)
    print(cols.to_string(index=False))

    print("\n[2] Row counts by fyear (INDL/STD/C/D):")
    counts = db.raw_sql("""
        SELECT fyear, COUNT(*) AS records
        FROM comp.funda
        WHERE indfmt='INDL' AND datafmt='STD' AND consol='C' AND popsrc='D'
          AND fyear BETWEEN 2010 AND 2025
        GROUP BY fyear ORDER BY fyear
    """)
    print(counts.to_string(index=False))


def get_funda(db, gvkeys: list[str], start_year: int, end_year: int) -> pd.DataFrame:
    """
    Pull Compustat Annual Fundamentals for a GVKEY universe.

    Standard US industrial firm filter: indfmt='INDL', datafmt='STD',
    consol='C', popsrc='D'. Keeps the canonical one-row-per-firm-year shape.
    """
    # Chunk the GVKEY list to avoid a monster IN clause.
    # Postgres handles ~thousands in IN, but we chunk for safety.
    CHUNK = 1000
    frames = []
    print(f"\nPulling comp.funda for {len(gvkeys):,} GVKEYs, "
          f"fyear {start_year}–{end_year} …")

    for i in range(0, len(gvkeys), CHUNK):
        batch = gvkeys[i:i + CHUNK]
        in_clause = ",".join(f"'{g}'" for g in batch)
        query = f"""
            SELECT
                gvkey, datadate, fyear, conm, sich,
                at, lt, ceq, csho, prcc_f,
                sale, ni, xrd, capx, ppent, mkvalt
            FROM comp.funda
            WHERE indfmt='INDL' AND datafmt='STD'
              AND consol='C' AND popsrc='D'
              AND fyear BETWEEN {start_year} AND {end_year}
              AND gvkey IN ({in_clause})
        """
        chunk_df = db.raw_sql(query)
        frames.append(chunk_df)
        print(f"  batch {i//CHUNK + 1}: {len(chunk_df):,} rows")

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"  Retrieved {len(df):,} firm-year records "
          f"across {df['gvkey'].nunique():,} GVKEYs")
    return df


# =========================
# Derivations
# =========================

def derive_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Compute market cap and Tobin's Q (simple approximation)."""
    df = df.copy()
    df["market_cap"] = df["csho"] * df["prcc_f"]
    # Tobin's Q = (mkt equity + total assets - book equity) / total assets
    # Missing denominators or equity → NaN (downstream filters on notna)
    with pd.option_context("mode.use_inf_as_na", True):
        df["tobins_q"] = (df["market_cap"] + df["at"] - df["ceq"]) / df["at"]
    return df


def print_stats(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Rows:                 {len(df):,}")
    print(f"  Unique GVKEYs:        {df['gvkey'].nunique():,}")
    if "fyear" in df.columns and len(df):
        print(f"  Fyear range:          {int(df['fyear'].min())}–{int(df['fyear'].max())}")
    print(f"  Non-null market_cap:  {df['market_cap'].notna().sum():,}")
    print(f"  Non-null tobins_q:    {df['tobins_q'].notna().sum():,}")
    if df["tobins_q"].notna().any():
        q = df["tobins_q"].dropna()
        print(f"  Tobin's Q quantiles:  "
              f"p10={q.quantile(0.10):.2f}  p50={q.quantile(0.50):.2f}  "
              f"p90={q.quantile(0.90):.2f}")
    print("\n  Non-null rate by field:")
    for col in ["at", "lt", "ceq", "csho", "prcc_f", "sale", "ni", "xrd",
                "capx", "ppent", "sich"]:
        if col in df.columns:
            pct = df[col].notna().mean() * 100
            print(f"    {col:<12} {pct:5.1f}%")


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Pull Compustat Annual Fundamentals for the AI-sample GVKEYs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--explore", action="store_true",
                        help="Inspect table structure and exit")
    parser.add_argument("--start-year", type=int, default=START_YEAR,
                        help=f"First fiscal year (default: {START_YEAR})")
    parser.add_argument("--end-year", type=int, default=END_YEAR,
                        help=f"Last fiscal year (default: {END_YEAR})")
    parser.add_argument("--all-gvkeys", action="store_true",
                        help="Pull all GVKEYs in Compustat, not just sample universe")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT),
                        help=f"Output directory (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--stats", action="store_true",
                        help="Print summary statistics after pull")
    parser.add_argument("--wrds-username", type=str,
                        default=os.environ.get("WRDS_USERNAME"),
                        help="WRDS username (default: $WRDS_USERNAME). "
                             "Required for non-interactive runs; password is read from ~/.pgpass.")
    args = parser.parse_args()

    print("=" * 60)
    print("Compustat Annual Fundamentals Builder")
    print("=" * 60)
    print(f"  Years:  {args.start_year}–{args.end_year}")
    print(f"  Output: {args.output}")

    print("\nConnecting to WRDS …")
    conn_kwargs = {}
    if args.wrds_username:
        conn_kwargs["wrds_username"] = args.wrds_username
        # wrds library doesn't actually consult ~/.pgpass for the password
        # when wrds_username is supplied — read it ourselves.
        pgpass = Path.home() / ".pgpass"
        if pgpass.exists():
            for line in pgpass.read_text().splitlines():
                parts = line.strip().split(":")
                if len(parts) == 5 and parts[3] == args.wrds_username:
                    conn_kwargs["wrds_password"] = parts[4]
                    break
    db = wrds.Connection(**conn_kwargs)
    print("  Connected")

    try:
        if args.explore:
            explore_tables(db)
            return

        if args.all_gvkeys:
            # Unrestricted pull — rare; user opts in explicitly.
            print("\n[mode] Pulling ALL Compustat GVKEYs (--all-gvkeys)")
            df = db.raw_sql(f"""
                SELECT gvkey, datadate, fyear, conm, sich,
                       at, lt, ceq, csho, prcc_f,
                       sale, ni, xrd, capx, ppent, mkvalt
                FROM comp.funda
                WHERE indfmt='INDL' AND datafmt='STD'
                  AND consol='C' AND popsrc='D'
                  AND fyear BETWEEN {args.start_year} AND {args.end_year}
            """)
        else:
            gvkeys = load_gvkey_universe(ALL_PEOPLE_CSV)
            df = get_funda(db, gvkeys, args.start_year, args.end_year)

        if df.empty:
            print("\n[error] No rows returned.")
            return

        df = derive_fields(df)

        if args.stats:
            print_stats(df)

        # Write
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = output_dir / f"funda_{ts}.csv"
        df.to_csv(out_path, index=False)
        print(f"\n[write] {out_path}  ({len(df):,} rows)")

    finally:
        db.close()
        print("\nWRDS connection closed.")


if __name__ == "__main__":
    main()

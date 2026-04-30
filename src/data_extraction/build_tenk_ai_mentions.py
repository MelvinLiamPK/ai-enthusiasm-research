"""
Build 10-K AI-Mention Counts for the AI-Enthusiasm Pilot
========================================================

Pulls 10-K filing text from WRDS for the top-N firms by LinkedIn post
volume, applies the same AI keyword regex used on LinkedIn posts
(`src/data_analysis/sentiment_analysis_full.py`), and emits per-firm-year
counts: total words, AI mentions, mention ratio.

Pilot scope: most recent 10-K per firm × top 50–100 firms by post count.
Used downstream by `tenk_vs_linkedin_correlation.py` to produce the
deck-ready scatter (10-K AI mentions vs LinkedIn AI post share).

Workflow:
    1. python3 build_tenk_ai_mentions.py --wrds-username ml2068 --explore
       → discover the right WRDS table for 10-K text
    2. python3 build_tenk_ai_mentions.py --wrds-username ml2068 --pilot-top-n 100 --stats
       → pull and compute mentions

Output: data/extracted/tenk/tenk_ai_mentions_{YYYYMMDD_HHMMSS}.csv
Columns: gvkey, cik, fyear, accession, n_words, n_ai_mentions, ai_mention_ratio
"""

import argparse
import os
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    import wrds
except ImportError:
    sys.exit("[error] wrds not installed — `pip install wrds`")


# =========================
# Configuration
# =========================
SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "extracted" / "tenk"
SENT_DIR = PROJECT_ROOT / "outputs" / "sentiment_results"


# =========================
# AI keywords (single source of truth: sentiment_analysis_full.py)
# =========================

# Mirror of AI_KEYWORDS in src/data_analysis/sentiment_analysis_full.py:62-68.
# Re-imported here rather than literally `from sentiment_analysis_full import …`
# because that module imports heavy NLP deps (transformers etc.) we don't need.
AI_KEYWORDS = [
    'artificial intelligence', ' ai ', 'machine learning', ' ml ', 'deep learning',
    'neural network', 'llm', 'large language model', 'generative ai', 'gen ai',
    'chatgpt', 'gpt', 'claude', 'gemini', 'copilot', 'automation', 'algorithm',
    'data science', 'predictive analytics', 'nlp', 'natural language processing',
    'computer vision', 'robotics', 'autonomous',
]

# Word-boundary patterns. The `' ai '` and `' ml '` keywords are pre-padded
# with spaces, so use space-aware matching (same as the LinkedIn pipeline).
_AI_PATTERNS = []
for kw in AI_KEYWORDS:
    if kw.startswith(' ') and kw.endswith(' '):
        # Already space-padded — keep as-is, don't word-boundary it
        _AI_PATTERNS.append(re.compile(re.escape(kw), re.IGNORECASE))
    else:
        _AI_PATTERNS.append(re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE))


def count_ai_mentions(text: str) -> int:
    """Count word-boundary AI keyword matches across all keywords."""
    if not text:
        return 0
    return sum(len(p.findall(text)) for p in _AI_PATTERNS)


# =========================
# Text cleaning
# =========================

_TAG_RE   = re.compile(r"<[^>]+>")            # strip HTML/SGML tags
_AMP_RE   = re.compile(r"&[a-zA-Z]+;|&#\d+;") # strip HTML entities
_WS_RE    = re.compile(r"\s+")
_TBL_RE   = re.compile(r"<TABLE[\s>].*?</TABLE>", re.IGNORECASE | re.DOTALL)
_DOC_HDR  = re.compile(r"<TYPE>(?!10-K).*?</TEXT>", re.IGNORECASE | re.DOTALL)


def clean_filing_text(raw: str) -> str:
    """Strip SGML headers, exhibit tables, HTML tags, normalize whitespace."""
    if not raw:
        return ""
    # Drop non-10-K exhibits inside the SEC SGML wrapper
    text = _DOC_HDR.sub(" ", raw)
    # Drop large tables (financial statements) — they're noise for keyword counts
    text = _TBL_RE.sub(" ", text)
    # Strip remaining HTML/SGML
    text = _TAG_RE.sub(" ", text)
    text = _AMP_RE.sub(" ", text)
    # Normalize Unicode (e.g. smart quotes → ASCII)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = _WS_RE.sub(" ", text).strip()
    return text


# =========================
# WRDS connection
# =========================

def get_pgpass_password(username: str) -> str | None:
    pgpass = Path.home() / ".pgpass"
    if not pgpass.exists():
        return None
    for line in pgpass.read_text().splitlines():
        parts = line.strip().split(":", 4)
        if len(parts) == 5 and parts[3] == username:
            return parts[4]
    return None


def connect(args) -> "wrds.Connection":
    print("\nConnecting to WRDS …")
    kw = {}
    if args.wrds_username:
        kw["wrds_username"] = args.wrds_username
        pwd = get_pgpass_password(args.wrds_username)
        if pwd:
            kw["wrds_password"] = pwd
    db = wrds.Connection(**kw)
    print("  Connected")
    return db


# =========================
# Discovery (--explore)
# =========================

def explore(db) -> None:
    """List candidate SEC text schemas/tables on WRDS."""
    print("=" * 60)
    print("EXPLORING WRDS for 10-K text tables")
    print("=" * 60)

    print("\n[1] Schemas with 'sec' or 'tenk' or 'edgar' in the name:")
    rows = db.raw_sql("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name ILIKE '%sec%'
           OR schema_name ILIKE '%tenk%'
           OR schema_name ILIKE '%edgar%'
           OR schema_name ILIKE '%filings%'
           OR schema_name ILIKE '%lm_text%'
        ORDER BY schema_name
    """)
    print(rows.to_string(index=False))

    print("\n[2] Tables in those schemas with 'tenk', 'tenk_', '10k', or 'form' in the name:")
    candidates = rows["schema_name"].tolist()
    if not candidates:
        print("  none")
        return

    sch_clause = ",".join(f"'{s}'" for s in candidates)
    tabs = db.raw_sql(f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ({sch_clause})
          AND (table_name ILIKE '%tenk%'
               OR table_name ILIKE '%10k%'
               OR table_name ILIKE '%form%'
               OR table_name ILIKE '%text%'
               OR table_name ILIKE '%filing%')
        ORDER BY table_schema, table_name
    """)
    print(tabs.to_string(index=False))

    if tabs.empty:
        print("  No matching tables found.")
        return

    print("\n[3] Column samples for each candidate table:")
    for _, row in tabs.iterrows():
        sch, tab = row["table_schema"], row["table_name"]
        print(f"\n  --- {sch}.{tab} ---")
        try:
            cols = db.raw_sql(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{sch}' AND table_name = '{tab}'
                ORDER BY ordinal_position
                LIMIT 30
            """)
            print(cols.to_string(index=False))
        except Exception as e:
            print(f"  [error] {e}")

    print("\n[4] GVKEY ↔ CIK link table (for filtering):")
    try:
        cols = db.raw_sql("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema IN ('wrdsapps_link_crsp_compustat', 'wrdsapps')
              AND table_name ILIKE '%cik%'
            ORDER BY table_schema, table_name, ordinal_position
            LIMIT 30
        """)
        print(cols.to_string(index=False))
    except Exception as e:
        print(f"  [error] {e}")


# =========================
# Pilot universe
# =========================

def latest(glob_pat: str, directory: Path) -> Path | None:
    cands = sorted(p for p in directory.glob(glob_pat) if "_ai_only_" not in p.name)
    return cands[-1] if cands else None


def load_pilot_universe(top_n: int) -> pd.DataFrame:
    """Top-N firms by LinkedIn post count, with their gvkey + ticker."""
    sent_path = latest("company_sentiment_annual_*.csv", SENT_DIR)
    if sent_path is None:
        sys.exit("[error] No company_sentiment_annual_*.csv found. "
                 "Run aggregate_sentiment.py first.")
    print(f"[load] {sent_path.name}")
    df = pd.read_csv(sent_path, low_memory=False)
    df = df[df["gvkey"].notna()].copy()
    by_firm = (df.groupby(["gvkey", "company_name_clean", "ticker"], dropna=False)
                 ["n_posts"].sum().reset_index()
                 .sort_values("n_posts", ascending=False))
    pilot = by_firm.head(top_n).copy()
    pilot["gvkey"] = pilot["gvkey"].astype(int).astype(str).str.zfill(6)
    print(f"[pilot] top-{top_n}: {len(pilot)} firms, "
          f"sum n_posts = {pilot['n_posts'].sum():,}")
    return pilot


# =========================
# 10-K pull
# =========================

# Discovered via --explore:
#   wrds_sec_search.filing_10_k  → 228k 10-Ks (column `filing` = full text)
#   wrdssec_all.wrds_forms       → has both `accession` and `cik`
#   comp.company                 → has `gvkey` and `cik`
TENK_TEXT_TABLE = "wrds_sec_search.filing_10_k"
ACCESSION_CIK_TABLE = "wrdssec_all.wrds_forms"


def get_cik_for_gvkeys(db, gvkeys: list[str]) -> pd.DataFrame:
    """GVKEY → CIK from comp.company.cik. Keeps 10-digit zero-padded CIKs
    (the format used by both `comp.company` and `wrdssec_all.wrds_forms`)."""
    in_clause = ",".join(f"'{g}'" for g in gvkeys)
    df = db.raw_sql(f"""
        SELECT gvkey, cik
        FROM comp.company
        WHERE gvkey IN ({in_clause}) AND cik IS NOT NULL
    """)
    df["gvkey"] = df["gvkey"].astype(str).str.zfill(6)
    df["cik"]   = df["cik"].astype(str).str.zfill(10)
    df = df.dropna(subset=["cik"]).drop_duplicates(subset=["gvkey"])
    print(f"[link] comp.company: {len(df):,} GVKEY→CIK rows "
          f"({len(df)}/{len(gvkeys)} mapped)")
    return df


def pull_tenk_text(db, ciks: list[str],
                   start_year: int, end_year: int,
                   chunk_size: int = 500) -> pd.DataFrame:
    """
    Pull 10-K filing text for the given CIKs. Two-step join via accession,
    chunked to avoid hung WRDS connections on huge IN clauses.

      1. wrdssec_all.wrds_forms  → accession numbers (chunked by CIK)
      2. wrds_sec_search.filing_10_k → text body (chunked by accession)
    """
    print(f"\n[step 1/2] Fetching 10-K accession list for {len(ciks):,} CIKs "
          f"(chunks of {chunk_size}) …", flush=True)
    accs_frames = []
    for i in range(0, len(ciks), chunk_size):
        batch = ciks[i:i + chunk_size]
        in_clause = ",".join(f"'{c}'" for c in batch)
        chunk = db.raw_sql(f"""
            SELECT cik, accession, fdate, rdate, form
            FROM {ACCESSION_CIK_TABLE}
            WHERE form = '10-K'
              AND fdate BETWEEN '{start_year}-01-01' AND '{end_year}-12-31'
              AND cik IN ({in_clause})
        """)
        accs_frames.append(chunk)
        print(f"  chunk {i // chunk_size + 1}/{(len(ciks) + chunk_size - 1) // chunk_size}: "
              f"{len(chunk):,} accessions", flush=True)

    accs = pd.concat(accs_frames, ignore_index=True) if accs_frames else pd.DataFrame()
    if accs.empty:
        return accs
    accs["cik"] = accs["cik"].astype(str).str.zfill(10)
    print(f"  total: {len(accs):,} 10-K accessions across "
          f"{accs['cik'].nunique():,} CIKs", flush=True)

    # Stream-and-count: pull each chunk's text, compute n_words / n_ai_mentions
    # immediately, then DROP the body. Otherwise the checkpoint files balloon
    # to ~10 GB across the whole panel. The count-only checkpoints are tiny
    # (KBs per chunk) and let us resume cleanly after a kill.
    accessions = accs["accession"].dropna().unique().tolist()
    chunks_dir = DEFAULT_OUTPUT / "_chunk_counts"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[step 2/2] Pulling + counting from {TENK_TEXT_TABLE} "
          f"(chunks of {chunk_size}, count-only checkpoints in {chunks_dir.name}/) …",
          flush=True)

    n_chunks = (len(accessions) + chunk_size - 1) // chunk_size
    count_frames = []
    for i in range(0, len(accessions), chunk_size):
        batch = accessions[i:i + chunk_size]
        chunk_idx = i // chunk_size + 1
        chunk_path = chunks_dir / f"chunk_{chunk_idx:03d}.pkl"

        if chunk_path.exists():
            cf = pd.read_pickle(chunk_path)
            count_frames.append(cf)
            print(f"  chunk {chunk_idx}/{n_chunks}: cached "
                  f"({len(cf):,} rows)", flush=True)
            continue

        in_clause = ",".join(f"'{a}'" for a in batch)
        raw = db.raw_sql(f"""
            SELECT accession, filing AS body
            FROM {TENK_TEXT_TABLE}
            WHERE accession IN ({in_clause})
        """)
        # Process and discard — no body field stored
        rows = []
        for _, r in raw.iterrows():
            cleaned = clean_filing_text(r.get("body", "") or "")
            n_words = len(cleaned.split()) if cleaned else 0
            n_ai = count_ai_mentions(cleaned)
            rows.append({
                "accession":     r["accession"],
                "n_words":       n_words,
                "n_ai_mentions": n_ai,
            })
        cf = pd.DataFrame(rows)
        cf.to_pickle(chunk_path)
        del raw                     # free memory
        count_frames.append(cf)
        cum = sum(len(c) for c in count_frames)
        print(f"  chunk {chunk_idx}/{n_chunks}: +{len(cf):,} ({cum:,} cumulative, "
              f"avg ai_mentions={cf['n_ai_mentions'].mean():.1f})", flush=True)

    counts = pd.concat(count_frames, ignore_index=True) if count_frames else pd.DataFrame()
    print(f"  total: {len(counts):,} filings counted", flush=True)

    df = accs.merge(counts, on="accession", how="inner")
    df["fyear"] = pd.to_datetime(df["rdate"]).dt.year
    return df


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Pilot 10-K AI-mention counts via WRDS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--explore", action="store_true",
                        help="Probe WRDS for 10-K text schemas and exit.")
    parser.add_argument("--pilot-top-n", type=int, default=100,
                        help="Pull 10-Ks for the top-N firms by LinkedIn post volume "
                             "(default: 100).")
    parser.add_argument("--start-year", type=int, default=2018)
    parser.add_argument("--end-year",   type=int, default=2024)
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--wrds-username", type=str,
                        default=os.environ.get("WRDS_USERNAME"))
    args = parser.parse_args()

    db = connect(args)
    try:
        if args.explore:
            explore(db)
            return

        # Pilot universe
        pilot = load_pilot_universe(args.pilot_top_n)
        gvkeys = pilot["gvkey"].tolist()

        # GVKEY → CIK
        link = get_cik_for_gvkeys(db, gvkeys)
        ciks = link["cik"].dropna().unique().tolist()
        if not ciks:
            sys.exit("[error] No CIKs resolved.")

        # Pull text + count in the same chunk loop (counts checkpointed,
        # text discarded on the fly to keep disk footprint tiny).
        tenk = pull_tenk_text(db, ciks, args.start_year, args.end_year)
        if tenk.empty:
            sys.exit(f"[error] No 10-K rows from {TENK_TEXT_TABLE}.")

        # Most-recent only per (cik, fyear) to dedupe 10-K amendments etc.
        tenk = (tenk.sort_values(["cik", "fyear"])
                    .drop_duplicates(subset=["cik", "fyear"], keep="last"))

        out = tenk[["cik", "fyear", "accession", "n_words",
                    "n_ai_mentions"]].copy()
        out["cik"] = out["cik"].astype(str).str.zfill(10)
        out["fyear"] = out["fyear"].astype("Int64")
        out["ai_mention_ratio"] = out["n_ai_mentions"] / out["n_words"].where(
            out["n_words"] > 0
        )
        out["ai_mention_ratio"] = out["ai_mention_ratio"].fillna(0.0)

        out = out.merge(link, on="cik", how="left").merge(
            pilot[["gvkey", "company_name_clean", "ticker", "n_posts"]],
            on="gvkey", how="left",
        )

        if args.stats:
            print("\n" + "=" * 60)
            print("SUMMARY")
            print("=" * 60)
            print(f"  Filings:                {len(out):,}")
            print(f"  Unique GVKEYs:          {out['gvkey'].nunique():,}")
            print(f"  Mean n_words:           {out['n_words'].mean():,.0f}")
            print(f"  Mean n_ai_mentions:     {out['n_ai_mentions'].mean():.1f}")
            print(f"  Mean ai_mention_ratio:  {out['ai_mention_ratio'].mean()*1000:.2f} per 1K words")
            print(f"  Top 10 by AI mentions:")
            print(out.nlargest(10, "n_ai_mentions")[["company_name_clean", "fyear",
                  "n_words", "n_ai_mentions", "ai_mention_ratio"]]
                  .to_string(index=False))

        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"tenk_ai_mentions_{ts}.csv"
        out.to_csv(out_path, index=False)
        print(f"\n[write] {out_path}  ({len(out):,} rows)")

    finally:
        db.close()
        print("\nWRDS connection closed.")


if __name__ == "__main__":
    main()

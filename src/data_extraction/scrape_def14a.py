"""
DEF 14A Proxy Statement Scraper
================================

Downloads DEF 14A proxy statements from SEC EDGAR for companies in our
directors universe, caching the primary document HTML to disk so they can
be parsed for director bios in a separate step.

Why DEF 14A:
    Director bios in DEF 14A include the director's *primary* employer/role
    (e.g. "Mr. Smith is President and CEO of XYZ Corp"), which is the
    company they likely list on LinkedIn — unlike the board company in our
    WRDS data, which directors rarely list on LinkedIn. We use the
    extracted primary employer as a stronger anchor for LinkedIn URL
    discovery, especially for the 27k unmatched directors.

Pipeline:
    1. Load input CSV (default: data/extracted/directors/directors_all.csv).
    2. Resolve ticker -> CIK via SEC's company_tickers.json (cached).
    3. For each (CIK, year), query SEC EDGAR submissions API for DEF 14A
       filings with filingDate in that fiscal year.
    4. Download the primary document and cache to data/raw/def14a/.
    5. Write a manifest CSV recording every (cik, year) attempt + outcome.

Modes:
    --mode prototype  10 hand-picked S&P 100 tickers x latest year (smoke test)
    --mode sp100      Top 100 most frequent tickers in input x latest year
    --mode full       Every (ticker, year) in the input CSV

Rate limiting:
    SEC fair-use policy: <=10 req/s with a descriptive User-Agent. We use
    8 req/s to stay safely under.

Usage:
    python3 src/data_extraction/scrape_def14a.py --mode prototype
    python3 src/data_extraction/scrape_def14a.py --mode sp100
    python3 src/data_extraction/scrape_def14a.py --mode full --start-year 2020
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Iterable, Optional

import requests


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DEFAULT_INPUT = PROJECT_ROOT / "data" / "extracted" / "directors" / "directors_all.csv"
DEFAULT_FILING_DIR = PROJECT_ROOT / "data" / "raw" / "def14a"
DEFAULT_MANIFEST = DEFAULT_FILING_DIR / "manifest.csv"
TICKER_CACHE = DEFAULT_FILING_DIR / "_ticker_to_cik.json"

SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik10}.json"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{doc}"

# SEC requires a descriptive User-Agent identifying the requester.
USER_AGENT = "Stanford GSB AI Enthusiasm Research - Melvin Liam ml2068@stanford.edu"
RATE_LIMIT_SLEEP = 1.0 / 8  # 8 req/s ceiling (SEC allows 10)

PROTOTYPE_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "JPM", "JNJ", "WMT",
]


# =========================
# HTTP layer
# =========================

_session: Optional[requests.Session] = None
_last_request_at = 0.0


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate",
        })
    return _session


def throttled_get(url: str, timeout: int = 30) -> requests.Response:
    """GET with simple global rate limiting to honor SEC fair-use."""
    global _last_request_at
    now = time.time()
    wait = RATE_LIMIT_SLEEP - (now - _last_request_at)
    if wait > 0:
        time.sleep(wait)
    resp = get_session().get(url, timeout=timeout)
    _last_request_at = time.time()
    return resp


# =========================
# Ticker -> CIK mapping
# =========================

def load_ticker_to_cik(refresh: bool = False) -> dict:
    """Load (or build) a ticker-uppercase -> CIK-int mapping cached on disk."""
    TICKER_CACHE.parent.mkdir(parents=True, exist_ok=True)
    if TICKER_CACHE.exists() and not refresh:
        with open(TICKER_CACHE) as f:
            return {k: int(v) for k, v in json.load(f).items()}

    print(f"  Fetching SEC ticker map from {SEC_TICKER_URL}")
    resp = throttled_get(SEC_TICKER_URL)
    resp.raise_for_status()
    raw = resp.json()
    # company_tickers.json is keyed by integer indexes -> {cik_str, ticker, title}
    mapping = {}
    for entry in raw.values():
        ticker = str(entry.get("ticker", "")).upper().strip()
        cik = entry.get("cik_str")
        if ticker and cik is not None:
            mapping[ticker] = int(cik)

    with open(TICKER_CACHE, "w") as f:
        json.dump(mapping, f)
    print(f"  Cached {len(mapping)} ticker -> CIK entries to {TICKER_CACHE}")
    return mapping


# =========================
# EDGAR submissions
# =========================

SEC_SUBMISSIONS_FILE_URL = "https://data.sec.gov/submissions/{name}"


def _extract_def14a(block: dict) -> list[dict]:
    forms = block.get("form", [])
    dates = block.get("filingDate", [])
    accs = block.get("accessionNumber", [])
    docs = block.get("primaryDocument", [])
    out = []
    for form, date, acc, doc in zip(forms, dates, accs, docs):
        if form == "DEF 14A":
            out.append({
                "filing_date": date,
                "accession_number": acc,
                "primary_document": doc,
            })
    return out


def fetch_def14a_filings(cik: int, target_year: Optional[int] = None) -> list[dict]:
    """Return DEF 14A filings for a CIK.

    For high-volume filers (banks, brokers), older filings roll into paginated
    overflow files listed under filings.files. If target_year is given, walk
    only the overflow files whose date range covers that year; otherwise walk
    all overflow files.
    """
    url = SEC_SUBMISSIONS_URL.format(cik10=str(cik).zfill(10))
    resp = throttled_get(url)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    data = resp.json()

    out = _extract_def14a(data.get("filings", {}).get("recent", {}))

    overflow_files = data.get("filings", {}).get("files", []) or []
    for entry in overflow_files:
        if target_year is not None:
            from_year = int(entry.get("filingFrom", "0000")[:4] or 0)
            to_year = int(entry.get("filingTo", "9999")[:4] or 9999)
            if not (from_year <= target_year <= to_year):
                continue
        name = entry.get("name")
        if not name:
            continue
        sub_resp = throttled_get(SEC_SUBMISSIONS_FILE_URL.format(name=name))
        if sub_resp.status_code != 200:
            continue
        out.extend(_extract_def14a(sub_resp.json()))

    return out


def filing_url(cik: int, accession_number: str, primary_document: str) -> str:
    return SEC_ARCHIVE_URL.format(
        cik_int=cik,
        accession_nodash=accession_number.replace("-", ""),
        doc=primary_document,
    )


# =========================
# Target selection
# =========================

def load_input_pairs(input_csv: Path) -> list[tuple[str, int]]:
    """Read input CSV, return distinct (ticker, year) tuples (skipping rows missing either)."""
    pairs = set()
    with open(input_csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = (row.get("ticker") or "").strip().upper()
            year_raw = (row.get("year") or "").strip()
            if not ticker or not year_raw:
                continue
            try:
                year = int(float(year_raw))
            except ValueError:
                continue
            pairs.add((ticker, year))
    return sorted(pairs)


def select_targets(
    pairs: list[tuple[str, int]],
    mode: str,
    start_year: int,
    end_year: int,
) -> list[tuple[str, int]]:
    if mode == "prototype":
        latest = max(y for _, y in pairs if y <= end_year)
        return [(t, latest) for t in PROTOTYPE_TICKERS]

    if mode == "sp100":
        latest = max(y for _, y in pairs if y <= end_year)
        # Top 100 tickers by frequency in input (proxies S&P 100-ish coverage).
        from collections import Counter
        freq = Counter(t for t, _ in pairs)
        top = [t for t, _ in freq.most_common(100)]
        return [(t, latest) for t in top]

    # full
    return [(t, y) for t, y in pairs if start_year <= y <= end_year]


# =========================
# Manifest
# =========================

MANIFEST_FIELDS = [
    "ticker", "cik", "year", "filing_date", "accession_number",
    "primary_document", "filing_url", "local_path", "status", "note",
]


def load_manifest(path: Path) -> dict:
    """Load manifest into dict keyed by (ticker, year) -> row, for resume support."""
    if not path.exists():
        return {}
    out = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["ticker"], int(row["year"]))
            out[key] = row
    return out


def write_manifest(path: Path, rows: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in MANIFEST_FIELDS})


# =========================
# Per-target scrape
# =========================

def pick_filing_for_year(filings: list[dict], year: int) -> Optional[dict]:
    """Return the DEF 14A filed in the given calendar year (latest if multiple)."""
    same_year = [f for f in filings if f["filing_date"].startswith(str(year))]
    if not same_year:
        return None
    return sorted(same_year, key=lambda f: f["filing_date"])[-1]


def scrape_one(
    ticker: str,
    year: int,
    cik: int,
    filing_dir: Path,
) -> dict:
    """Download one DEF 14A. Returns a manifest row."""
    row = {
        "ticker": ticker, "cik": cik, "year": year,
        "filing_date": "", "accession_number": "", "primary_document": "",
        "filing_url": "", "local_path": "", "status": "", "note": "",
    }
    try:
        filings = fetch_def14a_filings(cik, target_year=year)
    except requests.HTTPError as e:
        row["status"] = "error"
        row["note"] = f"submissions HTTP {e.response.status_code if e.response else '?'}"
        return row
    if not filings:
        row["status"] = "no_def14a_in_recent"
        return row

    chosen = pick_filing_for_year(filings, year)
    if chosen is None:
        row["status"] = "no_def14a_in_year"
        return row

    row.update({
        "filing_date": chosen["filing_date"],
        "accession_number": chosen["accession_number"],
        "primary_document": chosen["primary_document"],
        "filing_url": filing_url(cik, chosen["accession_number"], chosen["primary_document"]),
    })

    # Cache by CIK + year (one filing per company-year).
    out_path = filing_dir / f"{cik}_{year}.html"
    if out_path.exists() and out_path.stat().st_size > 0:
        row["local_path"] = str(out_path.relative_to(PROJECT_ROOT))
        row["status"] = "cached"
        return row

    try:
        resp = throttled_get(row["filing_url"])
        resp.raise_for_status()
    except requests.HTTPError as e:
        row["status"] = "error"
        row["note"] = f"document HTTP {e.response.status_code if e.response else '?'}"
        return row

    out_path.write_bytes(resp.content)
    row["local_path"] = str(out_path.relative_to(PROJECT_ROOT))
    row["status"] = "downloaded"
    return row


# =========================
# Main
# =========================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download DEF 14A proxy statements from SEC EDGAR.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 src/data_extraction/scrape_def14a.py --mode prototype
  python3 src/data_extraction/scrape_def14a.py --mode sp100
  python3 src/data_extraction/scrape_def14a.py --mode full --start-year 2020
        """,
    )
    parser.add_argument("--mode", choices=["prototype", "sp100", "full"],
                        default="prototype",
                        help="Scope of scrape (default: prototype)")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT),
                        help=f"Input CSV with ticker+year columns (default: {DEFAULT_INPUT})")
    parser.add_argument("--filing-dir", type=str, default=str(DEFAULT_FILING_DIR),
                        help=f"Directory for cached filings (default: {DEFAULT_FILING_DIR})")
    parser.add_argument("--manifest", type=str, default=str(DEFAULT_MANIFEST),
                        help=f"Manifest CSV path (default: {DEFAULT_MANIFEST})")
    parser.add_argument("--start-year", type=int, default=2020,
                        help="First year to include in --mode full (default: 2020)")
    parser.add_argument("--end-year", type=int, default=2025,
                        help="Last year to include (default: 2025)")
    parser.add_argument("--refresh-tickers", action="store_true",
                        help="Re-download SEC ticker -> CIK mapping")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print target list and exit")
    args = parser.parse_args()

    input_path = Path(args.input)
    filing_dir = Path(args.filing_dir)
    manifest_path = Path(args.manifest)
    filing_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"Input not found: {input_path}", file=sys.stderr)
        return 1

    print("=" * 60)
    print("DEF 14A Scraper")
    print("=" * 60)
    print(f"  Mode:       {args.mode}")
    print(f"  Input:      {input_path}")
    print(f"  Filings:    {filing_dir}")
    print(f"  Manifest:   {manifest_path}")

    print("\nLoading input pairs...")
    pairs = load_input_pairs(input_path)
    print(f"  {len(pairs)} distinct (ticker, year) pairs in input")

    targets = select_targets(pairs, args.mode, args.start_year, args.end_year)
    print(f"  {len(targets)} targets selected for mode={args.mode}")

    if args.dry_run:
        for t, y in targets[:50]:
            print(f"    {t}  {y}")
        if len(targets) > 50:
            print(f"    ... ({len(targets) - 50} more)")
        return 0

    print("\nLoading ticker -> CIK map...")
    ticker_to_cik = load_ticker_to_cik(refresh=args.refresh_tickers)

    print("\nLoading existing manifest (for resume)...")
    manifest = load_manifest(manifest_path)
    print(f"  {len(manifest)} prior manifest rows")

    n_new = n_cached = n_skipped = n_error = 0
    for i, (ticker, year) in enumerate(targets, 1):
        key = (ticker, year)
        existing = manifest.get(key)
        if existing and existing["status"] in {"cached", "downloaded"}:
            n_cached += 1
            continue

        cik = ticker_to_cik.get(ticker)
        if cik is None:
            manifest[key] = {
                "ticker": ticker, "cik": "", "year": year,
                "filing_date": "", "accession_number": "", "primary_document": "",
                "filing_url": "", "local_path": "", "status": "no_cik", "note": "",
            }
            n_skipped += 1
            continue

        row = scrape_one(ticker, year, cik, filing_dir)
        manifest[key] = row
        if row["status"] == "downloaded":
            n_new += 1
        elif row["status"] == "cached":
            n_cached += 1
        elif row["status"].startswith("no_"):
            n_skipped += 1
        else:
            n_error += 1

        if i % 25 == 0 or i == len(targets):
            write_manifest(manifest_path, manifest.values())
            print(f"  [{i}/{len(targets)}] new={n_new} cached={n_cached} "
                  f"skipped={n_skipped} error={n_error}")

    write_manifest(manifest_path, manifest.values())

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Targets:          {len(targets)}")
    print(f"  Newly downloaded: {n_new}")
    print(f"  Already cached:   {n_cached}")
    print(f"  Skipped (no CIK / no filing): {n_skipped}")
    print(f"  Errors:           {n_error}")
    print(f"  Manifest:         {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

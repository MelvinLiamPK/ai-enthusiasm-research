"""
DEF 14A Director-Bio Extractor (Claude Haiku 4.5)
==================================================

Reads cached DEF 14A HTML filings (produced by scrape_def14a.py) and uses
Claude Haiku 4.5 to extract structured director / officer biographies:

    {full_name, primary_company, primary_role, is_current}

The point: recover each director's *primary* employer (the company they
list on LinkedIn — usually NOT the company whose board they sit on), so we
can re-anchor LinkedIn URL discovery for the ~27k unmatched directors.

Pipeline per filing:
    1. Read HTML, strip to plain text via BeautifulSoup.
    2. Narrow to the director-bio section heuristically (large recall win,
       cuts cost ~3x). If narrowing fails, fall back to the full document.
    3. Send to Haiku 4.5 with a structured JSON schema (output_config.format).
       System prompt is cached (stable across filings).
    4. Append rows to data/processed/def14a_extracted_bios.csv.

Modes:
    --mode prototype   Process whatever's in the manifest (default 10 filings).
                       Best paired with `scrape_def14a.py --mode prototype`.
    --mode sp100       Process all filings whose ticker is in the top-100
                       most-frequent tickers in the input.
    --mode full        Process every successfully-downloaded filing in the
                       manifest.

Cost estimate (Haiku 4.5: $1/MTok input, $5/MTok output, 200K context):
    ~10 KB text / filing after narrowing -> ~3K input tokens
    + ~500 output tokens
    => ~$0.005 / filing
    => ~$50 for 10k filings full director universe (within Nick's <$100 cap).

Requires:
    pip install anthropic beautifulsoup4
    ANTHROPIC_API_KEY in environment or .env

Usage:
    python3 src/data_extraction/parse_def14a_bios.py --mode prototype
    python3 src/data_extraction/parse_def14a_bios.py --mode sp100
    python3 src/data_extraction/parse_def14a_bios.py --mode full
"""

import argparse
import csv
import os
import re
import sys
import threading
from pathlib import Path
from typing import Iterable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependency: pip install beautifulsoup4", file=sys.stderr)
    sys.exit(1)

try:
    import anthropic
    from pydantic import BaseModel, Field
except ImportError:
    print("Missing dependencies: pip install anthropic pydantic", file=sys.stderr)
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except ImportError:
    pass  # dotenv optional; ANTHROPIC_API_KEY can come from shell


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

DEFAULT_MANIFEST = PROJECT_ROOT / "data" / "raw" / "def14a" / "manifest.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "processed" / "def14a_extracted_bios.csv"

MODEL = "claude-haiku-4-5"

# Heuristic anchors for finding the director-bio section in a DEF 14A.
# Order matters: most specific first.
SECTION_ANCHORS = [
    re.compile(r"\b(directors?\s+and\s+executive\s+officers?)\b", re.IGNORECASE),
    re.compile(r"\b(directors?\s+and\s+nominees?)\b", re.IGNORECASE),
    re.compile(r"\b(nominees?\s+for\s+director)\b", re.IGNORECASE),
    re.compile(r"\b(information\s+about\s+(?:the|our)\s+nominees?)\b", re.IGNORECASE),
    re.compile(r"\b(election\s+of\s+directors?)\b", re.IGNORECASE),
    re.compile(r"\b(our\s+board\s+of\s+directors?)\b", re.IGNORECASE),
    re.compile(r"\b(board\s+of\s+directors?)\b", re.IGNORECASE),
]

# Where the bio section typically ends.
SECTION_TERMINATORS = [
    re.compile(r"\b(executive\s+compensation)\b", re.IGNORECASE),
    re.compile(r"\b(compensation\s+discussion\s+and\s+analysis)\b", re.IGNORECASE),
    re.compile(r"\b(audit\s+committee\s+report)\b", re.IGNORECASE),
    re.compile(r"\b(security\s+ownership)\b", re.IGNORECASE),
    re.compile(r"\b(certain\s+relationships\s+and\s+related)\b", re.IGNORECASE),
    re.compile(r"\b(director\s+compensation)\b", re.IGNORECASE),
]

# Cap text we send per filing. Bio sections rarely exceed ~30k chars; this
# prevents pathological filings from blowing up costs.
MAX_TEXT_CHARS = 60_000


# =========================
# Output schema
# =========================

class Bio(BaseModel):
    full_name: str = Field(description="Person's full name as it appears in the filing")
    primary_company: Optional[str] = Field(
        description=(
            "The person's CURRENT primary employer (not the company filing the proxy). "
            "If they have retired with no current role, set to null."
        )
    )
    primary_role: Optional[str] = Field(
        description="Their current job title at primary_company, e.g. 'CEO', 'Founder and Managing Partner'."
    )
    is_current: bool = Field(
        description=(
            "True if the role is current (e.g. 'is', 'serves as', 'currently'). "
            "False if the bio describes a past role (e.g. 'formerly', 'previously', 'retired')."
        )
    )
    role_context: str = Field(
        description=(
            "One of: 'director' if the bio is a board director of the filing company, "
            "'officer' if a named executive officer of the filing company, or 'unknown'."
        )
    )


class BioList(BaseModel):
    """A list of bios extracted from a DEF 14A filing."""
    bios: list[Bio]


# =========================
# HTML -> text
# =========================

def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    # Collapse runs of whitespace within lines, drop blank lines.
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines)


# Markers that occur densely in actual director / officer bios and rarely
# elsewhere. We use these to score candidate sections rather than relying on
# section title alone (titles also appear in tables of contents, governance
# prose, and shareholder proposals — all of which are NOT what we want).
BIO_SIGNAL = re.compile(
    r"\bdirector\s+since\s+\d{4}\b|"
    r"\bsince\s+\d{4}\b|"
    r"\bage[:\s]+\d{2}\b|"
    r"\bcommittee\s+(?:chair|member)s?\b|"
    r"\bpreviously\s+(?:served|was|held)\b|"
    r"\bcurrently\s+(?:serves?|is)\b|"
    r"\bjoined\s+(?:our|the)\s+board\b|"
    r"\b(?:Mr|Ms|Mrs|Dr)\.\s+[A-Z]",
    re.IGNORECASE,
)

SCORING_WINDOW = 20_000  # chars after the anchor to count bio signals in


def _next_terminator(text: str, start: int) -> int:
    """Earliest terminator position after start+500, or len(text)."""
    end = len(text)
    for term in SECTION_TERMINATORS:
        m = term.search(text, start + 500)
        if m and m.start() < end:
            end = m.start()
    return end


def narrow_to_bio_section(text: str) -> str:
    """Slice text to the densest bio section in the document.

    For every section anchor match, count bio signals ('Director Since',
    'Age', honorifics, committee memberships, etc.) in the next
    SCORING_WINDOW chars and pick the highest-scoring anchor. Real bio
    sections concentrate these signals; TOCs, governance prose, and
    shareholder proposals do not.

    Falls back to the full document (capped) if no anchor scores above a
    minimum threshold.
    """
    candidates: list[tuple[int, int, int]] = []  # (score, start, end)
    for anchor in SECTION_ANCHORS:
        for m in anchor.finditer(text):
            start = m.start()
            window = text[start : start + SCORING_WINDOW]
            score = len(BIO_SIGNAL.findall(window))
            end = _next_terminator(text, start)
            candidates.append((score, start, end))

    if not candidates:
        return text[:MAX_TEXT_CHARS]

    score, start, end = max(candidates, key=lambda c: c[0])
    if score < 5:
        # No anchor sits near a dense bio cluster — fall back to whole doc.
        return text[:MAX_TEXT_CHARS]
    section = text[start:end]
    # If the chosen terminator landed inside an early governance/related-party
    # paragraph (sometimes 'Security Ownership' appears mid-stream in modern
    # proxies, well before the real bio roster), extend past it.
    if len(section) < 8000:
        section = text[start : start + MAX_TEXT_CHARS]
    return section[:MAX_TEXT_CHARS]


# =========================
# Manifest + output bookkeeping
# =========================

def load_manifest(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def select_targets(rows: list[dict], mode: str) -> list[dict]:
    downloaded = [r for r in rows if r.get("status") in {"downloaded", "cached"}]
    if mode == "prototype":
        return downloaded
    if mode == "sp100":
        from collections import Counter
        freq = Counter(r["ticker"] for r in downloaded)
        top = {t for t, _ in freq.most_common(100)}
        return [r for r in downloaded if r["ticker"] in top]
    if mode == "full":
        return downloaded
    raise ValueError(f"Unknown mode: {mode}")


OUTPUT_FIELDS = [
    "ticker", "cik", "year", "filing_date", "filing_url",
    "full_name", "primary_company", "primary_role",
    "is_current", "role_context",
]


def already_parsed_keys(output_path: Path) -> set[tuple[str, int]]:
    """(cik, year) tuples that already appear in the output CSV — used for resume."""
    if not output_path.exists():
        return set()
    out = set()
    with open(output_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                out.add((row["cik"], int(row["year"])))
            except (KeyError, ValueError):
                continue
    return out


def append_rows(output_path: Path, rows: Iterable[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not output_path.exists()
    with open(output_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        if is_new:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in OUTPUT_FIELDS})


# =========================
# LLM call
# =========================

SYSTEM_PROMPT = """You extract director and named executive officer biographies from SEC DEF 14A proxy statements.

Your task: for every individual whose biography appears in the provided text, return one record with:
- full_name: their full name (preserve middle names/initials as written)
- primary_company: their CURRENT primary employer. This is typically NOT the company filing the proxy; it is whatever company they actually work at (e.g. CEO of Apple, Managing Partner at KKR, Senior Advisor at McKinsey). Set to null if the person is retired with no current role.
- primary_role: their current job title at primary_company.
- is_current: true if the role is described as current ("is", "serves as", "currently"); false if past ("formerly", "previously", "retired from", "served as until 2020").
- role_context: "director" if the bio is for a board director of the filing company, "officer" if a named executive officer of the filing company, "unknown" otherwise.

Rules:
- Skip biographical entries for non-individuals (e.g. ownership tables for institutions like BlackRock).
- If a person has held many roles, prefer the CURRENT primary one. If they have retired, list their most recent past role with is_current=false.
- Return one record per unique person, even if they appear in multiple board sections.
- Do not invent information. If a field is genuinely unstated, set it to null (or false for is_current)."""


def parse_filing(client: anthropic.Anthropic, text: str):
    """Call Haiku with structured outputs. Returns (BioList, response)."""
    response = client.messages.parse(
        model=MODEL,
        max_tokens=8000,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{
            "role": "user",
            "content": (
                "Extract every director and named executive officer biography from this DEF 14A excerpt. "
                "Return a JSON object with a 'bios' array.\n\n"
                "=== FILING TEXT ===\n" + text
            ),
        }],
        output_format=BioList,
    )
    return response.parsed_output, response


# =========================
# Parallel Processing
# =========================

_output_lock = threading.Lock()


def process_filing_worker(row: dict, client: anthropic.Anthropic, args, output_path: Path) -> tuple:
    """Worker function for parallel processing. Returns (ticker, cik, year, ok, status_msg)."""
    ticker = row["ticker"]
    cik = row["cik"]
    year = int(row["year"])
    local_path = PROJECT_ROOT / row["local_path"]

    try:
        if not local_path.exists():
            return (ticker, cik, year, False, f"missing file")

        html = local_path.read_text(errors="replace")
        text = html_to_text(html)
        if not args.no_narrow:
            text = narrow_to_bio_section(text)
        text = text[:MAX_TEXT_CHARS]

        if args.dry_run:
            return (ticker, cik, year, True, f"{len(text):,} chars")

        result, response = parse_filing(client, text)
        bios = result.bios if result else []

        out_rows = [{
            "ticker": ticker, "cik": cik, "year": year,
            "filing_date": row.get("filing_date", ""),
            "filing_url": row.get("filing_url", ""),
            "full_name": b.full_name,
            "primary_company": b.primary_company or "",
            "primary_role": b.primary_role or "",
            "is_current": b.is_current,
            "role_context": b.role_context,
        } for b in bios]

        # Thread-safe append to CSV
        with _output_lock:
            append_rows(output_path, out_rows)

        return (ticker, cik, year, True, f"{len(bios)} bios" if bios else "0 bios")

    except anthropic.APIStatusError as e:
        return (ticker, cik, year, False, f"API {e.status_code}")
    except Exception as e:
        return (ticker, cik, year, False, f"{type(e).__name__}")


# =========================
# Main
# =========================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract director/officer bios from cached DEF 14A filings via Claude Haiku 4.5.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 src/data_extraction/parse_def14a_bios.py --mode prototype
  python3 src/data_extraction/parse_def14a_bios.py --mode sp100
  python3 src/data_extraction/parse_def14a_bios.py --mode full
        """,
    )
    parser.add_argument("--mode", choices=["prototype", "sp100", "full"],
                        default="prototype",
                        help="Scope of parse (default: prototype)")
    parser.add_argument("--manifest", type=str, default=str(DEFAULT_MANIFEST),
                        help=f"Manifest CSV from scrape_def14a.py (default: {DEFAULT_MANIFEST})")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT),
                        help=f"Output CSV (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--no-narrow", action="store_true",
                        help="Skip section narrowing; send full document text (more expensive)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after this many filings (for cost-bounded testing)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run the pipeline but skip the API call; print extracted text length")
    parser.add_argument("--workers", type=int, default=10,
                        help="Number of parallel workers for parsing (default 10)")
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    output_path = Path(args.output)

    if not manifest_path.exists():
        print(f"Manifest not found: {manifest_path}", file=sys.stderr)
        print("Run scrape_def14a.py first.", file=sys.stderr)
        return 1

    if not args.dry_run and not os.environ.get("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set. Add it to .env or export it.", file=sys.stderr)
        return 1

    print("=" * 60)
    print("DEF 14A Bio Parser (Haiku 4.5)")
    print("=" * 60)
    print(f"  Mode:     {args.mode}")
    print(f"  Manifest: {manifest_path}")
    print(f"  Output:   {output_path}")

    rows = load_manifest(manifest_path)
    targets = select_targets(rows, args.mode)
    print(f"  Filings:  {len(targets)} (downloaded/cached in manifest)")

    seen = already_parsed_keys(output_path)
    targets = [r for r in targets if (r["cik"], int(r["year"])) not in seen]
    print(f"  To do:    {len(targets)} (after skipping already-parsed)")

    if args.limit:
        targets = targets[: args.limit]
        print(f"  Limited:  {len(targets)} (--limit {args.limit})")

    client = None if args.dry_run else anthropic.Anthropic()

    n_ok = n_empty = n_error = 0

    # Parallel processing with ThreadPoolExecutor
    num_workers = 1 if args.dry_run else min(args.workers, len(targets))
    print(f"  Workers:  {num_workers}")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(process_filing_worker, row, client, args, output_path): (i, len(targets))
            for i, row in enumerate(targets, 1)
        }

        # Process results as they complete
        for i, future in enumerate(as_completed(futures), 1):
            try:
                ticker, cik, year, ok, msg = future.result()
                if ok:
                    n_ok += 1
                    print(f"  [{i}/{len(targets)}] {ticker} {year}: {msg}")
                else:
                    n_error += 1
                    print(f"  [{i}/{len(targets)}] {ticker} {year}: ERROR {msg}")
            except Exception as e:
                n_error += 1
                print(f"  [{i}/{len(targets)}] ERROR: {type(e).__name__}: {e}")

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Filings processed: {n_ok}  (errors: {n_error})")
    print(f"  Output: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

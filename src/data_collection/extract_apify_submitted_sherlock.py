#!/usr/bin/env python3
"""
Task-0 helper (runs on SHERLOCK): reconstruct the authoritative Apify submission
history for the WRDS post-scrape from the real run logs, so Chat 6 can compute the
hard 3-way backlog split (has_posts / submitted-empty / never-submitted).

What it reads (Sherlock-only files):
  - batch1 input    : data/.../all_people_linkedin_urls/batch_*_urls.csv   (98 chunks)
  - batch2 input    : data/.../all_people_linkedin_urls/remaining_urls.csv
  - batch3 input    : data/.../all_people_linkedin_urls/remaining_urls_final.csv
  - batch1 empties  : scraped_posts/no_posts_profiles_*.csv
  - batch2 empties  : scraped_posts_batch2/no_posts_profiles_*.csv
  - batch3 results  : scraped_posts_batch3/temp_results.jsonl  (streamed; profile_input field)

Emits small, normalized URL sets to outputs/coverage_audit_<ts>/ for scp back to local:
  - wrds_submitted_urls.txt   : norm_url of every URL ever fed to Apify (batch1/2/3 inputs)
  - wrds_no_posts_urls.txt    : norm_url of explicit submitted-and-empty (batch1/2 no_posts files)
  - batch3_has_posts_urls.txt : norm_url of profile_inputs that produced >=1 post in batch3
  - summary.json              : counts for sanity
URL normalization matches the project's join discipline (strip scheme/www/query/
fragment, /posts/->/in/, trailing slash, lowercase) — applied to every side of every join.
"""
from __future__ import annotations
import glob
import json
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
BASE = ROOT / "data/processed/all_people_linkedin_urls"

_SCHEME = re.compile(r"^https?://", re.I)
_WWW = re.compile(r"^www\.", re.I)


def norm_url(u) -> str | None:
    if u is None:
        return None
    s = str(u).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    s = _SCHEME.sub("", s)
    s = _WWW.sub("", s)
    s = s.split("?")[0].split("#")[0]
    if "/posts/" in s:
        parts = s.split("/posts/")
        if len(parts) > 1 and parts[1]:
            dom = parts[0].split("/")[0]
            username = parts[1].split("/")[0].split("-")[0]
            s = f"{dom}/in/{username}"
    s = s.rstrip("/").lower()
    return s or None


def col_urls(path: Path, col: str) -> set[str]:
    """Read one column of normalized URLs from a CSV with the std csv module (robust)."""
    import csv
    out: set[str] = set()
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        r = csv.DictReader(fh)
        if col not in (r.fieldnames or []):
            return out
        for row in r:
            n = norm_url(row.get(col))
            if n:
                out.add(n)
    return out


def main() -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = ROOT / "outputs" / f"coverage_audit_{ts}"
    outdir.mkdir(parents=True, exist_ok=True)

    # ---- submitted = union of all input lists fed to Apify ----
    submitted: set[str] = set()
    n_batch1_files = 0
    for f in sorted(glob.glob(str(BASE / "batch_*_urls.csv"))):
        submitted |= col_urls(Path(f), "linkedin_url")
        n_batch1_files += 1
    submitted |= col_urls(BASE / "remaining_urls.csv", "linkedin_url")
    submitted |= col_urls(BASE / "remaining_urls_final.csv", "linkedin_url")

    # ---- explicit submitted-and-empty (no_posts_profiles outputs) ----
    no_posts: set[str] = set()
    for f in glob.glob(str(BASE / "scraped_posts" / "no_posts_profiles_*.csv")):
        no_posts |= col_urls(Path(f), "profile_url")
    for f in glob.glob(str(BASE / "scraped_posts_batch2" / "no_posts_profiles_*.csv")):
        no_posts |= col_urls(Path(f), "profile_url")

    # ---- batch3: stream temp_results.jsonl, profile_input of every post record ----
    batch3_has_posts: set[str] = set()
    b3 = BASE / "scraped_posts_batch3" / "temp_results.jsonl"
    n_lines = 0
    if b3.exists():
        with open(b3, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                n_lines += 1
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                pi = d.get("profile_input") or d.get("url")
                n = norm_url(pi)
                if n:
                    batch3_has_posts.add(n)

    # batch3 inputs that never produced a post = batch3 submitted-empty (cross-check)
    batch3_input = col_urls(BASE / "remaining_urls_final.csv", "linkedin_url")
    batch3_empty = batch3_input - batch3_has_posts
    no_posts |= batch3_empty  # fold batch3 empties into the explicit-empty set

    # every URL we have empty/has-posts evidence for must be in submitted; widen submitted
    submitted |= no_posts | batch3_has_posts

    def dump(name: str, s: set[str]) -> None:
        with open(outdir / name, "w", encoding="utf-8") as fh:
            fh.write("\n".join(sorted(s)))

    dump("wrds_submitted_urls.txt", submitted)
    dump("wrds_no_posts_urls.txt", no_posts)
    dump("batch3_has_posts_urls.txt", batch3_has_posts)

    summary = {
        "timestamp": ts,
        "n_batch1_input_files": n_batch1_files,
        "n_submitted_urls": len(submitted),
        "n_explicit_no_posts_urls": len(no_posts),
        "n_batch3_lines_scanned": n_lines,
        "n_batch3_has_posts_urls": len(batch3_has_posts),
        "n_batch3_input_urls": len(batch3_input),
        "n_batch3_empty_urls": len(batch3_empty),
        "outdir": str(outdir),
    }
    with open(outdir / "summary.json", "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(json.dumps(summary, indent=2))
    print("OUTDIR=" + str(outdir))


if __name__ == "__main__":
    main()

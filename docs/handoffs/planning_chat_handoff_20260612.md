# Handoff → New planning chat (week of 2026-06-12)

**From:** prior planning chat (got long) · **To:** next planning chat
**Your job:** be the **planning hub** — write specs & handoffs, sequence chats, record
decisions, reconcile canonical releases. **Do NOT execute** (no scraping, builds, WRDS
pulls, commits, or flips — you draft instructions; the owner/exec chats run them).

## Read first (in order)
1. **`docs/PIPELINE_STATE.md`** — the living, file-backed state of what is actually done.
   **Read before assuming any stage needs doing.** Multiple chats (incl. this one, twice)
   mis-derived state from raw files; this doc + its source-of-truth file map is the fix.
2. `docs/handoffs/master_sequencing_20260602.md` — the wave/chat map (Chat 6 entry was
   reframed 2026-06-06; see below).
3. `CLAUDE.md` + `data/canonical/README.md`.

---

## Where things stand (2026-06-12)

**Wave 1 is COMPLETE** (all 5 deliverables built; canonical `current` NOT yet flipped):

| Wave-1 item | Status |
|---|---|
| Chat 1 · def14a merge | ✅ built + **merged to main** (`874f1d5`); release `2026-06-05_def14a` |
| Chat 2 · Python parity | ✅ reproduces John's `jvrmel_06` (3/4 exact); imputation rule locked |
| Chat 4 · RA dataset release | ✅ packaged (`outputs/stata_handoff_20260527_release_20260530.zip`) |
| Chat 5a · data aliases | ✅ `data/revelio/company_aliases.csv` (18,130 rows) |
| Monthly-CRSP pull | ✅ done + **committed** (`c4345bb`); release `2026-06-07_crsp_monthly` |

**Canonical `current` → `releases/2026-06-07_crsp_monthly` (FLIPPED 2026-06-12, commit `ec5d9f3`).**
The active release is now the complete 19-file snapshot inheriting `2026-06-05_def14a`
(→ `2026-05-27`) + the CRSP monthly panel + fixed-permno annual — so **def14a enrichment AND
CRSP monthly are live** for all analysis reading `current/`. Reversible (re-point the symlink).

---

## THE FULL WORKPLAN

### Owner queue (non-chat actions)
1. ✅ **DONE — flipped `current` → `2026-06-07_crsp_monthly`** (`ec5d9f3`). def14a + CRSP live.
2. ✅ **DONE — planning docs committed** (`2c9f498`). NOTE: `CLAUDE.md` is **gitignored** in
   this repo, so its edits (PIPELINE_STATE pointer + gotchas) are **local-only, not in git** —
   re-apply on any fresh clone; PIPELINE_STATE.md (committed) carries the same facts.
3. ◻ **Progress email — drafted, owner sends ~Monday.** Full text at
   **`docs/comms/progress_email_20260612.md`** (committed). To Nick/John/Daron, re the missed
   meeting. Owner will copy it into a mail client and send manually (~Mon); it is NOT auto-sent.
   If you revise project status before then, update that file so the sent version is current.

### Wave 2 — current focus (chats parallel; launch now)
- **Chat 6 · Apify BACKLOG scrape** — spec `chat6_apify_backlog_spec_20260606.md`. **REFRAMED:**
  Serper + Revelio crosscheck are already DONE for everyone, so this is **Apify-only**.
  Task 0 = build the person-level **coverage ledger** (certifies coverage + resolves the true
  backlog from Sherlock Apify submission logs; the "9,551 strong-match-without-posts" is an
  upper bound, true never-submitted count unknown). Task 1 = scrape never-submitted strong
  matches (+ ~200-URL recoverability probe). Task 2 = **LM-only** re-score → new release.
  **Runs on Sherlock. No dependency — can start immediately. Report Task 0 before any spend.**
- **Chat 3 · Full analysis v1** — spec `analysis_expansion_plan_20260530.md` §4/§5/§6
  **+ `chat3_length_intensity_addendum_20260606.md`** (John's post-length/intensity signal —
  our sentiment is a per-word *density*, so length is orthogonal; test it 3 ways).
  **Flip is now DONE (2026-06-12) → Chat 3 is unblocked, ready to launch.** Build the §4 panel
  WITHOUT tenure-gating (deferred to Wave 4).
- **Chat 5b/c · LLM aliases + apply** — **needs a spec** (currently split across
  `pre_meeting_plan §4` + meeting notes). 5b = LLM brand/subsidiary layer (Alphabet→YouTube/
  Waymo); 5c = apply union → recompute Revelio `strong_match` (lenient/non-date-gated) →
  quantify movers + Tobin's Q shift → new release. Depends on 5a (done).

### New tasks surfaced this week (slot into Wave 2 or 4)
- **Age + director-attributes → panel join (NEW gap).** `person_year.dta` has
  `def14a_director/tenure/gender/primary_company` but **NO age** and none of the
  `def14a_director_attributes.csv` fields (independent, committees, n_other_public_directorships,
  board_leadership_role). Age/attributes are **side-files, not on the panel.** Fix: join
  `def14a_director_status` age + the attributes onto `person_year` on **(profile_url/name,
  ticker, year)** — firm-keyed, so the ~2,421 execs-who-are-also-directors pick up age for free.
  Age is a **director-only** attribute (~95–96% of def14a directors; ~21% of the full universe;
  execs/blockholders get none unless separately pulled from Execucomp `AGE`). **Do this before
  Chat 3 if §6 wants age descriptives**, else fold into Wave 4's panel rebuild.
- **CODEBOOK erratum** — `2026-06-05_def14a/CODEBOOK.md` line 59 says board `age_coverage`
  mean ≈ 0.79; the **true value is 0.955** (0.79 was the stale pre-LLM-backfill figure; the
  data file is correct). Log in PIPELINE_STATE; optionally a one-line CODEBOOK fix.

### Wave 4 — final (after the backlog scrape lands + re-score)
- Build **tenure union (WRDS ∪ def14a)** keyed `(profile_url, ticker)` + **tenure-gating** of
  firm-year sentiment. def14a is the sole tenure source for the ~30% of directors not in WRDS.
- **Re-run Chats 2 + 3** on the post-expansion (v2) corpus → final numbers.
- **New-nominee instrument** (pre-board sentiment; `pre_board_year` flag already on the panel).
- Fold in the age/attributes join if not done earlier. **Needs a Wave-4 spec.**

### Open spec-writing tasks (for you, the new planning hub)
- Chat 5b/c spec · the age/attributes→panel join spec · the Wave-4 spec.

---

## Decisions locked — do NOT relitigate
1. Source of truth = `data/canonical/current/`; each chat writes a NEW dated release and
   does **not** auto-flip `current` (owner reviews + flips).
2. Zero-imputation is **regression-time only** (`ai_sent_new` = 0 where firm-year posted but
   no AI posts, NaN where it didn't post). Data keeps NaN. Identical in Chat 2 code + Chat 4 Stata/R.
3. **Strong-match (Tier-2 `strong_match_either`) = default sample**; min-post filters dropped;
   mean-of-mean is the headline measure.
4. **Chat 6 is Apify-only** (search + crosscheck done for everyone); **LM-only** re-score this
   round; **strong-match-only** scrape; runs on Sherlock.
5. **Aliases: lenient / non-date-gated** matching.
6. **Tenure work deferred to Wave 4** (needs the backlog directors to have posts first).
7. **Age is a director-only attribute**, currently a side-file (not on the panel) — needs a join.
8. **Sentiment is a per-word density** → post length is an orthogonal signal (test in Chat 3).
9. Never-elected placebo is empty; the surviving identification angle is **new-nominees'
   pre-board sentiment** (scrape-limited; grows with the backlog scrape). Exogeneity is firm-specific.

---

## Document map
| Doc | What | Status |
|---|---|---|
| `PIPELINE_STATE.md` | living file-backed state + source-of-truth file map | **READ FIRST** |
| `master_sequencing_20260602.md` | wave/chat map (Chat 6 reframed) | current |
| `chat6_apify_backlog_spec_20260606.md` | Chat 6 spec (ledger → scrape → re-score) | current |
| `chat3_length_intensity_addendum_20260606.md` | post-length signal for Chat 3 §5 | current |
| `analysis_expansion_plan_20260530.md` | Chat 2 §3 (done) · Chat 3 §4/§5/§6 | current |
| `monthly_crsp_pull_spec/done_20260607.md` | CRSP monthly (done) | reference |
| `ra_dataset_release_20260530_DONE.md` · `company_aliases_step_a_done_20260603.md` · `section3_python_parity_done_20260603.md` · `def14a_merge_release_RESULTS_to_planning_20260605.md` | Wave-1 results | reference |

## Key numbers (so you don't recompute)
Corpus (v2): **26,511** profiles · ~2.9M posts · 100% LM-scored. Strong-match URLs: **25,221**
(15,670 scraped / 9,551 without posts — backlog true size pending Chat 6 Task 0). def14a:
**96,955** director-rows, **20,724** distinct directors; age 95.5% of director-rows / ~21% of
the full universe. URL/validation source of truth: `revelio_validation_summary_v2.csv` (102,324).
WRDS universe: 96,968. Exec↔director name overlap: ~8%; blockholder↔director ~0%.

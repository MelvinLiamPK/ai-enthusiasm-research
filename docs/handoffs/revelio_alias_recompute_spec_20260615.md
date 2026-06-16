# Spec — Revelio alias recompute (5c): apply aliases → new strong_match labels

**Date:** 2026-06-15 · **From:** planning hub · **To:** execution (Redivis, username `ml2068`)
**Depends on:** 5a (DONE — `data/revelio/company_aliases.csv`, 18,130 rows). **Gates:** the final merge.

---

## 0. Why (the Pichai case)
Strong-match currently requires our firm name to match Revelio's recorded employer. **Sundar
Pichai is NOT a strong match today**: `revelio_url_match=True`, `revelio_name_confirmed=True`,
but every `revelio_company_confirmed_*` is `False` — his board company is `ALPHABET INC`/`GOOGL`
while Revelio records "Google." The alias list (5a) has `Alphabet ← Google`, `Meta ← Facebook`,
+ 2,052 renamed firms — but **it has never been applied to the crosscheck.** 5c applies it and
recomputes the labels. This rescues Pichai and every analogous case.

## 1. Where it runs — REDIVIS (not local)
The local `revelio_validation_summary_v2.csv` only stores the **boolean** company-confirm
results, not Revelio's **raw employer string** — so the match cannot be redone locally. Re-run
**`src/revelio/redivis_crosscheck_notebook_v2.ipynb`** on Redivis (where the raw Revelio company
field lives). (If the raw Revelio company string can be exported once per person, a local
recompute is possible — confirm first; otherwise Redivis is the route.)

## 2. The notebook changes (concrete — maps to `redivis_crosscheck_notebook_v2.ipynb`)

The company match lives in **Cell 6** (`company_in_positions` / `strip_legal` — matches ONE firm
name against Revelio's `company_cleaned` positions via legal-suffix-strip + bidirectional
substring + ≥50% token overlap) and is applied per-row in **Cell 10**. The alias file
(`company_aliases.csv`) is keyed on **`gvkey`** with `alias_name_clean`; e.g. gvkey **160329**
(Alphabet) contains both `ALPHABET INC` *and* `GOOGLE INC`. Pichai fails today because the match
only tests "alphabet" — never "google." Four edits:

**(a) New cell after Cell 6 — load aliases, build `gvkey → normalized-alias-set` (NON-date-gated).**
Upload `company_aliases.csv` to Redivis (as a project table, or attach as a notebook file). Then:
```python
# Cell 6b — alias index. Ignore valid_from/valid_to (lenient, per locked decision #5).
aliases_df = redivis.user("ml2068").dataset("company_aliases:<ver>") \
    .table("company_aliases").to_pandas_dataframe(variables=["gvkey","alias_name_clean"])
alias_by_gvkey = {}
for r in aliases_df.itertuples(index=False):
    g = norm_gvkey(r.gvkey)                 # reuse the notebook's norm_gvkey (defined in Cell 13)
    if g is None: continue
    a = strip_legal(r.alias_name_clean)     # reuse Cell 6 strip_legal → ascii/lower/no legal suffix
    if a and len(a) >= MIN_COMPANY_NAME_LEN:
        alias_by_gvkey.setdefault(g, set()).add(a)
print(len(alias_by_gvkey), "gvkeys;",
      "Alphabet(160329) =", sorted(alias_by_gvkey.get("160329", [])))  # expect {'alphabet','google'}
```
(If `norm_gvkey` is defined below in Cell 13, move it up or inline it here.)

**(b) New matcher in Cell 6 — test the firm's WHOLE alias set against Revelio positions.**
```python
def company_in_positions_aliased(user_id, alias_set):
    """True if ANY alias matches ANY Revelio position — same strip+substring+≥50%-token rule."""
    positions = positions_index.get(int(user_id), [])
    if not positions or not alias_set: return False
    pos_clean = [strip_legal(p) for p in positions if not pd.isna(p)]
    for our in alias_set:                                    # already strip_legal'd in Cell 6b
        toks = set(our.split()) - {""}
        if not toks: continue
        for pc in pos_clean:
            if not pc: continue
            if our in pc or pc in our: return True
            if len(toks & set(pc.split())) / len(toks) >= 0.5: return True
    return False

def company_in_positions_aliased_fuzzy(user_id, alias_set):
    if company_in_positions_aliased(user_id, alias_set): return True
    pos_clean = [strip_legal(p) for p in positions_index.get(int(user_id), []) if not pd.isna(p)]
    for our in alias_set:
        if len(our) < FUZZY_MIN_COMPANY_LEN: continue
        for pc in pos_clean:
            if pc and len(pc) >= FUZZY_MIN_COMPANY_LEN and \
               SequenceMatcher(None, our, pc).ratio() >= FUZZY_RATIO_THRESHOLD: return True
    return False
```

**(c) In Cell 10 — build each row's BOARD alias set and use the aliased matcher for the board leg.**
```python
g = norm_gvkey(getattr(row, "gvkey", None))
board_set = set(alias_by_gvkey.get(g, set()))
bc = strip_legal(board_co)
if bc and len(bc) >= MIN_COMPANY_NAME_LEN: board_set.add(bc)   # fallback if gvkey not in alias file
b_strict = company_in_positions_aliased(uid, board_set)
b_fuzzy  = company_in_positions_aliased_fuzzy(uid, board_set)
# primary stays the single-name check (free-text employer, often a DIFFERENT firm, no gvkey):
p_strict = company_in_positions(uid, primary_co, primary_co) if not pd.isna(primary_co) else False
p_fuzzy  = company_in_positions_fuzzy(uid, primary_co, primary_co) if not pd.isna(primary_co) else False
# everything downstream (_either, strong_match_*) is unchanged — board is now alias-aware,
# so strong_match_either = (name|verified) AND (board OR primary) rescues Pichai via the board leg.
```
**Why board-only aliasing:** the alias set is keyed on the row's `gvkey` = the **board** firm.
`primary_company` is free-text (often a different employer) with no reliable gvkey, so it keeps the
single-name check. That's enough — Pichai is rescued through the board leg. (Optional later: if a
`primary_company` resolves to its own gvkey, alias it too.)

**(d) Export as v3 (Cell 15).** Change the CSV name + output-table to `revelio_validation_summary_v3.csv`
(keep the same `output_cols` schema) so v2 is preserved for the movers diff.

## 2b. Movers + sanity (must pass before trusting v3)
- **Reproduction check (do this FIRST):** run once with aliasing OFF (`board_set = {strip_legal(board_co)}`
  only) — it must reproduce v2's `strong_match_either` **exactly**. Proves the alias layer is the
  *only* change. If it doesn't reproduce, STOP.
- **Pichai:** `strong_match_either == True` in v3 (gvkey 160329 / GOOGL).
- **No explosion:** total strong matches should rise **modestly** (genuine former-name/brand rescues),
  not balloon. If a generic alias ("group", "international") over-matches, raise `MIN_COMPANY_NAME_LEN`
  or drop short/generic aliases.
- **Movers file:** diff v3 vs v2 on `strong_match_either` (False→True) → `alias_movers_v3.csv`
  (URL, firm, matched alias). Report count + top gaining firms (expect Alphabet, Meta, renamed firms).

## 3. After the recompute — coverage is mostly already there
**Most movers already have posts.** Chat 6's Task-0 audit proved the original Apify run
(`batch1`) submitted **every found WRDS URL** (58,956/58,956), not just strong matches — so
alias-movers were already scraped; they were only excluded from the *sample*. So 5c is mostly a
**re-label → re-aggregate**, not a re-scrape. Only movers whose URL was found-but-returned-empty
need scraping — fold that small set into the existing backlog scrape logic (Apify, Sherlock,
strong-match-only). LM-score any new posts.

## 4. Feeds the final merge
The final unified release standardizes on the **alias-aware `strong_match_either` (Tier-2 — the
locked headline sample)**, which also resolves the latent v1-strict-vs-Tier-2 inconsistency (D3).
`company_sentiment` + the firm/person panels are then rebuilt on this label set.

## 5. Locked / out of scope
Lenient non-date-gated aliases (#5); `strong_match_either` = sample (#3); new release, no flip.
**5b (LLM brand aliases — YouTube/Waymo, brands never a legal name)** is an *optional fast-follow*
that further widens the alias set; not required for this pass. Tenure = Wave 4.

## 6. Deliverables back to planning
`revelio_validation_summary_v3.csv` + the movers file + the mover count/top-firms report +
the residual movers-without-posts list (for the small scrape), so the final merge can proceed.

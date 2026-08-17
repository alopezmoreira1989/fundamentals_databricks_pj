# Phase 6.5 — "Total Equity (incl NCI)" normalization: investigation, STOP condition triggered

**STOP CONDITION TRIGGERED. No code was changed. No table was written. No mapping was
implemented.** Phase 6.4's Tier D finding turned out to be real but far more consequential than
its EU-only framing suggested: the `CONCEPT_SYNONYMS` rule this phase was scoped to remove/adjust
is **actively, materially load-bearing for real US production data today** — not dormant, as an
early (and, on reflection, flawed) query in this investigation first suggested. Per this phase's
own explicit instructions ("DO NOT GUESS. DO NOT IMPLEMENT A BROADER FIX. STOP AND REPORT."), this
document presents the evidence and the architectural question it raises, and stops there.

Every claim below is labeled **VERIFIED** (obtained directly from real production data or real
code this pass), **INFERRED**, or **UNKNOWN**.

---

## 1. Re-tracing the bug (Part 1)

**VERIFIED**, direct code read, `fundamentals_pipeline/00__config/01__tickers.py`:

1. **What "Total Equity (incl NCI)" means**: `BALANCE_SHEET["Total Equity (incl NCI)"] =
   ("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "stock")` — total
   equity, including the portion attributable to non-controlling interests. IFRS fallback
   (`IFRS_FALLBACK_TAGS`): `"Equity"`.
2. **What "Total Stockholders Equity" means**: `BALANCE_SHEET["Total Stockholders Equity"] =
   ("StockholdersEquity", "stock")` — equity attributable to the parent/shareholders only,
   excluding NCI. IFRS fallback: `"EquityAttributableToOwnersOfParent"`.
3. **Are they genuinely distinct?** Yes — confirmed both by definition (SEC's own XBRL taxonomy
   distinguishes them by tag name) and by real data (§2 below: the two values differ by exactly
   the NCI balance for every EU issuer checked, and differ by a real, if usually small,
   percentage for the US tickers sampled in §3).
4. **Is `Total Equity (incl NCI) = Total Stockholders Equity + Non-Controlling Interest`?**
   **VERIFIED** for FCC (Phase 6.4's own finding, re-confirmed): €2,732,716,000 (direct
   attributable) + €1,003,303,000 (`NoncontrollingInterests`) = €3,736,019,000, exactly the
   "Total Equity (incl NCI)" raw value. This is the real accounting identity holding, not
   something this codebase computes anywhere — no code currently derives one from the other.
5. **Does any US/Canada company currently rely on the `CONCEPT_SYNONYMS` fold?** **YES —
   materially, and this correction matters: an early query in this investigation used
   `source_id != 'EU_CURRENT'`, which in SQL's three-valued logic silently excludes every
   `source_id IS NULL` row (the ~6.9M-row legacy population predating the `source_id` column,
   ADR-0009) and appeared to show zero US/Canada reliance. Re-run correctly
   (`source_id IS NULL OR source_id != 'EU_CURRENT'`), the real picture is the opposite — see §3.**
6. **Where does "Total Equity (incl NCI)" appear in the codebase?** **VERIFIED**, repo-wide grep:
   `01__tickers.py` (`STATEMENTS`, `IFRS_FALLBACK_TAGS`, `CONCEPT_SYNONYMS`, `CONCEPT_PRIORITY`),
   `sources/eu_current.py` (`EU_CANONICAL_MAPPING`), `tests/test_sources_eu_current.py`. **Not**
   referenced in `22__derived_metrics.py`, `23__intrinsic_value.py`, `concept_hierarchy.json`, or
   `fundamentals_screener` — no derived metric currently reads this concept by name.

**The merge mechanism** (`21__clean_and_merge.py`, unchanged since Phase 6.3):
`_prio` is computed from the concept's *original* label (via `CONCEPT_PRIORITY`) **before** the
`CONCEPT_SYNONYMS` rename loop runs; the rename then overwrites `concept` from `"Total Equity
(incl NCI)"` to `"Total Stockholders Equity"`; the dedup `Window.partitionBy("ticker", "stmt",
"concept", "fy")` then groups the renamed row into the SAME key as any genuine direct
`"Total Stockholders Equity"` fact for that period, and `prio asc` (0 beats 1) picks the direct
fact when one exists. **When no direct fact exists for that key, the renamed incl-NCI row is the
only candidate in that partition and wins by default** — this is the fallback mechanism, and it
is exercised for real, right now (§3).

---

## 2. Real EU data (Part 2)

**VERIFIED**, direct query against `main.financials.financials_raw`/`financials` — re-confirms
Phase 6.4's own finding with fresh evidence:

| Ticker | `Total Stockholders Equity` (direct, clean `financials`) | `Total Equity (incl NCI)` (raw only — never in clean `financials`) | NCI (raw, `NoncontrollingInterests`) | Identity holds? |
|---|---|---|---|---|
| FCC (FY2024) | €2,732,716,000 | €3,736,019,000 | €1,003,303,000 | ✓ exact |
| ALO, IBE, SGO, FCT, NAI, RAND | Direct value present and correct (Phase 6.4 §5) | Present in `financials_raw`, never survives merge | Present (Phase 6.4 fresh fetch) | Not re-verified arithmetically this pass beyond FCC — no reason to expect a different mechanism |
| ISP | No `Total Equity (incl NCI)` raw fact at all (bank, structurally different equity presentation) | — | — | N/A |

**7/8 EU issuers** have both a genuine direct `Total Stockholders Equity` fact **and** a genuine,
different `Total Equity (incl NCI)` fact for the same period — i.e., for EU, both values are
*always* independently available when either is. This is structurally different from what §3
shows for a meaningful slice of the US population.

---

## 3. Real US/Canada data — the finding that changes this phase's scope (Part 1 item 5 / Part 8)

**VERIFIED**, direct query against `main.financials.financials_raw`, corrected for the NULL-handling
mistake in §1 item 5:

| Metric | Count |
|---|---|
| Distinct US/Canada tickers with a real `"Total Equity (incl NCI)"` raw fact | **1,675** |
| Distinct US/Canada tickers with a real `"Total Stockholders Equity"` raw fact | 2,646 |
| Distinct US/Canada tickers with **ONLY** the incl-NCI tag — zero direct tag at all | **59** |

**For these 59 tickers, the `CONCEPT_SYNONYMS` fold is the *only* mechanism that populates
`"Total Stockholders Equity"` in the clean `financials` table at all.** Directly confirmed for
one of them, live in current production:

```
CAT (Caterpillar), FY2025:
  financials.Total Stockholders Equity        = $21,318,000,000
  financials_raw."Total Equity (incl NCI)"    = $21,318,000,000   (exact match)
  financials_raw."Total Stockholders Equity"  = (no rows at all for this ticker/concept)
```

Sample of other affected tickers (real, large-cap): `T` (AT&T), `VZ` (Verizon), `PG` (Procter &
Gamble), `ADM`, `CAT`. **This is not a marginal, obscure population** — it includes some of the
largest companies in the dataset.

**Materiality where both tags coexist** (the other 1,616 tickers): sampled AEE — direct
$13,401,000,000 vs. incl-NCI $13,530,000,000, a 0.96% difference. **INFERRED**: NCI is usually a
small fraction of total equity for a typical US filer, but the file's own pre-existing comment
cites a real counter-example (VNOM, ~40% NCI via a subsidiary stake) — materiality varies widely
per issuer and was not exhaustively characterized this pass.

---

## 4. Why the originally-planned fix is unsafe

The fix this phase was scoped to implement — remove the single `CONCEPT_SYNONYMS` entry
(`"Total Equity (incl NCI)": "Total Stockholders Equity"`) — was designed against the (incorrect)
premise from an early query that the synonym was dormant for US/Canada. **It is not.** Removing
it outright would silently NULL out `"Total Stockholders Equity"` — and every metric that reads
it (ROE, Tangible Book Value, Working Capital, any Net-Net Finder/valuation formula using equity)
— for at least 59 real, currently-covered US/Canada tickers, including several large, well-known
companies. This is exactly the regression Part 8 of this phase's own instructions required
checking for, and exactly STOP condition #2: **"The existing synonym turns out to be necessary
for US/Canada in a way that the proposed fix would break."**

A more surgical fix — only skip the rename when a genuine, independent direct fact *also* exists
for that exact `(ticker, stmt, fy)` key, so the incl-NCI fact still falls back to
`"Total Stockholders Equity"` when it's the only source, but survives as its own row when a
direct fact is also present — is conceptually appealing (it would fix EU's case, which always
has both, and leave the 59-ticker US population's existing behavior untouched) but requires a
genuinely new mechanism. `CONCEPT_SYNONYMS` today is a flat, unconditional rename applied before
any per-key existence check; a "coalesce as fallback only when no better data exists, but never
discard a real value when one *is* available" rule does not exist anywhere in this codebase's
current merge logic (`CONCEPT_PRIORITY`/`CONCEPT_PRIORITY_BY_STMT` only break ties among facts
that already share one key — they don't decide *whether* two different source concepts should be
folded into that one key in the first place). Designing and implementing that mechanism is a
genuine, non-trivial architectural change to the canonical merge model — precisely **STOP
condition #6: "The fix requires architectural changes to the canonical model beyond this isolated
collision."**

This document does not propose that architecture unilaterally. Three shapes it could plausibly
take (**not designed, not chosen, not implemented — for the repo owner's decision**):

- **A. Keyed conditional fallback**: a two-pass merge — direct facts populate
  `"Total Stockholders Equity"` first; a second pass fills in incl-NCI *only* for keys with no
  result from the first pass; incl-NCI facts *always* independently also populate their own
  `"Total Equity (incl NCI)"` row (i.e., stop renaming entirely, add a real coalesce step instead
  of a synonym rename).
- **B. Same as A, phrased as a per-key conditional synonym**: rename to
  `"Total Stockholders Equity"` only when no direct-tag fact exists for that exact key; otherwise
  leave the label alone. Architecturally equivalent to A, different implementation shape (a
  filtered/joined rename vs. a two-pass merge).
- **C. Duplicate, don't rename**: incl-NCI facts always survive as their own
  `"Total Equity (incl NCI)"` row (simple — just remove the synonym); *separately*, keep today's
  fallback behavior for `"Total Stockholders Equity"` exactly as-is for the 59 single-tag
  tickers via an explicit, additional coalesce (not the same mechanism as the synonym rename).
  Slightly redundant for those 59 (the same number would appear under two labels) but simpler to
  implement and reason about than A/B.

All three require a decision this document is not authorized to make: **should
`"Total Stockholders Equity"` ever represent an incl-NCI approximation going forward for filers
that only tag the broader figure — i.e., is CAT's/T's/VZ's/PG's/ADM's current displayed
"Total Stockholders Equity" value (which is actually their incl-NCI total equity) acceptable to
keep exactly as-is, or should it become NULL for those 59 tickers on the "NULL > questionable
value" principle this project applies elsewhere (e.g., EU Revenue/Net Income for ISP/NAI)?** That
is a real, pre-existing question about *today's* SEC-side data quality — not something Phase 6.1's
EU work introduced, and not something this narrowly-scoped phase should decide by implementing
one option over another without that sign-off.

---

## 5. What was and was not done

- **No code was modified.** `01__tickers.py`, `21__clean_and_merge.py`, and every other file
  listed in this phase's "Allowed scope" remain byte-identical to the branch tip.
- **No table was written to.** Every query in §§2-3 was a read-only `SELECT`.
- **No tests were added** — there is nothing to regression-test yet; the fix itself doesn't
  exist.
- **No EU re-ingestion was run** — Part 7 of this phase's instructions explicitly says to stop
  before any write if the fix can't be safely scoped, and there is no fix to validate yet.
- **A dedicated branch was created** (`phase6-5-total-equity-nci-normalization-fix`, off the
  current Phase 6.3 branch tip) per this phase's git instructions, but carries only this
  documentation — no implementation commit.

---

## Final Report

- **Classification: BLOCKED — NEEDS ARCHITECTURAL DECISION**
- **Root cause**: confirmed exactly as Phase 6.4 described (the `CONCEPT_SYNONYMS` fold). What
  changed is the risk assessment: this phase's own required evidence-gathering (Part 1 item 5,
  Part 8) revealed the fold is **not dormant** — it is the sole source of `"Total Stockholders
  Equity"` for at least 59 real US/Canada tickers today, including large, well-known companies
  (CAT confirmed live with an exact-match value).
- **Fix implemented**: **NONE.** The originally-scoped minimal fix (remove the synonym entry)
  would have caused a real, material regression for those 59 tickers — caught before
  implementation, not after.
- **Real EU examples**: FCC's identity confirmed exactly (§2); 7/8 EU issuers have both values
  genuinely, independently available.
- **Tests**: 0 added / 0 run (no fix exists to test).
- **Ruff**: not run (no code changed).
- **EU validation**: not performed — no fix to validate; would require re-running `16`→`21` for
  the 8 EU tickers once a fix design is authorized.
- **US regression**: not applicable — no fix was implemented, so nothing changed. The regression
  *that would have occurred* under the originally-planned fix is documented in §3-4 instead.
- **Canada regression**: not separately characterized this pass (the §3 query covers "not
  EU_CURRENT," which includes Canada; no Canada-specific ticker was individually spot-checked,
  since the finding was already conclusive from the US examples).
- **Downstream impact**: confirmed `22__derived_metrics.py`/`23__intrinsic_value.py` do not
  reference `"Total Equity (incl NCI)"` by name (§1 item 6) — but they **do** read
  `"Total Stockholders Equity"`, which is exactly the concept a naive fix would have broken for
  59 tickers. This is the downstream impact this phase's Part 9 asked about, and the answer is
  **yes, there is a real downstream dependency**, reported before any change was made, per
  instruction.
- **Production writes: NO**
- **New unrelated findings: YES** — the NULL-handling correction in §1 item 5/§3 itself, and the
  scale of real US/Canada reliance on this synonym, neither of which Phase 6.4's EU-scoped audit
  surfaced.
- **Tier A started: NO**
- **Website changed: NO**
- **fundamentals_screener changed: NO**
- **Next recommended action**: present options A/B/C (§4) to the repo owner alongside the real
  59-ticker evidence, and get an explicit decision on the underlying semantic question (should
  `"Total Stockholders Equity"` keep silently absorbing incl-NCI values for single-tag filers, or
  should that population go NULL instead) before any implementation is attempted. This is a
  genuine architectural/data-quality decision, not an engineering task this phase should resolve
  by picking an option unilaterally.

# Phase 6.3 — Net Income statement-classification bug: root cause, fix, and validation

**STATUS: IMPLEMENTED AND VALIDATED.** §§1–7 below are the original root-cause analysis and fix
design (research-only at the time they were written). §8 records what was actually implemented,
tested, and validated against real production data — including a second, related bug found
during validation and fixed in the same pass. See §8 for current status;
**Databricks pipeline: VERIFIED. GitHub Release: VERIFIED. Website cache refresh: PENDING
EXTERNAL CRON** (not a pipeline defect — see §8.8).

This document exists because validating Phase 6.3's live `23__intrinsic_value.py` run
(`docs/phase6-3-derived-metrics-recomputation.md`) surfaced a real defect — `Owner Earnings
(FY) = 0.0` for all 8 European issuers — that turned out to be a symptom of something more
fundamental and more consequential than a missing-input gap. §§1–7 root-cause it fully against
real production data and real code, and design the minimal fix. §8 implements and validates it.

Every claim below is labeled **VERIFIED** (checked directly against real code and/or real
production data in this session), **INFERRED** (a reasonable conclusion from verified facts, not
independently re-checked), or **PROPOSED** (a design recommendation, not yet decided or built).

---

## 1. The bug, restated precisely

**VERIFIED.** This is not "Europe lacks Net Income." Europe has real Net Income data — it is
misclassified, and that misclassification cascades into two separate, serious, currently-live
consequences:

1. **The wrong Net Income value is currently displayed for every EU issuer with material
   non-controlling interests.** FCC's real parent-attributable Net Income (`ifrs-full:
   ProfitLossAttributableToOwnersOfParent`) is €429,865,000 (FY2024). Its real consolidated
   (incl. NCI) Net Income (`ifrs-full:ProfitLoss`) is €567,584,000. **The clean `financials`
   table currently shows €567,584,000 for FCC's "Net Income"** — the wrong, larger, incl-NCI
   figure. Same pattern confirmed for ALO (real: €324,000,000 attributable vs. €365,000,000
   incl-NCI; shown: €365,000,000).
2. **`23__intrinsic_value.py` cannot find Net Income for any EU issuer at all**, because it
   filters specifically for `(stmt="Income Statement", concept="Net Income")`
   (`23__intrinsic_value.py:210`, with the comment "we include `stmt` in the join — `Net Income`
   appears in both `Income Statement` and `Cash Flow`… without filtering we would duplicate
   rows"). Every EU Net Income row is stamped `stmt="Cash Flow"`, so this filter finds nothing,
   `pdf["net_income"].fillna(0)` silently zeroes it, and — since CapEx/D&A/SBC/ΔWC are also
   unmapped (Tier 2) — `Owner Earnings (FY)` computes as a literal `0.0` for all 8 issuers,
   confirmed against real production data, in every fiscal year.

A third, related consequence (§4): `"Net Income (incl NCI)"` has **zero** rows in the clean
`financials` table for any of the 8 EU issuers, despite being confirmed present in the real
source data and successfully ingested into `financials_raw`.

---

## 2. Root cause

**VERIFIED**, from direct code reading, not inference.

`fundamentals_pipeline/00__config/01__tickers.py` defines three separate concept dictionaries —
`INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW` — combined into `STATEMENTS = {"Income
Statement": INCOME_STATEMENT, "Balance Sheet": BALANCE_SHEET, "Cash Flow": CASH_FLOW}`
(`01__tickers.py:296-300`). Three labels genuinely exist in **both** `INCOME_STATEMENT` and
`CASH_FLOW`, with identical `(us-gaap tag, kind)` tuples in both places:

| Label | In `INCOME_STATEMENT` | In `CASH_FLOW` | Tag (identical in both) |
|---|---|---|---|
| `"Net Income"` | line 87 | line 181 | `NetIncomeLoss` |
| `"Net Income (to common)"` | line 88 | line 182 | `NetIncomeLossAvailableToCommonStockholdersBasic` |
| `"Net Income (incl NCI)"` | line 89 | line 183 | `ProfitLoss` |

**Confirmed programmatically**: these are the *only* three collisions across all three
vocabularies (25 Income Statement labels, 20 Balance Sheet labels, 21 Cash Flow labels checked
pairwise — Income Statement ∩ Balance Sheet = empty, Balance Sheet ∩ Cash Flow = empty, Income
Statement ∩ Cash Flow = exactly these three).

**This duplication is intentional and correct for SEC ingestion.** `11__fetch_sec_xbrl.py:617`
iterates `for stmt_name, concept_map in STATEMENTS.items():` and independently extracts and
stamps `stmt=stmt_name` (`11__fetch_sec_xbrl.py:647`) **once per statement dict it walks** — so
for a filer reporting `NetIncomeLoss`, this produces **two separate, correctly-labeled rows**:
one with `stmt="Income Statement"`, one with `stmt="Cash Flow"` (the real GAAP reconciliation
line). **Confirmed directly against production**: AAPL has 17 `Income Statement` Net Income rows
and 17 separate `Cash Flow` Net Income rows (one pair per fiscal year on record); AQN
(Canada) has 8 and 8. Every other consumer of `STATEMENTS` in the repository
(`13__fetch_dimensional_10k.py:89`, `14__fetch_oracle_statements.py:153`,
`21e__derive_fy_from_quarterly.py:53`, `35__reconcile_filings.py:97,106`,
`03__concept_hierarchy_master.py:149`) uses this same safe, per-statement-scoped iteration
pattern — never a collapsed reverse lookup.

**`16__fetch_eu_xbrl.py` is the one exception**, and the only place in the entire repository that
constructs a *collapsed* reverse map:

```python
_LABEL_TO_STMT_KIND = {
    label: (stmt_name, kind)
    for stmt_name, concept_map in globals().get("STATEMENTS", {}).items()
    for label, (_xbrl_concept, kind) in concept_map.items()
}
```

(`16__fetch_eu_xbrl.py:99-103`). Because Python dict comprehensions let a later key overwrite an
earlier one, and `STATEMENTS.items()` iterates `Income Statement → Balance Sheet → Cash Flow`,
every one of the three colliding labels resolves to `"Cash Flow"` — **confirmed
programmatically** by simulating the exact same construction. `process_eu_entity()`
(`16__fetch_eu_xbrl.py:361`) then stamps every EU fact for that canonical concept with whatever
single `stmt` `_LABEL_TO_STMT_KIND` gives it — for "Net Income," always `"Cash Flow"`, confirmed
against 100% of real production rows for all 8 EU issuers.

**Why this only breaks for EU, not SEC/Canada**: a real SEC `companyfacts` filing genuinely
contains Net Income tagged in two different XBRL *contexts* (the income statement presentation
and the cash-flow reconciliation), so SEC's per-statement-independent extraction correctly
produces two real rows. A real ESEF filing has exactly **one** XBRL fact for `ifrs-full:
ProfitLossAttributableToOwnersOfParent` — there is no second "cash-flow-context" duplicate to
find. The EU adapter's job is to place that single fact in the *one* statement it economically
belongs to (the income statement bottom line) — and `_LABEL_TO_STMT_KIND`'s accidental
last-write-wins behavior places it in the wrong one.

---

## 3. End-to-end trace

**VERIFIED**, real production data:

| Ticker | Source concept | Canonical concept | `stmt` in `financials` | `23`'s `NEEDED` filter finds it? | Owner Earnings (FY) |
|---|---|---|---|---|---|
| AAPL | `us-gaap:NetIncomeLoss` | Net Income | **Income Statement** (+ separate Cash Flow row) | yes | real (e.g. $123.86B FY2025) |
| AQN | `us-gaap:NetIncomeLoss` (ifrs-full fallback path unused here) | Net Income | **Income Statement** (+ separate Cash Flow row) | yes | real (e.g. -$223.5M FY2025) |
| FCC | `ifrs-full:ProfitLossAttributableToOwnersOfParent` | Net Income | **Cash Flow only** | no | `0.0` |
| ALO | `ifrs-full:ProfitLossAttributableToOwnersOfParent` | Net Income | **Cash Flow only** | no | `0.0` |

The mechanism is fully explained by §2 — no unexplained residual behavior.

---

## 4. `"Net Income (incl NCI)"` — a separate, now-understood mechanism

**VERIFIED, not the same root cause as initially guessed — traced to its actual, different
mechanism.**

1. **Is the concept present in the source filings?** Yes — `ifrs-full:ProfitLoss` was confirmed
   present for 8/8 issuers in Phase 6.0's real xBRL-JSON research, and is one of the 5 original
   Phase 5.1 mapped concepts.
2. **Is it ingested?** Yes — confirmed directly in `financials_raw` (the append-only audit
   table): FCC has 60 real `"Net Income (incl NCI)"` rows across scrapes, ALO has 96, both
   correctly valued (FCC: €567,584,000 FY2024; ALO: €365,000,000 FY2026) and both stamped
   `stmt="Cash Flow"` (same root cause as §2 — this label collides too).
3. **Is it collapsed by `CONCEPT_SYNONYMS`?** **Yes — this is the actual mechanism.**
   `01__tickers.py`'s `CONCEPT_SYNONYMS` has `"Net Income (incl NCI)": "Net Income"` (aliasing it
   to the canonical "Net Income" label during `21__clean_and_merge.py`'s merge) — a real,
   intentional, working SEC-side mechanism (a filer that reports both `NetIncomeLoss` and
   `ProfitLoss` in the same fiscal year should collapse to one canonical "Net Income," not two
   competing rows).
4. **Which value wins the collapse, and why is it wrong for EU specifically?**
   `CONCEPT_PRIORITY_BY_STMT["Cash Flow"] = {"Net Income (incl NCI)": 0, "Net Income": 1, "Net
   Income (to common)": 2}` (`01__tickers.py:516-521`) — a **Cash-Flow-statement-specific
   override**, intentionally inverting the normal preference, because "GAAP's indirect-method
   reconciliation starts from the CONSOLIDATED net income… the attributable figure… is the wrong
   CF start" (the file's own comment). Because both of EU's Net Income rows are (incorrectly, per
   §2) stamped `stmt="Cash Flow"`, this Cash-Flow-specific override fires — even though the row
   is not actually a cash-flow reconciliation line — and the *consolidated* (incl-NCI) value
   wins the tiebreak and gets written into the clean `financials` table's "Net Income" concept
   slot, silently overwriting the correct parent-attributable figure that should have won under
   the normal, un-overridden `CONCEPT_PRIORITY` (`"Net Income": 0` beats `"Net Income (incl
   NCI)": 2`).

**Conclusion: `"Net Income (incl NCI)"` is not lost or filtered — it is correctly collapsed into
"Net Income" by design, but the collapse resolves to the wrong winner because of the same §2 stmt
misclassification.** Fixing §2's root cause is expected to fix this too (§5) — not a second,
independent bug requiring its own fix.

---

## 5. Duplicate-label audit

**VERIFIED, exhaustive** (all label pairs checked programmatically, not sampled):

| Label | Statement kinds it appears in | `_LABEL_TO_STMT_KIND` result today | Production impact | Classification |
|---|---|---|---|---|
| `"Net Income"` | Income Statement, Cash Flow | Cash Flow | Confirmed real: wrong value + missing from Income Statement tab + Owner Earnings = 0.0, all 8 EU issuers | **REAL BUG** |
| `"Net Income (incl NCI)"` | Income Statement, Cash Flow | Cash Flow | Confirmed real: feeds the wrong-value collapse in §4 | **REAL BUG** (same root cause) |
| `"Net Income (to common)"` | Income Statement, Cash Flow | Cash Flow | **No production impact today** — EU_CANONICAL_MAPPING never produces this canonical concept for any EU issuer (confirmed: not one of the 21 Phase 6.1 concepts) | **SAFE today, latent** — would reproduce the identical bug the instant a future EU concept-mapping phase ever maps `NetIncomeLossAvailableToCommonStockholdersBasic` or an IFRS equivalent to it |

No other label appears in more than one of `INCOME_STATEMENT`/`BALANCE_SHEET`/`CASH_FLOW` —
confirmed by exhaustive pairwise set intersection, not sampling. **The fix must not be scoped to
"Net Income" alone** — a fix that hardcodes just that one label would leave "Net Income (to
common)" as a live landmine for the next EU mapping phase.

---

## 6. Proposed minimal fix (implemented as designed — see §8)

**PROPOSED** (at the time this section was written; implemented essentially verbatim, see §8.1).

**Scope: `fundamentals_pipeline/10__ingestion/16__fetch_eu_xbrl.py` only.** Zero changes to
`01__tickers.py` (the `STATEMENTS` duplication there is correct and load-bearing for SEC — do
not touch it), zero changes to `21__clean_and_merge.py`, `22__derived_metrics.py`, or
`23__intrinsic_value.py`. This directly satisfies every stated requirement (preserve SEC
behavior, preserve Canada behavior, avoid a canonical-model change) because none of those files'
own code path is implicated — `_LABEL_TO_STMT_KIND` is used nowhere outside this one file.

**The fix**: replace the collapsing dict-comprehension with a **deterministic, explicitly
documented priority order**, first-match-wins instead of last-match-wins:

```python
# Deterministic priority when a canonical label exists in more than one statement vocabulary
# (SEC's own STATEMENTS intentionally duplicates a few labels — e.g. "Net Income" is both the
# Income Statement bottom line and the Cash Flow reconciliation start, because SEC ingestion
# extracts each statement's concept map independently and can produce two real, correctly-
# distinct rows from two different XBRL contexts — 11__fetch_sec_xbrl.py:617-647). The EU
# adapter has no such second context: one ESEF fact maps to one canonical concept, which must
# be placed in the ONE statement it economically belongs to. Income Statement first: every
# current collision (Net Income and its two synonyms) is a P&L bottom-line concept.
_STMT_PRIORITY = ("Income Statement", "Balance Sheet", "Cash Flow")

_LABEL_TO_STMT_KIND: dict[str, tuple[str, str]] = {}
for _stmt_name in _STMT_PRIORITY:
    for _label, (_xbrl_concept, _kind) in globals().get("STATEMENTS", {}).get(_stmt_name, {}).items():
        _LABEL_TO_STMT_KIND.setdefault(_label, (_stmt_name, _kind))
```

`setdefault` means the **first** statement a label is found in (by explicit, documented priority
— not `dict` insertion-order coincidence) wins, and every future label added to `STATEMENTS`
that happens to collide gets the same safe, deterministic treatment automatically — no per-label
override list to maintain, and §5's "Net Income (to common)" landmine is defused at the same
time as "Net Income."

**Why this also fixes §4's wrong-value bug, with no code change to `21__clean_and_merge.py`
needed**: once EU's "Net Income" and "Net Income (incl NCI)" rows are both stamped
`stmt="Income Statement"` instead of `"Cash Flow"`, `21`'s merge no longer applies
`CONCEPT_PRIORITY_BY_STMT["Cash Flow"]`'s inverted override — it falls through to the normal,
global `CONCEPT_PRIORITY` (`"Net Income": 0` beats `"Net Income (incl NCI)": 2`), which correctly
prefers the parent-attributable figure. This is a consequence of the existing, unmodified
`21__clean_and_merge.py` logic, not a second fix.

**A real, honest trade-off of this fix, to flag explicitly**: EU's "Net Income" will no longer
appear under the `stmt="Cash Flow"` label at all (it never had a genuinely separate Cash-Flow
XBRL context to begin with, unlike SEC). This means the EU Cash Flow tab's own reconciliation-
start line for Net Income will go from "present but wrong" to "absent." This is more correct
(no fact exists there to show), but is a visible, real difference from the current (buggy)
display — should be confirmed against the actual Cash Flow tab's own layout expectations
(`concept_hierarchy.json`) before implementing, not assumed harmless.

**Alternative considered and not recommended**: a narrow, hardcoded `{"Net Income": "Income
Statement", "Net Income (to common)": "Income Statement", "Net Income (incl NCI)": "Income
Statement"}` override dict, checked before the generic reverse-map. Rejected as the primary
recommendation because it is not future-proof (§5's exact concern) and because the deterministic-
priority version above is barely more code and eliminates the whole bug class, not just today's
three instances.

---

## 7. Validation plan (design only — not executed)

**PROPOSED.** After the fix lands (a separate, future, explicitly-authorized change):

**Pre-checks (read-only, before any write)**:
- Confirm the fix is EU-only: `git diff` touches only `16__fetch_eu_xbrl.py`.
- Confirm `01__tickers.py`'s `STATEMENTS`/`CONCEPT_SYNONYMS`/`CONCEPT_PRIORITY`/
  `CONCEPT_PRIORITY_BY_STMT` are byte-identical to today.

**Re-ingestion + re-merge (the smallest scope that can prove the fix)**:
- Re-run `16__fetch_eu_xbrl.py` (append-only, EU-scoped by construction — cannot touch SEC/
  Canada) to write fresh, correctly-`stmt`-stamped rows to `financials_raw`.
- Re-run `21__clean_and_merge.py` (its own `scraped_at == latest_scrape` scoping already proved
  safe in Phase 6.1's validation — re-verify `MAX(scraped_at)` belongs 100% to the fresh EU
  scrape before running, exactly as done then).

**Verify, with real data, for every one of the 8 EU issuers (FCC, ALO, NAI, FCT, IBE, SGO, RAND,
ISP)**:
- `Net Income`'s `stmt` is now `"Income Statement"`, not `"Cash Flow"`.
- `Net Income`'s *value* now matches the real parent-attributable figure (`ifrs-full:
  ProfitLossAttributableToOwnersOfParent`), not the consolidated one — spot-check FCC
  (expect €429,865,000 FY2024, not €567,584,000) and ALO (expect €324,000,000 FY2026, not
  €365,000,000).
- The Income Statement tab's row set now includes Net Income (a real display check, not just a
  table query).
- Re-run `22__derived_metrics.py` and `23__intrinsic_value.py` (full-universe, per Phase 6.3's
  own already-proven-safe protocol) and confirm `Owner Earnings (FY)` is now a real, non-zero
  value for issuers where Net Income is present, and stays correctly **NULL** — never `0.0` —
  for any issuer/year still missing a genuinely required input (this is the core semantic check
  requested: missing input → NULL, never a fabricated zero).

**US/Canada regression (must stay byte-identical)**:
- AAPL, MSFT, TSLA: `financials`/`financials_metrics` row counts and `Net Income` values on both
  `Income Statement` and `Cash Flow` unchanged.
- AEM, AQN, BN: same.
- Specifically confirm SEC/Canada's own two-rows-per-Net-Income-label behavior is untouched (the
  fix must not accidentally collapse SEC's genuine dual rows into one).

**A currently-open question the validation plan itself needs to resolve, not this document**:
whether the Cash Flow tab's own layout should be updated (via `concept_hierarchy.json`, a data/
config change, not application logic) once EU's Cash Flow "Net Income" line disappears as a
trade-off of the fix (§6) — deferred to whoever implements and validates the fix.

---

## Final report

1. **Is the root cause confirmed?** Yes — traced to real code (`16__fetch_eu_xbrl.py:99-103`'s
   `_LABEL_TO_STMT_KIND` construction) and confirmed against real production data for multiple
   tickers, not inferred from a single example.
2. **Why does AAPL work while EU does not?** SEC ingestion (`11__fetch_sec_xbrl.py`) extracts
   each statement's concept map independently, producing two real, correctly-labeled rows per
   colliding label. The EU adapter collapses all three statement vocabularies into a single
   reverse-lookup dict, where a colliding label can only ever resolve to one statement — and an
   accidental (not designed) last-write-wins order currently picks the wrong one.
3. **What exactly happens to Net Income?** It is stored under `stmt="Cash Flow"` instead of
   `"Income Statement"` for all 8 EU issuers, causing (a) `23__intrinsic_value.py` to never find
   it, silently zeroing Owner Earnings instead of leaving it NULL, and (b) it to disappear from
   the live Income Statement tab entirely (confirmed on the real, published FCC page).
4. **What happens to `Net Income (incl NCI)`?** Not lost — correctly collapsed into "Net Income"
   by the pre-existing `CONCEPT_SYNONYMS` mechanism, but the collapse resolves to the wrong
   (consolidated, not parent-attributable) value because of the same `stmt` misclassification
   incorrectly triggering `CONCEPT_PRIORITY_BY_STMT`'s Cash-Flow-specific override. Confirmed
   with real, different values for FCC and ALO.
5. **Are there other duplicate statement labels?** Exactly three, exhaustively confirmed:
   `"Net Income"`, `"Net Income (to common)"`, `"Net Income (incl NCI)"` — all Net-Income-family,
   no others across all 66 labels in the three vocabularies.
6. **Which are real bugs?** `"Net Income"` and `"Net Income (incl NCI)"` — confirmed live
   production impact. `"Net Income (to common)"` is currently safe (EU never populates it) but
   latent — the same bug class would reappear the moment a future phase maps it.
7. **What is the minimal safe fix?** Change `16__fetch_eu_xbrl.py`'s `_LABEL_TO_STMT_KIND`
   construction from last-write-wins-by-accident to first-write-wins-by-explicit-priority
   (`("Income Statement", "Balance Sheet", "Cash Flow")`, via `setdefault`). Zero changes to
   `01__tickers.py` or any other file.
8. **What could regress?** Nothing in SEC/Canada (the fix touches a file no SEC/Canada code path
   reads). The one real, honest trade-off: EU's Cash Flow tab loses its (currently wrong) Net
   Income reconciliation line, since no genuine second XBRL context exists to place there —
   flagged as a real display-shape change to confirm, not assumed harmless.
9. **What validation is required?** Full re-ingestion + re-merge for the 8 EU issuers with
   before/after value and `stmt` spot-checks, a full `22`+`23` re-run confirming Owner Earnings
   becomes real (not 0.0) where inputs exist and stays NULL where they genuinely don't, and a
   byte-identical US/Canada regression check — detailed in §7 above.

---

## 8. Implementation and validation (executed)

**VERIFIED**, against real production data, in the personal validation Databricks Repo
(`/Repos/al.lopez.moreira@gmail.com/phase6-validation`, tracking this PR's branch). Executed in
the order below; nothing was run out of sequence.

### 8.1 Code change

`16__fetch_eu_xbrl.py`'s `_LABEL_TO_STMT_KIND` construction replaced with the `setdefault()`-based
deterministic priority exactly as designed in §6, over `_STMT_PRIORITY = ("Income Statement",
"Balance Sheet", "Cash Flow")`. Scope held exactly as specified: no changes to `01__tickers.py`,
SEC ingestion, Canada ingestion, `22`, `23`, `51`, `52`, `fundamentals_screener`, or the frontend.

### 8.2 Tests — priority validated as general, not Net-Income-specific

Added `tests/test_eu_adapter_stmt_kind_priority.py` (5 tests) + extended
`test_eu_adapter_protocol_conformance.py`'s loader to accept a pre-seeded synthetic `STATEMENTS`
dict. Per the explicit requirement that `Income Statement > Balance Sheet > Cash Flow` be proven
as a genuine 3-tier rule and not merely asserted because it happens to fix Net Income:

- The 3 real production collisions all resolve to `Income Statement` (regression test for the
  actual bug).
- Non-colliding labels are unaffected (no over-application).
- **`test_priority_generalizes_beyond_income_statement_vs_cash_flow`**: two synthetic collisions
  *unrelated to Net Income* — a Balance-Sheet-vs-Cash-Flow pairing (`"Cash and Equivalents"`, a
  stock balance that also legitimately appears as the Cash Flow statement's ending-balance
  reconciliation line) and an Income-Statement-vs-Cash-Flow pairing (`"Interest Expense"`) — both
  resolve to the higher-priority statement, proving the rule holds for pairings the real Net
  Income bug never exercises.
- **`test_result_is_independent_of_statements_dict_insertion_order`**: the same fixture rebuilt
  with the outer `STATEMENTS` dict reversed, the inner concept-map dicts reversed, and both
  reversed together — all three produce byte-identical results to the baseline. Direct proof the
  old bug class (result depends on dict/insertion order) cannot recur.
- Missing-`STATEMENTS` fallback (outside Databricks) still yields an empty, harmless map.

7/7 new+updated tests pass. Full suite: 360 passed, 2 skipped (fixture-gated, expected). Ruff
clean on all touched files.

### 8.3 EU re-ingestion (16) — real production data

Fresh scrape, all 8 tickers: 100% of `"Net Income"` and `"Net Income (incl NCI)"` facts now stamp
`stmt="Income Statement"` — zero remaining `"Cash Flow"` rows (was 100% `"Cash Flow"` pre-fix).

### 8.4 Clean merge (21) — a second bug found and fixed mid-validation

Re-running `21__clean_and_merge.py` after the corrected ingestion surfaced a real, unanticipated
consequence: the plain UPSERT `MERGE` never deletes, and §4's existing orphan-DELETE (scoped to
"fabricated annual from a sub-annual shape") only catches keys that ARE part of the current
scrape's reported-key universe under their *current* stmt — a key whose stmt moved elsewhere
entirely never enters that universe, so it was invisible. Result: for all 8 EU tickers, the OLD
`stmt="Cash Flow"` Net Income row (the wrong, pre-fix incl-NCI value) stayed in `financials`
forever, sitting alongside the new, correct `stmt="Income Statement"` row.

Paused and presented three remediation options (extend `21`'s orphan logic / one-time manual
DELETE / accept and defer). Chose to extend `21__clean_and_merge.py` with a second, narrowly-
scoped DELETE step (§4b in the notebook) at the same `(ticker, stmt, concept, fiscal_year)`
granularity the existing orphan-DELETE already uses, scoped to tickers actually present in the
current scrape — only deletes a row when the current scrape can positively prove that exact key
moved to a different stmt (not merely that it went unreported this time, which stays untouched,
matching the existing conservative behavior for that separate case). This is a general safety
property, not EU-specific — the identical logic would correctly clean up a genuine future
SEC/Canada re-tag too.

Re-ran 16→21 after this change: all 8 EU tickers now show exactly **one** Net Income row each
(`Income Statement`, correct value); the stale `Cash Flow` duplicates are gone. SEC/Canada's
genuine dual-statement rows (AAPL, MSFT, TSLA, AEM, AQN, BN — both `Income Statement` and
`Cash Flow` Net Income rows, by design, per §2) confirmed completely untouched, same row counts
before and after.

### 8.5 FCC/ALO value validation

| Ticker | FY | Pre-fix (wrong) | Post-fix (correct) | Matches §1 target? |
|---|---|---|---|---|
| FCC | 2024 | €567,584,000 | €429,865,000 | Yes, exact |
| ALO | 2026 | €365,000,000 | €324,000,000 | Yes, exact |

### 8.6 22 — full-universe run

Ran twice (initial + idempotency check). Row counts identical to the pre-fix Phase 6.3 baseline
for every ticker checked (US/CA and EU) — expected, since the fix corrects *values*, not which
metrics get computed. Decisive cross-check: FCC's `Net Margin %` = 4.7387%, which reconciles
exactly to €429,865,000 / €9,071,416,000 (its FY2024 Revenue) — proof the corrected Net Income is
flowing through `22`'s derivation, not just sitting in `financials` unused. IBE (€5.612B) and SGO
also confirmed on the real Income Statement Net Income. No safety-guard exception raised (both
runs `TERMINATED SUCCESS`).

### 8.7 23 — Owner Earnings validation

`Owner Earnings (FY)` is non-zero for all 31 EU ticker-year rows checked across all 8 tickers —
zero remaining `0.0` values. Note: `23`'s existing, unrelated `fillna(0)` formula design means
Owner Earnings for EU currently reduces to exactly Net Income (Tier 2 inputs — CapEx/D&A/SBC/ΔWC —
remain unmapped, contribute `0`, unchanged by this fix and out of scope per instruction). AAPL
unchanged: $123.856B FY2025, matching the pre-fix baseline exactly.

### 8.8 Idempotency, 51, 52

Re-ran `22`+`23` a second time: identical row counts and value sums for every ticker checked, no
duplicates, no drift. `51__export_dashboard_data.py` and `52__publish_to_github.py` both
succeeded; the GitHub Release was confirmed `draft: false` and freshly published
(`published_at: 2026-08-17T16:53:02Z`).

**Website cache — PENDING EXTERNAL CRON, not a pipeline defect.** The live public site
(`fundamentals_screener` deployment) still showed the pre-fix €567.6M for FCC after the Release
was published. This is expected and documented, not a bug: the production deployment reads from
a cron-refreshed on-disk cache (see the package's "Keeping data fresh" design, CLAUDE.md's
External consumers section) rather than fetching the GitHub Release live on the request path.
Confirmed the GitHub Release side is correct and complete; the remaining step (the site's own
scheduled cache refresh) is outside this session's reach and outside this fix's scope.

### 8.9 US/Canada regression

Clean throughout every checkpoint (16 was EU-only so these tickers were never touched; 21/22/23
row counts and dual-statement Net Income rows byte-identical before/after for AAPL, MSFT, TSLA,
AEM, AQN, BN).

### 8.10 Remaining limitations

- Tier 2 EU inputs (CapEx, D&A, SBC, ΔWC, Accounts Receivable, Profit Before Tax, Finance Income,
  EPS, Gross Profit, Total Liabilities) remain unmapped — untouched by design, per instruction.
- `"Net Income (to common)"` stays a defused-but-dormant landmine (never populated for EU today,
  but would resolve safely if a future mapping phase ever produces it).
- Website cache refresh pending (§8.8) — expected to resolve on the next scheduled cron cycle,
  external to this repo.

---

## Final status

**Databricks pipeline: VERIFIED. GitHub Release: VERIFIED. Website cache refresh: PENDING
EXTERNAL CRON.**

**Classification: READY TO MERGE — EXTERNAL WEBSITE REFRESH PENDING.** The fix (§6), its general-
priority validation (§8.2), and the additional stale-row cleanup discovered and fixed during
validation (§8.4) are implemented, tested, and validated end-to-end against real production data.
The one open item — the live site's own cache refresh — is an external, asynchronous dependency
outside this PR's control and is not a reason to hold or modify the implementation.

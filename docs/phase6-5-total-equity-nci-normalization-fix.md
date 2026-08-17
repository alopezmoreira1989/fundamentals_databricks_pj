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

---
---

## PHASE 6.5b — ARCHITECTURAL DECISION

**READ-ONLY investigation. No code changed, no table written, nothing published, nothing
implemented.** This section resolves the open architectural question §4/§5 of the section above
raised, with the additional real evidence this phase's own instructions required, and ends with
one concrete, chosen recommendation — not a menu.

Every claim is labeled **VERIFIED** (obtained directly, this pass, from real code or real
production data), **INFERRED** (a reasonable conclusion from verified facts), or **PROPOSED**
(a design recommendation, not yet decided or built).

---

## B1. How `Total Stockholders Equity` is actually consumed downstream (Part 1)

**VERIFIED**, direct code read, current branch tip.

`22__derived_metrics.py` reads `"Total Stockholders Equity"` in **eight** places: `Debt / Equity`
(`:321-322`), `Invested Capital` → `ROIC %` (`:379`), `ROE %` (`:390`), the NCAV/Net-Net Finder's
Total-Liabilities fallback (`Total Assets − Total Stockholders Equity` when a filer doesn't tag
`Liabilities` directly, `:438-439`), `Tangible Book Value` (`:521-526`), the same computation
reused for `val_wide`'s own lineage (`_tbv_val`, `:1259-1264`), `P/B` (`:1302`), `Book Yield %`
(`:1325`), and the Altman Z-Score's `X4` leverage-ratio fallback (`:1348-1358`, whose own comment
already explicitly documents accepting "the usually small" NCI-approximation error — direct,
pre-existing evidence the project already tolerates this exact trade-off elsewhere, deliberately,
not by oversight).

`23__intrinsic_value.py` reads it via the `"equity"` alias (`:216`, one of `STOCK_ALIASES`) into
`bvps` (Book Value Per Share, `:789`) and the TTM margin-of-safety scenarios' `_ttm_equity`
(`:808`).

**No formula anywhere reads `"Total Equity (incl NCI)"` by its own name** — confirmed again this
pass (§1 item 6 of the section above already established this via repo-wide grep).

---

## B2. Real US/Canada evidence (Part 2)

**VERIFIED**, direct query against `main.financials.financials_raw` (deduplicated).

**All five named tickers (CAT, T, VZ, PG, ADM) have *only* the incl-NCI tag across their ENTIRE
ingested history** — not just recently:

| Ticker | Years with `Total Equity (incl NCI)` raw fact | Years with `Total Stockholders Equity` raw fact |
|---|---|---|
| CAT | 2006–2025 (20 years) | 0 |
| T | 2006–2025 (20 years) | 0 |
| VZ | 2007–2025 (19 years) | 0 |
| PG | 2007–2026 (20 years, fiscal-June) | 0 |
| ADM | 2007–2025 (19 years) | 0 |

This is a **structural, permanent characteristic** of how these five filers tag their XBRL, not
an intermittent gap — none of them has ever, in this pipeline's ingestion history, tagged the
narrower `us-gaap:StockholdersEquity` concept at all. **INFERRED**: for a company like PG (widely
known to carry negligible non-controlling interest), this is very likely a tagging-convention
choice with near-zero practical distortion (the two figures are probably nearly identical in
practice, even though only the broader tag was ever filed) — not independently verified this
pass, since no separate "Non-Controlling Interest" concept exists in the canonical model to
cross-check against for US filers (Phase 6.4's own cross-cutting gap finding). For T and VZ,
real, material NCI from telecom joint ventures/tower entities is plausible and would introduce a
real, non-trivial approximation — also not independently quantified this pass for lack of a
canonical NCI concept to compare against.

**Materiality where both tags exist** (1,514 distinct US/Canada tickers, 20,112 ticker-years):

| Statistic | Value |
|---|---|
| Median absolute % difference (incl-NCI vs. attributable) | **0.73%** |
| 90th percentile | 25.14% |
| Ticker-years where the two values are *exactly* equal | 4,267 (21%) |

**INFERRED**: for the typical filer, the two figures are close; a real, meaningful minority
(roughly the top decile) diverges substantially — consistent with the file's own pre-existing
comment citing VNOM's ~40% NCI as a known, real outlier case. The current tiebreak
(`CONCEPT_PRIORITY`: direct tag wins, 0 vs. 1) already handles this population correctly today —
**this evidence doesn't change anything about how "both exist" cases are resolved; it only
confirms that resolution is already working as intended.**

---

## B3. Real European evidence (Part 3)

**VERIFIED**, direct query against `main.financials.financials_raw` (`source_id = 'EU_CURRENT'`).

| Ticker | `Total Equity (incl NCI)` raw | `Total Stockholders Equity` raw | Both present every year? |
|---|---|---|---|
| FCC | ✓ | ✓ | Yes |
| ALO | ✓ | ✓ | Yes |
| IBE | ✓ | ✓ | Yes |
| SGO | ✓ | ✓ | Yes |
| FCT | ✓ | ✓ | Yes |
| NAI | ✓ | ✓ | Yes |
| RAND | ✓ | ✓ | Yes |
| ISP | — | — | Neither (bank; structurally different equity presentation, per Phase 6.4 §7) |

**Every one of the 7 EU issuers that has either concept has *both*, every fiscal year on
record — the exact opposite population shape from §B2's 59 single-tag US tickers.** EU never
needs the "only the broad figure exists" fallback case at all; it only ever needs "both exist,
preserve both."

---

## B4. Semantic test — does the accounting identity hold? (Part 4)

**VERIFIED for FCC** (re-confirmed, Phase 6.4's own finding): €2,732,716,000 (`Total Stockholders
Equity`, direct `EquityAttributableToOwnersOfParent` tag) + €1,003,303,000
(`NoncontrollingInterests`) = €3,736,019,000, exactly the `Total Equity (incl NCI)` raw value.

**Not independently re-verified for the other 6 EU issuers or for any US ticker this pass** (no
canonical `Non-Controlling Interest` concept exists to join against for US data at all — a
pre-existing, separate gap, Phase 6.4 §12 Tier A). **INFERRED, not verified**: the identity is a
basic accounting relationship (`Total Equity = Equity attributable to owners + NCI`) that should
hold generally by construction of the underlying financial statements, but this document does not
claim to have checked it beyond the one FCC data point.

**Does `Total Stockholders Equity` mean "attributable to owners of the parent," or has the
project used it more broadly in practice?** **VERIFIED, and this is the crux of the semantic
question**: the *canonical definition* (the SEC tag `StockholdersEquity`, the IFRS fallback
`EquityAttributableToOwnersOfParent`, and `22__derived_metrics.py`'s own Altman-Z comment, which
explicitly reasons about "attributable-only equity" as the norm it's deviating from) is
unambiguously "attributable to the parent only." **But the *actual, currently-published data*,
for 59 real US/Canada tickers, has never once contained that narrower figure** — every single
value ever shown under this label for those 59 tickers has been the broader, incl-NCI total,
silently, without any indication in the data that it's an approximation. The label's stated
meaning and its lived behavior for this subset of tickers have quietly diverged since before this
project's EU work began.

---

## B5. Evaluating the three existing options (Part 5)

Retrieved verbatim from §4 above, not silently replaced.

| | **A — Keyed two-pass merge** | **B — Per-key conditional synonym** | **C — Duplicate, don't rename** |
|---|---|---|---|
| **Semantics** | Direct facts populate `Total Stockholders Equity` first; a second pass fills remaining keys from incl-NCI; incl-NCI facts *always* also produce their own row | Rename to `Total Stockholders Equity` only when no direct fact exists for that exact key; otherwise leave the label alone | Incl-NCI facts always survive as their own row (delete the synonym); *separately*, an explicit, additional coalesce step reproduces today's fallback for `Total Stockholders Equity` |
| **Implementation complexity** | Medium — one new merge pass + a per-key existence check | Medium — functionally identical to A, phrased as a filtered rename (a join against "does a direct fact exist for this key" before deciding to rename) | Medium — same two ingredients as A/B (a "don't discard" step + a "fill missing" step), just organized as "duplicate then patch" rather than "two ordered passes" |
| **US impact** | None for existing `Total Stockholders Equity` values (identical fallback behavior preserved); **new** `Total Equity (incl NCI)` rows appear for ~1,675 tickers (not just the 59) — see §B7 | Same as A | Same as A/B |
| **Canada impact** | Same reasoning as US — no Canada-specific evidence gathered this pass, but the mechanism is source-agnostic by construction | Same | Same |
| **EU impact** | Both concepts now survive distinctly for all 7 affected issuers — the original Phase 6.4 ask, satisfied | Same | Same |
| **Downstream impact** | Zero formula changes needed — every consumer of `Total Stockholders Equity` (§B1) keeps reading the exact same values it does today | Same | Same |
| **Migration requirements** | Remove the `CONCEPT_SYNONYMS` entry; add the new keyed-fallback logic to `21__clean_and_merge.py`; the two `CONCEPT_PRIORITY` entries for these labels become vestigial once nothing competes under a shared key anymore (worth removing for clarity, not required for correctness) | Same removals; the "filter" framing may reuse more of the existing prio/window-based Spark idioms already in `21` | Same removals, plus care that the "duplicate" step doesn't double-write when both concepts already exist independently |
| **Risk** | Low — additive only; no existing row's value changes | Low — same | Low, but slightly higher: a "duplicate" framing risks accidentally writing `Total Stockholders Equity` = incl-NCI value even for the 1,616 "both exist" tickers if the "don't overwrite when direct exists" guard is implemented incorrectly — A/B's ordered-pass framing makes that mistake structurally harder to make |
| **Reversibility** | Fully reversible (logic/config only); already-written new rows persist until a future cleanup if reverted, the same characteristic Phase 6.3's own stale-row fix already had to handle | Same | Same |

**A and B are functionally equivalent** — they express the identical rule ("direct wins;
incl-NCI fills only genuinely empty keys; incl-NCI always independently survives") through two
different but equally valid Spark implementation shapes. **C is the same rule, organized
differently, with a slightly higher chance of an implementation bug reintroducing exactly the kind
of silent overwrite this whole investigation exists to prevent.**

**No fourth option is proposed** — A/B already form a complete, minimal, safe answer once
combined with the real evidence in §B2-B4; nothing in this investigation surfaced a need for a
structurally different approach.

---

## B6. The "specific wins, fallback only fills gaps, never overwrite" model (Part 6)

**Is this semantically defensible?** **Yes — and this is the recommended rule (B10).** It matches
real accounting practice (present the parent-attributable figure as primary book equity; treat
the broader consolidated figure as a last-resort proxy only when nothing narrower is available)
and, critically, it is **exactly today's actual behavior for the 59 single-tag US tickers**
— the fix does not change what those 59 tickers show, it only stops throwing away real EU (and,
incidentally, US/Canada) data that was previously discarded even when a *better* figure already
existed for the exact same key.

**Important, explicit caveat** (per this phase's own instruction not to conflate numerical
usefulness with semantic correctness): this rule does **not** claim `"Total Stockholders Equity"`
*is* attributable-only equity for those 59 tickers — it claims that showing the best real figure
available, unchanged from current behavior, remains an acceptable, already-precedented trade-off
(§B1's Altman-Z comment), not that the label becomes newly, fully accurate for them. That
labeling imprecision is real, pre-existing, and not resolved by this fix — see §B7's open
question.

---

## B7. NULL vs. fallback (Part 7)

**Is using `Total Equity (incl NCI)` as a fallback for `Total Stockholders Equity` "a
questionable value" under this project's own "NULL > questionable value" principle, or "a
documented, deterministic approximation"?**

**PROPOSED classification: a documented, deterministic approximation — not the kind of
"questionable value" that principle was built to prevent.** The principle's own precedent cases
(ISP/NAI's Revenue, Phase 6.3's Net Income stmt-misclassification) are all situations where the
*wrong* concept was at risk of silently substituting for the *right* one, or where a source
concept is **structurally different** from the canonical target (a bank's interest income is not
economically the same thing as corporate Revenue). Here, by contrast, `Total Equity (incl NCI)`
is the *same underlying quantity* (total book equity) at a slightly different level of
consolidation — a real, if imprecise, proxy, not a category error. The Altman-Z fallback
(`22__derived_metrics.py:1348-1358`) already treats this exact kind of approximation as acceptable
for a comparable purpose, in code the repo owner has already reviewed and merged.

**That said — this is a real, unresolved imprecision, not a closed question.** Two enhancements
are **PROPOSED, not decided or implemented** this pass, should the repo owner want to close the
labeling gap in the future: (1) a boolean/enum "provenance" column on `financials`
(e.g. `is_approximated` or `source_concept`) so a consumer could distinguish an exact attributable
figure from an incl-NCI proxy; (2) surfacing that distinction in `fundamentals_screener`'s display
(e.g. a footnote marker). **Neither is implemented, scoped, or recommended for immediate action**
— they would meaningfully widen this phase's footprint (schema change, `51`/`52`/frontend touch),
explicitly out of scope per this phase's own non-goals.

---

## B8. Generic architecture (Part 8)

**Does the recommended design depend on ticker/country/market?** **No — confirmed by
construction.** The rule ("prefer the direct concept; fall back to the broader one only when the
direct one is absent for that exact key; never discard the broader one when both exist") is
expressed purely in terms of **canonical concepts and their relationship** (`Total Stockholders
Equity` = primary, `Total Equity (incl NCI)` = fallback source), with no `if market == "EU"` or
`if ticker in (...)` branch anywhere. It happens to resolve correctly for both real population
shapes found in this investigation — EU's "always both" case (§B3) and the 59-ticker US "only
broad" case (§B2) — *because* it's generic, not despite being generic. This satisfies Part 8's
explicit preference for a market-agnostic mechanism, and no evidence gathered this pass showed a
need to deviate from that preference.

---

## B9. Regression analysis for the recommended design (Part 9)

**VERIFIED counts, real production data, read-only:**

| | Count |
|---|---|
| US/Canada tickers where `Total Stockholders Equity`'s *value* changes | **0** — by construction, the fallback rule for this concept is unchanged from today |
| US/Canada tickers where a NEW `Total Equity (incl NCI)` row appears | **up to 1,675** (every ticker with a real raw fact for it — the 59 single-tag ones already show this value under `Total Stockholders Equity` today; the other ~1,616 "both exist" tickers would see it appear as a genuinely new, previously-invisible row) |
| EU tickers where a NEW `Total Equity (incl NCI)` row appears | **7** (FCC, ALO, IBE, SGO, FCT, NAI, RAND) |
| EU tickers affected at all | 7 of 8 (ISP has neither tag) |
| Rows that would become NULL | **0** |
| Rows whose value would change | **0** |
| Downstream metrics affected | **0 formula changes** — every consumer in §B1 keeps reading `Total Stockholders Equity` exactly as it does today; none of them currently reads `Total Equity (incl NCI)` |

**The one real, non-trivial consequence worth flagging explicitly**: this fix's blast radius, done
generically (per Part 8's own instruction not to scope it to EU only), is **much larger than the
8 EU tickers Phase 6.4 originally found** — roughly 1,675 US/Canada tickers would newly show a
`Total Equity (incl NCI)` row too. This is additive and non-destructive (§B9's own count table),
but it is a real, sizeable increase in published data volume the repo owner should be aware of
before authorizing implementation, and it means the eventual validation pass (Part 7 of the
section above, still not run) should check a representative US/Canada sample **in addition to**
the 8 EU tickers, not just the EU set.

**Known, separate, already-flagged limitation, unaffected by this fix**: `concept_hierarchy.json`
has no entry for `"Total Equity (incl NCI)"` (Phase 6.4 §8.1) — so even after this fix lands,
the concept would not render in `fundamentals_screener` for EU *or* the newly-surfaced US/Canada
tickers without a separate, later registration. Not addressed here — explicitly out of this
phase's scope (no `fundamentals_screener`/display changes).

---

## B10. Recommendation (Part 10) — ONE chosen architecture

**Recommended: Option A (keyed two-pass merge), generic, source-agnostic.**

1. **Canonical semantics**: `"Total Stockholders Equity"` = equity attributable to the parent/
   shareholders (unchanged definition). `"Total Equity (incl NCI)"` = total equity including
   non-controlling interests (unchanged definition). Both remain independent, first-class
   canonical concepts in `STATEMENTS`/`financials` — no schema change.
2. **Fallback semantics**: `"Total Equity (incl NCI)"` may fill the `"Total Stockholders Equity"`
   canonical slot **only** for an exact `(ticker, stmt, fy)` key that has **no** direct
   `"Total Stockholders Equity"` fact at all. This is a per-key, existence-conditioned fallback,
   not a blanket rename.
3. **Precedence**: direct `"Total Stockholders Equity"` fact always wins when present (unchanged
   from today).
4. **Both exist**: both concepts survive as two independent rows in `financials` — `"Total
   Stockholders Equity"` keeps the direct value; `"Total Equity (incl NCI)"` is no longer
   discarded.
5. **Only fallback exists**: `"Total Stockholders Equity"` = the incl-NCI value (identical to
   today's actual behavior for the 59 known tickers — §B2); `"Total Equity (incl NCI)"` *also*
   independently appears as its own row (new).
6. **Neither exists**: both NULL (unchanged).
7. **NCI interaction**: none introduced by this fix. The accounting identity (§B4) is not derived
   or enforced by any new code — both values simply come from their own real, independent source
   facts. A standalone `"Non-Controlling Interest"` canonical concept remains a separate, deferred
   Tier A candidate (Phase 6.4 §12), not part of this recommendation.
8. **Downstream metric behavior**: zero changes required. Every one of the eight `22`/two `23`
   consumers in §B1 continues reading `"Total Stockholders Equity"` unmodified — its values are
   identical before and after this fix for every ticker in the current dataset.
9. **Migration strategy**: remove the `CONCEPT_SYNONYMS` entry for this pair; add the keyed
   fallback as new logic in `21__clean_and_merge.py` (distinct from, not an extension of, the
   existing blanket-rename synonym loop — that loop remains exactly as-is for every *other*
   synonym, including Net Income's, which is intentionally out of scope); remove or repurpose the
   now-vestigial `CONCEPT_PRIORITY` entries for these two labels as part of the same change, since
   nothing will compete under a shared key for them anymore.
10. **Regression risk**: LOW for correctness (§B9: zero value changes, zero new NULLs), MODERATE
    for *scope* (§B9: ~1,675 US/Canada tickers gain a new row, not just the 8 EU ones) — a real,
    manageable, additive consequence to plan validation around, not a reason to narrow the fix to
    EU-only (which Part 8 explicitly discourages and which would reintroduce a market-specific
    branch this evidence doesn't justify).

**Why this beats simply deleting the synonym**: deleting it outright (the original Phase 6.5 plan)
would have silently NULLed `"Total Stockholders Equity"` — and every metric in §B1 — for the 59
real tickers in §B2, a severe regression this investigation caught before implementation. The
recommended design achieves the original goal (EU keeps both concepts distinctly) **without**
that regression, because it reproduces §B2's existing fallback behavior exactly rather than
removing it.

---

## Final Report (Phase 6.5b)

- **Classification: NEEDS ARCHITECTURAL DECISION → RESOLVED to a single recommendation
  (Option A). Implementation is not authorized by this document — it remains a decision for the
  repo owner to approve before any code is written.**
- **Recommended architecture**: keyed two-pass merge (direct-wins, existence-conditioned
  fallback, incl-NCI always independently preserved) — §B10.
- **Why**: the only design evaluated that satisfies all three hard constraints simultaneously —
  EU keeps both concepts, the 59 real US/Canada tickers see zero change, and the mechanism is
  generic (no market/ticker branching) — §B5-B8.
- **Fallback rule**: `Total Equity (incl NCI)` fills `Total Stockholders Equity` only for keys
  with no direct fact; never overwrites a direct fact when one exists.
- **Both-concepts rule**: both survive as independent rows whenever both raw facts exist.
- **NCI handling**: not introduced by this fix; deferred to Phase 6.4's separate Tier A
  candidate.
- **US/Canada impact**: zero value changes to `Total Stockholders Equity` for any ticker; up to
  1,675 tickers gain a new, previously-discarded `Total Equity (incl NCI)` row.
- **EU impact**: 7 of 8 issuers gain a distinct, correct `Total Equity (incl NCI)` row; zero
  change to their existing `Total Stockholders Equity` values.
- **Downstream impact**: zero formula changes in `22__derived_metrics.py`/`23__intrinsic_value.py`
  — confirmed by direct code trace, §B1.
- **Production writes: NO**
- **Code changes: NO**
- **Tier A started: NO**
- **Website changed: NO**
- **Next step**: if this recommendation is approved, implement it as Phase 6.5's actual code
  change (scoped to `21__clean_and_merge.py` + the `CONCEPT_SYNONYMS`/`CONCEPT_PRIORITY` config
  entries, per §B10 item 9), with tests covering both population shapes (EU "always both," the 59
  US "only broad") and a validation pass across a representative US/Canada sample in addition to
  the 8 EU tickers, given the larger-than-EU blast radius §B9 identified. Not started in this
  document, per instruction.

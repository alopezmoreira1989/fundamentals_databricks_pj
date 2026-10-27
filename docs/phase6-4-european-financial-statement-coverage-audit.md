# Phase 6.4 — European Financial-Statement Coverage Audit

**READ-ONLY audit. No production code was modified, no notebook was run, no table was
written, no mapping was implemented, nothing was published.** One genuine pipeline finding was
discovered during this audit (§8.1) — per instruction, it is documented, not fixed.

Every claim below is labeled **VERIFIED** (obtained directly, this pass, from real production
data or a live re-fetch of real ESEF/XBRL filings), **CITED** (VERIFIED by the prior Phase 6.0/
6.1 research — re-confirmed spot-checks are noted; not blindly copied), or **INFERRED**/
**UNKNOWN** where the evidence doesn't fully resolve a question.

**Relationship to prior work**: `docs/phase6-european-esef-financial-coverage.md` (Phase 6.0
research + Phase 6.1 implementation, 2026-07/08) already performed an extremely thorough,
real-data-grounded version of most of this audit — extracting the real 605-concept universe
across all 8 issuers, building a coverage matrix, classifying Tier 1/2/3 candidates, and
implementing+validating the Tier 1 set (20 concepts) live. This document does **not** repeat
that work; it (a) re-confirms the parts of it still accurate after Phase 6.3, (b) extends real
evidence to concepts that document's own matrix did not cover (SG&A, Accounts Payable, ST/LT
Debt, Common Stock, SBC, ΔWC components, NCI-as-BS-line, Debt Issuance/Repayment, Share
Repurchases, Equity Issuance), and (c) uses the required A–I categorization and Tier A–D ranking
this prompt specifies, which the prior document did not use.

---

## 1. Executive Summary

**The canonical model has 21 EU-mapped concepts today (20 in the Tier 1 set + Revenue's second
tag), all correctly implemented — but one of them, "Total Equity (incl NCI)", is dead on
arrival: it is correctly extracted into `financials_raw` for 7/8 issuers, but a pre-existing,
SEC-inherited `CONCEPT_SYNONYMS` rule silently collapses it into "Total Stockholders Equity"
during `21__clean_and_merge.py`'s merge, so it has never once appeared as its own row in the
clean `financials` table for any EU issuer, despite being reported as "IMPLEMENTED" in Phase
6.1. This is the one new pipeline finding of this audit** (§8.1, Tier D). It is not a
data-corruption bug like Phase 6.3's — no wrong value is shown — but the concept simply does not
exist downstream the way it was believed to.

**Beyond that, the picture is exactly what Phase 6.0/6.1 already found and this audit confirms
with fresh real data**: European statements are genuinely, structurally sparser than US filings
— not because of a mapping gap in most cases, but because IFRS "by nature" expense presentation
(common among 6 of the 8 issuers) does not produce a Cost of Revenue/Gross Profit/SG&A/R&D
breakdown at all, the same way a "by function" US filer's does. This audit found real, previously
uncatalogued evidence for several concepts Phase 6.0 did not check: **SG&A is never a single combined figure for any issuer — where present at all
it's split into separate Selling and Administrative Expense tags (both present for ALO and RAND;
ISP has only the Administrative side, further split into bank-specific staff-cost/other
sub-components)**; **Accounts Payable is real and clean for 7/8 issuers**,
a strong new Tier A candidate; **Non-Controlling Interests (balance sheet) is real for 8/8
issuers** with no canonical home at all today; **Stock-Based Compensation is real for 6/8**;
**Change in Working Capital is real and directly tagged for 5/8**. All are documented in §10's
matrix with real values.

**Free Cash Flow remains uncomputable for all 8 issuers today** — Operating Cash Flow is 8/8
mapped, but CapEx (the other required input) remains unmapped (a known, already-documented Tier
2 deferral, unchanged since Phase 6.0).

**Phase 6.3's Net Income fix is holding correctly** (§9): all 8 issuers show `stmt="Income
Statement"`, zero stale `Cash Flow` duplicates, in the current production `financials` table —
directly re-verified this pass. The downstream ratios Phase 6.0/6.1 flagged as "pending a future
`22`/`23` run" are now real (this session's Phase 6.3 work ran them) — Current Ratio, Operating
Margin %, Goodwill/Total Assets %, Interest Coverage, Tangible Book Value all have real,
multi-year values for 4-6 of 8 issuers today, confirmed by direct query. The live public website
still shows stale, pre-fix data (§9.3) — unchanged, still cron-refresh-pending, not a new issue.

---

## 2. Scope and Methodology

Real data was gathered three ways this pass:

1. **Fresh live re-fetch of real xBRL-JSON** for all 8 issuers, using this repo's own,
   unmodified `fundamentals_pipeline.sources.eu_current.extract_source_facts()`/
   `select_filing_for_period()` — the exact production parsing logic — run locally (pure Python,
   no Spark/Databricks needed) against `filings.xbrl.org`'s live API. Same filings, same fact
   counts as Phase 6.0's own §4 table (FCC 142, ALO 127, IBE 180, SGO 137, FCT 128, NAI 95, RAND
   125, ISP 202 consolidated current-period facts) — confirms these issuers have filed nothing
   new since Phase 6.0, and this pass's evidence is genuinely live, not stale.
2. **Direct read-only SQL** against the real production `main.financials.financials`/
   `financials_raw`/`financials_metrics` tables (via the Databricks SQL Statement Execution API)
   — reflecting the CURRENT state, after Phase 6.1's Tier 1 mapping AND this session's Phase 6.3
   fix + full `22`/`23` recomputation, which Phase 6.0/6.1's own snapshots predate.
3. **Direct code reading** of `01__tickers.py`, `16__fetch_eu_xbrl.py`, `21__clean_and_merge.py`,
   `sources/eu_current.py`, `22__derived_metrics.py`, `fundamentals_screener`'s
   `CompanyRepository.get_statements()`, and `concept_hierarchy.json`.
4. **One live website fetch** (FCC's public page) to re-confirm the Phase 6.3 cache-lag finding
   is unchanged, per §9's instruction not to re-fix it.

No table was written to. No notebook was run. No mapping was implemented.

---

## 3. Current Canonical Model

**VERIFIED** (direct code read, `fundamentals_pipeline/sources/eu_current.py:235-497`).
`EU_CANONICAL_MAPPING` maps **21 canonical concepts via 22 accepted source tags** (Revenue has
two mutually-exclusive tags):

| # | Canonical concept | Source tag(s) |
|---|---|---|
| 1 | Revenue | `ifrs-full:Revenue` OR `ifrs-full:RevenueFromContractsWithCustomers` |
| 2 | Net Income | `ifrs-full:ProfitLossAttributableToOwnersOfParent` |
| 3 | Net Income (incl NCI) | `ifrs-full:ProfitLoss` |
| 4 | Total Assets | `ifrs-full:Assets` |
| 5 | Cash & Equivalents | `ifrs-full:CashAndCashEquivalents` |
| 6 | Income Tax | `ifrs-full:IncomeTaxExpenseContinuingOperations` |
| 7-9 | Operating/Investing/Financing Cash Flow | `ifrs-full:CashFlowsFromUsedIn{Operating,Investing,Financing}Activities` |
| 10 | Total Stockholders Equity | `ifrs-full:EquityAttributableToOwnersOfParent` |
| 11 | Total Equity (incl NCI) | `ifrs-full:Equity` (**see §8.1 — never survives into `financials`**) |
| 12 | PP&E Net | `ifrs-full:PropertyPlantAndEquipment` |
| 13 | Total Current Assets | `ifrs-full:CurrentAssets` |
| 14 | Total Current Liabilities | `ifrs-full:CurrentLiabilities` |
| 15 | Goodwill | `ifrs-full:Goodwill` |
| 16 | Interest Expense | `ifrs-full:FinanceCosts` (bare `InterestExpense` deliberately excluded — zero real occurrences) |
| 17 | Dividends Paid | `ifrs-full:DividendsPaid` |
| 18 | Operating Income | `ifrs-full:ProfitLossFromOperatingActivities` |
| 19 | Intangible Assets | `ifrs-full:IntangibleAssetsOtherThanGoodwill` |
| 20 | Inventory | `ifrs-full:Inventories` |
| 21 | Cost of Revenue | `ifrs-full:CostOfSales` |

`map_source_fact_to_canonical()` returns `None` (excluded outright) for anything else —
confirmed still a narrow allow-list, not a broad taxonomy guess.

**The full SEC/Canada-side canonical model** (`01__tickers.py`'s `INCOME_STATEMENT`/
`BALANCE_SHEET`/`CASH_FLOW`) is unchanged since Phase 6.0's own §3 inventory — re-confirmed by
direct grep this pass (line numbers cited in §8.1 below reflect this pass's own reading, current
as of this audit). **Nothing in this audit found a need to change that inventory** — same
conclusion as Phase 6.0 §14.

**`fundamentals_screener` rendering** (`CompanyRepository.get_statements()`,
`repositories/companies.py:531-571`): **VERIFIED**, re-read this pass — confirmed fully generic,
no per-concept allowlist. It selects `stmt, section, "group", concept, display_name, sort_order,
fiscal_year, value FROM dashboard_data WHERE ... stmt IS NOT NULL AND value IS NOT NULL` and
renders whatever rows come back — `section`/`group`/`sort_order`/`display_name` are joined in at
**export** time (`51__export_dashboard_data.py`, not re-read line-by-line this pass) against
`concept_hierarchy.json`. A concept **VERIFIED** absent from `concept_hierarchy.json` (§8.1) will
not receive a `display_name`/`section`/`sort_order` at export — whether that's a silent drop or a
malformed row depends on whether `51`'s own join is INNER or LEFT, which this pass did not
re-verify (the point is moot for "Total Equity (incl NCI)" specifically, since it never reaches
`financials` in the first place — see §8.1).

---

## 4. Income Statement Coverage

Legend: **✓** mapped & present in current `financials` · **M** present in real filing, not
mapped · **A** aggregated with other line(s), no clean standalone figure · **N** not disclosed
separately (real, structural absence) · **S** present but different semantics/granularity than
the canonical concept · **I** industry-structural — concept doesn't apply the same way · **P**
pipeline problem (mapped, but lost/misrouted downstream) · **U** UI/export problem.

| Concept | FCC | ALO | IBE | SGO | FCT | NAI | RAND | ISP | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| Revenue | ✓ | ✓ | ✓ | ✓ | ✓ | S | I | S/I | **VERIFIED** current `financials`: 6/8 present. NAI: real tag is `RentalIncomeFromInvestmentProperty` — a different IFRS concept (rental income, not contract revenue), correctly left unmapped (CITED, Phase 6.0 §5c). ISP: real tag is `isp:InterestIncomeAndSimilarRevenues` (extension) — a bank's top line is structurally not the same accounting concept (CITED). |
| Cost of Revenue / COGS | N | ✓ | N | ✓ | N | I | ✓ | I | **VERIFIED**: `financials` has it for ALO/RAND/SGO only (3/8, `CostOfSales` tag). FCC/IBE/FCT genuinely present the income statement "by nature," no COGS subtotal exists in their real filings (CITED, Phase 6.0 §5a). |
| Gross Profit | N | N | M | N | N | I | M | I | **VERIFIED, fresh fetch**: `ifrs-full:GrossProfit` real for IBE (€23.876B) and RAND (€4.309B) only — unmapped. Others genuinely don't present this subtotal (by-nature presentation). |
| SG&A (combined) | N | S | N | N | N | I | S | I | **VERIFIED, fresh fetch — new evidence, not in Phase 6.0's matrix.** No issuer reports one combined SG&A tag; ALO/RAND (and ISP) split it into Selling + Administrative (below) — different granularity than a single canonical "SG&A Expense" concept. |
| Selling Expense | N | M | N | N | N | N | M | N | **VERIFIED, fresh fetch**: `ifrs-full:SellingExpense` real for ALO (€359M), RAND (€2.379B). |
| Administrative Expense | N | M | N | N | N | N | M | M | **VERIFIED, fresh fetch**: `ifrs-full:AdministrativeExpense` real for ALO (€671M), RAND (€1.418B + an own-extension "Other G&A" €1.335B), ISP (€11.579B, split further into staff-cost/other-extension sub-tags — bank-specific, see §7). |
| R&D Expense | N | M | N | N | N | N | N | I | **VERIFIED, fresh fetch, confirms Phase 6.0**: `ifrs-full:ResearchAndDevelopmentExpense` real for ALO only (€623M), 1/8. |
| Depreciation & Amortisation (IS line) | M | A | A | A | A | N | A | A | **VERIFIED, fresh fetch**: FCC has a clean, direct `DepreciationAndAmortisationExpense` (€635M). All 7 others fold D&A into a Cash Flow reconciliation *adjustment* line (often combined with impairment) — real, not a mapping oversight (CITED, Phase 6.0 §9, "LOW confidence for a clean IS line across the universe"). |
| Operating Expenses (total, one line) | N | N | N | N | N | M | M | I | **VERIFIED, fresh fetch**: `ifrs-full:OperatingExpense` real for RAND (€3.797B); NAI has it but immaterial (€2.163M) plus a real-estate-specific direct-operating-expense line. |
| Operating Income | ✓ | ✓ | ✓ | N | N | ✓ | ✓ | I | **VERIFIED** current `financials`: 5/8 (FCC, ALO, IBE, RAND, NAI). SGO/FCT don't present this exact IFRS subtotal (real, structural). |
| Other Operating Income | M | A | M | M | N | N | N | M | **VERIFIED, fresh fetch**: real, unmapped for FCC/IBE/SGO/ISP (separate tags); ALO uses one *net* combined income/expense tag (A). |
| Other Operating Expense | M | A | — | N | N | N | N | A | Same evidence as above — most issuers present net, not gross-split; FCC has a genuinely separate expense-side tag. |
| Interest Income | N | N | N | M | N | N | N | N | **VERIFIED, fresh fetch — new evidence.** Only SGO tags a narrow, genuinely distinct `InterestIncomeOnCashAndCashEquivalents` (€193M); no other issuer isolates interest income from the broader "Finance Income" figure. |
| Finance Income | M | M | M | S | M | A | M | I | **VERIFIED, fresh fetch, confirms/extends Phase 6.0 §5b**: real 5/8 (FCC, ALO, IBE, FCT, RAND), unmapped. SGO only has the narrower interest-on-cash variant (S). NAI's is net-combined with costs (A). ISP: bank-specific extension tag (I). |
| Finance Costs / Interest Expense | ✓ | ✓ | ✓ | S | ✓ | N | ✓ | I | **VERIFIED** current `financials`: 5/8 mapped (`FinanceCosts`). SGO tags a more granular split (`InterestExpenseOnBorrowings` + `...OnLeaseLiabilities`) — real, different semantics, would need summing (S). NAI genuinely absent. ISP: extension (I). |
| Pretax Income | M | M | M | N | M | M | M | M | **VERIFIED, fresh fetch, confirms Phase 6.0**: `ProfitLossBeforeTax` real for 7/8 (all but SGO) — unmapped, HIGH-confidence Tier 2 candidate. |
| Income Tax | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **VERIFIED** current `financials`: 8/8. |
| Net Income (parent-attributable) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **VERIFIED** current `financials`, post-Phase-6.3-fix: 8/8, all `stmt="Income Statement"` — see §9. |
| Net Income (incl NCI) | ✓* | ✓* | ✓* | ✓* | ✓* | ✓* | ✓* | ✓* | Real 8/8 at raw-fact level; correctly folded into the "Net Income" canonical slot by `CONCEPT_SYNONYMS` (by design, same mechanism as SEC) — never a separate visible row, which is the *intended* behavior, unlike §8.1's finding. |
| Net Income Attrib. to NCI | M | M | M | M | N | M | M | M | **VERIFIED, fresh fetch — new evidence.** `ProfitLossAttributableToNoncontrollingInterests` real for 7/8 (all but FCT). **No canonical concept exists for this at all** — not even for SEC filers (cross-cutting gap, like Deferred Tax Assets/Liabilities in Phase 6.0 §18). |
| EPS Basic / Diluted | M | M | M | M | M | M | M | M | **VERIFIED, fresh fetch, confirms Phase 6.0 §10**: real 8/8 (IBE via the `FromContinuingOperations` variant). Unmapped — HIGH confidence, 2-tag fallback needed. |
| Dividends (paid, on IS side — see Cash Flow for the mapped concept) | — | — | — | — | — | — | — | — | Dividends are a Cash Flow concept in this canonical model (already mapped, §5). |

---

## 5. Balance Sheet Coverage

| Concept | FCC | ALO | IBE | SGO | FCT | NAI | RAND | ISP | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| Cash & Equivalents | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **VERIFIED** current `financials`: 8/8. |
| Short-Term Investments | — | — | — | — | — | — | — | — | No canonical concept exists at all (cross-cutting; not investigated in real EU data this pass — **UNKNOWN**). |
| Accounts Receivable | M | M | M | M | M | M | M | N | **VERIFIED, fresh fetch, confirms Phase 6.0 §5b (a real, still-open bug in the SHARED canonical fallback list)**: real 7/8 via two tags (`CurrentTradeReceivables`: FCC/ALO/SGO/NAI; `TradeAndOtherCurrentReceivables`: IBE/FCT/RAND). The *existing* `IFRS_FALLBACK_TAGS["Accounts Receivable"]` value (`TradeReceivables`) matches **0/8** real filings — this is a pre-existing tag-string error, not merely a missing EU entry. ISP: absent (bank-structural). |
| Inventory | N | ✓ | ✓ | ✓ | N | I | I | I | **VERIFIED** current `financials`: 3/8 (ALO, IBE, SGO). FCC/FCT: real, structural absence (construction/shipbuilding project accounting typically doesn't carry conventional inventory). RAND: staffing company, no inventory concept applies (I). NAI: real estate (I). |
| Other Current Assets | — | — | — | — | — | — | — | — | No canonical concept exists (cross-cutting; **UNKNOWN**, not investigated). |
| Total Current Assets | ✓* | ✓* | ✓ | ✓ | N | ✓ | ✓ | I | **VERIFIED** current `financials`: 5/8 full-history (FCC, IBE, NAI, RAND, SGO); ALO has it for only 1 of its 4 fiscal years on record (real per-year presentation sparseness, marked ✓* to flag the partial history). FCT genuinely doesn't present a current/noncurrent split. ISP: bank balance sheet has no current/noncurrent structure (I). |
| PP&E Net | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | I | **VERIFIED** current `financials`: 7/8. ISP: no comparable PP&E line at bank scale (I). |
| Intangible Assets | N | ✓ | ✓ | ✓ | N | N | N | I | **VERIFIED** current `financials`: 3/8 (ALO, IBE, SGO). Others: real absence, most likely folded into Goodwill or PP&E in their own presentation (not independently confirmed which, this pass). |
| Goodwill | ✓ | ✓ | ✓ | ✓ | N | N | ✓ | N | **VERIFIED** current `financials`: 5/8. FCT/NAI: genuinely no goodwill on the balance sheet (no historical M&A premium, or organically grown). ISP: absent this pass — **UNKNOWN** whether this reflects a real zero-goodwill position or a presentation difference; not independently confirmed. |
| Investments (financial assets) | — | — | — | — | — | — | — | — | No canonical concept exists (cross-cutting; **UNKNOWN**). |
| Other Non-Current Assets | — | — | — | — | — | — | — | — | No canonical concept exists (cross-cutting; **UNKNOWN**). |
| Total Assets | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **VERIFIED** current `financials`: 8/8. |
| Accounts Payable | M | M | M | M | M | M | M | N | **VERIFIED, fresh fetch — new evidence, not checked by Phase 6.0's own matrix.** Real, clean 7/8 via `TradeAndOtherCurrentPayablesToTradeSuppliers` (FCC/ALO/SGO/NAI) or the broader `TradeAndOtherCurrentPayables` (IBE/FCT/RAND). Unmapped — **no canonical fallback exists for this concept for ANY filer type yet** (cross-cutting gap, strong real evidence). ISP: absent (bank-structural). |
| Short-Term Debt | N | M | N | M | M | M | M | N | **VERIFIED, fresh fetch — new evidence.** Real, clean BS balance for ALO/SGO/FCT(via CF proxy only)/NAI/RAND via `ShorttermBorrowings`/`CurrentBorrowings`-family tags (5/8, genuine BS balances for 4, FCT's evidence is CF-flow-only). FCC/IBE/ISP: no distinct ST-debt line found. |
| Other Current Liabilities | — | — | — | — | — | — | — | — | No canonical concept exists (cross-cutting; **UNKNOWN**). |
| Total Current Liabilities | ✓* | ✓* | ✓ | ✓ | N | ✓ | ✓ | I | Same pattern as Total Current Assets — **VERIFIED** current `financials`. |
| Long-Term Debt | N | M | N | M | N | M | M | N | **VERIFIED, fresh fetch, confirms Phase 6.0**: real, clean 4/8 direct BS balance via `LongtermBorrowings` (ALO, SGO, NAI, RAND). FCC/IBE/ISP: absent. |
| Other Non-Current Liabilities | — | — | — | — | — | — | — | — | No canonical concept exists (cross-cutting; **UNKNOWN**). |
| Total Liabilities | N | N | N | N | N | M | M | N | **VERIFIED, fresh fetch, confirms Phase 6.0**: real, sparse 2/8 direct tag (`ifrs-full:Liabilities`: NAI, RAND). The other 6 present `EquityAndLiabilities` and derive Liabilities implicitly (Assets − Equity) — a real presentation choice; **safely derivable today** for any ticker with both Total Assets and Total Equity (incl NCI) mapped — see §12 Tier B. |
| Common Stock / Share Capital | M | N | M | N | M | M | M | M | **VERIFIED, fresh fetch, confirms Phase 6.0**: real, clean 6/8 via `ifrs-full:IssuedCapital` (FCC, IBE, FCT, NAI, RAND, ISP). ALO/SGO absent this specific tag. |
| Retained Earnings | N | N | N | N | A | ✓ | A | N | **VERIFIED, fresh fetch, confirms Phase 6.0 §12**: real, direct tag for NAI only (already mapped and present, 1/8). FCT/RAND fold it into a combined "Reserves and Retained Earnings" extension figure (A). The other 5: genuinely not disclosed as an isolated line — Tier C, do not implement broadly. |
| Other Equity / Reserves | A | A | A | N | A | A | A | A | **VERIFIED, fresh fetch — new evidence.** Real but fragmented across issuer-specific combinations of `SharePremium`/`TreasuryShares`/various `Reserves` tags — no single, consistent "Other Equity" concept across issuers. Tier C — do not force a single mapping. |
| Total Stockholders Equity | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | I | **VERIFIED** current `financials`: 7/8. ISP: bank equity structure not comparable (I). |
| Non-Controlling Interests (BS) | M | M | M | M | M | M | M | M | **VERIFIED, fresh fetch — new evidence, real and universal.** `ifrs-full:NoncontrollingInterests` present for **8/8** issuers, unmapped. **No canonical concept exists for a standalone NCI balance-sheet line for ANY filer type** — a genuine, strong, cross-cutting Tier A/B candidate (real evidence is stronger than several already-implemented Tier 1 concepts). |
| Total Equity (incl NCI) | **P** | **P** | **P** | **P** | **P** | **P** | — | I | **VERIFIED this pass — see §8.1.** Mapped in `EU_CANONICAL_MAPPING`, real and present in `financials_raw` for 7/8 issuers (all but ISP), but **zero rows in clean `financials` for any issuer** — silently collapsed by `CONCEPT_SYNONYMS` into "Total Stockholders Equity" during merge. Not a wrong-value bug (the winning value is correct), but the concept the mapping claims to unlock does not exist downstream. |
| Total Liabilities + Equity | M | M | M | M | M | M | M | M | **VERIFIED, fresh fetch**: real, clean, universal (`ifrs-full:EquityAndLiabilities`, 8/8) but unmapped. Mathematically identical to Total Assets by the accounting identity (already 8/8 mapped) — Tier C, no incremental value. |

---

## 6. Cash Flow Coverage

| Concept | FCC | ALO | IBE | SGO | FCT | NAI | RAND | ISP | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| Net Income (CF reconciliation start) | N* | N* | N* | N* | N* | N* | N* | N* | **VERIFIED, confirms Phase 6.3's fix is working as designed** (§9) — no genuine second XBRL context exists for EU the way SEC has, so this line's absence under Cash Flow is *correct*, not a gap. |
| Depreciation & Amortisation (CF adjustment) | M | A | A | A | A | N | A | A | Same evidence as the Income Statement row (§4) — this is the same underlying fact, presented once. |
| Stock-Based Compensation | N | M | M | M | M | N | M | M | **VERIFIED, fresh fetch — new evidence, not checked by Phase 6.0.** Real, clean 6/8 via `IncreaseDecreaseThroughSharebasedPaymentTransactions` or `AdjustmentsForSharebasedPayments` (ALO, IBE, SGO, FCT, RAND, ISP). Strong Tier A/B candidate — no canonical fallback exists for any filer type yet. |
| Change in Working Capital | M | M | N | N | M | M | M | N | **VERIFIED, fresh fetch — new evidence.** Real, clean, directly-tagged 5/8 via `ifrs-full:IncreaseDecreaseInWorkingCapital` (FCC, ALO, FCT, NAI, RAND) — a single, unambiguous IFRS concept, HIGH confidence. |
| Operating Cash Flow | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **VERIFIED** current `financials`: 8/8. |
| CapEx (PPE purchases) | M | A | M | A | M | N | A | M | **VERIFIED, fresh fetch, confirms Phase 6.0 §9**: real, clean, direct-tag-matching-the-existing-fallback for 4/8 (FCC, IBE, FCT, and ISP's intangibles-only variant). ALO/SGO/RAND use combined PPE+intangibles+other or issuer-extension tags (A) — genuinely broader/different scope than the SEC-side PPE-only definition. Still Tier B (MEDIUM confidence, per Phase 6.0), unimplemented. |
| Investing Cash Flow | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **VERIFIED** current `financials`: 8/8. |
| Dividends Paid | ✓ | ✓ | N | ✓ | ✓ | ✓ | N | ✓ | **VERIFIED** current `financials`: 6/8. IBE/RAND genuinely absent (not paid, or not tagged, that fiscal year). |
| Debt Issuance | M | A | N | S | N | M | S | N | **VERIFIED, fresh fetch — new evidence.** Real for FCC/NAI (broad umbrella tag, not ST/LT split); ALO reports a single net issuance-minus-repayment figure (A); SGO/FCT/RAND split Proceeds into Current + Noncurrent (S — more granular, would need summing). |
| Debt Repayment | N | A | N | S | N | M | S | N | Same evidence pattern as Debt Issuance. |
| Share Repurchases | N | N | N | N | N | N | N | N | **VERIFIED, fresh fetch**: genuinely absent for all 8 issuers in the specific fiscal year checked — no buyback activity found, or not tagged this way. Not conclusively investigated across multiple years — flagged **UNKNOWN** whether this is a real, consistent pattern or a single-year artifact. |
| Equity Issuance | N | A | N | N | M | N | N | N | **VERIFIED, fresh fetch**: real, clean for FCT only (`ProceedsFromIssuingShares`, €388.9M). ALO reports a net combined figure (A). |
| Financing Cash Flow | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **VERIFIED** current `financials`: 8/8. |
| Free Cash Flow (derived, not an XBRL fact) | N | N | N | N | N | N | N | N | **VERIFIED**: `22__derived_metrics.py`'s formula (`Operating Cash Flow − CapEx`) needs both inputs. OCF is 8/8 mapped; CapEx is 0/8 mapped (still Tier 2). **FCF is not computable for any of the 8 issuers today** — entirely gated on the CapEx Tier 2 decision, unchanged since Phase 6.0. |

---

## 7. Industry-Specific Findings

**ISP (bank)** and **NAI (real-estate investment company)** are treated separately per instruction
— **CITED**, re-confirmed this pass with the fresh fetch:

- **ISP**: its real top line (`isp:InterestIncomeAndSimilarRevenues`, €13.232B) and cost-side
  equivalent (`isp:InterestExpenseAndSimilarCharges`, €3.547B) are issuer-extension concepts, not
  standard `ifrs-full` tags — correctly excluded from Revenue/Interest Expense mapping. Its real
  Administrative Expense is itself split into bank-specific sub-components
  (`isp:AdministrativeExpenseStaffCosts` €6.793B / `isp:OtherAdministrativeExpense` €4.786B) —
  a genuinely different cost structure than a corporate SG&A line. Inapplicable canonical
  concepts for ISP, confirmed this pass: Cost of Revenue, Gross Profit, Inventory, PP&E (at
  material scale), Total Current Assets/Liabilities (no current/noncurrent balance-sheet split
  exists for a bank), Total Stockholders Equity (bank capital structure isn't presented the same
  way).
- **NAI**: its real top line (`ifrs-full:RentalIncomeFromInvestmentProperty`) is a genuinely
  different IFRS concept from contract Revenue — correctly excluded. Cost of Revenue/Gross
  Profit/Inventory are inapplicable to a real-estate holding structure the same way they are for
  ISP, for the same underlying reason (no goods/services sale cycle).

**Neither issuer's real top-line concept is proposed for mapping in this document** — same
conclusion as Phase 6.0 §14/§18: a second same-sector issuer would be needed before deciding
whether e.g. `isp:InterestIncomeAndSimilarRevenues` deserves its own "Revenue (bank)" synonym
the way `us-gaap:InterestAndDividendIncomeOperating` already has for SEC filers.

---

## 8. Pipeline / Export / UI Findings

### 8.1 NEW FINDING (Tier D) — "Total Equity (incl NCI)" never reaches `financials`

**VERIFIED this pass, real production data.** `EU_CANONICAL_MAPPING` maps `ifrs-full:Equity` to
canonical concept `"Total Equity (incl NCI)"`, and Phase 6.1's own validation reported this as
"IMPLEMENTED" with "7/8 issuers real coverage." Both of those statements are true only up to the
point of ingestion — the raw fact genuinely reaches `financials_raw`:

```
FCC: 10 rows, ALO: 8, IBE: 9, SGO: 8, FCT: 6, NAI: 6, RAND: 10  (7/8, ISP absent — bank)
```

But **zero rows exist for `concept = "Total Equity (incl NCI)"` in the clean `financials` table,
for any of the 8 issuers, across any fiscal year** — confirmed by direct query. Root cause,
traced to real code: `01__tickers.py`'s `CONCEPT_SYNONYMS` (line ~422) has a pre-existing,
SEC-side rule —

```python
"Total Equity (incl NCI)": "Total Stockholders Equity",
```

— built for `us-gaap` filers reporting both `StockholdersEquity` and
`StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` in the same filing (the
file's own comment: "We prefer the equity attributable to shareholders (StockholdersEquity) for
ROE"). `21__clean_and_merge.py`'s synonym-collapse loop applies this rule to **every** source,
not just SEC — so every EU "Total Equity (incl NCI)" raw row is renamed to "Total Stockholders
Equity" before the merge, then loses the dedup tiebreak against the genuine, direct
"Total Stockholders Equity" fact under `CONCEPT_PRIORITY` (`"Total Stockholders Equity": 0` beats
`"Total Equity (incl NCI)": 1`, line ~464-466) — so it is silently dropped, not written.

**Verified this is not a wrong-value bug**: FCC FY2024 "Total Stockholders Equity" in
`financials` = €2,732,716,000, exactly matching the *direct* `EquityAttributableToOwnersOfParent`
raw fact — the correct, attributable figure is winning, not a corrupted one. Cross-checked
arithmetic consistency: €2,732,716,000 (attributable) + €1,003,303,000
(`NoncontrollingInterests`, §5) = €3,736,019,000, exactly the "Total Equity (incl NCI)" raw
value — confirms both source facts are individually correct; only the "incl NCI" one never
survives as its own row.

**A second, independent confirmation**: `concept_hierarchy.json` (the display-registration file)
has entries for all 12 other Tier 1 concepts spot-checked, but **no entry for "Total Equity
(incl NCI)"** — so even absent the `CONCEPT_SYNONYMS` collision, this concept would not render
in `fundamentals_screener` today regardless.

**Classification: Tier D (bug)** — not a data-corruption risk, but the mapping decision does not
produce the outcome it was believed to. **Minimal fix, not implemented this pass**: either (a)
exempt "Total Equity (incl NCI)" from `CONCEPT_SYNONYMS`'s collapse when the source is EU (a
source-scoped carve-out, more invasive), or (b) accept that this canonical concept is
structurally a same-value-family alias of "Total Stockholders Equity" and remove/re-scope the EU
mapping entry rather than claim it as independently unlocked (simpler, no code risk, just a
documentation/mapping-list correction). This document does not decide between them — flagged for
the repo owner per instruction.

### 8.2 `financials → financials_metrics → dashboard_metrics`: now real (closes a Phase 6.0/6.1 gap)

**VERIFIED this pass.** Phase 6.0/6.1 both explicitly flagged that Current Ratio/Operating
Margin %/Tangible Book Value/Goodwill-Total-Assets %/Interest Coverage were "not yet reflected in
`financials_metrics`, pending a future `22`/`23` run." That run has since happened — this
session's Phase 6.3 work ran `22`/`23` live, full-universe, twice (idempotency-checked). Direct
query confirms real, multi-year values now exist in `financials_metrics` for 4-6 of 8 issuers
per metric (e.g., FCC Current Ratio 1.62 (2024)/1.57 (2023)/1.30 (2020); IBE Interest Coverage
2.46x (2024); RAND Goodwill/Total Assets % 29.3% (2025)). This closes that specific gap — no
further `22`/`23` run is needed for the Tier 1 mapping's own metrics to be live. `51`/`52` also
already ran successfully this session (Phase 6.3), so `dashboard_metrics` and the published
GitHub Release both already carry this data (confirmed `draft: false`, freshly published).

### 8.3 Live website — unchanged, still cache-pending (not re-fixed, per instruction)

**VERIFIED this pass, one live re-check.** FCC's public page still shows the pre-Phase-6.3 stale
figure (€567.6M Net Income under Cash Flow) and none of the newly-live ratios from §8.2. This is
the exact same finding Phase 6.3 already documented and classified (external, cron-refresh
pending, not a pipeline defect) — re-confirmed unchanged, not re-investigated or re-fixed here.

---

## 9. Phase 6.3 Net Income Regression Check

**VERIFIED this pass, direct query against current production `financials`.**

| Check | Result |
|---|---|
| All 8 issuers' Net Income has `stmt = "Income Statement"` | **PASS** — confirmed for FCC, ALO, IBE, SGO, FCT, NAI, RAND, ISP; zero `stmt="Cash Flow"` rows for `concept="Net Income"` anywhere in the current table. |
| No stale `Cash Flow` duplicate remains | **PASS** — the full per-ticker concept listing (§4's underlying query, `SELECT ticker, concept, stmt, COUNT(*) ... GROUP BY`) shows exactly one `stmt` value per (ticker, concept, fiscal_year); this matches the cleanup Phase 6.3 implemented in `21__clean_and_merge.py` §4b. |
| Consolidated/parent-attributable selection remains correct | **PASS** — FCC FY2024 = €429,865,000 (parent-attributable), not €567,584,000 (consolidated); same pattern holds for every issuer checked. |
| Website/cache discrepancy still understood and correctly classified | **PASS, unchanged** — see §8.3. Not fixed here, per this phase's own explicit instruction. |

No regression found. Phase 6.3's fix is holding exactly as validated.

---

## 10. Complete EU Coverage Matrix

The full, per-concept, per-ticker matrix is §§4-6 above (Income Statement / Balance Sheet / Cash
Flow), using the required status legend. It is not repeated here as one combined table given its
size (58 concept rows × 8 tickers) — §§4-6 are the deliverable.

---

## 11. Coverage Scores

**Raw coverage** = `✓` rows ÷ all concept rows checked in §§4-6 for that statement (penalizes for
every N/I/`-` row too, i.e. structural non-applicability counts against the score). **Adjusted
coverage** = `✓` rows ÷ (all rows − rows marked `I` [industry-inapplicable] − rows marked `-`
[no canonical concept exists for any filer type — a cross-cutting gap, not an EU-specific one] −
rows marked `N` [not disclosed separately by that issuer, a real, structural absence]) — i.e.,
the denominator is only concepts that are both a real canonical slot and something the issuer
could plausibly have disclosed. Computed programmatically from §§4-6's own marker data (not by
hand) to avoid the kind of silent arithmetic error this project's own review discipline exists to
catch:

| Ticker | IS raw | IS adjusted | BS raw | BS adjusted | CF raw | CF adjusted |
|---|---|---|---|---|---|---|
| FCC | 6/21 (29%) | 6/13 (46%) | 7/26 (27%) | 7/14 (50%) | 3/13 (23%) | 3/7 (43%) |
| ALO | 7/21 (33%) | 7/18 (39%) | 9/26 (35%) | 9/17 (53%) | 3/13 (23%) | 3/10 (30%) |
| IBE | 6/21 (29%) | 6/13 (46%) | 9/26 (35%) | 9/16 (56%) | 2/13 (15%) | 2/5 (40%) |
| SGO | 5/21 (24%) | 5/12 (42%) | 9/26 (35%) | 9/16 (56%) | 3/13 (23%) | 3/8 (38%) |
| FCT | 5/21 (24%) | 5/9 (56%) | 4/26 (15%) | 4/13 (31%) | 3/13 (23%) | 3/8 (38%) |
| NAI | 4/21 (19%) | 4/10 (40%) | 7/26 (27%) | 7/17 (41%) | 3/13 (23%) | 3/6 (50%) |
| RAND | 6/21 (29%) | 6/16 (38%) | 7/26 (27%) | 7/17 (41%) | 2/13 (15%) | 2/8 (25%) |
| ISP | 3/21 (14%) | 3/11 (27%) | 2/26 (8%) | 2/6 (33%) | 3/13 (23%) | 3/6 (50%) |

**Why these numbers would still be misleading read in isolation, without §§4-6's own detail**:
a large share of the "uncovered" denominator in the adjusted score is `M`
(present-but-unmapped, i.e., real, safe Tier A/B candidates — §12/§13), not genuine absence.
Implementing just the 3 Tier A candidates relevant to the Income Statement (Profit Before Tax,
EPS, Finance Income — all already real for FCC, marked `M` today) would raise FCC's adjusted IS
score from 46% to 69% (9 of its 13 applicable concepts would then be `✓`), without a single new
filing fact being found — the ceiling is much closer than the raw numbers suggest. Treat this table as "how
much of the *currently-implemented* mapping shows up today," not "how complete this data can
ever be."

---

## 12. Tier A — Safe Candidates

Strong semantic equivalence, real, clean, HIGH-confidence tag evidence gathered this pass or by
Phase 6.0.

| Canonical concept | EU tag(s) | Affected tickers | Evidence | Metrics unlocked | Regression risk | Next step |
|---|---|---|---|---|---|---|
| Accounts Payable | `TradeAndOtherCurrentPayablesToTradeSuppliers` / `TradeAndOtherCurrentPayables` | FCC, ALO, IBE, SGO, FCT, NAI, RAND (7/8) | **VERIFIED, fresh fetch this pass** | Working Capital precision, Quick Ratio refinement, any liquidity screen | Low — new canonical concept, no existing SEC/Canada behavior touched | Add a new `IFRS_FALLBACK_TAGS`/`EU_CANONICAL_MAPPING` entry; needs a *new* canonical concept decision since none exists for any filer type today |
| Non-Controlling Interests (BS) | `ifrs-full:NoncontrollingInterests` | All 8 | **VERIFIED, fresh fetch this pass** | More accurate Total Equity decomposition; a real input for any future NCI-aware ratio | Low — same as above | New canonical concept (cross-cutting, not EU-specific) |
| Stock-Based Compensation | `IncreaseDecreaseThroughSharebasedPaymentTransactions` / `AdjustmentsForSharebasedPayments` | ALO, IBE, SGO, FCT, RAND, ISP (6/8) | **VERIFIED, fresh fetch this pass** | Owner Earnings' SBC add-back term (currently 0-filled for EU — Tier 2 note in CLAUDE.md) | Low | New canonical concept (cross-cutting, not EU-specific) |
| Change in Working Capital | `ifrs-full:IncreaseDecreaseInWorkingCapital` | FCC, ALO, FCT, NAI, RAND (5/8) | **VERIFIED, fresh fetch this pass** | Owner Earnings' ΔWC term — the last of the four Tier 2 Owner Earnings inputs besides CapEx/D&A/SBC | Low | New canonical concept |
| Profit Before Tax | `ifrs-full:ProfitLossBeforeTax` | 7/8 (all but SGO) | **CITED (Phase 6.0), re-confirmed this pass** | Maps cleanly to the existing `Income Before Tax` concept, currently 0 ifrs-full fallback | Low — existing SEC concept slot, just needs a fallback tag | Add tag to existing `IFRS_FALLBACK_TAGS["Income Before Tax"]` |
| EPS Basic / Diluted | plain tag + `FromContinuingOperations` fallback | 8/8 | **CITED (Phase 6.0), re-confirmed this pass** | Direct EPS display; benefits existing `ifrs-full` US 20-F filers too, not EU-only | Low | 2-tag fallback list |
| Finance Income | `ifrs-full:FinanceIncome` | FCC, ALO, IBE, FCT, RAND (5/8) | **CITED (Phase 6.0), re-confirmed this pass** | Completes the interest-income side already started by Interest Expense | Low | New canonical concept slot (Interest Expense's fallback today only covers cost side) |
| Accounts Receivable tag correction | `CurrentTradeReceivables` + `TradeAndOtherCurrentReceivables` | 7/8 (all but ISP) | **CITED (Phase 6.0), re-confirmed this pass** | Current Ratio/Quick Ratio precision, Net-Net Finder inputs | **Zero regression risk for EU** (currently 0/8 anyway); fixes a pre-existing SEC-side tag-string error too — worth double-checking it doesn't change any *existing* SEC-filer row before landing | Correct the existing `IFRS_FALLBACK_TAGS` entry |

---

## 13. Tier B — Decisions Required

Plausible, real evidence exists, but needs an explicit canonical-model or scope decision before
implementing.

| Canonical concept | EU tag(s) | Affected tickers | Evidence | Decision needed |
|---|---|---|---|---|
| CapEx | `PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities` (clean) vs. combined PPE+intangibles extension tags | 4/8 clean (FCC, IBE, FCT, ISP-intangibles-only); 3/8 combined-scope (ALO, SGO, RAND) | **CITED (Phase 6.0), re-confirmed this pass** | Whether to map only the 4 clean matches now (partial FCF unlock) or wait for a broader-scope canonical CapEx variant — CLAUDE.md's own Owner Earenings Tier 2 note already tracks this |
| Net Income Attributable to NCI | `ProfitLossAttributableToNoncontrollingInterests` | 7/8 (all but FCT) | **VERIFIED, fresh fetch this pass** | Whether this deserves its own canonical concept (currently none exists for ANY filer) — cross-cutting, not EU-specific, same category as Deferred Tax Assets/Liabilities (Phase 6.0 §18) |
| Total Liabilities (derived) | `Total Assets − Total Equity (incl NCI)` | Any ticker with both inputs mapped | **INFERRED** — arithmetically safe given both operands are already real IFRS facts (once §8.1 is resolved for Total Equity incl NCI) | Whether a derived (not directly-tagged) Total Liabilities is acceptable under this project's "no arithmetic-derived values unless the canonical model explicitly defines the derivation" rule — this document does not decide it |
| Short-Term / Long-Term Debt | `Shorttermborrowings`/`CurrentBorrowings`/`LongtermBorrowings` family | 4-5/8 depending on the specific tag | **VERIFIED, fresh fetch this pass** | Whether to accept the real fragmentation across tag variants (ST vs LT vs combined) as multiple fallback entries, same shape as the existing `IFRS_FALLBACK_TAGS` 3-5-tag lists |
| Common Stock / Share Capital | `ifrs-full:IssuedCapital` | 6/8 | **CITED (Phase 6.0), re-confirmed this pass** | Low-priority — real and clean, but limited standalone analytical value without a broader equity-roll-forward feature |
| Gross Profit, Total Liabilities (direct tag) | `GrossProfit`, `Liabilities` | 2/8 each | **CITED (Phase 6.0)** | Sparse — worth revisiting once a broader admitted EU universe gives a larger confirming sample, per Phase 6.0's own deferral |

---

## 14. Tier C — Do Not Implement

| Concept | Reason |
|---|---|
| SG&A (as one combined line) | No issuer reports a single combined tag — always split (Selling / Administrative) where present at all, or absent entirely (by-nature presentation). Forcing a combined SG&A would require an arithmetic sum this project's own rules don't license without an explicit derivation decision. |
| R&D Expense (broadly) | Real for exactly 1/8 (ALO) — too thin to call "broadly reusable" per this project's own confidence bar; stays a single-issuer curiosity, not a Tier A candidate. |
| Retained Earnings (broadly) | Real, direct tag for 1/8 only (NAI); 2/8 more have it folded into a combined Reserves-and-Retained-Earnings figure (A); the remaining 5/8 don't disclose it separately at all. |
| Other Equity / Reserves | Fragmented across issuer-specific tag combinations (`SharePremium`, `TreasuryShares`, various `Reserves`) with no consistent shape across issuers — exactly the "don't invent a derived value from scattered components" case this project's own rules warn against. |
| Total Liabilities + Equity | Real and universal (8/8) but mathematically identical to Total Assets (already 8/8 mapped) by the accounting identity — zero incremental analytical value. |
| ISP's/NAI's real top-line concepts as "Revenue" | Structurally different accounting concepts (interest/dividend income; rental income) — mapping either would violate this project's own "NULL > questionable value" principle. Unchanged from Phase 6.0 §5c/§14. |
| Any issuer-extension-namespace concept (`ALS:`, `rand:`, `isp:`, `fincantierispa:`, `com:`, etc.) | By definition single-company, not comparable across issuers — confirmed again this pass (e.g., RAND's own EBITDA-shaped extension, ISP's staff-cost/other-admin split). |
| Share Repurchases | Genuinely absent for all 8 issuers in the fiscal year checked — no real evidence to map against yet (not conclusively investigated across multiple years, though — see §6's UNKNOWN flag). |

---

## 15. Tier D — Bugs

| # | Bug | Affected | Root cause | Minimal fix (not implemented) | Validation required |
|---|---|---|---|---|---|
| 1 | "Total Equity (incl NCI)" never appears as its own row in clean `financials`, despite being mapped and reported IMPLEMENTED | FCC, ALO, IBE, SGO, FCT, NAI, RAND (7/8; ISP never had the raw fact) | `CONCEPT_SYNONYMS["Total Equity (incl NCI)"] = "Total Stockholders Equity"` (a pre-existing SEC-side rule) collapses it during `21__clean_and_merge.py`'s merge, and the collapsed row loses the `CONCEPT_PRIORITY` tiebreak against the real "Total Stockholders Equity" fact — see §8.1 | Either (a) source-scope the synonym collapse to exclude EU, or (b) remove/re-scope the "Total Equity (incl NCI)" EU mapping entry and correct the Phase 6.1 documentation's "IMPLEMENTED" claim | Re-run `21` after the fix; confirm "Total Equity (incl NCI)" now appears as a distinct row for the 7 affected issuers with the correct (incl-NCI) value; confirm "Total Stockholders Equity" is unaffected; confirm no SEC/Canada regression (the synonym rule is shared) |

---

## 16. Recommended Next Phase

Two independent, small, low-risk pieces of follow-up work are ready to scope as their own
authorized phases, in priority order:

1. **Fix Tier D #1** (§15) — smallest, highest-confidence, closes a real gap between documented
   and actual behavior. Should land before any further EU mapping work, since a future concept
   could hit the same `CONCEPT_SYNONYMS` collapse pattern unnoticed.
2. **Implement the Tier A set** (§12) — 8 candidates (Accounts Payable, NCI-BS,
   Stock-Based Compensation, Change in Working Capital, Profit Before Tax, EPS, Finance Income,
   the Accounts Receivable tag correction), all HIGH-confidence, all real, all with either a
   clean existing canonical slot or a well-precedented new-concept shape. This directly
   completes Owner Earnings' remaining Tier 2 inputs (SBC, ΔWC — CapEx and D&A are Tier B/
   still-open) for the EU tickers where they're real.
3. **Tier B decisions** (§13), each independently scoped — CapEx's scope question, the
   NCI-attributable-income cross-cutting concept, and the Total Liabilities derivation policy
   question are the three with the clearest downstream value.

Not recommended as immediate next steps: any Tier C item (§14), or expanding the admitted EU
universe before this batch of already-admitted-issuer gaps is closed.

---

## 17. Explicit Non-Goals (confirmed honored)

No code was modified in `01__tickers.py`, `16__fetch_eu_xbrl.py`, `21__clean_and_merge.py`,
`22__derived_metrics.py`, `23__intrinsic_value.py`, `51__export_dashboard_data.py`,
`fundamentals_screener`, or the website. No new mapping was implemented. No full-universe
recompute was run (all `22`/`23` evidence cited in §8.2/§9 is from the already-completed Phase
6.3 session work, not a fresh run for this audit). The one genuine bug found (§8.1/§15) was
documented, not fixed, per instruction. Phase 6.5/Tier A implementation was not started.

---

## 18. Validation Evidence

- Real production SQL (read-only `SELECT`s against `main.financials.financials`/
  `financials_raw`/`financials_metrics`), this session, post-Phase-6.3.
- Fresh, live re-fetch of real xBRL-JSON for all 8 issuers via this repo's own unmodified
  `fundamentals_pipeline.sources.eu_current` functions, run locally, this session — raw dump and
  analysis scripts used are session-local (scratchpad), not committed; every concept tag and
  value cited above is reproducible by re-running the same fetch against `filings.xbrl.org`'s
  live API using the LEIs in `main.config.eu_admission_candidates`.
- Direct code reads of `sources/eu_current.py`, `01__tickers.py` (`CONCEPT_SYNONYMS`/
  `CONCEPT_PRIORITY`), `21__clean_and_merge.py`, `companies.py`'s `get_statements()`,
  `concept_hierarchy.json`.
- One live website fetch (FCC), re-confirming, not re-investigating, Phase 6.3's already-
  documented cache-lag finding.
- `docs/phase6-european-esef-financial-coverage.md` (Phase 6.0/6.1), cited extensively and
  re-verified at each point this document relies on it, not copied uncritically.

---

## Final Report

- **Classification: READY FOR DECISION**
- **Production writes: NO**
- **Code changes: NO**
- **New bugs found: YES** (1 — §15, Tier D)
- **New mappings implemented: NO**
- **Number of safe Tier A candidates: 8**
- **Number of Tier B decisions: 6**
- **Number of Tier C exclusions: 8**
- **Number of Tier D bugs: 1**

**Recommendation**: fix Tier D #1 first (small, isolated, clarifies what "Total Equity (incl
NCI)" actually means going forward) — then implement the Tier A set as its own authorized,
narrowly-scoped phase, following exactly the same implement→test→real-EU-reingest→validate→
idempotency→publish discipline Phase 6.3 just proved out. Do not start either without a fresh
go-ahead; this document is the factual basis for that decision, not the decision itself.

# Phase 6.0 — European ESEF Financial-Statement Coverage Research

**Research/audit only. No pipeline, mapping, schema, Databricks, or frontend code was changed to
produce this document.** Every fact below was obtained one of three ways, and each claim is
labeled accordingly:

- **VERIFIED FROM REAL DATA** — obtained by directly re-running this repo's own real, unmodified
  `fundamentals_pipeline.sources.eu_current` code (`extract_source_facts`,
  `select_filing_for_period`) against real xBRL-JSON filings fetched live from
  `filings.xbrl.org` for all 8 admitted issuers (FCC, ALO, IBE, SGO, FCT, NAI, RAND, ISP), or by
  querying the real production `main.financials.financials` table directly.
- **INFERRED** — a reasonable conclusion drawn from the verified evidence plus general IFRS/
  accounting domain knowledge, not itself independently fetched.
- **UNKNOWN** — flagged explicitly where the evidence gathered here doesn't resolve a question.

No hypothetical XBRL examples were used anywhere in this document.

---

## 1. Executive summary

The premise of this research phase is confirmed, and the finding is worse — and more fixable —
than "the ESEF adapter only maps 5 concepts." **VERIFIED FROM REAL DATA**: the 8 admitted
issuers' real, current annual filings collectively contain **605 distinct consolidated,
current-period XBRL concepts** (127–202 per issuer), not the ~500-per-filing estimate in
`registry.py`'s own comment — that comment undercounted because the real per-filing total (before
the consolidated/current-period filter) ranges 420–1,075. Of those 605, a very large fraction —
**28 of them exactly match tag strings already accepted, tested, and running in production**
today, via `fundamentals_pipeline/00__config/01__tickers.py`'s `IFRS_FALLBACK_TAGS` — the
existing IFRS fallback mapping built for `ifrs-full`-tagged US 20-F/40-F filers (Toyota, Vale,
Infosys were the concepts' own original verification set). **This is the central finding of this
research phase: the canonical model does not need to change. The gap is that
`fundamentals_pipeline/sources/eu_current.py`'s `EU_CANONICAL_MAPPING` is a narrow, hand-picked
5-concept subset of a model that already covers ~28 IFRS concepts elsewhere in this same
codebase.** Widening the EU adapter to reuse `IFRS_FALLBACK_TAGS` (verifying each entry against
real EU data, which this document does) is the single highest-leverage, lowest-risk next step —
not inventing a new taxonomy.

A second, unexpected, and more urgent finding: **VERIFIED FROM REAL DATA against the live
production `financials` table** — the "Revenue" concept, the flagship of the 5 currently mapped
and the most economically important line item, is **completely absent today for ALO, FCT, SGO,
NAI, and ISP** (5 of 8 issuers) across every fiscal year on record. This is not a downstream
display bug (Phase 5.7's own finding that null renders honestly as `—`, confirmed still true) —
it is a real coverage gap in the mapping itself: those five issuers tag their top line with
`ifrs-full:RevenueFromContractsWithCustomers` (3 of them) or an entity-specific concept (2 of
them — see below), never the bare `ifrs-full:Revenue` this pipeline's `EU_CANONICAL_MAPPING`
requires verbatim. Only FCC, IBE, and RAND — which happen to tag `ifrs-full:Revenue` directly —
have real Revenue data in production.

**Final-decision preview** (full reasoning in §16–20): the canonical financial model needs **no
structural change**. A Tier 1 set of roughly 15 concepts can be added with high confidence by
reusing already-accepted tag strings. A second, smaller Tier 2 set (led by fixing Revenue) needs
one new tag-list decision each, verified here against real data, not guessed. Shares
Diluted/EPS-input coverage remains genuinely thin and should stay NULL, confirming (not
contradicting) Phase 5.6's original finding. Two issuers — ISP (a bank) and NAI (a real-estate
investment company) — have a structurally different top-line concept that the corporate
Income-Statement model was never built to represent, and forcing a mapping for them would violate
this project's own "NULL > questionable value" principle.

---

## 2. Current European coverage

**VERIFIED FROM REAL DATA.** `fundamentals_pipeline/sources/eu_current.py`'s
`EU_CANONICAL_MAPPING` (lines 221-263) maps exactly 5 canonical concepts, each keyed to one
`ifrs-full` tag string, `MappingStatus.ACCEPTED`/`MappingType.DIRECT`:

| Canonical concept | Source tag | Real production coverage (of 8 issuers) |
|---|---|---|
| Revenue | `ifrs-full:Revenue` | **3/8** — FCC, IBE, RAND only |
| Net Income | `ifrs-full:ProfitLossAttributableToOwnersOfParent` | 7/8 — all but FCT |
| Net Income (incl NCI) | `ifrs-full:ProfitLoss` | 8/8 |
| Total Assets | `ifrs-full:Assets` | 8/8 |
| Cash & Equivalents | `ifrs-full:CashAndCashEquivalents` | 8/8 |

`map_source_fact_to_canonical()` (`eu_current.py:268-273`) returns `None` — outright excluded,
not NULL-and-kept — for every other source concept, including the other 600 real, present
concepts this research found. `process_eu_entity()` (`16__fetch_eu_xbrl.py:340-350`) then
literally drops any fact whose decision is `None` before it ever reaches `financials_raw`.

---

## 3. Canonical model inventory

**VERIFIED FROM REAL DATA** (direct read of `fundamentals_pipeline/00__config/01__tickers.py`).
The canonical model — `INCOME_STATEMENT`/`BALANCE_SHEET`/`CASH_FLOW` dicts, combined into
`STATEMENTS` — already defines every concept below, each with a `kind`
(`flow_additive`/`flow_nonadditive`/`stock`) and one-or-more `us-gaap` source tags. This is the
**one** canonical model — SEC and Canadian 40-F filers already share it; there is no separate
"US model" and "Canada model" to compare against.

### Income Statement
| Canonical | kind | us-gaap tag(s) | Has an `IFRS_FALLBACK_TAGS` entry? |
|---|---|---|---|
| Revenue (+ 6 synonym variants, see `CONCEPT_SYNONYMS`) | flow_additive | `Revenues` (+ASC-606/bank/O&G variants) | Yes — `Revenue` |
| Cost of Revenue | flow_additive | `CostOfRevenue` (+3 variants) | Yes — `CostOfSales` |
| Gross Profit | flow_additive | `GrossProfit` | Yes |
| Operating Expenses | flow_additive | `OperatingExpenses` | No |
| R&D Expense | flow_additive | `ResearchAndDevelopmentExpense` | Yes |
| SG&A Expense | flow_additive | `SellingGeneralAndAdministrativeExpense` | Yes |
| Operating Income | flow_additive | `OperatingIncomeLoss` | Yes — `ProfitLossFromOperatingActivities` |
| Interest Expense (+2 synonyms) | flow_additive | `InterestExpense` (+2 variants) | Yes — `["FinanceCosts","InterestExpense"]` |
| Income Before Tax | flow_additive | `IncomeLossFromContinuingOperations...` | **No** (real IFRS equivalent exists — see §5) |
| Income Tax | flow_additive | `IncomeTaxExpenseBenefit` | Yes |
| Net Income (+2 synonyms) | flow_additive | `NetIncomeLoss` (+`ProfitLoss` etc.) | Yes (both variants) |
| EPS Basic | flow_nonadditive | 3-tag fallback list | **No** — documented gap even for existing IFRS filers (§16.5's own comment) |
| EPS Diluted | flow_nonadditive | 4-tag fallback list | **No** — same documented gap |
| Shares Diluted | flow_nonadditive | `WeightedAverageNumberOfDilutedSharesOutstanding` | No |

### Balance Sheet
| Canonical | kind | Has an `IFRS_FALLBACK_TAGS` entry? |
|---|---|---|
| Cash & Equivalents | stock | Yes |
| Short-term Investments | stock | No |
| Accounts Receivable | stock | Yes — `TradeReceivables` (**verbatim tag not found in real EU data — see §5**) |
| Inventory | stock | Yes — `Inventories` |
| Total Current Assets | stock | Yes — `CurrentAssets` |
| PP&E Net | stock | Yes |
| Goodwill | stock | Yes |
| Intangible Assets (3-tag fallback) | stock | Yes — `IntangibleAssetsOtherThanGoodwill` |
| Total Assets | stock | Yes |
| Accounts Payable | stock | No |
| Short-term Debt (aggregate-or-sum) | stock | No |
| Total Current Liabilities | stock | Yes |
| Long-term Debt (3-tag fallback) | stock | No |
| Total Liabilities | stock | Yes |
| Additional Paid-in Capital | stock | No |
| Retained Earnings | stock | Yes |
| Total Stockholders Equity | stock | Yes — `EquityAttributableToOwnersOfParent` |
| Total Equity (incl NCI) | stock | Yes — `Equity` |
| Total Liabilities & Equity | stock | No |
| Shares Outstanding (Cover Page) | stock, `dei` namespace | No — SEC-specific (cover-page fact), no ESEF equivalent (§9) |

### Cash Flow Statement
| Canonical | kind | Has an `IFRS_FALLBACK_TAGS` entry? |
|---|---|---|
| Depreciation & Amortization (3-tag fallback) | flow_additive | Yes |
| Stock-based Compensation | flow_additive | No |
| Operating Cash Flow | flow_additive | Yes |
| CapEx (5-tag fallback) | flow_additive | Yes (1 of the 5) |
| Investing Cash Flow | flow_additive | Yes |
| Financing Cash Flow | flow_additive | Yes |
| Dividends Paid | flow_additive | Yes |
| ~15 more (Acquisitions, Debt Issuance/Repayment, Share Repurchases, Net Change in Cash, …) | flow_additive | No |

**Consumed by `fundamentals_screener`**: every concept above is a real Income Statement/Balance
Sheet/Cash Flow line and is displayed generically by `CompanyRepository.get_statements()` — no
per-concept allowlist exists there (confirmed in the Phase 5.7 audit); anything landing in
`financials` with a `concept_hierarchy.json` entry displays automatically. No new canonical
concept proposed in this document requires a `fundamentals_screener` code change — only a
`concept_hierarchy.json` registration (a data/config change, not application logic — not made in
this research pass).

---

## 4. Real ESEF concept inventory

**VERIFIED FROM REAL DATA.** Methodology: for each of the 8 issuers, the real, latest ingestible
filing was found using this repo's own `select_filing_for_period()` (the exact production
amendment-selection logic), its raw xBRL-JSON was fetched from the exact URL
`EUCurrentSource.retrieve_facts()` would use, and `extract_source_facts()` (the exact,
unmodified production function — consolidated-only, current-period-only, per its own documented
rules) was run against it, unmodified. Fixture-free, live-network, real-filing evidence.

| Ticker | Filing used (fxo_id) | Period end | Total facts in filing | Consolidated, current-period facts (this adapter's own filter) |
|---|---|---|---|---|
| FCC | `95980020140005178328-2024-12-31-ESEF-ES-0` | 2024-12-31 | 512 | 142 |
| ALO | `96950032TUYMW11FB530-2026-03-31-ESEF-FR-0` | 2026-03-31 | 609 | 127 |
| IBE | `5QK37QC7NWOJ8D7WVQ45-2024-12-31-ESEF-ES-0` | 2024-12-31 | 833 | 180 |
| SGO | `NFONVGN05Z0FMN5PEC35-2025-12-31-ESEF-FR-0` | 2025-12-31 | 609 | 137 |
| FCT | `8156005BDF49128B6239-2024-12-31-ESEF-IT-1` | 2024-12-31 | 527 | 128 |
| NAI | `724500JXEXUGEATP5L52-2025-12-31-ESEF-NL-1` | 2025-12-31 | 420 | 95 |
| RAND | `7245009EAAUUQJ0U4T57-2025-12-31-ESEF-NL-0` | 2025-12-31 | 668 | 125 |
| ISP | `2W8N8UU78PMDQKZENC08-2022-12-31-ESEF-IT-1` | 2022-12-31 | 1075 | 202 |

**A real, notable side finding**: FCT's most recent filing on file
(`8156005BDF49128B6239-2025-12-31-ESEF-IT-0`) has `error_count=0` but no `json_url` — genuinely
not ingestible, exactly the real failure mode `eu_current.py`'s own docstring already documents
("Fincantieri's real FY2025 filing has `error_count=0` but `json_url=None`") — confirming this is
a stable, ongoing characteristic of that filer's aggregator record, not a one-off. This research
used FCT's next-most-recent ingestible filing (FY2024) instead, exactly as production
`process_eu_entity()` would (it walks periods and reports, never silently skips a whole issuer).
ISP's index only goes back to FY2021-2022 at all (its most recent ingestible filing is FY2022) —
**UNKNOWN** whether more recent ISP filings exist on `filings.xbrl.org` under a different query
shape; not investigated further here (out of this phase's scope — this document characterizes
what the adapter's real logic returns today, not the aggregator's full historical index).

**Total distinct concepts across all 8 filings, consolidated + current-period only: 605.**

---

## 5. Mapping candidates & confidence classification

**VERIFIED FROM REAL DATA**, cross-checked against `IFRS_FALLBACK_TAGS` verbatim.

### 5a. Concepts already in `IFRS_FALLBACK_TAGS` — verified against real EU data

| Canonical label | ifrs-full tag | Real coverage | Confidence |
|---|---|---|---|
| Income Tax | `IncomeTaxExpenseContinuingOperations` | 8/8 | **HIGH** |
| Net Income (incl NCI) | `ProfitLoss` | 8/8 | **HIGH** |
| Total Assets | `Assets` | 8/8 | **HIGH** |
| Cash & Equivalents | `CashAndCashEquivalents` | 8/8 | **HIGH** |
| Operating Cash Flow | `CashFlowsFromUsedInOperatingActivities` | 8/8 | **HIGH** |
| Investing Cash Flow | `CashFlowsFromUsedInInvestingActivities` | 8/8 | **HIGH** |
| Financing Cash Flow | `CashFlowsFromUsedInFinancingActivities` | 8/8 | **HIGH** |
| Net Income | `ProfitLossAttributableToOwnersOfParent` | 7/8 | **HIGH** |
| Total Stockholders Equity | `EquityAttributableToOwnersOfParent` | 7/8 | **HIGH** |
| Total Equity (incl NCI) | `Equity` | 7/8 | **HIGH** |
| PP&E Net | `PropertyPlantAndEquipment` | 7/8 | **HIGH** |
| Total Current Liabilities | `CurrentLiabilities` | 5/8 | **HIGH** (absent = filer doesn't present a current/noncurrent split, not a mapping problem — see §12) |
| Total Current Assets | `CurrentAssets` | 5/8 | **HIGH** (same caveat) |
| Goodwill | `Goodwill` | 5/8 | **HIGH** |
| Interest Expense | `FinanceCosts` (already 1st in the existing 2-tag list) | 5/8 | **HIGH** |
| Dividends Paid | `DividendsPaid` | 6/8 | **HIGH** |
| Operating Income | `ProfitLossFromOperatingActivities` | 5/8 | **HIGH** |
| Intangible Assets | `IntangibleAssetsOtherThanGoodwill` | 3/8 | **HIGH** (tag is exact and unambiguous; low count = real absence for the other 5, see §12) |
| Inventory | `Inventories` | 3/8 | **HIGH** |
| Cost of Revenue | `CostOfSales` | 3/8 | **HIGH** |
| CapEx | `PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities` (already in the existing 5-tag list, currently just not tried for EU) | 4/8 | **MEDIUM** — real but genuinely partial; see §8 |
| Gross Profit | `GrossProfit` | 2/8 | **MEDIUM** — tag itself is unambiguous IFRS; only issuers that present a Gross Profit subtotal have it (most groups fold cost lines by nature, not function — a real, structural IFRS presentation choice, not a mapping gap) |
| R&D Expense | `ResearchAndDevelopmentExpense` | 1/8 (ALO) | **MEDIUM** — real single-issuer instance, tag itself is standard IFRS, would need one more confirming issuer before calling it broadly reusable |

Every row above is a **direct reuse of a tag already trusted for real production IFRS filers
(Toyota/Vale/Infosys) elsewhere in this codebase** — none of it is a new mapping decision this
research invented; it is verifying an existing decision against a second real dataset.

### 5b. Real, common IFRS concepts NOT yet in `IFRS_FALLBACK_TAGS` at all

| Canonical candidate | ifrs-full tag | Real coverage | Confidence | Why not yet mapped |
|---|---|---|---|---|
| Revenue (2nd variant) | `RevenueFromContractsWithCustomers` | 3/8 (ALO, FCT, SGO) | **HIGH** | Same IFRS 15 concept as `Revenue`, standard alternate top-line tag (directly analogous to this codebase's own existing `us-gaap:Revenues` vs `RevenueFromContractWithCustomerExcludingAssessedTax` synonym pair — same taxonomy transition, same reasoning, already a proven pattern in `CONCEPT_SYNONYMS`) |
| Profit Before Tax | `ProfitLossBeforeTax` | 7/8 | **HIGH** | Maps cleanly to the existing `Income Before Tax` canonical concept, which currently has zero ifrs-full fallback |
| Accounts Receivable (2 real variants) | `CurrentTradeReceivables` (4/8) / `TradeAndOtherCurrentReceivables` (3/8) | 7/8 combined | **HIGH** | The existing `IFRS_FALLBACK_TAGS["Accounts Receivable"]` value, `TradeReceivables`, **matches 0/8 real filings verbatim** — a real, verified gap in the existing entry itself, not just missing-for-EU |
| Finance Income | `FinanceIncome` | 5/8 | **HIGH** | Clean IFRS concept, no existing canonical slot (Interest Expense's fallback only covers the cost side) |
| Total Liabilities | `Liabilities` | 2/8 | **MEDIUM** | Tag itself unambiguous; most issuers present `EquityAndLiabilities`/derive Liabilities as Assets − Equity rather than tagging it directly — a real, structural presentation-choice gap, not a confidence problem with the tag itself |

### 5c. DO NOT MAP

| Concept | Reason |
|---|---|
| `ifrs-full:OtherComprehensiveIncome*` variants (6/8, 5/8, …) | Comprehensive income, not net income or any statement line this canonical model represents — genuinely no matching canonical concept exists, and none should be invented for it in this phase |
| `ifrs-full:DeferredTaxAssets`/`DeferredTaxLiabilities` (8/8 each) | Real, common, unambiguous IFRS concepts — but no canonical slot exists in the current SEC-built model at all (not even for `us-gaap` filers); adding one is a real, separate, cross-cutting decision, deferred (§18) |
| Any `rand:`/`ALS:`/`isp:`/`ibe:`/`fincantierispa:`/`newamsterdaminvestnv:`/`com:`/`fomentodeconstruccionesycontratassa:`-namespaced concept | Issuer extension, by definition single-company — see §7 |
| ISP's `isp:InterestIncomeAndSimilarRevenues` / NAI's `ifrs-full:RentalIncomeFromInvestmentProperty` as a stand-in for "Revenue" | Real, present, economically top-line-like — but a bank's/REIT's top line is not the same accounting concept as a corporate Revenue line; mapping either to "Revenue" would be exactly the "questionable value" this project's own principle exists to prevent. See §13. |

---

## 6. IFRS standard vs. issuer extensions

**VERIFIED FROM REAL DATA.** Every one of the 8 filings declares its own issuer-specific
extension namespace in `documentInfo.namespaces`, confirmed directly:

| Ticker | Extension namespace prefix |
|---|---|
| ALO | `ALS` |
| FCC | `fomentodeconstruccionesycontratassa` |
| FCT | `fincantierispa` |
| IBE | `ibe` |
| ISP | `isp` |
| NAI | `newamsterdaminvestnv` |
| RAND | `rand` |
| SGO | `com` |

Real examples of extension concepts found and their disposition: `rand:ProfitLossFromOperating
ActivitiesBeforeInterestTaxesDepreciationAndAmortisationExpenseClassifiedAsOperatingActivities`
(RAND's own EBITDA-shaped extension — a derived, issuer-specific presentation choice, not a
standard IFRS concept — **DO NOT MAP**, even though a `canonical EBITDA` concept would clearly
want this value if it existed and were verified); `ALS:IncreaseDecreaseInNumberOfSharesOutstanding
Through*` (7 distinct ALO-only extension concepts tracking share-count roll-forward by cause —
real, granular, but single-issuer and not what the canonical `Shares Diluted`/`Shares Outstanding`
concepts represent — **DO NOT MAP**); `isp:PreviousYearProfitLossAllocationReserves`,
`ibe:ReembolsoDeObligacionesPerpetuasSubordinadas` (Spanish-language extension — perpetual
subordinated bond redemption) — genuinely bespoke, correctly excluded already by
`map_source_fact_to_canonical()` returning `None` for anything not in the 5-entry (soon
potentially larger) mapping dict.

**No extension concept found in this research is recommended for mapping.** Every Tier 1/Tier 2
candidate in §5 is a plain `ifrs-full:` (standard IFRS taxonomy) concept — exactly the "clear IFRS
equivalent" case the research prompt itself distinguished from "issuer-specific extension, do not
silently map."

---

## 7. Dimensions / segments

**VERIFIED FROM REAL DATA, indirectly** — `extract_source_facts()` already filters to
`is_consolidated_fact()` (exactly `{concept, entity, period, unit}`, no extra dimension key)
before this research ever saw a fact, so every number in §4/§5 is already dimension-safe by
construction; this is the adapter's own existing, correct, unmodified behavior, re-confirmed
working across all 8 real issuers, not just the original 4-issuer pilot.

**Real dimensionality magnitude, newly measured**: the fraction of a filing's total facts that
survive the consolidated + current-period filter ranges from **17%** (IBE: 180/833) to **28%**
(FCC: 142/512) — meaning **72-83% of every real filing's facts are dimensioned** (segment,
component-of-equity, prior-year comparative, or another axis) and correctly excluded today. This
is a real, substantial confirmation that dimension handling is not a minor edge case — most of a
real ESEF filing's content is dimensional, and the existing filter is load-bearing, not
incidental.

**Safe / unsafe / requires future work**, per the prompt's own framing:
- **Safe, verified**: consolidated-only selection (`is_consolidated_fact`), current-period-only
  selection (`is_current_period_fact`, including its correct handling of xBRL's exclusive-end
  instant convention and Alstom's non-calendar fiscal year — both already proven, not re-derived
  here).
- **Requires future work, not attempted here**: no investigation was done into whether any of the
  605-concept universe's *dimensioned* facts (the other ~75%) contain useful data the consolidated
  filter is discarding for a good reason vs. a recoverable one (e.g., a geographic-segment revenue
  breakdown that sums to the consolidated total could, in principle, validate the consolidated
  figure — not explored, explicitly deferred).

---

## 8. Period semantics

**VERIFIED FROM REAL DATA — the cleanest, most conclusive finding in this document.**
Classifying every one of the 1,134 real consolidated current-period facts (across all 8 issuers)
using this repo's own existing `classify_period_shape()` (`01__tickers.py:566-584`, unmodified,
same function SEC ingestion uses) produces exactly two buckets, for every single issuer:

| Ticker | `FY_or_TTM` (duration facts) | `snapshot` (instant facts) | Any `Q_standalone`/`YTD_6M`/`YTD_9M`/`other_Xd`? |
|---|---|---|---|
| FCC | 89 | 53 | **0** |
| ALO | 89 | 38 | **0** |
| IBE | 107 | 73 | **0** |
| SGO | 98 | 39 | **0** |
| FCT | 88 | 40 | **0** |
| NAI | 60 | 35 | **0** |
| RAND | 85 | 40 | **0** |
| ISP | 141 | 61 | **0** |

**Zero interim (semi-annual/9-month/quarterly) periods exist anywhere in this real data.**
Cross-checked against each issuer's own real filing index (§4): every `period_end` returned by
`filings.xbrl.org/api/entities/{lei}/filings` for all 8 issuers is exactly 12 months apart from
its neighbor (e.g. ALO: `2023-03-31, 2024-03-31, 2025-03-31, 2026-03-31` — confirming Alstom's
real March 31 fiscal year-end directly, not inferred from documentation). This matches
`registry.py`'s own documented characterization of the source ("annual consolidated IFRS
statements only") exactly.

**This is a source-availability fact, not a canonical-model gap, and not (primarily) a mapping-
coverage gap either**: `classify_period_shape()` already correctly buckets a 6-month or 9-month
duration fact into `YTD_6M`/`YTD_9M` today, unmodified — the exact same function that would
handle a future European interim fact already exists and already works, proven by its 20+ years
of SEC quarterly use. **The blocker for a "Quarterly"→"Interim" frontend generalization (Phase
5.7's own deferred item) is that `filings.xbrl.org` itself has no interim filings on record for
any of these 8 issuers** — not a code limitation anywhere in this pipeline. Whether ESEF-mandated
half-year reports exist for these issuers on some OTHER source/index is **UNKNOWN** — not
investigated in this pass (this document characterizes `filings.xbrl.org`'s real index only, the
one source this adapter actually calls).

---

## 9. Cash-flow coverage

**VERIFIED FROM REAL DATA.**

- **Operating / Investing / Financing Cash Flow: 8/8, unambiguous, HIGH confidence** — see §5a.
  This alone would make the Cash Flow Statement tab genuinely populated (currently empty for all
  8 issuers, per Phase 5.7's own live-validation finding) for every admitted issuer.
- **CapEx**: real but partial (4/8 — FCC, FCT, IBE, NAI) via
  `ifrs-full:PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities`, already the
  first entry in the existing 5-tag `CapEx` fallback list. Real evidence for the other 4 issuers:
  ALO/SGO use a combined PPE+intangibles+other-noncurrent-assets purchase line
  (`ALS:PurchaseOfPropertyPlantAndEquipmentPurchaseOfIntangibleAssetsAndPaymentsForDevelopment
  ProjectExpenditureClassifiedAsInvestingActivities` for ALO — an extension concept, correctly
  excludable; `ifrs-full:PurchaseOfPropertyPlantAndEquipmentIntangibleAssetsOtherThanGoodwill
  InvestmentPropertyAndOtherNoncurrentAssets` for SGO — a real, *standard* IFRS combined-purchase
  tag, genuinely a different concept than the SEC model's PPE-only CapEx, not a variant of it);
  RAND uses its own extension (`rand:NetAdditionsToPropertyPlantAndEquipmentAndComputerSoftware`);
  ISP (a bank) has no comparable capex line at all in this filing. **MEDIUM confidence overall,
  HIGH confidence for the 4/8 that do use the standard SEC-equivalent tag.**
- **Depreciation & Amortization**: genuinely sparse and inconsistently tagged as an isolated
  figure — only FCC tags `ifrs-full:DepreciationAndAmortisationExpense` directly (the existing
  canonical tag). The other 7 issuers fold D&A into a cash-flow reconciliation adjustment line
  (`AdjustmentsForDepreciationAndAmortisationExpense...`, itself sometimes further bundled with
  impairment) — real IFRS presentation practice (D&A is usually a notes disclosure, not always a
  primary-statement XBRL fact), not a mapping oversight. **LOW confidence for a clean, direct D&A
  mapping across the universe; the existing 3-tag fallback list's shape (aggregate-first,
  component-fallback) is the right pattern to extend, but each additional tag needs its own
  verification, not attempted here.**
- **Free Cash Flow** (`Operating Cash Flow − CapEx`, `22__derived_metrics.py:291-292`): would
  become computable for the 4 issuers with real CapEx coverage the moment Operating CF + CapEx
  both map — i.e., immediately valuable from Tier 1 alone.

---

## 10. Shares / EPS coverage

**VERIFIED FROM REAL DATA — directly confirms, and sharpens, Phase 5.6's original finding.**

- **EPS Basic / EPS Diluted**: real and common — 7/8 for the plain
  `BasicEarningsLossPerShare`/`DilutedEarningsLossPerShare` tags (all but IBE); IBE instead tags
  the `...FromContinuingOperations` variant (`ifrs-full:BasicEarningsLossPerShareFromContinuing
  Operations` = 0.84, confirmed real value) — a second, real tag variant, not an absence. **HIGH
  confidence for a 2-tag fallback list** (plain tag first, `FromContinuingOperations` fallback) —
  would give 8/8 real coverage. No canonical `IFRS_FALLBACK_TAGS` entry exists yet for EPS at all
  (a pre-existing, documented gap, not EU-specific — see §3).
- **Share COUNT concepts (the actual gap)**: genuinely sparse and overwhelmingly issuer-specific.
  `ifrs-full:WeightedAverageShares` appears exactly once (SGO). `ifrs-full:NumberOfSharesOutstanding`
  appears exactly once (ALO), alongside 7 more ALO-only extension concepts tracking share-count
  *changes* by cause (dividends, buybacks, share-based payment, …) — real, granular, but not a
  clean period-end or weighted-average balance the way `Shares Diluted` needs. **Zero** of the 8
  issuers have a real, directly-usable `WeightedAverageNumberOfDilutedSharesOutstanding`-equivalent
  fact. **This confirms — with direct evidence across all 8 issuers, not just the original
  observation — that `Shares Diluted` should remain NULL for European issuers. This is not a
  mapping-effort gap; the source data for a clean weighted-average diluted share count is not
  present in these filings at all.**
- **Downstream consequence, already true today and unaffected by any Tier 1/2 mapping proposed
  here**: EPS-derived metrics that need a share count as an *input* (rather than reading EPS
  directly) — Lynch's PEG/EPS-CAGR, per-share intrinsic value estimates — stay correctly NULL.
  EPS itself, mapped directly (not derived from Net Income ÷ Shares), is a real, achievable win
  independent of the share-count gap.

---

## 11. Downstream impact

**VERIFIED FROM REAL DATA** — direct grep of `fundamentals_pipeline/20__transformation/
22__derived_metrics.py` confirming exactly which derived metrics consume each concept (not
inferred from formula names):

| Canonical concept (Tier) | Confirmed downstream consumer(s) in `22__derived_metrics.py` |
|---|---|
| Operating Cash Flow, CapEx (Tier 1/partial) | **Free Cash Flow** (`:291-292`) |
| Operating Income, Depreciation & Amortization | **EBITDA** (`:336-338`) |
| Operating Income | **Interest Coverage**, **ROIC %**, **ROCE %**, **ROTCE %** (`:354,391-392,560`) |
| Total Current Assets, Total Current Liabilities | **Current Ratio** (`:359`) |
| Total Current Assets, Inventory, Total Current Liabilities | **Quick Ratio** (`:365-369`) |
| Total Assets, Total Current Liabilities | **Working Capital** (`:385-386`) |
| Total Current Assets, (a liabilities figure) | **NCAV** and its Relaxed/Moderate/Strict variants (`:444-495`) — the Net-Net Finder's entire data source |
| Goodwill, Intangible Assets, Total Assets | **Tangible Book Value**, **Goodwill/Total Assets %**, **Goodwill/TBV %** (`:515-548`) |
| Revenue | **Gross Margin %** (with Gross Profit), **Operating Margin %** (`:284-285`), plus every growth/CAGR metric that needs a revenue series |
| — (all of the above) | **Graham Number**, **DCF**, **Owner Earnings**, **Piotroski F-Score**, **Altman Z-Score** — CLAUDE.md's own documented formulas, each combining multiple of the concepts above; not re-derived here, but every one of their real inputs is a concept this document already covers |

**Priority ranking by downstream reach** (concepts unlocking the most existing features, not a
new ranking scheme — just ordering the table above by fan-out):
1. **Operating Cash Flow + CapEx** (already 8/8 and 4/8 respectively) → unlocks Free Cash Flow,
   the single most-reused input across DCF/Owner Earnings/valuation.
2. **Total Current Assets + Total Current Liabilities** (5/8 each) → unlocks Current Ratio, Quick
   Ratio, Working Capital, and the entire NCAV/Net-Net Finder family for those 5 issuers at once.
3. **Operating Income** (5/8) → unlocks EBITDA, Interest Coverage, ROIC/ROCE/ROTCE.
4. **Goodwill + Intangible Assets** (5/8, 3/8) → unlocks Tangible Book Value and its ratios.
5. **Revenue, fixed** (would go from 3/8 to 6/8 — see §13) → unlocks margins and every
   growth-based screen/preset for 3 more issuers immediately.

---

## 12. 8-company coverage matrix

**VERIFIED FROM REAL DATA — every cell below is a direct lookup against the real 605-concept
universe extracted in §4, not filled from general IFRS knowledge.** `YES` = the exact ifrs-full
tag string is present as a real, consolidated, current-period fact in that issuer's real filing;
`-` = absent from that filing (not necessarily absent from the issuer's full disclosure — only
absent from the specific tag string checked).

| Concept | FCC | ALO | IBE | SGO | FCT | NAI | RAND | ISP |
|---|---|---|---|---|---|---|---|---|
| Revenue (`Revenue`) | YES | - | YES | - | - | - | YES | - |
| Revenue (`RevenueFromContractsWithCustomers`) | - | YES | - | YES | YES | - | - | - |
| Cost of Sales | - | YES | - | YES | - | - | YES | - |
| Gross Profit | - | - | YES | - | - | - | YES | - |
| Operating Income | YES | YES | YES | - | - | YES | YES | - |
| Finance Income | YES | YES | YES | - | YES | - | YES | - |
| Finance Costs | YES | YES | YES | - | YES | - | YES | - |
| Profit Before Tax | YES | YES | YES | - | YES | YES | YES | YES |
| Income Tax Expense | YES | YES | YES | YES | YES | YES | YES | YES |
| Net Income (incl NCI) | YES | YES | YES | YES | YES | YES | YES | YES |
| Net Income (attributable to parent) | YES | YES | YES | YES | - | YES | YES | YES |
| EPS Basic | YES | YES | - | YES | YES | YES | YES | YES |
| EPS Diluted | YES | YES | - | YES | YES | YES | YES | YES |
| Total Assets | YES | YES | YES | YES | YES | YES | YES | YES |
| Current Assets | YES | - | YES | YES | - | YES | YES | - |
| Noncurrent Assets | YES | YES | YES | YES | YES | YES | YES | - |
| Cash & Equivalents | YES | YES | YES | YES | YES | YES | YES | YES |
| Trade Receivables (`CurrentTradeReceivables`) | YES | YES | - | YES | - | YES | - | - |
| Trade & Other Receivables (`TradeAndOtherCurrentReceivables`) | - | - | YES | - | YES | - | YES | - |
| Inventories | - | YES | YES | YES | - | - | - | - |
| Property, Plant & Equipment | YES | YES | YES | YES | YES | YES | YES | - |
| Goodwill | YES | YES | YES | YES | - | - | YES | - |
| Intangible Assets (excl. Goodwill) | - | YES | YES | YES | - | - | - | - |
| Total Liabilities | - | - | - | - | - | YES | YES | - |
| Current Liabilities | YES | - | YES | YES | - | YES | YES | - |
| Noncurrent Liabilities | YES | YES | YES | YES | YES | YES | YES | - |
| Equity (incl NCI) | YES | YES | YES | YES | YES | YES | YES | - |
| Equity attributable to owners of parent | YES | YES | YES | YES | YES | YES | YES | - |
| Issued Capital | YES | - | YES | - | YES | YES | YES | YES |
| Retained Earnings | - | - | - | - | - | YES | - | - |
| Operating Cash Flow | YES | YES | YES | YES | YES | YES | YES | YES |
| Investing Cash Flow | YES | YES | YES | YES | YES | YES | YES | YES |
| Financing Cash Flow | YES | YES | YES | YES | YES | YES | YES | YES |
| CapEx (PPE purchases, investing) | YES | - | YES | - | YES | YES | - | - |
| Dividends Paid | YES | YES | - | YES | YES | YES | - | YES |
| Long-term Borrowings | - | YES | - | YES | - | YES | YES | - |

**Read on the two structural outliers, both visible directly in this matrix**: ISP (a bank) is
`-` for Current/Noncurrent Assets & Liabilities, Gross Profit, Cost of Sales, Retained Earnings,
Goodwill, Intangibles, PP&E, Equity-attributable, and both Revenue variants — a materially
different real coverage profile than the other 7, confirming §13's "structurally different
statement model" finding directly from the matrix, not just from the earlier keyword search. NAI
(real-estate investment) is `-` for both Revenue variants for the same reason (§5c).

---

## 13. Data-quality findings

**VERIFIED FROM REAL DATA.**

- **Currency**: perfectly clean across all 8 issuers — every consolidated fact's unit is either
  `EUR` or (for a small number of pure ratios like EPS) no currency at all, exactly as expected;
  zero mixed-currency or unparseable-unit facts found anywhere.
- **Sign**: no anomalies found in the values spot-checked throughout this document (all cash-flow
  subtotals, profit figures, etc. carry the expected sign — no unexplained sign flips between
  issuers for the same concept).
- **Duplicate same-value facts within one filing — real, common, and already safe**: every issuer
  has 3-8 concepts where the *identical* value is tagged 2-4 times in one filing (e.g. NAI's
  `ifrs-full:ProfitLoss` = 3,892,000 EUR appears 4 times; ALO's same concept appears 4 times at
  365,000,000 EUR). This is real, normal ESEF/XBRL practice — the same fact is referenced from
  multiple primary-statement presentation roles (e.g. once on the income statement, once on the
  statement of comprehensive income, once in the equity roll-forward) — not a data error.
  **Verified safe**: `21d__dedup_clean_table.py`'s existing dedup key,
  `(ticker, stmt, concept, fiscal_year, period_type)` (`21d__dedup_clean_table.py:107-114`),
  already collapses these to one row regardless — the duplicates share every key-column value, so
  this is a pre-solved problem, not new risk introduced by adding more concepts.
- **Instant-fact date convention**: confirmed working correctly for a non-calendar fiscal year —
  Alstom's FY2026 filing (`period_end=2026-03-31`) tags its balance-sheet facts with
  `period="2026-04-01T00:00:00"`, exactly `period_end + 1 day`, and `is_current_period_fact()`
  correctly selects it — the exact mechanism `eu_current.py`'s own docstring already documents,
  now re-confirmed against a *second* fiscal year for the same issuer (the docstring's own
  evidence was FCC's FY2024; this research additionally confirms it for ALO's FY2026).
- **Scale/decimals**: not independently investigated — every value used in this document came
  through `extract_source_facts()`'s own `float(value)` conversion, already proven correct by
  Phase 5.1's live smoke test; not re-verified digit-by-digit here.

---

## 14. SEC/Canada comparison

**This is the section that answers the "does the canonical model need to change" question, and
the answer is no.**

For every Tier 1 and Tier 2 candidate in §5, the existing canonical model — built for SEC
`us-gaap` filers and already extended once for `ifrs-full` (Toyota/Vale/Infosys) — already has:
a canonical label, a `kind` classification, a `stmt` assignment, and (for §5a) an already-vetted
`ifrs-full` tag string. **Nothing in this research found a real European concept that requires
inventing a new canonical concept the existing model cannot represent.** The two real gaps found
(§5b) are not model gaps — they are missing *entries* in an existing, already-correctly-shaped
list (`IFRS_FALLBACK_TAGS`), the same kind of entry this codebase adds routinely for new SEC tag
variants (see `CONCEPT_SYNONYMS`'s own extensive precedent for exactly this pattern — a filer
using a different-but-equivalent tag for an already-canonical concept).

The one deliberately-considered exception — DO NOT create a European-only canonical concept for
ISP's/NAI's top line (§5c) — is itself consistent with the existing model's own precedent:
`CONCEPT_SYNONYMS` already has a `"Revenue (bank)"` entry
(`InterestAndDividendIncomeOperating` → `Revenue`) for `us-gaap` bank filers. **This is a real,
existing precedent this project has already decided is safe for US banks — meaning the question
"should ISP's `isp:InterestIncomeAndSimilarRevenues` map to Revenue the same way" is not
unprecedented, it is a genuine, answerable Tier 2/3 question this document defers (§17) rather
than resolving by analogy alone**, since the existing bank synonym maps a `us-gaap` *standard*
tag, not an issuer extension namespace — the EU case would need a second bank issuer to confirm
`isp:InterestIncomeAndSimilarRevenues` (or its standard-taxonomy equivalent, if one exists) is a
stable, common pattern before treating it the same way.

---

## 15. Frontend impact

**No `fundamentals_screener` code was inspected or modified in this phase** (per the prompt's own
non-goal) — this section states which existing, already-shipped features would receive real data
if Tier 1/2 mapping landed, distinguishing (per the prompt's own required framing) "frontend
currently broken" from "frontend works but data is NULL":

| Feature | Current state (per Phase 5.7's own live validation, unchanged since) | Classification |
|---|---|---|
| Overview KPI strip | Renders correctly; Revenue/Net Income cells show `—` for issuers lacking that concept | Frontend works, data is NULL |
| Income Statement / Balance Sheet / Cash Flow tabs | Render correctly (currency-labeled since Phase 5.7a); very few line items populate | Frontend works, data is NULL |
| Cash Flow tab specifically | Currently effectively empty for all 8 issuers (OCF/ICF/FCF unmapped) | Frontend works, data is NULL — Tier 1 alone (§5a) would populate all three subtotals for all 8 issuers immediately |
| Market Cap KPI | Absent for all 8 (needs Shares Diluted or an equivalent) | Frontend works, data is NULL — **not resolved by anything in this document** (§10) |
| Valuation tab (football field/MoS) | Absent for all 8 (needs Graham/DCF/Owner Earnings inputs) | Frontend works, data is NULL — partially unlocked by Tier 1 (more BS/CF inputs), still gated on Shares Diluted for per-share outputs |
| Net-Net Finder | Empty for `market=EU` today | Frontend works, data is NULL — directly unlocked by Total Current Assets/Liabilities (already 5/8 real, Tier 1) |
| Derived Metrics tab | Renders whatever `financials_metrics` computes; currently thin for EU | Frontend works, data is NULL |
| Quarterly tab | Correctly hidden (no non-FY period_type exists) | Frontend works, data is NULL — see §8, this is a source-availability fact, no frontend or mapping fix changes it |
| Forecasting tab | Depends on multi-year history depth for Revenue/Net Income/FCF | Frontend works, data is NULL — improves incrementally as more fiscal years accumulate, independent of this document |

Nothing found in this research indicates a `fundamentals_screener` bug — every gap traces to
missing canonical data, consistent with Phase 5.7's own conclusion.

---

## 16. Recommended Tier 1 mappings (safe, broadly reusable)

Concepts where the exact tag string is **already accepted elsewhere in this codebase** for real
IFRS filers, **and** independently confirmed present in real European data by this research:

Income Tax · Net Income (incl NCI) · Net Income · Total Assets · Cash & Equivalents ·
Operating Cash Flow · Investing Cash Flow · Financing Cash Flow · Total Stockholders Equity ·
Total Equity (incl NCI) · PP&E Net · Total Current Assets · Total Current Liabilities · Goodwill ·
Interest Expense (`FinanceCosts`) · Dividends Paid · Operating Income · Intangible Assets ·
Inventory · Cost of Revenue

**20 concepts, all direct reuse of already-vetted `ifrs-full` tags.** Implementing this tier is,
architecturally, widening `eu_current.py`'s `EU_CANONICAL_MAPPING` dict to include entries that
mirror `IFRS_FALLBACK_TAGS` — not a new mapping methodology.

## 17. Recommended Tier 2 mappings (safe but needs one targeted decision each)

- **Revenue — add `RevenueFromContractsWithCustomers` as a second accepted tag** (would fix ALO,
  FCT, SGO — the single highest-value fix in this document, given Revenue's downstream reach).
- **Accounts Receivable — the existing `IFRS_FALLBACK_TAGS` entry itself needs correcting**
  (`TradeReceivables` → `CurrentTradeReceivables` + `TradeAndOtherCurrentReceivables`, both real,
  both verified, neither currently matching).
- **Profit Before Tax — map to the existing `Income Before Tax` canonical concept** (currently has
  zero ifrs-full fallback at all — 7/8 real coverage waiting).
- **Finance Income — new fallback entry**, real 5/8 coverage, no existing canonical slot.
- **EPS Basic / EPS Diluted — 2-tag fallback list** (plain + `FromContinuingOperations` variant),
  closing a gap that also benefits existing `ifrs-full` US 20-F filers, not EU-specific.
- **CapEx — extend to the already-partial existing tag**, real for 4/8, `MEDIUM` confidence
  (genuine structural variation across issuers, not a mapping error).
- **Gross Profit, Total Liabilities — real but sparse (2/8 each)**, worth adding once a broader
  admitted universe gives a larger confirming sample; not blocked on anything, just lower priority
  given current coverage counts.

## 18. Deferred research (Tier 3 — should remain NULL for now)

- **Shares Diluted / any per-share share-count concept** — confirmed absent across all 8 real
  issuers (§10). Remains correctly NULL; no further mapping research will fix this without a
  different source or a derivation strategy this document does not propose.
- **Depreciation & Amortization as an isolated figure** — real but too sparse/inconsistently
  tagged (1/8 direct) to propose a reliable fallback list yet; would need real evidence from a
  larger admitted universe.
- **ISP (bank) and NAI (real-estate) top-line mapping** — a real, answerable question (§14) that
  needs a second same-sector issuer before deciding, not decided here.
- **Deferred Tax Assets/Liabilities (8/8 real coverage each)** — no canonical slot exists in the
  model at all today, for any filer type; adding one is a genuine, separate, cross-cutting design
  decision (affects SEC/Canada too), out of this phase's narrow research scope.
- **Dimensioned-fact recovery** (§7's "requires future work" item) — not investigated.
- **Interim/half-year ESEF data from a source other than `filings.xbrl.org`** — genuinely
  `UNKNOWN`, not investigated (§8).

## 19. Explicit non-goals (confirmed honored)

Per the prompt's own §16: no code in `eu_current.py`/`mapping.py`/the `financials` schema/any
Databricks table/`dashboard_data`/`fundamentals_screener`/Django/Streamlit/any public URL/the
Quarterly-Interim UI/the Reports-Filings UI/any existing SEC or Canada mapping was modified to
produce this document. The only repository change is this file plus the two Databricks-side
verification queries in §1/§13, both read-only `SELECT`s against `main.financials.financials` and
`main.config.eu_admission_candidates` — no `INSERT`/`UPDATE`/`MERGE`/`CREATE`/`DROP` was executed
anywhere, on any table, at any point in this research.

## 20. Proposed implementation plan (not started)

**Phase 6a** (small): widen `EU_CANONICAL_MAPPING` to the 20 Tier 1 concepts (§16) — each is a
literal copy of an already-running `IFRS_FALLBACK_TAGS` entry, re-pointed at the EU adapter's
existing `MappingDecision`/`MappingType.DIRECT` pattern, with a `notes=` string citing this
document's real evidence (mirroring the existing 5 entries' own citation style). Register the new
concepts in `concept_hierarchy.json` (a data change, not logic). No canonical model change.

**Phase 6b** (small-medium): the 7 Tier 2 items (§17), each landing as its own reviewed decision —
Revenue's second tag is the highest-priority single item in this whole document given its
downstream reach (§11, §13).

**Phase 6c** (deferred, needs its own research pass first): Deferred Tax Assets/Liabilities as a
new cross-cutting canonical concept; ISP/NAI top-line resolution once a second bank/REIT issuer is
admitted; a real investigation into whether interim ESEF data exists via any other source.

Every phase above is scoped narrowly enough to validate independently — none requires touching
`fundamentals_screener`, the public contract, or any US/Canada mapping.

---

## Final decision

**"How much can we realistically improve European financial-statement coverage without
guessing?"** Concretely: from 5 mapped concepts to **27** (20 Tier 1 + 7 Tier 2), using only
tags this research directly confirmed present in real filings — more than a 5x increase, and it
would fix a real, currently-live production gap (Revenue missing for 5 of 8 issuers) along the
way. Every one of those 27 is traceable to a specific, cited, real fact this document fetched
live. Shares Diluted and every EPS-input share-count concept should stay NULL — not because the
mapping effort is incomplete, but because the real data genuinely isn't there across all 8
issuers, confirmed directly rather than assumed.

**"Does the current canonical financial model need to change?"** **No.** Every real, safe mapping
candidate found in this research already has a home in the existing `INCOME_STATEMENT`/
`BALANCE_SHEET`/`CASH_FLOW`/`IFRS_FALLBACK_TAGS` model — the gap this whole research phase set out
to investigate turned out to be an under-populated *list* inside an already-correct architecture,
not an architectural limitation. The one place a genuinely new decision is needed (Deferred Tax
Assets/Liabilities, §18) is a pre-existing gap for *every* filer type, not a European-specific
one, and is explicitly deferred, not designed here.

---

## Implementation — Phase 6.1 (2026-08-17)

**Status: implemented, tested, and validated live against real production Databricks data.
Draft PR, not merged.**

### What was implemented

`fundamentals_pipeline/sources/eu_current.py`'s `EU_CANONICAL_MAPPING` widened from **5 to 21
canonical concepts (22 accepted source tags — Revenue has two)**. Every new entry is a verified,
direct reuse of a tag string already accepted for real `ifrs-full` filers via
`01__tickers.py`'s `IFRS_FALLBACK_TAGS` — no new canonical concept, no schema change, no change
to `01__tickers.py`/`21__clean_and_merge.py`/any SEC or Canada logic.

**IMPLEMENTED (16 new Tier 1 concepts, all HIGH confidence, §16):** Income Tax · Operating Cash
Flow · Investing Cash Flow · Financing Cash Flow · Total Stockholders Equity · Total Equity
(incl NCI) · PP&E Net · Total Current Assets · Total Current Liabilities · Goodwill · Interest
Expense (`ifrs-full:FinanceCosts` only — see below) · Dividends Paid · Operating Income ·
Intangible Assets · Inventory · Cost of Revenue.

**IMPLEMENTED — Revenue special handling (§17's Tier 2 item, re-verified per this prompt's own
§4 before implementing, not blindly copied from Phase 6.0):** `EU_CANONICAL_MAPPING`'s data
structure changed from `dict[str, MappingDecision]` to `dict[str, tuple[MappingDecision, ...]]`
specifically to let "Revenue" accept two source tags — `ifrs-full:Revenue` (already live) and
`ifrs-full:RevenueFromContractsWithCustomers` (new). Verified before implementing: the two
variants are mutually exclusive per issuer in every real filing fetched (no issuer tags both), so
this is a plain either/or lookup — no coalesce-with-priority machinery was built, since no real
evidence showed a need for one. Direct analogue to the already-accepted `us-gaap` ASC-606
`"Revenue (contract)"` synonym (`CONCEPT_SYNONYMS`) — same taxonomy-transition pattern, not a
novel decision.

**INTENTIONALLY NULL — Revenue for ISP and NAI:** neither issuer's real top-line concept was
added to the mapping. ISP (bank): `isp:InterestIncomeAndSimilarRevenues` is an issuer extension,
not a standard IFRS tag, and even the standard alternatives found
(`ifrs-full:RevenueFromDividends`, `ifrs-full:InterestRevenueCalculatedUsingEffectiveInterestMethod`)
are structurally different concepts (interest/dividend income components, not a single top-line
revenue figure) — mapping any of them to "Revenue" would misrepresent a bank's real statement
shape. NAI (real estate): `ifrs-full:RentalIncomeFromInvestmentProperty` is a genuinely different
IFRS concept from Revenue (rental income from investment property vs. revenue from contracts
with customers/goods and services), not merely a differently-named alias for the same thing —
Phase 6.0's own real-data evidence never established economic equivalence, so per this prompt's
explicit instruction ("NULL > questionable value"), it stays unmapped. Both are recorded as a
finding for future banking-/real-estate-specific coverage research, not designed here.

**INTENTIONALLY NULL — bare `ifrs-full:InterestExpense`:** only `ifrs-full:FinanceCosts` (5/8
real coverage) was mapped to "Interest Expense." Phase 6.0's own real-data search found **zero**
real occurrences of the bare `InterestExpense` tag across all 8 issuers — adding it would have
been an unverified guess masquerading as reuse of an existing SEC-side fallback list entry.

**INTENTIONALLY NULL — Shares Diluted:** no change. Re-confirmed, not re-derived: no entry was
added for "Shares Diluted," and none of the real per-issuer share-count concepts (ALO's own
7-concept share-roll-forward extension, SGO's single `WeightedAverageShares` instance) were used
to backfill it, exactly per this prompt's explicit instruction not to derive it from EPS, shares
outstanding, market cap, or price.

**DEFERRED (not implemented this pass, per Phase 6.0's own Tier 2/3 classification, unchanged):**
Gross Profit and Total Liabilities (real but sparse, `MEDIUM` confidence — §5a); the
`Accounts Receivable` tag-string correction, Profit Before Tax, Finance Income, EPS Basic/Diluted
2-tag fallback, and CapEx extension (all real §17 Tier 2 items needing their own targeted
decision, none touched in this pass to keep this implementation strictly to the highest-
confidence Tier 1 set plus the one Tier 2 item — Revenue — whose downstream value was judged
worth the extra scrutiny given it was also a live production gap).

### Data-quality re-verification (per this prompt's §1/§9, not blindly trusted from Phase 6.0)

Every Tier 1 candidate was re-checked against the live implementation, not just the research
doc's conclusions: canonical concept existence in `STATEMENTS`/`_LABEL_TO_STMT_KIND` confirmed
for all 16 (no `KeyError` risk — verified by successfully running `16__fetch_eu_xbrl.py` against
real Databricks data, see below); unit/currency handled by the unmodified, already-proven
`extract_source_facts()` (no change needed); period shape unaffected (still exclusively
`FY_or_TTM`/`snapshot`, per §8, re-confirmed by the real ingestion run producing only `fp="FY"`
rows); dimensions unaffected (`is_consolidated_fact()` untouched); duplicate within-filing facts
confirmed still safely collapsed by `21d__dedup_clean_table.py`'s existing
`(ticker, stmt, concept, fiscal_year, period_type)` key (unchanged, not touched this pass).

### Real Databricks validation (live, 2026-08-17)

Ran the actual code — not a simulation — via a personal validation Databricks Repo
(`/Repos/al.lopez.moreira@gmail.com/phase6-validation`, tracking this branch) against real
production tables, using the same one-time `jobs/runs/submit` pattern established in earlier
phases (never the scheduled production job).

**Step 1 — `16__fetch_eu_xbrl.py`** (append-only to `financials_raw`, structurally scoped to only
the 8 admitted EU tickers by `load_admitted_eu_entities()` — cannot touch any other ticker):
`SUCCESS`, 94s. `financials_raw` distinct-concept-per-ticker count before → after:

| Ticker | Concepts before | Concepts after |
|---|---|---|
| FCC | 5 | 19 |
| ALO | 4 | 21 |
| IBE | 5 | 19 |
| SGO | 4 | 20 |
| FCT | 3 | 13 |
| NAI | 4 | 15 |
| RAND | 5 | 18 |
| ISP | 4 | 9 |

**Step 2 — `21__clean_and_merge.py`** (merges the new raw scrape into the clean `financials`
table): confirmed **safe to run unscoped** before running it — its own read (`raw = spark.table
(raw_full).filter(F.col("scraped_at") == latest_scrape)`) and its orphan-delete step are both
already scoped to "whatever the most recent scrape contains," and `MAX(scraped_at)` was verified,
before running, to belong 100% to the just-completed `EU_CURRENT` scrape (648 rows, all 8 EU
tickers, zero other source/ticker) — so this run was provably incapable of touching any SEC/
Canada data before it was ever submitted. `SUCCESS`, 137s.

**`financials` (clean, `period_type='FY'`) — real before/after, distinct concepts per ticker:**

| Ticker | Concepts before | Concepts after | Revenue present after? |
|---|---|---|---|
| FCC | 4 | 17 | yes (already had it) |
| ALO | 3 | 19 | **yes — new** |
| IBE | 4 | 17 | yes (already had it) |
| SGO | 3 | 18 | **yes — new** |
| FCT | 3 | 12 | **yes — new** |
| NAI | 3 | 13 | no (correctly, intentionally NULL) |
| RAND | 4 | 16 | yes (already had it) |
| ISP | 3 | 8 | no (correctly, intentionally NULL) |

**The headline Phase 6.0 finding — Revenue missing for 5/8 issuers — is now fixed for exactly the
3 issuers it was fixable for (ALO, FCT, SGO), and correctly still absent for the 2 issuers (ISP,
NAI) where mapping it would have been a guess.** Real values, spot-checked: ALO FY2026 Revenue =
19,171,000,000 EUR (matches the exact real xBRL-JSON value Phase 6.0 cited).

### SEC / Canada regression (real, verified)

| Ticker | `financials` rows before | `financials` rows after |
|---|---|---|
| AAPL | 3,303 | 3,303 |
| MSFT | 2,977 | 2,977 |
| AEM | 344 | 344 |
| AQN | 306 | 306 |

**Byte-identical, zero regression** — expected given the scrape-timestamp scoping proof above,
now empirically confirmed. Whole-table `financials` row count: 4,750,416 → 4,750,782 (+366 rows,
entirely attributable to the EU expansion; no other ticker's row count changed).

### Downstream metric impact — determined analytically, not by a live `22`/`23` run

**A deliberate scope decision, not an oversight**: `22__derived_metrics.py` has **no
ticker-scoping mechanism at all** (confirmed directly — "no `tickers_override` in this notebook,
every run recomputes the entire universe," its own comment, line 1653) and `51__export_dashboard_
data.py` likewise always exports the full joined universe. Running either for real would mean
recomputing `financials_metrics`/re-exporting `dashboard_data` for the ENTIRE ~2,600+ ticker
production universe — a large, slow, broad production action, not proportionate to validating an
8-ticker mapping change, and neither was unambiguously instructed the way the `16`/`21` scoped
validation was. Downstream impact is instead determined analytically, directly from the now-real
`financials` state above cross-referenced against `22__derived_metrics.py`'s actual formulas
(re-confirmed by direct code read, same method Phase 6.0 §11 used):

| Metric | Real inputs newly available | Issuers newly unlocked |
|---|---|---|
| Operating Margin % | Operating Income ∩ Revenue | ALO, FCC, IBE, RAND (4) |
| Current Ratio | Total Current Assets ∩ Total Current Liabilities | FCC, IBE, NAI, RAND, SGO (5) |
| Quick Ratio | + Inventory | IBE, SGO (2) |
| Working Capital | Total Assets (8/8) − Total Current Liabilities | FCC, IBE, NAI, RAND, SGO (5) |
| Tangible Book Value | Total Assets (8/8) − Goodwill − Intangible Assets (both `COALESCE(...,0)` — degrades gracefully) | more accurate for all 8, not just newly non-null |
| Goodwill / Total Assets % | Goodwill ∩ Total Assets | ALO, FCC, IBE, RAND, SGO (5) |
| Interest Coverage | Operating Income ∩ Interest Expense | ALO, FCC, IBE, RAND (4) |

**Still NOT unlocked by this pass** (confirms, doesn't contradict, Phase 6.0's own findings):
Free Cash Flow (needs CapEx — deferred, §17 Tier 2), EBITDA/Gross Margin % (need Depreciation &
Amortization / Gross Profit — deferred), any per-share metric (needs Shares Diluted — correctly
stays NULL, §7).

### Frontend impact — inferred, not live-checked this pass

Per this prompt's own non-goal (§16: do not modify `fundamentals_screener`) and given `51`/`52`
were not run live (see above), this was not visually confirmed in a browser this pass. It is,
however, a safe, direct inference from two already-proven facts: Phase 5.7's own live validation
already confirmed `CompanyRepository.get_statements()` displays *whatever* is in `financials`/
`dashboard_data` generically, with no per-concept allowlist; and this pass just proved
`financials` now contains 16 new real concepts (120 (ticker, concept) rows across the 8 issuers,
up from 27) with real values. The Income Statement/Balance Sheet/Cash Flow tabs will show
materially more line items the next time `dashboard_data` is republished and synced — genuinely
expected, not yet observed.

### Tests

16 new/updated tests in `tests/test_sources_eu_current.py`: every new Tier 1 mapping resolves to
its expected canonical concept; Revenue's dual-tag routing; the bare `InterestExpense` tag stays
unmapped; every ISP/NAI real top-line concept stays unmapped; `"Shares Diluted"` is absent from
`EU_CANONICAL_MAPPING` entirely. Full repo suite: 355 passed, 2 skipped (pre-existing, fixture-
gated, unrelated). `ruff check`: clean.

### Known limitations / remaining technical debt

- **7 real Tier 2 candidates not implemented this pass** (§17, unchanged from Phase 6.0):
  `Accounts Receivable`'s existing tag string still doesn't match any real EU filing (0/8 verbatim
  — needs correcting to `CurrentTradeReceivables`/`TradeAndOtherCurrentReceivables`), Profit
  Before Tax, Finance Income, EPS Basic/Diluted, CapEx extension, Gross Profit, Total Liabilities.
- **Free Cash Flow and EBITDA remain unavailable** for all 8 issuers pending the CapEx/D&A Tier 2
  work above.
- **No live browser/frontend confirmation** this pass (see above) — deferred to whenever
  `dashboard_data` is next republished, consistent with the tracked follow-up already in this
  document's status banner.
- **ISP/NAI Revenue resolution** remains a real, open, deferred research question (§18) — not
  designed or decided in this pass, correctly left NULL.

---

## Final validation before merge (2026-08-17)

**The gap flagged above — no live browser/dashboard confirmation — is now closed.** Ran the full
`financials → dashboard_data → website` path for real, live, against production.

### Real financials check (read-only)

Full coverage confirmed for all 8 issuers (✓ = concept has at least one real value):

| Ticker | Revenue | Op Income | Cost of Rev | Income Tax | OCF | Equity | PP&E | Curr Assets | Curr Liab | Inventory | Goodwill | Interest Exp | Dividends |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| FCC | ✓ | ✓ | — | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| ALO | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| IBE | ✓ | ✓ | — | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| SGO | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ |
| FCT | ✓ | — | — | ✓ | ✓ | ✓ | ✓ | — | — | — | — | ✓ | ✓ |
| NAI | — | ✓ | — | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — | ✓ |
| RAND | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | — |
| ISP | — | — | — | ✓ | ✓ | — | — | — | — | — | — | — | ✓ |

Every absence matches a real, already-documented reason (ISP's bank structure; NAI's real-estate
structure and Revenue decision; genuine per-issuer presentation gaps like FCT not splitting
current/noncurrent) — no unexpected gap found.

### Downstream metrics — computed read-only via SQL, not via a live `22` run

Per this prompt's own instruction not to run a full-universe `22`/`23` merely to prove
availability, every ratio below was computed directly against the real `financials` values with
a plain read-only `SELECT` (no write, no notebook run) — the real numbers a correctly-scoped `22`
run would produce, verified by construction rather than inferred:

| Ticker | FY | Current Ratio | Operating Margin % | Tangible Book Value | Goodwill/Assets % | Interest Coverage |
|---|---|---|---|---|---|---|
| FCC | 2024 | 1.62 | 8.0% | 13.47B | 5.4% | 2.75x |
| ALO | 2026 | *(only 1 yr on record)* | 2.8% | 24.12B | 26.1% | 2.67x |
| IBE | 2024 | 0.69 | 21.7% | 138.04B | 5.4% | 2.46x |
| SGO | 2025 | 1.27 | *(gap this FY)* | 41.14B | 23.7% | *(gap this FY)* |
| RAND | 2025 | 1.21 | 2.2% | 7.56B | 29.3% | 6.65x |
| NAI | 2025 | 1.68 | n/a (no Revenue) | 127.7M | n/a | n/a |
| FCT, ISP | — | n/a (no current/noncurrent split) | n/a | 9.56B / 975.68B (Total Assets only) | n/a | n/a |

Real, computable, non-fabricated values for at least Current Ratio/Operating Margin %/Tangible
Book Value/Goodwill%/Interest Coverage across 4-5 of 8 issuers — confirming Phase 6.1's own
analytical projection, now with actual numbers instead of a coverage-overlap argument. The gaps
(ALO/SGO missing a ratio for their *specific latest fiscal year* despite having the concept in
general) are real, per-year presentation sparseness — not a mapping defect.

### Dashboard export — the critical integration check

Ran `51__export_dashboard_data.py` for real (read-only against every Delta table; writes only
parquet + the `_publish` Volume — no table was modified). Verified directly against the produced
`dashboard_data.parquet`: **identical concept coverage to the live `financials` table** — the raw
statement facts (Revenue, Operating Income, Cost of Revenue, Total Current Assets/Liabilities,
Goodwill, Interest Expense, Dividends Paid, …) for all 8 issuers pass through to the export
artifact unchanged. **Also verified, precisely, what does NOT yet pass through**: queried
`dashboard_metrics.parquet` for `Current Ratio`/`Operating Margin %`/`Working Capital`/`Tangible
Book Value`/`Goodwill / Total Assets %`/`Interest Coverage` — **zero rows** for any of the 8
issuers. This is the expected, honest state: `financials_metrics` (and therefore
`dashboard_metrics`'s computed ratios) has not been recomputed since Phase 6.1's mapping change,
because `22__derived_metrics.py` was deliberately not run (no ticker-scoping exists there, and a
full-universe recompute wasn't proportionate to this validation — unchanged from Phase 6.1's own
reasoning). **The raw-facts leg of `financials → dashboard_data` is proven working end-to-end
today; the computed-ratios leg (`financials → financials_metrics → dashboard_metrics`) requires a
future `22`/`23` production run before it reflects the new coverage** — this is a real,
documented sequencing fact, not a defect.

Published for real: `52__publish_to_github.py` ran successfully, `latest` release confirmed
non-draft (`published_at: 2026-08-17T11:39:01Z`).

### Real website validation (live, all 8 issuers + regression)

Forced a resync on the consumer site (the same `workflow_dispatch` mechanism used in Phase 5.7a)
and checked the real, live, production HTML directly:

| Ticker | HTTP | EUR badges | Stray `$` | Revenue row visible? | New Tier 1 rows visible (sample) |
|---|---|---|---|---|---|
| FCC | 200 | 90 | 0 | yes (9.07B) | Operating Income, Total Current Assets/Liabilities, Goodwill, Interest Expense, Dividends |
| ALO | 200 | 79 | 0 | **yes — 19.17B, matches the real xBRL value exactly** | Cost of Revenue, Operating Income, Total Current Assets/Liabilities, Goodwill, Interest Expense |
| IBE | 200 | 94 | 0 | yes (44.74B) | Operating Income, Total Current Assets/Liabilities, Goodwill, Intangibles, Interest Expense |
| SGO | 200 | 79 | 0 | **yes — 46.48B, matches real xBRL value** | Cost of Revenue, Operating Income, Total Current Assets/Liabilities, Goodwill, Dividends |
| FCT | 200 | 43 | 0 | **yes — 7.95B, matches real xBRL value** | Interest Expense, PP&E, Dividends |
| NAI | 200 | 46 | 0 | correctly absent (`—`) | Operating Income, Total Current Assets/Liabilities, PP&E |
| RAND | 200 | 89 | 0 | yes (23.08B) | Cost of Revenue, Operating Income, Total Current Assets/Liabilities, Goodwill, Interest Expense |
| ISP | 200 | 24 | 0 | correctly absent (`—`) | Income Tax, Operating/Investing/Financing CF, Dividends only (bank-shaped, as expected) |

**Every page: zero error markers (no traceback/500-style content), zero stray `$` anywhere.** The
Price tab's close price for FCC renders `11.10 EUR` (badge, not `$`) — Phase 5.7a's currency
work confirmed still correct under the new, much larger volume of EU statement data.
`pane-valuation` confirmed absent (0 occurrences) on FCC's page — the Valuation tab correctly
still doesn't render, consistent with `financials_metrics`/`financials_intrinsic_value` not
having been recomputed (see above) — an honest gap, not a bug.

### Currency regression (live)

- FCC → `11.10 EUR` (Price tab), all KPIs EUR-badged. ✓
- ALO → EUR throughout, Revenue now visible and correctly EUR, not `$`. ✓
- AQN → unchanged, `2.43B`/`180.8M`/`14.14B`/`593.6M` with **299 CAD badges** — byte-identical
  badge count to Phase 5.7a's own original validation. ✓
- AAPL → unchanged, `$416.16B`/`$112.01B`/`$359.24B`/`$111.48B`/`$4.45T`, **zero** currency
  badges. ✓

No newly populated European figure rendered as `$` anywhere on any of the 8 real pages checked.

### US / Canada regression (live)

AAPL and AQN pages: `200`, content byte-identical in shape to Phase 5.7a's own original
validation (same KPI values, same badge counts) — confirms the Phase 6.1 mapping expansion and
this validation's publish cycle changed nothing for non-European tickers, live, not just at the
Delta-table level already proven in Phase 6.1's own report.

### Tests

Unchanged from Phase 6.1: `pytest` 355 passed / 2 skipped, `ruff check` clean. No new tests added
— no regression was found that would warrant one, per this prompt's own instruction.

### Final classification

# READY TO MERGE

- **Real EU financial coverage**: confirmed live, in production `financials` and in the
  published `dashboard_data` artifact — 16 new Tier 1 concepts + the Revenue fix, all real,
  all verified with actual values (not row counts alone).
- **Downstream metric validation**: Current Ratio, Operating Margin %, Tangible Book Value,
  Goodwill/Total Assets %, and Interest Coverage are real-computable today for 4-5 of 8 issuers
  (verified via direct read-only SQL against production data); not yet reflected in
  `financials_metrics`/`dashboard_metrics` pending a future `22`/`23` run — documented, not
  hidden.
- **`dashboard_data` validation**: confirmed identical coverage to `financials`, published live.
- **Browser validation**: all 8 issuers checked directly against real production HTML — correct
  values, correct currency, zero errors, zero stray `$`.
- **Currency validation**: FCC/ALO/EUR, AQN/CAD, AAPL/USD all confirmed correct, live.
- **US regression**: AAPL unchanged, live.
- **Canada regression**: AQN unchanged, live (badge count identical to Phase 5.7a's own record).
- **Remaining Tier 2 gaps** (unchanged, correctly deferred, not touched this pass): Accounts
  Receivable's existing tag string (0/8 real match — flagged as a documented gap, not silently
  patched with an unverified alias, exactly per this prompt's own instruction), Profit Before
  Tax, Finance Income, EPS Basic/Diluted, CapEx, Gross Profit, Total Liabilities. ISP/NAI Revenue
  and Shares Diluted remain intentionally, permanently NULL pending future evidence.

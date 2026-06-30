# Databricks notebook source
# MAGIC %md
# MAGIC # 00__config / 01__tickers
# MAGIC
# MAGIC Core constants and XBRL concept maps shared by all notebooks.
# MAGIC
# MAGIC - **Catalog / schema / table names** — single source of truth
# MAGIC - **XBRL concept maps** — used by ingestion and transformation notebooks
# MAGIC - **Concept kind** — `flow_additive` / `flow_nonadditive` / `stock`
# MAGIC   → controls quarterly derivation logic in `21b__derive_quarterly`
# MAGIC
# MAGIC > The ticker universe is managed in `02__tickers_master`, which writes to `main.config.tickers`.
# MAGIC > Ingestion notebooks read from that table directly.

# COMMAND ----------

# ── Catalog & Schema ──────────────────────────────────────────────────────────

CATALOG   = "main"
SCHEMA    = "financials"
RAW_TABLE = "financials_raw"
TABLE     = "financials"

SEC_USER_AGENT = "Alejandro Lopez Moreira al.lopez.moreira@gmail.com"

# Convenience
DB           = f"{CATALOG}.{SCHEMA}"
TICKERS_TABLE = f"{CATALOG}.config.tickers"

FAVORITES_JSON_PATH = "../00__config/favorites.json"

# ── Quarterly retention window ────────────────────────────────────────────────
QUARTERLY_WINDOW = 12

# ── Set context (assumes catalog/schema already exist) ────────────────────────
# We do NOT issue CREATE CATALOG / CREATE SCHEMA here because in many
# Unity Catalog workspaces the user lacks CREATE CATALOG at the metastore level
# and the statement hangs waiting for an approval that never comes. Catalog and
# schemas should be created once, manually, by an admin.
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA  {SCHEMA}")

print(f"Config loaded:")
print(f"  Target            : {DB}.{TABLE}")
print(f"  Raw table         : {DB}.{RAW_TABLE}")
print(f"  Tickers table     : {TICKERS_TABLE}")
print(f"  Quarterly window  : last {QUARTERLY_WINDOW} quarters per ticker")

# COMMAND ----------

# MAGIC %md ## XBRL concept maps
# MAGIC
# MAGIC Each concept carries a **kind**:
# MAGIC
# MAGIC | Kind | Examples | Quarterly logic |
# MAGIC |---|---|---|
# MAGIC | `flow_additive` | Revenue, Net Income, OCF, CapEx | Standalone if exists, else `YTD_n − YTD_(n-1)`; Q4 = FY − YTD_Q3 |
# MAGIC | `flow_nonadditive` | EPS, Shares Diluted | Standalone only; if not reported in 10-Q, leave NULL |
# MAGIC | `stock` | Assets, Cash, Equity (all BS) | Snapshot at `period_end`; dedupe by `(ticker, concept, period_end)` keeping latest `filed` |

# COMMAND ----------

INCOME_STATEMENT = {
    "Revenue":                    ("Revenues",                                                                                  "flow_additive"),
    "Revenue (contract)":         ("RevenueFromContractWithCustomerExcludingAssessedTax",                                       "flow_additive"),
    "Revenue (contract incl tax)":("RevenueFromContractWithCustomerIncludingAssessedTax",                                       "flow_additive"),
    "Revenue (sales net)":        ("SalesRevenueNet",                                                                           "flow_additive"),
    "Revenue (sales goods)":      ("SalesRevenueGoodsNet",                                                                      "flow_additive"),
    "Revenue (sales services)":   ("SalesRevenueServicesNet",                                                                   "flow_additive"),
    "Revenue (bank)":             ("InterestAndDividendIncomeOperating",                                                        "flow_additive"),
    "Revenue (oil & gas)":        ("OilAndGasRevenue",                                                                          "flow_additive"),
    "Cost of Revenue":            ("CostOfRevenue",                                                                             "flow_additive"),
    "Gross Profit":               ("GrossProfit",                                                                               "flow_additive"),
    "Operating Expenses":         ("OperatingExpenses",                                                                         "flow_additive"),
    "R&D Expense":                ("ResearchAndDevelopmentExpense",                                                             "flow_additive"),
    "SG&A Expense":               ("SellingGeneralAndAdministrativeExpense",                                                    "flow_additive"),
    "Operating Income":           ("OperatingIncomeLoss",                                                                       "flow_additive"),
    "Interest Expense":           ("InterestExpense",                                                                           "flow_additive"),
    "Interest Expense (nonoperating)": ("InterestExpenseNonoperating",                                                          "flow_additive"),
    "Interest Expense (incl debt)":    ("InterestAndDebtExpense",                                                               "flow_additive"),
    "Income Before Tax":          ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest","flow_additive"),
    "Income Tax":                 ("IncomeTaxExpenseBenefit",                                                                   "flow_additive"),
    "Net Income":                 ("NetIncomeLoss",                                                                             "flow_additive"),
    "Net Income (to common)":     ("NetIncomeLossAvailableToCommonStockholdersBasic",                                           "flow_additive"),
    "Net Income (incl NCI)":      ("ProfitLoss",                                                                                "flow_additive"),
    "EPS Basic":                  (["EarningsPerShareBasic",                            # standard basic EPS
                                    "IncomeLossFromContinuingOperationsPerBasicShare",  # two-class / continuing-ops filers
                                    "EarningsPerShareBasicAndDiluted"],                 # single combined tag (basic == diluted)
                                   "flow_nonadditive"),
    "EPS Diluted":                (["EarningsPerShareDiluted",                          # standard diluted EPS (wins whenever present)
                                    "IncomeLossFromContinuingOperationsPerDilutedShare",# two-class / continuing-ops filers (ABNB, REG, STAG...)
                                    "EarningsPerShareBasicAndDiluted",                  # single combined tag (basic == diluted)
                                    "EarningsPerShareBasic"],                           # last resort: negligible dilution (BRK-B, TR)
                                   "flow_nonadditive"),
    "Shares Diluted":             ("WeightedAverageNumberOfDilutedSharesOutstanding",                                           "flow_nonadditive"),
}

BALANCE_SHEET = {
    "Cash & Equivalents":         ("CashAndCashEquivalentsAtCarryingValue",      "stock"),
    "Short-term Investments":     ("ShortTermInvestments",                       "stock"),
    "Accounts Receivable":        ("AccountsReceivableNetCurrent",               "stock"),
    "Inventory":                  ("InventoryNet",                               "stock"),
    "Total Current Assets":       ("AssetsCurrent",                              "stock"),
    "PP&E Net":                   ("PropertyPlantAndEquipmentNet",               "stock"),
    "Goodwill":                   ("Goodwill",                                   "stock"),
    # List of tags in priority order (extract_series_multi, first-hit-wins). Many filers
    # (esp. those with indefinite-lived intangibles like trademarks) don't tag
    # FiniteLivedIntangibleAssetsNet at all — the aggregate ExcludingGoodwill tag is the
    # more general/common one and is tried first; FiniteLived is kept as a fallback for
    # filers that only break out the finite-lived piece. Fixes understated Intangible
    # Assets → overstated Tangible Book Value for those filers.
    "Intangible Assets":          (["IntangibleAssetsNetExcludingGoodwill",
                                     "FiniteLivedIntangibleAssetsNet",
                                     "OtherIntangibleAssetsNet"],               "stock"),
    "Total Assets":               ("Assets",                                     "stock"),
    "Accounts Payable":           ("AccountsPayableCurrent",                     "stock"),
    # Debt uses a LIST of tags. Long-term Debt COALESCES (first-hit-wins per period, see
    # extract_series_multi). Short-term Debt is special: DebtCurrent is the us-gaap AGGREGATE of
    # ShortTermBorrowings (commercial paper) + LongTermDebtCurrent (current maturities of LT debt),
    # which are DISJOINT additive lines. Coalescing dropped one when a filer presents both without
    # the aggregate (LIN/WMT) → understated leverage. So Short-term Debt is registered in
    # AGGREGATE_OR_SUM_CONCEPTS below and resolved by extract_series_aggregate_or_sum (aggregate if
    # present, else sum the components). The list here stays the full tag set for the oracle /
    # dimensional / reconciler readers (13/14/35), which only need to know the tags → concept map.
    "Short-term Debt":            (["DebtCurrent",            # AGGREGATE — total current debt (wins when present)
                                    "LongTermDebtCurrent",    # component — current maturities of long-term debt
                                    "ShortTermBorrowings"],   # component — commercial paper / revolver
                                   "stock"),
    "Total Current Liabilities":  ("LiabilitiesCurrent",                         "stock"),
    # Order: prefer the noncurrent/current split first to avoid double-counting;
    # fall back to the LongTermDebt aggregate only if the split is not reported.
    # ⚠ Double-count risk: if a filer reports ONLY LongTermDebt (which ALREADY includes the
    # current portion) and also a Short-term Debt tag, the current portion is
    # counted twice. Acceptable approximation for a leverage ratio.
    "Long-term Debt":             (["LongTermDebtNoncurrent",                    # original (current standard)
                                    "LongTermDebt",                              # total long-term debt — used by many large issuers (AT&T style)
                                    "LongTermDebtAndCapitalLeaseObligations"],   # when leases are included
                                   "stock"),
    "Total Liabilities":          ("Liabilities",                                "stock"),
    "Additional Paid-in Capital": ("AdditionalPaidInCapital",                    "stock"),
    "Retained Earnings":          ("RetainedEarningsAccumulatedDeficit",         "stock"),
    "Total Stockholders Equity":  ("StockholdersEquity",                         "stock"),
    "Total Equity (incl NCI)":    ("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "stock"),
    "Total Liabilities & Equity": ("LiabilitiesAndStockholdersEquity",           "stock"),
}

CASH_FLOW = {
    "Net Income":                  ("NetIncomeLoss",                                                 "flow_additive"),
    "Net Income (to common)":      ("NetIncomeLossAvailableToCommonStockholdersBasic",               "flow_additive"),
    "Net Income (incl NCI)":       ("ProfitLoss",                                                    "flow_additive"),
    # ── Depreciation & Amortization — per-period fallback list (extract_series_multi, coalesce,
    # never sums). The aggregate DepreciationDepletionAndAmortization is the canonical line and
    # wins when present (covers BSM/KRP/DMLP and the vast majority of filers). Oil & gas
    # royalty/mineral companies that report DEPLETION as an isolated cash-flow line and OMIT the
    # aggregate came out NULL (VNOM: DepletionOfOilAndGasProperties = 214,412 / 146,118 / 121,071
    # for FY24/23/22, aggregate absent). The standalone depletion tags fill those periods only when
    # the aggregate is missing; full-cost filers use the …FullCostMethod… variant. Bare `Depletion`
    # is EXCLUDED on purpose — it is generic (mining/timber) and a classic summable component, the
    # OUT_OF_SCOPE (component-sum) case tracked in 81__cashflow_coverage.MECHANISM.
    "Depreciation & Amortization": (["DepreciationDepletionAndAmortization",
                                     "DepletionOfOilAndGasProperties",
                                     "OilAndGasPropertyFullCostMethodDepletion"],
                                    "flow_additive"),
    "Stock-based Compensation":    ("ShareBasedCompensation",                                        "flow_additive"),
    "Changes in Working Capital":  ("IncreaseDecreaseInOperatingCapital",                            "flow_additive"),
    "Operating Cash Flow":         ("NetCashProvidedByUsedInOperatingActivities",                    "flow_additive"),
    "Operating Cash Flow (cont ops)": ("NetCashProvidedByUsedInOperatingActivitiesContinuingOperations", "flow_additive"),
    # ── CapEx — per-period fallback list (extract_series_multi). PP&E is the canonical tag;
    # many filers (small-cap, oil & gas) tag capex as ProductiveAssets, CapitalImprovements or
    # OtherPP&E. Recovery measured on the NULL set: ProductiveAssets 9/18 > PP&E 5/18.
    # PaymentsToAcquireMachineryAndEquipment is EXCLUDED (0 recovery, and it's the candidate most
    # likely to be a summable component line rather than an aggregate). Per-period coalesce: the
    # highest-priority tag present that year wins; two tags are NEVER summed — a filer that reports
    # PP&E + machinery as separate lines could undercount (acceptable approximation, same as the
    # balance-sheet debt chain).
    "CapEx":                       (["PaymentsToAcquirePropertyPlantAndEquipment",
                                     "PaymentsToAcquireProductiveAssets",
                                     "PaymentsForCapitalImprovements",
                                     "PaymentsToAcquireOtherPropertyPlantAndEquipment",
                                     "PaymentsToAcquireOilAndGasPropertyAndEquipment"],
                                    "flow_additive"),
    # ── Acquisitions — net-of-cash is the canonical line; some filers report the gross.
    # Recovery: NetOfCash 10/19, Gross 5/19. (AndInterestInAffiliates 0/19 → excluded.)
    "Acquisitions":                (["PaymentsToAcquireBusinessesNetOfCashAcquired",
                                     "PaymentsToAcquireBusinessesGross"],
                                    "flow_additive"),
    # ── Purchases of Investments — the aggregate PaymentsToAcquireInvestments is almost never
    # used (1/17 on the NULL set); filers tag by asset class (marketable, AFS-debt, HTM,
    # short-term). The aggregate stays at priority 0 (wins when present → no undercount); the
    # asset-class tags fill in when the aggregate is absent. ⚠ Coalesce-vs-sum risk: a filer that
    # reports AFS-debt + HTM as SEPARATE lines and WITHOUT the aggregate will undercount (coalesce
    # picks only one). Acceptable approximation; the per-sum refinement is deferred (see D&A).
    "Purchases of Investments":    (["PaymentsToAcquireInvestments",
                                     "PaymentsToAcquireMarketableSecurities",
                                     "PaymentsToAcquireAvailableForSaleSecuritiesDebt",
                                     "PaymentsToAcquireAvailableForSaleSecurities",
                                     "PaymentsToAcquireHeldToMaturitySecurities",
                                     "PaymentsToAcquireShortTermInvestments"],
                                    "flow_additive"),
    # ── Sales of Investments — the current tag (ProceedsFromSaleOfInvestments) is DEAD: 0/18 on
    # the NULL set and 0 rows in the whole table. Rebuilt from the sale/maturity-by-asset-class
    # variants. Same ⚠ coalesce-vs-sum risk as Purchases (sale and maturity are often separate
    # lines that SUM to total proceeds); the broadest aggregate is prioritized.
    # ProceedsFromSaleOfPropertyPlantAndEquipment is NOT included (that is PP&E disposal, a
    # different investing line — not a sale of financial investments).
    "Sales of Investments":        (["ProceedsFromSaleOfInvestments",
                                     "ProceedsFromSaleOfAvailableForSaleSecuritiesDebt",
                                     "ProceedsFromSaleOfAvailableForSaleSecurities",
                                     "ProceedsFromSaleMaturityAndCollectionsOfInvestments",
                                     "ProceedsFromSaleAndMaturityOfMarketableSecurities",
                                     "ProceedsFromMaturitiesPrepaymentsAndCallsOfAvailableForSaleSecurities",
                                     "ProceedsFromSaleOfShortTermInvestments"],
                                    "flow_additive"),
    "Investing Cash Flow":         ("NetCashProvidedByUsedInInvestingActivities",                    "flow_additive"),
    # cont-ops: filers with discontinued operations (RCAT FY2024/FY2025) report the investing
    # subtotal only as ...ContinuingOperations → the base comes out NULL. Synonym+priority below.
    "Investing Cash Flow (cont ops)": ("NetCashProvidedByUsedInInvestingActivitiesContinuingOperations", "flow_additive"),
    # ── Debt Issuance — DEFERRED this pass (revolver-vs-term risk: ProceedsFromLinesOfCredit
    # mixes revolving-line draws with term-debt issuance). Pending a semantics/sum decision.
    # Keep the single tag.
    "Debt Issuance":               ("ProceedsFromIssuanceOfLongTermDebt",                            "flow_additive"),
    # ── Debt Repayment — fallback with alternative repayment lines (aggregate, notes,
    # convertibles). Recovery: NotesPayable 5/16, Convertible 4/16, LongTermDebt 4/16. Revolving
    # credit lines (Repayments...LinesOfCredit) are EXCLUDED for the same revolver-vs-term reason
    # that deferred Debt Issuance — only term debt / aggregate / notes / convertibles.
    "Debt Repayment":              (["RepaymentsOfLongTermDebt",
                                     "RepaymentsOfDebt",
                                     "RepaymentsOfNotesPayable",
                                     "RepaymentsOfConvertibleDebt"],
                                    "flow_additive"),
    # ── Dividends Paid — DEFERRED this pass. The aggregate PaymentsOfDividends coexists with
    # components (PaymentsOfDividendsCommonStock / Preferred / MinorityInterest). A coalesce to
    # common-only would undercount the total when only components are reported → pending SUM logic
    # (like D&A). Keep the single aggregate tag.
    "Dividends Paid":              ("PaymentsOfDividends",                                           "flow_additive"),
    # ── Share Repurchases — the common-stock cash line is canonical (11/17); the Equity aggregate
    # is next. StockRepurchasedDuringPeriodValue (statement-of-changes-in-equity value of shares/
    # units repurchased) is the LAST-resort fallback: LP/MLP filers and the LP→C-corp conversion
    # year tag the buyback ONLY there, not under any cash-flow PaymentsForRepurchase* tag (VNOM
    # FY2023: $95,221K under StockRepurchasedDuringPeriodValue in the FY2023 LP 10-K; the cash tag
    # carries 2023 only as a later-filing comparative that 21's comparative guard drops). Per-period
    # coalesce + lowest priority → it ONLY fills periods where both cash tags are absent and can
    # NEVER overwrite a published cash value. Preferred-stock repurchase is EXCLUDED (a separate
    # summable component, not an equivalent aggregate).
    "Share Repurchases":           (["PaymentsForRepurchaseOfCommonStock",
                                     "PaymentsForRepurchaseOfEquity",
                                     "StockRepurchasedDuringPeriodValue"],
                                    "flow_additive"),
    "Financing Cash Flow":         ("NetCashProvidedByUsedInFinancingActivities",                    "flow_additive"),
    # cont-ops: same pattern as Investing CF (RCAT) → ...ContinuingOperations. Synonym+priority below.
    "Financing Cash Flow (cont ops)": ("NetCashProvidedByUsedInFinancingActivitiesContinuingOperations", "flow_additive"),
    # ── Net Change in Cash — variants of the SAME total (NOT summable): Including-FX is the real
    # period change in cash; Excluding-FX drops the exchange-rate effect; the pre-2018 tag
    # (CashAndCashEquivalentsPeriodIncreaseDecrease, no restricted) covers the older years.
    # Recovery on the NULL set: Excluding 15/16, pre-2018 14/16. Coalesce by priority (Including
    # first) is correct — they are NOT summed, they are versions of the same number.
    "Net Change in Cash":          (["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
                                     "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect",
                                     "CashAndCashEquivalentsPeriodIncreaseDecrease"],
                                    "flow_additive"),
}

STATEMENTS = {
    "Income Statement": INCOME_STATEMENT,
    "Balance Sheet":    BALANCE_SHEET,
    "Cash Flow":        CASH_FLOW,
}

# ── Aggregate-or-sum concepts ──────────────────────────────────────────────────
# Concepts whose value is an AGGREGATE over genuinely additive sub-lines. At ingestion (11),
# extract_series_aggregate_or_sum resolves these PER FILING CONTEXT: use the `aggregate` tag when
# the filer reports it (it already folds in the parts → no double-count), else SUM the `sum`
# components present for that context. This is the exception to the default COALESCE (one tag wins
# per period) used by every other multi-tag concept — coalescing here dropped a disjoint component
# and understated the line (Short-term Debt for commercial-paper issuers; confirmed by the
# linkbase-oracle reconciliation on LIN/WMT). Keyed by the canonical concept label; the matching
# STATEMENTS entry keeps the full tag list (aggregate + components) for the 13/14/35 readers.
AGGREGATE_OR_SUM_CONCEPTS = {
    "Short-term Debt": {
        "aggregate": "DebtCurrent",
        "sum":       ["LongTermDebtCurrent", "ShortTermBorrowings"],
    },
}

# COMMAND ----------

# MAGIC %md ## XBRL concept synonyms
# MAGIC
# MAGIC Some filers change their XBRL tag between years (mergers, reorganizations,
# MAGIC adoption of a new taxonomy). To avoid losing historical data, the merge in
# MAGIC `21__clean_and_merge.py` collapses these aliases to the canonical concept **after**
# MAGIC ingestion.
# MAGIC
# MAGIC Format: `"alternative_label": "canonical_label"` — both must appear as keys
# MAGIC in one of the STATEMENTS above to be ingested correctly.
# MAGIC When merging them, the dedup by `(ticker, stmt, concept, fy)` keeps the
# MAGIC most recent `filed`.

# COMMAND ----------

CONCEPT_SYNONYMS = {
    # ASC 606 post-2018 variants
    "Revenue (contract)":          "Revenue",   # RevenueFromContractWithCustomerExcludingAssessedTax
    "Revenue (contract incl tax)": "Revenue",   # RevenueFromContractWithCustomerIncludingAssessedTax (FLUT, RGTI, SOUN, DJCO, …)

    # Sales* pre-ASC 606 variants (PEP, KR, TPR, ECL, WULF, EPC, DJCO, …)
    "Revenue (sales net)":         "Revenue",   # SalesRevenueNet
    "Revenue (sales goods)":       "Revenue",   # SalesRevenueGoodsNet
    "Revenue (sales services)":    "Revenue",   # SalesRevenueServicesNet

    # Banks (MS, GS, GSBC, BKU, WAFD, RC, ESQ, BMRC, BCAL, NBBK, AGM)
    # Domain assumption: a bank's top-line = InterestAndDividendIncomeOperating.
    # Risk: if a filer reports BOTH Revenues and InterestAndDividendIncomeOperating
    # in the same fy, the dedup keeps the larger value — rare in practice because
    # banks don't usually report the Revenues tag.
    "Revenue (bank)":              "Revenue",   # InterestAndDividendIncomeOperating

    # Oil & gas — filers with a sector-specific concept (no usual overlap with Revenues)
    "Revenue (oil & gas)":         "Revenue",   # OilAndGasRevenue

    # ── Net Income ─────────────────────────────────────────────────────────────
    # Many companies stop tagging `NetIncomeLoss` in the 10-K and switch to
    # `ProfitLoss` (incl. non-controlling interests) or the
    # "available to common" line (after preferred dividends). Without these synonyms
    # FY Net Income is absent or frozen years back (→ ROE/Net Margin/P-E
    # stale). Affects ~125 tickers: AEE, OXY, LYB, QSR, JEF, MORN, PAYX, VRSN…
    # ⚠️ UNLIKE Revenue, here the three tags COEXIST in the same fy, so
    # the tiebreak cannot be `value desc` (would pick the largest = incl-NCI).
    # CONCEPT_PRIORITY (below) enforces attributable-first preference.
    "Net Income (to common)":      "Net Income",   # NetIncomeLossAvailableToCommonStockholdersBasic
    "Net Income (incl NCI)":       "Net Income",   # ProfitLoss

    # ── Total Stockholders Equity ──────────────────────────────────────────────
    # Companies with material NCI report `…IncludingPortionAttributableToNoncontrollingInterest`.
    # We prefer the equity attributable to shareholders (StockholdersEquity) for ROE.
    "Total Equity (incl NCI)":     "Total Stockholders Equity",  # StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest

    # ── Operating / Investing / Financing Cash Flow (cont ops) ─────────────────
    # Filers with discontinued operations report the cash-flow subtotals only as
    # ...ContinuingOperations instead of the base (RCAT: Investing/Financing came out NULL for
    # FY2024 and FY2025). Same mechanism as OCF: they collapse to the canonical at merge.
    "Operating Cash Flow (cont ops)": "Operating Cash Flow",  # NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
    "Investing Cash Flow (cont ops)": "Investing Cash Flow",  # NetCashProvidedByUsedInInvestingActivitiesContinuingOperations
    "Financing Cash Flow (cont ops)": "Financing Cash Flow",  # NetCashProvidedByUsedInFinancingActivitiesContinuingOperations

    # ── Interest Expense ───────────────────────────────────────────────────────
    # Filers migrate the interest line in the income statement away from
    # `InterestExpense`: VZ uses `InterestExpenseNonoperating` since fy2024 (InterestExpense
    # absent → Interest Coverage came out NULL); others use the `InterestAndDebtExpense` aggregate.
    # Both are GROSS expense (positive magnitude), so the F.abs(...) in Interest Coverage is
    # safe. NET tags (InterestIncomeExpenseNet, …) are EXCLUDED on purpose: they net income
    # against expense → ambiguous sign → would corrupt the ratio (a net-cash position gives nonsense coverage).
    "Interest Expense (nonoperating)": "Interest Expense",   # InterestExpenseNonoperating (VZ fy2024+)
    "Interest Expense (incl debt)":    "Interest Expense",   # InterestAndDebtExpense
}

# COMMAND ----------

# MAGIC %md ## Concept priority (tie-break between coexisting synonyms)
# MAGIC
# MAGIC When multiple XBRL tags collapse to the same canonical concept via
# MAGIC `CONCEPT_SYNONYMS` and **coexist in the same `fy`** (typical for Net Income:
# MAGIC `NetIncomeLoss` + `ProfitLoss` in the same 10-K), the `value desc` tiebreak
# MAGIC in the merge is INCORRECT (would pick the largest). `CONCEPT_PRIORITY` defines the
# MAGIC preference order by **source label** (lower = preferred). `21` and `21b` inject it
# MAGIC into the Window dedup `ORDER BY`, BEFORE `value desc`.
# MAGIC
# MAGIC Any label not listed → priority 0. Revenue is ALREADY prioritized below (like
# MAGIC Net Income): its synonyms coexist frequently (~6.5k fy) and `value desc` was picking the largest.

# COMMAND ----------

CONCEPT_PRIORITY = {
    # Net Income: attributable > to-common > incl-NCI
    "Net Income":             0,
    "Net Income (to common)": 1,
    "Net Income (incl NCI)":  2,
    # Equity: attributable to shareholders > incl-NCI
    "Total Stockholders Equity": 0,
    "Total Equity (incl NCI)":   1,
    # OCF / Investing / Financing: base > continuing-operations
    "Operating Cash Flow":            0,
    "Operating Cash Flow (cont ops)": 1,
    "Investing Cash Flow":            0,
    "Investing Cash Flow (cont ops)": 1,
    "Financing Cash Flow":            0,
    "Financing Cash Flow (cont ops)": 1,
    # Revenue: total revenue line (Revenues) > ASC-606 contract > Sales variants >
    # sector tags. Revenue synonyms COEXIST far more than the old comment assumed
    # (~6.5k fy, ~4.2k differ >2%): without priority, `value desc` was picking the
    # LARGEST tag (e.g. WMB fy2024: 12.632 from contract over 10.503 from Revenues, which is
    # the income-statement total and what its quarters sum to). `Revenues` matches ΣQ
    # 1982 vs 1103 from contract when both coexist. Pure-ASC606 filers report only
    # `contract` (no coexistence → priority is a no-op for them, no regression).
    "Revenue":                     0,   # us-gaap:Revenues (total on the face of the statement)
    "Revenue (contract)":          1,   # RevenueFromContractWithCustomerExcludingAssessedTax
    "Revenue (contract incl tax)": 2,   # …IncludingAssessedTax (includes sales tax → less preferred)
    "Revenue (sales net)":         3,   # SalesRevenueNet (pre-ASC606)
    "Revenue (sales goods)":       4,   # SalesRevenueGoodsNet
    "Revenue (sales services)":    5,   # SalesRevenueServicesNet
    "Revenue (oil & gas)":         6,   # OilAndGasRevenue
    "Revenue (bank)":              7,   # InterestAndDividendIncomeOperating
    # Interest Expense: primary income statement line > nonoperating > aggregate.
    # Rarely coexist in the same fy (filers migrate from one to another), so this
    # is almost always a no-op; included in case a 10-K reports more than one.
    "Interest Expense":                0,   # InterestExpense
    "Interest Expense (nonoperating)": 1,   # InterestExpenseNonoperating
    "Interest Expense (incl debt)":    2,   # InterestAndDebtExpense
}

# COMMAND ----------

# MAGIC %md ## Statement-scoped priority overrides (Cash Flow Net Income)
# MAGIC
# MAGIC `CONCEPT_PRIORITY` above is the global default and is correct for the Income Statement
# MAGIC (attributable `NetIncomeLoss` wins → EPS/ROE). The **Cash Flow** statement needs the
# MAGIC OPPOSITE Net Income preference: GAAP's indirect-method reconciliation starts from the
# MAGIC CONSOLIDATED net income (`ProfitLoss`, incl. non-controlling interest) — the figure that,
# MAGIC after the listed adjustments, equals Operating Cash Flow. For NCI-heavy filers (VNOM:
# MAGIC ~40% NCI via Diamondback's stake in the operating LLC — FY2024 attributable 359,245 vs
# MAGIC consolidated 603,646) the attributable figure that wins on the Income Statement is the
# MAGIC wrong CF start. `CONCEPT_PRIORITY_BY_STMT` inverts the Net Income order for `Cash Flow`
# MAGIC only; every other (stmt, label) falls back to the global `CONCEPT_PRIORITY` via
# MAGIC `concept_priority()`. Consumed by `21` and `21b` (Spark `_prio` column) and `35` (oracle
# MAGIC winner) — `concept_priority()` below is the single source of truth; keep the three in sync.

# COMMAND ----------

CONCEPT_PRIORITY_BY_STMT = {
    "Cash Flow": {
        "Net Income (incl NCI)":  0,   # ProfitLoss — consolidated, the indirect-method start
        "Net Income":             1,   # NetIncomeLoss — attributable to parent
        "Net Income (to common)": 2,
    },
}


def concept_priority(stmt: str, label: str) -> int:
    """Merge tie-break priority for a (stmt, source label) pair — lower = preferred.

    Statement-scoped overrides in CONCEPT_PRIORITY_BY_STMT win; otherwise the global
    CONCEPT_PRIORITY applies; unlisted labels → 0 (no preference). `21`/`21b` replicate this
    in Spark (chained F.when on stmt + concept); `35` calls it directly. Keep them in sync.
    """
    _over = CONCEPT_PRIORITY_BY_STMT.get(stmt)
    if _over is not None and label in _over:
        return _over[label]
    return CONCEPT_PRIORITY.get(label, 0)

# COMMAND ----------

# MAGIC %md ## Combined-filers (10-K dimensional)
# MAGIC
# MAGIC Tickers whose 10-K covers **two registrants** (REIT + Operating Partnership, e.g.
# MAGIC Tanger Inc. / SKT). Each primary-statement line carries a `dei:LegalEntityAxis` member,
# MAGIC so the SEC `companyfacts` API (which drops dimensional facts) does NOT return the
# MAGIC annual totals → `21` cannot find the FY. `10__ingestion/13__fetch_dimensional_10k`
# MAGIC processes ONLY the tickers in this dict: downloads the 10-K XBRL instance and extracts the
# MAGIC fact for the **parent-entity member** (`member`), emitting it without dimension.
# MAGIC
# MAGIC Kept separate from `favorites.json` **intentionally**: being here does NOT mark the ticker as
# MAGIC a favorite; it only activates dimensional retrieval. `cik` is optional (override if SEC cannot
# MAGIC resolve the ticker). Empty dict → `13` is a no-op.
# MAGIC
# MAGIC `member` = local-name of the parent member (validated against the 10-K instance, not
# MAGIC guessed). SKT→`TangerIncMember` verified FY2024: Revenue 526.06M, Net Income 98.595M.

# COMMAND ----------

COMBINED_FILERS = {
    "SKT": {"member": "TangerIncMember", "cik": "0000899715"},  # Tanger Inc. + Tanger Properties LP
}

# COMMAND ----------

# MAGIC %md ## Helper — classify XBRL period by duration

# COMMAND ----------

def classify_period_shape(start, end):
    """
    Given a fact's `start` and `end` dates, classify the period:
        Q_standalone  ~90d   (single quarter)
        YTD_6M       ~180d   (6 months YTD, Q2 cumulative)
        YTD_9M       ~270d   (9 months YTD, Q3 cumulative)
        FY_or_TTM    ~365d   (full year)
        snapshot     (start is NaT — stock concept)
        other_Xd     (anything else — flagged for inspection)
    """
    import pandas as pd
    if pd.isna(start):
        return "snapshot"
    days = (pd.to_datetime(end) - pd.to_datetime(start)).days
    if   70  <= days <=  100: return "Q_standalone"
    elif 160 <= days <=  200: return "YTD_6M"
    elif 250 <= days <=  290: return "YTD_9M"
    elif 350 <= days <=  380: return "FY_or_TTM"
    else:                     return f"other_{days}d"


def classify_period_shape_series(start, end):
    """Vectorized equivalent of `classify_period_shape` over pandas Series.

    Produces identical labels (snapshot iff `start` is NaT; same inclusive day
    buckets; same `other_<days>d` fallback) but without the per-row Python call and
    per-row `pd.to_datetime`/`import pandas` of the scalar version. This is the hot
    path in `11__fetch_sec_xbrl.extract_series`, called ~60 concepts × ~2.5k tickers;
    the old `df.apply(..., axis=1)` was GIL-bound and dominated stage-11 CPU time.
    """
    import numpy as np
    import pandas as pd
    start = pd.to_datetime(start, errors="coerce")
    end   = pd.to_datetime(end,   errors="coerce")
    days  = (end - start).dt.days                       # NaN where start is NaT
    days_str = days.astype("Int64").astype("string")    # "91" / <NA>, no ".0"
    return pd.Series(
        np.select(
            [
                start.isna(),
                (days >= 70)  & (days <= 100),
                (days >= 160) & (days <= 200),
                (days >= 250) & (days <= 290),
                (days >= 350) & (days <= 380),
            ],
            ["snapshot", "Q_standalone", "YTD_6M", "YTD_9M", "FY_or_TTM"],
            default=("other_" + days_str + "d"),         # unused where start is NaT
        ),
        index=end.index,
    )


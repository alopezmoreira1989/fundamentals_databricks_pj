# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 01__tickers
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

SEC_USER_AGENT = "MyCompany myemail@example.com"   # ← update with your details

# Convenience
DB           = f"{CATALOG}.{SCHEMA}"
TICKERS_TABLE = f"{CATALOG}.config.tickers"

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
    "Cost of Revenue":            ("CostOfRevenue",                                                                             "flow_additive"),
    "Gross Profit":               ("GrossProfit",                                                                               "flow_additive"),
    "Operating Expenses":         ("OperatingExpenses",                                                                         "flow_additive"),
    "R&D Expense":                ("ResearchAndDevelopmentExpense",                                                             "flow_additive"),
    "SG&A Expense":               ("SellingGeneralAndAdministrativeExpense",                                                    "flow_additive"),
    "Operating Income":           ("OperatingIncomeLoss",                                                                       "flow_additive"),
    "Interest Expense":           ("InterestExpense",                                                                           "flow_additive"),
    "Income Before Tax":          ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest","flow_additive"),
    "Income Tax":                 ("IncomeTaxExpenseBenefit",                                                                   "flow_additive"),
    "Net Income":                 ("NetIncomeLoss",                                                                             "flow_additive"),
    "EPS Basic":                  ("EarningsPerShareBasic",                                                                     "flow_nonadditive"),
    "EPS Diluted":                ("EarningsPerShareDiluted",                                                                   "flow_nonadditive"),
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
    "Intangible Assets":          ("FiniteLivedIntangibleAssetsNet",             "stock"),
    "Total Assets":               ("Assets",                                     "stock"),
    "Accounts Payable":           ("AccountsPayableCurrent",                     "stock"),
    "Short-term Debt":            ("ShortTermBorrowings",                        "stock"),
    "Total Current Liabilities":  ("LiabilitiesCurrent",                         "stock"),
    "Long-term Debt":             ("LongTermDebtNoncurrent",                     "stock"),
    "Total Liabilities":          ("Liabilities",                                "stock"),
    "Additional Paid-in Capital": ("AdditionalPaidInCapital",                    "stock"),
    "Retained Earnings":          ("RetainedEarningsAccumulatedDeficit",         "stock"),
    "Total Stockholders Equity":  ("StockholdersEquity",                         "stock"),
    "Total Liabilities & Equity": ("LiabilitiesAndStockholdersEquity",           "stock"),
}

CASH_FLOW = {
    "Net Income":                  ("NetIncomeLoss",                                                                                                     "flow_additive"),
    "Depreciation & Amortization": ("DepreciationDepletionAndAmortization",                                                                              "flow_additive"),
    "Stock-based Compensation":    ("ShareBasedCompensation",                                                                                            "flow_additive"),
    "Changes in Working Capital":  ("IncreaseDecreaseInOperatingCapital",                                                                                "flow_additive"),
    "Operating Cash Flow":         ("NetCashProvidedByUsedInOperatingActivities",                                                                       "flow_additive"),
    "CapEx":                       ("PaymentsToAcquirePropertyPlantAndEquipment",                                                                       "flow_additive"),
    "Acquisitions":                ("PaymentsToAcquireBusinessesNetOfCashAcquired",                                                                     "flow_additive"),
    "Purchases of Investments":    ("PaymentsToAcquireInvestments",                                                                                     "flow_additive"),
    "Sales of Investments":        ("ProceedsFromSaleOfInvestments",                                                                                    "flow_additive"),
    "Investing Cash Flow":         ("NetCashProvidedByUsedInInvestingActivities",                                                                       "flow_additive"),
    "Debt Issuance":               ("ProceedsFromIssuanceOfLongTermDebt",                                                                               "flow_additive"),
    "Debt Repayment":              ("RepaymentsOfLongTermDebt",                                                                                         "flow_additive"),
    "Dividends Paid":              ("PaymentsOfDividends",                                                                                              "flow_additive"),
    "Share Repurchases":           ("PaymentsForRepurchaseOfCommonStock",                                                                               "flow_additive"),
    "Financing Cash Flow":         ("NetCashProvidedByUsedInFinancingActivities",                                                                       "flow_additive"),
    "Net Change in Cash":          ("CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",   "flow_additive"),
}

STATEMENTS = {
    "Income Statement": INCOME_STATEMENT,
    "Balance Sheet":    BALANCE_SHEET,
    "Cash Flow":        CASH_FLOW,
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


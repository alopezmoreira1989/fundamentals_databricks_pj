# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 00_tickers
# MAGIC Master configuration file. **This is the only file you edit to add/remove companies.**
# MAGIC All other notebooks read from here via `%run ../config/tickers`.

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 00_tickers
# MAGIC Master configuration file. **This is the only file you edit to add/remove companies.**
# MAGIC All other notebooks read from here via `%run ../config/tickers`.

# COMMAND ----------

# ── Catalog & Schema ──────────────────────────────────────────────────────────

CATALOG   = "main"
SCHEMA    = "financials"
RAW_TABLE = "financials_raw"
TABLE     = "financials"

SEC_USER_AGENT = "MyCompany myemail@example.com"   # ← update with your details

# ── Master Ticker List ────────────────────────────────────────────────────────
# Add a new company by adding one dict to this list. Nothing else needs to change.
#
# Fields:
#   ticker    (str)  required  — exchange ticker symbol
#   name      (str)  required  — human-readable company name
#   sector    (str)  optional  — used for grouping in dashboards
#   active    (bool) optional  — set False to pause scraping without deleting

TICKERS = [
    # ── Technology ────────────────────────────────────────────────────────────
    {"ticker": "AAPL",  "name": "Apple Inc.",             "sector": "Technology", "active": True},
    {"ticker": "MSFT",  "name": "Microsoft Corp.",         "sector": "Technology", "active": True},
    {"ticker": "GOOGL", "name": "Alphabet Inc.",           "sector": "Technology", "active": True},
    {"ticker": "META",  "name": "Meta Platforms Inc.",     "sector": "Technology", "active": True},
    {"ticker": "NVDA",  "name": "NVIDIA Corp.",            "sector": "Technology", "active": True},

    # ── Consumer ──────────────────────────────────────────────────────────────
    {"ticker": "AMZN",  "name": "Amazon.com Inc.",         "sector": "Consumer",   "active": True},
    {"ticker": "TSLA",  "name": "Tesla Inc.",              "sector": "Consumer",   "active": True},

    # ── Financials ────────────────────────────────────────────────────────────
    {"ticker": "JPM",   "name": "JPMorgan Chase & Co.",    "sector": "Financials", "active": True},
    {"ticker": "V",     "name": "Visa Inc.",               "sector": "Financials", "active": True},

    # ── Healthcare ────────────────────────────────────────────────────────────
    {"ticker": "JNJ",   "name": "Johnson & Johnson",       "sector": "Healthcare", "active": True},

    # ── Add new companies below this line ─────────────────────────────────────
    # {"ticker": "TSM",  "name": "Taiwan Semiconductor",   "sector": "Technology", "active": True},
]

# ── Convenience helpers (used by other notebooks) ─────────────────────────────

# Only tickers flagged as active
ACTIVE_TICKERS = [t["ticker"] for t in TICKERS if t.get("active", True)]

# Lookup: ticker → full metadata dict
TICKER_META = {t["ticker"]: t for t in TICKERS}

# Tickers grouped by sector
from collections import defaultdict
TICKERS_BY_SECTOR = defaultdict(list)
for t in TICKERS:
    if t.get("active", True):
        TICKERS_BY_SECTOR[t.get("sector", "Other")].append(t["ticker"])

# ── XBRL Concept Maps ─────────────────────────────────────────────────────────
# Centralised here so every notebook uses the same definitions.

INCOME_STATEMENT = {
    "Revenue":                    "Revenues",
    "Revenue (contract)":         "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Cost of Revenue":            "CostOfRevenue",
    "Gross Profit":               "GrossProfit",
    "Operating Expenses":         "OperatingExpenses",
    "R&D Expense":                "ResearchAndDevelopmentExpense",
    "SG&A Expense":               "SellingGeneralAndAdministrativeExpense",
    "Operating Income":           "OperatingIncomeLoss",
    "Interest Expense":           "InterestExpense",
    "Income Before Tax":          "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    "Income Tax":                 "IncomeTaxExpenseBenefit",
    "Net Income":                 "NetIncomeLoss",
    "EPS Basic":                  "EarningsPerShareBasic",
    "EPS Diluted":                "EarningsPerShareDiluted",
    "Shares Diluted":             "WeightedAverageNumberOfDilutedSharesOutstanding",
}

BALANCE_SHEET = {
    "Cash & Equivalents":         "CashAndCashEquivalentsAtCarryingValue",
    "Short-term Investments":     "ShortTermInvestments",
    "Accounts Receivable":        "AccountsReceivableNetCurrent",
    "Inventory":                  "InventoryNet",
    "Total Current Assets":       "AssetsCurrent",
    "PP&E Net":                   "PropertyPlantAndEquipmentNet",
    "Goodwill":                   "Goodwill",
    "Intangible Assets":          "FiniteLivedIntangibleAssetsNet",
    "Total Assets":               "Assets",
    "Accounts Payable":           "AccountsPayableCurrent",
    "Short-term Debt":            "ShortTermBorrowings",
    "Total Current Liabilities":  "LiabilitiesCurrent",
    "Long-term Debt":             "LongTermDebtNoncurrent",
    "Total Liabilities":          "Liabilities",
    "Additional Paid-in Capital": "AdditionalPaidInCapital",
    "Retained Earnings":          "RetainedEarningsAccumulatedDeficit",
    "Total Stockholders Equity":  "StockholdersEquity",
    "Total Liabilities & Equity": "LiabilitiesAndStockholdersEquity",
}

CASH_FLOW = {
    "Net Income":                  "NetIncomeLoss",
    "Depreciation & Amortization": "DepreciationDepletionAndAmortization",
    "Stock-based Compensation":    "ShareBasedCompensation",
    "Changes in Working Capital":  "IncreaseDecreaseInOperatingCapital",
    "Operating Cash Flow":         "NetCashProvidedByUsedInOperatingActivities",
    "CapEx":                       "PaymentsToAcquirePropertyPlantAndEquipment",
    "Acquisitions":                "PaymentsToAcquireBusinessesNetOfCashAcquired",
    "Purchases of Investments":    "PaymentsToAcquireInvestments",
    "Sales of Investments":        "ProceedsFromSaleOfInvestments",
    "Investing Cash Flow":         "NetCashProvidedByUsedInInvestingActivities",
    "Debt Issuance":               "ProceedsFromIssuanceOfLongTermDebt",
    "Debt Repayment":              "RepaymentsOfLongTermDebt",
    "Dividends Paid":              "PaymentsOfDividends",
    "Share Repurchases":           "PaymentsForRepurchaseOfCommonStock",
    "Financing Cash Flow":         "NetCashProvidedByUsedInFinancingActivities",
    "Net Change in Cash":          "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
}

STATEMENTS = {
    "Income Statement": INCOME_STATEMENT,
    "Balance Sheet":    BALANCE_SHEET,
    "Cash Flow":        CASH_FLOW,
}

# ── Create catalog & schema if they don't exist ───────────────────────────────
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# ── Print summary on load ──────────────────────────────────────────────────────
print(f"Config loaded:")
print(f"  Target    : {CATALOG}.{SCHEMA}.{TABLE}")
print(f"  Tickers   : {len(ACTIVE_TICKERS)} active — {ACTIVE_TICKERS}")
print(f"  Sectors   : {dict(TICKERS_BY_SECTOR)}")
print(f"  Schema    : ✓ {CATALOG}.{SCHEMA} ready")

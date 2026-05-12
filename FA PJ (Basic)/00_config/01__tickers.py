# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config / 01__tickers
# MAGIC
# MAGIC Core constants and XBRL concept maps shared by all notebooks.
# MAGIC
# MAGIC - **Catalog / schema / table names** — single source of truth
# MAGIC - **XBRL concept maps** — used by ingestion and transformation notebooks
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

# ── Create catalog & schema if they don't exist ───────────────────────────────
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.config")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA  {SCHEMA}")

print(f"Config loaded:")
print(f"  Target        : {DB}.{TABLE}")
print(f"  Tickers table : {TICKERS_TABLE}")
print(f"  Schema        : ✓ {DB} ready")

# COMMAND ----------

# MAGIC %md ## XBRL concept maps
# MAGIC Centralised here so every notebook uses the same definitions.

# COMMAND ----------

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

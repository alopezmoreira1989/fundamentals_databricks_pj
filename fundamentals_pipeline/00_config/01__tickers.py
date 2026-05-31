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

SEC_USER_AGENT = "Alejandro Lopez Moreira al.lopez.moreira@gmail.com"

# Convenience
DB           = f"{CATALOG}.{SCHEMA}"
TICKERS_TABLE = f"{CATALOG}.config.tickers"

FAVORITES_JSON_PATH = "../00_config/favorites.json"

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
    "Income Before Tax":          ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest","flow_additive"),
    "Income Tax":                 ("IncomeTaxExpenseBenefit",                                                                   "flow_additive"),
    "Net Income":                 ("NetIncomeLoss",                                                                             "flow_additive"),
    "Net Income (to common)":     ("NetIncomeLossAvailableToCommonStockholdersBasic",                                           "flow_additive"),
    "Net Income (incl NCI)":      ("ProfitLoss",                                                                                "flow_additive"),
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
    # Debt usa LISTA de tags en orden de prioridad (first-hit-wins en la ingesta,
    # ver extract_series_multi en 11__fetch_sec_xbrl.py). Muchos filers (p.ej. T/VZ)
    # NO reportan bajo ShortTermBorrowings/LongTermDebtNoncurrent → la columna salía
    # NULL → Total Debt=0 → Debt/Equity=0.00. El fallback cubre las variantes us-gaap.
    "Short-term Debt":            (["DebtCurrent",            # porción corriente de la deuda total (más general)
                                    "LongTermDebtCurrent",    # porción corriente de la deuda LP
                                    "ShortTermBorrowings"],   # original
                                   "stock"),
    "Total Current Liabilities":  ("LiabilitiesCurrent",                         "stock"),
    # Orden: preferir el split noncurrent/current primero para NO doble-contar;
    # caer al agregado LongTermDebt sólo si el split no se reporta.
    # ⚠ Riesgo doble-conteo: si un filer reporta SÓLO LongTermDebt (que YA incluye la
    # porción corriente) y además un tag de Short-term Debt, la porción corriente se
    # cuenta dos veces. Aproximación aceptable para un ratio de apalancamiento.
    "Long-term Debt":             (["LongTermDebtNoncurrent",                    # original (estándar actual)
                                    "LongTermDebt",                              # deuda LP total — usada por muchos grandes emisores (estilo AT&T)
                                    "LongTermDebtAndCapitalLeaseObligations"],   # cuando los leases van incluidos
                                   "stock"),
    "Total Liabilities":          ("Liabilities",                                "stock"),
    "Additional Paid-in Capital": ("AdditionalPaidInCapital",                    "stock"),
    "Retained Earnings":          ("RetainedEarningsAccumulatedDeficit",         "stock"),
    "Total Stockholders Equity":  ("StockholdersEquity",                         "stock"),
    "Total Equity (incl NCI)":    ("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "stock"),
    "Total Liabilities & Equity": ("LiabilitiesAndStockholdersEquity",           "stock"),
}

CASH_FLOW = {
    "Net Income":                  ("NetIncomeLoss",                                                                                                     "flow_additive"),
    "Net Income (to common)":      ("NetIncomeLossAvailableToCommonStockholdersBasic",                                                                   "flow_additive"),
    "Net Income (incl NCI)":       ("ProfitLoss",                                                                                                        "flow_additive"),
    "Depreciation & Amortization": ("DepreciationDepletionAndAmortization",                                                                              "flow_additive"),
    "Stock-based Compensation":    ("ShareBasedCompensation",                                                                                            "flow_additive"),
    "Changes in Working Capital":  ("IncreaseDecreaseInOperatingCapital",                                                                                "flow_additive"),
    "Operating Cash Flow":         ("NetCashProvidedByUsedInOperatingActivities",                                                                       "flow_additive"),
    "Operating Cash Flow (cont ops)": ("NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",                                                "flow_additive"),
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

# MAGIC %md ## XBRL concept synonyms
# MAGIC
# MAGIC Algunos emisores cambian de tag XBRL entre años (fusiones, reorganizaciones,
# MAGIC adopción de un taxonomy nuevo). Para que no perdamos histórico, el merge en
# MAGIC `21__clean_and_merge.py` colapsa estos alias al concepto canónico **después**
# MAGIC de la ingesta.
# MAGIC
# MAGIC Formato: `"label_alternativo": "label_canónico"` — ambos deben aparecer como
# MAGIC keys en alguno de los STATEMENTS de arriba para que ingesten correctamente.
# MAGIC Al fusionarlos, la dedup por `(ticker, stmt, concept, fy)` se queda con el
# MAGIC `filed` más reciente.

# COMMAND ----------

CONCEPT_SYNONYMS = {
    # Variantes ASC 606 post-2018
    "Revenue (contract)":          "Revenue",   # RevenueFromContractWithCustomerExcludingAssessedTax
    "Revenue (contract incl tax)": "Revenue",   # RevenueFromContractWithCustomerIncludingAssessedTax (FLUT, RGTI, SOUN, DJCO, …)

    # Variantes Sales* pre-ASC 606 (PEP, KR, TPR, ECL, WULF, EPC, DJCO, …)
    "Revenue (sales net)":         "Revenue",   # SalesRevenueNet
    "Revenue (sales goods)":       "Revenue",   # SalesRevenueGoodsNet
    "Revenue (sales services)":    "Revenue",   # SalesRevenueServicesNet

    # Bancos (MS, GS, GSBC, BKU, WAFD, RC, ESQ, BMRC, BCAL, NBBK, AGM)
    # Asunción de dominio: top-line de un banco = InterestAndDividendIncomeOperating.
    # Riesgo: si un emisor reporta TANTO Revenues como InterestAndDividendIncomeOperating
    # en el mismo fy, la dedup se queda con el valor mayor — raro en la práctica porque
    # los bancos no suelen reportar el tag Revenues.
    "Revenue (bank)":              "Revenue",   # InterestAndDividendIncomeOperating

    # Oil & gas — emisores con concept específico del sector (sin overlap habitual con Revenues)
    "Revenue (oil & gas)":         "Revenue",   # OilAndGasRevenue

    # ── Net Income ─────────────────────────────────────────────────────────────
    # Muchas empresas dejan de etiquetar `NetIncomeLoss` en el 10-K y pasan a
    # `ProfitLoss` (incl. participaciones no controladoras) o a la línea
    # "available to common" (tras dividendos preferentes). Sin estos sinónimos el
    # FY Net Income queda ausente o congelado años atrás (→ ROE/Net Margin/P-E
    # obsoletos). Afecta ~125 tickers: AEE, OXY, LYB, QSR, JEF, MORN, PAYX, VRSN…
    # ⚠️ A DIFERENCIA de Revenue, aquí los tres tags COEXISTEN en el mismo fy, así
    # que el desempate no puede ser `value desc` (elegiría el mayor = incl-NCI).
    # CONCEPT_PRIORITY (abajo) fuerza la preferencia attributable-first.
    "Net Income (to common)":      "Net Income",   # NetIncomeLossAvailableToCommonStockholdersBasic
    "Net Income (incl NCI)":       "Net Income",   # ProfitLoss

    # ── Total Stockholders Equity ──────────────────────────────────────────────
    # Empresas con NCI material reportan `…IncludingPortionAttributableToNoncontrollingInterest`.
    # Preferimos el equity atribuible al accionista (StockholdersEquity) para ROE.
    "Total Equity (incl NCI)":     "Total Stockholders Equity",  # StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest

    # ── Operating Cash Flow ────────────────────────────────────────────────────
    # Muchas reportan la variante "continuing operations" en vez de la base.
    "Operating Cash Flow (cont ops)": "Operating Cash Flow",  # NetCashProvidedByUsedInOperatingActivitiesContinuingOperations
}

# COMMAND ----------

# MAGIC %md ## Concept priority (tie-break entre sinónimos que coexisten)
# MAGIC
# MAGIC Cuando varios tags XBRL colapsan al mismo concepto canónico vía
# MAGIC `CONCEPT_SYNONYMS` y **coexisten en el mismo `fy`** (típico de Net Income:
# MAGIC `NetIncomeLoss` + `ProfitLoss` en el mismo 10-K), el desempate por `value desc`
# MAGIC del merge es INCORRECTO (elegiría el mayor). `CONCEPT_PRIORITY` define el orden
# MAGIC de preferencia por **label de origen** (menor = preferido). `21` y `21b` lo
# MAGIC insertan en el `ORDER BY` del Window de dedup, ANTES de `value desc`.
# MAGIC
# MAGIC Cualquier label no listado → prioridad 0 (preserva el comportamiento previo de
# MAGIC Revenue, cuyos sinónimos rara vez coexisten y se resuelven por `value desc`).

# COMMAND ----------

CONCEPT_PRIORITY = {
    # Net Income: attributable > to-common > incl-NCI
    "Net Income":             0,
    "Net Income (to common)": 1,
    "Net Income (incl NCI)":  2,
    # Equity: atribuible al accionista > incl-NCI
    "Total Stockholders Equity": 0,
    "Total Equity (incl NCI)":   1,
    # OCF: base > continuing-operations
    "Operating Cash Flow":            0,
    "Operating Cash Flow (cont ops)": 1,
}

# COMMAND ----------

# MAGIC %md ## Combined-filers (10-K dimensional)
# MAGIC
# MAGIC Tickers cuyo 10-K cubre **dos registrantes** (REIT + Operating Partnership, p.ej.
# MAGIC Tanger Inc. / SKT). Cada línea del estado primario lleva un miembro `dei:LegalEntityAxis`,
# MAGIC así que la API SEC `companyfacts` (que descarta facts dimensionales) NO devuelve los
# MAGIC totales anuales → `21` no encuentra FY. `10_ingestion/13__fetch_dimensional_10k`
# MAGIC procesa SOLO los tickers de este dict: baja la instancia XBRL del 10-K y extrae el
# MAGIC fact del **miembro de la entidad padre** (`member`), emitiéndolo sin dimensión.
# MAGIC
# MAGIC Config separada de `favorites.json` **a propósito**: estar aquí NO marca el ticker como
# MAGIC favorito; solo activa la recuperación dimensional. `cik` es opcional (override si SEC no
# MAGIC resuelve el ticker). Dict vacío → `13` es no-op.
# MAGIC
# MAGIC `member` = local-name del miembro padre (validado contra la instancia del 10-K, no
# MAGIC adivinado). SKT→`TangerIncMember` verificado FY2024: Revenue 526.06M, Net Income 98.595M.

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


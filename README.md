# 20_transformation / 23__wide_tables

Pivots the long-format `financials` fact table into three analyst-ready wide tables (or views), one per financial statement. Each row represents a single ticker/year combination with all relevant line items as columns.

```
ticker | company | year | Revenue ($bn) | Gross Profit ($bn) | Net Income ($bn) | ...
-------|---------|------|---------------|-------------------|-----------------|----
AAPL   | Apple   | 2020 |        274.52 |             104.96 |           57.41 | ...
AAPL   | Apple   | 2021 |        365.82 |             152.84 |           94.68 | ...
```

All monetary values are expressed in **billions USD**, rounded to 2 decimal places. Per-share figures (EPS) and share counts are kept in their native units.

---

## Output tables

| Table | Statement | Description |
|---|---|---|
| `main.financials.income_statement` | Income Statement | Revenue through net income, EPS, share count |
| `main.financials.balance_sheet` | Balance Sheet | Assets, liabilities, and equity |
| `main.financials.cash_flow` | Cash Flow | Operating, investing, and financing activities |

> The notebook exists in two versions: one that materialises physical **Delta tables** (v1), and one that creates lightweight **Delta Views** (v2). The view-based approach is always in sync with `financials` and incurs zero extra storage cost.

---

## Dependencies

| Dependency | Path |
|---|---|
| Config / tickers | `00_config/01__tickers` (provides `CATALOG`, `SCHEMA`) |
| Source table | `{CATALOG}.{SCHEMA}.financials` (long-format fact table) |

The source table is expected to have the following columns:

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol (e.g. `AAPL`) |
| `company` | STRING | Company name |
| `year` | INT | Fiscal year |
| `statement` | STRING | One of `Income Statement`, `Balance Sheet`, `Cash Flow` |
| `concept` | STRING | Financial line item name (XBRL-derived) |
| `value` | DOUBLE | Raw value in USD |

---

## Column reference

### Income Statement — `income_statement`

| Column | Unit | Notes |
|---|---|---|
| `Revenue ($bn)` | $bn | Coalesced from `Revenue` and `Revenue (contract)` XBRL tags |
| `Cost of Revenue ($bn)` | $bn | |
| `Gross Profit ($bn)` | $bn | |
| `R&D ($bn)` | $bn | Research & development expense |
| `SG&A ($bn)` | $bn | Selling, general & administrative expense |
| `Operating Expenses ($bn)` | $bn | |
| `Operating Income ($bn)` | $bn | |
| `Interest Expense ($bn)` | $bn | |
| `Income Before Tax ($bn)` | $bn | |
| `Income Tax ($bn)` | $bn | |
| `Net Income ($bn)` | $bn | |
| `EPS Basic` | USD | Earnings per share, basic |
| `EPS Diluted` | USD | Earnings per share, diluted |
| `Shares Diluted (bn)` | bn shares | Diluted share count |

### Balance Sheet — `balance_sheet`

| Column | Unit |
|---|---|
| `Cash & Equivalents ($bn)` | $bn |
| `ST Investments ($bn)` | $bn |
| `Accounts Receivable ($bn)` | $bn |
| `Inventory ($bn)` | $bn |
| `Current Assets ($bn)` | $bn |
| `PP&E Net ($bn)` | $bn |
| `Goodwill ($bn)` | $bn |
| `Intangibles ($bn)` | $bn |
| `Total Assets ($bn)` | $bn |
| `Accounts Payable ($bn)` | $bn |
| `ST Debt ($bn)` | $bn |
| `Current Liabilities ($bn)` | $bn |
| `LT Debt ($bn)` | $bn |
| `Total Liabilities ($bn)` | $bn |
| `Paid-in Capital ($bn)` | $bn |
| `Retained Earnings ($bn)` | $bn |
| `Total Equity ($bn)` | $bn |
| `Total Liabilities & Equity ($bn)` | $bn |

### Cash Flow — `cash_flow`

| Column | Unit | Notes |
|---|---|---|
| `Net Income ($bn)` | $bn | Reconciliation starting point |
| `D&A ($bn)` | $bn | Depreciation & amortization |
| `SBC ($bn)` | $bn | Stock-based compensation |
| `Working Capital Change ($bn)` | $bn | |
| `Operating CF ($bn)` | $bn | |
| `CapEx ($bn)` | $bn | Capital expenditures |
| `Acquisitions ($bn)` | $bn | |
| `Purchases of Investments ($bn)` | $bn | |
| `Sales of Investments ($bn)` | $bn | |
| `Investing CF ($bn)` | $bn | |
| `Debt Issuance ($bn)` | $bn | |
| `Debt Repayment ($bn)` | $bn | |
| `Dividends ($bn)` | $bn | |
| `Buybacks ($bn)` | $bn | Share repurchases |
| `Financing CF ($bn)` | $bn | |
| `Net Change in Cash ($bn)` | $bn | |
| `Free Cash Flow ($bn)` | $bn | Derived: `Operating CF − CapEx` (view version only) |

---

## How it works

### Physical tables (v1)

`build_wide_table()` performs a three-step operation for each statement:

1. **Filter** — selects rows from `financials` matching the target statement and the concepts defined in the concept map.
2. **Pivot** — groups by `(ticker, company, year)` and pivots `concept` values into columns using `F.first()`.
3. **Write** — saves the result as a Delta table partitioned by `ticker`, with `autoOptimize` enabled.

Revenue uses two XBRL tags (`Revenue` and `Revenue (contract)`) depending on the reporting company; the two columns are coalesced into one after the pivot.

### Views (v2)

Each view is a single SQL `SELECT` with conditional aggregation (`MAX(CASE WHEN concept = '...' THEN value END)`), which avoids any physical data duplication. The cash flow view also computes `Free_Cash_Flow_bn` as a derived column (`Operating CF − CapEx`).

---

## Delta table properties

Physical tables are created with the following optimisation hints:

```
PARTITIONED BY (ticker)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
)
```

---

## Sanity check

The final cell queries each output for distinct ticker count, year range, and total row count — a quick way to confirm the pivot produced the expected shape.

```sql
SELECT
    COUNT(DISTINCT ticker) AS tickers,
    COUNT(DISTINCT year)   AS years,
    MIN(year)              AS first_year,
    MAX(year)              AS last_year,
    COUNT(*)               AS total_rows
FROM main.financials.income_statement
```

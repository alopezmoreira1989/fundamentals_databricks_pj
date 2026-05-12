# Fundamentals Analytics — Financial Statements

Produces the three core financial statement outputs (Income Statement, Balance Sheet, Cash Flow) by querying the long-format `financials` fact table directly. No intermediate wide tables or views are materialised.

```
ticker | company | year | concept           | value
-------|---------|------|-------------------|----------
AAPL   | Apple   | 2020 | Revenue           | 274520000000
AAPL   | Apple   | 2020 | Gross Profit      | 104956000000
AAPL   | Apple   | 2020 | Net Income        | 57411000000
```

Dashboard queries pivot and format this data on the fly, filtering by `statement` and `concept` as needed.

---

## Source table

| Table | Description |
|---|---|
| `{CATALOG}.{SCHEMA}.financials` | Long-format fact table — one row per ticker / year / concept |

Expected schema:

| Column | Type | Description |
|---|---|---|
| `ticker` | STRING | Stock ticker symbol (e.g. `AAPL`) |
| `company` | STRING | Company name |
| `year` | INT | Fiscal year |
| `statement` | STRING | One of `Income Statement`, `Balance Sheet`, `Cash Flow` |
| `concept` | STRING | Financial line item name (XBRL-derived) |
| `value` | DOUBLE | Raw value in USD |

---

## Statement filters

Each dashboard statement filters on the `statement` column and selects the relevant `concept` values:

| Statement | `statement` filter |
|---|---|
| Income Statement | `'Income Statement'` |
| Balance Sheet | `'Balance Sheet'` |
| Cash Flow | `'Cash Flow'` |

---

## Column reference

All monetary values are in **billions USD** (divided by 1e9, rounded to 2 decimal places) for display. Per-share figures (EPS) and share counts are kept in their native units.

### Income Statement

| Concept | Display label | Unit |
|---|---|---|
| `Revenue` / `Revenue (contract)` | Revenue | $bn |
| `Cost of Revenue` | Cost of Revenue | $bn |
| `Gross Profit` | Gross Profit | $bn |
| `R&D Expense` | R&D | $bn |
| `SG&A Expense` | SG&A | $bn |
| `Operating Expenses` | Operating Expenses | $bn |
| `Operating Income` | Operating Income | $bn |
| `Interest Expense` | Interest Expense | $bn |
| `Income Before Tax` | Income Before Tax | $bn |
| `Income Tax` | Income Tax | $bn |
| `Net Income` | Net Income | $bn |
| `EPS Basic` | EPS Basic | USD |
| `EPS Diluted` | EPS Diluted | USD |
| `Shares Diluted` | Shares Diluted | bn shares |

> Revenue is coalesced from two XBRL tags (`Revenue` and `Revenue (contract)`) since companies report under different tags.

### Balance Sheet

| Concept | Display label | Unit |
|---|---|---|
| `Cash & Equivalents` | Cash & Equivalents | $bn |
| `Short-term Investments` | ST Investments | $bn |
| `Accounts Receivable` | Accounts Receivable | $bn |
| `Inventory` | Inventory | $bn |
| `Total Current Assets` | Current Assets | $bn |
| `PP&E Net` | PP&E Net | $bn |
| `Goodwill` | Goodwill | $bn |
| `Intangible Assets` | Intangibles | $bn |
| `Total Assets` | Total Assets | $bn |
| `Accounts Payable` | Accounts Payable | $bn |
| `Short-term Debt` | ST Debt | $bn |
| `Total Current Liabilities` | Current Liabilities | $bn |
| `Long-term Debt` | LT Debt | $bn |
| `Total Liabilities` | Total Liabilities | $bn |
| `Additional Paid-in Capital` | Paid-in Capital | $bn |
| `Retained Earnings` | Retained Earnings | $bn |
| `Total Stockholders Equity` | Total Equity | $bn |
| `Total Liabilities & Equity` | Total Liabilities & Equity | $bn |

### Cash Flow

| Concept | Display label | Unit | Notes |
|---|---|---|---|
| `Net Income` | Net Income | $bn | Reconciliation starting point |
| `Depreciation & Amortization` | D&A | $bn | |
| `Stock-based Compensation` | SBC | $bn | |
| `Changes in Working Capital` | Working Capital Change | $bn | |
| `Operating Cash Flow` | Operating CF | $bn | |
| `CapEx` | CapEx | $bn | |
| `Acquisitions` | Acquisitions | $bn | |
| `Purchases of Investments` | Purchases of Investments | $bn | |
| `Sales of Investments` | Sales of Investments | $bn | |
| `Investing Cash Flow` | Investing CF | $bn | |
| `Debt Issuance` | Debt Issuance | $bn | |
| `Debt Repayment` | Debt Repayment | $bn | |
| `Dividends Paid` | Dividends | $bn | |
| `Share Repurchases` | Buybacks | $bn | |
| `Financing Cash Flow` | Financing CF | $bn | |
| `Net Change in Cash` | Net Change in Cash | $bn | |

> Free Cash Flow is derived at query time as `Operating CF − CapEx` and is not stored as a concept in `financials`.

---

## Design decisions

**No wide tables or views.** An earlier iteration of this project materialised the three statements as Delta wide tables (one column per concept), and later as Delta views. Both approaches were discarded in favour of querying `financials` directly, which keeps the pipeline simpler and avoids any schema drift between the fact table and its derivatives.
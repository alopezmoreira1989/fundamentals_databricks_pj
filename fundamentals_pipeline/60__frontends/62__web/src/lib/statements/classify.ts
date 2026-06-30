// Statement-table row classification — port of lib/colors.py (the subset render_table_html
// uses): sparkline stroke colour, CSS row class, section class, per-share / share-count
// concept sets. Hex literals match colors.py (= the app's signal/accent tokens).

const GREEN = "#0F6E56";
const BLUE = "#185FA5";
const GRAY = "#888780";
const CORAL = "#993C1D";
const AMBER = "#BA7517";
const PURPLE = "#534AB7";
export const CREAM = "#FAF8F4"; // grand-total rows on the dark band

const CONCEPT_COLORS: Record<string, string> = {
  Revenue: GREEN,
  "Gross Profit": GREEN,
  "Operating Income": GREEN,
  "Operating Cash Flow": GREEN,
  "Net Income": BLUE,
  "Total Assets": BLUE,
  "Total Liabilities & Equity": BLUE,
  "Net Change in Cash": BLUE,
  "R&D Expense": BLUE,
  "EPS Basic": PURPLE,
  "EPS Diluted": PURPLE,
  "Shares Diluted": GRAY,
  "Short-term Debt": CORAL,
  "Long-term Debt": CORAL,
  "Retained Earnings": CORAL,
  "Income Tax": CORAL,
  CapEx: AMBER,
  Acquisitions: AMBER,
  "Investing Cash Flow": AMBER,
  "Purchases of Investments": AMBER,
  "Sales of Investments": AMBER,
  "Debt Repayment": CORAL,
  "Share Repurchases": CORAL,
  "Financing Cash Flow": CORAL,
  "Dividends Paid": GRAY,
  "Debt Issuance": GRAY,
};

export function rowColor(concept: string): string {
  return CONCEPT_COLORS[concept] ?? GRAY;
}

export const GRAND_TOTAL_CONCEPTS = new Set(["Total Assets", "Total Liabilities & Equity"]);
const HEADLINE_CONCEPTS = new Set(["Income Statement|Net Income", "Cash Flow|Net Change in Cash"]);
const SUBTOTAL_CONCEPTS = new Set([
  "Gross Profit",
  "Operating Income",
  "Operating Cash Flow",
  "Investing Cash Flow",
  "Financing Cash Flow",
]);

export type RowClass = "grand-total" | "headline" | "subtotal" | "";

export function rowClass(stmt: string, concept: string, displayName: string): RowClass {
  if (GRAND_TOTAL_CONCEPTS.has(concept)) return "grand-total";
  if (HEADLINE_CONCEPTS.has(`${stmt}|${concept}`)) return "headline";
  if (SUBTOTAL_CONCEPTS.has(concept)) return "subtotal";
  if (displayName.startsWith("Total ")) return "subtotal";
  return "";
}

const SECTION_CLASSES: Record<string, string> = {
  "Balance Sheet|Assets": "assets",
  "Balance Sheet|Liabilities & Equity": "liab",
  "Cash Flow|Operating Activities": "operating",
  "Cash Flow|Investing Activities": "investing",
  "Cash Flow|Financing Activities": "financing",
  "Cash Flow|Net Change": "netchange",
};

export function sectionClass(stmt: string, section: string): string {
  return SECTION_CLASSES[`${stmt}|${section}`] ?? "";
}

export const PER_SHARE_CONCEPTS = new Set(["EPS Basic", "EPS Diluted"]);
export const SHARE_COUNT_CONCEPTS = new Set(["Shares Diluted"]);

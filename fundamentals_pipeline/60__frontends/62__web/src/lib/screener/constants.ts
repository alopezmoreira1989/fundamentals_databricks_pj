// Screener constants — 1:1 port of the module-level tables in 61__streamlit/lib/screener.py.
// Keep in sync with that file; the labels here double as URL-state values (shareable links).

export const MARKET_CAP = "Market Cap";

/** Default visible metric columns (display order). */
export const DEFAULT_COLUMNS = [
  "Market Cap",
  "P/E",
  "P/S",
  "Net Margin %",
  "ROE %",
  "Revenue YoY %",
  "EV/EBITDA",
] as const;

/** Universe label → flag column on the wide frame ("" = no filter). */
export const UNIVERSE_FLAGS: Record<string, "" | "is_favorite" | "in_sp500" | "in_r3000"> = {
  All: "",
  "S&P 500": "in_sp500",
  "Russell 3000": "in_r3000",
  Favorites: "is_favorite",
};
export const UNIVERSE_LABELS = Object.keys(UNIVERSE_FLAGS);

export const UNKNOWN_SECTOR = "Unknown";
export const UNKNOWN_INDUSTRY = "Unknown";
export const ALL_INDUSTRIES = "All industries";

/** 11 canonical GICS sectors (sorted) + the no-op "All sectors" default at index 0. */
export const SECTORS = [
  "All sectors",
  ...[
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
  ],
];
export const ALL_SECTORS = SECTORS[0];

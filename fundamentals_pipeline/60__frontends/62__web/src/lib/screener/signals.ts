// Health traffic-light for derived metrics — port of 61__streamlit/lib/signals.py
// (signal_absolute + threshold_text). Conservative Graham-style thresholds; the chips this
// drives must mean the same thing as the Streamlit detail page and the bucket pills.

export type Signal = "good" | "warn" | "bad";

// (green_max, red_min) for "lower is better".
const LOWER_IS_BETTER: Record<string, [number, number]> = {
  "P/E": [15, 25],
  "P/S": [2, 5],
  "P/FCF": [15, 25],
  "P/B": [1.5, 3],
  "EV/EBITDA": [15, 25],
  "Goodwill / Total Assets %": [15, 30],
  "Goodwill / Tangible Equity %": [50, 100],
  "Goodwill / Market Cap %": [20, 50],
  "Price / Tangible Book Value": [1.5, 3],
  "Debt / Equity": [0.5, 1.0],
  "Debt / Assets": [0.3, 0.5],
  "Net Debt / EBITDA": [3.0, 4.0],
  "Accruals Ratio": [0.05, 0.15],
  "Dividend Payout Ratio": [0.6, 0.9],
  "Buyback Payout Ratio": [0.6, 1.0],
  "Total Payout Ratio": [0.8, 1.2],
  "Payout / FCF": [0.8, 1.2],
};

// (green_min, red_max) for "higher is better".
const HIGHER_IS_BETTER: Record<string, [number, number]> = {
  "Current Ratio": [2.0, 1.5],
  "Interest Coverage": [6.0, 2.0],
  "Cash Flow to Debt": [0.4, 0.15],
  "ROE %": [15, 8],
  "ROIC %": [15, 8],
  "ROCE %": [15, 8],
  "ROA %": [10, 5],
  "CROIC %": [15, 8],
  "ROTCE %": [15, 8],
  "Return on Tangible Equity %": [15, 8],
  "Gross Margin %": [40, 20],
  "Operating Margin %": [20, 10],
  "Net Margin %": [20, 10],
  "FCF Margin %": [15, 8],
  "Op Cash Flow Margin %": [15, 8],
  "Earnings Yield %": [8, 4],
  "FCF Yield %": [6, 3],
  "Op Cash Flow Yield %": [6, 3],
  "Sales Yield %": [10, 5],
  "Book Yield %": [8, 4],
  "EBITDA Yield %": [8, 4],
  "Dividend Yield %": [4, 1],
  "Buyback Yield %": [4, 1],
  "Shareholder Yield %": [6, 2],
  "Net Buyback Yield %": [3, 0],
  "Dividend Coverage": [2.0, 1.0],
  "Altman Z-Score": [3.0, 1.8],
  "Piotroski F-Score": [7.0, 3.0],
};

/** Strip a trailing " (...)" suffix so "P/E (TTM)" resolves like "P/E". */
function baseName(metric: string): string {
  return metric.split(" (")[0].trim();
}

/** Graham threshold text for a metric's row tooltip ("" when the metric has no band). */
export function thresholdText(metric: string): string {
  const base = baseName(metric);
  const lo = LOWER_IS_BETTER[base];
  if (lo) return `${base}: green ≤ ${lo[0]}, red ≥ ${lo[1]} (lower is better)`;
  const hi = HIGHER_IS_BETTER[base];
  if (hi) return `${base}: green ≥ ${hi[0]}, red ≤ ${hi[1]} (higher is better)`;
  if (base.startsWith("MoS %")) return "MoS %: green > 30, amber 0–30, red < 0";
  return "";
}

/**
 * For valuation multiples: today vs the series average (≈10y, excluding the current year).
 * "bad" = expensive (>tol above avg), "good" = cheap (>tol below avg). Port of signal_vs_history.
 */
export function signalVsHistory(
  value: number | null | undefined,
  history: (number | null)[],
  tol = 0.2,
): Signal | null {
  const hist = history.slice(0, -1).filter((h) => h !== null && !Number.isNaN(h)) as number[];
  if (value === null || value === undefined || Number.isNaN(value) || hist.length < 3) return null;
  const avg = hist.reduce((a, b) => a + b, 0) / hist.length;
  if (avg === 0) return null;
  const dev = (value - avg) / Math.abs(avg);
  return dev > tol ? "bad" : dev < -tol ? "good" : "warn";
}

/** Traffic-light signal for a metric value, or null when the metric has no band. */
export function signalAbsolute(metric: string, value: number | null | undefined): Signal | null {
  if (value === null || value === undefined || Number.isNaN(value)) return null;
  const base = baseName(metric);
  const lo = LOWER_IS_BETTER[base];
  if (lo) {
    const [g, b] = lo;
    return value <= g ? "good" : value >= b ? "bad" : "warn";
  }
  const hi = HIGHER_IS_BETTER[base];
  if (hi) {
    const [g, b] = hi;
    return value >= g ? "good" : value <= b ? "bad" : "warn";
  }
  if (base.startsWith("MoS %")) {
    return value > 30 ? "good" : value < 0 ? "bad" : "warn";
  }
  return null;
}

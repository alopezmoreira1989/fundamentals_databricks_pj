// Fixed bucket system — port of the band tables + buckets_for/bucket_mask in
// 61__streamlit/lib/screener.py. Boundaries deliberately track the green/amber/red signal
// bands so a screener chip means the same thing as the detail-page health signal.
//
// Each bucket is [label, loInclusive, hiExclusive]; open ends use ±Infinity.

import { MARKET_CAP } from "./constants";

export type Bucket = [label: string, lo: number, hi: number];

const INF = Infinity;
const NINF = -Infinity;

const VALUATION_MULTIPLE_BAND: Bucket[] = [
  ["Loss", NINF, 0.0],
  ["Cheap", 0.0, 10.0],
  ["Fair", 10.0, 15.0],
  ["Full", 15.0, 25.0],
  ["Rich", 25.0, INF],
];
const SALES_BOOK_BAND: Bucket[] = [
  ["<1", NINF, 1.0],
  ["1–2", 1.0, 2.0],
  ["2–5", 2.0, 5.0],
  [">5", 5.0, INF],
];
const RETURN_BAND: Bucket[] = [
  ["Negative", NINF, 0.0],
  ["0–8%", 0.0, 8.0],
  ["8–15%", 8.0, 15.0],
  [">15%", 15.0, INF],
];
const ROA_BAND: Bucket[] = [
  ["Negative", NINF, 0.0],
  ["0–5%", 0.0, 5.0],
  ["5–10%", 5.0, 10.0],
  [">10%", 10.0, INF],
];
const MARGIN_BAND: Bucket[] = [
  ["Negative", NINF, 0.0],
  ["0–10%", 0.0, 10.0],
  ["10–20%", 10.0, 20.0],
  ["20–40%", 20.0, 40.0],
  [">40%", 40.0, INF],
];
const YOY_BAND: Bucket[] = [
  ["Declining", NINF, 0.0],
  ["0–15%", 0.0, 15.0],
  ["15–30%", 15.0, 30.0],
  [">30%", 30.0, INF],
];
const YIELD_BAND: Bucket[] = [
  ["<3%", NINF, 3.0],
  ["3–6%", 3.0, 6.0],
  ["6–10%", 6.0, 10.0],
  [">10%", 10.0, INF],
];
const MOS_BAND: Bucket[] = [
  ["Overvalued", NINF, 0.0],
  ["Slim", 0.0, 15.0],
  ["Decent", 15.0, 30.0],
  ["Wide", 30.0, INF],
];
const DEBT_EQUITY_BAND: Bucket[] = [
  ["Low", NINF, 0.5],
  ["Mod", 0.5, 1.0],
  ["High", 1.0, INF],
];
const DEBT_ASSETS_BAND: Bucket[] = [
  ["Low", NINF, 0.3],
  ["Mod", 0.3, 0.5],
  ["High", 0.5, INF],
];
const CURRENT_RATIO_BAND: Bucket[] = [
  ["Tight", NINF, 1.0],
  ["OK", 1.0, 1.5],
  ["Healthy", 1.5, INF],
];
const CAP_SIZE_BAND: Bucket[] = [
  ["Micro", NINF, 300e6],
  ["Small", 300e6, 2e9],
  ["Mid", 2e9, 10e9],
  ["Large", 10e9, 200e9],
  ["Mega", 200e9, INF],
];

const EXPLICIT_BUCKETS: Record<string, Bucket[]> = {
  "P/E": VALUATION_MULTIPLE_BAND,
  "P/FCF": VALUATION_MULTIPLE_BAND,
  "EV/EBITDA": VALUATION_MULTIPLE_BAND,
  "P/S": SALES_BOOK_BAND,
  "P/B": SALES_BOOK_BAND,
  "ROE %": RETURN_BAND,
  "ROIC %": RETURN_BAND,
  "ROCE %": RETURN_BAND,
  "CROIC %": RETURN_BAND,
  "ROA %": ROA_BAND,
  "Gross Margin %": MARGIN_BAND,
  "Operating Margin %": MARGIN_BAND,
  "Net Margin %": MARGIN_BAND,
  "FCF Margin %": MARGIN_BAND,
  "Op Cash Flow Margin %": MARGIN_BAND,
  "Revenue YoY %": YOY_BAND,
  "Net Income YoY %": YOY_BAND,
  "Operating Cash Flow YoY %": YOY_BAND,
  "Free Cash Flow YoY %": YOY_BAND,
  "Op Cash Flow Yield %": YIELD_BAND,
  "FCF Yield %": YIELD_BAND,
  "Earnings Yield %": YIELD_BAND,
  "Sales Yield %": YIELD_BAND,
  "Book Yield %": YIELD_BAND,
  "EBITDA Yield %": YIELD_BAND,
  "Debt / Equity": DEBT_EQUITY_BAND,
  "Debt / Assets": DEBT_ASSETS_BAND,
  "Current Ratio": CURRENT_RATIO_BAND,
  [MARKET_CAP]: CAP_SIZE_BAND,
  EV: CAP_SIZE_BAND,
};

const UNIT_BUCKETS: Record<string, Bucket[]> = {
  usd: [
    ["Negative", NINF, 0.0],
    ["<$100M", 0.0, 100e6],
    ["$100M–1B", 100e6, 1e9],
    ["$1–10B", 1e9, 10e9],
    [">$10B", 10e9, INF],
  ],
  percent: [
    ["Negative", NINF, 0.0],
    ["0–10%", 0.0, 10.0],
    ["10–25%", 10.0, 25.0],
    [">25%", 25.0, INF],
  ],
  ratio: [
    ["<1", NINF, 1.0],
    ["1–2", 1.0, 2.0],
    ["2–5", 2.0, 5.0],
    [">5", 5.0, INF],
  ],
};

/**
 * Resolve the bucket table for a metric: exact name → suffix-stripped base name →
 * "MoS %" prefix → unit fallback. Returns [] when nothing matches.
 */
export function bucketsFor(metric: string, unit: string | undefined): Bucket[] {
  if (EXPLICIT_BUCKETS[metric]) return EXPLICIT_BUCKETS[metric];
  const base = metric.split(" (")[0].trim();
  if (EXPLICIT_BUCKETS[base]) return EXPLICIT_BUCKETS[base];
  if (base.startsWith("MoS %")) return MOS_BAND;
  return UNIT_BUCKETS[unit ?? ""] ?? [];
}

/**
 * True if `value` falls in any of the `selected` bucket labels (OR-ed). An empty selection
 * is "no filter" (always true). When a filter is active, a null/NaN value matches nothing —
 * a company with no value for this metric belongs in no bucket (matches bucket_mask).
 */
export function bucketMatch(
  value: number | null | undefined,
  selected: string[],
  buckets: Bucket[],
): boolean {
  if (selected.length === 0) return true;
  if (value === null || value === undefined || Number.isNaN(value)) return false;
  for (const [label, lo, hi] of buckets) {
    if (selected.includes(label) && value >= lo && value < hi) return true;
  }
  return false;
}

// Screener data frame — the web equivalent of build_screener_frame() in
// 61__streamlit/lib/screener.py. One row per ticker, with the latest available fiscal-year
// value of every metric.
//
// Strategy: a single DuckDB query reduces the long metrics parquet to the latest-FY value
// per (ticker, metric), which we pivot to wide in JS (≈2,500 tickers — trivial). Sector,
// industry, company name and the universe flags come from meta.json `tickers` (NOT the
// parquet), joined here — matching the Streamlit `wide.merge(info, ...)`. Older artifacts
// missing a field degrade to its default (all-False flags, "Unknown" sector/industry).
//
// The whole frame is built once; column selection, filtering and sorting then happen
// in-memory in the table component (instant, no re-query) — same semantics as the cached
// Streamlit frame, and far simpler than a parametrised SQL round-trip per keystroke.

import { ARTIFACT_FILES, loadMeta, type TickerMeta } from "../data";
import { MARKET_CAP, UNKNOWN_INDUSTRY, UNKNOWN_SECTOR } from "../screener/constants";
import { queryRows } from "./runQuery";

export interface ScreenerRow {
  ticker: string;
  company: string;
  sector: string;
  industry: string;
  is_favorite: boolean;
  in_sp500: boolean;
  in_r3000: boolean;
  fiscal_year: number | null;
  /** metric name → latest-FY value (or null if absent). */
  metrics: Record<string, number | null>;
}

export interface ScreenerFrame {
  rows: ScreenerRow[];
  /** metric name → unit ("percent" | "ratio" | "usd"). */
  unitMap: Record<string, string>;
  /** metric names ordered by metrics_hierarchy.sort_order, Market Cap first. */
  metricOrder: string[];
}

/** Coerce a meta flag (bool, "true"/"1", or missing) to a native boolean — mirrors `_as_bool`. */
function asBool(v: unknown): boolean {
  return v === true || ["true", "1"].includes(String(v ?? "").trim().toLowerCase());
}

function cleanLabel(v: unknown, fallback: string): string {
  const s = String(v ?? "").trim();
  return s === "" ? fallback : s;
}

const METRICS = ARTIFACT_FILES.metrics;

export async function loadScreenerFrame(): Promise<ScreenerFrame> {
  // ── latest-FY value per (ticker, metric) ──────────────────────────────────────────────
  const factRows = await queryRows<{ ticker: string; metric: string; value: number; fiscal_year: number }>(
    `WITH fy AS (
       SELECT ticker, metric, fiscal_year, value
       FROM '${METRICS}'
       WHERE period_type = 'FY' AND value IS NOT NULL
     ),
     ranked AS (
       SELECT ticker, metric, value, fiscal_year,
              row_number() OVER (PARTITION BY ticker, metric ORDER BY fiscal_year DESC) AS rn
       FROM fy
     )
     SELECT ticker, metric, value, fiscal_year FROM ranked WHERE rn = 1`,
    ["metrics"],
  );

  // ── unit + sort order per metric (small: one row per metric) ──────────────────────────
  const metricMeta = await queryRows<{ metric: string; unit: string | null; sort_order: number | null }>(
    `SELECT metric, any_value(unit) AS unit, min(sort_order) AS sort_order
     FROM '${METRICS}' WHERE period_type = 'FY' GROUP BY metric`,
    ["metrics"],
  );

  const unitMap: Record<string, string> = {};
  for (const m of metricMeta) {
    if (m.unit != null) unitMap[m.metric] = String(m.unit);
  }
  if (!(MARKET_CAP in unitMap)) unitMap[MARKET_CAP] = "usd";

  // Order by sort_order (NaN/missing last), then name; Market Cap pinned first.
  const ordered = [...metricMeta].sort((a, b) => {
    const sa = a.sort_order ?? Number.POSITIVE_INFINITY;
    const sb = b.sort_order ?? Number.POSITIVE_INFINITY;
    return sa !== sb ? sa - sb : a.metric.localeCompare(b.metric);
  });
  const metricOrder = [MARKET_CAP, ...ordered.map((m) => m.metric).filter((m) => m !== MARKET_CAP)];

  // ── pivot long → wide in JS ───────────────────────────────────────────────────────────
  const byTicker = new Map<string, ScreenerRow>();
  for (const r of factRows) {
    let row = byTicker.get(r.ticker);
    if (!row) {
      row = {
        ticker: r.ticker,
        company: r.ticker,
        sector: UNKNOWN_SECTOR,
        industry: UNKNOWN_INDUSTRY,
        is_favorite: false,
        in_sp500: false,
        in_r3000: false,
        fiscal_year: null,
        metrics: {},
      };
      byTicker.set(r.ticker, row);
    }
    row.metrics[r.metric] = r.value;
    const fy = Number(r.fiscal_year);
    if (!Number.isNaN(fy) && (row.fiscal_year === null || fy > row.fiscal_year)) {
      row.fiscal_year = fy;
    }
  }

  // ── join meta.tickers (company, sector, industry, universe flags) ─────────────────────
  const meta = await loadMeta();
  const infoByTicker = new Map<string, TickerMeta>();
  for (const t of meta.tickers) {
    if (t?.ticker) infoByTicker.set(String(t.ticker), t);
  }
  for (const row of Array.from(byTicker.values())) {
    const info = infoByTicker.get(row.ticker);
    if (!info) continue;
    row.company = cleanLabel(info.company, row.ticker);
    row.sector = cleanLabel(info.sector, UNKNOWN_SECTOR);
    row.industry = cleanLabel(info.industry, UNKNOWN_INDUSTRY);
    row.is_favorite = asBool(info.is_favorite);
    row.in_sp500 = asBool(info.in_sp500);
    row.in_r3000 = asBool(info.in_r3000);
  }

  return { rows: Array.from(byTicker.values()), unitMap, metricOrder };
}

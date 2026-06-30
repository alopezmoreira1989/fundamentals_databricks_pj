// Company page data — the web equivalent of the per-ticker slice the Streamlit company view
// builds (load_latest_data + compute_industry_benchmarks, scoped to one ticker).
//
// Benchmarks: compute_industry_benchmarks in lib/data.py builds them for ALL tickers and
// caches. For a single company page we only need this ticker's own 10y averages (computed in
// JS from the metric slice) and its industry's latest-FY medians (one targeted DuckDB query
// over the industry peers) — far cheaper than the global pass.

import { ARTIFACT_FILES, loadMeta, type TickerMeta } from "../data";
import { latestOf } from "../format";
import { queryRows } from "./runQuery";

const METRICS = ARTIFACT_FILES.metrics;
const DATA = ARTIFACT_FILES.data;

// Excluded from benchmark averaging (levels you don't average for comps / clamped).
const BENCH_EXCLUDE = new Set(["Piotroski F-Score", "Altman Z-Score", "Accruals Ratio"]);
// Only the null-category Market Cap row is excluded from the grid (surfaced as a KPI instead);
// Intrinsic Value renders as bull/mid/bear scenario rows (see components/company/valuation).
const SKIP_CATEGORIES = new Set<string>();

export interface MetricSeries {
  metric: string;
  unit: string | null;
  /** oldest → newest FY values. */
  values: (number | null)[];
  latest: number | null;
}
export interface SubGroup {
  subcategory: string;
  rows: MetricSeries[];
}
export interface CatGroup {
  category: string;
  subgroups: SubGroup[];
}

export interface CompanyView {
  ticker: string;
  company: string;
  sector: string;
  industry: string;
  hasLogo: boolean | null;
  fyMin: number | null;
  fyMax: number | null;
  nYears: number;
  buildTimestamp: string;
  kpis: {
    marketCap: (number | null)[];
    revenue: (number | null)[];
    netIncome: (number | null)[];
    netMargin: (number | null)[];
  };
  categories: CatGroup[];
  /** metric → company 10y average (percent/ratio metrics). */
  companyBench: Record<string, number>;
  /** metric → industry latest-FY median (n ≥ 3). */
  industryBench: Record<string, number>;
}

const sqlStr = (s: string) => `'${s.replace(/'/g, "''")}'`;

interface MetricRow {
  category: string | null;
  subcategory: string | null;
  metric: string;
  unit: string | null;
  fiscal_year: number;
  value: number | null;
}

export async function loadCompanyView(ticker: string): Promise<CompanyView | null> {
  const meta = await loadMeta();
  const info: TickerMeta | undefined = meta.tickers.find((t) => t?.ticker === ticker);

  // ── this ticker's FY metric rows (ordered oldest→newest within each metric) ────────────
  const metricRows = await queryRows<MetricRow>(
    `SELECT category, subcategory, metric, unit, fiscal_year, value
     FROM ${sqlStr(METRICS)} WHERE ticker = ${sqlStr(ticker)} AND period_type = 'FY'
     ORDER BY sort_order NULLS LAST, fiscal_year`,
    ["metrics"],
  );
  if (metricRows.length === 0) return null;

  // ── Income-statement concepts for the KPI strip ───────────────────────────────────────
  const isRows = await queryRows<{ concept: string; fiscal_year: number; value: number | null }>(
    `SELECT concept, fiscal_year, value
     FROM ${sqlStr(DATA)} WHERE ticker = ${sqlStr(ticker)} AND period_type = 'FY'
       AND stmt = 'Income Statement' AND concept IN ('Revenue', 'Net Income')
     ORDER BY fiscal_year`,
    ["data"],
  );

  // ── pivot metric rows → category → subcategory → metric series ─────────────────────────
  const cats = new Map<string, Map<string, Map<string, MetricSeries>>>();
  const seriesByMetric = new Map<string, MetricSeries>(); // for KPIs + company_10y

  for (const r of metricRows) {
    const series =
      seriesByMetric.get(r.metric) ?? { metric: r.metric, unit: r.unit, values: [], latest: null };
    series.values.push(r.value);
    seriesByMetric.set(r.metric, series);

    if (!r.category || SKIP_CATEGORIES.has(r.category)) continue;
    const sub = r.subcategory ?? "";
    const subMap = cats.get(r.category) ?? new Map<string, Map<string, MetricSeries>>();
    cats.set(r.category, subMap);
    const metricMap = subMap.get(sub) ?? new Map<string, MetricSeries>();
    subMap.set(sub, metricMap);
    metricMap.set(r.metric, series); // same object reference as seriesByMetric
  }
  for (const s of Array.from(seriesByMetric.values())) s.latest = latestOf(s.values);

  const categories: CatGroup[] = Array.from(cats.entries()).map(([category, subMap]) => ({
    category,
    subgroups: Array.from(subMap.entries()).map(([subcategory, metricMap]) => ({
      subcategory,
      rows: Array.from(metricMap.values()),
    })),
  }));

  // ── company 10y averages (percent/ratio, excluding scores + MoS), n ≥ 2 ────────────────
  const companyBench: Record<string, number> = {};
  for (const s of Array.from(seriesByMetric.values())) {
    if (s.unit !== "percent" && s.unit !== "ratio") continue;
    if (BENCH_EXCLUDE.has(s.metric) || s.metric.startsWith("MoS %")) continue;
    const last10 = (s.values.filter((v) => v !== null && !Number.isNaN(v)) as number[]).slice(-10);
    if (last10.length >= 2) companyBench[s.metric] = last10.reduce((a, b) => a + b, 0) / last10.length;
  }

  // ── industry latest-FY medians across peers (n ≥ 3) ────────────────────────────────────
  const industry = (info?.industry ?? "").toString();
  const industryBench: Record<string, number> = {};
  if (industry) {
    const peers = meta.tickers.filter((t) => t?.industry === industry && t?.ticker).map((t) => String(t.ticker));
    if (peers.length >= 3) {
      const inList = peers.map(sqlStr).join(",");
      const med = await queryRows<{ metric: string; med: number; n: number }>(
        `WITH fy AS (
           SELECT ticker, metric, fiscal_year, value, unit FROM ${sqlStr(METRICS)}
           WHERE period_type = 'FY' AND value IS NOT NULL AND ticker IN (${inList})
         ),
         latest AS (
           SELECT ticker, metric, value, unit,
             row_number() OVER (PARTITION BY ticker, metric ORDER BY fiscal_year DESC) AS rn FROM fy
         )
         SELECT metric, median(value) AS med, count(DISTINCT ticker) AS n FROM latest
         WHERE rn = 1 AND unit IN ('percent','ratio')
           AND metric NOT LIKE 'MoS %%' AND metric NOT IN ('Piotroski F-Score','Altman Z-Score','Accruals Ratio')
         GROUP BY metric HAVING count(DISTINCT ticker) >= 3`,
        ["metrics"],
      );
      for (const r of med) industryBench[r.metric] = Number(r.med);
    }
  }

  const fyRange = meta.fy_ranges.find((e) => e?.ticker === ticker);

  return {
    ticker,
    company: (info?.company ?? ticker).toString(),
    sector: (info?.sector ?? "Unknown").toString() || "Unknown",
    industry,
    hasLogo: (info?.has_logo as boolean | null | undefined) ?? null,
    fyMin: fyRange?.fy_min ?? null,
    fyMax: fyRange?.fy_max ?? null,
    nYears: fyRange ? fyRange.fy_max - fyRange.fy_min + 1 : 0,
    buildTimestamp: meta.build_timestamp,
    kpis: {
      marketCap: seriesByMetric.get("Market Cap")?.values ?? [],
      netMargin: seriesByMetric.get("Net Margin %")?.values ?? [],
      revenue: isRows.filter((r) => r.concept === "Revenue").map((r) => r.value),
      netIncome: isRows.filter((r) => r.concept === "Net Income").map((r) => r.value),
    },
    categories,
    companyBench,
    industryBench,
  };
}

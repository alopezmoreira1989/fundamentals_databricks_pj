// Number formatting — port of the subset of 61__streamlit/lib/format.py used by the company
// masthead, KPI strip and derived-metrics grid. Keep in sync with that module.

export const EM_DASH = "—";

export function isMissing(v: number | null | undefined): v is null | undefined {
  return v === null || v === undefined || Number.isNaN(v);
}

/** KPI strip / usd metric: $391.0B / $93.7M / $1,234 (scale-aware, one decimal). */
export function fmtKpi(v: number | null | undefined): string {
  if (isMissing(v)) return EM_DASH;
  const sign = v < 0 ? "-" : "";
  const a = Math.abs(v);
  if (a >= 1_000_000_000) return `${sign}$${(a / 1_000_000_000).toFixed(1)}B`;
  if (a >= 1_000_000) return `${sign}$${(a / 1_000_000).toFixed(1)}M`;
  return `${sign}$${a.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
}

/**
 * Derived metric, formatted by unit. `signed` forces a leading "+" on positive percents —
 * only for direction metrics (YoY growth, margin of safety), never levels (margins/returns).
 */
export function fmtMetric(
  v: number | null | undefined,
  unit: string | null | undefined,
  signed = false,
): string {
  if (isMissing(v)) return EM_DASH;
  if (unit === "percent") return signed ? `${v >= 0 ? "+" : ""}${v.toFixed(1)}%` : `${v.toFixed(1)}%`;
  if (unit === "ratio") return `${v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}x`;
  if (unit === "usd") return fmtKpi(v);
  return v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/** Margin-of-safety %: clamp the DISPLAY to ±100% (the signal still uses the true value). */
export function fmtMos(v: number | null | undefined): string {
  if (isMissing(v)) return EM_DASH;
  if (v >= 100) return ">+100%";
  if (v <= -100) return "<−100%"; // real minus sign U+2212
  return `${v >= 0 ? "+" : ""}${v.toFixed(1)}%`;
}

export type DeltaClass = "up" | "down" | "flat";

/** YoY delta for a KPI card: [label, css class]. */
export function fmtDelta(yoyPct: number | null | undefined): [string, DeltaClass] {
  if (isMissing(yoyPct)) return ["· no prior year", "flat"];
  if (yoyPct > 0.05) return [`▲ ${yoyPct.toFixed(1)}% YoY`, "up"];
  if (yoyPct < -0.05) return [`▼ ${Math.abs(yoyPct).toFixed(1)}% YoY`, "down"];
  return [`≈ ${yoyPct >= 0 ? "+" : ""}${yoyPct.toFixed(1)}% YoY`, "flat"];
}

/** (latest − prior) / |prior| as a percent, or null when not computable. Series is oldest→newest. */
export function yoy(values: (number | null)[]): number | null {
  const n = values.length;
  if (n < 2) return null;
  const a = values[n - 1];
  const b = values[n - 2];
  if (isMissing(a) || isMissing(b) || b === 0) return null;
  return ((a - b) / Math.abs(b)) * 100;
}

/** Latest non-missing value of an oldest→newest series, or null. */
export function latestOf(values: (number | null)[]): number | null {
  const last = values[values.length - 1];
  return isMissing(last) ? null : last;
}

const commas = (v: number, dp = 0) =>
  v.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp });

/** Statement number cell: thousands-separated integer, accounting-style negatives. */
export function fmtNum(v: number | null | undefined): string {
  if (isMissing(v)) return EM_DASH;
  return v < 0 ? `(${commas(Math.abs(v))})` : commas(v);
}

/** Statement cell with an optional unit scale (1 / 1e3 / 1e6 / 1e9). divisor 1 ⇒ fmtNum. */
export function fmtNumScaled(v: number | null | undefined, divisor = 1): string {
  if (isMissing(v)) return EM_DASH;
  if (divisor === 1) return fmtNum(v);
  const scaled = v / divisor;
  return scaled < 0 ? `(${commas(Math.abs(scaled), 1)})` : commas(scaled, 1);
}

/** Per-share row: 2 decimals, accounting-style negatives. */
export function fmtEps(v: number | null | undefined): string {
  if (isMissing(v)) return EM_DASH;
  return v < 0 ? `(${commas(Math.abs(v), 2)})` : commas(v, 2);
}

/** CAGR over `years` periods → [label, css class]. "n/a" on sign change / zero start. */
export function fmtCagr(
  start: number | null,
  end: number | null,
  years: number,
): [string, "" | "up" | "down"] {
  if (isMissing(start) || isMissing(end) || years < 1) return [EM_DASH, ""];
  if (start === 0) return ["n/a", ""];
  if (start < 0 !== end < 0) return ["n/a", ""]; // crosses zero → undefined
  let cagr: number;
  if (start < 0 && end < 0) {
    cagr = -((Math.abs(end) / Math.abs(start)) ** (1 / years) - 1) * 100;
  } else {
    cagr = ((end / start) ** (1 / years) - 1) * 100;
  }
  const cls = cagr > 0.05 ? "up" : cagr < -0.05 ? "down" : "";
  return [`${cagr >= 0 ? "+" : ""}${cagr.toFixed(1)}%`, cls];
}

/**
 * Robust annual growth via log-linear OLS on ln|value| vs year offset → [growth%, r²].
 * Strictly-positive or all-negative series only; sign-crossing or <3 points → [null, null].
 */
export function trendGrowth(values: (number | null)[]): [number | null, number | null] {
  const pts = values
    .map((v, i) => [i, v] as const)
    .filter(([, v]) => !isMissing(v)) as [number, number][];
  if (pts.length < 3) return [null, null];
  const vals = pts.map(([, v]) => v);
  const allPos = vals.every((v) => v > 0);
  const allNeg = vals.every((v) => v < 0);
  if (!allPos && !allNeg) return [null, null];
  const xs = pts.map(([i]) => i);
  const ys = pts.map(([, v]) => Math.log(Math.abs(v)));
  const n = xs.length;
  const xBar = xs.reduce((a, b) => a + b, 0) / n;
  const yBar = ys.reduce((a, b) => a + b, 0) / n;
  const sxx = xs.reduce((a, x) => a + (x - xBar) ** 2, 0);
  if (sxx === 0) return [null, null];
  const slope = xs.reduce((a, x, i) => a + (x - xBar) * (ys[i] - yBar), 0) / sxx;
  let growth = (Math.exp(slope) - 1) * 100;
  if (allNeg) growth = -growth;
  const intercept = yBar - slope * xBar;
  const ssTot = ys.reduce((a, y) => a + (y - yBar) ** 2, 0);
  const ssRes = xs.reduce((a, x, i) => a + (ys[i] - (slope * x + intercept)) ** 2, 0);
  const r2 = ssTot > 0 ? 1 - ssRes / ssTot : null;
  return [growth, r2];
}

/** Robust trend growth for display → [label, css class, r² badge text]. */
export function fmtTrendCagr(values: (number | null)[]): [string, "" | "up" | "down", string] {
  const [growth, r2] = trendGrowth(values);
  if (growth === null) return ["n/a", "", ""];
  const cls = growth > 0.05 ? "up" : growth < -0.05 ? "down" : "";
  const badge = r2 !== null ? `R² ${r2.toFixed(2)}` : "";
  return [`${growth >= 0 ? "+" : ""}${growth.toFixed(1)}%`, cls, badge];
}

/** 2024 → '24. */
export function shortYearLabel(year: number): string {
  return `'${String(year).slice(-2)}`;
}

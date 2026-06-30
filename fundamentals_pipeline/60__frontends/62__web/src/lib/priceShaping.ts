// Price windowing + resampling — port of slice_window / resample_prices / WINDOW_* in
// lib/prices.py. Pure functions over the daily PricePoint series.

import type { PricePoint } from "./queries/prices";

export const WINDOW_SETS: Record<string, string[]> = {
  Daily: ["1M", "3M", "6M", "1Y", "3Y", "5Y", "Max"],
  Weekly: ["3M", "6M", "1Y", "3Y", "5Y", "Max"],
  Monthly: ["1Y", "3Y", "5Y", "10Y", "Max"],
};
export const WINDOW_DEFAULTS: Record<string, string> = { Daily: "1Y", Weekly: "1Y", Monthly: "5Y" };
export const FREQUENCIES = ["Daily", "Weekly", "Monthly"] as const;

// label → [months, years] trailing offset; "Max" → null (no truncation).
const WINDOW_OFFSETS: Record<string, [number, number] | null> = {
  "1M": [1, 0],
  "3M": [3, 0],
  "6M": [6, 0],
  "1Y": [0, 1],
  "3Y": [0, 3],
  "5Y": [0, 5],
  "10Y": [0, 10],
  Max: null,
};

const t = (d: string) => new Date(`${d}T00:00:00Z`).getTime();

/** Trailing window slice; "Max"/unknown is a pass-through. Anchor = the series' last date. */
export function sliceWindow(series: PricePoint[], window: string): PricePoint[] {
  const offset = WINDOW_OFFSETS[window];
  if (!offset || series.length === 0) return series;
  const last = new Date(`${series[series.length - 1].date}T00:00:00Z`);
  const cutoff = new Date(last);
  cutoff.setUTCMonth(cutoff.getUTCMonth() - offset[0]);
  cutoff.setUTCFullYear(cutoff.getUTCFullYear() - offset[1]);
  const cut = cutoff.getTime();
  return series.filter((p) => t(p.date) >= cut);
}

/** Daily → period-close (last daily point per bucket). SMAs carried as the period's last value. */
export function resample(series: PricePoint[], freq: string): PricePoint[] {
  if (freq === "Daily" || series.length === 0) return series;
  const bucketOf =
    freq === "Monthly"
      ? (d: string) => d.slice(0, 7) // YYYY-MM
      : (d: string) => String(Math.floor(t(d) / (7 * 86_400_000))); // 7-day bucket
  const lastPerBucket = new Map<string, PricePoint>();
  for (const p of series) lastPerBucket.set(bucketOf(p.date), p); // series is ascending → last wins
  return Array.from(lastPerBucket.values());
}

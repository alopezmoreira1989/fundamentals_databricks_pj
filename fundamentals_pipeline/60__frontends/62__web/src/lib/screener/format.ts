// Cell + header formatting — port of _header_label / _fmt_value in views/screener.py.
// USD metrics (incl. Market Cap) render in $B; the raw value is kept on the row and the
// /1e9 happens here so display and sort stay consistent (sort uses the raw value).

import { MARKET_CAP } from "./constants";

const EM_DASH = "—";

export function isUsd(metric: string, unit: string | undefined): boolean {
  return metric === MARKET_CAP || unit === "usd";
}

/** Header text mirroring the old column_config labels ("$B" suffix for USD). */
export function headerLabel(metric: string, unit: string | undefined): string {
  return isUsd(metric, unit) ? `${metric} ($B)` : metric;
}

/** Format one numeric cell; null/NaN → em-dash. */
export function fmtValue(
  metric: string,
  unit: string | undefined,
  value: number | null | undefined,
): string {
  if (value === null || value === undefined || Number.isNaN(value)) return EM_DASH;
  if (isUsd(metric, unit)) return `$${(value / 1e9).toFixed(1)}B`;
  if (unit === "percent") return `${value.toFixed(1)}%`;
  if (unit === "ratio") return `${value.toFixed(2)}x`;
  return value.toFixed(2);
}

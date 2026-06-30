// SVG sparkline / mini-bar generator — port of 61__streamlit/lib/sparkline.py. The scaling
// is pure math copied near-literally; only the language changes. Returns a raw SVG string
// (injected via dangerouslySetInnerHTML in <Sparkline>). Colors are CSS custom properties so
// they stay token-driven (the Python passes hex literals from lib/colors.py — same values).

import { isMissing } from "./format";

/** lib/colors.py sparkline palette → the CSS vars they map to (same hex). */
export const SPARK_COLORS = {
  green: "var(--signal-good)", // #0F6E56
  blue: "var(--accent)", // #185FA5
  gray: "var(--ink-3)", // #888780
  coral: "var(--signal-bad)", // #993C1D
  amber: "var(--signal-warn)", // #BA7517
} as const;

const BAR_MUTED = "var(--rule)"; // muted non-final bars (Python used #CDD9CF)

/**
 * Full line sparkline (port of sparkline_svg) — used by the statement-table trend column.
 * `values` oldest→newest; missing points are skipped. Returns "" with <2 valid points.
 */
export function sparklineSvg(
  values: (number | null)[],
  opts: { color?: string; width?: number; height?: number; stroke?: number; endCircle?: boolean } = {},
): string {
  const { color = SPARK_COLORS.blue, width = 78, height = 20, stroke = 1.5, endCircle = false } = opts;
  const valid = values.map((v, i) => [i, v] as const).filter(([, v]) => !isMissing(v)) as [number, number][];
  if (valid.length < 2) return "";
  const vmin = Math.min(...valid.map(([, v]) => v));
  const vmax = Math.max(...valid.map(([, v]) => v));
  const span = vmax - vmin || 1;
  const n = values.length;
  const x = (i: number) => 2 + (i * (width - 4)) / Math.max(n - 1, 1);
  const y = (v: number) => 18 - (16 * (v - vmin)) / span; // verbatim from sparkline.py
  const points = valid.map(([i, v]) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  let svg =
    `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">` +
    `<polyline points="${points}" fill="none" stroke="${color}" stroke-width="${stroke}"/>`;
  if (endCircle) {
    const [li, lv] = valid[valid.length - 1];
    svg += `<circle cx="${x(li).toFixed(1)}" cy="${y(lv).toFixed(1)}" r="2.3" fill="${color}"/>`;
  }
  return `${svg}</svg>`;
}

/** Line sparkline (mini variant: 60×16). Returns "" with fewer than 2 valid points. */
export function miniSparklineSvg(values: (number | null)[], color: string = SPARK_COLORS.blue): string {
  const width = 60;
  const height = 16;
  const valid = values.map((v, i) => [i, v] as const).filter(([, v]) => !isMissing(v)) as [number, number][];
  if (valid.length < 2) return "";
  const vmin = Math.min(...valid.map(([, v]) => v));
  const vmax = Math.max(...valid.map(([, v]) => v));
  const span = vmax - vmin || 1;
  const n = values.length;
  const x = (i: number) => 2 + (i * (width - 4)) / Math.max(n - 1, 1);
  const y = (v: number) => 18 - (16 * (v - vmin)) / span; // verbatim from sparkline.py
  const points = valid.map(([i, v]) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  return (
    `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">` +
    `<polyline points="${points}" fill="none" stroke="${color}" stroke-width="1.5"/></svg>`
  );
}

/** Mini bar chart for the last `n` years; the final bar is colored, the rest muted. */
export function miniBarsSvg(values: (number | null)[], color: string = SPARK_COLORS.green, n = 5): string {
  const width = 64;
  const height = 26;
  const vals = (values.filter((v) => !isMissing(v)) as number[]).slice(-n);
  if (vals.length < 2) return "";
  const vmin = Math.min(...vals);
  const vmax = Math.max(...vals);
  const span = vmax - vmin || 1;
  const bw = (width - (vals.length - 1) * 3) / vals.length;
  const bars = vals
    .map((v, i) => {
      const h = 4 + ((v - vmin) / span) * (height - 6);
      const x = i * (bw + 3);
      const fill = i === vals.length - 1 ? color : BAR_MUTED;
      return `<rect x="${x.toFixed(1)}" y="${(height - h).toFixed(1)}" width="${bw.toFixed(1)}" height="${h.toFixed(1)}" rx="1" fill="${fill}"/>`;
    })
    .join("");
  return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">${bars}</svg>`;
}

/** Sparkline color for a metric row — port of _metric_sparkline_color. */
export function metricSparklineColor(category: string, metric: string, latest: number | null): string {
  if (category === "Valuation") return SPARK_COLORS.blue;
  if (metric.includes("YoY") || metric.includes("Growth")) {
    return !isMissing(latest) && latest < 0 ? SPARK_COLORS.coral : SPARK_COLORS.gray;
  }
  if (category === "Financial Health") return metric.includes("Debt") ? SPARK_COLORS.coral : SPARK_COLORS.gray;
  return SPARK_COLORS.green; // Profitability / Cash Flow — positive trends green
}

/** Categories whose rows use mini bars instead of a line sparkline (_USE_BARS). */
export const USE_BARS = new Set(["Profitability", "Growth"]);

// Intrinsic-value + valuation logic — port of the IV/football-field helpers in lib/render.py
// (_iv_metric_action, _clean_iv, _IV_SCENARIO_METHODS, iv_price_from_metrics, FF_METHODS,
// render_valuation_football_field geometry, _iv_bar_row). Pure: the React components in
// components/company/valuation/* render from these.

import type { CatGroup, MetricSeries } from "./queries/company";

// Mid value-metric base → the tag used in its paired "MoS % ({tag}, {period})" label.
export const IV_SCENARIO_METHODS: Record<string, string> = {
  "Graham Number": "Graham Number",
  "Graham Revised Value": "Graham Revised",
  "DCF Value per Share": "DCF",
  "Owner Earnings Value/Share": "Owner Earnings",
};

export type IvAction = "skip" | "scenario" | "normal";

/** Classify an IV metric: bull/bear variants + MoS fold into a scenario row (skip); a mid
 * value-metric of a scenario method → 3-col row; anything else → generic single-value. */
export function ivMetricAction(metric: string): IvAction {
  if (metric.endsWith(" — Bull") || metric.endsWith(" — Bear")) return "skip";
  const base = metric.split(" (")[0].trim();
  if (base === "MoS %") return "skip";
  if (base in IV_SCENARIO_METHODS) return "scenario";
  return "normal";
}

/** Strip the (FY)/(TTM) period suffix from IV labels for display. */
export function cleanIv(name: string): string {
  return name.replace(/\s*\((FY|TTM)\)/, "").replace(/,\s*(FY|TTM)\)/, ")");
}

export interface Scenario {
  label: string;
  unit: string | null;
  bear: number | null;
  mid: number | null;
  bull: number | null;
  bearMos: number | null;
  midMos: number | null;
  bullMos: number | null;
}

/** Assemble the bear/mid/bull value + MoS triplets for one scenario method from a subgroup. */
export function buildScenario(rows: MetricSeries[], midValueMetric: string, period: string): Scenario {
  const byMetric = new Map(rows.map((r) => [r.metric, r]));
  const get = (label: string): number | null => byMetric.get(label)?.latest ?? null;
  const base = midValueMetric.split(" (")[0].trim();
  const mosTag = IV_SCENARIO_METHODS[base];
  const valMid = `${base} (${period})`;
  const mosMid = `MoS % (${mosTag}, ${period})`;
  return {
    label: base,
    unit: byMetric.get(valMid)?.unit ?? "usd",
    bear: get(`${valMid} — Bear`),
    mid: get(valMid),
    bull: get(`${valMid} — Bull`),
    bearMos: get(`${mosMid} — Bear`),
    midMos: get(mosMid),
    bullMos: get(`${mosMid} — Bull`),
  };
}

// Football-field methods (Graham Number excluded on purpose — often a wild outlier).
// [value-metric base, MoS method tag, display label]
export const FF_METHODS: [string, string, string][] = [
  ["DCF Value per Share", "DCF", "DCF"],
  ["Owner Earnings Value/Share", "Owner Earnings", "Owner Earnings"],
  ["Graham Revised Value", "Graham Revised", "Graham Revised"],
];

/** Flat label → latest-value map across every IV-category row (both FY and TTM). */
export function ivLatestMap(categories: CatGroup[]): Map<string, number | null> {
  const out = new Map<string, number | null>();
  const iv = categories.find((c) => c.category === "Intrinsic Value");
  if (iv) for (const sg of iv.subgroups) for (const r of sg.rows) out.set(r.metric, r.latest);
  return out;
}

/** Back out the market price the IV methods were scored against: price = iv·(1 − MoS/100). */
export function ivPriceFromMetrics(ivLatest: Map<string, number | null>, period: string): number | null {
  for (const [base, tag] of FF_METHODS) {
    const v = ivLatest.get(`${base} (${period})`);
    const mos = ivLatest.get(`MoS % (${tag}, ${period})`);
    if (v === null || v === undefined || mos === null || mos === undefined || v <= 0) continue;
    const price = v * (1 - mos / 100);
    if (Number.isFinite(price) && price > 0) return price;
  }
  return null;
}

// ── mini range bar (one scenario method) — port of _iv_bar_row ────────────────────────────
export interface IvBarGeom {
  segLeft: number;
  segWidth: number;
  marks: { cls: string; left: number }[];
}

export function ivBarGeom(
  bear: number | null,
  mid: number | null,
  bull: number | null,
  price: number | null,
): IvBarGeom | null {
  if (mid === null || mid <= 0) return null;
  const pts = [bear, mid, bull].filter((v): v is number => v !== null && v > 0);
  let lo = Math.min(...pts);
  let hi = Math.max(...pts);
  const hasPrice = price !== null && Number.isFinite(price) && price > 0;
  if (hasPrice) {
    lo = Math.min(lo, price!);
    hi = Math.max(hi, price!);
  }
  const pad = (hi - lo) * 0.08 || hi * 0.08 || 1;
  const dmin = lo - pad;
  const span = hi + pad - (lo - pad) || 1;
  const pct = (v: number) => Math.max(0, Math.min(100, ((v - dmin) / span) * 100));

  const segLo = bear !== null && bear > 0 ? bear : mid;
  const segHi = bull !== null && bull > 0 ? bull : mid;
  const pLo = pct(Math.min(segLo, segHi));
  const pHi = pct(Math.max(segLo, segHi));
  const marks: { cls: string; left: number }[] = [];
  if (bear !== null && bear > 0) marks.push({ cls: "mk-bear", left: pct(bear) });
  marks.push({ cls: "mk-mid", left: pct(mid) });
  if (bull !== null && bull > 0) marks.push({ cls: "mk-bull", left: pct(bull) });
  if (hasPrice) marks.push({ cls: "mk-price", left: pct(price!) });
  return { segLeft: pLo, segWidth: Math.max(pHi - pLo, 0.5), marks };
}

// ── valuation football field SVG — port of render_valuation_football_field ─────────────────
const CORAL = "#993C1D";
const GREEN = "#0F6E56";
const BLUE = "#185FA5";
const GRAY = "#888780";
const FF_POS_SOFT = "#E1F5EE";
const FF_NEG_SOFT = "#FAECE7";
const FF_ACC_SOFT = "#E6F1FB";
const FF_INK = "#1C1B18";

interface FfBar {
  label: string;
  base: number;
  low: number;
  high: number;
}

export function footballBars(ivLatest: Map<string, number | null>, period: string): FfBar[] {
  const bars: FfBar[] = [];
  for (const [baseMetric, , label] of FF_METHODS) {
    const base = ivLatest.get(`${baseMetric} (${period})`);
    if (base === null || base === undefined || !Number.isFinite(base) || base <= 0) continue;
    let low = ivLatest.get(`${baseMetric} (${period}) — Bear`) ?? null;
    let high = ivLatest.get(`${baseMetric} (${period}) — Bull`) ?? null;
    low = low !== null && Number.isFinite(low) && low > 0 ? low : base;
    high = high !== null && Number.isFinite(high) && high > 0 ? high : base;
    bars.push({ label, base, low: Math.min(low, high), high: Math.max(low, high) });
  }
  return bars;
}

const money = (v: number) => `$${Math.round(v).toLocaleString("en-US")}`;
const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/** Returns the football-field SVG string, or "" when no method has a usable mid value. */
export function footballFieldSvg(bars: FfBar[], price: number | null): string {
  if (bars.length === 0) return "";
  const hasPrice = price !== null && Number.isFinite(price) && price > 0;
  let lo = Math.min(...bars.map((b) => b.low));
  let hi = Math.max(...bars.map((b) => b.high));
  if (hasPrice) {
    lo = Math.min(lo, price!);
    hi = Math.max(hi, price!);
  }
  const pad = (hi - lo) * 0.1 || hi * 0.1 || 1;
  const dmin = lo - pad;
  const span = hi + pad - (lo - pad) || 1;

  const W = 720;
  const labelW = 150;
  const rpad = 28;
  const rowH = 32;
  const top = 12;
  const axisH = 30;
  const x0 = labelW;
  const x1 = W - rpad;
  const plotBottom = top + bars.length * rowH;
  const H = plotBottom + axisH;
  const X = (v: number) => x0 + ((v - dmin) / span) * (x1 - x0);

  const parts: string[] = [
    `<svg viewBox="0 0 ${W} ${H}" width="100%" preserveAspectRatio="xMidYMid meet" font-family="JetBrains Mono, SF Mono, monospace">`,
  ];

  if (hasPrice) {
    const px = X(price!);
    parts.push(
      `<line x1="${px.toFixed(1)}" y1="${top}" x2="${px.toFixed(1)}" y2="${plotBottom}" stroke="${CORAL}" stroke-width="1.5" stroke-dasharray="3 2"/>`,
      `<text x="${px.toFixed(1)}" y="${top - 2}" fill="${CORAL}" font-size="11" text-anchor="middle">${money(price!)}</text>`,
    );
  }

  bars.forEach((bar, i) => {
    const cy = top + i * rowH + rowH / 2;
    const [fill, edge] = hasPrice ? (bar.base >= price! ? [FF_POS_SOFT, GREEN] : [FF_NEG_SOFT, CORAL]) : [FF_ACC_SOFT, BLUE];
    const xl = X(bar.low);
    const xh = X(bar.high);
    const xb = X(bar.base);
    parts.push(
      `<rect x="${xl.toFixed(1)}" y="${(cy - 7).toFixed(1)}" width="${Math.max(xh - xl, 1).toFixed(1)}" height="14" rx="3" fill="${fill}" stroke="${edge}" stroke-width="1"/>`,
      `<circle cx="${xb.toFixed(1)}" cy="${cy.toFixed(1)}" r="4" fill="${BLUE}"/>`,
      `<text x="8" y="${(cy + 4).toFixed(1)}" fill="${FF_INK}" font-size="12" font-family="-apple-system, system-ui, sans-serif">${esc(bar.label)}</text>`,
    );
    if (xh > x0 + 0.72 * (x1 - x0)) {
      parts.push(`<text x="${(xl - 6).toFixed(1)}" y="${(cy + 4).toFixed(1)}" fill="${GRAY}" font-size="10.5" text-anchor="end">${money(bar.base)}</text>`);
    } else {
      parts.push(`<text x="${(xh + 6).toFixed(1)}" y="${(cy + 4).toFixed(1)}" fill="${GRAY}" font-size="10.5">${money(bar.base)}</text>`);
    }
  });

  for (let t = 0; t < 5; t++) {
    const v = dmin + (span * t) / 4;
    const xt = X(v);
    parts.push(
      `<line x1="${xt.toFixed(1)}" y1="${plotBottom}" x2="${xt.toFixed(1)}" y2="${plotBottom + 4}" stroke="${GRAY}" stroke-width="1"/>`,
      `<text x="${xt.toFixed(1)}" y="${plotBottom + 16}" fill="${GRAY}" font-size="11" text-anchor="middle">${money(v)}</text>`,
    );
  }
  parts.push("</svg>");
  return parts.join("");
}

// ── Valuation card range bar (multiples) — port of _render_valuation_row's vbar ────────────
export const VALUATION_MULTIPLES = new Set(["P/E", "P/S", "P/FCF", "P/B", "EV/EBITDA"]);

export interface ValBar {
  avgLeft: number; // % position of the historical average marker
  nowLeft: number; // % position of the current value
  avg: number;
  devPct: number; // (now − avg)/|avg|, as a fraction
}

/** min–avg–max range-bar geometry for a valuation multiple, or null with <3 history points. */
export function valBarGeom(values: (number | null)[]): ValBar | null {
  const hist = values.filter((v): v is number => v !== null && !Number.isNaN(v));
  if (hist.length < 3) return null;
  const mn = Math.min(...hist);
  const mx = Math.max(...hist);
  const now = hist[hist.length - 1];
  const avg = hist.slice(0, -1).reduce((a, b) => a + b, 0) / Math.max(hist.length - 1, 1);
  const sp = mx - mn || 1;
  return {
    avgLeft: Math.max(0, Math.min(100, ((avg - mn) / sp) * 100)),
    nowLeft: Math.max(0, Math.min(100, ((now - mn) / sp) * 100)),
    avg,
    devPct: avg ? (now - avg) / Math.abs(avg) : 0,
  };
}

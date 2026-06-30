// Balance-sheet liquidity-ramp composition — pure-logic port of render_bs_composition's
// data side (lib/charts.py): family → current/non-current → line item, with an "Other" plug
// so every bar ties out to Total Assets, and a dark→light colour ramp per family. The hex
// ramp endpoints are chart-specific (not app tokens) and copied verbatim from charts.py.

import { type StatementFrame, frameVal } from "../queries/statements";

const ASSET_DARK = "#0E3D6B";
const ASSET_LIGHT = "#C9DBED";
const LIAB_DARK = "#7A2415";
const LIAB_LIGHT = "#E2B0A4";
const EQUITY = "#0F6E56"; // GREEN
const INK = "#1C1B18";
const CREAM = "#FAF8F4";

// Fixed liquidity rank per subgroup: [display label, concept]. Order is liquidity, not value.
const ASSET_CURRENT: [string, string][] = [
  ["Cash & equivalents", "Cash & Equivalents"],
  ["ST investments", "Short-term Investments"],
  ["Receivables", "Accounts Receivable"],
  ["Inventory", "Inventory"],
];
const ASSET_NONCURRENT: [string, string][] = [
  ["PP&E", "PP&E Net"],
  ["Goodwill", "Goodwill"],
  ["Intangibles", "Intangible Assets"],
];
const LIAB_CURRENT: [string, string][] = [
  ["Payables", "Accounts Payable"],
  ["ST debt", "Short-term Debt"],
];
const LIAB_NONCURRENT: [string, string][] = [["LT debt", "Long-term Debt"]];

const SEG_CAP = 5;
const EPS = 1e6;

export interface Segment {
  name: string;
  value: number;
  color: string;
  ink: string;
  boundary?: boolean;
}
export interface SubGroup {
  name: string;
  subtotal: number;
  segs: Segment[];
}
export interface BsComposition {
  total: number;
  equity: number;
  liabTotal: number;
  assetGroups: SubGroup[];
  liabGroups: SubGroup[];
  assetsFlat: Segment[];
  liabFlat: Segment[];
  equitySeg: Segment;
  rampAsset: [string, string];
  rampLiab: [string, string];
}

function hexToRgb(h: string): [number, number, number] {
  const s = h.replace("#", "");
  return [parseInt(s.slice(0, 2), 16), parseInt(s.slice(2, 4), 16), parseInt(s.slice(4, 6), 16)];
}
function lerpHex(c0: string, c1: string, t: number): string {
  const a = hexToRgb(c0);
  const b = hexToRgb(c1);
  const rgb = a.map((v, i) => Math.round(v + (b[i] - v) * t));
  return `#${rgb.map((v) => v.toString(16).padStart(2, "0")).join("")}`.toUpperCase();
}
function ramp(c0: string, c1: string, n: number): string[] {
  if (n <= 1) return Array(Math.max(n, 0)).fill(c0);
  return Array.from({ length: n }, (_, i) => lerpHex(c0, c1, i / (n - 1)));
}
function inkFor(hex: string): string {
  const [r, g, b] = hexToRgb(hex);
  const lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
  return lum > 0.6 ? INK : CREAM;
}

/** Compute the composition for one fiscal year, or null when Total Assets is missing/≤0. */
export function buildBsComposition(frame: StatementFrame, year: number): BsComposition | null {
  const val = (concept: string): number | null => frameVal(frame, concept, year);

  const total = val("Total Assets");
  if (total === null || total <= 0) return null;

  const buildSubgroup = (name: string, items: [string, string][], subtotalRaw: number): SubGroup => {
    const subtotal = Math.max(0, subtotalRaw);
    let present: [string, number][] = [];
    for (const [lbl, c] of items) {
      const v = val(c);
      if (v !== null && v > 0) present.push([lbl, Math.max(0, v)]);
    }
    if (present.length > SEG_CAP) {
      const keep = new Set(
        [...present].sort((a, b) => b[1] - a[1]).slice(0, SEG_CAP).map(([lbl]) => lbl),
      );
      present = present.filter(([lbl]) => keep.has(lbl));
    }
    const segs: Segment[] = present.map(([name2, value]) => ({ name: name2, value, color: "", ink: "" }));
    const other = subtotal - segs.reduce((a, s) => a + s.value, 0);
    if (other > EPS) {
      segs.push({ name: name ? `Other ${name.toLowerCase()}` : "Other", value: other, color: "", ink: "" });
    }
    return { name, subtotal, segs };
  };

  const assignRamp = (subgroups: SubGroup[], dark: string, light: string): Segment[] => {
    const flat = subgroups.flatMap((sg) => sg.segs);
    const colors = ramp(dark, light, flat.length);
    flat.forEach((s, i) => {
      s.color = colors[i];
      s.ink = inkFor(colors[i]);
    });
    for (const sg of subgroups.slice(1)) {
      if (sg.segs.length) {
        sg.segs[0].boundary = true;
        break;
      }
    }
    return flat;
  };

  const tca = val("Total Current Assets");
  const tcl = val("Total Current Liabilities");
  const classified = tca !== null && tcl !== null;

  const tse = val("Total Stockholders Equity");
  const tl = val("Total Liabilities");
  let equity: number;
  if (tse !== null) equity = Math.max(0, tse);
  else if (tl !== null) equity = Math.max(0, total - tl);
  else equity = 0;
  const nonEquity = Math.max(0, total - equity);

  let assetGroups: SubGroup[];
  let liabGroups: SubGroup[];
  if (classified) {
    assetGroups = [
      buildSubgroup("Current", ASSET_CURRENT, tca!),
      buildSubgroup("Non-current", ASSET_NONCURRENT, total - tca!),
    ];
    const curLiab = Math.min(Math.max(0, tcl!), nonEquity);
    liabGroups = [
      buildSubgroup("Current", LIAB_CURRENT, curLiab),
      buildSubgroup("Non-current", LIAB_NONCURRENT, nonEquity - curLiab),
    ];
  } else {
    assetGroups = [buildSubgroup("", [...ASSET_CURRENT, ...ASSET_NONCURRENT], total)];
    liabGroups = [buildSubgroup("", [...LIAB_CURRENT, ...LIAB_NONCURRENT], nonEquity)];
  }

  const assetsFlat = assignRamp(assetGroups, ASSET_DARK, ASSET_LIGHT);
  const liabFlat = assignRamp(liabGroups, LIAB_DARK, LIAB_LIGHT);
  const equitySeg: Segment = { name: "Equity", value: equity, color: EQUITY, ink: inkFor(EQUITY) };
  const liabTotal = liabFlat.reduce((a, s) => a + s.value, 0);

  return {
    total,
    equity,
    liabTotal,
    assetGroups,
    liabGroups,
    assetsFlat,
    liabFlat,
    equitySeg,
    rampAsset: [ASSET_DARK, ASSET_LIGHT],
    rampLiab: [LIAB_DARK, LIAB_LIGHT],
  };
}

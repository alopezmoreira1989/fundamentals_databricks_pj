// Price KPI strip — port of render_price_kpis: latest price, 1Y return, 52-week range, window
// CAGR. Derived stats use adjClose (split-safe, consistent with the plotted line); the LATEST
// PRICE headline uses raw close. All from the full daily series, independent of the freq toggle.

import type { PricePoint } from "@/lib/queries/prices";
import { fmtCagr } from "@/lib/format";

const px = (v: number) =>
  Math.abs(v) < 100 ? `$${v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : `$${Math.round(v).toLocaleString("en-US")}`;

function priceDelta(pct: number | null, suffix = ""): [string, string] {
  if (pct === null || Number.isNaN(pct)) return ["—", "flat"];
  if (pct > 0.05) return [`▲ ${pct.toFixed(1)}%${suffix}`, "up"];
  if (pct < -0.05) return [`▼ ${Math.abs(pct).toFixed(1)}%${suffix}`, "down"];
  return [`≈ ${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%${suffix}`, "flat"];
}
function signedPct(pct: number | null): [string, string] {
  if (pct === null || Number.isNaN(pct)) return ["—", "flat"];
  const cls = pct > 0.05 ? "up" : pct < -0.05 ? "down" : "flat";
  return [`${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%`, cls];
}
const valColor = (cls: string) => (cls === "up" ? "var(--signal-good)" : cls === "down" ? "var(--signal-bad)" : "var(--ink)");

function Card({ label, value, delta, deltaCls, valueCls }: { label: string; value: string; delta: string; deltaCls: string; valueCls?: string }) {
  return (
    <div className="kpi">
      <div className="label">{label}</div>
      <div className="value" style={valueCls ? { color: valColor(valueCls) } : undefined}>
        {value}
      </div>
      <div className={`delta ${deltaCls}`}>{delta}</div>
    </div>
  );
}

const fmtDate = (d: string, opts: Intl.DateTimeFormatOptions) => new Date(`${d}T00:00:00Z`).toLocaleDateString("en-US", { timeZone: "UTC", ...opts });
const dayMs = 86_400_000;

export default function PriceKpis({ series }: { series: PricePoint[] }) {
  if (series.length === 0) return null;
  const s = series; // already sorted ascending, adjClose non-null
  const lastIdx = s.length - 1;
  const last = s[lastIdx];
  const lastDate = new Date(`${last.date}T00:00:00Z`).getTime();
  const lastAdj = last.adjClose;
  const price = last.close ?? lastAdj;

  // 1) latest price + 1d change (raw close)
  let chg1d: number | null = null;
  const prev = s[lastIdx - 1];
  if (prev && prev.close !== null && prev.close !== 0) chg1d = ((price - prev.close) / Math.abs(prev.close)) * 100;
  const [d1Label, d1Cls] = priceDelta(chg1d, " (1d)");

  // 2) 1-year return (adjClose as-of ~1y ago)
  const oneYAgo = lastDate - 365 * dayMs;
  const priorRows = s.filter((p) => new Date(`${p.date}T00:00:00Z`).getTime() <= oneYAgo);
  let card2: React.ReactNode;
  if (priorRows.length === 0) {
    card2 = <Card label="1Y RETURN" value="—" delta="· < 1 year of data" deltaCls="flat" />;
  } else {
    const base = priorRows[priorRows.length - 1].adjClose;
    const ret = base ? (lastAdj / base - 1) * 100 : null;
    const [rv, rcls] = signedPct(ret);
    card2 = <Card label="1Y RETURN" value={rv} delta={`from ${px(base)} · ${fmtDate(priorRows[priorRows.length - 1].date, { month: "short", year: "numeric" })}`} deltaCls="flat" valueCls={rcls} />;
  }

  // 3) 52-week range + position
  const wkAgo = lastDate - 52 * 7 * dayMs;
  const win = s.filter((p) => new Date(`${p.date}T00:00:00Z`).getTime() >= wkAgo);
  const hi = Math.max(...win.map((p) => p.adjClose));
  const lo = Math.min(...win.map((p) => p.adjClose));
  const pos = hi > lo ? ((lastAdj - lo) / (hi - lo)) * 100 : 100;

  // 4) annualized return over the available window
  const first = s[0].adjClose;
  const years = Math.max(1, Math.round((lastDate - new Date(`${s[0].date}T00:00:00Z`).getTime()) / dayMs / 365.25));
  const [cagrLabel, cagrCls] = fmtCagr(first, lastAdj, years);

  return (
    <div className="kpi-strip">
      <Card label="LATEST PRICE" value={px(price)} delta={`${d1Label} · ${fmtDate(last.date, { month: "short", day: "numeric", year: "numeric" })}`} deltaCls={d1Cls} />
      {card2}
      <Card label="52W RANGE" value={`${px(lo)} – ${px(hi)}`} delta={`today at ${pos.toFixed(0)}% of range`} deltaCls="flat" />
      <Card label="PRICE CAGR" value={cagrLabel} delta={`annualized · ${years}y`} deltaCls={cagrCls} />
    </div>
  );
}

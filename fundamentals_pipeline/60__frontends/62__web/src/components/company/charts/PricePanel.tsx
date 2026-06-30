"use client";

// Price tab panel — frequency + quick-range controls, the KPI strip, and the adjusted-close +
// SMA 20/50/200 line chart with a Brush for fine zoom. Ports the Price-tab assembly in
// views/company.py (prices_for → slice_window → render_price_kpis + price_chart).

import { useMemo, useState } from "react";
import { Brush, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { PricePoint } from "@/lib/queries/prices";
import { FREQUENCIES, resample, sliceWindow, WINDOW_DEFAULTS, WINDOW_SETS } from "@/lib/priceShaping";
import PriceKpis from "./PriceKpis";

const SERIES = [
  { key: "adjClose", name: "Price", color: "#1a1a18", width: 2.4 },
  { key: "sma20", name: "SMA 20", color: "#C8881F", width: 1.1 },
  { key: "sma50", name: "SMA 50", color: "var(--signal-good)", width: 1.1 },
  { key: "sma200", name: "SMA 200", color: "var(--signal-bad)", width: 1.1 },
] as const;

const fmtAxisDate = (d: string) => new Date(`${d}T00:00:00Z`).toLocaleDateString("en-US", { timeZone: "UTC", month: "short", year: "2-digit" });

export default function PricePanel({ series }: { series: PricePoint[] }) {
  const [freq, setFreq] = useState<string>("Daily");
  const [windows, setWindows] = useState<Record<string, string>>({});
  const window = windows[freq] ?? WINDOW_DEFAULTS[freq];

  const windowedDaily = useMemo(() => sliceWindow(series, window), [series, window]);
  const chartData = useMemo(() => resample(windowedDaily, freq), [windowedDaily, freq]);

  const setWindow = (w: string) => setWindows((m) => ({ ...m, [freq]: w }));

  if (series.length === 0) {
    return (
      <p className="font-sans text-sm text-ink-2">
        No price history available for this ticker yet. Prices publish with the next pipeline run.
      </p>
    );
  }

  return (
    <div>
      <div className="panel-header">
        <h2>Price</h2>
        <div className="meta">
          Adjusted close · SMA 20/50/200 · last {window} · {freq} · source: market_prices_daily
        </div>
      </div>

      <div className="mb-3 flex flex-wrap items-center gap-2">
        <div className="flex gap-1">
          {FREQUENCIES.map((f) => (
            <button
              key={f}
              type="button"
              onClick={() => setFreq(f)}
              className={
                "rounded border px-2.5 py-0.5 font-mono text-[11px] transition-colors " +
                (f === freq ? "border-accent bg-accent-soft text-accent-ink" : "border-rule bg-bg-card text-ink-3 hover:border-accent")
              }
            >
              {f}
            </button>
          ))}
        </div>
        <span className="text-ink-3">·</span>
        <div className="flex flex-wrap gap-1">
          {WINDOW_SETS[freq].map((w) => (
            <button
              key={w}
              type="button"
              onClick={() => setWindow(w)}
              className={
                "rounded border px-2 py-0.5 font-mono text-[11px] font-semibold transition-colors " +
                (w === window ? "border-accent bg-accent text-white" : "border-rule bg-bg-card text-ink-2 hover:border-accent")
              }
            >
              {w}
            </button>
          ))}
        </div>
      </div>

      <PriceKpis series={windowedDaily} />

      <div className="qchart mt-4">
        <ResponsiveContainer width="100%" height={380}>
          <LineChart data={chartData} margin={{ top: 8, right: 16, bottom: 4, left: 0 }}>
            <CartesianGrid stroke="var(--rule-soft)" vertical={false} />
            <XAxis dataKey="date" tickFormatter={fmtAxisDate} minTickGap={48} tick={{ fontSize: 10, fill: "var(--ink-3)" }} axisLine={{ stroke: "var(--rule)" }} tickLine={false} />
            <YAxis
              domain={["auto", "auto"]}
              width={52}
              tick={{ fontSize: 10, fill: "var(--ink-3)" }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(v: number) => `$${v >= 100 ? Math.round(v) : v.toFixed(0)}`}
            />
            <Tooltip
              labelFormatter={(d) => new Date(`${d}T00:00:00Z`).toLocaleDateString("en-US", { timeZone: "UTC", year: "numeric", month: "short", day: "numeric" })}
              formatter={(value, name) => [`$${Number(value).toFixed(2)}`, name]}
              contentStyle={{ fontFamily: "var(--font-mono)", fontSize: 12, borderColor: "var(--rule)" }}
            />
            {SERIES.map((s) => (
              <Line key={s.key} type="monotone" dataKey={s.key} name={s.name} stroke={s.color} strokeWidth={s.width} dot={false} connectNulls isAnimationActive={false} />
            ))}
            <Brush dataKey="date" height={20} stroke="var(--accent)" tickFormatter={fmtAxisDate} travellerWidth={8} />
          </LineChart>
        </ResponsiveContainer>
        <div className="qchart-legend mt-2">
          {SERIES.map((s) => (
            <span key={s.key}>
              <span className="legend-dot" style={{ background: s.color }} />
              {s.name}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}

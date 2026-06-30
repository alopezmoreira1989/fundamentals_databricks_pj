"use client";

// Shared revenue-bars + YoY-growth-line combo (recharts). Used by the annual IS chart and the
// quarterly chart — both are the same shape, differing only in label cadence + data source.

import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { LegendDot, QChart } from "./QChart";

const ACCENT = "var(--accent)";
const GREEN = "var(--signal-good)";

export interface RevYoYPoint {
  label: string;
  revenueB: number | null;
  yoy: number | null;
}

export default function RevenueYoYChart({ title, data }: { title: string; data: RevYoYPoint[] }) {
  if (data.length === 0 || !data.some((d) => d.revenueB !== null)) return null;
  return (
    <QChart
      title={title}
      legend={
        <>
          <LegendDot color={ACCENT} label="Revenue ($B)" />
          <LegendDot color={GREEN} label="YoY growth (%)" />
        </>
      }
    >
      <ResponsiveContainer width="100%" height={280}>
        <ComposedChart data={data} margin={{ top: 10, right: 12, bottom: 4, left: 0 }}>
          <CartesianGrid stroke="var(--rule-soft)" vertical={false} />
          <XAxis dataKey="label" tick={{ fontSize: 11, fill: "var(--ink-2)" }} axisLine={{ stroke: "var(--ink)" }} tickLine={false} />
          <YAxis
            yAxisId="rev"
            tick={{ fontSize: 10, fill: "var(--ink-3)" }}
            axisLine={false}
            tickLine={false}
            width={44}
            tickFormatter={(v: number) => `${v.toFixed(0)}`}
          />
          <YAxis
            yAxisId="yoy"
            orientation="right"
            tick={{ fontSize: 10, fill: GREEN }}
            axisLine={false}
            tickLine={false}
            width={38}
            tickFormatter={(v: number) => `${v > 0 ? "+" : ""}${v.toFixed(0)}`}
          />
          <Tooltip
            formatter={(value, name) => {
              const v = Number(value);
              return name === "revenueB"
                ? [`$${v.toFixed(1)}B`, "Revenue"]
                : [`${v > 0 ? "+" : ""}${v.toFixed(1)}%`, "YoY"];
            }}
            contentStyle={{ fontFamily: "var(--font-mono)", fontSize: 12, borderColor: "var(--rule)" }}
          />
          <Bar yAxisId="rev" dataKey="revenueB" fill={ACCENT} radius={[2, 2, 0, 0]} maxBarSize={48} />
          <Line yAxisId="yoy" dataKey="yoy" stroke={GREEN} strokeWidth={2} dot={{ r: 3, fill: GREEN }} connectNulls />
        </ComposedChart>
      </ResponsiveContainer>
    </QChart>
  );
}

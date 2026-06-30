"use client";

// Cash-flow chart — Operating Cash Flow vs Free Cash Flow per fiscal year (gap = CapEx), the
// recharts analogue of render_cf_fcf. FCF = OCF − |CapEx|; a year missing CapEx has no FCF
// bar (never FCF = OCF). Rendered as grouped bars (clearer + interactive than the overlaid
// SVG bars in Streamlit); the tooltip surfaces the implied CapEx gap. Hidden when OCF absent.

import { Bar, BarChart, CartesianGrid, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { StatementFrame } from "@/lib/queries/statements";
import { frameVal } from "@/lib/queries/statements";
import { LegendDot, QChart, shortYear } from "./QChart";

const ACCENT_SOFT = "#E6F1FB";
const ACCENT = "var(--accent)";
const GREEN = "var(--signal-good)";

export default function CashFlowChart({ frame }: { frame: StatementFrame }) {
  const ocfByYear = new Map(frame.years.map((y) => [y, frameVal(frame, "Operating Cash Flow", y)]));
  if (!Array.from(ocfByYear.values()).some((v) => v !== null)) return null;

  const data = frame.years.map((year) => {
    const ocf = ocfByYear.get(year) ?? null;
    const capex = frameVal(frame, "CapEx", year);
    const fcf = ocf !== null && capex !== null ? ocf - Math.abs(capex) : null;
    return {
      label: shortYear(year),
      ocfB: ocf !== null ? ocf / 1e9 : null,
      fcfB: fcf !== null ? fcf / 1e9 : null,
    };
  });

  return (
    <QChart
      title="Operating vs free cash flow"
      legend={
        <>
          <LegendDot color={ACCENT_SOFT} border={ACCENT} label="Operating cash flow" />
          <LegendDot color={GREEN} label="Free cash flow" />
        </>
      }
    >
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={data} margin={{ top: 10, right: 12, bottom: 4, left: 0 }} barGap={2}>
          <CartesianGrid stroke="var(--rule-soft)" vertical={false} />
          <XAxis dataKey="label" tick={{ fontSize: 11, fill: "var(--ink-2)" }} axisLine={{ stroke: "var(--ink)" }} tickLine={false} />
          <YAxis
            tick={{ fontSize: 10, fill: "var(--ink-3)" }}
            axisLine={false}
            tickLine={false}
            width={44}
            tickFormatter={(v: number) => `${v.toFixed(0)}`}
          />
          <ReferenceLine y={0} stroke="var(--ink)" />
          <Tooltip
            formatter={(value, name) => [`$${Number(value).toFixed(1)}B`, name === "ocfB" ? "Operating CF" : "Free CF"]}
            contentStyle={{ fontFamily: "var(--font-mono)", fontSize: 12, borderColor: "var(--rule)" }}
          />
          <Bar dataKey="ocfB" fill={ACCENT_SOFT} stroke={ACCENT} radius={[2, 2, 0, 0]} maxBarSize={42} />
          <Bar dataKey="fcfB" fill={GREEN} radius={[2, 2, 0, 0]} maxBarSize={26} />
        </BarChart>
      </ResponsiveContainer>
    </QChart>
  );
}

"use client";

// Quarterly revenue bars + YoY-growth line (same quarter prior year) — port of
// render_quarterly_combo, reusing the shared revenue/YoY combo.

import type { QuarterlyData } from "@/lib/queries/quarterly";
import RevenueYoYChart from "./RevenueYoYChart";

export default function QuarterlyCombo({ data }: { data: QuarterlyData }) {
  const points = data.quarters.map((label, i) => ({
    label,
    revenueB: data.revenue[i] !== null ? data.revenue[i]! / 1e9 : null,
    yoy: data.yoy[i],
  }));
  return <RevenueYoYChart title="Quarterly revenue with YoY growth" data={points} />;
}

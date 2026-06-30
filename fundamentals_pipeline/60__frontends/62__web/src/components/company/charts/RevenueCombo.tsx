"use client";

// Annual income-statement chart — Revenue bars + YoY-growth line. YoY vs the prior fiscal year
// (looked up by year so a gap can't fabricate a YoY). Port of render_is_revenue_combo.

import type { StatementFrame } from "@/lib/queries/statements";
import { frameVal } from "@/lib/queries/statements";
import RevenueYoYChart from "./RevenueYoYChart";
import { shortYear } from "./QChart";

export default function RevenueCombo({ frame }: { frame: StatementFrame }) {
  const revByYear = new Map(frame.years.map((y) => [y, frameVal(frame, "Revenue", y)]));
  if (!Array.from(revByYear.values()).some((v) => v !== null)) return null;

  const data = frame.years.map((year) => {
    const rev = revByYear.get(year) ?? null;
    const prior = revByYear.get(year - 1) ?? null;
    const yoy = rev !== null && prior !== null && prior !== 0 ? ((rev - prior) / Math.abs(prior)) * 100 : null;
    return { label: shortYear(year), revenueB: rev !== null ? rev / 1e9 : null, yoy };
  });

  return <RevenueYoYChart title="Revenue with YoY growth" data={data} />;
}

// Revenue → Net income margin bridge — port of render_waterfall (a %-of-revenue horizontal
// bar breakdown for the latest FY). Hidden when Revenue is absent/zero.

import type { StatementFrame } from "@/lib/queries/statements";
import { frameVal } from "@/lib/queries/statements";
import { fmtKpi } from "@/lib/format";
import { shortYear } from "./QChart";

// [concept, fill] — copied from _WATERFALL_ITEMS.
const ITEMS: [string, string][] = [
  ["Revenue", "#185FA5"],
  ["Gross Profit", "#0F6E56"],
  ["Operating Income", "#534AB7"],
  ["Net Income", "#185FA5"],
];

export default function MarginBridge({ frame }: { frame: StatementFrame }) {
  if (frame.years.length === 0) return null;
  const latest = frame.years[frame.years.length - 1];
  const revenue = frameVal(frame, "Revenue", latest);
  if (!revenue) return null;
  const ni = frameVal(frame, "Net Income", latest) ?? 0;

  return (
    <div className="breakdown">
      <div className="breakdown-title">
        <h3>Revenue → Net income · FY{shortYear(latest)}</h3>
        <div className="sub">
          {fmtKpi(revenue)} yields {fmtKpi(ni)}
        </div>
      </div>
      {ITEMS.map(([concept, color]) => {
        const val = frameVal(frame, concept, latest);
        if (val === null) return null;
        const pct = (val / revenue) * 100;
        const inside = pct > 40;
        return (
          <div className="bar-row" key={concept}>
            <div className="bar-label">{concept}</div>
            <div className="bar-track">
              <div className="bar-fill" style={{ width: `${pct.toFixed(1)}%`, background: color }} />
              <div
                className="bar-value"
                style={inside ? { right: 10, color: "#FFFFFF" } : { left: `calc(${pct.toFixed(1)}% + 10px)`, color: "var(--ink-2)" }}
              >
                {fmtKpi(val)} · {pct.toFixed(1)}%
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

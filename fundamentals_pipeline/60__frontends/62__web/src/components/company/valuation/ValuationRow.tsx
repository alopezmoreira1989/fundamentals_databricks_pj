// Valuation card row — port of _render_valuation_row: dot-leader + value, plus a min–avg–max
// range bar for multiples (with a vs-history signal tint), and the benchmark sub-row.

import { fmtMetric, isMissing } from "@/lib/format";
import { signalAbsolute, signalVsHistory, thresholdText } from "@/lib/screener/signals";
import { valBarGeom, VALUATION_MULTIPLES } from "@/lib/valuation";
import type { MetricSeries } from "@/lib/queries/company";

export default function ValuationRow({
  row,
  companyBench,
  industryBench,
}: {
  row: MetricSeries;
  companyBench: Record<string, number>;
  industryBench: Record<string, number>;
}) {
  const { metric, unit, values, latest } = row;
  const base = metric.split(" (")[0].trim();
  const isMultiple = VALUATION_MULTIPLES.has(base);
  const formatted = fmtMetric(latest, unit);
  const tooltip = thresholdText(metric);

  const sig = isMultiple ? signalVsHistory(latest, values) : unit === "percent" ? signalAbsolute(metric, latest) : null;
  const bar = isMultiple ? valBarGeom(values) : null;

  const c = companyBench[metric];
  const i = industryBench[metric];
  const showBench = (unit === "percent" || unit === "ratio") && (!isMissing(c) || !isMissing(i));

  return (
    <>
      <div className={`val-row${sig ? ` row-${sig}` : ""}`} title={tooltip || undefined}>
        <div className="m-label">{metric}</div>
        {bar ? (
          <div className="vbar">
            <div className="vbar-avg" style={{ left: `${bar.avgLeft.toFixed(0)}%` }} />
            <div className={`vbar-mk${sig ? ` row-${sig}` : ""}`} style={{ left: `${bar.nowLeft.toFixed(0)}%` }} />
          </div>
        ) : (
          <div className="lead" />
        )}
        <div className="m-value">{formatted}</div>
        {bar && bar.avg ? (
          <span className="vchip">
            avg {bar.avg.toFixed(1)}x · {bar.devPct >= 0 ? "+" : ""}
            {Math.round(bar.devPct * 100)}%
          </span>
        ) : null}
      </div>
      {showBench ? (
        <div className="metric-bench">
          <span />
          <div className="bench-values">
            {!isMissing(c) && (
              <span className="bench-item">
                10y avg <strong>{fmtMetric(c, unit)}</strong>
              </span>
            )}
            {!isMissing(i) && (
              <span className="bench-item">
                Ind <strong>{fmtMetric(i, unit)}</strong>
              </span>
            )}
          </div>
        </div>
      ) : null}
    </>
  );
}

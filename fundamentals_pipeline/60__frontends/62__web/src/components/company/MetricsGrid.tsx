// Derived-metrics grid — port of render_metrics_grid. One card per metrics_hierarchy category:
//  • generic categories → label · value · sparkline rows, signal tint, benchmark sub-row;
//  • Valuation → ValuationRow (min–avg–max range bar + vs-history tint);
//  • Intrinsic Value → Bear/Mid/Bull scenario rows for the FY or TTM flavour (the football
//    field renders below the grid, in the Derived-metrics tab).

import type { CatGroup, CompanyView, MetricSeries } from "@/lib/queries/company";
import { EM_DASH, fmtMetric, fmtMos, isMissing } from "@/lib/format";
import { signalAbsolute, type Signal, thresholdText } from "@/lib/screener/signals";
import { metricSparklineColor, miniBarsSvg, miniSparklineSvg, SPARK_COLORS, USE_BARS } from "@/lib/sparkline";
import { buildScenario, cleanIv, ivMetricAction } from "@/lib/valuation";
import ValuationRow from "./valuation/ValuationRow";
import IvScenarioRow from "./valuation/IvScenarioRow";

const SIGNAL_SPARK: Record<Signal, string> = {
  good: SPARK_COLORS.green,
  warn: SPARK_COLORS.amber,
  bad: SPARK_COLORS.coral,
};

function gridValue(metric: string, unit: string | null, latest: number | null): string {
  if (metric.startsWith("MoS %")) return fmtMos(latest);
  if (metric === "Piotroski F-Score") return isMissing(latest) ? EM_DASH : `${Math.round(latest)}/9`;
  if (metric === "Altman Z-Score") return isMissing(latest) ? EM_DASH : latest.toFixed(2);
  if (metric === "Accruals Ratio") return isMissing(latest) ? EM_DASH : latest.toFixed(3);
  return fmtMetric(latest, unit, metric.includes("YoY"));
}

function BenchSubrow({ value, unit, label }: { value: number | undefined; unit: string | null; label: string }) {
  if ((unit !== "percent" && unit !== "ratio") || isMissing(value)) return null;
  return (
    <span className="bench-item">
      {label} <strong>{fmtMetric(value, unit)}</strong>
    </span>
  );
}

function MetricRow({
  category,
  row,
  companyBench,
  industryBench,
  label,
  bench = true,
}: {
  category: string;
  row: MetricSeries;
  companyBench: Record<string, number>;
  industryBench: Record<string, number>;
  label?: string;
  bench?: boolean;
}) {
  const { metric, unit, values, latest } = row;
  const sig = signalAbsolute(metric, latest);
  const tooltip = thresholdText(metric);
  const useBars = USE_BARS.has(category);
  const color = useBars && sig ? SIGNAL_SPARK[sig] : metricSparklineColor(category, metric, latest);
  const svg = useBars ? miniBarsSvg(values, color) : miniSparklineSvg(values, color);

  const c = bench ? companyBench[metric] : undefined;
  const i = bench ? industryBench[metric] : undefined;
  const showBench = bench && (unit === "percent" || unit === "ratio") && (!isMissing(c) || !isMissing(i));

  return (
    <>
      <div className={`metric-row${sig ? ` row-${sig}` : ""}`} title={tooltip || undefined}>
        <div className="m-label">{label ?? metric}</div>
        <div className="m-value">{gridValue(metric, unit, latest)}</div>
        <div className="m-spark" dangerouslySetInnerHTML={{ __html: svg }} />
      </div>
      {showBench ? (
        <div className="metric-bench">
          <span />
          <div className="bench-values">
            <BenchSubrow value={c} unit={unit} label="10y avg" />
            <BenchSubrow value={i} unit={unit} label="Ind" />
          </div>
        </div>
      ) : null}
    </>
  );
}

function CardShell({ category, tag, children }: { category: string; tag: string; children: React.ReactNode }) {
  return (
    <div className="metric-card">
      <div className="cat-header">
        <h4>{category}</h4>
        <div className="tag">{tag}</div>
      </div>
      {children}
    </div>
  );
}

function GenericCard({ cat, view }: { cat: CatGroup; view: CompanyView }) {
  return (
    <CardShell category={cat.category} tag={cat.subgroups[0]?.subcategory ?? ""}>
      {cat.subgroups.map((sub, idx) => (
        <div key={sub.subcategory || idx}>
          {idx > 0 && sub.subcategory ? (
            <div className="metric-row">
              <div className="m-subheader">{sub.subcategory}</div>
            </div>
          ) : null}
          {sub.rows.map((row) =>
            cat.category === "Valuation" ? (
              <ValuationRow key={row.metric} row={row} companyBench={view.companyBench} industryBench={view.industryBench} />
            ) : (
              <MetricRow
                key={row.metric}
                category={cat.category}
                row={row}
                companyBench={view.companyBench}
                industryBench={view.industryBench}
              />
            ),
          )}
        </div>
      ))}
    </CardShell>
  );
}

function IvCard({ cat, view, period, ivPrice }: { cat: CatGroup; view: CompanyView; period: string; ivPrice: number | null }) {
  const suffix = `(${period})`;
  const subs = cat.subgroups.filter((sg) => sg.subcategory.endsWith(suffix));
  if (subs.length === 0) return null;

  return (
    <CardShell category={cat.category} tag={cleanIv(subs[0].subcategory)}>
      <div className="scn-colheads">
        <span />
        <span>Bear</span>
        <span>Mid</span>
        <span>Bull</span>
      </div>
      {subs.map((sg, idx) => (
        <div key={sg.subcategory}>
          {idx > 0 ? (
            <div className="metric-row">
              <div className="m-subheader">{cleanIv(sg.subcategory)}</div>
            </div>
          ) : null}
          {sg.rows.map((row) => {
            const action = ivMetricAction(row.metric);
            if (action === "skip") return null;
            if (action === "scenario") {
              return <IvScenarioRow key={row.metric} s={buildScenario(sg.rows, row.metric, period)} price={ivPrice} />;
            }
            // "normal" — generic single-value IV row (e.g. absolute Owner Earnings $), no benchmark.
            return (
              <MetricRow
                key={row.metric}
                category={cat.category}
                row={row}
                companyBench={view.companyBench}
                industryBench={view.industryBench}
                label={cleanIv(row.metric)}
                bench={false}
              />
            );
          })}
        </div>
      ))}
    </CardShell>
  );
}

export default function MetricsGrid({
  view,
  period = "FY",
  ivPrice = null,
}: {
  view: CompanyView;
  period?: string;
  ivPrice?: number | null;
}) {
  if (view.categories.length === 0) {
    return <p className="font-sans text-sm text-ink-3">No derived metrics available.</p>;
  }
  return (
    <div className="metrics-grid">
      {view.categories.map((cat) =>
        cat.category === "Intrinsic Value" ? (
          <IvCard key={cat.category} cat={cat} view={view} period={period} ivPrice={ivPrice} />
        ) : (
          <GenericCard key={cat.category} cat={cat} view={view} />
        ),
      )}
    </div>
  );
}

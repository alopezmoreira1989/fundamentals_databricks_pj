// KPI strip — port of render_kpi_strip: Market Cap · Revenue · Net Income · Net Margin,
// each the latest FY value with its YoY delta.

import type { CompanyView } from "@/lib/queries/company";
import { fmtDelta, fmtKpi, fmtMetric, latestOf, yoy } from "@/lib/format";

function Card({ label, value, series }: { label: string; value: string; series: (number | null)[] }) {
  const [deltaLabel, deltaClass] = fmtDelta(yoy(series));
  return (
    <div className="kpi">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      <div className={`delta ${deltaClass}`}>{deltaLabel}</div>
    </div>
  );
}

export default function KpiStrip({ kpis }: { kpis: CompanyView["kpis"] }) {
  return (
    <div className="kpi-strip">
      <Card label="MARKET CAP" value={fmtKpi(latestOf(kpis.marketCap))} series={kpis.marketCap} />
      <Card label="REVENUE" value={fmtKpi(latestOf(kpis.revenue))} series={kpis.revenue} />
      <Card label="NET INCOME" value={fmtKpi(latestOf(kpis.netIncome))} series={kpis.netIncome} />
      <Card label="NET MARGIN" value={fmtMetric(latestOf(kpis.netMargin), "percent")} series={kpis.netMargin} />
    </div>
  );
}

// Shared chart-card chrome — the .qchart header + legend used by every statement chart
// (port of the wrapper markup in lib/charts.py). Token-driven; charts plug into `children`.

import type { ReactNode } from "react";

export function QChart({
  title,
  legend,
  children,
}: {
  title: string;
  legend?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="qchart">
      <div className="qchart-header">
        <h3>{title}</h3>
        {legend ? <div className="qchart-legend">{legend}</div> : null}
      </div>
      {children}
    </div>
  );
}

export function LegendDot({ color, label, border }: { color: string; label: string; border?: string }) {
  return (
    <span>
      <span className="legend-dot" style={{ background: color, border: border ? `1px solid ${border}` : undefined }} />
      {label}
    </span>
  );
}

/** Raw USD → "$391.0B" style label (chart axis / tooltip). */
export function fmtB(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return `$${(v / 1e9).toFixed(1)}B`;
}

export function shortYear(year: number): string {
  return `'${String(year).slice(-2)}`;
}

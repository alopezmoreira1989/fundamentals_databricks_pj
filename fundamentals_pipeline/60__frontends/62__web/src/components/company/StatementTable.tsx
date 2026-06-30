"use client";

// Statement table (IS / BS / CF) — port of render_table_html (non-quarterly path). One row per
// concept × fiscal year, with section dividers, group subheaders, indent, accounting-style
// numbers, a unit-scale selector, and the trend-sparkline / CAGR / robust-trend columns.

import { type ReactNode, useState } from "react";
import type { StatementTable as StatementTableData } from "@/lib/queries/statementTables";
import { fmtCagr, fmtEps, fmtNum, fmtNumScaled, fmtTrendCagr, isMissing, shortYearLabel } from "@/lib/format";
import { sparklineSvg } from "@/lib/sparkline";
import {
  CREAM,
  PER_SHARE_CONCEPTS,
  rowColor,
  sectionClass,
  SHARE_COUNT_CONCEPTS,
} from "@/lib/statements/classify";

const SCALES: Record<string, [number, string]> = {
  Units: [1, ""],
  Thousands: [1_000, "thousands"],
  Millions: [1_000_000, "millions"],
  Billions: [1_000_000_000, "billions"],
};

export default function StatementTable({ table }: { table: StatementTableData }) {
  const [scale, setScale] = useState<string>("Units");
  const [divisor, scaleWord] = SCALES[scale];

  if (table.rows.length === 0) {
    return <p className="font-sans text-sm text-ink-3">No data available.</p>;
  }

  const years = table.years;
  const nCols = years.length + 4; // label + years + trend + cagr + trend-cagr
  const usdLabel = divisor === 1 ? "USD" : `USD ${scaleWord}`;

  const body: ReactNode[] = [];
  let prevSection: string | null = null;
  let prevGroup: string | null = null;

  table.rows.forEach((row, ri) => {
    const { section, group, concept, displayName, indent, rowClass: cls, values } = row;

    if (section && section !== prevSection) {
      const sCls = sectionClass(table.stmt, section);
      body.push(
        <tr key={`sec-${ri}`} className={`section-row ${sCls}`}>
          <td colSpan={nCols}>{section}</td>
        </tr>,
      );
      prevSection = section;
      prevGroup = null;
    }
    if (group && group !== section && group !== prevGroup) {
      body.push(
        <tr key={`grp-${ri}`} className="group-row">
          <td colSpan={nCols}>{group}</td>
        </tr>,
      );
      prevGroup = group;
    }

    const isPerShare = PER_SHARE_CONCEPTS.has(concept);
    const isCount = SHARE_COUNT_CONCEPTS.has(concept);

    const numCells = values.map((v, i) => {
      const latest = i === values.length - 1 ? " latest" : "";
      if (isMissing(v)) return <td key={i} className={`num${latest}`}>—</td>;
      const text = isPerShare ? fmtEps(v) : isCount ? fmtNum(v) : fmtNumScaled(v, divisor);
      const muted = v < 0 ? " muted" : "";
      return <td key={i} className={`num${latest}${muted}`}>{text}</td>;
    });

    // Trend sparkline.
    const color = cls === "grand-total" ? CREAM : rowColor(concept);
    const endCircle = cls === "headline" || cls === "grand-total" || cls === "subtotal";
    const stroke = cls === "headline" || cls === "grand-total" ? 1.8 : cls === "subtotal" ? 1.6 : 1.5;
    const svg = sparklineSvg(values, { color, stroke, endCircle });

    // CAGR (first → last non-missing).
    const first = values.find((v) => !isMissing(v)) ?? null;
    const last = [...values].reverse().find((v) => !isMissing(v)) ?? null;
    const nYears = values.filter((v) => !isMissing(v)).length - 1;
    const [cagrLabel, cagrCls] = fmtCagr(first, last, nYears);
    const cagrExtra = cls === "headline" ? (cagrCls === "up" ? "up" : "") : cagrCls;

    const [tLabel, tCls, tBadge] = fmtTrendCagr(values);

    body.push(
      <tr key={`row-${ri}`} className={cls || undefined}>
        <td className={`label${indent ? ` indent-${indent}` : ""}`}>{displayName}</td>
        {numCells}
        <td className="trend" dangerouslySetInnerHTML={{ __html: svg }} />
        <td className={`cagr${cagrExtra ? ` ${cagrExtra}` : ""}`}>{cagrLabel}</td>
        <td className={`trend-cagr${tCls ? ` ${tCls}` : ""}`}>
          {tLabel}
          {tBadge ? <span className="r2-badge">{tBadge}</span> : null}
        </td>
      </tr>,
    );
  });

  return (
    <div>
      <div className="mb-3 flex items-center gap-1">
        {Object.keys(SCALES).map((s) => (
          <button
            key={s}
            type="button"
            onClick={() => setScale(s)}
            className={
              "rounded border px-2 py-0.5 font-mono text-[11px] transition-colors " +
              (s === scale
                ? "border-accent bg-accent-soft text-accent-ink"
                : "border-rule bg-bg-card text-ink-3 hover:border-accent")
            }
          >
            {s}
          </button>
        ))}
      </div>
      <div className="fs-table-wrap">
        <table className="fs">
          <thead>
            <tr>
              <th className="col-label">{usdLabel}</th>
              {years.map((yr, i) => (
                <th key={yr} className={`col-num${i === years.length - 1 ? " col-latest" : ""}`}>
                  {shortYearLabel(yr)}
                </th>
              ))}
              <th className="col-trend">10y trend</th>
              <th className="col-cagr">CAGR</th>
              <th className="col-trend-cagr">Trend</th>
            </tr>
          </thead>
          <tbody>{body}</tbody>
        </table>
      </div>
    </div>
  );
}

"use client";

// Quarterly mini-table — concept × last-12-quarters. The quarterly analogue of StatementTable
// (render_table_html statement="qt"): same section/group/indent/row-class decoration and
// accounting numbers, but no trend/CAGR columns (too few points per quarter series).

import { type ReactNode, useState } from "react";
import type { QuarterlyData } from "@/lib/queries/quarterly";
import { fmtEps, fmtNum, fmtNumScaled, isMissing } from "@/lib/format";
import { PER_SHARE_CONCEPTS, sectionClass, SHARE_COUNT_CONCEPTS } from "@/lib/statements/classify";

const SCALES: Record<string, [number, string]> = {
  Units: [1, ""],
  Thousands: [1_000, "thousands"],
  Millions: [1_000_000, "millions"],
  Billions: [1_000_000_000, "billions"],
};

export default function QuarterlyTable({ data }: { data: QuarterlyData }) {
  const [scale, setScale] = useState("Units");
  const [divisor, scaleWord] = SCALES[scale];

  if (data.rows.length === 0) return <p className="font-sans text-sm text-ink-3">No quarterly data.</p>;

  const { quarters } = data;
  const nCols = quarters.length + 1;
  const usdLabel = divisor === 1 ? "USD" : `USD ${scaleWord}`;

  const body: ReactNode[] = [];
  let prevSection: string | null = null;
  let prevGroup: string | null = null;

  data.rows.forEach((row, ri) => {
    const { section, group, concept, displayName, indent, rowClass: cls, values } = row;
    if (section && section !== prevSection) {
      body.push(
        <tr key={`sec-${ri}`} className={`section-row ${sectionClass("Income Statement", section)}`}>
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
    const cells = values.map((v, i) => {
      const latest = i === values.length - 1 ? " latest" : "";
      if (isMissing(v)) return <td key={i} className={`num${latest}`}>—</td>;
      const text = isPerShare ? fmtEps(v) : isCount ? fmtNum(v) : fmtNumScaled(v, divisor);
      return <td key={i} className={`num${latest}${v < 0 ? " muted" : ""}`}>{text}</td>;
    });
    body.push(
      <tr key={`row-${ri}`} className={cls || undefined}>
        <td className={`label${indent ? ` indent-${indent}` : ""}`}>{displayName}</td>
        {cells}
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
              {quarters.map((q, i) => (
                <th key={q} className={`col-num${i === quarters.length - 1 ? " col-latest" : ""}`}>
                  {q}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>{body}</tbody>
        </table>
      </div>
    </div>
  );
}

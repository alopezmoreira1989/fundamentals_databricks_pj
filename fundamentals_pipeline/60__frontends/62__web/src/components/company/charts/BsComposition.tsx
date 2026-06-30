"use client";

// Balance-sheet liquidity-ramp composition — port of render_bs_composition's view side. Two
// equal-height stacked bars (Assets | Liabilities + Equity, both tied to Total Assets) on the
// left, a hierarchical legend on the right. Segment heights are flex-grow = $-value (CSS
// container queries hide labels on short slices); the legend carries every value. The pure
// data logic (subgroups, Other plug, colour ramp) lives in lib/charts/bsComposition.ts.

import { useState } from "react";
import type { StatementFrame } from "@/lib/queries/statements";
import { buildBsComposition, type Segment, type SubGroup } from "@/lib/charts/bsComposition";
import { shortYear } from "./QChart";

const b = (v: number) => (v / 1e9).toFixed(1);

function Bar({ title, total, segments }: { title: string; total: number; segments: Segment[] }) {
  return (
    <div className="bs-bar">
      <div className="bs-bar-head">
        <span className="bs-bar-name">{title}</span>
        <span className="bs-bar-total">${b(total)}B</span>
      </div>
      <div className="bs-stack">
        {segments
          .filter((s) => s.value > 0)
          .map((s, i) => (
            <div
              key={`${s.name}-${i}`}
              className={s.boundary ? "bs-seg bs-boundary" : "bs-seg"}
              style={{ flexGrow: s.value / 1e9, background: s.color, color: s.ink }}
            >
              <span className="bs-seg-name">{s.name}</span>
              <span className="bs-seg-val">
                ${b(s.value)}B · {((s.value / total) * 100).toFixed(0)}%
              </span>
            </div>
          ))}
      </div>
    </div>
  );
}

function LegendFamily({
  name,
  famTotal,
  subgroups,
  dot,
}: {
  name: string;
  famTotal: number;
  subgroups: SubGroup[];
  dot?: string;
}) {
  return (
    <div className="bs-leg-fam">
      <div className="bs-leg-fam-head">
        <span className="bs-leg-fam-name">
          {dot ? <span className="bs-leg-dot" style={{ background: dot }} /> : null}
          {name}
        </span>
        <span className="bs-leg-total">${b(famTotal)}B</span>
      </div>
      {subgroups.map((sg, i) => (
        <div key={sg.name || i}>
          {sg.name ? (
            <div className="bs-leg-sub">
              <span>{sg.name}</span>
              <span>${b(sg.subtotal)}B</span>
            </div>
          ) : null}
          {sg.segs
            .filter((s) => s.value > 0)
            .map((s, j) => (
              <div className="bs-leg-item" key={`${s.name}-${j}`}>
                <span className="bs-leg-dot" style={{ background: s.color }} />
                <span className="bs-leg-name">{s.name}</span>
                <span className="bs-leg-val">${b(s.value)}B</span>
              </div>
            ))}
        </div>
      ))}
    </div>
  );
}

export default function BsComposition({ frame }: { frame: StatementFrame }) {
  const years = frame.years;
  const [year, setYear] = useState(years.length ? years[years.length - 1] : 0);
  if (!years.length) return null;

  const comp = buildBsComposition(frame, year);

  return (
    <div className="qchart">
      <div className="qchart-header">
        <h3>Balance sheet composition · FY{shortYear(year)}</h3>
        <select
          className="rounded-md border border-rule bg-bg-card px-2 py-1 font-mono text-xs text-ink"
          value={year}
          onChange={(e) => setYear(Number(e.target.value))}
        >
          {[...years].reverse().map((y) => (
            <option key={y} value={y}>
              FY {y}
            </option>
          ))}
        </select>
      </div>

      {comp === null ? (
        <p className="font-sans text-sm text-ink-3">Total Assets not available for FY {year}.</p>
      ) : (
        <div className="bs-composition">
          <div className="bs-bars">
            <Bar title="Assets" total={comp.total} segments={comp.assetsFlat} />
            <Bar title="Liabilities + Equity" total={comp.total} segments={[...comp.liabFlat, comp.equitySeg]} />
          </div>
          <div className="bs-legend">
            <div className="bs-leg-key">
              <span>Liquid</span>
              <span
                className="bs-leg-grad"
                style={{ background: `linear-gradient(90deg, ${comp.rampAsset[0]}, ${comp.rampAsset[1]})` }}
              />
              <span>Immobilized</span>
            </div>
            <LegendFamily name="Assets" famTotal={comp.total} subgroups={comp.assetGroups} />
            <LegendFamily name="Liabilities" famTotal={comp.liabTotal} subgroups={comp.liabGroups} />
            <LegendFamily name="Equity" famTotal={comp.equity} subgroups={[]} dot={comp.equitySeg.color} />
          </div>
        </div>
      )}
    </div>
  );
}

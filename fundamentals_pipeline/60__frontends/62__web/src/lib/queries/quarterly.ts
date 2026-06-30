// Quarterly Income-Statement data for the company Quarterly tab — port of tables.quarterly_df
// (last 12 quarters, concept × quarter) + the revenue/YoY series for render_quarterly_combo.

import { ARTIFACT_FILES } from "../data";
import { type RowClass, rowClass } from "../statements/classify";
import { queryRows } from "./runQuery";

const DATA = ARTIFACT_FILES.data;
const Q_NUM: Record<string, number> = { Q1: 1, Q2: 2, Q3: 3, Q4: 4 };

export interface QuarterlyRow {
  section: string | null;
  group: string | null;
  concept: string;
  displayName: string;
  indent: number;
  rowClass: RowClass;
  values: (number | null)[]; // aligned to `quarters`
}

export interface QuarterlyData {
  quarters: string[]; // last 12 labels "'YY-Qn", chronological
  rows: QuarterlyRow[];
  revenue: (number | null)[];
  yoy: (number | null)[]; // same-quarter prior-year growth %
}

const sqlStr = (s: string) => `'${s.replace(/'/g, "''")}'`;

interface RawQ {
  section: string | null;
  grp: string | null;
  concept: string;
  display_name: string | null;
  sort_order: number | null;
  fiscal_year: number;
  period_type: string;
  value: number | null;
}

export async function loadQuarterly(ticker: string): Promise<QuarterlyData> {
  const rows = await queryRows<RawQ>(
    `SELECT section, "group" AS grp, concept, display_name, sort_order, fiscal_year, period_type, value
     FROM ${sqlStr(DATA)} WHERE ticker = ${sqlStr(ticker)} AND stmt = 'Income Statement'
       AND period_type IN ('Q1','Q2','Q3','Q4')`,
    ["data"],
  );
  if (rows.length === 0) return { quarters: [], rows: [], revenue: [], yoy: [] };

  // Last 12 distinct quarters by chronological sort key (fy*10 + quarter#).
  const labelOf = (fy: number, pt: string) => `'${String(fy).slice(-2)}-${pt}`;
  const sortOf = (fy: number, pt: string) => fy * 10 + (Q_NUM[pt] ?? 0);
  const seen = new Map<string, number>();
  for (const r of rows) seen.set(labelOf(r.fiscal_year, r.period_type), sortOf(r.fiscal_year, r.period_type));
  const quarters = Array.from(seen.entries())
    .sort((a, b) => a[1] - b[1])
    .slice(-12)
    .map(([label]) => label);
  const keep = new Set(quarters);

  // Pivot concept → quarter → value, keeping decoration metadata.
  interface Acc {
    section: string | null;
    grp: string | null;
    concept: string;
    displayName: string;
    sortOrder: number | null;
    byQ: Map<string, number | null>;
    order: number;
  }
  const accs = new Map<string, Acc>();
  let seq = 0;
  // (fy, quarter#) → revenue, for YoY.
  const revByYQ = new Map<string, number>();

  for (const r of rows) {
    const label = labelOf(r.fiscal_year, r.period_type);
    if (!keep.has(label)) continue;
    const displayName = (r.display_name ?? "").trim() || r.concept;
    const k = `${r.section} ${r.grp} ${r.concept} ${displayName} ${r.sort_order}`;
    let acc = accs.get(k);
    if (!acc) {
      acc = { section: r.section ?? null, grp: r.grp ?? null, concept: r.concept, displayName, sortOrder: r.sort_order, byQ: new Map(), order: seq++ };
      accs.set(k, acc);
    }
    acc.byQ.set(label, r.value);
    if (r.concept === "Revenue" && r.value !== null) {
      revByYQ.set(`${r.fiscal_year}-${Q_NUM[r.period_type]}`, r.value);
    }
  }

  const ordered = Array.from(accs.values()).sort((a, b) => {
    const sa = a.sortOrder ?? Number.POSITIVE_INFINITY;
    const sb = b.sortOrder ?? Number.POSITIVE_INFINITY;
    return sa !== sb ? sa - sb : a.order - b.order;
  });

  const tableRows: QuarterlyRow[] = ordered.map((a) => {
    const group = a.grp ?? a.section;
    const cls = rowClass("Income Statement", a.concept, a.displayName);
    const hasInnerGroup = group != null && group !== a.section && group !== "";
    const styled = cls === "subtotal" || cls === "grand-total" || cls === "headline";
    return {
      section: a.section,
      group,
      concept: a.concept,
      displayName: a.displayName,
      indent: hasInnerGroup && !styled ? 1 : 0,
      rowClass: cls,
      values: quarters.map((q) => a.byQ.get(q) ?? null),
    };
  });

  // Revenue + YoY (same quarter prior year), aligned to `quarters`.
  const revRow = tableRows.find((r) => r.concept === "Revenue");
  const revenue = revRow ? revRow.values : quarters.map(() => null);
  const yoy = quarters.map((label) => {
    const m = /^'(\d{2})-(Q\d)$/.exec(label);
    if (!m) return null;
    const fy = 2000 + Number(m[1]);
    const qn = Q_NUM[m[2]];
    const cur = revByYQ.get(`${fy}-${qn}`);
    const prior = revByYQ.get(`${fy - 1}-${qn}`);
    if (cur === undefined || prior === undefined || prior === 0) return null;
    return ((cur - prior) / Math.abs(prior)) * 100;
  });

  return { quarters, rows: tableRows, revenue, yoy };
}

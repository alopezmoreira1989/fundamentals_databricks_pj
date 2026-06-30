// Statement frames for the company charts — the web equivalent of lib/tables.py's
// income_statement_df / balance_sheet_df / cash_flow_df, but reduced to what the charts need:
// concept → (fiscal_year → value) plus the sorted FY list, per statement. The full decorated
// statement TABLES (render_table_html) are a separate, larger port (a follow-up phase).

import { ARTIFACT_FILES } from "../data";
import { queryRows } from "./runQuery";

const DATA = ARTIFACT_FILES.data;

export interface StatementFrame {
  /** sorted ascending fiscal years present in this statement. */
  years: number[];
  /** concept → { fiscal_year → value }. */
  byConcept: Record<string, Record<number, number>>;
}

export interface Statements {
  is: StatementFrame;
  bs: StatementFrame;
  cf: StatementFrame;
}

const STMT_KEY: Record<string, keyof Statements> = {
  "Income Statement": "is",
  "Balance Sheet": "bs",
  "Cash Flow": "cf",
};

const sqlStr = (s: string) => `'${s.replace(/'/g, "''")}'`;

function emptyFrame(): StatementFrame {
  return { years: [], byConcept: {} };
}

/** Value of `concept` in `year`, or null. */
export function frameVal(frame: StatementFrame, concept: string, year: number): number | null {
  const v = frame.byConcept[concept]?.[year];
  return v === undefined ? null : v;
}

/** Oldest→newest series of `concept` across the frame's years (null where absent). */
export function frameSeries(frame: StatementFrame, concept: string): (number | null)[] {
  return frame.years.map((y) => frameVal(frame, concept, y));
}

export async function loadStatements(ticker: string): Promise<Statements> {
  const rows = await queryRows<{ stmt: string; concept: string; fiscal_year: number; value: number }>(
    `SELECT stmt, concept, fiscal_year, value FROM ${sqlStr(DATA)}
     WHERE ticker = ${sqlStr(ticker)} AND period_type = 'FY' AND value IS NOT NULL
     ORDER BY fiscal_year`,
    ["data"],
  );

  const out: Statements = { is: emptyFrame(), bs: emptyFrame(), cf: emptyFrame() };
  const yearSets: Record<keyof Statements, Set<number>> = { is: new Set(), bs: new Set(), cf: new Set() };

  for (const r of rows) {
    const key = STMT_KEY[r.stmt];
    if (!key) continue;
    const frame = out[key];
    const year = Number(r.fiscal_year);
    (frame.byConcept[r.concept] ??= {})[year] = r.value;
    yearSets[key].add(year);
  }
  for (const key of Object.keys(out) as (keyof Statements)[]) {
    out[key].years = Array.from(yearSets[key]).sort((a, b) => a - b);
  }
  return out;
}

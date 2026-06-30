// Price series for the company Price tab — port of lib/prices.py (prices_for: daily rows +
// SMA 20/50/200 on adj_close). Reads the 43 MB dashboard_prices artifact, so this loader is
// called lazily (only when the Price tab is opened) — see CompanyClient.
//
// NOTE: whole-file load of the 43 MB prices artifact to chart one ticker is the wasteful case
// flagged in artifacts.ts. It's cached in IndexedDB after first load; a follow-up can switch to
// DuckDB HTTP range reads or per-ticker slices once validated in-browser.

import { ARTIFACT_FILES } from "../data";
import { queryRows } from "./runQuery";

const PRICES = ARTIFACT_FILES.prices;

export interface PricePoint {
  date: string; // YYYY-MM-DD
  close: number | null;
  adjClose: number;
  sma20: number | null;
  sma50: number | null;
  sma200: number | null;
}

const sqlStr = (s: string) => `'${s.replace(/'/g, "''")}'`;

/** Trailing simple moving average; null until `win` points exist (matches pandas rolling). */
function sma(vals: number[], win: number): (number | null)[] {
  const out: (number | null)[] = [];
  let sum = 0;
  for (let i = 0; i < vals.length; i++) {
    sum += vals[i];
    if (i >= win) sum -= vals[i - win];
    out.push(i >= win - 1 ? sum / win : null);
  }
  return out;
}

export async function loadPrices(ticker: string): Promise<PricePoint[]> {
  const rows = await queryRows<{ date: string; close: number | null; adj_close: number }>(
    `SELECT CAST(date AS VARCHAR) AS date, close, adj_close
     FROM ${sqlStr(PRICES)} WHERE ticker = ${sqlStr(ticker)} AND adj_close IS NOT NULL
     ORDER BY date`,
    ["prices"],
  );
  if (rows.length === 0) return [];
  const adj = rows.map((r) => Number(r.adj_close));
  const s20 = sma(adj, 20);
  const s50 = sma(adj, 50);
  const s200 = sma(adj, 200);
  return rows.map((r, i) => ({
    date: r.date.slice(0, 10),
    close: r.close === null || r.close === undefined ? null : Number(r.close),
    adjClose: adj[i],
    sma20: s20[i],
    sma50: s50[i],
    sma200: s200[i],
  }));
}

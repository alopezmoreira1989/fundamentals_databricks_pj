// Query runner — the single choke point every domain query module (screener, company,
// compare, backtest) goes through. Ensures the required artifacts are registered in
// DuckDB-WASM, opens a short-lived connection, runs the SQL, and always closes the
// connection (a leaked connection slowly degrades the WASM instance).
//
// Two surfaces: `runQuery` returns the raw Arrow `Table` (use when you stream it straight
// into a table component); `queryRows` materialises plain JS objects (use for small result
// sets — KPI strips, summaries — where ergonomics beat zero-copy).

import type { Table } from "apache-arrow";
import { getDuckDb } from "../duckdb";
import { ensureArtifacts } from "../data/artifacts";
import type { ArtifactName } from "../data/types";

/**
 * Run a SQL query against the given artifacts, returning the Arrow result table.
 * Reference each artifact in SQL by its filename (`FROM 'dashboard_metrics.parquet'`); the
 * names listed here are what gets registered first.
 */
export async function runQuery(
  sql: string,
  artifacts: readonly ArtifactName[],
): Promise<Table> {
  await ensureArtifacts(artifacts);
  const db = await getDuckDb();
  const conn = await db.connect();
  try {
    return await conn.query(sql);
  } finally {
    await conn.close();
  }
}

/** Like {@link runQuery}, but materialises the rows as plain typed JS objects. */
export async function queryRows<T = Record<string, unknown>>(
  sql: string,
  artifacts: readonly ArtifactName[],
): Promise<T[]> {
  const table = await runQuery(sql, artifacts);
  return table.toArray().map((row) => row.toJSON() as T);
}

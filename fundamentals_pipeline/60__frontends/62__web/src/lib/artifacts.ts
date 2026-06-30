import type { Table } from "apache-arrow";
import { getDuckDb } from "./duckdb";

// Public GitHub Release (`latest`) maintained by 50__publish/52__publish_to_github.py.
// The repo is public, so the browser fetches these assets directly — no token, no backend,
// exactly the same artifacts the Streamlit app reads.
export const RELEASE_BASE =
  "https://github.com/alopezmoreira1989/fundamentals_databricks_pj/releases/download/latest";

export const ARTIFACTS = {
  data: "dashboard_data.parquet",
  metrics: "dashboard_metrics.parquet",
  prices: "dashboard_prices.parquet",
  backtest: "dashboard_backtest.parquet",
} as const;

const registered = new Set<string>();

/**
 * Fetch a published parquet once and register it as a DuckDB file (idempotent).
 *
 * We download the whole file and `registerFileBuffer` rather than using DuckDB's HTTP/range
 * protocol: a plain cross-origin GET of a public Release asset is reliable, whereas range
 * requests + CORS preflight against GitHub's asset CDN are not guaranteed. Fine for the scale
 * here; a follow-up can switch to range reads once confirmed in-browser.
 */
async function registerArtifact(asset: string): Promise<void> {
  if (registered.has(asset)) return;
  const db = await getDuckDb();
  const res = await fetch(`${RELEASE_BASE}/${asset}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch ${asset}: ${res.status} ${res.statusText}`);
  }
  const buf = new Uint8Array(await res.arrayBuffer());
  await db.registerFileBuffer(asset, buf);
  registered.add(asset);
}

/**
 * End-to-end proof query: the distinct tickers present in the published metrics artifact.
 * The returned Arrow Table's `numRows` is the ticker count — a real number from real data.
 * (Follow-up tasks will return the full filtered/ranked screener rows from here.)
 */
export async function getScreenerData(): Promise<Table> {
  const asset = ARTIFACTS.metrics;
  await registerArtifact(asset);
  const db = await getDuckDb();
  const conn = await db.connect();
  try {
    return await conn.query(`SELECT DISTINCT ticker FROM '${asset}' ORDER BY ticker`);
  } finally {
    await conn.close();
  }
}

// Loader for the `dashboard_meta.json` sidecar — company names, industry/sector, logo
// probe, FY ranges, row counts. The web mirror of the meta slice of `load_latest_data()`.
//
// JSON (not parquet), so it's fetched + cached directly (no DuckDB registration). Same
// IndexedDB TTL cache as the parquet artifacts. We validate only the required top-level
// keys (mirroring META_REQUIRED_KEYS in _core/schemas.py) — a published meta that drifts
// from the contract surfaces as a readable error instead of a downstream `undefined` crash.

import { ArtifactLoadError, META_FILE, type DashboardMeta } from "./types";
import { cachedFetch } from "./cache";
import { RELEASE_BASE } from "./artifacts";

const REQUIRED_KEYS = [
  "schema_version",
  "build_timestamp",
  "tickers",
  "fy_ranges",
  "row_counts",
  "retention",
] as const;

function assertMetaShape(meta: unknown): asserts meta is DashboardMeta {
  if (typeof meta !== "object" || meta === null) {
    throw new ArtifactLoadError(META_FILE, "expected a JSON object");
  }
  const obj = meta as Record<string, unknown>;
  const missing = REQUIRED_KEYS.filter((k) => !(k in obj));
  if (missing.length > 0) {
    throw new ArtifactLoadError(META_FILE, `missing required key(s): ${missing.join(", ")}`);
  }
  if (!Array.isArray(obj.tickers)) {
    throw new ArtifactLoadError(META_FILE, "'tickers' must be an array");
  }
}

async function fetchMeta(): Promise<DashboardMeta> {
  const res = await fetch(`${RELEASE_BASE}/${META_FILE}`);
  if (!res.ok) {
    throw new ArtifactLoadError(META_FILE, `${res.status} ${res.statusText}`);
  }
  const json = (await res.json()) as unknown;
  assertMetaShape(json);
  return json;
}

/** Fetch (cached) and validate the dashboard meta sidecar. */
export function loadMeta(): Promise<DashboardMeta> {
  return cachedFetch(META_FILE, fetchMeta);
}

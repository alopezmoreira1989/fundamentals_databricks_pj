// Shared data-layer types — the contract between the published Release artifacts
// (produced by 50__publish/51__export_dashboard_data.py, asserted against
// fundamentals_pipeline/schemas.py) and this web frontend.
//
// This mirrors the Streamlit `lib/data.py` loaders 1:1; keep the two in sync when the
// artifact contract changes. Required keys here track META_REQUIRED_KEYS / TICKER_REQUIRED_KEYS
// in _core/schemas.py — everything else is additive and optional.

/** The four published parquet artifacts, keyed by logical name. */
export type ArtifactName = "data" | "metrics" | "prices" | "backtest";

/** Logical name → published Release asset filename. */
export const ARTIFACT_FILES: Record<ArtifactName, string> = {
  data: "dashboard_data.parquet",
  metrics: "dashboard_metrics.parquet",
  prices: "dashboard_prices.parquet",
  backtest: "dashboard_backtest.parquet",
} as const;

/** Filename of the meta sidecar (not a parquet artifact). */
export const META_FILE = "dashboard_meta.json";

/**
 * Per-ticker metadata record from `meta.tickers`. Only `ticker` + `company` are
 * guaranteed by the contract (TICKER_REQUIRED_KEYS); the rest are additive fields
 * that have accreted across schema versions (industry, sector, logo probe, etc.) and
 * may be absent. Read them defensively.
 */
export interface TickerMeta {
  ticker: string;
  company: string;
  sector?: string | null;
  industry?: string | null;
  has_logo?: boolean | null;
  employees?: number | null;
  founded?: number | string | null;
  // Forward-compatible: tolerate fields added by later schema versions.
  [key: string]: unknown;
}

/**
 * The `dashboard_meta.json` sidecar. Required top-level keys mirror
 * META_REQUIRED_KEYS in _core/schemas.py. Sub-keys of `retention` / `row_counts`
 * are intentionally not pinned (they evolve across schema versions).
 */
/** Per-ticker fiscal-year coverage from `meta.fy_ranges`. */
export interface FyRange {
  ticker: string;
  fy_min: number;
  fy_max: number;
}

export interface DashboardMeta {
  schema_version: number | string;
  build_timestamp: string;
  tickers: TickerMeta[];
  fy_ranges: FyRange[];
  row_counts: Record<string, unknown>;
  retention: Record<string, unknown>;
  [key: string]: unknown;
}

/**
 * Raised when a Release artifact (or the meta sidecar) cannot be fetched, cached, or
 * registered. The web equivalent of Streamlit's `_render_load_error` path: callers
 * surface `.message` in a banner/toast rather than letting a raw fetch error bubble up.
 */
export class ArtifactLoadError extends Error {
  readonly artifact: string;

  constructor(artifact: string, message: string, options?: { cause?: unknown }) {
    super(`Failed to load ${artifact}: ${message}`, options);
    this.name = "ArtifactLoadError";
    this.artifact = artifact;
  }
}

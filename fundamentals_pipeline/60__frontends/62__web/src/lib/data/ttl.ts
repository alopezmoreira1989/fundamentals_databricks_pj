// Cache TTL for the Release-artifact loaders — the browser-side mirror of
// `_resolve_data_ttl()` in 61__streamlit/lib/data.py.
//
// Default 600s (10 min) so the app picks up a freshly published Release without a manual
// reboot. Override per environment via the `NEXT_PUBLIC_DASHBOARD_DATA_TTL` build-time env
// var (must be `NEXT_PUBLIC_` to be readable in the browser bundle) — e.g. a shorter value
// on a staging deploy for fast QA, a longer one on prod. Floored at 30s like Streamlit, so a
// misconfigured "0" can't disable caching and hammer the Release CDN on every filter change.

const DEFAULT_TTL_SECONDS = 600;
const MIN_TTL_SECONDS = 30;

/** Resolved cache TTL in **seconds** (mirrors the Streamlit floor/default semantics). */
export function resolveDataTtlSeconds(): number {
  const raw = process.env.NEXT_PUBLIC_DASHBOARD_DATA_TTL;
  if (raw === undefined || raw === "") return DEFAULT_TTL_SECONDS;
  const parsed = Number.parseInt(raw, 10);
  if (Number.isNaN(parsed)) return DEFAULT_TTL_SECONDS;
  return Math.max(MIN_TTL_SECONDS, parsed);
}

/** Resolved cache TTL in **milliseconds** (convenience for `Date.now()` comparisons). */
export function resolveDataTtlMs(): number {
  return resolveDataTtlSeconds() * 1000;
}

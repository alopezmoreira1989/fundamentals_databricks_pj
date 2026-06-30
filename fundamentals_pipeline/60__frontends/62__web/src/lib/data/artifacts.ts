// Lazy, cached registration of the published Release artifacts into DuckDB-WASM.
//
// Mirrors the Streamlit loaders in `lib/data.py`, but split per artifact and lazy: a route
// only pays for what it queries (the screener registers `data` + `metrics`; the price chart
// registers `prices` only when it mounts). Two cache layers:
//   1. `registered` Set — per page-session idempotency (don't re-register an already-loaded
//      file into the same DuckDB instance).
//   2. IndexedDB via `cachedFetch` — cross-visit byte cache with the Streamlit TTL, so a
//      repeat visit within the window skips the GitHub Release download entirely.
//
// We download the whole file and `registerFileBuffer` rather than DuckDB's HTTP/range
// protocol: a plain cross-origin GET of a public Release asset is reliable, whereas range
// requests + CORS preflight against GitHub's asset CDN are not guaranteed. This is fine for
// `data` (10MB) / `metrics` (13MB) / `backtest` (<1MB). `prices` (43MB) is the one artifact
// where whole-file load is wasteful for a single-ticker query — revisit with range reads or
// per-ticker slices when the price chart lands (Phase 4/5), validated in-browser.

import { getDuckDb } from "../duckdb";
import { ArtifactLoadError, ARTIFACT_FILES, type ArtifactName } from "./types";
import { cachedFetch } from "./cache";

// Public GitHub Release (`latest`) maintained by 50__publish/52__publish_to_github.py.
// The repo is public, so the browser fetches these assets directly — no token, no backend,
// exactly the same artifacts the Streamlit app reads.
export const RELEASE_BASE =
  "https://github.com/alopezmoreira1989/fundamentals_databricks_pj/releases/download/latest";

const registered = new Set<ArtifactName>();
// De-dupe concurrent registrations of the same artifact (two components mounting at once
// must not both fetch + register the same file).
const inFlight = new Map<ArtifactName, Promise<void>>();

async function fetchArtifactBuffer(asset: string): Promise<Uint8Array> {
  const res = await fetch(`${RELEASE_BASE}/${asset}`);
  if (!res.ok) {
    throw new ArtifactLoadError(asset, `${res.status} ${res.statusText}`);
  }
  return new Uint8Array(await res.arrayBuffer());
}

/**
 * Ensure the named artifact is fetched (cached) and registered as a DuckDB file, then
 * return the in-DuckDB filename to use in SQL (`SELECT ... FROM '<file>'`). Idempotent and
 * concurrency-safe. Throws {@link ArtifactLoadError} on fetch failure.
 */
export async function ensureArtifact(name: ArtifactName): Promise<string> {
  const asset = ARTIFACT_FILES[name];
  if (registered.has(name)) return asset;

  const existing = inFlight.get(name);
  if (existing) {
    await existing;
    return asset;
  }

  const task = (async () => {
    try {
      const buf = await cachedFetch(asset, () => fetchArtifactBuffer(asset));
      const db = await getDuckDb();
      await db.registerFileBuffer(asset, buf);
      registered.add(name);
    } catch (err) {
      if (err instanceof ArtifactLoadError) throw err;
      throw new ArtifactLoadError(asset, err instanceof Error ? err.message : String(err), {
        cause: err,
      });
    } finally {
      inFlight.delete(name);
    }
  })();

  inFlight.set(name, task);
  await task;
  return asset;
}

/** Register several artifacts concurrently; returns their in-DuckDB filenames in order. */
export function ensureArtifacts(names: readonly ArtifactName[]): Promise<string[]> {
  return Promise.all(names.map(ensureArtifact));
}

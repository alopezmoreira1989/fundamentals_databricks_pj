// IndexedDB-backed TTL cache for the published Release artifacts.
//
// Streamlit caches the fetched parquet frames server-side via `@st.cache_data(ttl=...)`.
// In the browser there is no server cache, so without this every visit (and every cold
// route) re-downloads ~MBs of parquet from the GitHub Release CDN. We persist the raw
// bytes (and the meta JSON) in IndexedDB keyed by artifact, with the same TTL semantics as
// `_resolve_data_ttl()`, so a repeat visit within the window is instant and offline-tolerant.
//
// Defensive by design: IndexedDB can be unavailable (SSR, private-mode quirks, disabled
// storage). Every cache touch is wrapped so a storage failure degrades to a plain network
// fetch — it must never be the reason data fails to load.

import { get, set } from "idb-keyval";
import { resolveDataTtlMs } from "./ttl";

interface CacheEntry<T> {
  payload: T;
  /** Epoch ms when this entry was written (for TTL expiry). */
  ts: number;
}

const KEY_PREFIX = "artifact:";

function cacheKey(name: string): string {
  return `${KEY_PREFIX}${name}`;
}

async function readEntry<T>(name: string): Promise<CacheEntry<T> | undefined> {
  try {
    return await get<CacheEntry<T>>(cacheKey(name));
  } catch {
    return undefined; // storage unavailable → treat as a miss
  }
}

async function writeEntry<T>(name: string, payload: T): Promise<void> {
  try {
    await set(cacheKey(name), { payload, ts: Date.now() } satisfies CacheEntry<T>);
  } catch {
    // Quota exceeded / storage disabled — non-fatal, we just won't cache this entry.
  }
}

/**
 * Return a cached payload for `name` if present and younger than the resolved TTL;
 * otherwise call `fetcher`, persist the result, and return it.
 *
 * `ttlMs` defaults to {@link resolveDataTtlMs}; pass an explicit value only to override
 * the global TTL for a specific artifact (e.g. a shorter window for live-ish data).
 */
export async function cachedFetch<T>(
  name: string,
  fetcher: () => Promise<T>,
  ttlMs: number = resolveDataTtlMs(),
): Promise<T> {
  const entry = await readEntry<T>(name);
  if (entry && Date.now() - entry.ts < ttlMs) {
    return entry.payload;
  }
  const payload = await fetcher();
  await writeEntry(name, payload);
  return payload;
}

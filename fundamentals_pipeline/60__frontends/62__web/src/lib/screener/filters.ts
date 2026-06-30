// Screener filter predicates — port of universe_mask / sector_mask / industry_mask /
// search_mask / bucket_mask + industry_options in 61__streamlit/lib/screener.py. Applied
// in-memory over the wide frame (≈2,500 rows), so every filter change is instant.

import type { ScreenerRow } from "../queries/screener";
import { type Bucket, bucketMatch } from "./buckets";
import { ALL_INDUSTRIES, ALL_SECTORS, UNIVERSE_FLAGS, UNKNOWN_INDUSTRY } from "./constants";

export interface FilterState {
  universe: string;
  sector: string;
  industry: string;
  query: string;
  /** metric name → selected bucket labels. */
  buckets: Record<string, string[]>;
}

function sectorMatch(row: ScreenerRow, sector: string): boolean {
  return !sector || sector === ALL_SECTORS || row.sector === sector;
}

function universeMatch(row: ScreenerRow, universe: string): boolean {
  const col = UNIVERSE_FLAGS[universe] ?? "";
  if (!col) return true;
  return row[col] === true;
}

function industryMatch(row: ScreenerRow, industry: string): boolean {
  return !industry || industry === ALL_INDUSTRIES || row.industry === industry;
}

function searchMatch(row: ScreenerRow, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return row.ticker.toLowerCase().includes(q) || row.company.toLowerCase().includes(q);
}

/**
 * Sorted industry choices, scoped to the active sector: "All industries" (no-op default)
 * + the distinct non-Unknown industries present among rows in that sector. Data-driven,
 * because Yahoo carries ~145 industries and the published set varies.
 */
export function industryOptions(rows: ScreenerRow[], sector: string): string[] {
  const present = new Set<string>();
  for (const row of rows) {
    if (!sectorMatch(row, sector)) continue;
    if (row.industry && row.industry !== UNKNOWN_INDUSTRY) present.add(row.industry);
  }
  return [ALL_INDUSTRIES, ...Array.from(present).sort((a, b) => a.localeCompare(b))];
}

/** Apply the full filter state to the frame. `bucketSpecs` maps each filterable metric to its band table. */
export function applyFilters(
  rows: ScreenerRow[],
  state: FilterState,
  bucketSpecs: Record<string, Bucket[]>,
): ScreenerRow[] {
  const activeBuckets = Object.entries(state.buckets).filter(([, labels]) => labels.length > 0);
  return rows.filter((row) => {
    if (!universeMatch(row, state.universe)) return false;
    if (!sectorMatch(row, state.sector)) return false;
    if (!industryMatch(row, state.industry)) return false;
    if (!searchMatch(row, state.query)) return false;
    for (const [metric, labels] of activeBuckets) {
      const spec = bucketSpecs[metric];
      if (spec && !bucketMatch(row.metrics[metric], labels, spec)) return false;
    }
    return true;
  });
}

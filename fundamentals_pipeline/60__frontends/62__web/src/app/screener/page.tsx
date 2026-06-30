"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import { useQueryStates } from "nuqs";
import type { SortingState } from "@tanstack/react-table";
import ScreenerFilters from "@/components/screener/ScreenerFilters";
import ScreenerTable from "@/components/screener/ScreenerTable";
import { loadScreenerFrame, type ScreenerFrame } from "@/lib/queries/screener";
import { type Bucket, bucketsFor } from "@/lib/screener/buckets";
import { ALL_INDUSTRIES, MARKET_CAP, SECTORS, UNIVERSE_LABELS } from "@/lib/screener/constants";
import { applyFilters, industryOptions } from "@/lib/screener/filters";
import { screenerParsers } from "@/lib/screener/urlState";

// nuqs reads useSearchParams(), which requires a Suspense boundary above it for the static
// (CSR-bailout) prerender on Cloudflare Pages. The whole view is client-only anyway.
export default function ScreenerPage() {
  return (
    <Suspense fallback={<section className="py-10 font-mono text-sm text-ink-3">Loading…</section>}>
      <ScreenerView />
    </Suspense>
  );
}

function ScreenerView() {
  const [frame, setFrame] = useState<ScreenerFrame | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [{ u, sec, ind, q, cols, b, sort, dir }, setQuery] = useQueryStates(screenerParsers);

  useEffect(() => {
    let cancelled = false;
    loadScreenerFrame()
      .then((f) => !cancelled && setFrame(f))
      .catch((e) => !cancelled && setError(e instanceof Error ? e.message : String(e)));
    return () => {
      cancelled = true;
    };
  }, []);

  // ── derived state (cheap; recomputed on any filter change) ────────────────────────────
  const industryOpts = useMemo(
    () => (frame ? industryOptions(frame.rows, sec) : [ALL_INDUSTRIES]),
    [frame, sec],
  );
  // A sector change can orphan the industry selection — fall back to the no-op default.
  const industry = industryOpts.includes(ind) ? ind : ALL_INDUSTRIES;

  const visibleCols = useMemo(
    () => (frame ? frame.metricOrder.filter((m) => cols.includes(m)) : []),
    [frame, cols],
  );

  // Bucket specs for EVERY metric with a band (used for filtering, incl. stale selections on
  // a deselected column); the pill UI only renders the subset for visible columns.
  const allBucketSpecs = useMemo(() => {
    const out: Record<string, Bucket[]> = {};
    if (!frame) return out;
    for (const m of frame.metricOrder) {
      const spec = bucketsFor(m, frame.unitMap[m]);
      if (spec.length) out[m] = spec;
    }
    return out;
  }, [frame]);

  // Filterable metrics shown as pill groups: Market Cap + visible columns that have a band.
  const filterMetrics = useMemo(() => {
    if (!frame) return [];
    const wanted = [MARKET_CAP, ...visibleCols.filter((c) => c !== MARKET_CAP)];
    return wanted.filter((m) => allBucketSpecs[m]);
  }, [frame, visibleCols, allBucketSpecs]);

  const filteredRows = useMemo(() => {
    if (!frame) return [];
    return applyFilters(
      frame.rows,
      { universe: u, sector: sec, industry, query: q, buckets: b },
      allBucketSpecs,
    );
  }, [frame, u, sec, industry, q, b, allBucketSpecs]);

  const sorting: SortingState = sort ? [{ id: sort, desc: dir === "desc" }] : [];
  function onSortingChange(next: SortingState) {
    if (next.length === 0) {
      void setQuery({ sort: "", dir: "asc" });
    } else {
      void setQuery({ sort: next[0].id, dir: next[0].desc ? "desc" : "asc" });
    }
  }

  return (
    <section className="py-10">
      <header className="mb-6">
        <div className="mb-1 font-mono text-[11px] uppercase tracking-widest text-ink-3">
          US Equities · Fundamental Analysis
        </div>
        <h1 className="font-display text-3xl font-medium tracking-tight text-ink">
          US Stock FA Screener
        </h1>
        <p className="mt-1 font-sans text-sm text-ink-2">
          {frame ? `${frame.rows.length.toLocaleString()} companies` : "Loading…"} · Latest
          available FY · USD · SEC EDGAR XBRL
        </p>
      </header>

      {error ? (
        <div className="rounded-md border border-signal-bad/40 bg-bg-card p-4 font-mono text-sm text-signal-bad">
          Failed to load data: {error}
        </div>
      ) : !frame ? (
        <div className="font-mono text-sm text-ink-3">Loading screener from the published artifact…</div>
      ) : (
        <>
          <ScreenerFilters
            universe={u}
            onUniverse={(v) => setQuery({ u: v })}
            universeLabels={UNIVERSE_LABELS}
            sector={sec}
            onSector={(v) => setQuery({ sec: v, ind: ALL_INDUSTRIES })}
            sectors={SECTORS}
            industry={industry}
            onIndustry={(v) => setQuery({ ind: v })}
            industryOpts={industryOpts}
            query={q}
            onQuery={(v) => setQuery({ q: v })}
            metricOrder={frame.metricOrder}
            visibleCols={visibleCols}
            onCols={(c) => setQuery({ cols: c })}
            buckets={b}
            onBuckets={(next) => setQuery({ b: next })}
            filterMetrics={filterMetrics}
            bucketSpecs={allBucketSpecs}
          />

          <p className="mt-4 font-mono text-xs text-ink-3">
            {filteredRows.length.toLocaleString()} companies after filters
          </p>

          <div className="mt-2">
            <ScreenerTable
              rows={filteredRows}
              columns={visibleCols}
              unitMap={frame.unitMap}
              sorting={sorting}
              onSortingChange={onSortingChange}
            />
          </div>
        </>
      )}
    </section>
  );
}

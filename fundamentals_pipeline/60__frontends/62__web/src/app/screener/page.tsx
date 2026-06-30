"use client";

import { useEffect, useState } from "react";
import { getScreenerData } from "@/lib/artifacts";

// Scaffolding proof-of-pipeline: fetch the live Release artifact, count distinct tickers in
// DuckDB-WASM, render the real number. Filters + the sortable table come in a follow-up task.
export default function ScreenerPage() {
  const [count, setCount] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getScreenerData()
      .then((table) => {
        if (!cancelled) setCount(table.numRows);
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <section className="py-14">
      <h1 className="font-display text-3xl font-medium tracking-tight text-ink">Screener</h1>
      <p className="mt-2 max-w-2xl font-sans text-sm text-ink-2">
        Placeholder — filters and the sortable table land in a follow-up. The figure below is the
        end-to-end proof: fetched live from the published Release artifact and counted in
        DuckDB-WASM, in your browser.
      </p>
      <div className="mt-8 font-mono text-sm">
        {error ? (
          <p className="text-signal-bad">Error: {error}</p>
        ) : count === null ? (
          <p className="text-ink-3">Loading tickers from the published artifact…</p>
        ) : (
          <p className="text-signal-good">Tickers loaded: {count}</p>
        )}
      </div>
    </section>
  );
}

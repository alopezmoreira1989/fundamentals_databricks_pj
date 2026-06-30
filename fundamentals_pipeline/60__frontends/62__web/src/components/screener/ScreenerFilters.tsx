"use client";

import type { Bucket } from "@/lib/screener/buckets";

interface Props {
  universe: string;
  onUniverse: (v: string) => void;
  universeLabels: string[];

  sector: string;
  onSector: (v: string) => void;
  sectors: string[];

  industry: string;
  onIndustry: (v: string) => void;
  industryOpts: string[];

  query: string;
  onQuery: (v: string) => void;

  metricOrder: string[];
  visibleCols: string[];
  onCols: (cols: string[]) => void;

  buckets: Record<string, string[]>;
  onBuckets: (next: Record<string, string[]>) => void;
  filterMetrics: string[];
  bucketSpecs: Record<string, Bucket[]>;
}

const SELECT_CLASS =
  "w-full rounded-md border border-rule bg-bg-card px-2.5 py-1.5 font-sans text-sm text-ink " +
  "focus:border-accent focus:outline-none";
const LABEL_CLASS = "mb-1 block font-mono text-[11px] uppercase tracking-wide text-ink-3";

export default function ScreenerFilters(props: Props) {
  const {
    universe, onUniverse, universeLabels,
    sector, onSector, sectors,
    industry, onIndustry, industryOpts,
    query, onQuery,
    metricOrder, visibleCols, onCols,
    buckets, onBuckets, filterMetrics, bucketSpecs,
  } = props;

  function toggleCol(metric: string) {
    onCols(
      visibleCols.includes(metric)
        ? visibleCols.filter((c) => c !== metric)
        : // preserve metricOrder ordering when adding
          metricOrder.filter((m) => visibleCols.includes(m) || m === metric),
    );
  }

  function toggleBucket(metric: string, label: string) {
    const current = buckets[metric] ?? [];
    const next = current.includes(label)
      ? current.filter((l) => l !== label)
      : [...current, label];
    const merged = { ...buckets, [metric]: next };
    if (next.length === 0) delete merged[metric];
    onBuckets(merged);
  }

  return (
    <div className="rounded-lg border border-rule bg-bg-card/60 p-4">
      <div className="grid grid-cols-1 gap-3 md:grid-cols-4">
        <div>
          <label className={LABEL_CLASS}>Universe</label>
          <select className={SELECT_CLASS} value={universe} onChange={(e) => onUniverse(e.target.value)}>
            {universeLabels.map((u) => (
              <option key={u} value={u}>{u}</option>
            ))}
          </select>
        </div>
        <div>
          <label className={LABEL_CLASS}>Sector</label>
          <select className={SELECT_CLASS} value={sector} onChange={(e) => onSector(e.target.value)}>
            {sectors.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
        <div>
          <label className={LABEL_CLASS}>Industry</label>
          <select className={SELECT_CLASS} value={industry} onChange={(e) => onIndustry(e.target.value)}>
            {industryOpts.map((i) => (
              <option key={i} value={i}>{i}</option>
            ))}
          </select>
        </div>
        <div>
          <label className={LABEL_CLASS}>Search (ticker or name)</label>
          <input
            className={SELECT_CLASS}
            value={query}
            placeholder="e.g. AAPL or Apple"
            onChange={(e) => onQuery(e.target.value)}
          />
        </div>
      </div>

      {/* Columns picker */}
      <details className="mt-3 rounded-md border border-rule-soft bg-bg-subtle/40 px-3 py-2">
        <summary className="cursor-pointer select-none font-mono text-[11px] uppercase tracking-wide text-ink-3">
          Columns ({visibleCols.length})
        </summary>
        <div className="mt-2 flex flex-wrap gap-1.5">
          {metricOrder.map((m) => {
            const on = visibleCols.includes(m);
            return (
              <button
                key={m}
                type="button"
                onClick={() => toggleCol(m)}
                className={
                  "rounded-full border px-2.5 py-1 font-sans text-xs transition-colors " +
                  (on
                    ? "border-accent bg-accent-soft text-accent-ink"
                    : "border-rule bg-bg-card text-ink-2 hover:border-accent")
                }
              >
                {m}
              </button>
            );
          })}
        </div>
      </details>

      {/* Range (bucket) filters — one pill group per filterable metric */}
      {filterMetrics.length > 0 && (
        <div className="mt-3">
          <div className={LABEL_CLASS}>Range filters</div>
          <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
            {filterMetrics.map((metric) => {
              const spec = bucketSpecs[metric];
              if (!spec || spec.length === 0) return null;
              const selected = buckets[metric] ?? [];
              return (
                <div key={metric}>
                  <div className="mb-1 font-sans text-xs text-ink-2">{metric}</div>
                  <div className="flex flex-wrap gap-1">
                    {spec.map(([label]) => {
                      const on = selected.includes(label);
                      return (
                        <button
                          key={label}
                          type="button"
                          onClick={() => toggleBucket(metric, label)}
                          className={
                            "rounded border px-1.5 py-0.5 font-mono text-[11px] transition-colors " +
                            (on
                              ? "border-accent bg-accent-soft text-accent-ink"
                              : "border-rule bg-bg-card text-ink-3 hover:border-accent")
                          }
                        >
                          {label}
                        </button>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

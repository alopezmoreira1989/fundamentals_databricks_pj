"use client";

import { useMemo } from "react";
import { useRouter } from "next/navigation";
import {
  type ColumnDef,
  type SortingState,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";
import type { ScreenerRow } from "@/lib/queries/screener";
import { fmtValue, headerLabel } from "@/lib/screener/format";
import { signalAbsolute } from "@/lib/screener/signals";

const CHIP_CLASS = { good: "scr-chip-g", warn: "scr-chip-a", bad: "scr-chip-r" } as const;

interface Props {
  rows: ScreenerRow[];
  /** Visible metric columns, in display order. */
  columns: string[];
  unitMap: Record<string, string>;
  sorting: SortingState;
  onSortingChange: (next: SortingState) => void;
}

// Numeric metric cell — formatted value wrapped in a semantic green/amber/red chip.
function MetricCell({ metric, unit, value }: { metric: string; unit: string | undefined; value: number | null }) {
  const text = fmtValue(metric, unit, value);
  if (text === "—") return <span className="scr-na">—</span>;
  const sig = signalAbsolute(metric, value);
  const chip = sig ? CHIP_CLASS[sig] : null;
  return chip ? <span className={chip}>{text}</span> : <span>{text}</span>;
}

export default function ScreenerTable({ rows, columns, unitMap, sorting, onSortingChange }: Props) {
  const router = useRouter();

  const columnDefs = useMemo<ColumnDef<ScreenerRow>[]>(() => {
    const defs: ColumnDef<ScreenerRow>[] = [
      {
        id: "ticker",
        header: "Ticker",
        accessorFn: (r) => r.ticker,
        cell: (c) => <span className="scr-td-tkr">{c.row.original.ticker}</span>,
      },
      {
        id: "company",
        header: "Company",
        accessorFn: (r) => r.company,
        cell: (c) => (
          <span className="scr-td-co" title={c.row.original.company}>
            {c.row.original.company}
          </span>
        ),
      },
    ];
    for (const metric of columns) {
      defs.push({
        id: metric,
        header: headerLabel(metric, unitMap[metric]),
        // undefined (not null) so tanstack's sortUndefined keeps blanks last in both directions.
        accessorFn: (r) => r.metrics[metric] ?? undefined,
        sortUndefined: "last",
        meta: { numeric: true },
        cell: (c) => (
          <MetricCell metric={metric} unit={unitMap[metric]} value={c.row.original.metrics[metric] ?? null} />
        ),
      });
    }
    return defs;
  }, [columns, unitMap]);

  const table = useReactTable({
    data: rows,
    columns: columnDefs,
    state: { sorting },
    onSortingChange: (updater) => {
      onSortingChange(typeof updater === "function" ? updater(sorting) : updater);
    },
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  return (
    <div className="scr-tbl-wrap">
      <table className="scr-tbl">
        <thead>
          {table.getHeaderGroups().map((hg) => (
            <tr key={hg.id}>
              {hg.headers.map((header) => {
                const sorted = header.column.getIsSorted();
                const numeric = (header.column.columnDef.meta as { numeric?: boolean } | undefined)?.numeric;
                return (
                  <th
                    key={header.id}
                    className={numeric ? "scr-th-num" : undefined}
                    onClick={header.column.getToggleSortingHandler()}
                  >
                    <span className="scr-th-btn">
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      {sorted ? <span className="scr-arrow">{sorted === "desc" ? "▾" : "▴"}</span> : null}
                    </span>
                  </th>
                );
              })}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id} className="scr-row" onClick={() => router.push(`/company/${row.original.ticker}`)}>
              {row.getVisibleCells().map((cell) => {
                const numeric = (cell.column.columnDef.meta as { numeric?: boolean } | undefined)?.numeric;
                return (
                  <td key={cell.id} className={numeric ? "scr-td-num" : undefined}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

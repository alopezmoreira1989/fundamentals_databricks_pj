"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { loadCompanyView, type CompanyView } from "@/lib/queries/company";
import { loadStatements, type Statements } from "@/lib/queries/statements";
import { loadStatementTables, type StatementTables } from "@/lib/queries/statementTables";
import { loadQuarterly, type QuarterlyData } from "@/lib/queries/quarterly";
import { loadPrices, type PricePoint } from "@/lib/queries/prices";
import CompanyMasthead from "./CompanyMasthead";
import KpiStrip from "./KpiStrip";
import MetricsGrid from "./MetricsGrid";
import StatementTable from "./StatementTable";
import QuarterlyTable from "./QuarterlyTable";
import RevenueCombo from "./charts/RevenueCombo";
import CashFlowChart from "./charts/CashFlowChart";
import MarginBridge from "./charts/MarginBridge";
import BsComposition from "./charts/BsComposition";
import QuarterlyCombo from "./charts/QuarterlyCombo";
import PricePanel from "./charts/PricePanel";
import FootballField from "./valuation/FootballField";
import { ivLatestMap, ivPriceFromMetrics } from "@/lib/valuation";

interface Loaded {
  view: CompanyView;
  statements: Statements;
  tables: StatementTables;
  quarterly: QuarterlyData;
}
type State =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "empty" }
  | { status: "ready"; data: Loaded };

type PricesState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ready"; series: PricePoint[] };

const TABS = ["Income statement", "Balance sheet", "Cash flow", "Price", "Quarterly", "Derived metrics"] as const;
type Tab = (typeof TABS)[number];

const TAB_META: Record<Tab, { title: string; meta: string }> = {
  "Income statement": { title: "Income statement", meta: "Up to 10 fiscal years · USD" },
  "Balance sheet": { title: "Balance sheet", meta: "Fiscal year-end snapshots · USD" },
  "Cash flow": { title: "Cash flow", meta: "Operating · Investing · Financing" },
  Price: { title: "Price", meta: "" },
  Quarterly: { title: "Quarterly", meta: "Last 12 quarters · USD · YoY = same quarter prior year" },
  "Derived metrics": { title: "Derived metrics", meta: "From metrics_hierarchy.json · latest FY" },
};

export default function CompanyClient({ ticker }: { ticker: string }) {
  const [state, setState] = useState<State>({ status: "loading" });
  const [tab, setTab] = useState<Tab>("Income statement");
  const [ivPeriod, setIvPeriod] = useState<"FY" | "TTM">("FY");
  // Prices (43 MB artifact) load lazily — only when the Price tab is first opened.
  const [prices, setPrices] = useState<PricesState>({ status: "idle" });

  useEffect(() => {
    let cancelled = false;
    setState({ status: "loading" });
    setPrices({ status: "idle" });
    Promise.all([loadCompanyView(ticker), loadStatements(ticker), loadStatementTables(ticker), loadQuarterly(ticker)])
      .then(([view, statements, tables, quarterly]) => {
        if (cancelled) return;
        setState(view ? { status: "ready", data: { view, statements, tables, quarterly } } : { status: "empty" });
      })
      .catch((e) => {
        if (!cancelled) setState({ status: "error", message: e instanceof Error ? e.message : String(e) });
      });
    return () => {
      cancelled = true;
    };
  }, [ticker]);

  useEffect(() => {
    if (tab !== "Price" || prices.status !== "idle") return;
    let cancelled = false;
    setPrices({ status: "loading" });
    loadPrices(ticker)
      .then((series) => !cancelled && setPrices({ status: "ready", series }))
      .catch((e) => !cancelled && setPrices({ status: "error", message: e instanceof Error ? e.message : String(e) }));
    return () => {
      cancelled = true;
    };
  }, [tab, ticker, prices.status]);

  const data = state.status === "ready" ? state.data : null;

  const tabBody = useMemo(() => {
    if (!data) return null;
    const { view, statements, tables, quarterly } = data;
    switch (tab) {
      case "Income statement":
        return (
          <>
            <RevenueCombo frame={statements.is} />
            <MarginBridge frame={statements.is} />
            <StatementTable table={tables.is} />
          </>
        );
      case "Balance sheet":
        return (
          <>
            <BsComposition frame={statements.bs} />
            <StatementTable table={tables.bs} />
          </>
        );
      case "Cash flow":
        return (
          <>
            <CashFlowChart frame={statements.cf} />
            <StatementTable table={tables.cf} />
          </>
        );
      case "Quarterly":
        return (
          <>
            <QuarterlyCombo data={quarterly} />
            <QuarterlyTable data={quarterly} />
          </>
        );
      case "Price":
        return null; // rendered separately (lazy prices state)
      case "Derived metrics": {
        const ivPrice = ivPriceFromMetrics(ivLatestMap(view.categories), ivPeriod);
        return (
          <>
            <div className="mb-4 flex items-center gap-1">
              <span className="mr-2 font-mono text-[11px] uppercase tracking-wide text-ink-3">Intrinsic value</span>
              {(["FY", "TTM"] as const).map((p) => (
                <button
                  key={p}
                  type="button"
                  onClick={() => setIvPeriod(p)}
                  className={
                    "rounded border px-2.5 py-0.5 font-mono text-[11px] transition-colors " +
                    (p === ivPeriod
                      ? "border-accent bg-accent-soft text-accent-ink"
                      : "border-rule bg-bg-card text-ink-3 hover:border-accent")
                  }
                >
                  {p}
                </button>
              ))}
            </div>
            <MetricsGrid view={view} period={ivPeriod} ivPrice={ivPrice} />
            <FootballField categories={view.categories} period={ivPeriod} price={ivPrice} />
          </>
        );
      }
    }
  }, [data, tab, ivPeriod]);

  return (
    <section className="py-10">
      <Link href="/screener" className="font-mono text-xs text-ink-2 transition-colors hover:text-accent">
        ← Screener
      </Link>

      {state.status === "loading" && (
        <p className="mt-8 font-mono text-sm text-ink-3">Loading {ticker} from the published artifact…</p>
      )}
      {state.status === "error" && (
        <div className="mt-8 rounded-md border border-signal-bad/40 bg-bg-card p-4 font-mono text-sm text-signal-bad">
          Failed to load {ticker}: {state.message}
        </div>
      )}
      {state.status === "empty" && (
        <p className="mt-8 font-sans text-sm text-ink-2">
          No data found for <strong>{ticker}</strong>.
        </p>
      )}

      {state.status === "ready" && data && (
        <>
          <div className="mt-4">
            <CompanyMasthead view={data.view} />
          </div>
          <div className="mt-5">
            <KpiStrip kpis={data.view.kpis} />
          </div>

          <div className="company-tabs">
            {TABS.map((t) => (
              <button key={t} type="button" onClick={() => setTab(t)} className={`company-tab${t === tab ? " is-active" : ""}`}>
                {t}
              </button>
            ))}
          </div>

          {tab === "Price" ? (
            <div className="mt-6">
              {prices.status === "ready" ? (
                <PricePanel series={prices.series} />
              ) : prices.status === "error" ? (
                <div className="rounded-md border border-signal-bad/40 bg-bg-card p-4 font-mono text-sm text-signal-bad">
                  Failed to load prices: {prices.message}
                </div>
              ) : (
                <p className="font-mono text-sm text-ink-3">Loading prices (≈43 MB, cached after first load)…</p>
              )}
            </div>
          ) : (
            <>
              <div className="panel-header">
                <h2>{TAB_META[tab].title}</h2>
                <div className="meta">{TAB_META[tab].meta}</div>
              </div>
              {tabBody}
            </>
          )}

          <div className="footnote">
            <span style={{ color: "var(--ink)", fontWeight: 500 }}>main.financials.financials</span>
            <span className="pipe">|</span> Derived metrics from financials_metrics
            <span className="pipe">|</span> Source · SEC EDGAR XBRL
            <span className="pipe">|</span> Built {data.view.buildTimestamp}
            {process.env.NEXT_PUBLIC_LOGO_DEV_KEY ? (
              <>
                <span className="pipe">|</span> Logos by{" "}
                <a href="https://logo.dev" target="_blank" rel="noopener noreferrer">
                  Logo.dev
                </a>
              </>
            ) : null}
          </div>
        </>
      )}
    </section>
  );
}

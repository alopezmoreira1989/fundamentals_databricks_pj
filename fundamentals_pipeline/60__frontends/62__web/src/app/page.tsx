import Link from "next/link";

export default function Home() {
  return (
    <section className="py-14">
      <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-3">
        Public · Release-fed
      </p>
      <h1 className="mt-2 font-display text-5xl font-medium tracking-tight text-ink">
        Equity Fundamentals
      </h1>
      <p className="mt-5 max-w-2xl font-sans text-base text-ink-2">
        SEC EDGAR fundamentals, derived metrics, and intrinsic values — served straight from the
        public GitHub Release artifacts and queried in your browser with DuckDB-WASM. No backend,
        no credentials.
      </p>
      <p className="mt-8 font-sans text-sm text-ink-2">
        Start with the{" "}
        <Link href="/screener" className="text-accent underline underline-offset-2">
          Screener
        </Link>
        .
      </p>
    </section>
  );
}

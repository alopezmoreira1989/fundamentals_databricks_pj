// Dynamic route. `runtime = "edge"` so @cloudflare/next-on-pages builds it for Cloudflare
// Pages (non-static routes must run on the edge runtime). Real company view is a follow-up.
export const runtime = "edge";

export default function CompanyPage({ params }: { params: { ticker: string } }) {
  return (
    <section className="py-14">
      <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-3">Company</p>
      <h1 className="mt-2 font-display text-3xl font-medium tracking-tight text-ink">
        {params.ticker.toUpperCase()}
      </h1>
      <p className="mt-4 max-w-2xl font-sans text-ink-2">
        Placeholder — the company financials view (statements, metrics, intrinsic value) is built
        in a follow-up task.
      </p>
    </section>
  );
}

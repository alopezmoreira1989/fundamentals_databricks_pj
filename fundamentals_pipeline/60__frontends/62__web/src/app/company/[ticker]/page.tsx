// Dynamic route. `runtime = "edge"` so @cloudflare/next-on-pages builds it for Cloudflare
// Pages (non-static routes run on the edge runtime). The page shell is server-rendered; all
// data work happens client-side in <CompanyClient> (DuckDB-WASM in the browser).
import CompanyClient from "@/components/company/CompanyClient";

export const runtime = "edge";

export default function CompanyPage({ params }: { params: { ticker: string } }) {
  return <CompanyClient ticker={decodeURIComponent(params.ticker).toUpperCase()} />;
}

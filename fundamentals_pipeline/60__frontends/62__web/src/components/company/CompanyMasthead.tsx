// Company masthead — port of render_masthead. Logo + identity (name, ticker/sector/industry
// chips) on the left; FY coverage + source on the right.

import type { CompanyView } from "@/lib/queries/company";
import CompanyLogo from "./CompanyLogo";

export default function CompanyMasthead({ view }: { view: CompanyView }) {
  const fyRange = view.fyMin !== null ? `FY ${view.fyMin} — FY ${view.fyMax}` : "";
  return (
    <div className="masthead">
      <div className="masthead-left">
        <CompanyLogo ticker={view.ticker} company={view.company} hasLogo={view.hasLogo} />
        <div className="masthead-identity">
          <div className="eyebrow">Fundamentals · Annual filings</div>
          <h1>{view.company}</h1>
          <div className="ticker-row">
            <span className="ticker-chip">{view.ticker}</span>
            <span className="ticker-chip">{view.sector}</span>
            {view.industry ? <span className="ticker-chip">{view.industry}</span> : null}
          </div>
        </div>
      </div>
      <div className="masthead-right">
        <div className="date">{fyRange}</div>
        <div>{view.nYears} fiscal years · USD</div>
        <div style={{ marginTop: 6 }}>SEC EDGAR XBRL</div>
      </div>
    </div>
  );
}

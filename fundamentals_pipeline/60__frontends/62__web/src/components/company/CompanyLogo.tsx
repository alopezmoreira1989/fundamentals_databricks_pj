// Masthead logo box — port of _render_company_logo. With a Logo.dev publishable key
// (NEXT_PUBLIC_LOGO_DEV_KEY) and not a known miss, hotlink the CDN as a background image;
// otherwise paint the editorial monogram. The key is a publishable client token by design.

const LOGO_DEV_BASE = "https://img.logo.dev/ticker";

function logoDevUrl(ticker: string, key: string, size = 144): string {
  return `${LOGO_DEV_BASE}/${encodeURIComponent(ticker)}?token=${encodeURIComponent(key)}&size=${size}&format=png`;
}

export default function CompanyLogo({
  ticker,
  company,
  hasLogo,
}: {
  ticker: string;
  company: string;
  hasLogo: boolean | null;
}) {
  const key = process.env.NEXT_PUBLIC_LOGO_DEV_KEY;
  if (!key || hasLogo === false) {
    const letter = (company || ticker).trim().charAt(0).toUpperCase() || "•";
    return (
      <div className="company-logo is-monogram">
        <span className="logo-monogram">{letter}</span>
      </div>
    );
  }
  return (
    <div
      className="company-logo"
      style={{
        backgroundImage: `url('${logoDevUrl(ticker, key)}')`,
        backgroundSize: "contain",
        backgroundRepeat: "no-repeat",
        backgroundPosition: "center",
      }}
    />
  );
}

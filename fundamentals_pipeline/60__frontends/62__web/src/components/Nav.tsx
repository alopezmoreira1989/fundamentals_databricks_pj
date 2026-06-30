import Link from "next/link";

const LINKS = [
  { href: "/", label: "Overview" },
  { href: "/screener", label: "Screener" },
  { href: "/compare", label: "Compare" },
];

// Minimal masthead shell other pages plug into. Fraunces wordmark, Inter links,
// cream background — tokens only.
export default function Nav() {
  return (
    <header className="border-b border-rule bg-bg">
      <div className="mx-auto flex max-w-[1280px] items-baseline justify-between px-5 py-4">
        <Link
          href="/"
          className="font-display text-2xl font-medium tracking-tight text-ink"
        >
          Equity Fundamentals
        </Link>
        <nav className="flex gap-6">
          {LINKS.map((l) => (
            <Link
              key={l.href}
              href={l.href}
              className="font-sans text-sm text-ink-2 transition-colors hover:text-accent"
            >
              {l.label}
            </Link>
          ))}
        </nav>
      </div>
    </header>
  );
}

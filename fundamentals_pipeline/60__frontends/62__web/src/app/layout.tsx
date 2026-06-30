import type { Metadata } from "next";
import { Fraunces, Inter, JetBrains_Mono } from "next/font/google";
import Nav from "@/components/Nav";
import "./globals.css";

// next/font self-hosts these and exposes each as a CSS variable (no <link>, no FOUT).
// globals.css composes them into --font-display / --font-sans / --font-mono.
const fraunces = Fraunces({ subsets: ["latin"], variable: "--font-fraunces", display: "swap" });
const inter = Inter({ subsets: ["latin"], variable: "--font-inter", display: "swap" });
const jetbrainsMono = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--font-jetbrains-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: "Equity Fundamentals",
  description:
    "SEC fundamentals, derived metrics and intrinsic values — served from public Release artifacts, queried in-browser.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html
      lang="en"
      className={`${fraunces.variable} ${inter.variable} ${jetbrainsMono.variable}`}
    >
      <body className="min-h-screen bg-bg font-sans text-ink antialiased">
        <Nav />
        <main className="mx-auto max-w-[1280px] px-5">{children}</main>
      </body>
    </html>
  );
}

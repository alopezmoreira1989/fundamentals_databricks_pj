import type { Config } from "tailwindcss";

// Colors + fonts are defined as CSS custom properties in src/app/globals.css
// (ported from the Streamlit app). Tailwind utilities here just point at those
// vars so we never hardcode a hex in a className.
const config: Config = {
  content: ["./src/**/*.{js,ts,jsx,tsx,mdx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--bg)",
        "bg-card": "var(--bg-card)",
        "bg-subtle": "var(--bg-subtle)",
        ink: "var(--ink)",
        "ink-2": "var(--ink-2)",
        "ink-3": "var(--ink-3)",
        rule: "var(--rule)",
        "rule-soft": "var(--rule-soft)",
        accent: "var(--accent)",
        "accent-soft": "var(--accent-soft)",
        "accent-ink": "var(--accent-ink)",
        "signal-good": "var(--signal-good)",
        "signal-warn": "var(--signal-warn)",
        "signal-bad": "var(--signal-bad)",
      },
      fontFamily: {
        display: ["var(--font-display)"],
        sans: ["var(--font-sans)"],
        mono: ["var(--font-mono)"],
      },
    },
  },
  plugins: [],
};
export default config;

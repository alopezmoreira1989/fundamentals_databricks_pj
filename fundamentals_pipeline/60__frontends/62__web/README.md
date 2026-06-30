# 62__web — public web frontend (Next.js + DuckDB-WASM)

Second frontend consumer under `fundamentals_pipeline/60__frontends/`, alongside
`61__streamlit/`. It reads the **same** published artifacts the Streamlit app reads — the
Parquet files on the GitHub Release `latest` tag uploaded by
`50__publish/52__publish_to_github.py` — but queries them with **DuckDB-WASM in the visitor's
browser**. No Databricks at runtime, no backend, no credentials, no duplication of the export
logic.

> Status: **scaffolding**. The Screener proves the pipeline end-to-end (fetch → DuckDB-WASM →
> a real ticker count on screen). The real Screener / Company / Compare UI is built in
> follow-up tasks.

## Stack

- Next.js 14 (App Router, TypeScript, `src/` dir), React 18
- Tailwind CSS 3 — design tokens ported from `61__streamlit/styles.css` into
  `src/app/globals.css` as CSS custom properties (cream `--bg`, accent `--accent`, signal
  good/warn/bad, Fraunces / Inter / JetBrains Mono via `next/font/google`)
- `@duckdb/duckdb-wasm` + `apache-arrow` for in-browser Parquet querying
- `@cloudflare/next-on-pages` for Cloudflare Pages deploys

Package manager is **pnpm** (a `pnpm-workspace.yaml` here isolates this Node project from the
parent Python repo and approves the native build scripts pnpm 11 requires via `allowBuilds`).

## Run locally

```bash
cd fundamentals_pipeline/60__frontends/62__web
pnpm install
pnpm dev          # http://localhost:3000  (open /screener for the live proof)
pnpm lint
pnpm build
```

## How the data fetch works

`src/lib/artifacts.ts` defines `RELEASE_BASE`:

```
https://github.com/alopezmoreira1989/fundamentals_databricks_pj/releases/download/latest
```

Assets (schemas pinned in `fundamentals_pipeline/_core/schemas.py`): `dashboard_data.parquet`,
`dashboard_metrics.parquet`, `dashboard_prices.parquet`, `dashboard_backtest.parquet`,
`dashboard_meta.json`.

The browser fetches an asset over plain HTTPS (the repo is public — no token), then registers
the bytes with DuckDB via `registerFileBuffer` and runs SQL against it. `getScreenerData()`
currently runs `SELECT DISTINCT ticker FROM 'dashboard_metrics.parquet'`; the Screener page
renders the row count as the end-to-end proof.

`src/lib/duckdb.ts` holds a single shared `AsyncDuckDB` instance (WASM bundle auto-selected
from jsDelivr), reused across queries.

## Deploy to Cloudflare Pages (manual — not done here)

Connect this repo in the Cloudflare dashboard with:

| Setting | Value |
|---|---|
| Root directory | `fundamentals_pipeline/60__frontends/62__web` |
| Build command | `pnpm dlx @cloudflare/next-on-pages` (or `pnpm pages:build`) |
| Build output directory | `.vercel/output/static` |
| Compatibility flags | `nodejs_compat` (see `wrangler.toml`) |

`@cloudflare/next-on-pages` is deprecated in favor of the OpenNext Cloudflare adapter; it's
used here because it's the tool the task specified and it supports Next 14. Swapping to
`@opennextjs/cloudflare` is a later option.

No environment variables or secrets are required — every data URL is a public Release asset.

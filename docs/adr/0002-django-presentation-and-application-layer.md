# ADR-0002: Django as the presentation + application layer (replacing the decoupled Next.js frontend)

- **Status:** Accepted
- **Date:** 2026-07-03  <!-- recorded retroactively; decided during the web-layer initiative on dev_alm -->
- **Deciders:** repo owner

## Context

The pipeline publishes its results as Parquet/JSON artifacts to a GitHub Release, already
consumed by a public **Streamlit** app (`60__frontends/61__streamlit/`). A second, richer
web surface was wanted — one with **authentication and user-owned data** (watchlists,
favorites, browsing history) and a REST API, not just read-only dashboards.

An earlier attempt was a decoupled **Next.js** frontend (`60__frontends/62__web/`) talking to a
separate API. That split a small solo project across two languages and two deploy targets, and
it still needed a Python backend for auth, user data, and to reuse the `fundamentals_pipeline`
financial logic — which a JS frontend cannot import.

## Decision

We will build the richer web surface as a **Django** application under `web/` that is **both the
presentation and the application layer** — server-rendered templates, session auth, user data in
PostgreSQL, and a DRF REST API — and remove the Next.js frontend. Django imports
`fundamentals_pipeline` as a normal dependency and reads the same published artifacts (via
DuckDB), so all financial logic and the data contract stay in the one shared library.

## Consequences

- One language (Python) and one deployable for the whole authenticated surface; the frontend can
  `import fundamentals_pipeline` directly, so financial logic is never reimplemented in JS.
- Server-rendered pages are simpler to secure (session auth, CSRF, `@login_required`) than a
  separate SPA + token API, and SEO/first-paint come for free.
- We forgo a SPA's client-side interactivity; where a page needs live JSON, the same read model
  is exposed via the DRF API (`apps/api`) rather than a separate service.
- The Streamlit app remains a **separate, independent** public consumer of the artifacts — this
  decision is about the authenticated web app, not about replacing Streamlit.

## Alternatives considered

- **Keep the Next.js frontend + a separate Python API.** Rejected: two stacks/deploys for a solo
  project, and the JS layer still couldn't reuse `fundamentals_pipeline`.
- **Extend the Streamlit app with auth/user data.** Rejected: Streamlit is a dashboard tool, not
  an application framework — no first-class auth, URL routing, forms, or relational user data.
- **A Python API-only backend (DRF) with a static SPA.** Rejected for now: more moving parts than
  server-rendered Django buys us at this scale; the DRF API is still provided for API consumers.

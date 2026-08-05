# ADR-0008: Retire `web/`, consolidate the presentation layer on `fundamentals_screener`

- **Status:** Accepted
- **Date:** 2026-08-05
- **Deciders:** repo owner

## Context

ADR-0002 through ADR-0007 built `web/`, a full Django application (session auth, user data in
PostgreSQL, watchlists/favorites/history, a DRF REST API) intended as a second, richer
frontend alongside the public Streamlit app. Around the same time, `fundamentals_screener/`
was extracted from `web/`'s own `apps/companies`/`apps/screener`/`apps/valuation` as a
separate, standalone installable Django app (own `pyproject.toml`, own base template/CSS/JS,
no auth, no PostgreSQL) for reuse by an external project — the repo owner's personal site,
alopezm.xyz.

By 2026-08, `fundamentals_screener` was live in production there (Dinahosting CGI hosting,
consumed via a pinned-git-tag release pipeline — see its own README's "External consumers"
section). `web/` had never actually been deployed: `fly.toml`/`render.yaml` (issue #153) were
prepared deployment scaffolding with placeholder app names, but no live Fly.io or Render
instance was ever created from them, and no `.github/workflows/` job deployed it either. So
the two apps had drifted into an asymmetric state: one deployed, real, and load-bearing; the
other a fully-built but unlaunched second implementation of largely the same read-only
screener/company/valuation surface, maintained in parallel.

This asymmetry surfaced concretely mid-way through the Forecasting milestone (#18): issue
#335's repository/service/view work was built against `web/` (PR #346), then had to be
re-built from scratch in `fundamentals_screener` once this decision was made — direct,
measured proof that keeping two frontends in sync was already costing real duplicated effort,
for a `web/` surface nobody was using.

The one capability `web/` had that `fundamentals_screener` deliberately doesn't (per its own
README's "Not ported" section) is login-scoped personalization — watchlists, favorites,
browsing history, plus the DRF REST API. None of that was in active use (no live deployment),
and `fundamentals_screener`'s own `services.py`/`repositories/` are explicitly designed to be
extended if that capability is ever actually needed.

## Decision

We will delete `web/` entirely — the Django app, its `fly.toml`/`render.yaml` deploy configs,
`docs/deploy-fly.md`/`docs/deploy-render.md`, `docs/architecture.md` (its layering doc), and
the `web` CI job — and treat `fundamentals_screener` as the repo's one Django presentation
layer going forward. Any future personalization/auth need gets built there, from scratch,
when it's actually needed — not resurrected from `web/`'s implementation, since
`fundamentals_screener`'s architecture (no ORM/PostgreSQL dependency, CGI-safe synchronous
data layer, Python 3.9 floor) is deliberately different from `web/`'s.

ADR-0002 through ADR-0007 remain historically accurate records of decisions that were in
force while `web/` existed; they are marked **Superseded by ADR-0008** rather than deleted,
per this log's own "immutable once Accepted" convention (ADR-0001). None of their individual
decisions (Django as presentation+application layer, strict repository layering, DuckDB read
model, UUID pks, mandatory-repositories-for-analytical-storage, custom user model from the
first migration) are re-litigated here — they simply no longer apply to any code in this
repo. A future `fundamentals_screener` auth/personalization effort that wants any of them
back should write its own new ADR referencing the relevant superseded one, not assume it's
still in force.

## Consequences

- One less deployed-or-not-deployed frontend to keep in sync with the pipeline's published
  artifacts and with each other. Every future frontend feature (this ADR's own trigger,
  Forecasting) is built once, in `fundamentals_screener`.
- `fundamentals_screener`'s own architecture — no strict `views → services → repositories →
  infrastructure` layering doc of its own (it has one, informally, in its README's
  "Architecture" section, but nothing as formal as ADR-0003) — becomes the only Django
  architecture this repo has to reason about. If it grows enough to need a repository-tier ADR
  of its own, write one; don't assume ADR-0003 still governs it just because the shape looks
  similar.
- Personalization (watchlists/favorites/history) and a REST API are gone until/unless
  rebuilt. This is accepted, not a gap to backfill proactively — nothing was consuming them.
- The external consumer contract (`fundamentals_screener`'s pinned-git-tag release pipeline,
  documented in the root `CLAUDE.md`'s "External consumers" section) is unaffected — this
  decision doesn't touch that package's public API, only removes an unrelated, undeployed
  sibling app from the same repo.

## Alternatives considered

- **Keep both, formalize `web/` as the "future" app.** Rejected — there was no concrete plan
  or timeline to actually deploy `web/`, and the Forecasting milestone had just demonstrated
  the real cost of building every read-only feature twice.
- **Port `web/`'s auth/watchlists/favorites/history into `fundamentals_screener` before
  deleting `web/`, to avoid losing that code.** Rejected — nothing was using it, `git history`
  preserves `web/`'s implementation if it's ever wanted as a reference, and porting unused
  code is speculative work against a requirement that doesn't exist yet.
- **Deprecate `web/` in place (leave the code, stop maintaining it) rather than delete it.**
  Rejected — the repo owner's explicit read is that keeping unlaunched, unmaintained code
  around is itself the confusion this decision is meant to resolve (verbatim: "it would be
  better if we delete /web is causing to many confusions"), not a lesser version of the same
  problem.

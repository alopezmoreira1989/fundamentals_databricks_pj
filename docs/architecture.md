# Architecture

Two independent layers around one shared library.

```
web (Django)  ──imports──▶  fundamentals_pipeline  ◀──imports──  Databricks notebooks
   │                          (installable package,
   │                           single source of truth)
   └── DuckDB ──▶ published Parquet artifacts (GitHub Release)   ◀── published by the pipeline
```

## Layers

- **`fundamentals_pipeline/`** — the installable package and the project's **single source
  of truth**: the export/app schema contract and the pure-Python financial reference logic
  (`schemas`, `valuation`, `periods`, `backtest`, `splits`), plus the Databricks notebook
  stages (`NN__` dirs). Runs standalone on Databricks; no dependency on the web layer.
- **`web/`** (Django) — the **presentation + application layer only**: authentication,
  templates, REST API, and user data. It imports `fundamentals_pipeline` as a normal
  dependency and **never reimplements financial logic**.

## Rules (locked)

1. **Dependency direction is one-way:** `web → fundamentals_pipeline`. The pipeline never
   imports `web`; Databricks never imports `web`.
2. **No financial/business logic in Django.** All calculations, valuations, ratios,
   transformations, and the schema contract live in `fundamentals_pipeline`.
3. **No `sys.path` manipulation.** Every environment installs the package the same way
   (`pip install -e .`): Django, tests, scripts, Streamlit, and Databricks (via the
   `91__full_pipeline` session-dependencies cell).
4. **Django reads published Parquet artifacts via DuckDB** (`web/infrastructure/`), never
   runs analytical queries against Databricks during user requests.
5. **PostgreSQL stores only application data** (users, sessions, watchlists, favorites,
   history, preferences) — never financial data.
6. **Every access to persistent data goes through the repository tier.** Views must never
   query DuckDB or PostgreSQL directly, and neither must application services. The layering
   is strict and one-directional — each tier calls only the one below it:

   ```
   views  →  services  →  repositories  →  infrastructure (DuckDB / PostgreSQL)  →  fundamentals_pipeline
   ```

   - **views** (`apps/*/views.py`) — HTTP + presentation only; call **services**.
   - **services** (`services/`) — application/use-case orchestration; call **repositories**
     (and `fundamentals_pipeline` for business logic). Never touch a storage engine/ORM.
   - **repositories** (`repositories/`) — the *only* tier that reads/writes persistent data
     (the DuckDB engine, the artifact store, and — later — the Django ORM); return DTOs.
   - **infrastructure** (`infrastructure/`) — raw DuckDB/artifact + ORM access primitives.
   - **fundamentals_pipeline** — the installed package; all financial logic (rule 2).

   This keeps the presentation layer independent of storage details, so swapping DuckDB for
   another engine (or the artifact source) touches only `infrastructure/` + `repositories/`.

## Web layer layout

- `config/` — Django project (settings `base`/`dev`; `prod` added at deployment).
- `apps/` — `users`, `companies`, `screener`, `valuation`, `watchlists`, `favorites`,
  `history`, `api`. **Views/presentation + user-domain only** (top tier — call `services/`).
- `services/` — application/use-case orchestration (the tier views call). Calls
  `repositories/` for data and `fundamentals_pipeline` for business logic; no persistence.
- `repositories/` — domain read/write repositories → DTOs. The **only** tier that touches
  `infrastructure/` or the ORM. No business logic (delegates to `fundamentals_pipeline`).
- `infrastructure/` — DuckDB/PostgreSQL access: `storage` (fetch/validate/cache the Release
  artifacts) and `duckdb` (query engine over the cached parquet). No business logic.
- `templates/`, `static/`, `media/` — presentation assets (created as they gain content).

## Notes carried across phases

- **Custom user model:** introduced in the `users` app with `AUTH_USER_MODEL` set **before
  the first `migrate`** (Phase 4). Phase 1 runs no migration, so this stays trap-free.
- **Docker build context is the repo root**, so the web image can install the sibling
  `fundamentals_pipeline` package.
- **Test suites are separate:** the root `pytest` (`testpaths=["tests"]`) covers the
  pipeline library; the web layer gets its own `pytest-django` config when it has tests.

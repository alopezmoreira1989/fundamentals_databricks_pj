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
6. **Every access to persistent data goes through the repository tier** — the full contract
   is the **Data access architecture** section below (responsibilities, forbidden list, and
   the replaceable-storage goal). Views and services never query DuckDB/PostgreSQL directly.

## Data access architecture (locked)

Every access to persistent data goes through a repository — with one judgment-based
exception for trivial Django-ORM CRUD (see *When a repository earns its place*, below). The
dependency flow is strict and one-directional — each tier calls only the one below it:

```
views  →  services  →  repositories  →  DuckDB / PostgreSQL  →  fundamentals_pipeline (when business logic is required)
```

(In this repo the "DuckDB / PostgreSQL" tier is the `infrastructure/` package — the DuckDB
engine + artifact store today, the Django ORM later. Repositories are its only caller.)

### Responsibilities

- **Views** (`apps/*/views.py`) — handle HTTP requests; validate input; call services;
  return HTML or JSON. **Never access storage directly.**
- **Services** (`services/`) — coordinate application use cases; call `fundamentals_pipeline`
  when business logic is required; coordinate one or more repositories; contain application
  workflow only. **Never execute SQL.**
- **Repositories** (`repositories/`) — the *only* layer allowed to access analytical
  storage (DuckDB / Parquet / a future Databricks SQL backend) and the home for any
  non-trivial ORM access; hide storage implementation details; return domain objects / DTOs.
  **Never contain business rules or financial calculations.** (Trivial ORM CRUD may skip the
  repository — see *When a repository earns its place*.)
- **`fundamentals_pipeline`** — the single source of truth: owns all financial, valuation,
  screening, transformation, and business-rule logic.

### Forbidden

- **Views must never** execute SQL, query DuckDB, query PostgreSQL, read Parquet files, or
  contain business logic.
- **Services must never** execute raw SQL, or depend on Django models for business rules.
- **Repositories must never** implement financial formulas, perform valuations, or contain
  screening logic.

### Goal — the storage layer must be replaceable

Swapping the storage engine — e.g. **DuckDB → Databricks SQL / PostgreSQL / Snowflake /
BigQuery** — must require changes **only inside the repository tier** (and the
`infrastructure/` adapters it wraps). Views, services, and `fundamentals_pipeline` must not
change. This is the litmus test for whether a piece of code sits in the right tier: if a
storage swap would force an edit to a view or a service, that view/service is reaching past
its layer.

### When a repository earns its place (judgment over mechanism)

The repository pattern must add architectural value, not boilerplate. Introduce a repository
when it provides one or more of: **storage abstraction, complex queries, DTO mapping,
aggregation, multiple data sources, caching, or infrastructure isolation.**

- **Mandatory — always behind a repository:** DuckDB, Parquet artifacts, a future Databricks
  SQL backend, and any external/analytical storage. These *are* the storage-abstraction /
  replaceability cases the goal above protects, so the mandate is absolute regardless of how
  simple the query looks.
- **Exception — trivial Django-ORM CRUD** (e.g. `CustomUser`): simple create/read/update/
  delete on ORM-backed application models may use the ORM **directly from the service layer**
  when a repository would only duplicate the ORM API. Wrapping `User.objects.get(pk=...)` in
  a pass-through repository adds indirection, not value. Promote to a repository the moment
  the access earns one of the value bullets above — a non-trivial/composed query, DTO mapping
  across models, aggregation, caching, or hiding a second data source.

The exception is narrow: it covers ORM CRUD only. Analytical storage (DuckDB / Parquet /
Databricks) is **never** touched from a view or service — always through a repository.

### Read model & query strategy (locked)

DuckDB is a **read-only analytical store**. The read path obeys:

- **Repositories return immutable DTOs, not raw rows.** A repository maps query results to
  frozen dataclasses (or domain objects) — never hands a `dict`, tuple, `Row`, or DataFrame
  up to a service/view. **Views and services must not depend on SQL column names**; they
  see typed DTO attributes, so a column rename lives and dies inside the repository.
- **Repositories own all SQL. No SQL anywhere else.** Every query is **parameterized**
  (bound `?` placeholders — never string-formatted values). **No `SELECT *`** — list exactly
  the columns the DTO needs, so a schema change can't silently reshape a result.
- **Push work into DuckDB; don't over-fetch.** Filter, aggregate, and `LIMIT` in SQL so the
  parquet scan reads only the needed rows/columns (DuckDB does predicate + projection
  pushdown). Never load an entire parquet artifact into memory to filter it in Python.
  Avoid unnecessary copies; prefer streaming/lazy evaluation where a result set is large.
- **Services stay storage-agnostic.** A service coordinates repositories and
  `fundamentals_pipeline`; it does not know how (or that) DuckDB stores the data.

**Goal:** replacing DuckDB with another analytical backend (Databricks SQL, Postgres,
Snowflake, BigQuery) changes **only the repositories** (+ their `infrastructure/` adapters).
The DTOs they return, and every view/service/pipeline above them, remain untouched.

## Web layer layout

Two placement axes: **domain-specific** code lives inside its Django app; **cross-cutting**
code shared by several apps lives in the top-level tier packages.

- `config/` — Django project (settings `base`/`dev`; `prod` added at deployment).
- `apps/<name>/` — one app per domain (`users`, `companies`, `screener`, `valuation`,
  `watchlists`, `favorites`, `history`, `api`). Holds that app's `views.py`, `urls.py`, and
  app-specific `services.py`; plus `models.py` **only if the app owns PostgreSQL application
  data** (e.g. `users`, `favorites`). Purely analytical apps (`valuation`, `screener`) own no
  Postgres data and have no `models.py` — they read pipeline artifacts through a repository.
- `services/` — **cross-cutting** application services used by more than one app (single-app
  workflow stays in `apps/<name>/services.py`). Coordinate repositories + `fundamentals_
  pipeline`; no persistence, no raw SQL.
- `repositories/` — **shared** repositories → DTOs (the DuckDB/artifact reads reused across
  `companies`/`screener`/`valuation`). The only tier that touches analytical storage. No
  business logic (delegates to `fundamentals_pipeline`); trivial ORM CRUD may bypass it.
- `infrastructure/` — storage backends, **organized by backend so each is swappable in
  isolation**: `duckdb/` (query engine over the cached parquet) and `storage/` (fetch/
  validate/cache the Release Parquet/JSON artifacts) today; a `postgres/` / `databricks/`
  adapter is added only when a backend actually needs one — not pre-stubbed. No business logic.
- `templates/`, `static/`, `media/` — presentation assets (created as they gain content).

## Model conventions (locked)

- **UUID primary keys for every application model, from the start.** Application entities
  declare an explicit `id = models.UUIDField(primary_key=True, default=uuid.uuid4,
  editable=False)` — never `AutoField`/`BigAutoField`. UUID keys avoid a later pk migration
  and suit a distributed system (no central sequence; keys are safe to generate anywhere and
  to expose in URLs). `DEFAULT_AUTO_FIELD` stays `BigAutoField` because it governs only the
  framework's own tables (auth/admin/sessions/contenttypes), which are not our entities.
- **Custom user model (`apps/users/models.py`):** `User(AbstractUser)` — keeps username
  login and Django's permission/staff/superuser plumbing; adds a **UUID pk** and a
  **unique, required email**. A small `UserManager` enforces the email invariant on
  `create_user`/`create_superuser`. Intentionally minimal (YAGNI) — no profile fields yet.
  `AUTH_USER_MODEL = "users.User"` was set **before the first migration**, so Django's
  default User was never migrated. Users CRUD uses the ORM directly from the service layer
  (no `UserRepository` — see *When a repository earns its place*).

## Notes carried across phases

- **Docker build context is the repo root**, so the web image can install the sibling
  `fundamentals_pipeline` package.
- **Test suites are separate:** the root `pytest` (`testpaths=["tests"]`) covers the
  pipeline library; the web layer has its own `pytest-django` config (`web/pytest.ini`,
  `DJANGO_SETTINGS_MODULE=config.settings.test` → SQLite in-memory, no Postgres needed).

# Deploying the web layer to Render (free tier) + Aiven PostgreSQL

The Django `web/` app runs on Render as a single containerized web service, with its Postgres
database hosted separately on Aiven. Both have a real, permanent free tier that needs no credit
card — the combination this project targets for a zero-cost, recruiter-visible deployment.
(`fly.toml` / `docs/deploy-fly.md` remain in the repo as an alternative if you ever want Fly
instead; Render + Aiven is the primary path.)

| File | Purpose |
| --- | --- |
| `render.yaml` (repo root) | Render Blueprint: build, env vars, `/readyz` health check, free plan. At the repo root so the Docker build context is the repo root. |
| `web/docker/Dockerfile.prod` | Multi-stage production image: installs `fundamentals_pipeline` + `web/requirements/prod.txt` into a venv, bakes `collectstatic`, runs gunicorn as a non-root user. |
| `web/docker/entrypoint.prod.sh` | Optionally migrates (`RUN_MIGRATIONS_ON_BOOT=true`, Render-only — see **Migrations** below), warms the artifact cache (non-fatal), then execs gunicorn on `$PORT`. |

The app reads its analytical data from the published GitHub Release parquet artifacts (via
DuckDB, cached on the machine's local disk) and stores user/app data in **PostgreSQL** (Aiven).
No Databricks access at request time.

## Why Render + Aiven, not one platform

Render's own managed Postgres is free for 90 days only, then it's deleted. Aiven's free
PostgreSQL plan (1 GB storage/RAM, 1 CPU) has **no time limit and needs no card** — pairing it
with Render's free web service avoids a forced migration 90 days in. The trade-off: Aiven and
Render live in different clouds, so there's one extra network hop between the app and its
database (both still terminate TLS; latency is small compared to the DuckDB artifact queries
anyway).

## Prerequisites

- A Render account (https://render.com) — no card required for the free plan.
- An Aiven account (https://aiven.io) — no card required for the free plan.
- Run Render deploys from the repo root's `render.yaml` (Blueprint), or configure the same
  settings by hand in the Render dashboard.

## One-time setup: Aiven for PostgreSQL

1. Sign up / log in at https://aiven.io, create (or use) a project.
2. **Services -> Create service -> PostgreSQL**. Free plan: you cannot pick a cloud/region — that's
   expected, only the Free plan is restricted this way.
3. Name the service (e.g. `fundamentals-db`) and create it. Status goes `Rebuilding` -> `Running`
   (a minute or two).
4. Open the service, go to **Overview -> Quick connect**, and copy the connection string. It looks
   like:
   ```
   postgres://avnadmin:<password>@<host>.aivencloud.com:<port>/defaultdb?sslmode=require
   ```
   Keep `?sslmode=require` — Aiven requires TLS, and `django-environ`'s `env.db()` passes that
   query parameter straight through to the `postgres` backend, so no code change is needed for SSL.

## One-time setup: Render

1. Push `render.yaml` to the repo (already done in this branch).
2. In the Render dashboard: **New + -> Blueprint**, connect this GitHub repo, and let Render read
   `render.yaml`. It proposes one service (`fundamentals-web`, free plan, Docker).
3. Before the first deploy, fill in the env vars marked `sync: false` in `render.yaml` (Render
   dashboard -> service -> **Environment**):

   | Key | Value |
   | --- | --- |
   | `DJANGO_SECRET_KEY` | `python -c "import secrets;print(secrets.token_urlsafe(64))"` |
   | `DJANGO_ALLOWED_HOSTS` | `fundamentals-web.onrender.com` (your actual `*.onrender.com` host, plus any custom domain) |
   | `DJANGO_CSRF_TRUSTED_ORIGINS` | `https://fundamentals-web.onrender.com` (scheme-qualified; add custom domains too) |
   | `DATABASE_URL` | the Aiven connection string from above (with `?sslmode=require`) |
   | `SENTRY_DSN` | optional; leave empty to disable error tracking |

   `DJANGO_ALLOWED_HOSTS` **must** include the host Render serves the app under, or Django rejects
   every request (and the health check) with a 400. `DJANGO_CSRF_TRUSTED_ORIGINS` must list the
   same host(s) **scheme-qualified** (`https://…`) or login/signup form POSTs fail CSRF checks
   with a 403.
4. Deploy. Render builds `web/docker/Dockerfile.prod` with the repo root as context.

The public REST API is rate-limited per client IP (`API_THROTTLE_ANON`, default `120/min`);
`DJANGO_NUM_PROXIES` (default `1`) tells Django how many proxy hops to trust so the throttle keys
on the real client IP rather than Render's proxy. The limit is **per gunicorn worker** (in-process
counter) — for a hard global limit, rate-limit at the edge instead.

## Migrations — why they run at boot here (not in Fly's release-machine style)

Render's **pre-deploy command** (the closest equivalent to Fly's `release_command` — a one-off
step that runs once before the new version takes traffic) is a **paid-plan-only** feature; the
free plan doesn't have it. Running `migrate` unconditionally in the entrypoint (on every container
boot) would be unsafe on a platform that can scale to several instances at once — concurrent
migrations can race. But Render's **free plan never runs more than one instance** of a service, so
it's safe there.

`render.yaml` sets `RUN_MIGRATIONS_ON_BOOT=true`, which `web/docker/entrypoint.prod.sh` checks
before exec'ing gunicorn. Fly's `fly.toml` never sets this flag, so its deploys keep using the
release-machine (`release_command`) path unchanged. Idempotent either way — safe to re-run.

## Deploy

Render deploys automatically on every push to the connected branch once the Blueprint is set up
(no separate deploy command, unlike `flyctl deploy`). To trigger one manually: dashboard -> service
-> **Manual Deploy -> Deploy latest commit**.

What happens:
1. Render builds `web/docker/Dockerfile.prod` with the repo root as context.
2. The app machine boots: `entrypoint.prod.sh` runs `migrate --noinput` (see above), warms the
   artifact cache, then starts gunicorn bound to `$PORT` (Render injects this — never hardcode a
   port).
3. Render routes traffic to the instance only once `GET /readyz` returns 200 (DB reachable + core
   artifacts cached).

## Verify

```sh
curl https://fundamentals-web.onrender.com/healthz   # {"status":"ok"}
curl https://fundamentals-web.onrender.com/readyz    # {"status":"ready", "checks":[...], "cache_metrics":{...}}
```

Or open the dashboard -> service -> **Logs** for structured JSON access/error logs, one line per
request (`config.middleware.RequestLogMiddleware`).

To sanity-check the Aiven connection directly (e.g. from your own machine, useful when debugging a
`/readyz` DB-check failure):

```sh
psql "postgres://avnadmin:<password>@<host>.aivencloud.com:<port>/defaultdb?sslmode=require" -c "select 1"
```

## Operations

- **Cold starts**: the free plan **sleeps the instance after 15 minutes of inactivity** and takes
  ~30-50s to wake on the next request. Acceptable for a portfolio project; not for anything
  latency-sensitive. There is no free "always-on" option on Render — an external uptime pinger
  (e.g. a free UptimeRobot/Better Uptime monitor hitting `/healthz` every 10 min) keeps it warm if
  you want to avoid the cold-start hit, at the cost of the instance never truly idling.
- **Artifact freshness** is automatic: cached artifacts revalidate in the background once older
  than `ARTIFACTS_TTL` (stale-while-revalidate), so a fresh publish is picked up without a
  redeploy.
- **Scaling off the free plan**: a paid Render plan re-enables `preDeployCommand` (you'd then move
  the migration out of `entrypoint.prod.sh` and unset `RUN_MIGRATIONS_ON_BOOT`, mirroring the Fly
  setup) and lets you run more than once instance safely.
- **Rollback**: dashboard -> service -> **Events/Deploys** -> pick a previous successful deploy ->
  **Redeploy**.

## Gotchas

- **Secrets vs env**: `render.yaml`'s plain `envVars` entries are committed and public — only
  non-secret config goes there. Everything sensitive is `sync: false` and set in the dashboard.
- **Health check over HTTP**: like Fly, Render's internal health check hits the container in plain
  HTTP. `prod.py` already exempts `/healthz` and `/readyz` from `SECURE_SSL_REDIRECT`
  (`SECURE_REDIRECT_EXEMPT`) so they aren't 301'd to https and marked unhealthy.
- **The artifact cache is ephemeral** (instance-local, re-warmed on boot) — intentional, it's
  derivable from the Release.
- **Aiven free-plan connection cap**: the free Postgres plan has a modest max-connections limit.
  `DJANGO_CONN_MAX_AGE=60` (already the default) reuses connections across requests instead of
  reconnecting each time, which keeps a single free-tier Render instance well under that cap
  without needing a separate pooler (e.g. PgBouncer) at this scale.
- **`DATABASE_URL` must keep `?sslmode=require`** — Aiven refuses unencrypted connections; dropping
  the query string from the copied connection string breaks the DB connection at boot.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `/readyz` returns 503, `checks` shows a database failure | `DATABASE_URL` wrong/missing, or Aiven service not `Running` yet | Re-copy the connection string from Aiven's Quick Connect page; confirm the Aiven service status |
| Every request 400s | Render's `*.onrender.com` host (or custom domain) not in `DJANGO_ALLOWED_HOSTS` | Add the exact host Render serves under |
| Login/signup POST returns 403 | Host missing from `DJANGO_CSRF_TRUSTED_ORIGINS`, or missing the `https://` scheme | Add the scheme-qualified origin |
| First request after a deploy is very slow / times out | Free-plan cold start (instance was asleep) or boot-time migrate + artifact-cache warm | Expected on free tier; retry, or add an uptime pinger (see Operations) |
| New migration isn't applied after deploy | `RUN_MIGRATIONS_ON_BOOT` unset/false | Confirm it's `"true"` in the service's Environment tab (already set by `render.yaml` for a fresh Blueprint deploy) |
| SSL error connecting to Postgres | `?sslmode=require` dropped from `DATABASE_URL` | Restore it — see Gotchas above |
| Local `docker compose` dev stack broken | Unrelated to Render — see `web/docker/docker-compose.yml`, uses its own local Postgres, no Aiven/Render env needed | `docker compose -f web/docker/docker-compose.yml up --build` |

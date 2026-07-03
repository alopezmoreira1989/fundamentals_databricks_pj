# Architecture Decision Records

This log records the significant, hard-to-reverse decisions behind the codebase — the *why* and
the alternatives behind the "locked" rules summarized in [`../architecture.md`](../architecture.md).
`architecture.md` is the always-current *what*; the ADRs are the durable *why*. See
[ADR-0001](0001-record-architecture-decisions.md) for the rationale of keeping this log at all.

## When to write an ADR

Write one for a decision that is **significant and hard to reverse** — it constrains the layering,
picks a storage/runtime approach, sets a data or model convention, or otherwise shapes how future
code must be written. Routine choices don't need an ADR; this is not a change log.

## Conventions

- **Filename:** `NNNN-kebab-title.md`, numbered monotonically from `0001`. (ADRs are docs, not
  pipeline stages, so they use this ADR numbering — not the repo's `NN__` stage-naming rule.)
- **One decision per record.** Start from [`0000-template.md`](0000-template.md).
- **Immutable once Accepted.** To change a decision, add a *new* ADR that supersedes the old one,
  and set the old record's **Status** to `Superseded by ADR-XXXX` (don't rewrite its substance).
- **Retroactive ADRs** (0002–0007 here) document decisions already in force; their **Date** is the
  date the record was written, and they note that the decision predates the record.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions in ADRs | Accepted |
| [0002](0002-django-presentation-and-application-layer.md) | Django as the presentation + application layer (replacing the decoupled Next.js frontend) | Accepted |
| [0003](0003-strict-layering-with-repository-tier.md) | Strict one-directional layering with a repository tier | Accepted |
| [0004](0004-artifact-fed-read-model-via-duckdb.md) | Artifact-fed read model via DuckDB — no Databricks at request time | Accepted |
| [0005](0005-uuid-primary-keys-for-application-models.md) | UUID primary keys for application models | Accepted |
| [0006](0006-repositories-mandatory-for-analytical-storage.md) | Repositories mandatory for analytical storage; trivial ORM CRUD may skip them | Accepted |
| [0007](0007-custom-user-model-from-first-migration.md) | Custom user model, set before the first migration | Accepted |

_Template: [`0000-template.md`](0000-template.md)._

# ADR-0001: Record architecture decisions in ADRs

- **Status:** Accepted
- **Date:** 2026-07-03
- **Deciders:** repo owner

## Context

The project's significant architecture decisions — the strict layering, the artifact-fed read
model, UUID primary keys, the storage-replaceability goal — were until now captured only as
"locked" rules in [`../architecture.md`](../architecture.md) and as prose in `CLAUDE.md`. Those
documents state *what* the rules are, but not the *why*, the alternatives that were weighed, or
the trade-offs accepted. As the codebase and its history grow, that reasoning is easy to lose,
and a future change can silently violate a decision without anyone realizing it was deliberate.

## Decision

We will keep a log of **Architecture Decision Records (ADRs)** under `docs/adr/`, one file per
significant decision, using the lightweight template in
[`0000-template.md`](0000-template.md) (Nygard-style: Context / Decision / Consequences /
Alternatives). ADRs are numbered monotonically and are immutable once Accepted — a decision is
changed by adding a new ADR that **supersedes** the old one, never by editing the old record.
`architecture.md` remains the always-current summary and links to the ADRs for the reasoning.

## Consequences

- The reasoning behind each "locked" rule is now discoverable and reviewable; a PR that would
  break an invariant can be pointed at the ADR that established it.
- A small, ongoing discipline: a genuinely significant or hard-to-reverse decision warrants an
  ADR. Routine choices do not — this is not a change log.
- Two sources must not drift: `architecture.md` (current state) and the ADR log (the decisions
  behind it). The ADRs own the *why*; `architecture.md` owns the *what*.

## Alternatives considered

- **Only `architecture.md`.** Rejected: a single evolving document loses the history and the
  alternatives — you see the current rule, never why it beat the options, or when it changed.
- **Git history / commit messages.** Rejected: decisions are diffuse across many commits and not
  addressable; you cannot cite "the decision" or see its status at a glance.
- **A heavier RFC process.** Rejected as overkill for a solo project; ADRs are the minimal
  durable format that still captures context and trade-offs.

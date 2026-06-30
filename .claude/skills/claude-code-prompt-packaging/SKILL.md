---
name: claude-code-prompt-packaging
description: Package a multi-file change for this repo as a self-contained Claude Code prompt that explores before writing, does no production writes during diagnosis, stays idempotent, delivers complete files instead of diffs, keeps pipeline and frontend changes in separate prompts, and follows the repo workflow of working on a branch then merging to main after validation. Use when asked to write a Claude Code prompt, package a change, prep a prompt for a multi-file fix, or hand off work to another agent or session. This produces a prompt; it does not itself modify code.
metadata:
  author: Alejandro López Moreira
  version: 1.0.0
---

# claude-code-prompt-packaging

## CRITICAL

- **Explore and plan before any write.** The prompt must instruct the agent to read the named files and ground its plan in real code, then pause/confirm before editing stateful pipeline files (they do Delta writes and SEC/yfinance calls).
- **No production writes during diagnosis.** Reads, dry runs, and read-only validators only until the change is agreed. Never have a diagnosis step mutate Delta tables, publish a Release, or run ingestion.
- **Idempotent and re-runnable.** Every edit instruction must be safe to apply twice (anchored edits, "create if absent", re-run-safe on the latest `scraped_at`).
- **Complete files, not diffs/fragments**, so the receiving agent has unambiguous targets.
- **Separate pipeline and frontend prompts.** Pipeline (`fundamentals_pipeline/00..50, 90`) and Streamlit (`60__frontends/61__streamlit`) have different runtimes, conventions, and blast radius — package them as distinct prompts.
- **Workflow: branch, validate, then merge to `main`.** GitHub `main` is the single source of truth; the Databricks Repo is a read-only mirror synced from `main`. Work on a feature branch (e.g. `dev_alm`), validate, then merge to `main` via the normal flow. **Do not force-push to `main`** — it triggers the sync and is the production source.

## Anatomy of a good prompt

1. **Objective** — one paragraph: what changes and why, in domain terms (how it affects the metrics).
2. **Hard constraints** — explore-first; no prod writes during diagnosis; idempotent; complete files; branch-then-merge; what must NOT change.
3. **Grounding** — the exact files to read first (real paths under `fundamentals_pipeline/...`), and "where my notes disagree with the code, the code wins — flag it".
4. **Phase 0/1 (no writes)** — inventory + a plan to present and PAUSE on.
5. **Phase 2** — apply the approved plan; deliver complete files; keep edits idempotent.
6. **Phase 3 — validate** — which skills/validators to run: [[pipeline-preflight]] before a run; [[financials-invariants]] / [[validate-quarters]] / [[validate-concept-hierarchy]] after; [[external-benchmark-validation]] for a reference cross-check.
7. **Deliverable** — a summary table of what changed and the validation result, for human review before merge.

## Repo-specific reminders to bake in

- Flag Databricks-only assumptions (`%run`, `dbutils`, `spark`, three-part names) so they don't surprise a local Databricks Connect run.
- Keep the `NN__name.py` naming and the bilingual Spanish style ([[databricks-notebook]]).
- Don't add `CREATE CATALOG` / `CREATE SCHEMA`; schemas are pre-provisioned.
- Serverless: `localCheckpoint(eager=True)`, not `.cache()`/`.persist()`.
- Don't edit the synced Repo under `/Workspace/Shared/...`; edit in Git and let the sync mirror it.

## What NOT to do

- Don't tell the receiving agent to force-push `main` or to bypass the branch-then-merge flow.
- Don't bundle pipeline and frontend edits into one prompt.
- Don't hand over diffs/fragments, or instructions that aren't re-runnable.
- Don't let any diagnosis step write to production (Delta, GitHub Release, ingestion APIs).
- Don't skip the explore/plan/pause phase for stateful notebooks.

## Related

- [[databricks-notebook]] — conventions for any pipeline file the prompt creates.
- [[streamlit-dashboard]] — conventions for any frontend prompt.
- [[pipeline-preflight]] / [[financials-invariants]] / [[validate-quarters]] — the validation steps to require in Phase 3.

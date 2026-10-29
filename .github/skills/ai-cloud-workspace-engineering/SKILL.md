---
name: ai-cloud-workspace-engineering
description: Use when auditing or changing the agent loop, sandbox execution, GitHub operations, authentication, BYOK keys, persistence, previews, UI, or deployment in AI Cloud Workspace.
---

# AI Cloud Workspace Engineering Skill

## Core rule
Trace the complete path from user action to an authoritative persisted outcome. A button, helper, prompt, tool schema, database function or README claim is not proof that a feature works.

## 1. Establish baseline
- Inspect repository, branch, commit, working tree, history, dependencies, configuration, tests and CI.
- Preserve user changes. Never reset or overwrite them.
- Do not print secrets during inspection.
- Read relevant code and call sites before editing.
- Record baseline checks and exact outputs.

## 2. Trace the feature
Map: UI control → callback → identity/authorization → service → agent tool → sandbox/provider → persistence → status/events → UI result → tests.
Record missing links, duplicate implementations, silent exceptions, stale docs, unbounded work and absent failure states.

## 3. Security-first checklist
- OAuth: random per-attempt PKCE, state, callback allowlist, expiry and replay protection.
- Credentials: least privilege, short-lived where possible, no tokens in Git remotes, commands, logs, tool outputs or model context.
- Encryption: fail closed; never silently store plaintext or treat failed decryption as plaintext.
- Tenancy: verify Supabase RLS and test cross-user access.
- Sandbox: untrusted code, constrained network, CPU/memory/disk/time/output budgets, cancellation, isolation and orphan cleanup.
- Paths: canonicalize and confine reads/writes; reject traversal and escaping symlinks.
- Shell/Git: no interpolation of untrusted values; inspect diffs/staged files; exclude secrets before commit.
- UI: escape model, terminal, repository, filename and chat content; avoid unsafe HTML.
- Preview: explicit exposure, expiry, stop/revoke and cleanup.
- Privacy: redact secrets and minimize stored code/output/personal data.

## 4. Truthful agent outcomes
- Validate tool names/arguments; handle malformed JSON and provider errors.
- Distinguish transport success from command exit code.
- A nonzero exit is not a passing test.
- Step-limit exhaustion is incomplete, not successful.
- Distinguish completed, failed, incomplete, cancelled, awaiting approval and needs-user-action.
- Capture step timing, outcomes, exit codes, retries and sanitized errors.
- Never claim a commit or PR exists until GitHub confirms it.

## 5. Durable execution
- Persist a run before dispatch.
- Do not run long agent loops solely inside a Streamlit interaction.
- Use a durable worker/job service with bounded concurrency, deadlines, idempotency, heartbeats, cancellation, restart recovery and orphan cleanup.
- Reconcile remote Git state after ambiguous push/PR timeouts.
- Verify run history after tab closure, refresh and process restart.
- A database table alone is not a durable queue.

## 6. Implement incrementally
- Fix confirmed root causes, not symptoms.
- Add focused regression tests with each fix.
- Preserve interfaces unless changes are necessary and documented.
- Document migrations and compatibility implications.
- Do not deploy, force-push, rotate credentials or make destructive data changes without authorization.
- Avoid large framework adoption or UI rewrites without evidence.

## 7. Verify normal, failure and recovery paths
Test OAuth mismatch/expiry/replay; encryption missing/invalid key; token redaction; cross-user access/RLS; malformed tools; nonzero exits; timeouts; provider errors; step limit; cancellation; path traversal; unsafe HTML; resource/network restrictions; sandbox crash/cleanup/restart; Git command injection; secret files in diffs; push-success/PR-failure; preview expiry/revocation; Supabase outage; backup/restore; and UI refresh consistency.

Run available checks and report actual outcomes. Label tests skipped or blocked and explain why.

## 8. Report every phase
Include changed files and rationale; confirmed defects fixed with evidence; commands/tests actually run and results; tests not run and why; configuration/migration/deployment implications; remaining risks; and the exact next action.

## Completion rule
Do not say a feature is done until connected end to end, failures are explicit, state is truthful, security boundaries are tested, regression checks pass, and limitations are disclosed. Do not call the product production-ready without representative runtime evidence.

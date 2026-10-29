# AI Cloud Workspace — Phase 1 Repository Audit

Repository: https://github.com/laxjovial/ai-cloud-workspace
Branch audited: main
Commit audited: 871e7e2803fc185469900e009c44406c16211358
Date: 2026-10-29
Method: source and documentation review through GitHub. The application, tests, and live integrations were not executed.

## Executive assessment

AI Cloud Workspace is an early-stage Streamlit application intended to provide a cloud-hosted autonomous coding workspace. It combines Supabase authentication and storage, LiteLLM model calls, E2B sandboxes, GitHub credentials/repositories, terminal execution, file editing, dev-server previews, and pull-request creation.

Assessment: meaningful prototype functionality, but not yet demonstrated production-ready. The inspected default-branch tree contains app.py, agent.py, sandbox.py, db.py, requirements and Streamlit configuration. It contains no visible test suite, CI workflow, schema/migration files, deployment manifests, or separate worker/service layer. GitHub Actions returned zero workflow runs. No tests were executed, so no claim about test pass/fail is made.

## Product definition

An AI-powered cloud development environment and autonomous software-engineering workspace. Users connect a GitHub repository, select a model using their own provider key, ask an agent to inspect/edit/test code in a cloud Linux sandbox, launch a live preview, and optionally create a Git branch/commit/pull request.

Core promise: delegate a bounded engineering task and later inspect the exact changes, actual test evidence, run history, and truthful outcome.

## Repository inventory

| Path | Observed responsibility | Audit focus |
|---|---|---|
| app.py | Large Streamlit UI, auth/OAuth callback, navigation, account/key management, chat and agent invocation | session state, OAuth, UI-to-service coupling, XSS, task lifetime |
| agent.py | LiteLLM tool-calling loop: terminal, server, file and PR tools | validation, loop termination, prompt injection, truthful outcomes, budgets |
| sandbox.py | E2B lifecycle, Git clone, command execution, file I/O, preview URL, Git push and PR creation | secrets, shell injection, isolation, path confinement, cleanup |
| db.py | Supabase auth/data, local fallback store, key/token encryption, sessions/messages | tenancy, OAuth PKCE, encryption fallback, consistency |
| requirements.txt | Runtime dependency declarations | reproducible install, dependency scanning |
| .streamlit/secrets.toml.example | Placeholder configuration | safe setup and required-secret validation |
| .gitignore | Excludes local secret/user stores and environments | tracked secrets/user data |

Latest visible commit: 871e7e2, “fix: resolve sandbox API compatibility, sync navigation state, add reasoning effort controls, and persistent key alerts.”

## Confirmed code-level findings

### P0 — Static OAuth PKCE verifier
Evidence: db.py defines FIXED_PKCE_VERIFIER as a hard-coded deterministic constant and derives the challenge from it. PKCE should use an unpredictable verifier tied to one authorization attempt/session. A fixed verifier defeats that property and may enable code interception or replay risks depending on the flow.

Fix: generate a cryptographically random verifier per attempt, bind it to the initiating session, validate OAuth state, use exact redirect URI allowlists, expire it, and test replay/mismatch behavior.

### P0 — GitHub token embedded in clone URL and shell command
Evidence: sandbox.py builds an authenticated Git URL containing the GitHub token, then interpolates it into a shell command. The token may persist in .git/config as the origin URL and may be exposed through diagnostics or later agent commands inspecting remotes.

Fix: never place tokens in command strings or Git remotes. Use a short-lived credential helper or scoped ephemeral credential mechanism, scrub credentials after clone, redact outputs, and prefer fine-grained/short-lived credentials.

### P0 — Encryption silently falls back to plaintext
Evidence: db.py returns plaintext from encrypt_secret when ENCRYPTION_KEY is missing/invalid or encryption fails. decrypt_secret returns the stored value when decryption fails, treating it as legacy plaintext. API keys and GitHub tokens can therefore be stored unencrypted without explicit failure.

Fix: fail closed for new writes. Create an explicit audited migration for legacy plaintext, distinguish ciphertext from plaintext, and never silently treat failed decryption as plaintext.

### P1 — Arbitrary model-directed shell execution
Evidence: agent.py exposes a terminal tool accepting arbitrary shell commands and calls sandbox.run_command. This is a core feature, not automatically a vulnerability; the risk depends on isolation, credentials, network policy, resource limits and tools exposed.

Fix: treat executed code as hostile. Add command deadlines, output caps, per-task isolation, network egress restrictions, resource limits, scoped credentials, cancellation, and approval gates for consequential Git/external actions.

### P1 — Git branch and commit values interpolated into shell
Evidence: sandbox.py interpolates branch_name and commit_message into a shell script joined with &&. Model-supplied shell metacharacters can change command execution.

Fix: use argument-safe process APIs and strict branch validation; inspect diff/staged files before commit; avoid unrestricted git add . without secret and artifact checks.

### P1 — Agent reports success after reaching step limit
Evidence: agent.py returns success=True with a “Reached maximum steps limit” message when max_steps is exhausted without a final answer. Reaching the limit is not proof of completion.

Fix: report incomplete/limit-reached and distinguish completed, failed, cancelled, incomplete and awaiting-approval states.

### P1 — Command transport success conflated with command success
Evidence: sandbox.py returns success=True for a command result even if exit_code is nonzero. The exit code is present, but consumers may misinterpret the boolean.

Fix: separate transport success from process exit status; command success requires exit code zero.

### P1 — Local and Supabase stores can silently diverge
Evidence: db.py writes to local user data and Supabase in parallel and often catches exceptions, falls back, or returns success even if a remote write failed. This affects keys, tokens, sessions and messages.

Fix: define one authoritative store per deployment mode; surface persistence failures; use explicit reconciliation/transactional patterns for dual writes and test outage recovery.

### P1 — Full API keys are displayed in the UI
Evidence: app.py includes a “Copy [provider] Key” expander that renders stored keys in st.code. This deliberately exposes the secret in the logged-in browser session and clipboard.

Fix: prefer masked display and deliberate reveal with confirmation/re-authentication; ensure keys never enter chat context or logs; provide rotation/revocation and least-privilege guidance.

### P1 — Public preview exposure lacks demonstrated controls
Evidence: start_dev_server launches a process and returns a public HTTPS host from the sandbox. The preview may expose application data or debug routes.

Fix: make exposure explicit and time-bounded, add stop/revoke controls, restrict ports/lifetime, and use authenticated preview access where possible.

### P1 — OAuth redirect is hard-coded to localhost
Evidence: app.py sets redirect_url to http://localhost:8501. This does not fit hosted deployments unless deliberately configured that way.

Fix: derive callback URLs from validated deployment config, require HTTPS outside local development, allowlist callbacks, and test each environment.

### P1 — Unsafe HTML rendering needs systematic review
Evidence: app.py uses unsafe_allow_html=True and inserts dynamic values in HTML contexts, including terminal output. This does not alone prove exploitable XSS, but dynamic model output, logs, filenames, repository names and stored messages require escaping/sanitization.

Fix: remove unsafe HTML where possible; escape untrusted values; test malicious model output, terminal output, filenames and chat history.

### P2 — Path confinement is not proven
Evidence: WorkspaceSandbox._resolve_path maps paths to the workspace but does not normalize and assert the resolved path remains within the workspace root. Traversal via ../ may escape the intended directory depending on filesystem behavior.

Fix: canonicalize paths, reject traversal and escaping symlinks, and verify final resolved paths are inside the workspace root.

### P2 — Resource budgets and timeouts are not apparent
Evidence: the agent loop has a step limit, but the inspected core loop does not show explicit command deadlines, total wall-clock budget, output cap, spend ceiling or cancellation.

Fix: add separate limits for model/tool calls, wall time, output bytes, sandbox CPU/memory/disk, network and spend.

## Architecture gaps to verify or address

1. No durable execution job model is visible. Agent execution occurs inline during a Streamlit interaction. Tab closure does not necessarily stop server-side Python immediately, but no independent durable worker, restart recovery or resume-from-checkpoint protocol is demonstrated.
2. No first-class run lifecycle is visible: run IDs, step events, cancellation, checkpoint history and orphan-sandbox recovery.
3. No tests or CI are visible in the inspected tree.
4. Dependency ranges are broad and not locked.
5. No schema/migration files are visible. Document Supabase schema, RLS, migrations, backup/restore and retention/deletion.
6. README claims Supabase RLS, but policies are not verifiable from this repository. Client-side user_id filters are not substitutes for server-enforced RLS.
7. Sandbox close() exists, but unconditional cleanup in all UI failure paths and cleanup after process crashes need verification.
8. PR creation does not demonstrate diff review, secret scanning, test gating or recovery when push succeeds but PR creation fails.
9. Conversation persistence catches errors; UI must not imply durable save when persistence is unconfirmed.
10. Test malformed tool JSON, unknown tools, multiple tool calls, provider rate limits, context growth, unsupported model parameters and API failures.

## Competitive research and references

| Project | Link | What to learn |
|---|---|---|
| OpenHands | https://github.com/OpenHands/OpenHands | Separate UI, Agent Server/SDK, workspaces, event stream and automation |
| OpenHands Software Agent SDK | https://github.com/OpenHands/software-agent-sdk | Typed tools, agent/server/workspace abstractions and events |
| E2B | https://github.com/e2b-dev/E2B | Sandbox lifecycle, process/filesystem execution and isolation boundaries |
| Coder | https://github.com/coder/coder | Managed workspace lifecycle, identity, policy, secret injection and cleanup |
| SWE-agent | https://github.com/SWE-agent/SWE-agent | Research-backed repository task workflows and evaluation |
| mini-SWE-agent | https://github.com/SWE-agent/mini-swe-agent | Simple coding loop, reproducible tasks and measured outcomes |
| Daytona | https://github.com/daytonaio/daytona | Useful historical sandbox patterns; repo states core development moved private in June 2026 and public repo is no longer maintained |
| Cloudflare sandbox coding-agent examples | https://developers.cloudflare.com/sandbox/coding-agents/codex/ | Isolated agent execution and constrained network/credential access |

Additional references:
- OpenHands architecture: https://github.com/OpenHands/OpenHands/blob/main/docs/architecture.md
- Coder overview: https://coder.com/docs/about
- OpenAI discussion of sandboxing, approvals and telemetry: https://openai.com/index/running-codex-safely/

Competitive conclusion: AI Cloud Workspace's differentiator is the combination of Streamlit simplicity, BYOK, multiple GitHub accounts, E2B execution and mobile previews. Mature projects go further in separating orchestration from UI, defining execution lifecycle APIs, controlling sandbox boundaries, testing outcomes and operating durable workspaces. Do not recreate all of OpenHands or Coder inside this small repository. Adopt principles incrementally and keep Streamlit if it serves the product.

## Prioritized roadmap

### Phase A — Security and correctness
- Replace fixed OAuth PKCE with per-attempt random verifier and state.
- Stop storing tokens in Git remotes or shell command strings.
- Fail closed if encryption is unavailable.
- Remove shell interpolation from Git operations.
- Validate tool calls and report step-limit exhaustion as incomplete.
- Separate command transport success from exit-code success.
- Review dynamic HTML and secret display.
- Verify Supabase RLS and cross-user isolation.
- Add regression tests for each issue.

### Phase B — Execution lifecycle and isolation
- Add durable run/task records with owner, repository/ref, status, progress, step events, sandbox ID, budgets, result and failure reason.
- Move long-running agent work behind a worker/job service; do not depend on an active Streamlit interaction.
- Add bounded concurrency, deadlines, idempotent start, cancellation, heartbeat, crash recovery and orphan cleanup.
- Add sandbox network/secret/resource policies and safe preview lifecycle.
- Prove run history survives process restart.

### Phase C — Persistence and operations
- Resolve local/Supabase consistency; stop reporting success after failed writes.
- Document/version schema, RLS, migrations, backups, restore, retention and deletion.
- Add structured logs with run/step IDs and redaction.
- Add dependency lock, CI, lint/type checks, tests and security scans.
- Document clean install and deployment.

### Phase D — Agent quality and product integrity
- Add typed tool schemas, budgets, bounded context and explicit stop reasons.
- Record each step and command result; final response must cite changed files and actual test output.
- Inspect diffs and staged files before commit; block secrets from commits.
- Require explicit approval for push/PR creation unless a narrow user policy authorizes it.
- Add coding-task evaluations and report success, regressions, failures, cost and time.
- Audit every UI control against real behavior.

### Phase E — Product improvements after reliability
- Resumable workspace sessions instead of always starting a fresh ephemeral sandbox.
- Concurrent tasks and durable run history.
- Diff viewer, test summaries, changed-file summary and preview management.
- Workspace templates/cached environments and safe dependency installation.
- Fine-grained GitHub permissions, token revocation and account isolation.
- Optional automation triggers after task lifecycle and security boundaries exist.

## Critical journey acceptance criteria

1. OAuth state and PKCE are per-attempt, expiring and replay-resistant.
2. GitHub tokens do not appear in logs, remotes, terminal output, model context or UI by default.
3. Starting a run creates a durable record before dispatch.
4. The agent reports exact repository and ref.
5. Every tool call has validated arguments, limits, timing and outcome.
6. Nonzero test exits are failures; step-limit exhaustion is incomplete.
7. Tab closure, Streamlit rerun and process restart do not corrupt task history.
8. Cancellation stops work and cleans up or records sandbox state.
9. Public previews are explicit, time-bounded and revocable.
10. Diff and staged files are checked for secrets before PR creation.
11. Push and PR creation are separate states; partial failure is represented accurately.
12. Dashboard state reflects persisted truth after refresh.
13. Cross-user tests prevent access to other users' sessions, keys, repos and logs.
14. Clean install, tests, lint/type checks, scans and representative sandbox workflows pass; skipped checks are disclosed.

## Limitations

This is source review only. The app and tests were not run; no live Supabase, GitHub, E2B or model-provider integration was exercised; no penetration test or dependency scan was performed; hosted RLS policies and deployment settings remain unverified. Recheck findings as each fix is implemented.

## Immediate next action

Implement Phase A with focused regression tests. Do not start by replacing Streamlit, E2B, Supabase or LiteLLM. First fix OAuth and credential risks, make tool outcomes truthful, and add tests/CI. Then design durable execution and prove restart recovery before advertising reliable background operation.

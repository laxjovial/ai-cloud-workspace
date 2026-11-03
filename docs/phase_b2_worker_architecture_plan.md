# Phase B.2 — Autonomous Worker Engine: Consolidated Architecture Specification & Engineering Contract

**Repository:** `laxjovial/ai-cloud-workspace`  
**Working Branch:** `feat/phase-b-durable-execution`  
**Specification Revision:** 2.2 (Final Pre-Implementation Engineering Contract)  
**Baseline (`main`):** `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)  
**Scope Boundary:** Phase B.2 covers only the background worker engine, process supervisor, heartbeat/fencing watchdog, crash-resilient sandbox registry/reconciler, and associated tests. It introduces **no Streamlit changes (`app.py` untouched)** and **no Phase C work**.

---

## 1. Process Supervision, Process Ownership, & Supervisor Finalization Authority

```mermaid
flowchart TD
    subgraph Host ["Supervisor Process (worker.py supervisor)"]
        SP["WorkerSupervisor (Main Daemon)"]
        PM["Process Pool Manager (concurrency=N)"]
        RC["Reaper & Sandbox Reconciler (10s interval)"]
        SP --> PM
        SP --> RC
    end

    subgraph Workers ["Spawned Worker Child Processes"]
        W1["Worker Child 1 (Instance: worker_inst_uuid_1, PID: 101)"]
        W2["Worker Child 2 (Instance: worker_inst_uuid_2, PID: 102)"]
        PM -->|spawn with restart backoff| W1
        PM -->|spawn with restart backoff| W2
    end

    subgraph Store ["SQLite WAL Store (jobs.db)"]
        W1 -->|claim_next_job (enforces pre-claim cleanup)| DB[(jobs.db)]
        W2 -->|claim_next_job (enforces pre-claim cleanup)| DB
        RC -->|reap_stale_jobs & supervisor_finalize_job| DB
    end

    subgraph E2B ["Verified E2B Cloud Sandbox SDK"]
        W1 -->|create with metadata={'job_id': job_id}| SB1["E2B Sandbox 1"]
        RC -.->|Sandbox.kill(sandbox_id) / Sandbox.list()| SB1
    end
```

### 1.1 Durable Worker Identity & Authority Validation
- **Durable Worker Identity**:
  A bare OS PID is unsafe to trust indefinitely because operating systems rapidly reuse PIDs.
  Each worker process generates a cryptographic instance identity at spawn time:
  `worker_instance_id = f"worker_{uuid.uuid4().hex[:12]}_{os.getpid()}"`
- **Supervisor-Worker Ownership Registry**:
  The supervisor maintains a durable registry table in `jobs.db`: `worker_instances` (instance_id, pid, start_time, last_heartbeat, status).
  When issuing signals (`SIGTERM`, `SIGKILL`), the supervisor verifies both PID and process creation time (via `/proc/<pid>/stat` start_time on Linux or `psutil.Process(pid).create_time()`) to guarantee it never signals an unrelated recycled OS process.

### 1.2 Supervisor Finalization Authority Check
A supervisor must **never** be able to terminate or finalize a valid worker's job merely because it knows the current lease generation.
To execute `supervisor_finalize_job()`, the supervisor must prove authority through one of three mutually exclusive, verified conditions:
1. **Verified Process Death**: The assigned worker process PID is confirmed dead (verified via `/proc/<pid>` absent, `process.is_alive() == False`, or `create_time` mismatch proving PID recycling).
2. **Verified Forced Termination**: The supervisor itself successfully sent `SIGKILL` to the verified PID and confirmed child termination via `os.waitpid()`.
3. **Verified Lease Expiration**: The job's `lease_expires_at` in the database is strictly `< time.time()` (worker lost its heartbeat lease and is fenced out).

If none of these conditions are met, `supervisor_finalize_job()` **refuses to finalize** and raises `JobQueueError("Supervisor authority check failed: assigned worker is active with valid lease.")`.

```python
def supervisor_finalize_job(
    self,
    job_id: str,
    expected_lease_generation: int,
    target_status: str,  # 'CANCELLED', 'TIMED_OUT', 'FAILED', 'RECOVERABLE'
    supervisor_id: str,
    authority_proof: Dict[str, Any],  # {"type": "PROCESS_DEAD"|"FORCED_KILL"|"LEASE_EXPIRED", "verified_at": float}
    reason: str,
    error: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Privileged supervisor finalization.
    
    Invariants:
    1. Validates authority_proof against database lease state and supervisor records.
    2. Validates lease_generation == expected_lease_generation (fences stale supervisors).
    3. Conditional atomic transition: requires status IN ('RUNNING', 'RECOVERABLE').
    4. Disallows target_status == 'COMPLETED' (supervisors cannot forge successful work).
    5. Records finalization event in job_events citing supervisor_id, authority_proof, reason, and error.
    6. Increments lease_generation to fence out any delayed/zombie worker activity.
    """
```

### 1.3 Child Crash & Crash-Loop Protection
1. The supervisor monitors child processes using non-blocking checks (`process.is_alive()` and `os.waitpid(-1, os.WNOHANG)`).
2. If a child terminates unexpectedly (exit code $\neq 0$, OOM, segfault):
   - Logs the crash event with instance ID, PID, and exit code.
   - **Crash-Loop Backoff**: If a worker crashes within 10 seconds of launch, the supervisor increments a slot failure counter and applies exponential backoff: $1s \to 2s \to 4s \to 8s \to 16s \to 30s$ max.
   - If a slot crashes 5 consecutive times without successfully claiming or completing work, the slot pauses and raises a critical supervisor telemetry alert.

---

## 2. Dedicated Sandbox Lifecycle, Pre-Claim Invariant, & Operator Resolution

### 2.1 Database Schema Migration 2
```sql
-- Migration 2: Dedicated Sandbox Lifecycle, Privileged Reconciliation, and Side Effects
ALTER TABLE jobs ADD COLUMN sandbox_cleanup_status TEXT NOT NULL DEFAULT 'NONE'; 
-- Allowed sandbox_cleanup_status: 'NONE', 'CLEANED', 'ORPHANED', 'FAILED', 'MANUAL_INTERVENTION_REQUIRED'
ALTER TABLE jobs ADD COLUMN sandbox_cleanup_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN sandbox_cleanup_error TEXT;
ALTER TABLE jobs ADD COLUMN sandbox_cleanup_next_retry REAL;
ALTER TABLE jobs ADD COLUMN side_effect_intents_json TEXT NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS worker_instances (
    instance_id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    start_time REAL NOT NULL,
    last_heartbeat REAL NOT NULL,
    status TEXT NOT NULL -- 'STARTING', 'ALIVE', 'STOPPING', 'DEAD'
);

CREATE INDEX IF NOT EXISTS idx_jobs_claim_eligibility ON jobs (status, sandbox_cleanup_status, created_at);
```

### 2.2 Dedicated Fenced Worker API: `register_sandbox()`
```python
def register_sandbox(
    self,
    job_id: str,
    sandbox_id: str,
    worker_id: str,
    lease_generation: int,
) -> bool:
    """Atomically registers an active sandbox container ID.
    
    Invariants:
    1. Validates status == 'RUNNING', worker_id == expected, lease_generation == expected.
    2. Validates lease_expires_at IS NOT NULL AND lease_expires_at >= now.
    3. Idempotent: If sandbox_id already matches, returns True without error.
    4. If another conflicting sandbox_id is already registered for this generation, raises JobQueueError.
    """
```

### 2.3 Strict Pre-Claim Invariant Enforced Inside `claim_next_job()`
A reaped job whose lease expired **must never be claimed by another worker until its previous cloud sandbox is confirmed destroyed (or independently confirmed absent)**.

The queue itself enforces this invariant in `claim_next_job()`:
```sql
SELECT id, lease_generation FROM jobs
WHERE status = 'QUEUED'
  AND (sandbox_cleanup_status = 'NONE' OR sandbox_cleanup_status = 'CLEANED')
ORDER BY created_at ASC
LIMIT 1;
```
If a job has `sandbox_cleanup_status IN ('ORPHANED', 'FAILED', 'MANUAL_INTERVENTION_REQUIRED')`, **it cannot be claimed under any circumstances**, preventing duplicate execution and concurrent container contention.

### 2.4 Cleanup Escalation & The Operator-Resolution Contract
- **Never Abandon Live Cloud Resources**:
  If E2B sandbox destruction fails (provider 500 error, network timeout, rate limit), the reconciler retries with exponential backoff: $2s \to 4s \to 8s \dots$ up to 300s.
- **Escalation to `MANUAL_INTERVENTION_REQUIRED`**:
  If cleanup fails 10 consecutive times:
  1. `sandbox_cleanup_status` transitions to `'MANUAL_INTERVENTION_REQUIRED'`.
  2. The job execution status transitions to terminal `FAILED` (if not already terminal), but its resource cleanup lifecycle remains active.
  3. The reconciler continues retrying cleanup indefinitely on a 15-minute background cycle until the container is confirmed gone.
- **Operator-Resolution API**:
  To resolve a stuck cleanup manually, an operator invokes a dedicated administrative method:
  ```python
  def resolve_manual_cleanup(
      self,
      job_id: str,
      operator_id: str,
      action: str,  # 'CONFIRMED_DELETED' | 'FORCE_CLEANUP_RETRY'
      verification_proof: str,  # e.g. "Verified absent via E2B console / CLI"
  ) -> Dict[str, Any]:
      """Operator-authenticated resolution of stuck cloud resources.
      
      If action == 'CONFIRMED_DELETED':
          Sets sandbox_cleanup_status = 'CLEANED'.
          Records audit event in job_events citing operator_id and verification_proof.
          Job transitions to terminal state or becomes eligible for re-claim.
      If action == 'FORCE_CLEANUP_RETRY':
          Resets sandbox_cleanup_attempts = 0, next_retry = now, status = 'ORPHANED'.
      """
  ```

---

## 3. Worker Heartbeat, Cancellation Polling, & Self-Fencing

### 3.1 Bounded Interruption & Two-Thread Architecture
Inside each child worker process:
1. **Execution Thread**:
   - Steps through `agent.run_agent_turn(..., execution_controller=ctrl)`.
   - Uses `time.monotonic()` for in-process wall-clock timeout tracking (immune to NTP steps).
   - Before invoking LiteLLM and before executing each tool, checks `ctrl.is_aborted()`.
2. **Heartbeat & Watchdog Thread**:
   - Runs every 5 seconds while a job is active.
   - Calls atomic `poll_worker_status(job_id, worker_id, lease_generation, lease_duration=30.0)`.

### 3.2 Atomic Method: `poll_worker_status()`
Combines lease renewal and cancellation inspection in a single atomic transaction:
```python
def poll_worker_status(
    self,
    job_id: str,
    worker_id: str,
    lease_generation: int,
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS,
) -> str:
    """Atomically validates lease, renews expiration, and inspects cancellation state.
    
    Returns:
    - 'ACTIVE': Lease extended, job is running normally.
    - 'CANCEL_REQUESTED': Lease extended, user requested cancellation.
    - 'STALE': Worker lease expired or was superseded (Worker must self-fence immediately).
    """
```

### 3.3 Strict Self-Fencing on Stale Lease
If `poll_worker_status` returns `'STALE'` (or if the database connection fails):
1. The heartbeat thread immediately sets `ctrl.abort(reason="Lease expired or superseded")`.
2. **Self-Fencing Guard**: The execution thread halts immediately. **No tool call is executed, no further logs are written, and the sandbox is destroyed locally.**
3. The worker exits cleanly without attempting further database updates.

---

## 4. Comprehensive PR & Branch Side-Effect Reconciliation (Fail-Closed Protocol)

To prevent duplicate side effects under at-least-once execution semantics:

### 4.1 Side-Effect Identity & Intent Lifecycle
Every external action with irreversible side effects is assigned a deterministic identity:
`side_effect_id = sha256(f"{job_id}:{tool_name}:{action_key}")[:16]`

Valid Intent Lifecycle: `PENDING \to CONFIRMED | FAILED_VERIFIED`

### 4.2 Fail-Closed PR Reconciliation Protocol
Before invoking `repo.create_pull()`:
1. **Record Intent**:
   Worker records `{"side_effect_id": id, "action": "create_pull_request", "status": "PENDING"}` in `side_effect_intents_json`.
2. **Query GitHub Across All States (Fail-Closed on API Error)**:
   The worker queries GitHub across **all states** (`state="all"`):
   ```python
   try:
       pulls = list(repo.get_pulls(state="all", head=f"{repo.owner.login}:{branch_name}"))
   except Exception as gh_err:
       # FAIL CLOSED: Do NOT assume 'no PR exists' on API timeout, 500 error, or rate limit!
       raise SideEffectReconciliationError(
           f"Inconclusive PR lookup on GitHub due to API error: {str(gh_err)}. "
           "Refusing to create duplicate PR; retrying reconciliation on next attempt."
       ) from gh_err
   ```
3. **Reconciliation Outcomes**:
   - **Case A: Matching Open PR Found**:
     Worker re-attaches to existing PR metadata (`pr.html_url`, `pr.number`). Updates intent state to `CONFIRMED`. Skips creation.
   - **Case B: Matching Closed or Merged PR Found**:
     Worker detects previous PR was closed or merged. Re-attaches to historical PR metadata. Records intent `CONFIRMED`. Logs: `"Target branch already merged or closed in PR #{pr.number}; skipping creation."`
   - **Case C: Definitively No PR Found (Query Succeeded with 0 Results)**:
     Worker issues `repo.create_pull()`. Upon return, records `CONFIRMED` with PR URL in `side_effect_intents_json`.
4. **Deterministic Branch Scoping & Ownership**:
   - Default branch: `ai-cloud/job-{job_id[:8]}`.
   - If user requests a custom branch name, worker enforces unique job scoping:
     `ai-cloud/job-{job_id[:8]}-{custom_branch_name}`.
   - A worker **never** resets or pushes to a branch that does not start with its own `ai-cloud/job-{job_id[:8]}` prefix, preventing branch collisions across concurrent runs or tenants.

---

## 5. Execution Controller: Cooperative vs. Bounded Forced Interruption

### 5.1 Dual-Level Interruption Protocol
Setting a thread event (`threading.Event`) **cannot** forcibly abort an already-blocked synchronous I/O or network socket call. Phase B.2 defines explicit per-operation timeouts and provider kill hooks:

```mermaid
sequenceDiagram
    participant Ctrl as ExecutionController
    participant Thread as Agent Execution Thread
    participant Sandbox as E2B Sandbox API
    participant Watchdog as Worker Watchdog Thread

    Note over Thread: Blocked in sandbox.run_command()
    Watchdog->>Ctrl: abort(reason="Timeout exceeded")
    Ctrl->>Thread: set _abort_event
    
    par Cooperative Check
        Thread->>Ctrl: is_aborted()? (skipped if blocked in I/O)
    and Bounded Forced Provider Kill
        Ctrl->>Sandbox: Sandbox.kill(sandbox_id) [Timeout: 5.0s]
        Sandbox-->>Thread: Socket abruptly closed; run_command raises ConnectionError
        Note over Thread: Synchronous call unblocks immediately with exception
    end
    
    Thread->>Ctrl: Catches abort/connection error; exits cleanly
```

1. **Per-Operation Bounded Timeouts**:
   - LLM Completion: Bounded client timeout of **60 seconds** in LiteLLM (`timeout=60.0`).
   - Terminal Command: Bounded timeout of **300 seconds** in `sandbox.run_command(..., timeout=300)`.
   - Provider Kill: Bounded timeout of **5.0 seconds** in `Sandbox.kill()`.
2. **Forced Socket Interruption**:
   If a command hangs and the execution watchdog trips `ctrl.abort()`:
   - The watchdog immediately invokes `Sandbox.kill(sandbox_id)` in the background.
   - The provider terminates the remote container, which abruptly closes the underlying WebSocket / HTTP connection.
   - The synchronous blocked call in the execution thread immediately unblocks with a connection reset exception.
   - The execution thread catches the exception, verifies `ctrl.is_aborted()`, and exits cleanly without running subsequent steps.

---

## 6. Concrete Credential & Configuration Trust Boundary

To ensure strict tenant isolation and zero credential leakage across detached OS processes:

### 6.1 Trust Boundary Architecture
- **Supervisor Role**: Runs with local system permissions and has access to environment variables (`E2B_API_KEY`, `ENCRYPTION_KEY`). It **never** receives, handles, or caches user GitHub PATs or BYOK LLM keys.
- **Worker Process Isolation**:
  Spawned child processes inherit `ENCRYPTION_KEY` and `E2B_API_KEY` via standard OS process environment inheritance.
- **Authenticated User-Store Resolution**:
  When executing a job:
  1. The worker reads `job["user_id"]` from the cryptographically verified SQLite row.
  2. The worker resolves user credentials strictly via `db._load_local_user_data(user_id=job["user_id"])`.
  3. Path traversal is impossible: `db._get_safe_filename(user_id)` strictly hashes or validates the tenant ID.
  4. Only keys matching `job["credential_refs"]` are decrypted into memory.
  5. Keys are passed directly to `WorkspaceSandbox` in memory. They are **never serialized to CLI flags, `/proc/<pid>/cmdline`, environment dumps, or database event messages**.
  6. Phase A's `GIT_ASKPASS` credential helper handles Git authentication with permissions `0700` and automated `finally` removal.

---

## 7. Verified E2B SDK Capabilities (Empirically Validated)

Inspected live in `.venv/bin/python3`:
- **Version**: E2B Python SDK v1.x (modern API).
- **Metadata Tagging**: `Sandbox.create(metadata={"job_id": job_id, "managed_by": "ai-cloud-workspace"})` is directly supported.
- **Enumeration**: `Sandbox.list(query=...)` supports paginated enumeration of active containers.
- **Destruction**: `Sandbox.kill(sandbox_id)` is a verified `@class_method_variant` capable of directly killing any sandbox by ID without needing an existing instance connection.

---

## 8. Comprehensive Failure-Injection Test Plan for Phase B.2

Dedicated test file: `tests/test_phase_b_worker_engine.py`

| # | Test Scenario | Injection / Failure Condition | Verification Invariant |
|---|---|---|---|
| **1** | **Normal Worker Turn** | Standard multi-step agent turn. | Job completes, logs stream, sandbox destroyed, terminal status `COMPLETED`. |
| **2** | **Heartbeat Maintenance** | Long-running task (10s) with 3s lease. | Heartbeat continuously extends lease; job never expires. |
| **3** | **Pre-Step Self-Fencing** | Worker lease expires mid-run; heartbeat returns `'STALE'`. | Worker halts immediately; **zero** subsequent tool calls made; sandbox destroyed locally. |
| **4** | **Cooperative Cancellation** | User requests cancellation during turn 1 of 3. | Worker detects `CANCEL_REQUESTED`, halts before turn 2, destroys sandbox, records `CANCELLED`. |
| **5** | **Supervisor Authority & Forced Finalization** | Supervisor attempts `supervisor_finalize_job` on active worker without proof. | Fails closed; requires valid proof (`PROCESS_DEAD`, `FORCED_KILL`, or `LEASE_EXPIRED`). |
| **6** | **Forced Cancellation Escalation** | Mock tool hangs indefinitely after cancel requested. | Supervisor triggers `SIGTERM`/`SIGKILL` at 15s; reconciler destroys sandbox, marks `CANCELLED`. |
| **7** | **Blocked Command Forced Interruption** | Active tool call hangs; timeout watchdog trips. | Watchdog kills sandbox; synchronous call unblocks immediately; records `TIMED_OUT`. |
| **8** | **Crash & Re-Claim Pre-Condition** | Worker killed via `SIGKILL` while holding active sandbox. | Job has `ORPHANED` status; `claim_next_job()` strictly refuses claim until confirmed `CLEANED`. |
| **9** | **Cleanup Escalation & Operator Resolution** | Reconciler encounters repeated provider 500 errors (10 times). | Status becomes `MANUAL_INTERVENTION_REQUIRED`; operator API resolves; job unblocks. |
| **10** | **Fail-Closed PR Reconciliation** | GitHub API raises 500/timeout during reconciliation check. | Worker fails closed; does **not** create duplicate PR; retries reconciliation cleanly. |
| **11** | **PR Idempotency across All States** | PR was merged/closed on GitHub before worker retry. | Worker detects existing closed/merged PR, re-attaches cleanly, does not duplicate. |
| **12** | **Supervisor Crash-Loop Protection** | Child worker process repeatedly crashes on startup. | Supervisor applies exponential backoff ($1s \to 30s$); pauses slot after 5 crashes. |
| **13** | **Spawned Worker Credential Isolation** | Inspect child process `/proc/<pid>/cmdline`, environment, and `job_events`. | Zero plaintext GitHub tokens or LLM API keys leaked across IPC boundaries. |

---

## 9. Verification Gate & Commit Evidence

- **Specification Commit**: `68808a9c5122273ad1b4484e540e5fd4c2640fa9` (Updated to Revision 2.2)
- **Main Baseline**: `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)
- **Scope Commitment**: `app.py` remains untouched. Phase B.2 implementation will not start until this specification is authorized.

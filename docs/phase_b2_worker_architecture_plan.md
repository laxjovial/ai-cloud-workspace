# Phase B.2 — Autonomous Worker Engine: Consolidated Architecture Specification & Engineering Contract

**Repository:** `laxjovial/ai-cloud-workspace`  
**Working Branch:** `feat/phase-b-durable-execution`  
**Specification Revision:** 2.1 (Production-Grade Engineering Contract)  
**Baseline (`main`):** `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)  
**Scope Boundary:** Phase B.2 covers only the background worker engine, process supervisor, heartbeat/fencing watchdog, crash-resilient sandbox registry/reconciler, and associated tests. It introduces **no Streamlit changes (`app.py` untouched)** and **no Phase C work**.

---

## 1. Process Supervision, Process Ownership, & Worker Lifecycle

```mermaid
flowchart TD
    subgraph SupervisorProcess ["Supervisor Process (worker.py supervisor)"]
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

### 1.1 Process Model & Durable Worker Instance Identity
- **Durable Worker Identity**:
  A bare OS PID is unsafe to trust indefinitely because operating systems rapidly reuse PIDs.
  Each worker process generates a cryptographic instance identity at spawn time:
  `worker_instance_id = f"worker_{uuid.uuid4().hex[:12]}_{os.getpid()}"`
- **Supervisor-Worker Ownership Registry**:
  The supervisor maintains a durable registry table in `jobs.db`: `worker_instances` (PID, instance_id, started_at, last_seen, status).
  When issuing signals (`SIGTERM`, `SIGKILL`), the supervisor verifies both PID and process creation time (via `/proc/<pid>/stat` on Linux or `psutil`) to guarantee it never signals an unrelated recycled OS process.
- **Child Crash & Crash-Loop Protection**:
  1. The supervisor monitors child processes using non-blocking checks (`process.is_alive()` and `os.waitpid(-1, os.WNOHANG)`).
  2. If a child terminates unexpectedly (exit code $\neq 0$, OOM, segfault):
     - Logs the crash event with instance ID, PID, and exit code.
     - **Crash-Loop Backoff**: If a worker crashes within 10 seconds of launch, the supervisor increments a slot failure counter and applies exponential backoff: $1s \to 2s \to 4s \to 8s \to 16s \to 30s$ max.
     - If a slot crashes 5 consecutive times without successfully claiming or completing work, the slot pauses and raises a critical supervisor telemetry alert.
- **Supervisor Self-Recovery & Restart**:
  When the supervisor itself starts or restarts:
  1. It performs a **Startup Reconciliation Pass** before booting workers:
     - Scans `jobs` for `status = 'RUNNING'`.
     - Stale workers from the previous supervisor run are immediately fenced out.
     - Unreconciled cloud sandboxes are queued for cleanup.
  2. Spawns fresh child workers once the queue state is clean.
- **Clean Deployment Shutdown (`SIGTERM` / `SIGINT`)**:
  - Supervisor shutdown is **not** a user cancellation.
  - The supervisor sets `shutdown_event` and sends `SIGTERM` to child workers.
  - Workers cease polling. Active jobs are given a **15-second grace period** to pause cleanly and transition to `status = 'RECOVERABLE'` (allowing another worker to resume after restart), without falsely marking the job `CANCELLED`.
  - After 15 seconds, remaining uncooperative children receive `SIGKILL`, and their sandboxes are reconciled.

---

## 2. Dedicated Sandbox Lifecycle, Pre-Claim Invariant, & Supervisor Recovery APIs (B.1 Migration 2)

To eliminate state machine violations (`RUNNING \to RUNNING`), enforce pre-claim cleanup invariants inside the database query, and prevent supervisor privilege bypasses, Phase B.2 introduces **Migration 2** in `job_queue.py`:

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
    started_at REAL NOT NULL,
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

### 2.4 Dedicated Privileged Supervisor API: `supervisor_finalize_job()`
Supervisors cannot pretend to be workers or forge worker leases. When a worker has been killed or has abandoned a job, the supervisor finalizes it through a dedicated, fenced API:
```python
def supervisor_finalize_job(
    self,
    job_id: str,
    expected_lease_generation: int,
    target_status: str,  # 'CANCELLED', 'TIMED_OUT', 'FAILED', 'RECOVERABLE'
    supervisor_id: str,
    reason: str,
    error: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Privileged supervisor finalization.
    
    Invariants:
    1. Validates lease_generation == expected_lease_generation (fences stale supervisors).
    2. Disallows target_status == 'COMPLETED' (supervisors cannot forge successful work).
    3. Records finalization event in job_events citing supervisor_id, reason, and error.
    4. Increments lease_generation to fence out any delayed/zombie worker activity.
    """
```

### 2.5 Resilient Sandbox Cleanup & Escalation Lifecycle
- When `reap_stale_jobs()` detects an expired lease:
  - If `sandbox_id` is present, it sets `status = 'RECOVERABLE'` and `sandbox_cleanup_status = 'ORPHANED'`.
  - Structured reap record returned:
    `{"job_id": jid, "sandbox_id": s_id, "lease_generation": gen, "cancel_requested": bool}`
- The supervisor reconciler attempts destruction via verified E2B API:
  `Sandbox.kill(sandbox_id)` (with a bounded 5-second HTTP timeout).
  - **Success / 404 Not Found**: Container confirmed gone. Calls atomic `confirm_sandbox_cleaned(job_id)`. The job transitions to `QUEUED` (or `CANCELLED` if cancellation was pending).
  - **Failure (500 Error, Timeout, Rate Limit)**:
    - Increments `sandbox_cleanup_attempts`.
    - Sets `sandbox_cleanup_status = 'FAILED'`.
    - Sets `sandbox_cleanup_next_retry = now + min(300, 2 ** attempts)`.
    - Retries destruction on subsequent iterations.
  - **Escalation Policy (No Abandonment of Live Billing)**:
    - If cleanup fails 10 consecutive times, the status transitions to:
      `sandbox_cleanup_status = 'MANUAL_INTERVENTION_REQUIRED'`.
    - The job status transitions to terminal `FAILED` with explicit error:
      `{"error": "Cloud sandbox cleanup failed; operator intervention required to terminate container.", "sandbox_id": sandbox_id}`.
    - **Crucial Invariant**: The reconciler continues retrying cleanup indefinitely every 15 minutes until confirmed destroyed, or until an operator resolves it. Live cloud resources are **never** abandoned silently.

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

## 4. Idempotency & Side-Effect Reconciliation Protocol

To prevent duplicate side effects under at-least-once execution semantics:

### 4.1 Side-Effect Identity & Intent Table
Every external action with irreversible side effects is assigned a deterministic identity:
`side_effect_id = sha256(f"{job_id}:{tool_name}:{action_key}")[:16]`

### 4.2 Pull Request Reconciliation Protocol
Before invoking `repo.create_pull()`:
1. **Record Intent**:
   Worker records `{"side_effect_id": id, "action": "create_pull_request", "status": "PENDING"}` in `side_effect_intents_json`.
2. **Comprehensive Remote PR Query (Open, Closed, Merged)**:
   The worker queries GitHub across **all states**:
   `repo.get_pulls(state="all", head=f"{repo.owner.login}:{branch_name}")`
   - **If an open PR exists**:
     Re-attaches to the existing PR URL/number. Records intent `CONFIRMED`. Skips creation.
   - **If a closed/merged PR exists**:
     Logs: `"Branch has already been merged or closed in PR #{pr.number}."`
     Does not attempt duplicate creation. Re-attaches to existing PR metadata.
   - **If no PR exists**:
     Issues `repo.create_pull()`. Upon return, records `CONFIRMED` in `side_effect_intents_json`.
3. **Deterministic Branch Scoping**:
   - Standard branch: `ai-cloud/job-{job_id[:8]}`.
   - If user requests a custom branch name, worker enforces unique job scoping:
     `ai-cloud/{job_id[:8]}-{custom_branch_name}`.
   - If the remote branch already exists, worker pulls/resets cleanly with ASKPASS isolation, preventing collisions.

---

## 5. Execution-Control Interface & Agent Interruption Contract

To allow the background worker to interrupt the synchronous agent loop cleanly:

### 5.1 Execution Controller Interface (`ExecutionController`)
```python
class ExecutionController:
    """Coordinates execution bounds, cancellation, and sandbox interruption between worker threads."""
    def __init__(self, timeout_seconds: float):
        self.started_monotonic = time.monotonic()
        self.timeout_seconds = timeout_seconds
        self._abort_event = threading.Event()
        self.abort_reason: Optional[str] = None
        self.current_sandbox: Optional[WorkspaceSandbox] = None

    def is_aborted(self) -> bool:
        if self._abort_event.is_set():
            return True
        if time.monotonic() - self.started_monotonic > self.timeout_seconds:
            self.abort(reason=f"Wall-clock timeout ({self.timeout_seconds}s) exceeded")
            return True
        return False

    def abort(self, reason: str):
        self.abort_reason = reason
        self._abort_event.set()
        if self.current_sandbox:
            # Issue bounded kill to active command/sandbox
            try:
                self.current_sandbox.close()
            except Exception:
                pass
```

### 5.2 Agent Interruption Contract in `agent.run_agent_turn()`
- `run_agent_turn()` accepts an optional `controller: Optional[ExecutionController] = None`.
- **Pre-Step Check**: Checks `controller.is_aborted()` before LiteLLM completion calls and before each tool invocation.
- If aborted: Immediately terminates the turn loop and returns:
  `{"success": False, "status": "aborted", "error": controller.abort_reason, "terminal_logs": ...}`.

---

## 6. Spawned Worker Credential & Configuration Resolution

To maintain Phase A security boundaries in a detached child process:

### 6.1 Configuration & E2B API Key Resolution
- The worker supervisor and child workers load environment variables via `python-dotenv` from a secure configuration file or environment variables (`E2B_API_KEY`, `ENCRYPTION_KEY`).
- `sandbox.py` is updated to accept explicit `api_key: Optional[str] = None` in `WorkspaceSandbox.__init__`, falling back cleanly to environment variables if Streamlit secrets are unavailable.

### 6.2 Authenticated User-Store Lookup
- When executing a job, the worker reads user data exclusively from:
  `db._load_local_user_data(user_id=job["user_id"])`.
- The user ID is taken strictly from the authenticated `job["user_id"]` stored at enqueue time, never from untrusted payload strings.
- Keys are resolved in memory, passed to `WorkspaceSandbox` in memory, and never logged or serialized to `job_events`.

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
| **5** | **Forced Cancellation Escalation** | Mock tool hangs indefinitely after cancel requested. | Supervisor triggers `SIGTERM`/`SIGKILL` at 15s; reconciler destroys sandbox, marks `CANCELLED`. |
| **6** | **Monotonic Timeout Interruption** | Mock command exceeds `timeout_seconds`. | Watchdog halts execution via `ExecutionController`, terminates container, records `TIMED_OUT`. |
| **7** | **Crash & Re-Claim Pre-Condition** | Worker killed via `SIGKILL` while holding active sandbox. | Stale job is **not** eligible for `claim_next_job()` until reconciler confirms old sandbox destroyed. |
| **8** | **Sandbox Creation Race Window** | Worker killed immediately after sandbox create, before DB registration. | Orphan scanner discovers tagged container via `Sandbox.list()` and destroys it. |
| **9** | **Cleanup API Failure & Retry** | Reconciler encounters simulated 500 error calling `Sandbox.kill()`. | Job transitions to `ORPHANED` / `FAILED` cleanup; retries with exponential backoff; never re-claimed while cleanup pending. |
| **10** | **PR Idempotency on Worker Retry** | Worker creates PR on GitHub, then crashes before DB write. Next worker retries job. | Worker inspects remote PRs across all states, re-attaches to existing PR, avoids creating duplicate. |
| **11** | **Supervisor Crash-Loop Protection** | Child worker process repeatedly crashes on startup. | Supervisor applies exponential backoff ($1s \to 30s$); prevents fork bomb; pauses slot after 5 crashes. |
| **12** | **Spawned Worker Credential Isolation** | Inspect child process `/proc/<pid>/cmdline`, environment, and `job_events`. | Zero plaintext GitHub tokens or LLM API keys leaked across IPC boundaries. |

---

## 9. Verification Gate & Commit Evidence

- **Specification Commit**: `c8eca94b0f0507fcf4725c77e9a437e5e8e1d6a0` (Updated to Revision 2.1)
- **Main Baseline**: `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)
- **Scope Commitment**: `app.py` remains untouched. Phase B.2 implementation will not start until this specification is authorized.

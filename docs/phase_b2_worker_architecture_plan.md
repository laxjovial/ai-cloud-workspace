# Phase B.2 — Autonomous Worker Engine: Consolidated Architecture Specification & Engineering Contract

**Repository:** `laxjovial/ai-cloud-workspace`  
**Working Branch:** `feat/phase-b-durable-execution`  
**Specification Revision:** 2.3 (Pre-Implementation Authoritative Engineering Contract)  
**Baseline (`main`):** `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)  
**Specification Baseline Commit:** `cd53c2b06bb2ceba792c402435d9cd2e1be3fb22`  
**Scope Boundary:** Phase B.2 covers only the background worker engine, process supervisor, heartbeat/fencing watchdog, crash-resilient sandbox registry/reconciler, side-effect intent ledger, and associated tests. It introduces **no Streamlit changes (`app.py` untouched)** and **no Phase C work**.

---

## 1. Process Supervision, Process Ownership, & Non-Forgeable Supervisor Authority

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

### 1.1 Durable Worker Identity & Process Ownership
- **Durable Worker Identity**:
  A bare OS PID is unsafe to trust indefinitely because operating systems rapidly reuse PIDs.
  Each worker process generates a cryptographic instance identity at spawn time:
  `worker_instance_id = f"worker_{uuid.uuid4().hex[:12]}_{os.getpid()}"`
- **Supervisor-Worker Ownership Registry**:
  The supervisor maintains a durable registry table in `jobs.db`:
  `worker_instances (instance_id, pid, start_time, last_heartbeat, status)`
  When issuing signals (`SIGTERM`, `SIGKILL`), the supervisor verifies both PID and process creation time (via `/proc/<pid>/stat` start_time on Linux or `psutil.Process(pid).create_time()`) against the recorded `start_time` in `worker_instances` to guarantee it never signals an unrelated recycled OS process.

### 1.2 Non-Forgeable Supervisor Authority Verification
A caller or supervisor must **never** be able to finalize an active worker's job simply by providing arbitrary metadata or an unverified dictionary.
`supervisor_finalize_job()` verifies authority proofs **internally within the atomic database transaction** rather than trusting caller-supplied timestamps:

1. **Proof Type `PROCESS_DEAD`**:
   - The supervisor passes `worker_instance_id`.
   - The queue engine inspects `worker_instances` for `worker_instance_id` to get `pid` and recorded `start_time`.
   - Engine performs an OS check:
     - On Linux: `/proc/<pid>` does not exist, OR `/proc/<pid>/stat` indicates `Z` (zombie), OR the process start time in `/proc/<pid>/stat` differs from the recorded `start_time` (PID recycled).
     - Standard fallback: `os.kill(pid, 0)` raises `ProcessLookupError` (or `psutil.Process(pid).is_running() == False`).
2. **Proof Type `FORCED_KILL`**:
   - The supervisor issued `SIGKILL` to the child worker process and successfully reaped the process exit status via `os.waitpid(pid, 0)` or `multiprocessing.Process.join(timeout=0)`.
   - The supervisor supplies the recorded exit status and execution token from `worker_instances`.
   - Engine validates that the process table marks `status = 'DEAD'` with verified termination time.
3. **Proof Type `LEASE_EXPIRED`**:
   - The database engine executes an atomic SQL query:
     `SELECT lease_expires_at FROM jobs WHERE id = ? AND lease_generation = ?`
   - Rechecks `lease_expires_at IS NOT NULL AND lease_expires_at < strftime('%s', 'now')`.
   - If the worker lease is still in the future, the operation **fails closed** and raises `JobQueueError("Supervisor authority check failed: assigned worker lease is still active.")`.

```python
def supervisor_finalize_job(
    self,
    job_id: str,
    expected_lease_generation: int,
    target_status: str,  # 'CANCELLED', 'TIMED_OUT', 'FAILED', 'RECOVERABLE'
    supervisor_id: str,
    authority_proof: Dict[str, Any],  # {"type": "PROCESS_DEAD"|"FORCED_KILL"|"LEASE_EXPIRED", "worker_instance_id": str}
    reason: str,
    error: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Privileged supervisor finalization with internal proof verification.
    
    Invariants:
    1. Validates proof internally within the SQLite transaction:
       - If 'LEASE_EXPIRED': checks row['lease_expires_at'] < now.
       - If 'PROCESS_DEAD' or 'FORCED_KILL': verifies worker_instance_id in worker_instances
         and validates OS process non-existence or PID start_time mismatch.
    2. Validates lease_generation == expected_lease_generation (fences stale supervisors).
    3. Conditional atomic transition: requires status IN ('RUNNING', 'RECOVERABLE').
    4. Disallows target_status == 'COMPLETED' (supervisors cannot forge successful work).
    5. Records finalization event in job_events citing supervisor_id, verified authority_proof, reason, and error.
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

## 2. Dedicated Sandbox Lifecycle, Pre-Claim Invariant, & Operator Resolution Contract

### 2.1 Dedicated Sandbox Cleanup Lifecycle States
A sandbox container's cloud lifecycle is tracked independently from the job execution status:
- `NONE`: No cloud sandbox provisioned yet.
- `ACTIVE`: Sandbox is currently live in E2B and owned by a running worker.
- `ORPHANED`: Sandbox requires background cleanup by the reconciler.
- `FAILED`: Cleanup attempt failed, awaiting exponential backoff retry.
- `MANUAL_INTERVENTION_REQUIRED`: Automated cleanup failed 10 consecutive times; requires operator intervention.
- `CLEANED`: Sandbox is confirmed destroyed or verified absent in E2B.

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
    3. Sets sandbox_cleanup_status = 'ACTIVE'.
    4. Idempotent: If sandbox_id already matches, returns True without error.
    5. If another conflicting sandbox_id is already registered for this generation, raises JobQueueError.
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
If a job has `sandbox_cleanup_status IN ('ACTIVE', 'ORPHANED', 'FAILED', 'MANUAL_INTERVENTION_REQUIRED')`, **it cannot be claimed under any circumstances**, preventing duplicate execution and concurrent container contention.

### 2.4 Cleanup Escalation & Decoupled Operator Resolution
- **Automated Reconciler Retries**:
  If E2B sandbox destruction fails (provider 500 error, network timeout, rate limit), the reconciler retries with exponential backoff: $2s \to 4s \to 8s \dots$ up to 300s.
- **Escalation to `MANUAL_INTERVENTION_REQUIRED`**:
  If cleanup fails 10 consecutive times:
  1. `sandbox_cleanup_status` transitions to `'MANUAL_INTERVENTION_REQUIRED'`.
  2. The job execution status transitions to terminal `'FAILED'` (if not already terminal).
  3. The reconciler stops automated high-frequency polling on this job, shifting to an hourly background safety audit.
- **Decoupled Operator Resolution Contract**:
  The operator resolution API strictly updates the **resource cleanup state** without silently mutating or reviving the job's terminal execution status:
  - If a job reached terminal `'FAILED'` (or `'CANCELLED'`), confirming the sandbox deletion transitions `sandbox_cleanup_status = 'CLEANED'`, but does **NOT** revive the job into `'QUEUED'`. The execution remains terminal `'FAILED'`.
  - If a job is non-terminal (`'RECOVERABLE'`), confirming deletion transitions `sandbox_cleanup_status = 'CLEANED'` and allows the job to return to `'QUEUED'` for safe re-claim.

```python
def resolve_manual_cleanup(
    self,
    job_id: str,
    operator_id: str,
    action: str,  # 'CONFIRMED_DELETED' | 'FORCE_CLEANUP_RETRY'
    verification_proof: str,  # e.g. "Verified absent via E2B console or CLI"
    admin_auth_token: str,    # Cryptographic admin token validated against environment/config
) -> Dict[str, Any]:
    """Operator-authenticated resolution of stuck cloud resources.
    
    Invariants:
    1. Validates admin_auth_token.
    2. If action == 'CONFIRMED_DELETED':
       - Sets sandbox_cleanup_status = 'CLEANED'.
       - Does NOT alter execution status if already in terminal state ('FAILED', 'CANCELLED', 'TIMED_OUT').
       - If status == 'RECOVERABLE', transitions status = 'QUEUED' (now safe to re-claim).
       - Records structured audit event in job_events citing operator_id and verification_proof.
    3. If action == 'FORCE_CLEANUP_RETRY':
       - Resets sandbox_cleanup_attempts = 0, next_retry = now, sandbox_cleanup_status = 'ORPHANED'.
    """
```

---

## 3. Worker Heartbeat, Cancellation Polling, & Strict Self-Fencing

### 3.1 Bounded Interruption & Two-Thread Process Architecture
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

## 4. Dedicated Side-Effect Ledger & Fail-Closed PR Protocol

To prevent duplicate external actions (e.g., duplicate GitHub PRs or Git branch conflicts) under at-least-once execution semantics:

### 4.1 Schema Migration 2: Dedicated Side-Effect Ledger
Side effects are stored in a dedicated, queryable relational table rather than an opaque JSON string:

```sql
CREATE TABLE IF NOT EXISTS job_side_effects (
    side_effect_id TEXT PRIMARY KEY, -- Full 64-character SHA-256 hex string
    job_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    lease_generation INTEGER NOT NULL,
    action_type TEXT NOT NULL,       -- e.g. 'CREATE_PULL_REQUEST', 'GIT_PUSH'
    status TEXT NOT NULL,            -- 'PENDING', 'CONFIRMED', 'FAILED_VERIFIED'
    intent_payload TEXT NOT NULL,    -- JSON payload of target parameters
    confirmed_result TEXT,           -- JSON payload of result (e.g. PR URL, PR number)
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs(id)
);

CREATE INDEX IF NOT EXISTS idx_side_effects_job ON job_side_effects (job_id, action_type);
```

### 4.2 Deterministic 64-Character Identity & Atomic Intent Reservation
1. **Full SHA-256 Identity**:
   `side_effect_id = hashlib.sha256(f"{job_id}:{action_type}:{action_key}".encode("utf-8")).hexdigest()`
2. **Atomic Intent Reservation**:
   Before performing the external API call, the worker conditionally inserts into `job_side_effects`:
   - Checks if an entry already exists with `status = 'CONFIRMED'`. If found, the worker skips execution and re-uses `confirmed_result`.
   - Checks if an entry exists with `status = 'PENDING'` for a different active worker lease. If so, rejects concurrent execution.
   - If no entry exists, inserts with `status = 'PENDING'`, current `worker_id`, and `lease_generation`.

### 4.3 Fail-Closed PR Reconciliation Protocol
Before invoking `repo.create_pull()`:
1. **Record Intent**:
   Worker records `status = 'PENDING'` in `job_side_effects` for `side_effect_id`.
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
     Worker re-attaches to existing PR metadata (`pr.html_url`, `pr.number`). Updates `job_side_effects` to `CONFIRMED`. Skips creation.
   - **Case B: Matching Closed or Merged PR Found**:
     Worker detects previous PR was closed or merged. Re-attaches to historical PR metadata. Records `CONFIRMED` in `job_side_effects`. Logs: `"Target branch already merged or closed in PR #{pr.number}; skipping creation."`
   - **Case C: Definitively No PR Found (Query Succeeded with 0 Results)**:
     Worker issues `repo.create_pull()`. Upon return, updates `job_side_effects` to `CONFIRMED` with `pr.html_url` and `pr.number`.
4. **Deterministic Branch Scoping & Ownership**:
   - Default branch: `ai-cloud/job-{job_id[:8]}`.
   - If user requests a custom branch name, worker enforces unique job scoping:
     `ai-cloud/job-{job_id[:8]}-{custom_branch_name}`.
   - A worker **never** resets or pushes to a branch that does not start with its own `ai-cloud/job-{job_id[:8]}` prefix, preventing branch collisions across concurrent runs or tenants.

---

## 5. Execution Controller: Cooperative, Provider Kill, & Supervisor Fallback Interruption

### 5.1 Multi-Tier Interruption Hierarchy
Setting a thread event (`threading.Event`) **cannot** forcibly abort an already-blocked synchronous network socket or C-level system call. Phase B.2 defines an escalating 3-tier interruption protocol:

```mermaid
sequenceDiagram
    participant Ctrl as ExecutionController
    participant Thread as Agent Execution Thread
    participant Sandbox as E2B Sandbox API
    participant Watchdog as Worker Watchdog Thread
    participant Sup as WorkerSupervisor Process

    Note over Thread: Blocked in synchronous sandbox socket call
    Watchdog->>Ctrl: abort(reason="Timeout / Cancellation")
    Ctrl->>Thread: set _abort_event (Cooperative tier)
    
    par Cooperative Check (Tier 1)
        Thread->>Ctrl: is_aborted()? (skipped if blocked in socket)
    and Bounded Forced Provider Kill (Tier 2)
        Ctrl->>Sandbox: Sandbox.kill(sandbox_id) [Timeout: 5.0s]
        alt Sandbox connection breaks
            Sandbox-->>Thread: Abrupt TCP FIN/RST; run_command raises ConnectionError
            Note over Thread: Thread unblocks and exits cleanly
        else Socket remains hung (E2B outage or OS socket dead)
            Note over Watchdog: Worker process fails to terminate within 15s grace period
        end
    end

    Note over Sup: Supervisor Grace Period Expiry (Tier 3)
    Sup->>Sup: verified_pid = registry.get(worker_inst)
    Sup->>Sup: os.kill(verified_pid, SIGTERM)
    Sup->>Sup: time.sleep(2.0)
    alt Still alive
        Sup->>Sup: os.kill(verified_pid, SIGKILL)
        Sup->>Sup: os.waitpid(verified_pid, 0)
        Note over Sup: Slot freed deterministically; orphan reconciler destroys sandbox
    end
```

1. **Tier 1 — Cooperative Interruption**:
   - Monitored between steps, before LLM calls, and before tool invocations via `ctrl.is_aborted()`.
   - LLM calls enforce bounded client timeout of **60 seconds** in LiteLLM (`timeout=60.0`).
   - Sandbox commands enforce bounded timeout of **300 seconds** in `sandbox.run_command(..., timeout=300)`.
2. **Tier 2 — In-Process Provider Kill**:
   - If execution exceeds turn timeout or cancellation is requested, the watchdog thread triggers `ctrl.abort()`.
   - The watchdog thread invokes `Sandbox.kill(sandbox_id)` with a **5.0-second timeout**.
   - If successful, the remote container termination tears down the WebSocket/HTTP connection, raising a `ConnectionError` in the blocked execution thread and unblocking it.
3. **Tier 3 — Supervisor Process Escalation Fallback**:
   - If provider API is unreachable, or the Python thread remains wedged in an un-interruptible socket call, the worker process fails to terminate within its **15-second shutdown grace period**.
   - The supervisor's process watchdog fires:
     - Looks up verified worker `pid` and verifies creation time against `worker_instances`.
     - Sends `SIGTERM`. If child remains alive after 2.0 seconds, issues `SIGKILL`.
     - Reaps child with `os.waitpid()`.
     - Atomically calls `supervisor_finalize_job(target_status='CANCELLED'|'TIMED_OUT', proof={'type': 'FORCED_KILL'})`.
     - Releases execution slot deterministically.

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
| **5** | **Supervisor Non-Forgeable Authority** | Supervisor attempts `supervisor_finalize_job` on active worker without proof. | Fails closed; requires verified proof (`PROCESS_DEAD`, `FORCED_KILL`, or `LEASE_EXPIRED`). |
| **6** | **Supervisor Forced Termination Fallback** | Worker process hangs in socket; watchdog triggers forced kill. | Supervisor sends `SIGTERM`/`SIGKILL`, confirms process death, reclaims slot, registers orphan sandbox. |
| **7** | **Blocked Command Forced Interruption** | Active tool call hangs; timeout watchdog trips. | Watchdog kills sandbox; synchronous call unblocks immediately; records `TIMED_OUT`. |
| **8** | **Crash & Re-Claim Pre-Condition** | Worker killed via `SIGKILL` while holding active sandbox. | Job has `ORPHANED` status; `claim_next_job()` strictly refuses claim until confirmed `CLEANED`. |
| **9** | **Cleanup Escalation & Operator Resolution** | Reconciler encounters repeated provider 500 errors (10 times). | Status becomes `MANUAL_INTERVENTION_REQUIRED`; operator API resolves; job does NOT silently re-queue if terminal. |
| **10** | **Fail-Closed PR Reconciliation** | GitHub API raises 500/timeout during reconciliation check. | Worker fails closed; does **not** create duplicate PR; retries reconciliation cleanly. |
| **11** | **PR Idempotency across All States** | PR was merged/closed on GitHub before worker retry. | Worker detects existing closed/merged PR in `job_side_effects`, re-attaches cleanly, does not duplicate. |
| **12** | **Supervisor Crash-Loop Protection** | Child worker process repeatedly crashes on startup. | Supervisor applies exponential backoff ($1s \to 30s$); pauses slot after 5 crashes. |
| **13** | **Spawned Worker Credential Isolation** | Inspect child process `/proc/<pid>/cmdline`, environment, and `job_events`. | Zero plaintext GitHub tokens or LLM API keys leaked across IPC boundaries. |

---

## 9. Verification Gate & Commit Evidence

- **Specification Base Commit**: `cd53c2b06bb2ceba792c402435d9cd2e1be3fb22`
- **Main Baseline**: `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)
- **Scope Commitment**: `app.py` remains untouched. Phase B.2 implementation (`worker.py`) will not start until this specification is authorized.

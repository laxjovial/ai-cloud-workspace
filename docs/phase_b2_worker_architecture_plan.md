# Phase B.2 — Autonomous Worker Engine: Consolidated Architecture Specification & Engineering Contract

**Repository:** `laxjovial/ai-cloud-workspace`  
**Working Branch:** `feat/phase-b-durable-execution`  
**Specification Revision:** 2.4 (Final Authoritative Engineering Contract Before Implementation)  
**Baseline (`main`):** `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)  
**Specification Baseline Commit:** `49b74ed69bc5867ef7b34c0d3b08d0d66ab3e078`  
**Scope Boundary:** Phase B.2 covers only the background worker engine, process supervisor, heartbeat/fencing watchdog, crash-resilient sandbox registry/reconciler, side-effect intent ledger, and associated tests. It introduces **no Streamlit changes (`app.py` untouched)** and **no Phase C work**.

---

## 1. Process Supervision, Process Ownership, & Verifiable Child Reap Evidence

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
- **Supervisor-Worker Ownership Registry Schema**:
  The supervisor maintains a durable registry table in `jobs.db` with complete lifecycle and reap accounting:
  ```sql
  CREATE TABLE IF NOT EXISTS worker_instances (
      instance_id TEXT PRIMARY KEY,
      pid INTEGER NOT NULL,
      start_time REAL NOT NULL,
      last_heartbeat REAL NOT NULL,
      status TEXT NOT NULL,                -- 'STARTING', 'ALIVE', 'STOPPING', 'DEAD'
      exit_code INTEGER,                   -- Process exit code from os.waitpid
      exit_signal INTEGER,                 -- Signal number if terminated by signal (e.g. 9 for SIGKILL)
      terminated_at REAL,                  -- Timestamp when reap occurred
      reap_token TEXT                      -- Cryptographic supervisor reap verification token (UUID hex)
  );
  ```
  When issuing signals (`SIGTERM`, `SIGKILL`), the supervisor verifies both PID and process creation time (via `/proc/<pid>/stat` start_time on Linux or `psutil.Process(pid).create_time()`) against the recorded `start_time` in `worker_instances` to guarantee it never signals an unrelated recycled OS process.

### 1.2 Durable, Verifiable Child-Process Reap Record & Authoritative Finalization
`os.waitpid()` is parent-process-specific: only the parent supervisor process can reap its direct child processes; the queue engine cannot execute `waitpid()` a second time.
To establish non-forgeable reap evidence across process boundaries:

1. **Authoritative Supervisor Child Reap Operation (`record_process_reap`)**:
   When the supervisor reaps a child process via `os.waitpid(pid, os.WNOHANG)` or after a forced `SIGKILL`:
   - The supervisor derives `exit_code` and `exit_signal` using `os.WEXITSTATUS(status)` and `os.WTERMSIG(status)`.
   - The supervisor generates a cryptographic one-time `reap_token = uuid.uuid4().hex`.
   - The supervisor calls:
     ```python
     def record_process_reap(
         self,
         instance_id: str,
         pid: int,
         exit_code: Optional[int],
         exit_signal: Optional[int],
         supervisor_id: str,
     ) -> str:
         """Atomically records child reap verification evidence into worker_instances.
         
         Invariants:
         1. Verifies instance_id exists and status != 'DEAD'.
         2. Sets status = 'DEAD', exit_code, exit_signal, terminated_at = now.
         3. Issues and returns reap_token.
         """
     ```
2. **Internal Authority Verification in `supervisor_finalize_job`**:
   The engine validates the supervisor's claim inside the atomic SQLite transaction:
   - **For `FORCED_KILL`**:
     Supervisor provides `authority_proof = {"type": "FORCED_KILL", "worker_instance_id": instance_id, "reap_token": reap_token}`.
     The engine queries `worker_instances`:
     ```sql
     SELECT status, exit_signal, reap_token FROM worker_instances WHERE instance_id = ?
     ```
     Verifies `status == 'DEAD'`, `reap_token == authority_proof['reap_token']`, and `exit_signal IN (9, 15)`.
     This proves the child was genuinely killed and reaped by the supervisor.
   - **For `PROCESS_DEAD`**:
     Supervisor provides `worker_instance_id`. Engine checks `worker_instances`. If marked `status == 'DEAD'`, accepts; if still marked `ALIVE`, performs OS verification (`/proc/<pid>` absent, zombie status, or start_time mismatch).
   - **For `LEASE_EXPIRED`**:
     The engine queries `lease_expires_at` directly from `jobs` and validates `lease_expires_at < strftime('%s', 'now')`. If the lease has not expired, the operation **fails closed**.

```python
def supervisor_finalize_job(
    self,
    job_id: str,
    expected_lease_generation: int,
    target_status: str,  # 'CANCELLED', 'TIMED_OUT', 'FAILED', 'RECOVERABLE'
    supervisor_id: str,
    authority_proof: Dict[str, Any],  # {"type": "FORCED_KILL"|"PROCESS_DEAD"|"LEASE_EXPIRED", ...}
    reason: str,
    error: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Privileged supervisor finalization with internal proof verification.
    
    Invariants:
    1. Validates proof internally within SQLite transaction against worker_instances and jobs tables.
    2. Validates lease_generation == expected_lease_generation (fences stale supervisors).
    3. Requires current status IN ('RUNNING', 'RECOVERABLE').
    4. Disallows target_status == 'COMPLETED' (supervisors cannot forge successful work).
    5. Records finalization event in job_events citing supervisor_id, verified authority_proof, reason, and error.
    6. Increments lease_generation to fence out any delayed/zombie worker activity.
    """
```

### 1.3 Child Crash & Crash-Loop Protection
1. The supervisor monitors child processes using non-blocking checks (`process.is_alive()` and `os.waitpid(-1, os.WNOHANG)`).
2. If a child terminates unexpectedly (exit code $\neq 0$, OOM, segfault):
   - Supervisor records reap evidence via `record_process_reap()`.
   - **Crash-Loop Backoff**: If a worker crashes within 10 seconds of launch, the supervisor increments a slot failure counter and applies exponential backoff: $1s \to 2s \to 4s \to 8s \to 16s \to 30s$ max.
   - If a slot crashes 5 consecutive times without successfully claiming or completing work, the slot pauses and raises a critical supervisor telemetry alert.

---

## 2. Dedicated Sandbox Lifecycle, Pre-Claim Invariant, & Authenticated Operator Resolution

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

### 2.4 Cleanup Escalation & Authenticated Operator Identity Attribution
- **Automated Reconciler Retries**:
  If E2B sandbox destruction fails (provider 500 error, network timeout, rate limit), the reconciler retries with exponential backoff: $2s \to 4s \to 8s \dots$ up to 300s.
- **Escalation to `MANUAL_INTERVENTION_REQUIRED`**:
  If cleanup fails 10 consecutive times:
  1. `sandbox_cleanup_status` transitions to `'MANUAL_INTERVENTION_REQUIRED'`.
  2. The job execution status transitions to terminal `'FAILED'` (if not already terminal).
  3. The reconciler stops automated high-frequency polling on this job, shifting to an hourly background safety audit.
- **Authenticated Principal Verification & Non-Forgeable Attribution**:
  To prevent audit spoofing via a shared token or unauthenticated caller ID, the resolution API validates credentials against an administrative keystore mapping tokens to authenticated identities:
  - Tokens are hashed using SHA-256 and matched against configured admin credentials (`ADMIN_AUTH_KEYS = {"hash": {"principal_id": "operator_alice", "roles": ["ADMIN"]}}`).
  - The audit record in `job_events` records the **cryptographically verified `authenticated_principal_id`**, rather than trusting a caller-supplied argument.
  - If the token is invalid or principal is not authorized, the operation raises `JobQueueError("Unauthorized operator resolution request: invalid admin credential.")`.
- **Decoupled Operator Resolution Contract**:
  - If a job reached terminal `'FAILED'` (or `'CANCELLED'`), confirming deletion sets `sandbox_cleanup_status = 'CLEANED'`, but does **NOT** revive the job to `'QUEUED'`. The execution remains terminal `'FAILED'`.
  - If a job is non-terminal (`'RECOVERABLE'`), confirming deletion sets `sandbox_cleanup_status = 'CLEANED'` and allows the job to return to `'QUEUED'` for safe re-claim.

```python
def resolve_manual_cleanup(
    self,
    job_id: str,
    action: str,  # 'CONFIRMED_DELETED' | 'FORCE_CLEANUP_RETRY'
    verification_proof: str,  # e.g. "Verified absent via E2B console or CLI"
    admin_auth_token: str,    # Bearer token authenticated against admin credential store
) -> Dict[str, Any]:
    """Operator-authenticated resolution of stuck cloud resources with non-forgeable attribution.
    
    Invariants:
    1. Authenticates admin_auth_token against admin keystore; resolves authenticated_principal_id.
    2. If action == 'CONFIRMED_DELETED':
       - Sets sandbox_cleanup_status = 'CLEANED'.
       - Does NOT alter execution status if already in terminal state ('FAILED', 'CANCELLED', 'TIMED_OUT').
       - If status == 'RECOVERABLE', transitions status = 'QUEUED' (now safe to re-claim).
       - Records structured audit event in job_events citing authenticated_principal_id, role, proof.
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

## 4. Dedicated Side-Effect Ledger & Abandoned Intent Recovery Protocol

To guarantee idempotency and prevent duplicate external actions (such as duplicate GitHub PRs or corrupted Git branches) under at-least-once execution semantics:

### 4.1 Schema Migration 2: Dedicated Side-Effect Ledger
Side effects are stored in a dedicated, queryable relational table:

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
   Before performing the external API call, the worker conditionally accesses `job_side_effects` inside a transaction:
   - If an entry exists with `status = 'CONFIRMED'`: Worker reuses `confirmed_result` and skips the external call.
   - If an entry exists with `status = 'PENDING'` for an **active** lease generation: Rejects concurrent execution.
   - If no entry exists: Inserts row with `status = 'PENDING'`, current `worker_id`, and `lease_generation`.

### 4.3 Recovery & Fencing of Abandoned `PENDING` Intents
Consider the failure scenario:
- Worker A reserves intent as `PENDING` (generation 1).
- Worker A creates the PR on GitHub successfully.
- Worker A crashes or loses its lease before recording `CONFIRMED`.
- Worker B reclaims the job with a higher lease generation (generation 2).

The recovery protocol defines strict reconciliation before any external action is taken:

```mermaid
flowchart TD
    W2["Worker B Claims Job (Generation 2)"] --> Q["Query job_side_effects for job_id"]
    Q --> Check{"Any intent in PENDING state?"}
    Check -->|No| Safe["Proceed with Normal Step Execution"]
    Check -->|Yes (e.g. Generation 1 abandoned)| Reconcile["Enforce Remote Reconciliation Protocol"]
    
    Reconcile --> GH["Query GitHub across state='all' for branch"]
    GH -->|API Error / Timeout| FailClosed["FAIL CLOSED: Raise SideEffectReconciliationError (Retry later)"]
    GH -->|Matching PR Found| Adopt["Update intent: status='CONFIRMED', lease_gen=2, worker=Worker B"]
    GH -->|0 PRs Found| Rollback["Update intent: status='FAILED_VERIFIED', lease_gen=2 (Safe to re-execute)"]
    
    Adopt --> Fenced["Worker A Fenced: Stale lease_gen=1 rejected if zombie awakens"]
    Rollback --> Fenced
```

1. **Mandatory Pre-Execution Recovery Check**:
   Before executing any side-effect tool, the new worker checks for existing `PENDING` intents associated with the `job_id`:
   ```python
   def recover_pending_side_effects(self, job_id: str, new_worker_id: str, new_lease_gen: int):
       pending_intents = db.get_pending_side_effects(job_id)
       for intent in pending_intents:
           if intent["lease_generation"] < new_lease_gen:
               # Older lease abandoned this intent; perform remote reconciliation
               self._reconcile_abandoned_intent(intent, new_worker_id, new_lease_gen)
   ```
2. **Remote Provider Verification (Fail-Closed)**:
   The new worker queries GitHub across **all states** (`state="all"`):
   - **Matching PR Exists**:
     Worker B verifies the PR was indeed created by Worker A.
     Worker B updates `job_side_effects`:
     `UPDATE job_side_effects SET status = 'CONFIRMED', worker_id = ?, lease_generation = ?, confirmed_result = ? WHERE side_effect_id = ?`
     Worker B re-attaches to the existing PR URL and skips creation.
   - **No PR Exists (Query succeeded with 0 results)**:
     Worker B confirms Worker A crashed before creating the PR.
     Worker B updates `job_side_effects`:
     `UPDATE job_side_effects SET status = 'FAILED_VERIFIED', worker_id = ?, lease_generation = ? WHERE side_effect_id = ?`
     Worker B is now permitted to reserve a fresh intent and perform the creation.
   - **Query Inconclusive (API Error / Timeout / Rate Limit)**:
     **Fails closed**. Worker B raises `SideEffectReconciliationError` and halts execution turn without performing duplicate actions.
3. **Fencing the Old Worker**:
   When Worker B updates `lease_generation = new_lease_gen` in `job_side_effects`:
   If Worker A awakens as a zombie and attempts to update `status = 'CONFIRMED'`, the conditional SQL update:
   `WHERE side_effect_id = ? AND lease_generation = ?`
   matches 0 rows. Worker A's update is rejected, and Worker A discovers it is fenced out.

4. **Deterministic Branch Scoping**:
   - Default branch: `ai-cloud/job-{job_id[:8]}`.
   - Custom branch: `ai-cloud/job-{job_id[:8]}-{custom_branch_name}`.
   - A worker **never** pushes to or resets a branch that does not begin with its unique job prefix.

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
    Ctrl->>Thread: set _abort_event (Cooperative tier 1)
    
    par Cooperative Check (Tier 1)
        Thread->>Ctrl: is_aborted()? (skipped if blocked in socket)
    and Bounded Forced Provider Kill (Tier 2)
        Ctrl->>Sandbox: Sandbox.kill(sandbox_id) [Timeout: 5.0s]
        alt Sandbox connection breaks
            Sandbox-->>Thread: Connection reset / socket EOF
            Note over Thread: Synchronous call unblocks; thread exits
        else Socket remains hung (transport wedged)
            Note over Watchdog: Worker process fails to exit within 15s grace period
        end
    end

    Note over Sup: Supervisor Grace Period Expiry (Tier 3)
    Sup->>Sup: verified_pid = registry.get(worker_inst)
    Sup->>Sup: os.kill(verified_pid, SIGTERM)
    Sup->>Sup: time.sleep(2.0)
    alt Still alive
        Sup->>Sup: os.kill(verified_pid, SIGKILL)
        Sup->>Sup: os.waitpid(verified_pid, 0)
        Sup->>Sup: reap_token = record_process_reap(...)
        Sup->>DB: supervisor_finalize_job(proof={'type': 'FORCED_KILL', 'reap_token': reap_token})
        Note over Sup: Slot freed deterministically; orphan reconciler destroys sandbox
    end
```

1. **Tier 1 — Cooperative Interruption**:
   - Monitored between steps, before LLM calls, and before tool invocations via `ctrl.is_aborted()`.
   - LLM calls enforce bounded client timeout of **60 seconds** in LiteLLM (`timeout=60.0`).
   - Sandbox commands enforce bounded timeout of **300 seconds** in `sandbox.run_command(..., timeout=300)`.
2. **Tier 2 — In-Process Provider Kill (Implementation Hypothesis)**:
   - If execution exceeds turn timeout or cancellation is requested, the watchdog thread triggers `ctrl.abort()`.
   - The watchdog thread invokes `Sandbox.kill(sandbox_id)` with a **5.0-second timeout**.
   - If transport unblocks, the synchronous call raises an exception, and the thread exits cleanly.
   - **Empirical Boundary**: Because socket teardown behavior on remote container kill depends on low-level transport and client implementation, Tier 2 is treated as an optimization. **Tier 3 is the authoritative enforcement boundary.**
3. **Tier 3 — Supervisor Process Escalation Fallback (Essential Boundary)**:
   - If the worker process does not exit within its **15-second shutdown grace period**, the supervisor process watchdog executes deterministic OS termination:
     - Verifies worker PID against `start_time` in `worker_instances`.
     - Sends `SIGTERM`. If child remains alive after 2.0 seconds, issues `SIGKILL`.
     - Reaps child with `os.waitpid()`, extracting exit status and signal.
     - Authoritatively records reap evidence with `record_process_reap()`.
     - Calls `supervisor_finalize_job(target_status='CANCELLED'|'TIMED_OUT', proof={'type': 'FORCED_KILL', 'reap_token': token})`.
     - Releases the process slot deterministically and schedules background sandbox reconciliation.

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
| **5** | **Supervisor Non-Forgeable Authority** | Supervisor attempts `supervisor_finalize_job` on active worker without proof. | Fails closed; requires verified proof (`PROCESS_DEAD`, `FORCED_KILL` with reap token, or `LEASE_EXPIRED`). |
| **6** | **Supervisor Forced Termination Fallback** | Worker process hangs in socket; watchdog triggers forced kill. | Supervisor sends `SIGTERM`/`SIGKILL`, reaps child, generates reap token, finalizes job, frees slot. |
| **7** | **Blocked Command Forced Interruption** | Active tool call hangs; timeout watchdog trips. | Watchdog kills sandbox; synchronous call unblocks; supervisor falls back to SIGKILL if unblocked call fails. |
| **8** | **Crash & Re-Claim Pre-Condition** | Worker killed via `SIGKILL` while holding active sandbox. | Job has `ORPHANED` status; `claim_next_job()` strictly refuses claim until confirmed `CLEANED`. |
| **9** | **Cleanup Escalation & Authenticated Operator Resolution** | Reconciler encounters repeated provider 500 errors (10 times). | Status becomes `MANUAL_INTERVENTION_REQUIRED`; operator API validates token, attributes principal, resolves status without reviving terminal job. |
| **10** | **Abandoned PENDING Side-Effect Recovery** | Worker A crashes after creating PR but before CONFIRMED. | Worker B reclaims job, discovers PENDING intent, verifies existing PR on GitHub, marks CONFIRMED without duplicating PR. Old Worker A fenced. |
| **11** | **Fail-Closed PR Reconciliation** | GitHub API raises 500/timeout during reconciliation check. | Worker fails closed; does **not** create duplicate PR; retries reconciliation cleanly. |
| **12** | **PR Idempotency across All States** | PR was merged/closed on GitHub before worker retry. | Worker detects existing closed/merged PR in `job_side_effects`, re-attaches cleanly, does not duplicate. |
| **13** | **Supervisor Crash-Loop Protection** | Child worker process repeatedly crashes on startup. | Supervisor applies exponential backoff ($1s \to 30s$); pauses slot after 5 crashes. |
| **14** | **Spawned Worker Credential Isolation** | Inspect child process `/proc/<pid>/cmdline`, environment, and `job_events`. | Zero plaintext GitHub tokens or LLM API keys leaked across IPC boundaries. |

---

## 9. Verification Gate & Commit Evidence

- **Specification Base Commit**: `49b74ed69bc5867ef7b34c0d3b08d0d66ab3e078`
- **Main Baseline**: `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)
- **Scope Commitment**: `app.py` remains untouched. Phase B.2 implementation (`worker.py`) will not start until this specification is authorized.

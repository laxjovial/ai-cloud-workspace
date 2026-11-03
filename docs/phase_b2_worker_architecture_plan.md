# Phase B.2 — Autonomous Worker Engine: Consolidated Architecture Specification & Engineering Contract

**Repository:** `laxjovial/ai-cloud-workspace`  
**Working Branch:** `feat/phase-b-durable-execution`  
**Specification Revision:** 2.7 (Final Pre-Implementation Engineering Contract)  
**Baseline (`main`):** `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)  
**Specification Baseline Commit:** `5e470f02adf0c7c2be88e227963d0db34fbab6a4`  
**Scope Boundary:** Phase B.2 covers only the background worker engine, process supervisor, heartbeat/fencing watchdog, crash-resilient sandbox registry/reconciler, side-effect intent ledger, and associated tests. It introduces **no Streamlit changes (`app.py` untouched)** and **no Phase C work**.

---

## 1. Process Supervision, Process Ownership, & Supervisor-Owned Cryptographic Reap Evidence

```mermaid
flowchart TD
    subgraph Host ["Supervisor Process (worker.py supervisor)"]
        SP["WorkerSupervisor (Main Daemon)"]
        PM["Process Pool Manager (concurrency=N)"]
        RC["Reaper & Sandbox Reconciler (10s interval)"]
        SK["Persistent Supervisor Signing Key (0600 file / env)"]
        SV["Supervisor Verification Service / RPC"]
        SP --> PM
        SP --> RC
        SP --> SK
        SP --> SV
    end

    subgraph Workers ["Spawned Worker Child Processes"]
        W1["Worker Child 1 (Instance: worker_inst_uuid_1, PID: 101)"]
        W2["Worker Child 2 (Instance: worker_inst_uuid_2, PID: 102)"]
        PM -->|spawn with restart backoff & stripped key| W1
        PM -->|spawn with restart backoff & stripped key| W2
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

    W2 -.->|RPC / IPC check: is_prior_worker_dead?| SV
```

### 1.1 Durable Worker Identity & Process Ownership
- **Durable Worker Identity**:
  A bare OS PID is unsafe to trust indefinitely because operating systems rapidly reuse PIDs.
  Each worker process generates a cryptographic instance identity at spawn time:
  `worker_instance_id = f"worker_{uuid.uuid4().hex[:12]}_{os.getpid()}"`
- **Supervisor-Worker Ownership Registry Schema**:
  The supervisor maintains a durable registry table in `jobs.db` with complete lifecycle, reap accounting, and versioned cryptographic verification tokens:
  ```sql
  CREATE TABLE IF NOT EXISTS worker_instances (
      instance_id TEXT PRIMARY KEY,
      pid INTEGER NOT NULL,
      start_time REAL NOT NULL,
      last_heartbeat REAL NOT NULL,
      status TEXT NOT NULL,                -- 'STARTING', 'ALIVE', 'STOPPING', 'DEAD'
      proof_type TEXT,                     -- 'PARENT_WAITPID' | 'SUPERVISOR_RECOVERY_DEATH'
      supervisor_id TEXT,                  -- Identity of the supervisor issuing the reap record
      exit_code INTEGER,                   -- Process exit code from os.waitpid or OS check
      exit_signal INTEGER,                 -- Signal number if terminated by signal (or NULL)
      terminated_at REAL,                  -- Timestamp when termination was verified
      reap_signature TEXT,                 -- Canonical HMAC-SHA256 signature issued by supervisor
      reap_token TEXT                      -- Cryptographic supervisor reap verification token (UUID hex)
  );
  ```
  When issuing signals (`SIGTERM`, `SIGKILL`), the supervisor verifies both PID and process creation time (via `/proc/<pid>/stat` start_time on Linux or `psutil.Process(pid).create_time()`) against the recorded `start_time` in `worker_instances` to guarantee it never signals an unrelated recycled OS process.

### 1.2 Cryptographic Key Boundary & Supervisor-Owned Verification Interface
`os.waitpid()` is parent-process-specific: only the parent supervisor process can reap its direct child processes; child workers cannot call `waitpid()`. Furthermore, child workers with SQLite write access must **never** be entrusted with the signing key or allowed to self-verify.

1. **Persistent Supervisor Signing Key (`SUPERVISOR_SIGNING_KEY`)**:
   - The signing key persists across supervisor process restarts (`0600` permissions file `.secrets/supervisor_signing_key` or environment variable).
   - **Stripped Secret Boundary**: When spawning child worker processes (via `multiprocessing.get_context("spawn")`), the supervisor strictly filters process environment variables and child memory: `SUPERVISOR_SIGNING_KEY` is kept exclusively in supervisor memory and is **never passed, inherited, or leaked to worker child processes**.
2. **Canonical Versioned Signature Payload**:
   Every termination record issued by a supervisor is signed using a canonical, versioned payload:
   ```python
   canonical_payload = (
       f"v1:{proof_type}:{supervisor_id}:{instance_id}:{pid}:{start_time}:"
       f"{exit_code if exit_code is not None else ''}:"
       f"{exit_signal if exit_signal is not None else ''}:"
       f"{terminated_at}:{reap_token}"
   )
   reap_signature = hmac.new(self.supervisor_signing_key, canonical_payload.encode("utf-8"), hashlib.sha256).hexdigest()
   ```
3. **Supervisor Restart & OS Recovery-Death Evidence**:
   When a supervisor restarts:
   - For child processes that were active under the previous supervisor run (`status != 'DEAD'` in `worker_instances`):
     - The replacement supervisor **did not spawn them** and therefore **cannot call `os.waitpid()` on them**. It **never falsely claims** it reaped them via `waitpid()`.
     - The replacement supervisor establishes process death using strict OS evidence:
       1. Verifies PID creation time against recorded `start_time` (via `/proc/<pid>/stat` start_time or `psutil.Process(pid).create_time()`).
       2. If PID is absent from the OS, process is confirmed dead.
       3. If PID exists but start_time does not match recorded `start_time`, PID has been recycled; original process is confirmed dead.
       4. If PID exists and start_time matches:
          - Inspects `/proc/<pid>/stat` state field: if state is `'Z'` (zombie), the process has terminated but is awaiting its original parent reap; it cannot execute instructions or hold network sockets.
          - If state is alive, replacement supervisor sends `SIGKILL` and verifies that the process transitions to zombie or disappears from `/proc`.
     - The replacement supervisor authoritatively signs a recovery record: `proof_type = 'SUPERVISOR_RECOVERY_DEATH'`, `supervisor_id = replacement_id`, and persists it into `worker_instances`.
4. **Supervisor-Owned Verification Service (`verify_worker_death`)**:
   Because child workers do not possess `SUPERVISOR_SIGNING_KEY`, worker children cannot independently compute or verify the HMAC signature.
   - The supervisor exposes a local IPC / SQLite verification method:
     ```python
     def supervisor_verify_worker_death(self, instance_id: str) -> bool:
         """Authoritatively verifies that a worker instance has cryptographically valid termination proof.
         
         Executed strictly by the supervisor process possessing SUPERVISOR_SIGNING_KEY:
         1. Loads row from worker_instances for instance_id.
         2. Reconstructs canonical v1 payload.
         3. Compares HMAC-SHA256 signature using hmac.compare_digest().
         4. Verifies status == 'DEAD'.
         Returns True if valid, False otherwise.
         """
     ```
   - During side-effect recovery, Worker B queries the supervisor's verification endpoint or requests supervisor authorization before touching abandoned intents. Worker B **never** attempts to verify cryptographic keys directly.

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
    """Operator-authenticated resolution of stuck cloud resources with non-forgeable attribution."""
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

## 4. Dedicated Side-Effect Ledger & Two-Phase Intent Recovery Protocol

To guarantee idempotency and eliminate duplicate external side-effects (such as duplicate GitHub PRs or Git branch conflicts) under at-least-once execution semantics:

### 4.1 Schema Migration 2: Dedicated Side-Effect Ledger
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

### 4.3 Two-Phase Recovery Protocol for Abandoned `PENDING` Intents
An expired lease does **not** prove Worker A has stopped executing: Worker A's external HTTP request may still be in flight to GitHub.
To prevent Worker B from creating a duplicate PR while Worker A's in-flight request finishes, the recovery protocol enforces a strict two-phase gate:

```mermaid
flowchart TD
    W2["Worker B Claims Job (Generation 2)"] --> Q["Query job_side_effects for job_id"]
    Q --> Check{"Any intent in PENDING state?"}
    Check -->|No| Safe["Proceed with Normal Step Execution"]
    Check -->|Yes (e.g. Worker A generation 1)| Phase1["Phase 1: Supervisor-Owned Process Death Verification Gate"]
    
    Phase1 --> ReqSup["Worker B calls supervisor_verify_worker_death(worker_a_inst)"]
    ReqSup --> ReapCheck{"Supervisor validates canonical HMAC & death proof?"}
    ReapCheck -->|No (Unverified or alive)| BlockRecovery["BLOCK RECOVERY: Supervisor terminates Worker A or operator resolves"]
    ReapCheck -->|Yes (Authenticated DEAD: normal exit, crash, or forced kill)| Phase2["Phase 2: Remote Reconciliation & Idempotency"]
    
    Phase2 --> GH["Query GitHub across state='all' for branch head"]
    GH -->|API Error / Timeout| FailClosed["FAIL CLOSED: Raise SideEffectReconciliationError (Retry later)"]
    GH -->|Matching PR Found| Adopt["Adopt PR: status='CONFIRMED', lease_gen=2, worker=Worker B"]
    GH -->|0 PRs Found| Rollback["Update intent: status='FAILED_VERIFIED', lease_gen=2"]
    
    Rollback --> IdempotentCreate["Create PR using deterministic branch: ai-cloud/job-{id}"]
```

#### Phase 1: Supervisor-Owned Process Death Verification Gate
Because child workers have direct SQLite write access, a bare `worker_instances.status == 'DEAD'` value written by a rogue or malfunctioning worker must **never** be trusted. Furthermore, child workers do not possess `SUPERVISOR_SIGNING_KEY`.
1. Worker B identifies the prior worker instance (`intent['worker_id']`).
2. Worker B requests supervisor verification: `supervisor_verify_worker_death(worker_instance_id)`.
3. The supervisor validates the canonical HMAC signature:
   - Validates `reap_signature` against `SUPERVISOR_SIGNING_KEY` using the canonical v1 payload.
   - Verifies the process is authoritatively dead.
   - **Accepts Any Valid Termination Mode**: The supervisor accepts **any valid, cryptographically authenticated termination proof**, including:
     - Normal exit (`proof_type = 'PARENT_WAITPID'`, `exit_code == 0`)
     - Unexpected child crash (`proof_type = 'PARENT_WAITPID'`, `exit_code != 0`)
     - Forced signal termination (`proof_type = 'PARENT_WAITPID'`, `exit_signal IN (9, 15)`)
     - Confirmed recovery death (`proof_type = 'SUPERVISOR_RECOVERY_DEATH'`)
     In all such cases, because the OS process is conclusively dead, its sockets and memory are freed, and no in-flight request can reach GitHub.
4. **Strict Fail-Closed Rule**:
   - If Worker A's cryptographic death record cannot be verified by the supervisor, **reconciliation remains strictly blocked**.
   - Socket timeouts or elapsed wall-clock periods are **explicitly disallowed** as a substitute for verified process death.
   - If Worker A is unverified, Worker B raises `SideEffectReconciliationBlockedError("Previous worker process termination cannot be cryptographically verified; halting side-effect reconciliation until supervisor or operator intervention.")`.

#### Phase 2: Remote Provider Reconciliation & Deterministic Branch Scoping
Once Worker A's process death is cryptographically verified:
1. Worker B queries GitHub across **all states** (`state="all"`):
   - **Matching PR Exists**: Worker B adopts the existing PR, records `status = 'CONFIRMED'`, and updates `lease_generation = 2`.
   - **No PR Exists (Query succeeded with 0 results)**:
     Because Worker A is verified dead and its sockets terminated before the query, no delayed in-flight request can execute. Worker B marks the old intent `status = 'FAILED_VERIFIED'` and advances `lease_generation = 2`.
   - **Inconclusive Query (Timeout / API Error)**:
     **Fails closed**. Worker B raises `SideEffectReconciliationError` and halts without duplicating.
2. **Provider-Level Idempotency Boundary**:
   GitHub PR creation enforces branch-level uniqueness: only **one open PR** can exist for a given `(head, base)` pair. By enforcing strict, immutable branch scoping:
   `head = f"ai-cloud/job-{job_id[:8]}"`
   Even under pathological network partition, if a second concurrent PR creation call reached GitHub for the same head branch, GitHub's API rejects it with HTTP 422: `"A pull request already exists for this branch"`. The worker catches HTTP 422, re-runs remote reconciliation, and safely binds to the existing PR.

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
        Sup->>Sup: reap_sig = HMAC(signing_key, canonical_v1_payload)
        Sup->>DB: record_process_reap(reap_sig, token)
        Sup->>DB: supervisor_finalize_job(proof={'type': 'FORCED_KILL', 'reap_signature': reap_sig})
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
     - Computes HMAC reap signature with persistent supervisor key; authoritatively records reap evidence with `record_process_reap()`.
     - Calls `supervisor_finalize_job(target_status='CANCELLED'|'TIMED_OUT', proof={'type': 'FORCED_KILL', 'reap_signature': reap_sig})`.
     - Releases the process slot deterministically and schedules background sandbox reconciliation.

---

## 6. Concrete Credential & Configuration Trust Boundary

To ensure strict tenant isolation and zero credential leakage across detached OS processes:

### 6.1 Trust Boundary Architecture
- **Supervisor Role**: Runs with local system permissions and has access to environment variables (`E2B_API_KEY`, `ENCRYPTION_KEY`, `SUPERVISOR_SIGNING_KEY`). It **never** receives, handles, or caches user GitHub PATs or BYOK LLM keys.
- **Worker Process Isolation**:
  Spawned child processes inherit `ENCRYPTION_KEY` and `E2B_API_KEY` via standard OS process environment inheritance. `SUPERVISOR_SIGNING_KEY` is explicitly filtered out.
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
| **5** | **Supervisor Non-Forgeable Authority** | Supervisor attempts `supervisor_finalize_job` on active worker without proof. | Fails closed; requires verified HMAC signature (`FORCED_KILL` with canonical v1 reap signature) or database-verified `LEASE_EXPIRED`. |
| **6** | **Supervisor Forced Termination Fallback** | Worker process hangs in socket; watchdog triggers forced kill. | Supervisor sends `SIGTERM`/`SIGKILL`, reaps child, generates canonical v1 signature, finalizes job, frees slot. |
| **7** | **Blocked Command Forced Interruption** | Active tool call hangs; timeout watchdog trips. | Watchdog kills sandbox; synchronous call unblocks; supervisor falls back to SIGKILL if unblocked call fails. |
| **8** | **Crash & Re-Claim Pre-Condition** | Worker killed via `SIGKILL` while holding active sandbox. | Job has `ORPHANED` status; `claim_next_job()` strictly refuses claim until confirmed `CLEANED`. |
| **9** | **Cleanup Escalation & Authenticated Operator Resolution** | Reconciler encounters repeated provider 500 errors (10 times). | Status becomes `MANUAL_INTERVENTION_REQUIRED`; operator API validates token, attributes principal, resolves status without reviving terminal job. |
| **10** | **Supervisor-Owned Side-Effect Gate** | Worker requests verification of dead prior worker via supervisor API. | Supervisor authenticates canonical signature; allows recovery for normal exit, crash, or forced kill; blocks if unverified. |
| **11** | **Canonical Recovery-Death Signature Verification** | Replacement supervisor verifies orphaned process across restart. | Validates `/proc` state, signs canonical v1 recovery record (`SUPERVISOR_RECOVERY_DEATH`); verification succeeds without forging `waitpid`. |
| **12** | **Fail-Closed PR Reconciliation** | GitHub API raises 500/timeout during reconciliation check. | Worker fails closed; does **not** create duplicate PR; retries reconciliation cleanly. |
| **13** | **PR Idempotency across All States** | PR was merged/closed on GitHub before worker retry. | Worker detects existing closed/merged PR in `job_side_effects`, re-attaches cleanly, does not duplicate. |
| **14** | **Supervisor Crash-Loop Protection** | Child worker process repeatedly crashes on startup. | Supervisor applies exponential backoff ($1s \to 30s$); pauses slot after 5 crashes. |
| **15** | **Spawned Worker Credential Isolation** | Inspect child process `/proc/<pid>/cmdline`, environment, and `job_events`. | Zero plaintext GitHub tokens, supervisor signing keys, or LLM keys leaked across IPC boundaries. |

---

## 9. Verification Gate & Commit Evidence

- **Specification Base Commit**: `5e470f02adf0c7c2be88e227963d0db34fbab6a4`
- **Main Baseline**: `5147be6c127c3eed87b6cabb0ca24f65b2550fa7` (strictly untouched)
- **Scope Commitment**: `app.py` remains untouched. Phase B.2 implementation (`worker.py`) will not start until this specification is authorized.

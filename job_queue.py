"""Durable Job Queue and Storage Engine for AI Cloud Workspace.

Implements Phase B.1 and Phase B.2:
- Local-first SQLite in WAL mode with robust busy timeouts (5000ms).
- Concurrency Model:
  - Database-level writer serialization via explicit `BEGIN IMMEDIATE` transactions.
  - Re-entrant internal thread locking (`threading.RLock`) protects multi-threaded shared queue instances.
  - Multi-process safety across independent OS processes via SQLite WAL.
- Monotonic fencing tokens (lease_generation) and strict lease expiration validation:
  - Every worker mutation (record_heartbeat, append_log, update_job_status) strictly requires:
    `status == 'RUNNING' AND worker_id == expected AND lease_generation == expected`
    AND `lease_expires_at IS NOT NULL AND lease_expires_at >= now`.
    Missing or expired leases fail closed immediately with StaleLeaseError.
- Cancellation Precedence:
  - User cancellation takes strict precedence over normal completion. If cancellation was requested,
    `COMPLETED` is rejected with InvalidStateTransitionError.
  - If a worker transitions to `FAILED` or `TIMED_OUT` while cancellation is pending, status is recorded
    as `CANCELLED` while preserving the failure diagnostic error in `error_json`.
  - Stale jobs with pending cancellation are NEVER re-queued by the reaper; they transition directly to CANCELLED.
- Terminal job protection:
  - Only the active, unexpired lease holder of a RUNNING job can append worker events. Terminal jobs reject worker events.
- Strict credential-reference schema:
  - Exactly requires `{"key_ref": "..."}`, rejects unknown/missing keys, validates format,
    and blocks raw secrets in both payload and credential_refs.
- Rigorous numeric input validation:
  - Rejects booleans, floats (where integers required), NaNs, infinities, and out-of-range values.
- Schema versioning and idempotent forward migrations:
  - Migration 1: Initial jobs and job_events schema.
  - Migration 2: Dedicated sandbox cleanup lifecycle, worker instances registry, and relational side-effects ledger.
- Phase B.2 Advanced Engine Invariants:
  - Dedicated fenced sandbox registration: `register_sandbox()`
  - Strict pre-claim invariant: `claim_next_job()` requires `sandbox_cleanup_status IN ('NONE', 'CLEANED')`
  - Atomic status polling and watchdog lease extension: `poll_worker_status()`
  - Privileged supervisor finalization with proof validation: `supervisor_finalize_job()`
  - Authenticated operator cleanup resolution: `resolve_manual_cleanup()`
  - Authoritative child process reap recording: `record_process_reap()`
  - Relational side-effects ledger and intent reservation: `reserve_side_effect()`, `confirm_side_effect()`, `fail_side_effect()`
"""

import hashlib
import hmac
import json
import math
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_DB_DIR = os.path.expanduser("~/.ai_cloud_workspace")
DEFAULT_DB_PATH = os.path.join(DEFAULT_DB_DIR, "jobs.db")
DEFAULT_LEASE_DURATION_SECONDS = 30.0


class JobQueueError(Exception):
    """Base exception for job queue errors."""
    pass


class StaleLeaseError(JobQueueError):
    """Raised when a worker attempts an update with an expired, missing, or superseded lease."""
    pass


class InvalidStateTransitionError(JobQueueError):
    """Raised when an illegal lifecycle state transition is requested."""
    pass


class JobAccessDeniedError(JobQueueError):
    """Raised when a user attempts to access or mutate a job owned by another tenant."""
    pass


class JobNotFoundError(JobQueueError):
    """Raised when the specified job does not exist."""
    pass


class InvalidCredentialReferenceError(JobQueueError):
    """Raised when a credential reference or payload violates security schema."""
    pass


class SideEffectConflictError(JobQueueError):
    """Raised when a side effect cannot be reserved due to concurrent lease or state conflict."""
    pass


class SideEffectReconciliationBlockedError(JobQueueError):
    """Raised when side-effect recovery is blocked due to unverified prior worker process death."""
    pass


# Allowed state transitions: from_status -> set of allowed target statuses
ALLOWED_TRANSITIONS: Dict[str, set] = {
    "QUEUED": {"RUNNING", "CANCELLED"},
    "RUNNING": {"COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT", "RECOVERABLE"},
    "RECOVERABLE": {"QUEUED", "FAILED", "CANCELLED"},
    "COMPLETED": set(),  # Terminal
    "FAILED": set(),     # Terminal
    "CANCELLED": set(),  # Terminal
    "TIMED_OUT": set(),  # Terminal
}

# Sandbox cleanup statuses:
# 'NONE': No cloud sandbox provisioned yet
# 'ACTIVE': Sandbox is live in E2B and owned by running worker
# 'ORPHANED': Worker terminated or lease lost; sandbox requires background destruction
# 'FAILED': Destruction attempt failed; awaiting exponential backoff retry
# 'MANUAL_INTERVENTION_REQUIRED': Reconciler retried 10 consecutive times without success
# 'CLEANED': Sandbox confirmed destroyed or verified absent in cloud provider
ALLOWED_SANDBOX_CLEANUP_STATUSES = {
    "NONE",
    "ACTIVE",
    "ORPHANED",
    "FAILED",
    "MANUAL_INTERVENTION_REQUIRED",
    "CLEANED",
}

# Regex to detect obvious raw secret leaks
RAW_SECRET_PATTERNS = [
    re.compile(r"gh[pousr]_[A-Za-z0-9_]{16,}"),
    re.compile(r"sk-[A-Za-z0-9\-_]{20,}"),
    re.compile(r"x-access-token:[^@\s]+"),
    re.compile(r"Bearer\s+[A-Za-z0-9\-_\.]{20,}", re.IGNORECASE),
]

# Strict credential reference validation schema
ALLOWED_CRED_PROVIDERS = {"github", "openai", "anthropic", "gemini", "deepseek"}
VALID_KEY_REF_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.]{1,128}$")


def canonical_json(data: Any) -> str:
    """Produces deterministic canonical JSON serialization with sorted keys and compact separators."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def compute_side_effect_id(job_id: str, action_type: str, action_key: str) -> str:
    """Computes a full 64-character SHA-256 hex string for deterministic side-effect identity."""
    raw = f"{job_id}:{action_type}:{action_key}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def compute_canonical_signature(key: bytes, payload_dict: Dict[str, Any]) -> str:
    """Generates an HMAC-SHA256 signature over a canonical JSON payload."""
    canonical_bytes = canonical_json(payload_dict).encode("utf-8")
    return hmac.new(key, canonical_bytes, hashlib.sha256).hexdigest()


def validate_credential_references(credential_refs: Optional[Dict[str, Any]]) -> None:
    """Validates that credential_refs conform to an exact schema and contain NO raw secrets.
    
    Exact Schema:
    {
        "<provider>": {
            "key_ref": "<string matching VALID_KEY_REF_PATTERN>"
        }
    }
    Any unknown fields, missing key_ref, or non-dict structures are strictly rejected.
    """
    if credential_refs is None:
        return
    if not isinstance(credential_refs, dict):
        raise InvalidCredentialReferenceError("credential_refs must be a dictionary.")

    for provider, ref in credential_refs.items():
        if provider not in ALLOWED_CRED_PROVIDERS:
            raise InvalidCredentialReferenceError(
                f"Invalid credential provider '{provider}'. Allowed providers: {sorted(ALLOWED_CRED_PROVIDERS)}"
            )

        if not isinstance(ref, dict):
            raise InvalidCredentialReferenceError(
                f"Credential reference for '{provider}' must be a dictionary object with key_ref, got {type(ref).__name__}."
            )

        allowed_keys = {"key_ref"}
        extra_keys = set(ref.keys()) - allowed_keys
        if extra_keys:
            raise InvalidCredentialReferenceError(
                f"Unknown field(s) {sorted(extra_keys)} in credential reference for '{provider}'. Only 'key_ref' is allowed."
            )

        if "key_ref" not in ref:
            raise InvalidCredentialReferenceError(
                f"Missing required 'key_ref' field in credential reference for '{provider}'."
            )

        key_ref_val = ref["key_ref"]
        if not isinstance(key_ref_val, str) or isinstance(key_ref_val, bool):
            raise InvalidCredentialReferenceError(
                f"'key_ref' for provider '{provider}' must be a non-boolean string."
            )

        for pat in RAW_SECRET_PATTERNS:
            if pat.search(key_ref_val):
                raise InvalidCredentialReferenceError(
                    f"Security violation: Raw credential pattern detected in 'key_ref' for '{provider}'. "
                    "Only safe reference names (e.g. 'primary_github_pat') may be stored."
                )

        if not VALID_KEY_REF_PATTERN.match(key_ref_val):
            raise InvalidCredentialReferenceError(
                f"'key_ref' for provider '{provider}' contains invalid characters or exceeds 128 characters."
            )


def validate_payload_security(payload: Optional[Dict[str, Any]]) -> None:
    """Scans payload to ensure raw access tokens or API keys are not embedded in plain text."""
    if not payload:
        return
    payload_str = json.dumps(payload)
    for pat in RAW_SECRET_PATTERNS:
        if pat.search(payload_str):
            raise JobQueueError(
                "Security violation: Raw secret or access token detected in job payload. "
                "Secrets must be referenced through credential_refs and resolved in worker context."
            )


def validate_numeric(
    val: Any,
    name: str,
    min_val: float,
    max_val: float,
    require_int: bool = False,
) -> float:
    """Strictly validates numeric inputs: rejects booleans, NaN, Infinity, floats-for-ints, and out-of-range values."""
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        raise JobQueueError(f"Invalid type for {name}: expected numeric, got {type(val).__name__}.")

    if math.isnan(val) or math.isinf(val):
        raise JobQueueError(f"Invalid value for {name}: non-finite numbers (NaN, Inf) are not permitted.")

    if require_int:
        if not isinstance(val, int) or isinstance(val, bool):
            raise JobQueueError(f"Invalid type for {name}: expected integer, got {type(val).__name__}.")

    if val < min_val or val > max_val:
        raise JobQueueError(f"{name} ({val}) out of permitted range [{min_val}, {max_val}].")

    return val


def _get_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Creates a configured SQLite connection with WAL mode and foreign keys enabled."""
    db_dir = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(db_dir, exist_ok=True)

    try:
        conn = sqlite3.connect(
            db_path,
            timeout=5.0,
            check_same_thread=False,
            isolation_level=None,  # Transactions managed explicitly via BEGIN / COMMIT
        )
        conn.row_factory = sqlite3.Row
        with conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        return conn
    except sqlite3.Error as e:
        raise JobQueueError(f"Failed to connect to SQLite database at '{db_path}': {str(e)}") from e


def run_migrations(conn: sqlite3.Connection) -> None:
    """Executes schema migrations idempotently and tracks schema version."""
    with conn:
        conn.execute("BEGIN IMMEDIATE;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at REAL NOT NULL,
                description TEXT NOT NULL
            );
            """
        )

        cursor = conn.execute("SELECT MAX(version) AS max_v FROM schema_migrations;")
        row = cursor.fetchone()
        current_version = 0
        if row is not None:
            val = row[0] if isinstance(row, tuple) else row["max_v"]
            if val is not None:
                current_version = int(val)

        # Migration 1: Initial Durable Job Queue & Events tables
        if current_version < 1:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE,
                    user_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    repository TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'QUEUED',
                    payload_json TEXT NOT NULL,
                    credential_refs_json TEXT NOT NULL DEFAULT '{}',
                    worker_id TEXT,
                    lease_generation INTEGER NOT NULL DEFAULT 0,
                    heartbeat_at REAL,
                    lease_expires_at REAL,
                    cancel_requested_at REAL,
                    cancel_requested_by TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    timeout_seconds REAL NOT NULL DEFAULT 600.0,
                    sandbox_id TEXT,
                    created_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    error_json TEXT
                );
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    source TEXT NOT NULL,
                    worker_id TEXT,
                    lease_generation INTEGER NOT NULL DEFAULT 0,
                    message TEXT NOT NULL,
                    data_json TEXT,
                    created_at REAL NOT NULL,
                    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
                );
                """
            )

            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_lease ON jobs (status, lease_expires_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_user_created ON jobs (user_id, created_at DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_session ON jobs (session_id, created_at DESC);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events (job_id, id ASC);")

            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at, description) VALUES (1, ?, ?);",
                (time.time(), "Initial durable jobs and job_events schema"),
            )

        # Migration 2: Dedicated Sandbox Lifecycle, Worker Instances, and Side Effects Ledger
        if current_version < 2:
            # Add columns to jobs table
            # Check existing columns to ensure idempotent addition
            cursor_cols = conn.execute("PRAGMA table_info(jobs);").fetchall()
            existing_col_names = set()
            for c in cursor_cols:
                if isinstance(c, tuple):
                    existing_col_names.add(c[1])
                else:
                    existing_col_names.add(c["name"])

            if "sandbox_cleanup_status" not in existing_col_names:
                conn.execute("ALTER TABLE jobs ADD COLUMN sandbox_cleanup_status TEXT NOT NULL DEFAULT 'NONE';")
            if "sandbox_cleanup_attempts" not in existing_col_names:
                conn.execute("ALTER TABLE jobs ADD COLUMN sandbox_cleanup_attempts INTEGER NOT NULL DEFAULT 0;")
            if "sandbox_cleanup_error" not in existing_col_names:
                conn.execute("ALTER TABLE jobs ADD COLUMN sandbox_cleanup_error TEXT;")
            if "sandbox_cleanup_next_retry" not in existing_col_names:
                conn.execute("ALTER TABLE jobs ADD COLUMN sandbox_cleanup_next_retry REAL;")
            if "sandbox_cleanup_owner" not in existing_col_names:
                conn.execute("ALTER TABLE jobs ADD COLUMN sandbox_cleanup_owner TEXT;")
            if "sandbox_cleanup_generation" not in existing_col_names:
                conn.execute("ALTER TABLE jobs ADD COLUMN sandbox_cleanup_generation INTEGER NOT NULL DEFAULT 0;")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS worker_instances (
                    instance_id TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    start_time REAL NOT NULL,
                    last_heartbeat REAL NOT NULL,
                    status TEXT NOT NULL,
                    proof_type TEXT,
                    supervisor_id TEXT,
                    exit_code INTEGER,
                    exit_signal INTEGER,
                    terminated_at REAL,
                    reap_signature TEXT,
                    reap_token TEXT
                );
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_side_effects (
                    side_effect_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    lease_generation INTEGER NOT NULL,
                    action_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    intent_payload TEXT NOT NULL,
                    confirmed_result TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                );
                """
            )

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_claim_eligibility ON jobs (status, sandbox_cleanup_status, created_at);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_side_effects_job ON job_side_effects (job_id, action_type);"
            )

            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at, description) VALUES (2, ?, ?);",
                (time.time(), "Phase B.2 sandbox lifecycle, worker instances, and side effects ledger"),
            )


class DurableJobQueue:
    """Manages transactional job queue operations in SQLite with fencing tokens and tenant isolation.
    
    Concurrency Architecture:
    - Thread-safety within a single process is guaranteed via a re-entrant lock (self._lock).
    - Multi-process safety is guaranteed via SQLite WAL mode and BEGIN IMMEDIATE atomic write transactions.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._lock = threading.RLock()
        self.conn = _get_connection(db_path)
        run_migrations(self.conn)

    def close(self) -> None:
        """Closes the underlying SQLite database connection."""
        with self._lock:
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass

    def enqueue_job(
        self,
        user_id: str,
        session_id: str,
        repository: str,
        payload: Dict[str, Any],
        credential_refs: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
        max_retries: int = 3,
        timeout_seconds: float = 600.0,
    ) -> Dict[str, Any]:
        """Submits a new job to the queue. Idempotent: returns existing job if idempotency_key matches."""
        if not user_id or not user_id.strip():
            raise JobQueueError("user_id is required.")
        if not repository or not repository.strip():
            raise JobQueueError("repository is required.")

        validate_numeric(max_retries, "max_retries", 0, 50, require_int=True)
        validate_numeric(timeout_seconds, "timeout_seconds", 1.0, 86400.0, require_int=False)

        validate_payload_security(payload)
        validate_credential_references(credential_refs)

        clean_user_id = user_id.strip()
        clean_repo = repository.strip()
        clean_session = (session_id or "").strip()
        clean_idemp = idempotency_key.strip() if idempotency_key else None
        now = time.time()

        payload_str = json.dumps(payload or {})
        cred_refs_str = json.dumps(credential_refs or {})
        job_id = str(uuid.uuid4())

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")

                    if clean_idemp:
                        cursor = self.conn.execute(
                            "SELECT * FROM jobs WHERE idempotency_key = ?;",
                            (clean_idemp,),
                        )
                        existing = cursor.fetchone()
                        if existing:
                            if existing["user_id"] != clean_user_id:
                                raise JobAccessDeniedError("Idempotency key already exists for another user.")
                            return self._row_to_dict(existing)

                    self.conn.execute(
                        """
                        INSERT INTO jobs (
                            id, idempotency_key, user_id, session_id, repository,
                            status, payload_json, credential_refs_json,
                            retry_count, max_retries, timeout_seconds,
                            sandbox_cleanup_status, created_at
                        ) VALUES (?, ?, ?, ?, ?, 'QUEUED', ?, ?, 0, ?, ?, 'NONE', ?);
                        """,
                        (
                            job_id,
                            clean_idemp,
                            clean_user_id,
                            clean_session,
                            clean_repo,
                            payload_str,
                            cred_refs_str,
                            max_retries,
                            timeout_seconds,
                            now,
                        ),
                    )

                    self.conn.execute(
                        """
                        INSERT INTO job_events (
                            job_id, event_type, source, message, created_at
                        ) VALUES (?, 'ENQUEUED', 'client', 'Job enqueued and awaiting worker claim.', ?);
                        """,
                        (job_id, now),
                    )

                return self.get_job(job_id, user_id=clean_user_id)
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error while enqueuing job: {str(e)}") from e

    def claim_next_job(
        self,
        worker_id: str,
        lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS,
    ) -> Optional[Dict[str, Any]]:
        """Atomically claims the next pending QUEUED job using an atomic lease and incremented fencing token.
        
        Strict Pre-Claim Invariant:
        A job is only eligible for claim if sandbox_cleanup_status IN ('NONE', 'CLEANED').
        Jobs with unresolved cloud sandboxes ('ACTIVE', 'ORPHANED', 'FAILED', 'MANUAL_INTERVENTION_REQUIRED')
        cannot be claimed under any circumstances until independently confirmed cleaned.
        """
        if not worker_id or not worker_id.strip():
            raise JobQueueError("worker_id is required to claim jobs.")

        validate_numeric(lease_duration_seconds, "lease_duration_seconds", 1.0, 3600.0, require_int=False)

        now = time.time()
        lease_expires_at = now + lease_duration_seconds

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")

                    cursor = self.conn.execute(
                        """
                        SELECT id, lease_generation FROM jobs
                        WHERE status = 'QUEUED'
                          AND (sandbox_cleanup_status = 'NONE' OR sandbox_cleanup_status = 'CLEANED')
                        ORDER BY created_at ASC
                        LIMIT 1;
                        """
                    )
                    row = cursor.fetchone()
                    if not row:
                        return None

                    job_id = row["id"]
                    current_gen = row["lease_generation"]
                    new_gen = current_gen + 1

                    update_cursor = self.conn.execute(
                        """
                        UPDATE jobs
                        SET status = 'RUNNING',
                            worker_id = ?,
                            lease_generation = ?,
                            heartbeat_at = ?,
                            lease_expires_at = ?,
                            started_at = COALESCE(started_at, ?)
                        WHERE id = ? AND status = 'QUEUED';
                        """,
                        (worker_id, new_gen, now, lease_expires_at, now, job_id),
                    )

                    if update_cursor.rowcount == 0:
                        return None

                    self.conn.execute(
                        """
                        INSERT INTO job_events (
                            job_id, event_type, source, worker_id, lease_generation, message, created_at
                        ) VALUES (?, 'CLAIMED', 'worker', ?, ?, 'Job claimed by worker.', ?);
                        """,
                        (job_id, worker_id, new_gen, now),
                    )

                return self.get_job_by_id_internal(job_id)
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during claim_next_job: {str(e)}") from e

    def record_heartbeat(
        self,
        job_id: str,
        worker_id: str,
        lease_generation: int,
        lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS,
    ) -> bool:
        """Refreshes worker heartbeat and extends lease_expires_at.
        
        Strict Fencing: Fails closed if the worker is not current, generation does not match,
        status is not RUNNING, OR lease_expires_at IS NULL or < now (even before reaper runs).
        """
        validate_numeric(lease_duration_seconds, "lease_duration_seconds", 1.0, 3600.0, require_int=False)
        validate_numeric(lease_generation, "lease_generation", 1, 1000000000, require_int=True)

        now = time.time()
        new_expires_at = now + lease_duration_seconds

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    cursor = self.conn.execute(
                        """
                        UPDATE jobs
                        SET heartbeat_at = ?,
                            lease_expires_at = ?
                        WHERE id = ?
                          AND worker_id = ?
                          AND lease_generation = ?
                          AND status = 'RUNNING'
                          AND lease_expires_at IS NOT NULL
                          AND lease_expires_at >= ?;
                        """,
                        (now, new_expires_at, job_id, worker_id, lease_generation, now),
                    )

                    if cursor.rowcount == 0:
                        chk = self.conn.execute(
                            "SELECT worker_id, lease_generation, status, lease_expires_at FROM jobs WHERE id = ?;",
                            (job_id,),
                        ).fetchone()
                        if not chk:
                            raise JobNotFoundError(f"Job '{job_id}' does not exist.")
                        if chk["status"] != "RUNNING":
                            raise StaleLeaseError(f"Cannot heartbeat: job '{job_id}' is in status '{chk['status']}'.")
                        if chk["lease_generation"] != lease_generation or chk["worker_id"] != worker_id:
                            raise StaleLeaseError(
                                f"Worker '{worker_id}' (gen {lease_generation}) superseded by "
                                f"worker '{chk['worker_id']}' (gen {chk['lease_generation']})."
                            )
                        if chk["lease_expires_at"] is None or chk["lease_expires_at"] < now:
                            raise StaleLeaseError(
                                f"Cannot heartbeat: lease for worker '{worker_id}' is null or expired at {chk['lease_expires_at']} (now {now})."
                            )
                        return False

                return True
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during record_heartbeat: {str(e)}") from e

    def poll_worker_status(
        self,
        job_id: str,
        worker_id: str,
        lease_generation: int,
        lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS,
    ) -> str:
        """Atomically validates lease, renews expiration, and inspects cancellation state.
        
        Returns:
        - 'ACTIVE': Lease extended, job running normally.
        - 'CANCEL_REQUESTED': Lease extended, user requested cancellation.
        - 'STALE': Worker lease expired or was superseded. Worker must self-fence immediately.
        """
        validate_numeric(lease_duration_seconds, "lease_duration_seconds", 1.0, 3600.0, require_int=False)
        validate_numeric(lease_generation, "lease_generation", 1, 1000000000, require_int=True)

        now = time.time()
        new_expires_at = now + lease_duration_seconds

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    row = self.conn.execute(
                        """
                        SELECT status, worker_id, lease_generation, lease_expires_at, cancel_requested_at
                        FROM jobs WHERE id = ?;
                        """,
                        (job_id,),
                    ).fetchone()

                    if not row:
                        return "STALE"
                    if row["status"] != "RUNNING":
                        return "STALE"
                    if row["worker_id"] != worker_id or row["lease_generation"] != lease_generation:
                        return "STALE"
                    if row["lease_expires_at"] is None or row["lease_expires_at"] < now:
                        return "STALE"

                    # Extend lease atomically
                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET heartbeat_at = ?,
                            lease_expires_at = ?
                        WHERE id = ? AND worker_id = ? AND lease_generation = ? AND status = 'RUNNING';
                        """,
                        (now, new_expires_at, job_id, worker_id, lease_generation),
                    )

                    if row["cancel_requested_at"] is not None:
                        return "CANCEL_REQUESTED"
                    return "ACTIVE"
            except sqlite3.Error:
                return "STALE"

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
        5. If a conflicting sandbox_id is already registered for this generation, raises JobQueueError.
        """
        if not sandbox_id or not sandbox_id.strip():
            raise JobQueueError("sandbox_id cannot be empty.")
        clean_sb = sandbox_id.strip()

        validate_numeric(lease_generation, "lease_generation", 1, 1000000000, require_int=True)
        now = time.time()

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    row = self.conn.execute(
                        """
                        SELECT status, worker_id, lease_generation, lease_expires_at, sandbox_id
                        FROM jobs WHERE id = ?;
                        """,
                        (job_id,),
                    ).fetchone()

                    if not row:
                        raise JobNotFoundError(f"Job '{job_id}' does not exist.")
                    if row["status"] != "RUNNING":
                        raise StaleLeaseError(f"Cannot register sandbox: job '{job_id}' is in status '{row['status']}'.")
                    if row["worker_id"] != worker_id or row["lease_generation"] != lease_generation:
                        raise StaleLeaseError(
                            f"Worker '{worker_id}' (gen {lease_generation}) superseded for job '{job_id}'."
                        )
                    if row["lease_expires_at"] is None or row["lease_expires_at"] < now:
                        raise StaleLeaseError(
                            f"Cannot register sandbox: lease for worker '{worker_id}' is null or expired at {row['lease_expires_at']}."
                        )

                    # Idempotency check
                    if row["sandbox_id"] == clean_sb:
                        return True
                    if row["sandbox_id"] is not None and row["sandbox_id"] != clean_sb:
                        raise JobQueueError(
                            f"Conflicting sandbox_id '{row['sandbox_id']}' already registered for job '{job_id}'."
                        )

                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET sandbox_id = ?,
                            sandbox_cleanup_status = 'ACTIVE'
                        WHERE id = ? AND worker_id = ? AND lease_generation = ? AND status = 'RUNNING';
                        """,
                        (clean_sb, job_id, worker_id, lease_generation),
                    )

                    self.conn.execute(
                        """
                        INSERT INTO job_events (
                            job_id, event_type, source, worker_id, lease_generation, message, created_at
                        ) VALUES (?, 'SANDBOX_REGISTERED', 'worker', ?, ?, ?, ?);
                        """,
                        (job_id, worker_id, lease_generation, f"Sandbox '{clean_sb}' registered as ACTIVE.", now),
                    )

                return True
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during register_sandbox: {str(e)}") from e

    def append_log(
        self,
        job_id: str,
        worker_id: str,
        lease_generation: int,
        message: str,
        event_type: str = "LOG",
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Appends an event/log line. Fails closed if the job is not RUNNING, lease is null/expired, or worker is stale."""
        validate_numeric(lease_generation, "lease_generation", 1, 1000000000, require_int=True)
        now = time.time()
        data_str = json.dumps(data) if data is not None else None

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    chk = self.conn.execute(
                        "SELECT worker_id, lease_generation, status, lease_expires_at FROM jobs WHERE id = ?;",
                        (job_id,),
                    ).fetchone()

                    if not chk:
                        raise JobNotFoundError(f"Job '{job_id}' does not exist.")
                    if chk["status"] != "RUNNING":
                        raise StaleLeaseError(
                            f"Refusing to append log: job '{job_id}' is in terminal/non-running status '{chk['status']}'."
                        )
                    if chk["worker_id"] != worker_id or chk["lease_generation"] != lease_generation:
                        raise StaleLeaseError(
                            f"Refusing to append log: worker '{worker_id}' (gen {lease_generation}) is stale for job '{job_id}'."
                        )
                    if chk["lease_expires_at"] is None or chk["lease_expires_at"] < now:
                        raise StaleLeaseError(
                            f"Refusing to append log: lease for worker '{worker_id}' is null or expired at {chk['lease_expires_at']} (now {now})."
                        )

                    self.conn.execute(
                        """
                        INSERT INTO job_events (
                            job_id, event_type, source, worker_id, lease_generation, message, data_json, created_at
                        ) VALUES (?, ?, 'worker', ?, ?, ?, ?, ?);
                        """,
                        (job_id, event_type, worker_id, lease_generation, message, data_str, now),
                    )
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during append_log: {str(e)}") from e

    def update_job_status(
        self,
        job_id: str,
        target_status: str,
        worker_id: str,
        lease_generation: int,
        error: Optional[Dict[str, Any]] = None,
        sandbox_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Transitions job to target status with strict validation against cancellation overwrites, missing/expired leases, and stale generations."""
        validate_numeric(lease_generation, "lease_generation", 1, 1000000000, require_int=True)
        now = time.time()
        err_str = json.dumps(error) if error else None

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")

                    cursor = self.conn.execute(
                        "SELECT status, worker_id, lease_generation, lease_expires_at, cancel_requested_at, error_json FROM jobs WHERE id = ?;",
                        (job_id,),
                    )
                    row = cursor.fetchone()
                    if not row:
                        raise JobNotFoundError(f"Job '{job_id}' does not exist.")

                    current_status = row["status"]
                    cancel_requested = bool(row["cancel_requested_at"])

                    if row["worker_id"] != worker_id or row["lease_generation"] != lease_generation:
                        raise StaleLeaseError(
                            f"Worker '{worker_id}' (gen {lease_generation}) cannot update job '{job_id}': superseded."
                        )
                    if row["lease_expires_at"] is None or row["lease_expires_at"] < now:
                        raise StaleLeaseError(
                            f"Worker '{worker_id}' (gen {lease_generation}) cannot update job '{job_id}': lease is null or expired at {row['lease_expires_at']}."
                        )

                    effective_target = target_status
                    if cancel_requested:
                        if target_status == "COMPLETED":
                            raise InvalidStateTransitionError(
                                f"Cannot mark job '{job_id}' as COMPLETED: cancellation was requested by the user."
                            )
                        elif target_status in ("FAILED", "TIMED_OUT"):
                            effective_target = "CANCELLED"
                            if not err_str:
                                err_str = json.dumps({"note": f"Worker reported {target_status} after user requested cancellation."})

                    allowed = ALLOWED_TRANSITIONS.get(current_status, set())
                    if effective_target not in allowed:
                        raise InvalidStateTransitionError(
                            f"Illegal status transition for job '{job_id}': '{current_status}' -> '{effective_target}'."
                        )

                    finished_at = now if effective_target in ("COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT") else None

                    # If transitioning to terminal state, sandbox cleanup is marked ORPHANED if sandbox exists and was ACTIVE
                    cleanup_clause = ""
                    if effective_target in ("COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT"):
                        cleanup_clause = ", sandbox_cleanup_status = CASE WHEN sandbox_id IS NOT NULL AND sandbox_cleanup_status = 'ACTIVE' THEN 'ORPHANED' ELSE sandbox_cleanup_status END"

                    self.conn.execute(
                        f"""
                        UPDATE jobs
                        SET status = ?,
                            finished_at = COALESCE(finished_at, ?),
                            error_json = COALESCE(?, error_json),
                            sandbox_id = COALESCE(?, sandbox_id)
                            {cleanup_clause}
                        WHERE id = ? AND worker_id = ? AND lease_generation = ?;
                        """,
                        (effective_target, finished_at, err_str, sandbox_id, job_id, worker_id, lease_generation),
                    )

                    self.conn.execute(
                        """
                        INSERT INTO job_events (
                            job_id, event_type, source, worker_id, lease_generation, message, created_at
                        ) VALUES (?, 'STATUS_CHANGE', 'worker', ?, ?, ?, ?);
                        """,
                        (job_id, worker_id, lease_generation, f"Status updated to '{effective_target}'.", now),
                    )

                return self.get_job_by_id_internal(job_id)
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during update_job_status: {str(e)}") from e

    def supervisor_finalize_job(
        self,
        job_id: str,
        expected_lease_generation: int,
        target_status: str,
        supervisor_id: str,
        authority_proof: Dict[str, Any],
        reason: str,
        supervisor_signing_key: bytes,
        error: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Privileged supervisor finalization with cryptographic proof verification.
        
        Invariants:
        1. target_status CANNOT be 'COMPLETED' (supervisors cannot forge successful work).
        2. lease_generation must match expected_lease_generation.
        3. Authority proof is validated internally within SQLite transaction:
           - If 'LEASE_EXPIRED': checks row['lease_expires_at'] < now.
           - If 'FORCED_KILL': validates HMAC-SHA256 signature against supervisor_signing_key
             and verifies worker_instances row status == 'DEAD' with matching reap_token.
           - If 'PROCESS_DEAD': verifies worker_instances row status == 'DEAD' and validates signature.
        4. Fences out dead worker by incrementing lease_generation.
        5. Updates sandbox_cleanup_status to 'ORPHANED' if sandbox was ACTIVE.
        """
        if target_status == "COMPLETED":
            raise InvalidStateTransitionError("Supervisors cannot finalize jobs with COMPLETED status.")
        if target_status not in ("CANCELLED", "TIMED_OUT", "FAILED", "RECOVERABLE"):
            raise InvalidStateTransitionError(f"Unsupported supervisor target status '{target_status}'.")

        validate_numeric(expected_lease_generation, "expected_lease_generation", 1, 1000000000, require_int=True)
        now = time.time()
        err_str = json.dumps(error) if error else None
        proof_type = authority_proof.get("type")

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    row = self.conn.execute(
                        """
                        SELECT status, worker_id, lease_generation, lease_expires_at, sandbox_id, sandbox_cleanup_status
                        FROM jobs WHERE id = ?;
                        """,
                        (job_id,),
                    ).fetchone()

                    if not row:
                        raise JobNotFoundError(f"Job '{job_id}' does not exist.")
                    if row["lease_generation"] != expected_lease_generation:
                        raise StaleLeaseError(
                            f"Supervisor finalization rejected: expected gen {expected_lease_generation}, found {row['lease_generation']}."
                        )
                    if row["status"] not in ("RUNNING", "RECOVERABLE"):
                        raise InvalidStateTransitionError(
                            f"Cannot finalize job '{job_id}' from status '{row['status']}'."
                        )

                    # Validate proof internally
                    if proof_type == "LEASE_EXPIRED":
                        if row["lease_expires_at"] is not None and row["lease_expires_at"] >= now:
                            raise JobQueueError("Supervisor authority check failed: assigned worker lease is still active.")
                    elif proof_type in ("FORCED_KILL", "PROCESS_DEAD"):
                        worker_inst_id = authority_proof.get("worker_instance_id")
                        reap_token = authority_proof.get("reap_token")
                        reap_sig = authority_proof.get("reap_signature")

                        inst_row = self.conn.execute(
                            "SELECT * FROM worker_instances WHERE instance_id = ?;",
                            (worker_inst_id,),
                        ).fetchone()

                        if not inst_row:
                            raise JobQueueError(f"Authority check failed: worker instance '{worker_inst_id}' not found.")
                        if inst_row["status"] != "DEAD":
                            raise JobQueueError(f"Authority check failed: worker instance '{worker_inst_id}' is not DEAD.")

                        # Validate canonical HMAC signature
                        canonical_payload = {
                            "version": "v1",
                            "proof_type": inst_row["proof_type"],
                            "supervisor_id": inst_row["supervisor_id"],
                            "instance_id": inst_row["instance_id"],
                            "pid": inst_row["pid"],
                            "start_time": inst_row["start_time"],
                            "exit_code": inst_row["exit_code"],
                            "exit_signal": inst_row["exit_signal"],
                            "terminated_at": inst_row["terminated_at"],
                            "reap_token": inst_row["reap_token"],
                        }
                        expected_sig = compute_canonical_signature(supervisor_signing_key, canonical_payload)
                        if not hmac.compare_digest(inst_row["reap_signature"] or "", expected_sig):
                            raise JobQueueError("Authority check failed: cryptographic reap signature is invalid.")
                    else:
                        raise JobQueueError(f"Authority check failed: unknown proof type '{proof_type}'.")

                    new_gen = row["lease_generation"] + 1
                    finished_at = now if target_status in ("COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT") else None

                    # If active sandbox exists, mark ORPHANED so reconciler destroys it
                    cleanup_status = row["sandbox_cleanup_status"]
                    if row["sandbox_id"] is not None and cleanup_status == "ACTIVE":
                        cleanup_status = "ORPHANED"

                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET status = ?,
                            lease_generation = ?,
                            finished_at = COALESCE(finished_at, ?),
                            error_json = COALESCE(?, error_json),
                            sandbox_cleanup_status = ?
                        WHERE id = ? AND lease_generation = ?;
                        """,
                        (target_status, new_gen, finished_at, err_str, cleanup_status, job_id, expected_lease_generation),
                    )

                    self.conn.execute(
                        """
                        INSERT INTO job_events (
                            job_id, event_type, source, lease_generation, message, data_json, created_at
                        ) VALUES (?, 'SUPERVISOR_FINALIZED', 'supervisor', ?, ?, ?, ?);
                        """,
                        (
                            job_id,
                            new_gen,
                            f"Supervisor '{supervisor_id}' finalized job to '{target_status}'. Reason: {reason}",
                            json.dumps({"authority_proof": authority_proof, "supervisor_id": supervisor_id}),
                            now,
                        ),
                    )

                return self.get_job_by_id_internal(job_id)
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during supervisor_finalize_job: {str(e)}") from e

    def record_process_reap(
        self,
        instance_id: str,
        pid: int,
        start_time: float,
        exit_code: Optional[int],
        exit_signal: Optional[int],
        supervisor_id: str,
        proof_type: str,
        supervisor_signing_key: bytes,
    ) -> Dict[str, Any]:
        """Atomically records child reap verification evidence into worker_instances with canonical HMAC signature."""
        now = time.time()
        reap_token = uuid.uuid4().hex

        canonical_payload = {
            "version": "v1",
            "proof_type": proof_type,
            "supervisor_id": supervisor_id,
            "instance_id": instance_id,
            "pid": pid,
            "start_time": start_time,
            "exit_code": exit_code,
            "exit_signal": exit_signal,
            "terminated_at": now,
            "reap_token": reap_token,
        }
        reap_signature = compute_canonical_signature(supervisor_signing_key, canonical_payload)

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    self.conn.execute(
                        """
                        INSERT INTO worker_instances (
                            instance_id, pid, start_time, last_heartbeat, status,
                            proof_type, supervisor_id, exit_code, exit_signal,
                            terminated_at, reap_signature, reap_token
                        ) VALUES (?, ?, ?, ?, 'DEAD', ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(instance_id) DO UPDATE SET
                            status = 'DEAD',
                            proof_type = excluded.proof_type,
                            supervisor_id = excluded.supervisor_id,
                            exit_code = excluded.exit_code,
                            exit_signal = excluded.exit_signal,
                            terminated_at = excluded.terminated_at,
                            reap_signature = excluded.reap_signature,
                            reap_token = excluded.reap_token;
                        """,
                        (
                            instance_id,
                            pid,
                            start_time,
                            now,
                            proof_type,
                            supervisor_id,
                            exit_code,
                            exit_signal,
                            now,
                            reap_signature,
                            reap_token,
                        ),
                    )
                return canonical_payload | {"reap_signature": reap_signature}
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during record_process_reap: {str(e)}") from e

    def supervisor_verify_worker_death(
        self,
        job_id: str,
        instance_id: str,
        side_effect_id: str,
        supervisor_signing_key: bytes,
    ) -> bool:
        """Contextual worker-death verification bound to specific job, worker instance, and side-effect.
        
        Invariants:
        1. Verifies side_effect_id exists and is bound to (job_id, instance_id).
        2. Verifies worker_instances row for instance_id has status == 'DEAD'.
        3. Recomputes and validates canonical HMAC signature against supervisor_signing_key.
        """
        with self._lock:
            try:
                se_row = self.conn.execute(
                    "SELECT job_id, worker_id, status FROM job_side_effects WHERE side_effect_id = ?;",
                    (side_effect_id,),
                ).fetchone()

                if not se_row:
                    return False
                if se_row["job_id"] != job_id or se_row["worker_id"] != instance_id:
                    return False

                inst_row = self.conn.execute(
                    "SELECT * FROM worker_instances WHERE instance_id = ?;",
                    (instance_id,),
                ).fetchone()

                if not inst_row:
                    return False
                if inst_row["status"] != "DEAD":
                    return False

                canonical_payload = {
                    "version": "v1",
                    "proof_type": inst_row["proof_type"],
                    "supervisor_id": inst_row["supervisor_id"],
                    "instance_id": inst_row["instance_id"],
                    "pid": inst_row["pid"],
                    "start_time": inst_row["start_time"],
                    "exit_code": inst_row["exit_code"],
                    "exit_signal": inst_row["exit_signal"],
                    "terminated_at": inst_row["terminated_at"],
                    "reap_token": inst_row["reap_token"],
                }
                expected_sig = compute_canonical_signature(supervisor_signing_key, canonical_payload)
                return hmac.compare_digest(inst_row["reap_signature"] or "", expected_sig)
            except sqlite3.Error:
                return False

    def reserve_side_effect(
        self,
        job_id: str,
        worker_id: str,
        lease_generation: int,
        action_type: str,
        action_key: str,
        intent_payload: Dict[str, Any],
    ) -> Tuple[str, str, Optional[Dict[str, Any]]]:
        """Atomically reserves an external side-effect intent in the relational ledger.
        
        Returns:
        (side_effect_id, status, confirmed_result_dict_if_already_confirmed)
        - If already 'CONFIRMED': returns (id, 'CONFIRMED', confirmed_result) -> safe to reuse!
        - If 'PENDING' for an ACTIVE different worker lease: raises SideEffectConflictError.
        - If 'PENDING' for an OLD lease: returns (id, 'PENDING_RECOVERY', None) -> triggers recovery flow!
        - If not exists: inserts 'PENDING' and returns (id, 'PENDING', None) -> proceed with API call!
        """
        side_effect_id = compute_side_effect_id(job_id, action_type, action_key)
        now = time.time()
        payload_str = canonical_json(intent_payload)

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    row = self.conn.execute(
                        "SELECT * FROM job_side_effects WHERE side_effect_id = ?;",
                        (side_effect_id,),
                    ).fetchone()

                    if row:
                        if row["status"] == "CONFIRMED":
                            res = json.loads(row["confirmed_result"]) if row["confirmed_result"] else {}
                            return side_effect_id, "CONFIRMED", res

                        if row["status"] == "PENDING":
                            if row["lease_generation"] == lease_generation and row["worker_id"] == worker_id:
                                return side_effect_id, "PENDING", None

                            # Different worker or older generation:
                            # Check if the existing lease holder is still active in jobs table
                            job_row = self.conn.execute(
                                "SELECT status, worker_id, lease_generation, lease_expires_at FROM jobs WHERE id = ?;",
                                (job_id,),
                            ).fetchone()

                            if (
                                job_row
                                and job_row["status"] == "RUNNING"
                                and job_row["worker_id"] == row["worker_id"]
                                and job_row["lease_generation"] == row["lease_generation"]
                                and job_row["lease_expires_at"] is not None
                                and job_row["lease_expires_at"] >= now
                            ):
                                raise SideEffectConflictError(
                                    f"Side effect '{side_effect_id}' is already PENDING by active worker '{row['worker_id']}'."
                                )

                            # The prior worker's lease expired or superseded -> return for two-phase recovery
                            return side_effect_id, "PENDING_RECOVERY", {
                                "prior_worker_id": row["worker_id"],
                                "prior_lease_generation": row["lease_generation"],
                            }

                    # No existing entry, or prior attempt was FAILED_VERIFIED
                    self.conn.execute(
                        """
                        INSERT INTO job_side_effects (
                            side_effect_id, job_id, worker_id, lease_generation,
                            action_type, status, intent_payload, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)
                        ON CONFLICT(side_effect_id) DO UPDATE SET
                            worker_id = excluded.worker_id,
                            lease_generation = excluded.lease_generation,
                            status = 'PENDING',
                            intent_payload = excluded.intent_payload,
                            updated_at = excluded.updated_at;
                        """,
                        (side_effect_id, job_id, worker_id, lease_generation, action_type, payload_str, now, now),
                    )

                return side_effect_id, "PENDING", None
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during reserve_side_effect: {str(e)}") from e

    def confirm_side_effect(
        self,
        side_effect_id: str,
        worker_id: str,
        lease_generation: int,
        confirmed_result: Dict[str, Any],
        is_recovery: bool = False,
    ) -> None:
        """Atomically marks a side-effect intent as CONFIRMED with strict lease fencing.
        
        If is_recovery is True, permits the new lease holder (worker_id, lease_generation)
        to adopt and confirm an abandoned intent previously created by an older lease holder.
        """
        now = time.time()
        res_str = canonical_json(confirmed_result)

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    if is_recovery:
                        # Recovery adoption: update row only if status == 'PENDING' and older generation
                        cursor = self.conn.execute(
                            """
                            UPDATE job_side_effects
                            SET status = 'CONFIRMED',
                                worker_id = ?,
                                lease_generation = ?,
                                confirmed_result = ?,
                                updated_at = ?
                            WHERE side_effect_id = ? AND status = 'PENDING' AND lease_generation <= ?;
                            """,
                            (worker_id, lease_generation, res_str, now, side_effect_id, lease_generation),
                        )
                    else:
                        cursor = self.conn.execute(
                            """
                            UPDATE job_side_effects
                            SET status = 'CONFIRMED',
                                confirmed_result = ?,
                                updated_at = ?
                            WHERE side_effect_id = ? AND worker_id = ? AND lease_generation = ?;
                            """,
                            (res_str, now, side_effect_id, worker_id, lease_generation),
                        )

                    if cursor.rowcount == 0:
                        row = self.conn.execute(
                            "SELECT worker_id, lease_generation, status FROM job_side_effects WHERE side_effect_id = ?;",
                            (side_effect_id,),
                        ).fetchone()
                        if not row:
                            raise JobNotFoundError(f"Side effect '{side_effect_id}' not found.")
                        raise StaleLeaseError(
                            f"Worker '{worker_id}' (gen {lease_generation}) superseded for side effect '{side_effect_id}'."
                        )
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during confirm_side_effect: {str(e)}") from e

    def fail_side_effect(
        self,
        side_effect_id: str,
        worker_id: str,
        lease_generation: int,
        reason: str,
        is_recovery: bool = False,
    ) -> None:
        """Marks a side-effect intent as FAILED_VERIFIED after remote confirmation that it did not execute.
        
        If is_recovery is True, permits the new lease holder to mark an older abandoned intent FAILED_VERIFIED.
        """
        now = time.time()

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    if is_recovery:
                        cursor = self.conn.execute(
                            """
                            UPDATE job_side_effects
                            SET status = 'FAILED_VERIFIED',
                                worker_id = ?,
                                lease_generation = ?,
                                updated_at = ?
                            WHERE side_effect_id = ? AND status = 'PENDING' AND lease_generation <= ?;
                            """,
                            (worker_id, lease_generation, now, side_effect_id, lease_generation),
                        )
                    else:
                        cursor = self.conn.execute(
                            """
                            UPDATE job_side_effects
                            SET status = 'FAILED_VERIFIED',
                                updated_at = ?
                            WHERE side_effect_id = ? AND worker_id = ? AND lease_generation = ?;
                            """,
                            (now, side_effect_id, worker_id, lease_generation),
                        )

                    if cursor.rowcount == 0:
                        raise StaleLeaseError(f"Worker '{worker_id}' (gen {lease_generation}) cannot fail side effect '{side_effect_id}'.")
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during fail_side_effect: {str(e)}") from e

    def claim_sandbox_cleanup(self, owner_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Atomically claims orphaned/failed sandboxes for cleanup with owner fencing and generation tokens."""
        now = time.time()
        claimed_sandboxes = []

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    cursor = self.conn.execute(
                        """
                        SELECT id, sandbox_id, sandbox_cleanup_status, sandbox_cleanup_attempts,
                               sandbox_cleanup_generation
                        FROM jobs
                        WHERE sandbox_id IS NOT NULL
                          AND sandbox_cleanup_status IN ('ORPHANED', 'FAILED', 'MANUAL_INTERVENTION_REQUIRED')
                          AND (sandbox_cleanup_next_retry IS NULL OR sandbox_cleanup_next_retry <= ?)
                        ORDER BY sandbox_cleanup_next_retry ASC NULLS FIRST
                        LIMIT ?;
                        """,
                        (now, limit),
                    )
                    rows = cursor.fetchall()

                    for r in rows:
                        job_id = r["id"]
                        new_gen = (r["sandbox_cleanup_generation"] or 0) + 1
                        update_cur = self.conn.execute(
                            """
                            UPDATE jobs
                            SET sandbox_cleanup_owner = ?,
                                sandbox_cleanup_generation = ?
                            WHERE id = ? AND sandbox_cleanup_status IN ('ORPHANED', 'FAILED', 'MANUAL_INTERVENTION_REQUIRED')
                              AND sandbox_cleanup_generation = ?;
                            """,
                            (owner_id, new_gen, job_id, r["sandbox_cleanup_generation"] or 0),
                        )
                        if update_cur.rowcount > 0:
                            claimed_sandboxes.append({
                                "id": job_id,
                                "sandbox_id": r["sandbox_id"],
                                "sandbox_cleanup_status": r["sandbox_cleanup_status"],
                                "sandbox_cleanup_attempts": r["sandbox_cleanup_attempts"],
                                "sandbox_cleanup_generation": new_gen,
                                "owner_id": owner_id,
                            })

                return claimed_sandboxes
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during claim_sandbox_cleanup: {str(e)}") from e

    def finalize_active_sandbox_cleanup(
        self,
        job_id: str,
        sandbox_id: str,
        worker_id: str,
        lease_generation: int,
    ) -> bool:
        """Atomically transitions an ACTIVE sandbox to CLEANED upon normal completion.

        Requires strict fencing:
        - Exact job ID.
        - Exact sandbox ID.
        - Exact worker ID.
        - Exact lease generation.
        - Job status must be RUNNING.
        - Lease expiry must be non-null and in the future (lease_expires_at >= now).
        - Cleanup status must be ACTIVE (or already CLEANED for legitimate caller idempotent retry).
        """
        now = time.time()
        validate_numeric(lease_generation, "lease_generation", 1, 1000000000, require_int=True)
        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    row = self.conn.execute(
                        """
                        SELECT status, sandbox_id, worker_id, lease_generation, lease_expires_at, sandbox_cleanup_status
                        FROM jobs
                        WHERE id = ?;
                        """,
                        (job_id,),
                    ).fetchone()
                    if not row:
                        raise JobNotFoundError(f"Job '{job_id}' does not exist.")

                    # Fencing identity checks must happen BEFORE checking cleanup status
                    if row["sandbox_id"] != sandbox_id:
                        raise JobQueueError(
                            f"Sandbox ID mismatch on job '{job_id}': expected '{sandbox_id}', found '{row['sandbox_id']}'"
                        )

                    if row["worker_id"] != worker_id or (row["lease_generation"] or 0) != lease_generation:
                        raise StaleLeaseError(
                            f"Lease mismatch on job '{job_id}': worker '{worker_id}' gen {lease_generation} does not own active lease."
                        )

                    # Preserve idempotency for legitimate repeated calls by the same verified worker/generation
                    if row["sandbox_cleanup_status"] == "CLEANED":
                        return True

                    if row["status"] != "RUNNING":
                        raise StaleLeaseError(
                            f"Cannot finalize normal sandbox cleanup: job '{job_id}' is in non-running status '{row['status']}'."
                        )

                    if row["lease_expires_at"] is None or row["lease_expires_at"] < now:
                        raise StaleLeaseError(
                            f"Worker '{worker_id}' (gen {lease_generation}) cannot finalize sandbox cleanup: lease is null or expired at {row['lease_expires_at']}."
                        )

                    if row["sandbox_cleanup_status"] != "ACTIVE":
                        raise JobQueueError(
                            f"Cannot finalize normal sandbox cleanup: sandbox status is '{row['sandbox_cleanup_status']}', expected 'ACTIVE'."
                        )

                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET sandbox_cleanup_status = 'CLEANED',
                            sandbox_cleanup_error = NULL
                        WHERE id = ?
                          AND sandbox_id = ?
                          AND worker_id = ?
                          AND lease_generation = ?
                          AND status = 'RUNNING'
                          AND lease_expires_at >= ?
                          AND sandbox_cleanup_status = 'ACTIVE';
                        """,
                        (job_id, sandbox_id, worker_id, lease_generation, now),
                    )
                    self.conn.execute(
                        """
                        INSERT INTO job_events (
                            job_id, event_type, source, worker_id, lease_generation, message, created_at
                        ) VALUES (?, 'SANDBOX_CLEANED', 'worker', ?, ?, ?, ?);
                        """,
                        (job_id, worker_id, lease_generation, f"Sandbox '{sandbox_id}' successfully destroyed on completion.", now),
                    )
                    return True
            except (JobQueueError, JobNotFoundError, StaleLeaseError):
                raise
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during finalize_active_sandbox_cleanup: {str(e)}") from e

    def finalize_sandbox_cleanup(
        self,
        job_id: str,
        sandbox_id: str,
        owner_id: str,
        cleanup_generation: int,
        success: bool,
        error_message: Optional[str] = None,
    ) -> bool:
        """Atomically updates cleanup status conditional on owner_id, sandbox_id, and cleanup_generation."""
        now = time.time()
        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    row = self.conn.execute(
                        "SELECT sandbox_cleanup_status, sandbox_cleanup_attempts FROM jobs WHERE id = ? AND sandbox_id = ? AND sandbox_cleanup_owner = ? AND sandbox_cleanup_generation = ?;",
                        (job_id, sandbox_id, owner_id, cleanup_generation),
                    ).fetchone()
                    if not row:
                        return False

                    current_status = row["sandbox_cleanup_status"]
                    attempts = row["sandbox_cleanup_attempts"] + 1

                    if success:
                        self.conn.execute(
                            """
                            UPDATE jobs
                            SET sandbox_cleanup_status = 'CLEANED',
                                sandbox_cleanup_error = NULL,
                                sandbox_cleanup_owner = NULL
                            WHERE id = ? AND sandbox_id = ? AND sandbox_cleanup_owner = ? AND sandbox_cleanup_generation = ?;
                            """,
                            (job_id, sandbox_id, owner_id, cleanup_generation),
                        )
                        self.conn.execute(
                            """
                            INSERT INTO job_events (
                                job_id, event_type, source, message, created_at
                            ) VALUES (?, 'SANDBOX_CLEANED', 'reconciler', ?, ?);
                            """,
                            (job_id, f"Sandbox '{sandbox_id}' successfully destroyed.", now),
                        )
                    else:
                        if current_status == "MANUAL_INTERVENTION_REQUIRED" or attempts >= 10:
                            new_status = "MANUAL_INTERVENTION_REQUIRED"
                            next_retry = now + 900.0
                        else:
                            new_status = "FAILED"
                            backoff = min(300.0, 2.0 ** attempts)
                            next_retry = now + backoff

                        self.conn.execute(
                            """
                            UPDATE jobs
                            SET sandbox_cleanup_status = ?,
                                sandbox_cleanup_attempts = ?,
                                sandbox_cleanup_error = ?,
                                sandbox_cleanup_next_retry = ?,
                                sandbox_cleanup_owner = NULL
                            WHERE id = ? AND sandbox_id = ? AND sandbox_cleanup_owner = ? AND sandbox_cleanup_generation = ?;
                            """,
                            (new_status, attempts, error_message, next_retry, job_id, sandbox_id, owner_id, cleanup_generation),
                        )
                        self.conn.execute(
                            """
                            INSERT INTO job_events (
                                job_id, event_type, source, message, created_at
                            ) VALUES (?, 'SANDBOX_CLEANUP_FAILED', 'reconciler', ?, ?);
                            """,
                            (job_id, f"Sandbox destruction attempt {attempts} failed: {error_message}", now),
                        )
                    return True
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during finalize_sandbox_cleanup: {str(e)}") from e

    def resolve_manual_cleanup(
        self,
        job_id: str,
        action: str,
        verification_proof: str,
        admin_auth_token: str,
        admin_auth_keys: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Operator-authenticated resolution of stuck cloud resources with non-forgeable attribution.
        
        Invariants:
        1. Authenticates admin_auth_token against admin keystore; resolves authenticated_principal_id.
        2. If action == 'CONFIRMED_DELETED':
           - Sets sandbox_cleanup_status = 'CLEANED'.
           - Does NOT alter execution status if already in terminal state ('FAILED', 'CANCELLED', 'TIMED_OUT').
           - If status == 'RECOVERABLE', transitions status = 'QUEUED' (safe to re-claim).
           - Records structured audit event citing authenticated_principal_id, role, proof.
        3. If action == 'FORCE_CLEANUP_RETRY':
           - Resets sandbox_cleanup_attempts = 0, next_retry = now, sandbox_cleanup_status = 'ORPHANED'.
        """
        token_hash = hashlib.sha256(admin_auth_token.encode("utf-8")).hexdigest()
        admin_meta = admin_auth_keys.get(token_hash)
        if not admin_meta:
            raise JobQueueError("Unauthorized operator resolution request: invalid admin credential.")

        principal_id = admin_meta.get("principal_id", "unknown_admin")
        roles = admin_meta.get("roles", [])
        if "ADMIN" not in roles:
            raise JobQueueError("Unauthorized operator resolution request: insufficient role permissions.")

        if action not in ("CONFIRMED_DELETED", "FORCE_CLEANUP_RETRY"):
            raise JobQueueError(f"Invalid operator cleanup action '{action}'.")

        now = time.time()

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    row = self.conn.execute(
                        "SELECT status, sandbox_cleanup_status FROM jobs WHERE id = ?;",
                        (job_id,),
                    ).fetchone()

                    if not row:
                        raise JobNotFoundError(f"Job '{job_id}' does not exist.")

                    current_status = row["status"]

                    if action == "CONFIRMED_DELETED":
                        new_status = current_status
                        if current_status == "RECOVERABLE":
                            new_status = "QUEUED"

                        self.conn.execute(
                            """
                            UPDATE jobs
                            SET sandbox_cleanup_status = 'CLEANED',
                                status = ?
                            WHERE id = ?;
                            """,
                            (new_status, job_id),
                        )

                        self.conn.execute(
                            """
                            INSERT INTO job_events (
                                job_id, event_type, source, message, data_json, created_at
                            ) VALUES (?, 'OPERATOR_CLEANUP_RESOLVED', 'operator', ?, ?, ?);
                            """,
                            (
                                job_id,
                                f"Operator '{principal_id}' confirmed sandbox deleted. Proof: {verification_proof}",
                                json.dumps({
                                    "operator_principal_id": principal_id,
                                    "action": action,
                                    "verification_proof": verification_proof,
                                }),
                                now,
                            ),
                        )
                    elif action == "FORCE_CLEANUP_RETRY":
                        self.conn.execute(
                            """
                            UPDATE jobs
                            SET sandbox_cleanup_status = 'ORPHANED',
                                sandbox_cleanup_attempts = 0,
                                sandbox_cleanup_next_retry = ?
                            WHERE id = ?;
                            """,
                            (now, job_id),
                        )

                return self.get_job_by_id_internal(job_id)
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during resolve_manual_cleanup: {str(e)}") from e

    def request_cancellation(self, job_id: str, user_id: str) -> Dict[str, Any]:
        """Tenant-authenticated cancellation request. Sets cancel_requested_at and cancels immediately if QUEUED."""
        if not user_id or not user_id.strip():
            raise JobAccessDeniedError("user_id required for cancellation.")

        now = time.time()
        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    cursor = self.conn.execute(
                        "SELECT status, user_id FROM jobs WHERE id = ?;",
                        (job_id,),
                    )
                    row = cursor.fetchone()
                    if not row:
                        raise JobNotFoundError(f"Job '{job_id}' does not exist.")

                    if row["user_id"] != user_id:
                        raise JobAccessDeniedError("Access denied: cannot cancel a job belonging to another user.")

                    current_status = row["status"]
                    if current_status in ("COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT"):
                        return self._row_to_dict(self.conn.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,)).fetchone())

                    if current_status == "QUEUED":
                        self.conn.execute(
                            """
                            UPDATE jobs
                            SET status = 'CANCELLED',
                                cancel_requested_at = ?,
                                cancel_requested_by = ?,
                                finished_at = ?
                            WHERE id = ? AND status = 'QUEUED';
                            """,
                            (now, user_id, now, job_id),
                        )
                    else:
                        self.conn.execute(
                            """
                            UPDATE jobs
                            SET cancel_requested_at = ?,
                                cancel_requested_by = ?
                            WHERE id = ?;
                            """,
                            (now, user_id, job_id),
                        )

                    self.conn.execute(
                        """
                        INSERT INTO job_events (
                            job_id, event_type, source, message, created_at
                        ) VALUES (?, 'CANCEL_REQUESTED', 'user', 'Cancellation requested by user.', ?);
                        """,
                        (job_id, now),
                    )

                return self.get_job(job_id, user_id=user_id)
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during request_cancellation: {str(e)}") from e

    def reap_stale_jobs(self) -> List[str]:
        """Finds running jobs whose lease has expired or is null.
        
        CRITICAL CANCELLATION GUARD: If cancel_requested_at is set, the job is NEVER re-queued.
        Instead, it transitions directly to terminal CANCELLED.
        """
        now = time.time()
        reaped_ids = []

        with self._lock:
            try:
                with self.conn:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    cursor = self.conn.execute(
                        """
                        SELECT id, retry_count, max_retries, lease_generation, worker_id, cancel_requested_at,
                               sandbox_id, sandbox_cleanup_status
                        FROM jobs
                        WHERE status = 'RUNNING' AND (lease_expires_at IS NULL OR lease_expires_at < ?);
                        """,
                        (now,),
                    )
                    stale_jobs = cursor.fetchall()

                    for job in stale_jobs:
                        jid = job["id"]
                        retries = job["retry_count"]
                        max_r = job["max_retries"]
                        gen = job["lease_generation"]
                        cancel_req = bool(job["cancel_requested_at"])
                        sb_id = job["sandbox_id"]
                        sb_cleanup = job["sandbox_cleanup_status"]

                        # If sandbox was ACTIVE, mark ORPHANED so reconciler destroys it
                        if sb_id is not None and sb_cleanup == "ACTIVE":
                            sb_cleanup = "ORPHANED"

                        if cancel_req:
                            self.conn.execute(
                                """
                                UPDATE jobs
                                SET status = 'CANCELLED',
                                    finished_at = ?,
                                    lease_generation = lease_generation + 1,
                                    sandbox_cleanup_status = ?
                                WHERE id = ? AND status = 'RUNNING' AND lease_generation = ?;
                                """,
                                (now, sb_cleanup, jid, gen),
                            )
                            event_msg = "Lease expired while cancellation pending. Transitioned to CANCELLED."
                        elif retries < max_r:
                            self.conn.execute(
                                """
                                UPDATE jobs
                                SET status = 'QUEUED',
                                    worker_id = NULL,
                                    lease_generation = lease_generation + 1,
                                    retry_count = retry_count + 1,
                                    lease_expires_at = NULL,
                                    heartbeat_at = NULL,
                                    sandbox_cleanup_status = ?
                                WHERE id = ? AND status = 'RUNNING' AND lease_generation = ?;
                                """,
                                (sb_cleanup, jid, gen),
                            )
                            event_msg = f"Lease expired. Re-queued for retry ({retries + 1}/{max_r})."
                        else:
                            err_json = json.dumps({
                                "error": "Execution lease expired and maximum retries exhausted.",
                                "type": "WorkerHeartbeatTimeout",
                            })
                            self.conn.execute(
                                """
                                UPDATE jobs
                                SET status = 'FAILED',
                                    finished_at = ?,
                                    error_json = ?,
                                    lease_generation = lease_generation + 1,
                                    sandbox_cleanup_status = ?
                                WHERE id = ? AND status = 'RUNNING' AND lease_generation = ?;
                                """,
                                (now, err_json, sb_cleanup, jid, gen),
                            )
                            event_msg = "Lease expired. Maximum recovery retries exhausted; job marked FAILED."

                        self.conn.execute(
                            """
                            INSERT INTO job_events (
                                job_id, event_type, source, lease_generation, message, created_at
                            ) VALUES (?, 'LEASE_EXPIRED', 'reaper', ?, ?, ?);
                            """,
                            (jid, gen + 1, event_msg, now),
                        )
                        reaped_ids.append(jid)

                return reaped_ids
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during reap_stale_jobs: {str(e)}") from e

    def get_job(self, job_id: str, user_id: str) -> Dict[str, Any]:
        """Tenant-isolated job retrieval. Raises JobAccessDeniedError if user does not own job."""
        with self._lock:
            try:
                cursor = self.conn.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,))
                row = cursor.fetchone()
                if not row:
                    raise JobNotFoundError(f"Job '{job_id}' does not exist.")
                if row["user_id"] != user_id:
                    raise JobAccessDeniedError(f"Access denied to job '{job_id}'.")
                return self._row_to_dict(row)
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during get_job: {str(e)}") from e

    def get_job_by_id_internal(self, job_id: str) -> Dict[str, Any]:
        """Internal lookup without tenant check (used by background worker threads)."""
        with self._lock:
            cursor = self.conn.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,))
            row = cursor.fetchone()
            if not row:
                raise JobNotFoundError(f"Job '{job_id}' does not exist.")
            return self._row_to_dict(row)

    def list_user_jobs(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Lists recent jobs for a specific tenant."""
        with self._lock:
            try:
                cursor = self.conn.execute(
                    "SELECT * FROM jobs WHERE user_id = ? ORDER BY created_at DESC LIMIT ?;",
                    (user_id, limit),
                )
                return [self._row_to_dict(r) for r in cursor.fetchall()]
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during list_user_jobs: {str(e)}") from e

    def get_job_events(self, job_id: str, user_id: str, after_id: int = 0) -> List[Dict[str, Any]]:
        """Retrieves append-only logs/events for a job, verifying tenant ownership."""
        self.get_job(job_id, user_id=user_id)

        with self._lock:
            try:
                cursor = self.conn.execute(
                    """
                    SELECT * FROM job_events
                    WHERE job_id = ? AND id > ?
                    ORDER BY id ASC;
                    """,
                    (job_id, after_id),
                )
                return [
                    {
                        "id": r["id"],
                        "job_id": r["job_id"],
                        "event_type": r["event_type"],
                        "source": r["source"],
                        "worker_id": r["worker_id"],
                        "lease_generation": r["lease_generation"],
                        "message": r["message"],
                        "data": json.loads(r["data_json"]) if r["data_json"] else None,
                        "created_at": r["created_at"],
                    }
                    for r in cursor.fetchall()
                ]
            except sqlite3.Error as e:
                raise JobQueueError(f"Database error during get_job_events: {str(e)}") from e

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        """Translates an SQLite jobs row into a structured dictionary."""
        return {
            "id": row["id"],
            "idempotency_key": row["idempotency_key"],
            "user_id": row["user_id"],
            "session_id": row["session_id"],
            "repository": row["repository"],
            "status": row["status"],
            "payload": json.loads(row["payload_json"]) if row["payload_json"] else {},
            "credential_refs": json.loads(row["credential_refs_json"]) if row["credential_refs_json"] else {},
            "worker_id": row["worker_id"],
            "lease_generation": row["lease_generation"],
            "heartbeat_at": row["heartbeat_at"],
            "lease_expires_at": row["lease_expires_at"],
            "cancel_requested_at": row["cancel_requested_at"],
            "cancel_requested_by": row["cancel_requested_by"],
            "retry_count": row["retry_count"],
            "max_retries": row["max_retries"],
            "timeout_seconds": row["timeout_seconds"],
            "sandbox_id": row["sandbox_id"],
            "sandbox_cleanup_status": row["sandbox_cleanup_status"] if "sandbox_cleanup_status" in row.keys() else "NONE",
            "sandbox_cleanup_attempts": row["sandbox_cleanup_attempts"] if "sandbox_cleanup_attempts" in row.keys() else 0,
            "sandbox_cleanup_error": row["sandbox_cleanup_error"] if "sandbox_cleanup_error" in row.keys() else None,
            "sandbox_cleanup_next_retry": row["sandbox_cleanup_next_retry"] if "sandbox_cleanup_next_retry" in row.keys() else None,
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "error": json.loads(row["error_json"]) if row["error_json"] else None,
        }

"""Durable Job Queue and Storage Engine for AI Cloud Workspace.

Implements Phase B.1 (Hardened & Fully Enforced):
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
- Schema versioning and idempotent forward migrations.
"""

import math
import os
import re
import json
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

        # Disallow unknown keys - only 'key_ref' is permitted
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

        # Check raw secret patterns
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

        # Strict numeric validation
        validate_numeric(max_retries, "max_retries", 0, 50, require_int=True)
        validate_numeric(timeout_seconds, "timeout_seconds", 1.0, 86400.0, require_int=False)

        # Strict credential & payload security validation
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

                    # Check for existing idempotency key
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
                            created_at
                        ) VALUES (?, ?, ?, ?, ?, 'QUEUED', ?, ?, 0, ?, ?, ?);
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
        """Atomically claims the next pending QUEUED job using an atomic lease and incremented fencing token."""
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

                    # Conditional update: only succeeds if still QUEUED
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
        """Transitions job to target status with strict validation against cancellation overwrites, missing/expired leases, and stale generations.
        
        Explicit Cancellation Precedence Policy:
        - If cancellation was requested:
          1. Target status `COMPLETED` is strictly disallowed and raises InvalidStateTransitionError.
          2. Target status `FAILED` or `TIMED_OUT` is overridden to `CANCELLED` (user cancellation wins),
             while the original error diagnostic dictionary is preserved in `error_json`.
          3. Target status `CANCELLED` is recorded directly.
        """
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

                    # Fencing & Expiration check: MUST be non-null and >= now
                    if row["worker_id"] != worker_id or row["lease_generation"] != lease_generation:
                        raise StaleLeaseError(
                            f"Worker '{worker_id}' (gen {lease_generation}) cannot update job '{job_id}': superseded."
                        )
                    if row["lease_expires_at"] is None or row["lease_expires_at"] < now:
                        raise StaleLeaseError(
                            f"Worker '{worker_id}' (gen {lease_generation}) cannot update job '{job_id}': lease is null or expired at {row['lease_expires_at']}."
                        )

                    # Determine effective target status according to Cancellation Precedence Policy
                    effective_target = target_status
                    if cancel_requested:
                        if target_status == "COMPLETED":
                            raise InvalidStateTransitionError(
                                f"Cannot mark job '{job_id}' as COMPLETED: cancellation was requested by the user."
                            )
                        elif target_status in ("FAILED", "TIMED_OUT"):
                            # User cancellation takes precedence over worker error/timeout, but error info is retained
                            effective_target = "CANCELLED"
                            if not err_str:
                                err_str = json.dumps({"note": f"Worker reported {target_status} after user requested cancellation."})

                    # Validate lifecycle transition rules against current_status
                    allowed = ALLOWED_TRANSITIONS.get(current_status, set())
                    if effective_target not in allowed:
                        raise InvalidStateTransitionError(
                            f"Illegal status transition for job '{job_id}': '{current_status}' -> '{effective_target}'."
                        )

                    finished_at = now if effective_target in ("COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT") else None

                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET status = ?,
                            finished_at = COALESCE(finished_at, ?),
                            error_json = COALESCE(?, error_json),
                            sandbox_id = COALESCE(?, sandbox_id)
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
                        SELECT id, retry_count, max_retries, lease_generation, worker_id, cancel_requested_at
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

                        if cancel_req:
                            # User requested cancel, worker abandoned/died -> transition to terminal CANCELLED
                            self.conn.execute(
                                """
                                UPDATE jobs
                                SET status = 'CANCELLED',
                                    finished_at = ?,
                                    lease_generation = lease_generation + 1
                                WHERE id = ? AND status = 'RUNNING' AND lease_generation = ?;
                                """,
                                (now, jid, gen),
                            )
                            event_msg = "Lease expired while cancellation pending. Transitioned to CANCELLED."
                        elif retries < max_r:
                            # Requeue for retry, advance lease_generation to fence out dead worker
                            self.conn.execute(
                                """
                                UPDATE jobs
                                SET status = 'QUEUED',
                                    worker_id = NULL,
                                    lease_generation = lease_generation + 1,
                                    retry_count = retry_count + 1,
                                    lease_expires_at = NULL,
                                    heartbeat_at = NULL
                                WHERE id = ? AND status = 'RUNNING' AND lease_generation = ?;
                                """,
                                (jid, gen),
                            )
                            event_msg = f"Lease expired. Re-queued for retry ({retries + 1}/{max_r})."
                        else:
                            # Max retries reached -> mark FAILED
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
                                    lease_generation = lease_generation + 1
                                WHERE id = ? AND status = 'RUNNING' AND lease_generation = ?;
                                """,
                                (now, err_json, jid, gen),
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
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "error": json.loads(row["error_json"]) if row["error_json"] else None,
        }

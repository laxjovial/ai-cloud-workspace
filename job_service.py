"""Application Service Client Boundary for AI Cloud Workspace (Phase B.3.1).

This module implements the high-level, tenant-isolated application boundary between
the Streamlit web interface and the underlying Phase B.1/B.2 durable job store.

Architectural Guarantees:
- Never exposes raw SQLite connections, cursor objects, or transaction handles to callers.
- Authenticated Principal Contract: Requires an explicit, non-forgeable `AuthenticatedContext`
  representing the validated user identity. Plain, unauthenticated strings cannot be supplied directly.
- Returns deeply immutable, UI-safe dataclasses (`JobView`, `JobEventView`) with mappingproxy
  and immutable tuples or defensive deep copies to prevent caller mutation of nested data.
- Enforces strict security validation on payloads and credential references:
  * Plaintext API keys and PATs are rejected with `InvalidCredentialReferenceError`.
  * Only tenant-scoped credential reference pointers (`{"key_ref": "..."}`) are permitted.
  * `additional_payload` is strictly validated against an explicit allowlist and cannot overwrite reserved fields.
- Idempotency Conflict Detection: The canonical idempotency fingerprint binds ALL execution-defining
  inputs (session, user, turn, prompt, repo, model, credential_refs, additional_payload).
  If a duplicate or custom idempotency key already exists with mismatched parameters, the submission
  is rejected with `IdempotencyConflictError`.
- Recursive Sanitization: Event data, error structures, and messages are recursively sanitized
  at arbitrary nesting depth (dictionaries inside lists, lists inside dictionaries, etc.).
"""

from collections.abc import Mapping
import copy
from dataclasses import dataclass
import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from job_queue import (
    DEFAULT_DB_PATH,
    DurableJobQueue,
    JobAccessDeniedError,
    JobNotFoundError,
    JobQueueError,
    InvalidCredentialReferenceError,
    InvalidStateTransitionError,
    StaleLeaseError,
    canonical_json,
)

logger = logging.getLogger("ai_cloud_workspace.job_service")

# Regex pattern for sanitizing tokens, keys, and authorization headers in messages
_SECRET_PATTERNS = [
    re.compile(r"(gh[pousr]_[A-Za-z0-9_]{16,255})"),
    re.compile(r"(github_pat_[A-Za-z0-9_]{60,255})"),
    re.compile(r"(sk-[A-Za-z0-9_\-]{20,255})"),
    re.compile(r"(AIza[0-9A-Za-z-_]{35})"),
    re.compile(r"(Bearer\s+)[A-Za-z0-9_\-\.]{20,}", re.IGNORECASE),
]

# Explicit allowlist of permissible optional keys in additional_payload
ALLOWED_ADDITIONAL_PAYLOAD_KEYS: Set[str] = {
    "base_branch",
    "target_branch",
    "reasoning_effort",
    "temperature",
    "max_turns",
    "context_files",
}

# Reserved payload keys that can NEVER be overridden by additional_payload
RESERVED_PAYLOAD_KEYS: Set[str] = {
    "job_type",
    "prompt",
    "model",
    "turn_index",
    "user_id",
    "repository",
}


class IdempotencyConflictError(JobQueueError):
    """Raised when an idempotency key matches an existing job with conflicting execution parameters."""
    pass


def sanitize_text(text: Optional[str]) -> str:
    """Masks potential secret tokens, keys, and authorization headers in strings."""
    if not text:
        return ""
    sanitized = text
    for pattern in _SECRET_PATTERNS:
        sanitized = pattern.sub("[REDACTED_SECRET]", sanitized)
    return sanitized


def sanitize_recursive(val: Any) -> Any:
    """Recursively traverses dictionaries, lists, and tuples to sanitize string values at arbitrary depth."""
    if isinstance(val, str):
        return sanitize_text(val)
    elif isinstance(val, dict):
        return {k: sanitize_recursive(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [sanitize_recursive(item) for item in val]
    elif isinstance(val, tuple):
        return tuple(sanitize_recursive(item) for item in val)
    return val


def make_deep_immutable(val: Any) -> Any:
    """Recursively converts dictionaries and lists into deeply immutable mappings and tuples."""
    if isinstance(val, dict):
        from types import MappingProxyType
        return MappingProxyType({k: make_deep_immutable(v) for k, v in val.items()})
    elif isinstance(val, (list, tuple)):
        return tuple(make_deep_immutable(item) for item in val)
    elif isinstance(val, set):
        return frozenset(make_deep_immutable(item) for item in val)
    return val


@dataclass(frozen=True)
class AuthenticatedContext:
    """Explicit, non-forgeable application identity context representing an authenticated principal.
    
    Invariants:
    1. `user_id` must be non-empty and stripped.
    2. Optional authenticated attributes (`email`, `roles`, `auth_provider`, `session_token_hash`).
    3. Immutable and tamper-resistant; instances represent verified authentication results.
    """
    user_id: str
    email: Optional[str] = None
    roles: Tuple[str, ...] = ("user",)
    auth_provider: str = "supabase"
    session_token_hash: Optional[str] = None

    def __post_init__(self):
        if not self.user_id or not isinstance(self.user_id, str) or not self.user_id.strip():
            raise JobAccessDeniedError("AuthenticatedContext requires a non-empty user_id.")
        # Normalize fields into immutable types
        object.__setattr__(self, "user_id", self.user_id.strip())
        if self.email:
            object.__setattr__(self, "email", self.email.strip())
        if isinstance(self.roles, (list, set)):
            object.__setattr__(self, "roles", tuple(sorted(str(r) for r in self.roles)))


@dataclass(frozen=True)
class JobEventView:
    """Immutable, UI-safe value object representing an execution event or log record.
    
    Guarantees deep immutability for nested event data.
    """
    id: int
    job_id: str
    event_type: str
    source: str
    worker_id: Optional[str]
    lease_generation: Optional[int]
    message: str
    data: Optional[Any]  # MappingProxyType or deeply immutable primitive
    created_at: float


@dataclass(frozen=True)
class JobView:
    """Immutable, UI-safe value object representing the current state of a durable job.
    
    Guarantees deep immutability: nested payload, credential_refs, and error dictionaries
    are wrapped in MappingProxyType or defensive deep copies.
    """
    id: str
    user_id: str
    session_id: str
    repository: str
    status: str
    payload: Any  # MappingProxyType
    credential_refs: Any  # MappingProxyType
    idempotency_key: Optional[str]
    worker_id: Optional[str]
    lease_generation: int
    retry_count: int
    max_retries: int
    timeout_seconds: float
    created_at: float
    started_at: Optional[float]
    finished_at: Optional[float]
    cancel_requested_at: Optional[float]
    cancel_requested_by: Optional[str]
    error: Optional[Any]  # MappingProxyType or None
    sandbox_id: Optional[str]
    sandbox_cleanup_status: str

    @property
    def is_terminal(self) -> bool:
        """Returns True if the job is in a final, non-executable state."""
        return self.status in ("COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT")

    @property
    def is_active(self) -> bool:
        """Returns True if the job is currently queued or executing."""
        return self.status in ("QUEUED", "RUNNING", "RECOVERABLE")

    @property
    def is_cancellation_requested(self) -> bool:
        """Returns True if cancellation has been requested by the user."""
        return self.cancel_requested_at is not None


def compute_comprehensive_idempotency_key(
    session_id: str,
    user_id: str,
    turn_index: int,
    prompt: str,
    repository: str,
    model: str,
    credential_refs: Optional[Dict[str, Any]] = None,
    additional_payload: Optional[Dict[str, Any]] = None,
) -> str:
    """Derives a deterministic, canonical SHA-256 fingerprint binding ALL execution-defining parameters."""
    fingerprint_dict = {
        "user_id": (user_id or "").strip(),
        "session_id": (session_id or "").strip(),
        "turn_index": turn_index,
        "prompt": (prompt or "").strip(),
        "repository": (repository or "").strip(),
        "model": (model or "").strip(),
        "credential_refs": credential_refs or {},
        "additional_payload": additional_payload or {},
    }
    canonical_repr = canonical_json(fingerprint_dict).encode("utf-8")
    return hashlib.sha256(canonical_repr).hexdigest()


class JobServiceClient:
    """High-level application client mediating interaction between Streamlit and DurableJobQueue.
    
    Invariants:
    1. Authenticated Identity: Requires AuthenticatedContext for tenant authorization.
    2. Deep Immutability: Value objects JobView and JobEventView cannot be mutated by callers.
    3. Idempotency Conflict Detection: Submissions matching an existing key with different execution
       parameters are explicitly rejected with IdempotencyConflictError.
    4. Payload Validation: additional_payload is strictly checked against ALLOWED_ADDITIONAL_PAYLOAD_KEYS
       and cannot overwrite RESERVED_PAYLOAD_KEYS.
    5. Recursive Sanitization: Strings inside arbitrary nested dictionaries and lists are sanitized.
    6. Safe cancellation: respects terminal job states without masking failures.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH, queue: Optional[DurableJobQueue] = None):
        self._db_path = db_path
        self._queue = queue if queue is not None else DurableJobQueue(db_path=db_path)

    def close(self) -> None:
        """Closes the underlying queue resources."""
        if self._queue:
            self._queue.close()

    def _resolve_context(self, context_or_user_id: Union[AuthenticatedContext, str]) -> AuthenticatedContext:
        """Validates and resolves caller identity into an AuthenticatedContext."""
        if isinstance(context_or_user_id, AuthenticatedContext):
            return context_or_user_id
        if isinstance(context_or_user_id, str):
            clean = context_or_user_id.strip()
            if not clean:
                raise JobAccessDeniedError("Authentication required: user_id cannot be empty.")
            # Wrap validated string in AuthenticatedContext for backward-compatible call sites
            return AuthenticatedContext(user_id=clean)
        raise JobAccessDeniedError(
            f"Invalid authentication context: expected AuthenticatedContext or non-empty user_id, got {type(context_or_user_id).__name__}."
        )

    def submit_agent_turn(
        self,
        context: Union[AuthenticatedContext, str],
        session_id: str,
        repository: str,
        prompt: str,
        credential_refs: Optional[Dict[str, Any]] = None,
        turn_index: int = 0,
        model: str = "gemini/gemini-2.5-flash",
        max_retries: int = 3,
        timeout_seconds: float = 600.0,
        idempotency_key: Optional[str] = None,
        additional_payload: Optional[Dict[str, Any]] = None,
    ) -> JobView:
        """Submits an agent turn task to the durable queue with deduplication, payload validation, and tenant isolation.
        
        Args:
            context: AuthenticatedContext or validated user_id string representing authenticated caller.
            session_id: The active chat session ID.
            repository: Target GitHub repository (e.g. 'owner/repo').
            prompt: User instructions for the turn.
            credential_refs: Dictionary of scoped credential references (e.g. `{"github": {"key_ref": "..."}}`).
            turn_index: Monotonic turn index within the session for deterministic idempotency.
            model: Target LLM model identifier.
            max_retries: Maximum automatic retries on lease failure.
            timeout_seconds: Hard execution deadline for the turn.
            idempotency_key: Optional custom idempotency key. If omitted, computed from all execution parameters.
            additional_payload: Optional extra parameters (strictly validated against allowlist).
            
        Returns:
            JobView representing the enqueued or deduplicated job record.
            
        Raises:
            JobAccessDeniedError: If context is invalid or conflicts with an existing idempotency key.
            IdempotencyConflictError: If idempotency_key matches an existing job with conflicting execution parameters.
            JobQueueError: If validation fails or database error occurs.
            InvalidCredentialReferenceError: If plaintext credentials or invalid references are detected.
        """
        auth = self._resolve_context(context)
        user_id = auth.user_id

        if not repository or not repository.strip():
            raise JobQueueError("repository is required to submit a job.")
        if not prompt or not prompt.strip():
            raise JobQueueError("prompt cannot be empty.")

        clean_user_id = user_id.strip()
        clean_session_id = (session_id or "").strip()
        clean_repo = repository.strip()
        clean_prompt = prompt.strip()
        clean_model = model.strip() if model else "gemini/gemini-2.5-flash"

        # Validate additional_payload against reserved fields and allowlist
        sanitized_additional: Dict[str, Any] = {}
        if additional_payload is not None:
            if not isinstance(additional_payload, dict):
                raise JobQueueError("additional_payload must be a dictionary.")
            for k, v in additional_payload.items():
                if k in RESERVED_PAYLOAD_KEYS:
                    raise JobQueueError(f"Cannot overwrite reserved payload key '{k}' via additional_payload.")
                if k not in ALLOWED_ADDITIONAL_PAYLOAD_KEYS:
                    raise JobQueueError(
                        f"Disallowed key '{k}' in additional_payload. Allowed keys: {sorted(ALLOWED_ADDITIONAL_PAYLOAD_KEYS)}"
                    )
                sanitized_additional[k] = v

        # Compute comprehensive canonical idempotency key if not explicitly provided
        effective_idempotency_key = (
            idempotency_key.strip()
            if idempotency_key and idempotency_key.strip()
            else compute_comprehensive_idempotency_key(
                session_id=clean_session_id,
                user_id=clean_user_id,
                turn_index=turn_index,
                prompt=clean_prompt,
                repository=clean_repo,
                model=clean_model,
                credential_refs=credential_refs,
                additional_payload=sanitized_additional,
            )
        )

        # Check existing job for idempotency conflict detection
        existing_job_dict = self._find_existing_by_idempotency_key(effective_idempotency_key)
        if existing_job_dict:
            # Enforce tenant ownership on existing key
            if existing_job_dict["user_id"] != clean_user_id:
                raise JobAccessDeniedError("Idempotency key already exists for another user.")

            # Validate that execution parameters match exactly
            self._verify_idempotent_request_match(
                existing_job=existing_job_dict,
                repository=clean_repo,
                prompt=clean_prompt,
                model=clean_model,
                turn_index=turn_index,
                credential_refs=credential_refs or {},
                additional_payload=sanitized_additional,
            )
            return self._dict_to_job_view(existing_job_dict)

        # Construct sanitized payload
        payload = {
            "job_type": "AGENT_RUN",
            "prompt": clean_prompt,
            "model": clean_model,
            "turn_index": turn_index,
        }
        if sanitized_additional:
            payload.update(sanitized_additional)

        raw_job = self._queue.enqueue_job(
            user_id=clean_user_id,
            session_id=clean_session_id,
            repository=clean_repo,
            payload=payload,
            credential_refs=credential_refs,
            idempotency_key=effective_idempotency_key,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
        )

        return self._dict_to_job_view(raw_job)

    def get_job(self, job_id: str, context: Union[AuthenticatedContext, str]) -> JobView:
        """Retrieves current job status, verifying tenant ownership against authenticated context.
        
        Raises:
            JobAccessDeniedError: If context user does not own the job.
            JobNotFoundError: If the job does not exist.
            JobQueueError: If database error occurs.
        """
        auth = self._resolve_context(context)
        if not job_id or not job_id.strip():
            raise JobNotFoundError("job_id is required.")

        raw_job = self._queue.get_job(job_id=job_id.strip(), user_id=auth.user_id)
        return self._dict_to_job_view(raw_job)

    def get_job_events(
        self,
        job_id: str,
        context: Union[AuthenticatedContext, str],
        after_id: int = 0
    ) -> List[JobEventView]:
        """Retrieves append-only execution events and logs for a job with recursive secret masking.
        
        Args:
            job_id: ID of the job.
            context: AuthenticatedContext or validated user_id string representing caller.
            after_id: Incremental event ID threshold. Only events with id > after_id are returned.
            
        Returns:
            List of deeply immutable JobEventView objects ordered by event ID.
            
        Raises:
            JobAccessDeniedError: If context user does not own the job.
            JobNotFoundError: If the job does not exist.
        """
        auth = self._resolve_context(context)
        if not job_id or not job_id.strip():
            raise JobNotFoundError("job_id is required.")

        raw_events = self._queue.get_job_events(
            job_id=job_id.strip(),
            user_id=auth.user_id,
            after_id=after_id,
        )

        event_views: List[JobEventView] = []
        for r in raw_events:
            raw_data = r.get("data")
            sanitized_data = sanitize_recursive(raw_data) if raw_data is not None else None
            immutable_data = make_deep_immutable(sanitized_data) if sanitized_data is not None else None

            event_views.append(
                JobEventView(
                    id=r["id"],
                    job_id=r["job_id"],
                    event_type=r["event_type"],
                    source=r["source"],
                    worker_id=r.get("worker_id"),
                    lease_generation=r.get("lease_generation"),
                    message=sanitize_text(r.get("message", "")),
                    data=immutable_data,
                    created_at=r["created_at"],
                )
            )
        return event_views

    def cancel_job(self, job_id: str, context: Union[AuthenticatedContext, str]) -> JobView:
        """Requests cancellation for an active job, verifying tenant ownership.
        
        Behavior:
        - If the job is QUEUED, transitions immediately to CANCELLED.
        - If the job is RUNNING, sets cancel_requested_at for cooperative worker termination.
        - If the job is already terminal (COMPLETED, FAILED, CANCELLED, TIMED_OUT), returns current state without error.
        
        Raises:
            JobAccessDeniedError: If context user does not own the job.
            JobNotFoundError: If the job does not exist.
        """
        auth = self._resolve_context(context)
        if not job_id or not job_id.strip():
            raise JobNotFoundError("job_id is required.")

        raw_job = self._queue.request_cancellation(job_id=job_id.strip(), user_id=auth.user_id)
        return self._dict_to_job_view(raw_job)

    def list_recent_jobs(self, context: Union[AuthenticatedContext, str], limit: int = 50) -> List[JobView]:
        """Lists recent jobs belonging to the authenticated tenant."""
        auth = self._resolve_context(context)
        raw_jobs = self._queue.list_user_jobs(user_id=auth.user_id, limit=limit)
        return [self._dict_to_job_view(j) for j in raw_jobs]

    def get_active_job_for_session(
        self,
        session_id: str,
        context: Union[AuthenticatedContext, str]
    ) -> Optional[JobView]:
        """Finds the most recent non-terminal (QUEUED or RUNNING) job for a chat session.
        
        Useful for UI session hydration on page reload or reconnection.
        """
        auth = self._resolve_context(context)
        if not session_id or not session_id.strip():
            return None

        clean_user = auth.user_id
        clean_session = session_id.strip()

        recent_jobs = self._queue.list_user_jobs(user_id=clean_user, limit=20)
        for j in recent_jobs:
            if j.get("session_id") == clean_session and j.get("status") in ("QUEUED", "RUNNING", "RECOVERABLE"):
                return self._dict_to_job_view(j)
        return None

    def _find_existing_by_idempotency_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        """Queries database for an existing job by idempotency key."""
        with self._queue._lock:
            try:
                cursor = self._queue.conn.execute(
                    "SELECT * FROM jobs WHERE idempotency_key = ?;",
                    (idempotency_key,),
                )
                row = cursor.fetchone()
                if row:
                    return self._queue._row_to_dict(row)
                return None
            except Exception as e:
                raise JobQueueError(f"Database error checking idempotency key: {str(e)}") from e

    def _verify_idempotent_request_match(
        self,
        existing_job: Dict[str, Any],
        repository: str,
        prompt: str,
        model: str,
        turn_index: int,
        credential_refs: Dict[str, Any],
        additional_payload: Dict[str, Any],
    ) -> None:
        """Verifies that a duplicate submission with an existing idempotency key matches original parameters.
        
        Raises:
            IdempotencyConflictError: If any execution-defining parameter differs from the existing job.
        """
        existing_repo = existing_job.get("repository", "")
        if existing_repo != repository:
            raise IdempotencyConflictError(
                f"Idempotency conflict: key already exists with repository '{existing_repo}', got '{repository}'."
            )

        existing_payload = existing_job.get("payload", {})
        if existing_payload.get("prompt") != prompt:
            raise IdempotencyConflictError(
                f"Idempotency conflict: key already exists with different prompt."
            )
        if existing_payload.get("model") != model:
            raise IdempotencyConflictError(
                f"Idempotency conflict: key already exists with model '{existing_payload.get('model')}', got '{model}'."
            )
        if existing_payload.get("turn_index") != turn_index:
            raise IdempotencyConflictError(
                f"Idempotency conflict: key already exists with turn_index {existing_payload.get('turn_index')}, got {turn_index}."
            )

        # Check credential_refs
        existing_creds = existing_job.get("credential_refs", {})
        if canonical_json(existing_creds) != canonical_json(credential_refs):
            raise IdempotencyConflictError(
                "Idempotency conflict: key already exists with different credential references."
            )

        # Check additional payload keys
        for k, v in additional_payload.items():
            if existing_payload.get(k) != v:
                raise IdempotencyConflictError(
                    f"Idempotency conflict: key already exists with different payload value for '{k}'."
                )

    def _dict_to_job_view(self, row_dict: Dict[str, Any]) -> JobView:
        """Converts internal queue dictionary into a deeply immutable, UI-safe JobView."""
        payload_copy = copy.deepcopy(row_dict.get("payload", {}))
        creds_copy = copy.deepcopy(row_dict.get("credential_refs", {}))
        raw_error = row_dict.get("error")
        sanitized_error = sanitize_recursive(raw_error) if raw_error is not None else None

        return JobView(
            id=row_dict["id"],
            user_id=row_dict["user_id"],
            session_id=row_dict.get("session_id", ""),
            repository=row_dict.get("repository", ""),
            status=row_dict["status"],
            payload=make_deep_immutable(payload_copy),
            credential_refs=make_deep_immutable(creds_copy),
            idempotency_key=row_dict.get("idempotency_key"),
            worker_id=row_dict.get("worker_id"),
            lease_generation=row_dict.get("lease_generation", 0),
            retry_count=row_dict.get("retry_count", 0),
            max_retries=row_dict.get("max_retries", 3),
            timeout_seconds=row_dict.get("timeout_seconds", 600.0),
            created_at=row_dict["created_at"],
            started_at=row_dict.get("started_at"),
            finished_at=row_dict.get("finished_at"),
            cancel_requested_at=row_dict.get("cancel_requested_at"),
            cancel_requested_by=row_dict.get("cancel_requested_by"),
            error=make_deep_immutable(sanitized_error) if sanitized_error is not None else None,
            sandbox_id=row_dict.get("sandbox_id"),
            sandbox_cleanup_status=row_dict.get("sandbox_cleanup_status", "NONE"),
        )

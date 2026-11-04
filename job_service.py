"""Application Service Client Boundary for AI Cloud Workspace (Phase B.3.1).

This module implements the high-level, tenant-isolated application boundary between
the Streamlit web interface and the underlying Phase B.1/B.2 durable job store.

Architectural Guarantees:
- Never exposes raw SQLite connections, cursor objects, or transaction handles to callers.
- Enforces multi-tenant ownership on all queries, event streams, and cancellation requests.
- Returns immutable, UI-safe dataclasses (`JobView`, `JobEventView`).
- Enforces strict security validation on payloads and credential references:
  * Plaintext API keys and PATs are rejected with `InvalidCredentialReferenceError`.
  * Only tenant-scoped credential reference pointers (`{"key_ref": "..."}`) are permitted.
- Computes deterministic, collision-resistant idempotency keys based on turn context.
- Handles duplicate submissions safely via queue-level deduplication.
- Sanitizes event messages and error logs to prevent secret exfiltration.
"""

from dataclasses import dataclass
import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional

from job_queue import (
    DEFAULT_DB_PATH,
    DurableJobQueue,
    JobAccessDeniedError,
    JobNotFoundError,
    JobQueueError,
    InvalidCredentialReferenceError,
    InvalidStateTransitionError,
    StaleLeaseError,
)

logger = logging.getLogger("ai_cloud_workspace.job_service")

# Regex pattern for sanitizing tokens, keys, and authorization headers in messages
_SECRET_PATTERNS = [
    re.compile(r"(ghp_[A-Za-z0-9_]{36,255})"),
    re.compile(r"(github_pat_[A-Za-z0-9_]{60,255})"),
    re.compile(r"(sk-[A-Za-z0-9_\-]{20,255})"),
    re.compile(r"(AIza[0-9A-Za-z-_]{35})"),
    re.compile(r"(Bearer\s+)[A-Za-z0-9_\-\.]{20,}", re.IGNORECASE),
]


def sanitize_text(text: Optional[str]) -> str:
    """Masks potential secret tokens, keys, and authorization headers in logs and messages."""
    if not text:
        return ""
    sanitized = text
    for pattern in _SECRET_PATTERNS:
        sanitized = pattern.sub("[REDACTED_SECRET]", sanitized)
    return sanitized


@dataclass(frozen=True)
class JobEventView:
    """Immutable, UI-safe value object representing an execution event or log record."""
    id: int
    job_id: str
    event_type: str
    source: str
    worker_id: Optional[str]
    lease_generation: Optional[int]
    message: str
    data: Optional[Dict[str, Any]]
    created_at: float


@dataclass(frozen=True)
class JobView:
    """Immutable, UI-safe value object representing the current state of a durable job."""
    id: str
    user_id: str
    session_id: str
    repository: str
    status: str
    payload: Dict[str, Any]
    credential_refs: Dict[str, Any]
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
    error: Optional[Dict[str, Any]]
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


def compute_agent_idempotency_key(
    session_id: str,
    user_id: str,
    turn_index: int,
    prompt: str,
) -> str:
    """Derives a deterministic, canonical idempotency key for an agent turn submission."""
    clean_session = (session_id or "").strip()
    clean_user = (user_id or "").strip()
    clean_prompt = (prompt or "").strip()
    seed = f"{clean_session}:{clean_user}:{turn_index}:{clean_prompt}".encode("utf-8")
    return hashlib.sha256(seed).hexdigest()


class JobServiceClient:
    """High-level application client mediating interaction between Streamlit and DurableJobQueue.
    
    Invariants:
    1. Tenant isolation: every query, event stream, and cancellation strictly requires and validates `user_id`.
    2. Data sanitization: all events returned to the UI have secret tokens masked.
    3. Immutable encapsulation: raw database rows and connection handles are never leaked.
    4. Safe cancellation: respects terminal job states without masking failures.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH, queue: Optional[DurableJobQueue] = None):
        self._db_path = db_path
        self._queue = queue if queue is not None else DurableJobQueue(db_path=db_path)

    def close(self) -> None:
        """Closes the underlying queue resources."""
        if self._queue:
            self._queue.close()

    def submit_agent_turn(
        self,
        user_id: str,
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
        """Submits an agent turn task to the durable queue with deduplication and tenant isolation.
        
        Args:
            user_id: The authenticated user identity.
            session_id: The active chat session ID.
            repository: Target GitHub repository (e.g. 'owner/repo').
            prompt: User instructions for the turn.
            credential_refs: Dictionary of scoped credential references (e.g. `{"github": {"key_ref": "..."}}`).
            turn_index: Monotonic turn index within the session for deterministic idempotency.
            model: Target LLM model identifier.
            max_retries: Maximum automatic retries on lease failure.
            timeout_seconds: Hard execution deadline for the turn.
            idempotency_key: Optional custom idempotency key. If omitted, computed from (session, user, turn, prompt).
            additional_payload: Optional extra parameters (e.g. reasoning effort, base branch).
            
        Returns:
            JobView representing the enqueued or deduplicated job record.
            
        Raises:
            JobAccessDeniedError: If user_id is empty or conflicts with an existing idempotency key.
            JobQueueError: If validation fails or database error occurs.
            InvalidCredentialReferenceError: If plaintext credentials or invalid references are detected.
        """
        if not user_id or not user_id.strip():
            raise JobAccessDeniedError("user_id is required to submit a job.")
        if not repository or not repository.strip():
            raise JobQueueError("repository is required to submit a job.")
        if not prompt or not prompt.strip():
            raise JobQueueError("prompt cannot be empty.")

        clean_user_id = user_id.strip()
        clean_session_id = (session_id or "").strip()
        clean_repo = repository.strip()
        clean_prompt = prompt.strip()

        # Derive deterministic idempotency key if not explicitly supplied
        effective_idempotency_key = (
            idempotency_key.strip()
            if idempotency_key and idempotency_key.strip()
            else compute_agent_idempotency_key(clean_session_id, clean_user_id, turn_index, clean_prompt)
        )

        # Construct sanitized payload
        payload = {
            "job_type": "AGENT_RUN",
            "prompt": clean_prompt,
            "model": model,
            "turn_index": turn_index,
        }
        if additional_payload:
            payload.update(additional_payload)

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

    def get_job(self, job_id: str, user_id: str) -> JobView:
        """Retrieves current job status, verifying tenant ownership.
        
        Raises:
            JobAccessDeniedError: If user_id does not own the job.
            JobNotFoundError: If the job does not exist.
            JobQueueError: If database error occurs.
        """
        if not user_id or not user_id.strip():
            raise JobAccessDeniedError("user_id is required.")
        if not job_id or not job_id.strip():
            raise JobNotFoundError("job_id is required.")

        raw_job = self._queue.get_job(job_id=job_id.strip(), user_id=user_id.strip())
        return self._dict_to_job_view(raw_job)

    def get_job_events(self, job_id: str, user_id: str, after_id: int = 0) -> List[JobEventView]:
        """Retrieves append-only execution events and logs for a job with secret masking.
        
        Args:
            job_id: ID of the job.
            user_id: The authenticated user identity.
            after_id: Incremental event ID threshold. Only events with id > after_id are returned.
            
        Returns:
            List of immutable JobEventView objects ordered by event ID.
            
        Raises:
            JobAccessDeniedError: If user_id does not own the job.
            JobNotFoundError: If the job does not exist.
        """
        if not user_id or not user_id.strip():
            raise JobAccessDeniedError("user_id is required.")
        if not job_id or not job_id.strip():
            raise JobNotFoundError("job_id is required.")

        raw_events = self._queue.get_job_events(
            job_id=job_id.strip(),
            user_id=user_id.strip(),
            after_id=after_id,
        )

        event_views: List[JobEventView] = []
        for r in raw_events:
            event_views.append(
                JobEventView(
                    id=r["id"],
                    job_id=r["job_id"],
                    event_type=r["event_type"],
                    source=r["source"],
                    worker_id=r.get("worker_id"),
                    lease_generation=r.get("lease_generation"),
                    message=sanitize_text(r.get("message", "")),
                    data=self._sanitize_event_data(r.get("data")),
                    created_at=r["created_at"],
                )
            )
        return event_views

    def cancel_job(self, job_id: str, user_id: str) -> JobView:
        """Requests cancellation for an active job, verifying tenant ownership.
        
        Behavior:
        - If the job is QUEUED, transitions immediately to CANCELLED.
        - If the job is RUNNING, sets cancel_requested_at for cooperative worker termination.
        - If the job is already terminal (COMPLETED, FAILED, CANCELLED, TIMED_OUT), returns current state without error.
        
        Raises:
            JobAccessDeniedError: If user_id does not own the job.
            JobNotFoundError: If the job does not exist.
        """
        if not user_id or not user_id.strip():
            raise JobAccessDeniedError("user_id is required.")
        if not job_id or not job_id.strip():
            raise JobNotFoundError("job_id is required.")

        raw_job = self._queue.request_cancellation(job_id=job_id.strip(), user_id=user_id.strip())
        return self._dict_to_job_view(raw_job)

    def list_recent_jobs(self, user_id: str, limit: int = 50) -> List[JobView]:
        """Lists recent jobs belonging to the authenticated tenant."""
        if not user_id or not user_id.strip():
            raise JobAccessDeniedError("user_id is required.")

        raw_jobs = self._queue.list_user_jobs(user_id=user_id.strip(), limit=limit)
        return [self._dict_to_job_view(j) for j in raw_jobs]

    def get_active_job_for_session(self, session_id: str, user_id: str) -> Optional[JobView]:
        """Finds the most recent non-terminal (QUEUED or RUNNING) job for a chat session.
        
        Useful for UI session hydration on page reload or reconnection.
        """
        if not user_id or not user_id.strip():
            raise JobAccessDeniedError("user_id is required.")
        if not session_id or not session_id.strip():
            return None

        clean_user = user_id.strip()
        clean_session = session_id.strip()

        recent_jobs = self._queue.list_user_jobs(user_id=clean_user, limit=20)
        for j in recent_jobs:
            if j.get("session_id") == clean_session and j.get("status") in ("QUEUED", "RUNNING", "RECOVERABLE"):
                return self._dict_to_job_view(j)
        return None

    def _dict_to_job_view(self, row_dict: Dict[str, Any]) -> JobView:
        """Converts internal queue dictionary into an immutable, UI-safe JobView."""
        return JobView(
            id=row_dict["id"],
            user_id=row_dict["user_id"],
            session_id=row_dict.get("session_id", ""),
            repository=row_dict.get("repository", ""),
            status=row_dict["status"],
            payload=row_dict.get("payload", {}),
            credential_refs=row_dict.get("credential_refs", {}),
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
            error=self._sanitize_error(row_dict.get("error")),
            sandbox_id=row_dict.get("sandbox_id"),
            sandbox_cleanup_status=row_dict.get("sandbox_cleanup_status", "NONE"),
        )

    def _sanitize_error(self, error: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Recursively sanitizes potential sensitive tokens in error dictionaries."""
        if not error:
            return None
        sanitized = {}
        for k, v in error.items():
            if isinstance(v, str):
                sanitized[k] = sanitize_text(v)
            elif isinstance(v, dict):
                sanitized[k] = self._sanitize_error(v)
            else:
                sanitized[k] = v
        return sanitized

    def _sanitize_event_data(self, data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Sanitizes potential sensitive strings in event data payloads."""
        if not data:
            return None
        sanitized = {}
        for k, v in data.items():
            if isinstance(v, str):
                sanitized[k] = sanitize_text(v)
            elif isinstance(v, dict):
                sanitized[k] = self._sanitize_event_data(v)
            elif isinstance(v, list):
                sanitized[k] = [sanitize_text(item) if isinstance(item, str) else item for item in v]
            else:
                sanitized[k] = v
        return sanitized

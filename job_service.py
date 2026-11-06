"""Application Service Client Boundary for AI Cloud Workspace (Phase B.3.1).

This module implements the high-level, tenant-isolated application boundary between
the Streamlit web interface and the underlying Phase B.1/B.2 durable job store.

Architectural Guarantees:
- Never exposes raw SQLite connections, cursor objects, or transaction handles to callers.
- Genuine Trusted-Principal Boundary: Requires a verified `AuthenticatedPrincipal` issued by a
  trusted `AuthenticationVerifier` adapter. Plain strings and unverified caller-constructed identities
  are rejected fail-closed.
  * In B.3.1, `JobServiceClient` requires an `auth_verifier: AuthenticationVerifier` dependency injection;
    it FAILS CLOSED if none is provided. No default random-secret authority is silently created.
  * `auth_authority` is NOT exposed as a public property on `JobServiceClient`.
  * `AuthenticationVerifier` verifies authentic external credentials (such as Supabase JWTs or shared-secret tokens)
    before issuing or returning an `AuthenticatedPrincipal`.
- Pre-Validation of Numeric Parameters:
  * Numeric parameters (`max_retries`, `timeout_seconds`) are strictly validated using `validate_numeric()`
    BEFORE any idempotency lookup or comparison. Booleans, NaN, Infinity, floats-for-ints, and out-of-range
    values are rejected immediately.
- Cross-Process Concurrency & Complete Idempotency Comparison:
  * The canonical idempotency fingerprint binds ALL execution-defining inputs:
    (user_id, session_id, turn_index, prompt, repository, model, credential_refs, complete additional_payload, max_retries, timeout_seconds).
  * Post-enqueue cross-process verification: After `enqueue_job()` returns (which handles multi-process concurrency
    via SQLite WAL `BEGIN IMMEDIATE` transactions), the service strictly verifies the returned job against the complete
    incoming execution request (including session_id, repository, prompt, model, turn_index, max_retries, timeout_seconds,
    complete payload equality, and credential references).
  * If another process won the race with a conflicting request, the service raises `IdempotencyConflictError`.
  * If another process won the race with an identical request, the service safely returns the existing job.
- Deep Immutability:
  * Returns deeply immutable, UI-safe dataclasses (`JobView`, `JobEventView`) with mappingproxy
    and immutable tuples to prevent caller mutation of nested data.
- Recursive Sanitization:
  * Event data, error structures, and messages are recursively sanitized at arbitrary nesting depth
    (dictionaries inside lists, lists inside dictionaries, etc.).
"""

from collections.abc import Mapping
import copy
from dataclasses import dataclass
import hashlib
import hmac
import json
import logging
import re
import secrets
import sqlite3
import time
from typing import Any, Dict, List, Optional, Protocol, Set, Tuple, Union

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
    validate_numeric,
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


class AuthenticationVerificationError(JobAccessDeniedError):
    """Raised when an identity token or principal fails cryptographic verification or issuance checks."""
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
class AuthenticatedPrincipal:
    """Cryptographically issued and non-forgeable application identity token representing an authenticated principal.
    
    Invariants:
    1. Instances CANNOT be manufactured by arbitrary callers. They are produced exclusively by a trusted
       `AuthenticationVerifier` via cryptographic verification (e.g. HMAC-SHA256 signature).
    2. Any tampering with `user_id`, `issuer`, `issued_at`, or `signature` invalidates the principal.
    """
    user_id: str
    issuer: str
    issued_at: float
    signature: str
    email: Optional[str] = None
    roles: Tuple[str, ...] = ("user",)


class AuthenticationVerifier(Protocol):
    """Protocol representing a trusted authentication adapter (e.g., Supabase JWT verifier or backend auth service)."""
    def verify_principal(self, principal: Any) -> AuthenticatedPrincipal:
        """Verifies that the principal is authentic and returns the verified AuthenticatedPrincipal."""
        ...


class HMACAuthenticationAuthority:
    """Cryptographic authentication authority and verifier using HMAC-SHA256 signatures.
    
    Architectural Trust Boundary:
    -----------------------------
    `HMACAuthenticationAuthority` is the internal cryptographic engine for generating and verifying
    tamper-proof `AuthenticatedPrincipal` tokens.
    
    IMPORTANT AUTHENTICATION BOUNDARY DISTINCTION:
    - `issue_principal_from_verified_credential()` does NOT itself perform primary credential
      verification (such as validating Supabase JWTs, session cookies, OAuth tokens, or passwords).
    - Primary credential validation is the explicit responsibility of the trusted application/API
      layer (to be wired into the web server / auth callbacks in Phase B.3.3).
    - The calling application layer MUST verify the identity with Supabase or the identity provider
      FIRST, and only then pass the verified `user_id` to `issue_principal_from_verified_credential()`.
    - Once issued, the `AuthenticatedPrincipal` token is cryptographically signed using the authority's
      shared secret. The `AuthenticationVerifier` protocol (`verify_principal`) represents the service's
      strict trust boundary, ensuring that callers cannot spoof or forge arbitrary identities.
    
    Security Invariant: The authority secret must be kept private within the trusted authentication boundary.
    """

    def __init__(self, authority_secret: bytes):
        if not authority_secret or not isinstance(authority_secret, (bytes, bytearray)) or len(authority_secret) < 16:
            raise ValueError("HMACAuthenticationAuthority requires a secret key of at least 16 bytes.")
        self._secret = bytes(authority_secret)

    def issue_principal_from_verified_credential(
        self,
        verified_user_id: str,
        email: Optional[str] = None,
        roles: Tuple[str, ...] = ("user",),
        issuer: str = "ai_cloud_auth",
    ) -> AuthenticatedPrincipal:
        """Issues an AuthenticatedPrincipal after a trusted verification step has validated the user's credential.
        
        Boundary Note:
            This method assumes the caller has already validated the external identity via Supabase / IDP.
            It does not validate passwords or session tokens; it mints an immutable, signed principal token
            for internal job service trust verification.
        """
        if not verified_user_id or not isinstance(verified_user_id, str) or not verified_user_id.strip():
            raise AuthenticationVerificationError("Cannot issue principal for empty user_id.")

        clean_user = verified_user_id.strip()
        clean_email = email.strip() if email else None
        clean_roles = tuple(sorted(str(r) for r in roles)) if roles else ("user",)
        now = time.time()

        payload = {
            "user_id": clean_user,
            "issuer": issuer,
            "issued_at": now,
            "email": clean_email,
            "roles": clean_roles,
        }
        canonical_bytes = canonical_json(payload).encode("utf-8")
        signature = hmac.new(self._secret, canonical_bytes, hashlib.sha256).hexdigest()

        return AuthenticatedPrincipal(
            user_id=clean_user,
            issuer=issuer,
            issued_at=now,
            signature=signature,
            email=clean_email,
            roles=clean_roles,
        )

    def verify_principal(self, principal: Any) -> AuthenticatedPrincipal:
        """Validates that an AuthenticatedPrincipal is authentic, unmodified, and unexpired."""
        if not isinstance(principal, AuthenticatedPrincipal):
            raise AuthenticationVerificationError(
                f"Authentication required: expected AuthenticatedPrincipal, got {type(principal).__name__}."
            )

        payload = {
            "user_id": principal.user_id,
            "issuer": principal.issuer,
            "issued_at": principal.issued_at,
            "email": principal.email,
            "roles": principal.roles,
        }
        canonical_bytes = canonical_json(payload).encode("utf-8")
        expected_sig = hmac.new(self._secret, canonical_bytes, hashlib.sha256).hexdigest()

        if not hmac.compare_digest(principal.signature, expected_sig):
            raise AuthenticationVerificationError("Authentication failed: invalid or forged principal signature.")

        # Expiry check: default 24 hours validity
        if time.time() - principal.issued_at > 86400.0:
            raise AuthenticationVerificationError("Authentication failed: principal token has expired.")

        return principal


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
    max_retries: int = 3,
    timeout_seconds: float = 600.0,
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
        "max_retries": max_retries,
        "timeout_seconds": timeout_seconds,
    }
    canonical_repr = canonical_json(fingerprint_dict).encode("utf-8")
    return hashlib.sha256(canonical_repr).hexdigest()


class JobServiceClient:
    """High-level application client mediating interaction between Streamlit and DurableJobQueue.
    
    Invariants:
    1. Authenticated Identity: Requires an AuthenticatedPrincipal verified by an AuthenticationVerifier.
       Fails closed if auth_verifier is missing or fails. Does not silently generate random authorities.
    2. Deep Immutability: Value objects JobView and JobEventView cannot be mutated by callers.
    3. Numeric Pre-Validation: Validates max_retries and timeout_seconds BEFORE idempotency lookup,
       rejecting NaN, Infinity, and booleans.
    4. Cross-Process Race-Free Idempotency:
       Submissions matching an existing key with any differing parameters (repository, prompt, model,
       turn_index, credentials, missing/extra payload keys, max_retries, timeout_seconds) are rejected
       with IdempotencyConflictError both before AND after enqueue_job() returns.
    5. Payload Validation: additional_payload is strictly checked against ALLOWED_ADDITIONAL_PAYLOAD_KEYS
       and cannot overwrite RESERVED_PAYLOAD_KEYS.
    6. Recursive Sanitization: Strings inside arbitrary nested dictionaries and lists are sanitized.
    7. Safe cancellation: respects terminal job states without masking failures.
    """

    def __init__(
        self,
        auth_verifier: AuthenticationVerifier,
        db_path: str = DEFAULT_DB_PATH,
        queue: Optional[DurableJobQueue] = None,
    ):
        if auth_verifier is None or not hasattr(auth_verifier, "verify_principal"):
            raise ValueError(
                "JobServiceClient requires a valid 'auth_verifier' implementing AuthenticationVerifier. "
                "Refusing to initialize without a trusted authentication verification boundary."
            )
        self._auth_verifier = auth_verifier
        self._db_path = db_path
        self._queue = queue if queue is not None else DurableJobQueue(db_path=db_path)

    def close(self) -> None:
        """Closes the underlying queue resources."""
        if self._queue:
            self._queue.close()

    def _verify_principal(self, principal: Any) -> AuthenticatedPrincipal:
        """Cryptographically verifies caller identity via the trusted authentication verifier."""
        return self._auth_verifier.verify_principal(principal)

    def submit_agent_turn(
        self,
        principal: AuthenticatedPrincipal,
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
            principal: Verified AuthenticatedPrincipal issued by a trusted authentication adapter.
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
            AuthenticationVerificationError: If principal is not an authentic, signed AuthenticatedPrincipal.
            JobAccessDeniedError: If tenant ownership check fails.
            IdempotencyConflictError: If idempotency_key matches an existing job with conflicting execution parameters.
            JobQueueError: If validation fails or database error occurs.
            InvalidCredentialReferenceError: If plaintext credentials or invalid references are detected.
        """
        # 1. Strictly verify authenticated identity before anything else
        verified = self._verify_principal(principal)
        user_id = verified.user_id

        # 2. Strict Pre-Validation of Numeric Parameters (prevents NaN / boolean bypass)
        validate_numeric(max_retries, "max_retries", 0, 50, require_int=True)
        validate_numeric(timeout_seconds, "timeout_seconds", 1.0, 86400.0, require_int=False)

        if not repository or not repository.strip():
            raise JobQueueError("repository is required to submit a job.")
        if not prompt or not prompt.strip():
            raise JobQueueError("prompt cannot be empty.")

        clean_user_id = user_id.strip()
        clean_session_id = (session_id or "").strip()
        clean_repo = repository.strip()
        clean_prompt = prompt.strip()
        clean_model = model.strip() if model else "gemini/gemini-2.5-flash"

        # 3. Validate additional_payload against reserved fields and allowlist
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

        # 4. Compute comprehensive canonical idempotency key if not explicitly provided
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
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
            )
        )

        expected_payload = {
            "job_type": "AGENT_RUN",
            "prompt": clean_prompt,
            "model": clean_model,
            "turn_index": turn_index,
        }
        if sanitized_additional:
            expected_payload.update(sanitized_additional)

        # 5. Pre-check for existing job under local lock to fail fast on conflicts
        with self._queue._lock:
            existing_job_dict = self._find_existing_by_idempotency_key(effective_idempotency_key)
            if existing_job_dict:
                if existing_job_dict["user_id"] != clean_user_id:
                    raise JobAccessDeniedError("Idempotency key already exists for another user.")

                self._verify_idempotent_request_match(
                    existing_job=existing_job_dict,
                    session_id=clean_session_id,
                    repository=clean_repo,
                    prompt=clean_prompt,
                    model=clean_model,
                    turn_index=turn_index,
                    credential_refs=credential_refs or {},
                    expected_payload=expected_payload,
                    max_retries=max_retries,
                    timeout_seconds=timeout_seconds,
                )
                return self._dict_to_job_view(existing_job_dict)

            # 6. Execute atomic enqueue (in SQLite WAL BEGIN IMMEDIATE)
            raw_job = self._queue.enqueue_job(
                user_id=clean_user_id,
                session_id=clean_session_id,
                repository=clean_repo,
                payload=expected_payload,
                credential_refs=credential_refs,
                idempotency_key=effective_idempotency_key,
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
            )

        # 7. Post-Enqueue Cross-Process Verification:
        # If another process inserted this idempotency key concurrently, enqueue_job() returned
        # that existing row. We MUST verify that the returned job matches our execution parameters.
        self._verify_idempotent_request_match(
            existing_job=raw_job,
            session_id=clean_session_id,
            repository=clean_repo,
            prompt=clean_prompt,
            model=clean_model,
            turn_index=turn_index,
            credential_refs=credential_refs or {},
            expected_payload=expected_payload,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
        )

        return self._dict_to_job_view(raw_job)

    def get_job(self, job_id: str, principal: AuthenticatedPrincipal) -> JobView:
        """Retrieves current job status, verifying tenant ownership against authenticated principal.
        
        Raises:
            AuthenticationVerificationError: If principal verification fails.
            JobAccessDeniedError: If context user does not own the job.
            JobNotFoundError: If the job does not exist.
            JobQueueError: If database error occurs.
        """
        verified = self._verify_principal(principal)
        if not job_id or not job_id.strip():
            raise JobNotFoundError("job_id is required.")

        raw_job = self._queue.get_job(job_id=job_id.strip(), user_id=verified.user_id)
        return self._dict_to_job_view(raw_job)

    def get_job_events(
        self,
        job_id: str,
        principal: AuthenticatedPrincipal,
        after_id: int = 0
    ) -> List[JobEventView]:
        """Retrieves append-only execution events and logs for a job with recursive secret masking.
        
        Args:
            job_id: ID of the job.
            principal: Verified AuthenticatedPrincipal representing caller.
            after_id: Incremental event ID threshold. Only events with id > after_id are returned.
            
        Returns:
            List of deeply immutable JobEventView objects ordered by event ID.
            
        Raises:
            AuthenticationVerificationError: If principal verification fails.
            JobAccessDeniedError: If context user does not own the job.
            JobNotFoundError: If the job does not exist.
        """
        verified = self._verify_principal(principal)
        if not job_id or not job_id.strip():
            raise JobNotFoundError("job_id is required.")

        raw_events = self._queue.get_job_events(
            job_id=job_id.strip(),
            user_id=verified.user_id,
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

    def cancel_job(self, job_id: str, principal: AuthenticatedPrincipal) -> JobView:
        """Requests cancellation for an active job, verifying tenant ownership.
        
        Behavior:
        - If the job is QUEUED, transitions immediately to CANCELLED.
        - If the job is RUNNING, sets cancel_requested_at for cooperative worker termination.
        - If the job is already terminal (COMPLETED, FAILED, CANCELLED, TIMED_OUT), returns current state without error.
        
        Raises:
            AuthenticationVerificationError: If principal verification fails.
            JobAccessDeniedError: If context user does not own the job.
            JobNotFoundError: If the job does not exist.
        """
        verified = self._verify_principal(principal)
        if not job_id or not job_id.strip():
            raise JobNotFoundError("job_id is required.")

        raw_job = self._queue.request_cancellation(job_id=job_id.strip(), user_id=verified.user_id)
        return self._dict_to_job_view(raw_job)

    def list_recent_jobs(self, principal: AuthenticatedPrincipal, limit: int = 50) -> List[JobView]:
        """Lists recent jobs belonging to the authenticated tenant."""
        verified = self._verify_principal(principal)
        raw_jobs = self._queue.list_user_jobs(user_id=verified.user_id, limit=limit)
        return [self._dict_to_job_view(j) for j in raw_jobs]

    def get_active_job_for_session(
        self,
        session_id: str,
        principal: AuthenticatedPrincipal
    ) -> Optional[JobView]:
        """Finds the most recent non-terminal (QUEUED or RUNNING) job for a chat session.
        
        Useful for UI session hydration on page reload or reconnection.
        """
        verified = self._verify_principal(principal)
        if not session_id or not session_id.strip():
            return None

        clean_user = verified.user_id
        clean_session = session_id.strip()

        recent_jobs = self._queue.list_user_jobs(user_id=clean_user, limit=20)
        for j in recent_jobs:
            if j.get("session_id") == clean_session and j.get("status") in ("QUEUED", "RUNNING", "RECOVERABLE"):
                return self._dict_to_job_view(j)
        return None

    def _find_existing_by_idempotency_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        """Queries database for an existing job by idempotency key."""
        try:
            cursor = self._queue.conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key = ?;",
                (idempotency_key,),
            )
            row = cursor.fetchone()
            if row:
                return self._queue._row_to_dict(row)
            return None
        except sqlite3.Error as e:
            raise JobQueueError(f"Database error checking idempotency key: {str(e)}") from e

    def _verify_idempotent_request_match(
        self,
        existing_job: Dict[str, Any],
        session_id: str,
        repository: str,
        prompt: str,
        model: str,
        turn_index: int,
        credential_refs: Dict[str, Any],
        expected_payload: Dict[str, Any],
        max_retries: int,
        timeout_seconds: float,
    ) -> None:
        """Verifies that a duplicate submission with an existing idempotency key matches all original execution parameters.
        
        Raises:
            IdempotencyConflictError: If ANY execution-defining parameter differs from the existing job.
        """
        if existing_job.get("session_id") != session_id:
            raise IdempotencyConflictError(
                f"Idempotency conflict: key already exists with session_id '{existing_job.get('session_id')}', got '{session_id}'."
            )

        if existing_job.get("repository") != repository:
            raise IdempotencyConflictError(
                f"Idempotency conflict: key already exists with repository '{existing_job.get('repository')}', got '{repository}'."
            )

        if existing_job.get("max_retries") != max_retries:
            raise IdempotencyConflictError(
                f"Idempotency conflict: key already exists with max_retries {existing_job.get('max_retries')}, got {max_retries}."
            )

        if existing_job.get("timeout_seconds") != timeout_seconds:
            raise IdempotencyConflictError(
                f"Idempotency conflict: key already exists with timeout_seconds {existing_job.get('timeout_seconds')}, got {timeout_seconds}."
            )

        # Check full canonical equality of payload (detects missing, extra, or modified keys)
        existing_payload = existing_job.get("payload", {})
        if canonical_json(existing_payload) != canonical_json(expected_payload):
            raise IdempotencyConflictError(
                "Idempotency conflict: key already exists with different payload parameters."
            )

        # Check full canonical equality of credential_refs
        existing_creds = existing_job.get("credential_refs", {})
        if canonical_json(existing_creds) != canonical_json(credential_refs):
            raise IdempotencyConflictError(
                "Idempotency conflict: key already exists with different credential references."
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

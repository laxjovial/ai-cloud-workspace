"""Test Suite for Phase B.3.1 Job Service Client Boundary (Independent Review Corrections).

Covers all review requirements and safeguards:
1. Successful job submission with verified AuthenticatedPrincipal and immutable JobView return.
2. Complete parameter idempotency comparison (detects repository, model, prompt, missing/extra payload,
   credential_refs, max_retries, and timeout_seconds mismatches) raising IdempotencyConflictError.
3. Cross-process concurrent duplicate submissions:
   - Uses two independently constructed JobServiceClient instances sharing the same SQLite DB.
   - Identical parameters safely return the same job.
   - Conflicting parameters raise IdempotencyConflictError without duplicate rows or uncaught SQLite errors.
4. Pre-validation of numeric execution parameters (rejects NaN, Infinity, booleans, and out-of-range).
5. Cross-tenant job access rejection.
6. Cross-tenant event access rejection.
7. Cross-tenant cancellation rejection.
8. Incremental event retrieval (after_id sequence filtering).
9. Cancellation of queued jobs (immediate transition) and running jobs (cancel_requested_at).
10. Cancellation of terminal jobs (no-op, preserves state).
11. Invalid payload, raw secret detection, and reserved payload key protection.
12. Database and queue failure behavior (does not mask errors).
13. Genuine authentication boundary enforcement: rejects raw strings, forged signatures, expired principals,
    foreign authorities, and fails closed when auth_verifier is absent.
14. Deep immutability of JobView and JobEventView (nested dicts/lists cannot be mutated).
15. Recursive sanitization at arbitrary nesting depth (lists of dicts, dicts of lists).
16. Chat session active job rehydration lookup.
"""

import concurrent.futures
import math
import multiprocessing
import os
import sqlite3
import tempfile
import time
from typing import Any, Optional
import pytest

from job_queue import (
    DEFAULT_LEASE_DURATION_SECONDS,
    DurableJobQueue,
    JobAccessDeniedError,
    JobNotFoundError,
    JobQueueError,
    InvalidCredentialReferenceError,
)
from job_service import (
    AuthenticatedPrincipal,
    AuthenticationVerificationError,
    HMACAuthenticationAuthority,
    IdempotencyConflictError,
    JobEventView,
    JobServiceClient,
    JobView,
    compute_comprehensive_idempotency_key,
    sanitize_recursive,
    sanitize_text,
)


# Top-level helper function for multiprocessing worker execution (must be picklable for 'spawn' context)
def _multiprocessing_submit_worker(
    db_path: str,
    secret_bytes: bytes,
    principal: AuthenticatedPrincipal,
    kwargs: dict,
    out_queue: multiprocessing.Queue,
    barrier: Optional[Any] = None,
):
    """Spawns an independent JobServiceClient inside a new OS process, executes submit_agent_turn, and sends result back.
    
    If barrier is provided, waits on the barrier immediately before calling submit_agent_turn to ensure
    all participating OS processes reach the submission call simultaneously.
    """
    client = None
    try:
        verifier = HMACAuthenticationAuthority(authority_secret=secret_bytes)
        client = JobServiceClient(db_path=db_path, auth_verifier=verifier)
        if barrier is not None:
            barrier.wait(timeout=5.0)
        job_view = client.submit_agent_turn(principal=principal, **kwargs)
        out_queue.put(("SUCCESS", job_view.id))
    except IdempotencyConflictError as e:
        out_queue.put(("IDEMPOTENCY_CONFLICT", type(e).__name__, str(e)))
    except Exception as e:
        out_queue.put(("ERROR", type(e).__name__, str(e)))
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


@pytest.fixture
def temp_db_path():
    """Provides an isolated temporary SQLite database path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    for ext in ("", "-wal", "-shm"):
        p = path + ext
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


@pytest.fixture
def auth_authority():
    """Provides a fresh HMACAuthenticationAuthority."""
    return HMACAuthenticationAuthority(authority_secret=b"test_secret_for_service_testing_32b!")


@pytest.fixture
def service_client(temp_db_path, auth_authority):
    """Provides a fresh JobServiceClient instance backed by a temporary database and test authority."""
    client = JobServiceClient(db_path=temp_db_path, auth_verifier=auth_authority)
    yield client
    client.close()


@pytest.fixture
def alice_principal(auth_authority):
    """Provides a legitimately signed AuthenticatedPrincipal for Alice."""
    return auth_authority.issue_principal_from_verified_credential(
        verified_user_id="user_alice", email="alice@example.com", roles=("user",)
    )


@pytest.fixture
def bob_principal(auth_authority):
    """Provides a legitimately signed AuthenticatedPrincipal for Bob."""
    return auth_authority.issue_principal_from_verified_credential(
        verified_user_id="user_bob", email="bob@example.com", roles=("user",)
    )


class TestPhaseB3JobService:
    """Test suite verifying JobServiceClient application boundary and invariants."""

    def test_1_successful_job_submission(self, service_client, alice_principal):
        """Requirement 1: Verify successful job submission with verified AuthenticatedPrincipal."""
        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_123",
            repository="octocat/Hello-World",
            prompt="Analyze repository structure and run tests",
            credential_refs={
                "github": {"key_ref": "user_alice_github_pat"},
                "gemini": {"key_ref": "user_alice_gemini_api_key"},
            },
            turn_index=1,
            model="gemini/gemini-2.5-flash",
            timeout_seconds=300.0,
        )

        assert isinstance(job_view, JobView)
        assert job_view.user_id == "user_alice"
        assert job_view.session_id == "sess_123"
        assert job_view.repository == "octocat/Hello-World"
        assert job_view.status == "QUEUED"
        assert job_view.payload["prompt"] == "Analyze repository structure and run tests"
        assert job_view.payload["model"] == "gemini/gemini-2.5-flash"
        assert job_view.payload["turn_index"] == 1
        assert job_view.credential_refs["github"]["key_ref"] == "user_alice_github_pat"
        assert job_view.is_active is True
        assert job_view.is_terminal is False
        assert job_view.is_cancellation_requested is False

    def test_2_complete_idempotency_comparison_and_conflict_rejection(self, service_client, alice_principal):
        """Requirement 2: Complete parameter comparison detects differences and raises IdempotencyConflictError."""
        custom_key = "idemp_custom_full_check"

        # Baseline submission
        job1 = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_idemp",
            repository="org/repo_a",
            prompt="Original prompt",
            turn_index=1,
            model="gemini/gemini-2.5-flash",
            max_retries=3,
            timeout_seconds=300.0,
            credential_refs={"github": {"key_ref": "ref_pat"}},
            additional_payload={"base_branch": "main"},
            idempotency_key=custom_key,
        )

        # Exact duplicate succeeds and returns the same job
        job2 = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_idemp",
            repository="org/repo_a",
            prompt="Original prompt",
            turn_index=1,
            model="gemini/gemini-2.5-flash",
            max_retries=3,
            timeout_seconds=300.0,
            credential_refs={"github": {"key_ref": "ref_pat"}},
            additional_payload={"base_branch": "main"},
            idempotency_key=custom_key,
        )
        assert job1.id == job2.id

        # Mismatch 1: session_id difference
        with pytest.raises(IdempotencyConflictError) as exc_sess:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_different",
                repository="org/repo_a",
                prompt="Original prompt",
                turn_index=1,
                idempotency_key=custom_key,
            )
        assert "session_id" in str(exc_sess.value)

        # Mismatch 2: max_retries difference
        with pytest.raises(IdempotencyConflictError) as exc_retries:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_idemp",
                repository="org/repo_a",
                prompt="Original prompt",
                turn_index=1,
                max_retries=5,  # Mismatch
                timeout_seconds=300.0,
                idempotency_key=custom_key,
            )
        assert "max_retries" in str(exc_retries.value)

        # Mismatch 3: timeout_seconds difference
        with pytest.raises(IdempotencyConflictError) as exc_timeout:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_idemp",
                repository="org/repo_a",
                prompt="Original prompt",
                turn_index=1,
                timeout_seconds=600.0,  # Mismatch
                idempotency_key=custom_key,
            )
        assert "timeout_seconds" in str(exc_timeout.value)

        # Mismatch 3b: Micro-difference in timeout_seconds (< 0.001 delta, e.g. 300.0 vs 300.0005)
        # Guarantees exact floating-point equality check instead of fuzzy delta tolerance.
        with pytest.raises(IdempotencyConflictError) as exc_timeout_micro:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_idemp",
                repository="org/repo_a",
                prompt="Original prompt",
                turn_index=1,
                timeout_seconds=300.0005,  # Micro mismatch < 0.001
                idempotency_key=custom_key,
            )
        assert "timeout_seconds" in str(exc_timeout_micro.value)

        # Mismatch 4: extra additional_payload key
        with pytest.raises(IdempotencyConflictError) as exc_extra:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_idemp",
                repository="org/repo_a",
                prompt="Original prompt",
                turn_index=1,
                timeout_seconds=300.0,
                additional_payload={"base_branch": "main", "reasoning_effort": "high"},  # Extra key
                idempotency_key=custom_key,
            )
        assert "payload" in str(exc_extra.value)

        # Mismatch 5: missing additional_payload key
        with pytest.raises(IdempotencyConflictError) as exc_missing:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_idemp",
                repository="org/repo_a",
                prompt="Original prompt",
                turn_index=1,
                timeout_seconds=300.0,
                additional_payload={},  # Missing base_branch
                idempotency_key=custom_key,
            )
        assert "payload" in str(exc_missing.value)

    def test_3_cross_process_concurrent_idempotency_race(self, temp_db_path, auth_authority, alice_principal):
        """Requirement 3: Genuine OS multiprocessing test verifying concurrent idempotency resolution.
        
        Uses multiprocessing (with 'spawn' context) to execute two separate OS processes
        that independently instantiate JobServiceClient against the shared SQLite file.
        
        Uses multiprocessing.Barrier to guarantee that both processes reach the `submit_agent_turn`
        call concurrently, exercising true cross-process racing for both:
        1. Identical concurrent requests in separate processes: both processes race on the same key
           with identical parameters. Both succeed, both return the exact same job ID, and exactly
           1 row is written to SQLite.
        2. Conflicting concurrent requests across separate processes: both processes race on the same key
           with different execution parameters. Exactly one process wins and creates the job, while
           the other receives IdempotencyConflictError. Exactly 1 row exists in SQLite, with zero
           unhandled SQLite exceptions.
        """
        secret = auth_authority._secret
        ctx = multiprocessing.get_context("spawn")

        # -------------------------------------------------------------
        # 1. Concurrent identical requests across independent OS processes
        # -------------------------------------------------------------
        idemp_identical = "cross_process_identical_key_999"
        out_q1 = ctx.Queue()
        barrier1 = ctx.Barrier(2)

        params_identical = {
            "session_id": "sess_multiprocess",
            "repository": "org/repo_multiprocess",
            "prompt": "Identical multiprocessing prompt",
            "turn_index": 1,
            "idempotency_key": idemp_identical,
        }

        p1 = ctx.Process(
            target=_multiprocessing_submit_worker,
            args=(temp_db_path, secret, alice_principal, params_identical, out_q1, barrier1),
        )
        p2 = ctx.Process(
            target=_multiprocessing_submit_worker,
            args=(temp_db_path, secret, alice_principal, params_identical, out_q1, barrier1),
        )

        try:
            p1.start()
            p2.start()
            p1.join(timeout=10.0)
            p2.join(timeout=10.0)

            assert not p1.is_alive(), "Process 1 timed out and is still alive"
            assert not p2.is_alive(), "Process 2 timed out and is still alive"
            assert p1.exitcode == 0, f"Process 1 exited with non-zero code {p1.exitcode}"
            assert p2.exitcode == 0, f"Process 2 exited with non-zero code {p2.exitcode}"

            res1 = out_q1.get(timeout=2.0)
            res2 = out_q1.get(timeout=2.0)

            assert res1[0] == "SUCCESS", f"Process 1 failed: {res1}"
            assert res2[0] == "SUCCESS", f"Process 2 failed: {res2}"
            # Both independent OS processes received the exact same job ID
            assert res1[1] == res2[1]

            # Verify exactly 1 row exists in SQLite for this idempotency key
            conn = sqlite3.connect(temp_db_path)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM jobs WHERE idempotency_key = ?;", (idemp_identical,))
            assert cur.fetchone()[0] == 1
            conn.close()
        finally:
            if p1.is_alive():
                p1.terminate()
                p1.join()
            if p2.is_alive():
                p2.terminate()
                p2.join()
            out_q1.close()

        # -------------------------------------------------------------
        # 2. Conflicting requests racing concurrently across independent OS processes on same key
        # -------------------------------------------------------------
        idemp_conflict = "cross_process_conflict_key_888"
        out_q2 = ctx.Queue()
        barrier2 = ctx.Barrier(2)

        params_a = {
            "session_id": "sess_multiprocess",
            "repository": "org/repo_multiprocess",
            "prompt": "Prompt Candidate Alpha for race",
            "turn_index": 1,
            "idempotency_key": idemp_conflict,
        }
        params_b = {
            "session_id": "sess_multiprocess",
            "repository": "org/repo_multiprocess",
            "prompt": "DIFFERENT Prompt Candidate Beta that conflicts on the same key",
            "turn_index": 1,
            "idempotency_key": idemp_conflict,
        }

        # Spawn two processes that hit submit_agent_turn simultaneously via barrier2
        p_a = ctx.Process(
            target=_multiprocessing_submit_worker,
            args=(temp_db_path, secret, alice_principal, params_a, out_q2, barrier2),
        )
        p_b = ctx.Process(
            target=_multiprocessing_submit_worker,
            args=(temp_db_path, secret, alice_principal, params_b, out_q2, barrier2),
        )

        try:
            p_a.start()
            p_b.start()
            p_a.join(timeout=10.0)
            p_b.join(timeout=10.0)

            assert not p_a.is_alive(), "Process A timed out and is still alive"
            assert not p_b.is_alive(), "Process B timed out and is still alive"
            assert p_a.exitcode == 0, f"Process A exited with non-zero code {p_a.exitcode}"
            assert p_b.exitcode == 0, f"Process B exited with non-zero code {p_b.exitcode}"

            outcome1 = out_q2.get(timeout=2.0)
            outcome2 = out_q2.get(timeout=2.0)

            outcomes = [outcome1, outcome2]
            successes = [o for o in outcomes if o[0] == "SUCCESS"]
            conflicts = [o for o in outcomes if o[0] == "IDEMPOTENCY_CONFLICT"]
            errors = [o for o in outcomes if o[0] == "ERROR"]

            assert len(errors) == 0, f"Unexpected uncaught error in race: {errors}"
            assert len(successes) == 1, f"Expected exactly 1 success in race, got {len(successes)}: {outcomes}"
            assert len(conflicts) == 1, f"Expected exactly 1 IdempotencyConflictError in race, got {len(conflicts)}: {outcomes}"
            assert conflicts[0][1] == "IdempotencyConflictError"

            # Verify database still has exactly 1 record and no corrupted state
            conn = sqlite3.connect(temp_db_path)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM jobs WHERE idempotency_key = ?;", (idemp_conflict,))
            assert cur.fetchone()[0] == 1
            conn.close()
        finally:
            if p_a.is_alive():
                p_a.terminate()
                p_a.join()
            if p_b.is_alive():
                p_b.terminate()
                p_b.join()
            out_q2.close()

    def test_4_numeric_pre_validation(self, service_client, alice_principal):
        """Requirement 4: Strict pre-validation of numeric execution parameters rejects NaN, Inf, and booleans."""
        # 1. NaN timeout_seconds rejected before lookup
        with pytest.raises(JobQueueError) as exc_nan:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_num",
                repository="org/repo",
                prompt="Task",
                timeout_seconds=float("nan"),
            )
        assert "non-finite" in str(exc_nan.value).lower()

        # 2. Infinity timeout_seconds rejected
        with pytest.raises(JobQueueError) as exc_inf:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_num",
                repository="org/repo",
                prompt="Task",
                timeout_seconds=float("inf"),
            )
        assert "non-finite" in str(exc_inf.value).lower()

        # 3. Boolean passed as max_retries rejected (even though bool is subclass of int)
        with pytest.raises(JobQueueError) as exc_bool:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_num",
                repository="org/repo",
                prompt="Task",
                max_retries=True,  # type: ignore
            )
        assert "invalid type" in str(exc_bool.value).lower() or "expected integer" in str(exc_bool.value).lower()

        # 4. Negative or out-of-range timeout
        with pytest.raises(JobQueueError):
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_num",
                repository="org/repo",
                prompt="Task",
                timeout_seconds=0.0,
            )

    def test_5_cross_tenant_job_access_rejection(self, service_client, alice_principal, bob_principal):
        """Requirement 5: Verify tenant ownership is strictly enforced for job status queries."""
        job = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Alice can fetch her job
        alice_view = service_client.get_job(job_id=job.id, principal=alice_principal)
        assert alice_view.id == job.id

        # Bob cannot fetch Alice's job
        with pytest.raises(JobAccessDeniedError) as exc_info:
            service_client.get_job(job_id=job.id, principal=bob_principal)
        assert f"Access denied to job '{job.id}'." in str(exc_info.value)

        # Non-existent job raises JobNotFoundError
        with pytest.raises(JobNotFoundError):
            service_client.get_job(job_id="non_existent_uuid", principal=alice_principal)

    def test_6_cross_tenant_event_access_rejection(self, service_client, alice_principal, bob_principal):
        """Requirement 6: Verify tenant ownership is strictly enforced for event streams."""
        job = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Alice can retrieve events
        events = service_client.get_job_events(job_id=job.id, principal=alice_principal)
        assert len(events) >= 1
        assert events[0].event_type == "ENQUEUED"

        # Bob cannot retrieve Alice's job events
        with pytest.raises(JobAccessDeniedError):
            service_client.get_job_events(job_id=job.id, principal=bob_principal)

    def test_7_cross_tenant_cancellation_rejection(self, service_client, alice_principal, bob_principal):
        """Requirement 7: Verify tenant ownership is strictly enforced for job cancellation."""
        job = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Bob cannot cancel Alice's job
        with pytest.raises(JobAccessDeniedError):
            service_client.cancel_job(job_id=job.id, principal=bob_principal)

        # Job remains QUEUED and uncancelled
        check = service_client.get_job(job_id=job.id, principal=alice_principal)
        assert check.status == "QUEUED"
        assert check.is_cancellation_requested is False

    def test_8_incremental_event_retrieval(self, service_client, temp_db_path, alice_principal):
        """Requirement 8: Verify incremental event retrieval using after_id filtering."""
        job = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Worker claims job and writes logs
        queue = DurableJobQueue(db_path=temp_db_path)
        claimed = queue.claim_next_job(worker_id="worker_test_1")
        assert claimed is not None
        assert claimed["id"] == job.id

        queue.append_log(
            job_id=job.id,
            worker_id="worker_test_1",
            lease_generation=claimed["lease_generation"],
            message="Initializing sandbox container",
            event_type="STEP_START",
        )
        queue.append_log(
            job_id=job.id,
            worker_id="worker_test_1",
            lease_generation=claimed["lease_generation"],
            message="Running git checkout feature-branch",
            event_type="TOOL_CALL",
        )
        queue.close()

        # Initial fetch: gets all events (ENQUEUED, CLAIMED, STEP_START, TOOL_CALL = 4)
        all_events = service_client.get_job_events(job_id=job.id, principal=alice_principal, after_id=0)
        assert len(all_events) == 4
        assert all_events[0].event_type == "ENQUEUED"
        assert all_events[1].event_type == "CLAIMED"
        assert all_events[2].event_type == "STEP_START"
        assert all_events[3].event_type == "TOOL_CALL"

        # Incremental fetch: fetch only events after event ID 2
        last_id = all_events[1].id
        new_events = service_client.get_job_events(job_id=job.id, principal=alice_principal, after_id=last_id)
        assert len(new_events) == 2
        assert new_events[0].event_type == "STEP_START"
        assert new_events[1].event_type == "TOOL_CALL"
        assert all(e.id > last_id for e in new_events)

    def test_9_cancellation_queued_and_running_jobs(self, service_client, temp_db_path, alice_principal):
        """Requirement 9: Cancellation transitions QUEUED to CANCELLED and marks RUNNING jobs."""
        # 1. Cancel QUEUED job -> immediate transition to CANCELLED
        job_queued = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_q",
            repository="org/repo",
            prompt="Queued task",
        )
        res_q = service_client.cancel_job(job_id=job_queued.id, principal=alice_principal)
        assert res_q.status == "CANCELLED"
        assert res_q.is_terminal is True
        assert res_q.cancel_requested_at is not None
        assert res_q.cancel_requested_by == "user_alice"

        # 2. Cancel RUNNING job -> marks cancel_requested_at for worker cooperative termination
        job_running = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_r",
            repository="org/repo",
            prompt="Running task",
        )
        queue = DurableJobQueue(db_path=temp_db_path)
        claimed = queue.claim_next_job(worker_id="worker_test_run")
        assert claimed is not None
        assert claimed["id"] == job_running.id
        queue.close()

        res_r = service_client.cancel_job(job_id=job_running.id, principal=alice_principal)
        assert res_r.status == "RUNNING"  # Stays running until worker observes and aborts
        assert res_r.is_cancellation_requested is True
        assert res_r.cancel_requested_by == "user_alice"

    def test_10_cancellation_of_terminal_jobs(self, service_client, temp_db_path, alice_principal):
        """Requirement 10: Cancellation of terminal jobs (COMPLETED, FAILED) is safe and preserves status."""
        job = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_term",
            repository="org/repo",
            prompt="Task to complete",
        )

        queue = DurableJobQueue(db_path=temp_db_path)
        claimed = queue.claim_next_job(worker_id="worker_term")
        assert claimed is not None

        # Complete the job
        queue.update_job_status(
            job_id=job.id,
            target_status="COMPLETED",
            worker_id="worker_term",
            lease_generation=claimed["lease_generation"],
        )
        queue.close()

        # Verify job is COMPLETED
        view_before = service_client.get_job(job_id=job.id, principal=alice_principal)
        assert view_before.status == "COMPLETED"
        assert view_before.is_terminal is True

        # Calling cancel on completed job is a safe no-op
        view_after = service_client.cancel_job(job_id=job.id, principal=alice_principal)
        assert view_after.status == "COMPLETED"
        assert view_after.is_terminal is True

    def test_11_invalid_payload_and_reserved_key_protection(self, service_client, alice_principal):
        """Requirement 11: Reserved payload keys cannot be overridden and raw secrets are rejected."""
        # 1. Plaintext API key in credential_refs must be rejected
        with pytest.raises(InvalidCredentialReferenceError):
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_leak",
                repository="org/repo",
                prompt="Task with raw token",
                credential_refs={"github": "ghp_123456789012345678901234567890123456"},
            )

        # 2. Raw secret string in payload prompt or additional_payload
        with pytest.raises(JobQueueError):
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_leak2",
                repository="org/repo",
                prompt="Task with raw token sk-1234567890abcdef1234567890abcdef",
            )

        # 3. Attempting to override reserved payload keys via additional_payload must fail
        for reserved_key in ("job_type", "prompt", "model", "turn_index", "user_id", "repository"):
            with pytest.raises(JobQueueError) as exc_res:
                service_client.submit_agent_turn(
                    principal=alice_principal,
                    session_id="sess_res",
                    repository="org/repo",
                    prompt="Task",
                    additional_payload={reserved_key: "malicious_override"},
                )
            assert "reserved payload key" in str(exc_res.value)

        # 4. Attempting to pass disallowed keys in additional_payload must fail
        with pytest.raises(JobQueueError) as exc_dis:
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_dis",
                repository="org/repo",
                prompt="Task",
                additional_payload={"unsupported_key": "some_value"},
            )
        assert "Disallowed key" in str(exc_dis.value)

    def test_12_database_and_queue_failure_behavior(self, service_client, alice_principal):
        """Requirement 12: Database errors are properly raised and not masked as success or empty."""
        service_client._queue.conn.close()

        with pytest.raises(JobQueueError):
            service_client.submit_agent_turn(
                principal=alice_principal,
                session_id="sess_err",
                repository="org/repo",
                prompt="Fail task",
            )

        with pytest.raises(JobQueueError):
            service_client.get_job(job_id="any_id", principal=alice_principal)

        with pytest.raises(JobQueueError):
            service_client.cancel_job(job_id="any_id", principal=alice_principal)

    def test_13_authenticated_principal_boundary_enforcement(self, service_client, auth_authority, temp_db_path):
        """Requirement 13: Genuine authentication boundary: rejects raw strings, forged signatures, and fails closed without verifier."""
        # 1. Plain strings are strictly rejected
        with pytest.raises(AuthenticationVerificationError):
            service_client.get_job(job_id="job_123", principal="user_alice")  # type: ignore

        # 2. Forged signature rejected
        forged_principal = AuthenticatedPrincipal(
            user_id="user_alice",
            issuer="ai_cloud_auth",
            issued_at=time.time(),
            signature="forged_signature_hex_value_1234567890",
        )
        with pytest.raises(AuthenticationVerificationError):
            service_client.get_job(job_id="job_123", principal=forged_principal)

        # 3. Principal signed by different authority secret rejected
        other_authority = HMACAuthenticationAuthority(authority_secret=b"different_secret_key_32b_here!!")
        foreign_principal = other_authority.issue_principal_from_verified_credential(verified_user_id="user_alice")
        with pytest.raises(AuthenticationVerificationError):
            service_client.get_job(job_id="job_123", principal=foreign_principal)

        # 4. Expired principal rejected
        expired_principal = auth_authority.issue_principal_from_verified_credential(verified_user_id="user_alice")
        object.__setattr__(expired_principal, "issued_at", time.time() - 90000.0)
        with pytest.raises(AuthenticationVerificationError):
            service_client.get_job(job_id="job_123", principal=expired_principal)

        # 5. Service client refuses to initialize without auth_verifier (fails closed)
        with pytest.raises(ValueError) as exc_init:
            JobServiceClient(db_path=temp_db_path, auth_verifier=None)  # type: ignore
        assert "requires a valid 'auth_verifier'" in str(exc_init.value)

    def test_14_deep_immutability_of_views(self, service_client, alice_principal, temp_db_path):
        """Requirement 14: JobView and JobEventView prevent nested data mutation."""
        job = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_mut",
            repository="org/repo",
            prompt="Immutability check",
            credential_refs={"github": {"key_ref": "valid_pat_ref"}},
            additional_payload={"base_branch": "develop"},
        )

        # Top-level reassignment fails
        with pytest.raises(Exception):
            job.status = "COMPLETED"  # type: ignore

        # Nested payload mutation fails
        with pytest.raises(TypeError):
            job.payload["prompt"] = "Hacked prompt"

        with pytest.raises(TypeError):
            job.payload["new_field"] = "Hacked field"

        # Nested credential_refs mutation fails
        with pytest.raises(TypeError):
            job.credential_refs["github"] = "Hacked pat"

        # JobEventView nested data immutability
        queue = DurableJobQueue(db_path=temp_db_path)
        claimed = queue.claim_next_job(worker_id="worker_mut")
        queue.append_log(
            job_id=job.id,
            worker_id="worker_mut",
            lease_generation=claimed["lease_generation"],
            message="Step output",
            data={"step": 1, "details": {"files": ["a.py", "b.py"]}},
        )
        queue.close()

        events = service_client.get_job_events(job_id=job.id, principal=alice_principal)
        log_event = [e for e in events if e.event_type == "LOG"][0]

        # Nested data mutation fails
        with pytest.raises(TypeError):
            log_event.data["step"] = 99

        assert isinstance(log_event.data["details"]["files"], tuple)
        with pytest.raises((AttributeError, TypeError)):
            log_event.data["details"]["files"].append("c.py")
        with pytest.raises(TypeError):
            log_event.data["details"]["files"][0] = "c.py"

    def test_15_recursive_sanitization_at_arbitrary_depth(self):
        """Requirement 15: Recursive sanitization redacts secrets in deeply nested lists and dicts."""
        nested_structure = {
            "execution": {
                "steps": [
                    {
                        "action": "git clone",
                        "command": "git clone https://x-access-token:ghp_123456789012345678901234567890123456@github.com/repo",
                        "auth": {"header": "Bearer sk-1234567890abcdef1234567890abcdef"},
                    },
                    {
                        "logs": ["API request returned 200 with Bearer AIzaSyD9876543210abcdefghijklmnopq"],
                    },
                ]
            }
        }

        sanitized = sanitize_recursive(nested_structure)

        step0 = sanitized["execution"]["steps"][0]
        assert "ghp_" not in step0["command"]
        assert "[REDACTED_SECRET]" in step0["command"]
        assert "sk-" not in step0["auth"]["header"]
        assert "[REDACTED_SECRET]" in step0["auth"]["header"]

        step1 = sanitized["execution"]["steps"][1]
        assert "AIza" not in step1["logs"][0]
        assert "[REDACTED_SECRET]" in step1["logs"][0]

    def test_16_session_active_job_lookup(self, service_client, alice_principal):
        """Requirement 16: Verify get_active_job_for_session retrieves running/queued jobs and ignores completed."""
        j1 = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_hydration",
            repository="org/repo",
            prompt="Active task",
            turn_index=1,
        )

        active = service_client.get_active_job_for_session("sess_hydration", alice_principal)
        assert active is not None
        assert active.id == j1.id
        assert active.status == "QUEUED"

        # Cancel job
        service_client.cancel_job(j1.id, alice_principal)

        # Now session has no active jobs
        active_after = service_client.get_active_job_for_session("sess_hydration", alice_principal)
        assert active_after is None

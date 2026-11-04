"""Test Suite for Phase B.3.1 Job Service Client Boundary.

Covers all 10 mandatory testing requirements:
1. Successful job submission and JobView immutability.
2. Duplicate submission and idempotency preservation.
3. Cross-tenant job access rejection.
4. Cross-tenant event access rejection.
5. Cross-tenant cancellation rejection.
6. Incremental event retrieval (after_id sequence filtering).
7. Cancellation of queued jobs (immediate transition) and running jobs (cancel_requested_at).
8. Cancellation of terminal jobs (no-op, preserves state).
9. Invalid payload and raw credential rejection.
10. Database and queue failure behavior (does not mask errors).
"""

import os
import sqlite3
import tempfile
import time
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
    JobEventView,
    JobServiceClient,
    JobView,
    compute_agent_idempotency_key,
    sanitize_text,
)


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
def service_client(temp_db_path):
    """Provides a fresh JobServiceClient instance backed by a temporary database."""
    client = JobServiceClient(db_path=temp_db_path)
    yield client
    client.close()


class TestPhaseB3JobService:
    """Test suite verifying JobServiceClient application boundary and invariants."""

    def test_1_successful_job_submission(self, service_client):
        """Requirement 1: Verify successful job submission and immutable JobView return."""
        job_view = service_client.submit_agent_turn(
            user_id="user_alice",
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

        # Verify JobView is frozen/immutable
        with pytest.raises(Exception):
            job_view.status = "RUNNING"  # type: ignore

    def test_2_duplicate_submission_and_idempotency(self, service_client):
        """Requirement 2: Verify duplicate submissions return existing job record without re-enqueuing."""
        # First submission with auto-computed idempotency key
        job1 = service_client.submit_agent_turn(
            user_id="user_alice",
            session_id="sess_abc",
            repository="org/repo",
            prompt="Implement bugfix",
            turn_index=2,
        )

        # Second submission with identical parameters (same session, user, turn, prompt)
        job2 = service_client.submit_agent_turn(
            user_id="user_alice",
            session_id="sess_abc",
            repository="org/repo",
            prompt="Implement bugfix",
            turn_index=2,
        )

        assert job1.id == job2.id
        assert job1.idempotency_key == job2.idempotency_key
        assert job1.created_at == job2.created_at

        # Verify deterministic key calculation helper
        expected_key = compute_agent_idempotency_key("sess_abc", "user_alice", 2, "Implement bugfix")
        assert job1.idempotency_key == expected_key

        # Submitting with different prompt produces different job
        job3 = service_client.submit_agent_turn(
            user_id="user_alice",
            session_id="sess_abc",
            repository="org/repo",
            prompt="Another prompt",
            turn_index=3,
        )
        assert job3.id != job1.id

    def test_3_cross_tenant_job_access_rejection(self, service_client):
        """Requirement 3: Verify tenant ownership is strictly enforced for job status queries."""
        job = service_client.submit_agent_turn(
            user_id="user_alice",
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Alice can fetch her job
        alice_view = service_client.get_job(job_id=job.id, user_id="user_alice")
        assert alice_view.id == job.id

        # Bob cannot fetch Alice's job
        with pytest.raises(JobAccessDeniedError) as exc_info:
            service_client.get_job(job_id=job.id, user_id="user_bob")
        assert f"Access denied to job '{job.id}'." in str(exc_info.value)

        # Empty/whitespace user_id rejected
        with pytest.raises(JobAccessDeniedError):
            service_client.get_job(job_id=job.id, user_id="")

        # Non-existent job raises JobNotFoundError
        with pytest.raises(JobNotFoundError):
            service_client.get_job(job_id="non_existent_uuid", user_id="user_alice")

    def test_4_cross_tenant_event_access_rejection(self, service_client):
        """Requirement 4: Verify tenant ownership is strictly enforced for event streams."""
        job = service_client.submit_agent_turn(
            user_id="user_alice",
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Alice can retrieve events
        events = service_client.get_job_events(job_id=job.id, user_id="user_alice")
        assert len(events) >= 1
        assert events[0].event_type == "ENQUEUED"

        # Bob cannot retrieve Alice's job events
        with pytest.raises(JobAccessDeniedError):
            service_client.get_job_events(job_id=job.id, user_id="user_bob")

    def test_5_cross_tenant_cancellation_rejection(self, service_client):
        """Requirement 5: Verify tenant ownership is strictly enforced for job cancellation."""
        job = service_client.submit_agent_turn(
            user_id="user_alice",
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Bob cannot cancel Alice's job
        with pytest.raises(JobAccessDeniedError):
            service_client.cancel_job(job_id=job.id, user_id="user_bob")

        # Job remains QUEUED and uncancelled
        check = service_client.get_job(job_id=job.id, user_id="user_alice")
        assert check.status == "QUEUED"
        assert check.is_cancellation_requested is False

    def test_6_incremental_event_retrieval(self, service_client, temp_db_path):
        """Requirement 6: Verify incremental event retrieval using after_id filtering."""
        job = service_client.submit_agent_turn(
            user_id="user_alice",
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

        # Initial fetch: gets all events (ENQUEUED, CLAIMED, and 2 log events = 4 events)
        all_events = service_client.get_job_events(job_id=job.id, user_id="user_alice", after_id=0)
        assert len(all_events) == 4  # ENQUEUED, CLAIMED, STEP_START, TOOL_CALL
        assert all_events[0].event_type == "ENQUEUED"
        assert all_events[1].event_type == "CLAIMED"
        assert all_events[2].event_type == "STEP_START"
        assert all_events[3].event_type == "TOOL_CALL"

        # Incremental fetch: fetch only events after event ID 2
        last_id = all_events[1].id
        new_events = service_client.get_job_events(job_id=job.id, user_id="user_alice", after_id=last_id)
        assert len(new_events) == 2
        assert new_events[0].event_type == "STEP_START"
        assert new_events[1].event_type == "TOOL_CALL"
        assert all(e.id > last_id for e in new_events)

    def test_7_cancellation_queued_and_running_jobs(self, service_client, temp_db_path):
        """Requirement 7: Cancellation transitions QUEUED to CANCELLED and marks RUNNING jobs."""
        # 1. Cancel QUEUED job -> immediate transition to CANCELLED
        job_queued = service_client.submit_agent_turn(
            user_id="user_alice",
            session_id="sess_q",
            repository="org/repo",
            prompt="Queued task",
        )
        res_q = service_client.cancel_job(job_id=job_queued.id, user_id="user_alice")
        assert res_q.status == "CANCELLED"
        assert res_q.is_terminal is True
        assert res_q.cancel_requested_at is not None
        assert res_q.cancel_requested_by == "user_alice"

        # 2. Cancel RUNNING job -> marks cancel_requested_at for worker cooperative termination
        job_running = service_client.submit_agent_turn(
            user_id="user_alice",
            session_id="sess_r",
            repository="org/repo",
            prompt="Running task",
        )
        queue = DurableJobQueue(db_path=temp_db_path)
        claimed = queue.claim_next_job(worker_id="worker_test_run")
        assert claimed is not None
        assert claimed["id"] == job_running.id
        queue.close()

        res_r = service_client.cancel_job(job_id=job_running.id, user_id="user_alice")
        assert res_r.status == "RUNNING"  # Stays running until worker observes and aborts
        assert res_r.is_cancellation_requested is True
        assert res_r.cancel_requested_by == "user_alice"

    def test_8_cancellation_of_terminal_jobs(self, service_client, temp_db_path):
        """Requirement 8: Cancellation of terminal jobs (COMPLETED, FAILED) is safe and preserves status."""
        job = service_client.submit_agent_turn(
            user_id="user_alice",
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
        view_before = service_client.get_job(job_id=job.id, user_id="user_alice")
        assert view_before.status == "COMPLETED"
        assert view_before.is_terminal is True

        # Calling cancel on completed job is a safe no-op
        view_after = service_client.cancel_job(job_id=job.id, user_id="user_alice")
        assert view_after.status == "COMPLETED"
        assert view_after.is_terminal is True

    def test_9_invalid_payload_and_raw_credentials_handling(self, service_client):
        """Requirement 9: Raw secrets or invalid credential formats are rejected fail-closed."""
        # 1. Plaintext API key in credential_refs must be rejected
        with pytest.raises(InvalidCredentialReferenceError):
            service_client.submit_agent_turn(
                user_id="user_alice",
                session_id="sess_leak",
                repository="org/repo",
                prompt="Task with raw token",
                credential_refs={"github": "ghp_123456789012345678901234567890123456"},
            )

        # 2. Raw secret string in payload prompt or additional_payload
        with pytest.raises(JobQueueError):
            service_client.submit_agent_turn(
                user_id="user_alice",
                session_id="sess_leak2",
                repository="org/repo",
                prompt="Task with raw token",
                additional_payload={"api_key": "sk-1234567890abcdef1234567890abcdef"},
            )

        # 3. Missing user_id or repository
        with pytest.raises(JobAccessDeniedError):
            service_client.submit_agent_turn(
                user_id="",
                session_id="sess",
                repository="org/repo",
                prompt="Prompt",
            )
        with pytest.raises(JobQueueError):
            service_client.submit_agent_turn(
                user_id="user_alice",
                session_id="sess",
                repository="",
                prompt="Prompt",
            )
        with pytest.raises(JobQueueError):
            service_client.submit_agent_turn(
                user_id="user_alice",
                session_id="sess",
                repository="org/repo",
                prompt="",
            )

    def test_10_database_and_queue_failure_behavior(self, service_client, temp_db_path):
        """Requirement 10: Database errors are properly raised and not masked as success or empty."""
        # Intentionally close underlying connection to simulate database error
        service_client._queue.conn.close()

        with pytest.raises(JobQueueError):
            service_client.submit_agent_turn(
                user_id="user_alice",
                session_id="sess_err",
                repository="org/repo",
                prompt="Fail task",
            )

        with pytest.raises(JobQueueError):
            service_client.get_job(job_id="any_id", user_id="user_alice")

        with pytest.raises(JobQueueError):
            service_client.cancel_job(job_id="any_id", user_id="user_alice")

    def test_sanitization_helper(self):
        """Verify secret tokens and keys are redacted in messages and event text."""
        msg = "Error using ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 and sk-abcdefghijklmnopqrstuvwxyz123456"
        sanitized = sanitize_text(msg)
        assert "ghp_" not in sanitized
        assert "sk-" not in sanitized
        assert "[REDACTED_SECRET]" in sanitized

    def test_session_active_job_lookup(self, service_client):
        """Verify get_active_job_for_session retrieves running/queued jobs and ignores completed."""
        # Enqueue job for session 1
        j1 = service_client.submit_agent_turn(
            user_id="user_alice",
            session_id="sess_hydration",
            repository="org/repo",
            prompt="Active task",
            turn_index=1,
        )

        active = service_client.get_active_job_for_session("sess_hydration", "user_alice")
        assert active is not None
        assert active.id == j1.id
        assert active.status == "QUEUED"

        # Cancel job
        service_client.cancel_job(j1.id, "user_alice")

        # Now session has no active jobs
        active_after = service_client.get_active_job_for_session("sess_hydration", "user_alice")
        assert active_after is None

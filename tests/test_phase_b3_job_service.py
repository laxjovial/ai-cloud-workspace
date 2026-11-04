"""Test Suite for Phase B.3.1 Job Service Client Boundary (Targeted Corrections).

Covers all review requirements and safeguards:
1. Successful job submission with AuthenticatedContext and immutable JobView return.
2. Duplicate submission, canonical multi-attribute idempotency, and IdempotencyConflictError rejection.
3. Cross-tenant job access rejection.
4. Cross-tenant event access rejection.
5. Cross-tenant cancellation rejection.
6. Incremental event retrieval (after_id sequence filtering).
7. Cancellation of queued jobs (immediate transition) and running jobs (cancel_requested_at).
8. Cancellation of terminal jobs (no-op, preserves state).
9. Invalid payload, raw secret detection, and reserved payload key protection.
10. Database and queue failure behavior (does not mask errors).
11. Explicit AuthenticatedContext contract enforcement and unauthenticated identity rejection.
12. Deep immutability of JobView and JobEventView (nested dicts/lists cannot be mutated).
13. Recursive sanitization at arbitrary nesting depth (lists of dicts, dicts of lists).
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
    AuthenticatedContext,
    IdempotencyConflictError,
    JobEventView,
    JobServiceClient,
    JobView,
    compute_comprehensive_idempotency_key,
    sanitize_recursive,
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


@pytest.fixture
def alice_auth():
    """Provides a valid AuthenticatedContext for Alice."""
    return AuthenticatedContext(user_id="user_alice", email="alice@example.com", roles=("user",))


@pytest.fixture
def bob_auth():
    """Provides a valid AuthenticatedContext for Bob."""
    return AuthenticatedContext(user_id="user_bob", email="bob@example.com", roles=("user",))


class TestPhaseB3JobService:
    """Test suite verifying JobServiceClient application boundary and invariants."""

    def test_1_successful_job_submission(self, service_client, alice_auth):
        """Requirement 1: Verify successful job submission with AuthenticatedContext and immutable JobView."""
        job_view = service_client.submit_agent_turn(
            context=alice_auth,
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

    def test_2_duplicate_submission_and_idempotency_conflicts(self, service_client, alice_auth):
        """Requirement 2: Verify duplicate submissions return existing job, but parameter mismatches raise IdempotencyConflictError."""
        # 1. First submission
        job1 = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_abc",
            repository="org/repo",
            prompt="Implement bugfix",
            turn_index=2,
            model="gemini/gemini-2.5-flash",
        )

        # 2. Second exact duplicate submission -> returns identical JobView without re-enqueuing
        job2 = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_abc",
            repository="org/repo",
            prompt="Implement bugfix",
            turn_index=2,
            model="gemini/gemini-2.5-flash",
        )
        assert job1.id == job2.id
        assert job1.idempotency_key == job2.idempotency_key

        # 3. Explicit custom idempotency key with conflicting repository
        custom_key = "idemp_custom_12345"
        job_custom = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_custom",
            repository="org/repo_a",
            prompt="Custom prompt",
            idempotency_key=custom_key,
        )
        assert job_custom.idempotency_key == custom_key

        # Duplicate with same custom key but different repository -> IdempotencyConflictError
        with pytest.raises(IdempotencyConflictError) as exc_repo:
            service_client.submit_agent_turn(
                context=alice_auth,
                session_id="sess_custom",
                repository="org/repo_b",  # Differing repository!
                prompt="Custom prompt",
                idempotency_key=custom_key,
            )
        assert "repository" in str(exc_repo.value)

        # Duplicate with same custom key but different model -> IdempotencyConflictError
        with pytest.raises(IdempotencyConflictError) as exc_model:
            service_client.submit_agent_turn(
                context=alice_auth,
                session_id="sess_custom",
                repository="org/repo_a",
                prompt="Custom prompt",
                model="openai/gpt-4o",  # Differing model!
                idempotency_key=custom_key,
            )
        assert "model" in str(exc_model.value)

        # Duplicate with same custom key but different prompt -> IdempotencyConflictError
        with pytest.raises(IdempotencyConflictError) as exc_prompt:
            service_client.submit_agent_turn(
                context=alice_auth,
                session_id="sess_custom",
                repository="org/repo_a",
                prompt="Completely different prompt",  # Differing prompt!
                idempotency_key=custom_key,
            )
        assert "prompt" in str(exc_prompt.value)

        # Duplicate with same custom key but different credential_refs -> IdempotencyConflictError
        with pytest.raises(IdempotencyConflictError) as exc_creds:
            service_client.submit_agent_turn(
                context=alice_auth,
                session_id="sess_custom",
                repository="org/repo_a",
                prompt="Custom prompt",
                credential_refs={"github": {"key_ref": "different_pat"}},
                idempotency_key=custom_key,
            )
        assert "credential references" in str(exc_creds.value)

    def test_3_cross_tenant_job_access_rejection(self, service_client, alice_auth, bob_auth):
        """Requirement 3: Verify tenant ownership is strictly enforced for job status queries."""
        job = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Alice can fetch her job
        alice_view = service_client.get_job(job_id=job.id, context=alice_auth)
        assert alice_view.id == job.id

        # Bob cannot fetch Alice's job
        with pytest.raises(JobAccessDeniedError) as exc_info:
            service_client.get_job(job_id=job.id, context=bob_auth)
        assert f"Access denied to job '{job.id}'." in str(exc_info.value)

        # Non-existent job raises JobNotFoundError
        with pytest.raises(JobNotFoundError):
            service_client.get_job(job_id="non_existent_uuid", context=alice_auth)

    def test_4_cross_tenant_event_access_rejection(self, service_client, alice_auth, bob_auth):
        """Requirement 4: Verify tenant ownership is strictly enforced for event streams."""
        job = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Alice can retrieve events
        events = service_client.get_job_events(job_id=job.id, context=alice_auth)
        assert len(events) >= 1
        assert events[0].event_type == "ENQUEUED"

        # Bob cannot retrieve Alice's job events
        with pytest.raises(JobAccessDeniedError):
            service_client.get_job_events(job_id=job.id, context=bob_auth)

    def test_5_cross_tenant_cancellation_rejection(self, service_client, alice_auth, bob_auth):
        """Requirement 5: Verify tenant ownership is strictly enforced for job cancellation."""
        job = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_1",
            repository="org/repo",
            prompt="Alice prompt",
        )

        # Bob cannot cancel Alice's job
        with pytest.raises(JobAccessDeniedError):
            service_client.cancel_job(job_id=job.id, context=bob_auth)

        # Job remains QUEUED and uncancelled
        check = service_client.get_job(job_id=job.id, context=alice_auth)
        assert check.status == "QUEUED"
        assert check.is_cancellation_requested is False

    def test_6_incremental_event_retrieval(self, service_client, temp_db_path, alice_auth):
        """Requirement 6: Verify incremental event retrieval using after_id filtering."""
        job = service_client.submit_agent_turn(
            context=alice_auth,
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
        all_events = service_client.get_job_events(job_id=job.id, context=alice_auth, after_id=0)
        assert len(all_events) == 4
        assert all_events[0].event_type == "ENQUEUED"
        assert all_events[1].event_type == "CLAIMED"
        assert all_events[2].event_type == "STEP_START"
        assert all_events[3].event_type == "TOOL_CALL"

        # Incremental fetch: fetch only events after event ID 2
        last_id = all_events[1].id
        new_events = service_client.get_job_events(job_id=job.id, context=alice_auth, after_id=last_id)
        assert len(new_events) == 2
        assert new_events[0].event_type == "STEP_START"
        assert new_events[1].event_type == "TOOL_CALL"
        assert all(e.id > last_id for e in new_events)

    def test_7_cancellation_queued_and_running_jobs(self, service_client, temp_db_path, alice_auth):
        """Requirement 7: Cancellation transitions QUEUED to CANCELLED and marks RUNNING jobs."""
        # 1. Cancel QUEUED job -> immediate transition to CANCELLED
        job_queued = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_q",
            repository="org/repo",
            prompt="Queued task",
        )
        res_q = service_client.cancel_job(job_id=job_queued.id, context=alice_auth)
        assert res_q.status == "CANCELLED"
        assert res_q.is_terminal is True
        assert res_q.cancel_requested_at is not None
        assert res_q.cancel_requested_by == "user_alice"

        # 2. Cancel RUNNING job -> marks cancel_requested_at for worker cooperative termination
        job_running = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_r",
            repository="org/repo",
            prompt="Running task",
        )
        queue = DurableJobQueue(db_path=temp_db_path)
        claimed = queue.claim_next_job(worker_id="worker_test_run")
        assert claimed is not None
        assert claimed["id"] == job_running.id
        queue.close()

        res_r = service_client.cancel_job(job_id=job_running.id, context=alice_auth)
        assert res_r.status == "RUNNING"  # Stays running until worker observes and aborts
        assert res_r.is_cancellation_requested is True
        assert res_r.cancel_requested_by == "user_alice"

    def test_8_cancellation_of_terminal_jobs(self, service_client, temp_db_path, alice_auth):
        """Requirement 8: Cancellation of terminal jobs (COMPLETED, FAILED) is safe and preserves status."""
        job = service_client.submit_agent_turn(
            context=alice_auth,
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
        view_before = service_client.get_job(job_id=job.id, context=alice_auth)
        assert view_before.status == "COMPLETED"
        assert view_before.is_terminal is True

        # Calling cancel on completed job is a safe no-op
        view_after = service_client.cancel_job(job_id=job.id, context=alice_auth)
        assert view_after.status == "COMPLETED"
        assert view_after.is_terminal is True

    def test_9_invalid_payload_and_reserved_key_protection(self, service_client, alice_auth):
        """Requirement 9: Reserved payload keys cannot be overridden and raw secrets are rejected."""
        # 1. Plaintext API key in credential_refs must be rejected
        with pytest.raises(InvalidCredentialReferenceError):
            service_client.submit_agent_turn(
                context=alice_auth,
                session_id="sess_leak",
                repository="org/repo",
                prompt="Task with raw token",
                credential_refs={"github": "ghp_123456789012345678901234567890123456"},
            )

        # 2. Raw secret string in payload prompt or additional_payload
        with pytest.raises(JobQueueError):
            service_client.submit_agent_turn(
                context=alice_auth,
                session_id="sess_leak2",
                repository="org/repo",
                prompt="Task with raw token sk-1234567890abcdef1234567890abcdef",
            )

        # 3. Attempting to override reserved payload keys via additional_payload must fail
        for reserved_key in ("job_type", "prompt", "model", "turn_index", "user_id", "repository"):
            with pytest.raises(JobQueueError) as exc_res:
                service_client.submit_agent_turn(
                    context=alice_auth,
                    session_id="sess_res",
                    repository="org/repo",
                    prompt="Task",
                    additional_payload={reserved_key: "malicious_override"},
                )
            assert "reserved payload key" in str(exc_res.value)

        # 4. Attempting to pass disallowed keys in additional_payload must fail
        with pytest.raises(JobQueueError) as exc_dis:
            service_client.submit_agent_turn(
                context=alice_auth,
                session_id="sess_dis",
                repository="org/repo",
                prompt="Task",
                additional_payload={"unsupported_key": "some_value"},
            )
        assert "Disallowed key" in str(exc_dis.value)

        # 5. Allowed optional keys succeed
        job_allowed = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_ok",
            repository="org/repo",
            prompt="Task",
            additional_payload={"base_branch": "main", "reasoning_effort": "high"},
        )
        assert job_allowed.payload["base_branch"] == "main"
        assert job_allowed.payload["reasoning_effort"] == "high"

    def test_10_database_and_queue_failure_behavior(self, service_client, alice_auth):
        """Requirement 10: Database errors are properly raised and not masked as success or empty."""
        # Intentionally close underlying connection to simulate database error
        service_client._queue.conn.close()

        with pytest.raises(JobQueueError):
            service_client.submit_agent_turn(
                context=alice_auth,
                session_id="sess_err",
                repository="org/repo",
                prompt="Fail task",
            )

        with pytest.raises(JobQueueError):
            service_client.get_job(job_id="any_id", context=alice_auth)

        with pytest.raises(JobQueueError):
            service_client.cancel_job(job_id="any_id", context=alice_auth)

    def test_11_authenticated_identity_contract_enforcement(self, service_client):
        """Requirement 11: Explicit AuthenticatedContext contract enforcement."""
        # 1. Empty or whitespace user_id in AuthenticatedContext fails
        with pytest.raises(JobAccessDeniedError):
            AuthenticatedContext(user_id="")
        with pytest.raises(JobAccessDeniedError):
            AuthenticatedContext(user_id="   ")

        # 2. String without non-empty content passed to service fails
        with pytest.raises(JobAccessDeniedError):
            service_client.get_job(job_id="any_id", context="")

        # 3. Invalid context type fails
        with pytest.raises(JobAccessDeniedError):
            service_client.get_job(job_id="any_id", context=12345)  # type: ignore

    def test_12_deep_immutability_of_views(self, service_client, alice_auth, temp_db_path):
        """Requirement 12: JobView and JobEventView prevent nested data mutation."""
        job = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_mut",
            repository="org/repo",
            prompt="Immutability check",
            credential_refs={"github": {"key_ref": "valid_pat_ref"}},
            additional_payload={"base_branch": "develop"},
        )

        # 1. Top-level reassignment fails
        with pytest.raises(Exception):
            job.status = "COMPLETED"  # type: ignore

        # 2. Nested payload mutation fails
        with pytest.raises(TypeError):
            job.payload["prompt"] = "Hacked prompt"

        with pytest.raises(TypeError):
            job.payload["new_field"] = "Hacked field"

        # 3. Nested credential_refs mutation fails
        with pytest.raises(TypeError):
            job.credential_refs["github"] = "Hacked pat"

        # 4. JobEventView nested data immutability
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

        events = service_client.get_job_events(job_id=job.id, context=alice_auth)
        log_event = [e for e in events if e.event_type == "LOG"][0]

        # Nested data mutation fails
        with pytest.raises(TypeError):
            log_event.data["step"] = 99

        # Nested data mutation fails: 'files' is converted to immutable tuple
        assert isinstance(log_event.data["details"]["files"], tuple)
        with pytest.raises((AttributeError, TypeError)):
            log_event.data["details"]["files"].append("c.py")
        with pytest.raises(TypeError):
            log_event.data["details"]["files"][0] = "c.py"

    def test_13_recursive_sanitization_at_arbitrary_depth(self, service_client, alice_auth, temp_db_path):
        """Requirement 13: Recursive sanitization redacts secrets in deeply nested lists and dicts."""
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

        # Check nested dict inside list inside dict
        step0 = sanitized["execution"]["steps"][0]
        assert "ghp_" not in step0["command"]
        assert "[REDACTED_SECRET]" in step0["command"]
        assert "sk-" not in step0["auth"]["header"]
        assert "[REDACTED_SECRET]" in step0["auth"]["header"]

        # Check string inside list inside list inside dict
        step1 = sanitized["execution"]["steps"][1]
        assert "AIza" not in step1["logs"][0]
        assert "[REDACTED_SECRET]" in step1["logs"][0]

    def test_14_session_active_job_lookup(self, service_client, alice_auth):
        """Requirement 14: Verify get_active_job_for_session retrieves running/queued jobs and ignores completed."""
        j1 = service_client.submit_agent_turn(
            context=alice_auth,
            session_id="sess_hydration",
            repository="org/repo",
            prompt="Active task",
            turn_index=1,
        )

        active = service_client.get_active_job_for_session("sess_hydration", alice_auth)
        assert active is not None
        assert active.id == j1.id
        assert active.status == "QUEUED"

        # Cancel job
        service_client.cancel_job(j1.id, alice_auth)

        # Now session has no active jobs
        active_after = service_client.get_active_job_for_session("sess_hydration", alice_auth)
        assert active_after is None

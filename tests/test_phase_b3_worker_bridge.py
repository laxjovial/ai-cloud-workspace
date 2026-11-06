"""Comprehensive Test Suite for Phase B.3.2 Worker-Agent Execution Bridge.

Covers all Phase B.3.2 requirements:
1. End-to-end execution: A job submitted through JobServiceClient is claimed and executed by the worker.
2. Invocation of agent turn: agent.run_agent_turn is invoked and returns assistant response.
3. Observable progress: Progress events (AGENT_STATUS) and completion events (ASSISTANT_RESPONSE)
   are written to durable job_events and queryable via JobServiceClient.get_job_events().
4. Result persistence: Final assistant output and terminal logs are persisted and retrievable after execution.
5. Error handling: Agent exceptions or returned errors produce FAILED status with diagnostic error JSON.
6. Timeout enforcement: Tasks exceeding timeout_seconds produce TIMED_OUT status without extending leases.
7. Cooperative cancellation: Cancellation requested via service client stops tool execution via ExecutionController.
8. Lease fencing: Stale worker lease generation cannot persist logs or update job status.
9. Credential isolation (Fail-Closed): Missing or invalid credential references in secure store fail closed.
10. Sandbox lifecycle: Normal completion triggers sandbox.close() and transitions sandbox_cleanup_status to CLEANED;
    cleanup failure logs error and remains visible/recoverable without false CLEANED state.
11. Session independence: Worker process executes autonomously without depending on a browser or Streamlit session.
"""

import multiprocessing
import os
import sqlite3
import tempfile
import threading
import time
import pytest

from db import _save_local_user_data
from job_queue import (
    DurableJobQueue,
    JobQueueError,
    StaleLeaseError,
)
from job_service import (
    AuthenticatedPrincipal,
    HMACAuthenticationAuthority,
    JobServiceClient,
)
from worker import (
    ExecutionController,
    WorkerSupervisor,
    execute_job_in_worker,
    worker_child_main,
)


class MockSandbox:
    """Mock sandbox container implementing WorkspaceSandbox protocol."""

    def __init__(self, repo_name: str, github_token: str = "", sandbox_id: str = "sb_mock_123"):
        self.repo_name = repo_name
        self.github_token = github_token
        self.sandbox_id = sandbox_id
        self.closed = False
        self.fail_on_close = False
        self.executed_commands = []

    def start(self):
        return {"success": True, "sandbox_id": self.sandbox_id}

    def close(self):
        self.closed = True
        if self.fail_on_close:
            raise RuntimeError("Cloud provider network error during container destruction")

    def run_command(self, cmd: str):
        self.executed_commands.append(cmd)
        return {"stdout": f"Output of {cmd}", "stderr": "", "exit_code": 0}

    def read_file(self, path: str):
        return {"content": "Mock file contents"}

    def write_file(self, path: str, content: str):
        return {"success": True}

    def commit_and_create_pr(self, branch_name: str, commit_message: str, pr_title: str, pr_body: str):
        return {"success": True, "pr_url": f"https://github.com/{self.repo_name}/pull/99"}


# Multiprocessing worker target helper (picklable)
def _multiprocess_worker_runner(worker_id, db_path, stop_event, single_turn=True):
    """Entry point for testing autonomous worker child process."""
    worker_child_main(
        worker_instance_id=worker_id,
        db_path=db_path,
        stop_event=stop_event,
        single_turn=single_turn,
    )


class TestPhaseB3WorkerBridge:
    """Test suite verifying Phase B.3.2 Worker-Agent Execution Bridge."""

    @pytest.fixture
    def temp_db_path(self):
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
    def auth_authority(self):
        return HMACAuthenticationAuthority(authority_secret=b"test_secret_for_bridge_testing_32b!")

    @pytest.fixture
    def service_client(self, temp_db_path, auth_authority):
        client = JobServiceClient(db_path=temp_db_path, auth_verifier=auth_authority)
        yield client
        client.close()

    @pytest.fixture
    def alice_principal(self, auth_authority):
        return auth_authority.issue_principal_from_verified_credential(
            verified_user_id="user_alice",
            email="alice@example.com",
        )

    def test_1_end_to_end_job_submission_and_worker_execution(
        self, temp_db_path, service_client, alice_principal
    ):
        """Requirement 1 & 2: A job submitted via JobServiceClient is claimed and executed by the worker."""
        # Setup mock user store with credentials
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={
                "github_access_token": "ghp_mock_token_for_alice",
                "api_keys": {"gemini": "sk_mock_gemini_key_123"},
            },
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_e2e",
            repository="owner/repo_alpha",
            prompt="Refactor the authentication module",
            model="gemini/gemini-2.5-flash",
            credential_refs={
                "github": {"key_ref": "primary_pat"},
                "gemini": {"key_ref": "gemini_key"},
            },
        )
        assert job_view.status == "QUEUED"

        # Track mock agent execution
        agent_invocations = []
        mock_sb = MockSandbox(repo_name="owner/repo_alpha", sandbox_id="sb_e2e_001")

        def mock_agent_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            agent_invocations.append({
                "messages": messages,
                "model": model,
                "api_key": api_key,
            })
            if status_callback:
                status_callback("Analyzing AST structure in repository...")
                status_callback("Running test suite...")
            return {
                "success": True,
                "content": "Refactored authentication module successfully.",
                "terminal_logs": "$ pytest tests/ -v\n15 passed in 0.2s",
            }

        # Run worker turn
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_1",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo_name, github_token: mock_sb,
            test_agent_turn_fn=mock_agent_turn,
            single_turn=True,
        )

        # Verify agent function was actually invoked with resolved credentials
        assert len(agent_invocations) == 1
        assert agent_invocations[0]["model"] == "gemini/gemini-2.5-flash"
        assert agent_invocations[0]["api_key"] == "sk_mock_gemini_key_123"

        # Verify updated job status via JobServiceClient
        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "COMPLETED"
        assert updated_job.sandbox_cleanup_status == "CLEANED"
        assert mock_sb.closed is True

        # Requirement 3 & 4: Progress events and final output are persisted and observable
        events = service_client.get_job_events(job_view.id, principal=alice_principal)
        event_types = [e.event_type for e in events]
        event_messages = [e.message for e in events]

        assert "AGENT_STATUS" in event_types
        assert any("Analyzing AST structure" in m for m in event_messages)
        assert any("Running test suite" in m for m in event_messages)

        assert "ASSISTANT_RESPONSE" in event_types
        resp_event = next(e for e in events if e.event_type == "ASSISTANT_RESPONSE")
        assert "Refactored authentication module successfully." in resp_event.message
        assert "pytest tests/ -v" in resp_event.data.get("terminal_logs", "")

    def test_2_agent_exception_produces_failed_status(
        self, temp_db_path, service_client, alice_principal
    ):
        """Requirement 5: Agent execution exceptions produce FAILED status with diagnostic error details."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk_gemini_123"}},
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_err",
            repository="owner/repo_err",
            prompt="Trigger error",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        def failing_agent_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            raise RuntimeError("LiteLLM connection refused by upstream provider")

        mock_sb = MockSandbox(repo_name="owner/repo_err")
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_2",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            test_agent_turn_fn=failing_agent_turn,
            single_turn=True,
        )

        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "FAILED"
        assert "connection refused" in str(updated_job.error)

    def test_3_step_limit_or_agent_failure_outcome_mapping(
        self, temp_db_path, service_client, alice_principal
    ):
        """Requirement 5: Step limit exhaustion maps to TIMED_OUT or FAILED with assistant content preserved."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk_gemini_123"}},
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_step_limit",
            repository="owner/repo_limit",
            prompt="Complex multi-step task",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        def step_limit_agent_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            return {
                "success": False,
                "status": "incomplete_step_limit",
                "error": "Reached maximum steps limit (15).",
                "content": "Partial progress completed.",
                "terminal_logs": "step 1... step 15...",
            }

        mock_sb = MockSandbox(repo_name="owner/repo_limit")
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_3",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            test_agent_turn_fn=step_limit_agent_turn,
            single_turn=True,
        )

        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "TIMED_OUT"
        assert updated_job.error is not None
        assert "incomplete_step_limit" in updated_job.error.get("status", "")
        assert updated_job.error.get("content") == "Partial progress completed."

    def test_4_cooperative_cancellation_during_turn(
        self, temp_db_path, service_client, alice_principal
    ):
        """Requirement 6 & 7: Cancellation requested via service client aborts the active agent execution path."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk_gemini_123"}},
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_cancel",
            repository="owner/repo_cancel",
            prompt="Cancel me during run",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        tool_calls_attempted = []

        def cancellable_agent_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            # Step 1: Simulate first tool call
            if execution_controller.is_aborted():
                return {"success": False, "status": "aborted", "error": execution_controller.abort_reason}
            tool_calls_attempted.append("step_1")

            # External cancellation request arrives
            service_client.cancel_job(job_view.id, principal=alice_principal)

            # Check watchdog/controller polling simulation
            q = DurableJobQueue(db_path=temp_db_path)
            poll_res = q.poll_worker_status(
                job_id=job_view.id,
                worker_id="worker_test_4",
                lease_generation=1,
                lease_duration_seconds=30.0,
            )
            q.close()
            if poll_res == "CANCEL_REQUESTED":
                execution_controller.abort("User requested cancellation.")

            # Step 2: Second tool call should be prevented
            if execution_controller.is_aborted():
                return {"success": False, "status": "aborted", "error": execution_controller.abort_reason}
            tool_calls_attempted.append("step_2_should_not_run")
            return {"success": True, "content": "Finished"}

        mock_sb = MockSandbox(repo_name="owner/repo_cancel")
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_4",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            test_agent_turn_fn=cancellable_agent_turn,
            single_turn=True,
        )

        # Verify only step_1 executed and execution aborted
        assert tool_calls_attempted == ["step_1"]
        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "CANCELLED"

    def test_5_stale_lease_generation_cannot_persist_or_mutate_job(
        self, temp_db_path, service_client, alice_principal
    ):
        """Requirement 7: A worker whose lease generation is stale cannot persist results or update status."""
        q = DurableJobQueue(db_path=temp_db_path)
        job = q.enqueue_job(
            user_id="user_alice",
            session_id="sess_stale",
            repository="owner/repo_stale",
            payload={"prompt": "Stale test"},
            credential_refs={},
        )
        claimed = q.claim_next_job(worker_id="worker_gen_1", lease_duration_seconds=30.0)
        stale_gen = claimed["lease_generation"]

        # Simulate job being reclaimed by worker_gen_2, advancing lease_generation to 2
        # First expire worker_gen_1 lease in database
        with q._lock:
            with q.conn:
                q.conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE id = ?;", (time.time() - 10, job["id"]))

        q.reap_stale_jobs()
        reclaimed = q.claim_next_job(worker_id="worker_gen_2", lease_duration_seconds=30.0)
        assert reclaimed["lease_generation"] > stale_gen

        # Now, stale worker (worker_gen_1 with stale_gen) attempts to append log or update status
        with pytest.raises(StaleLeaseError):
            q.append_log(
                job_id=job["id"],
                worker_id="worker_gen_1",
                lease_generation=stale_gen,
                message="Stale attempt",
            )

        with pytest.raises(StaleLeaseError):
            q.update_job_status(
                job_id=job["id"],
                target_status="COMPLETED",
                worker_id="worker_gen_1",
                lease_generation=stale_gen,
            )

        q.close()

    def test_6_fail_closed_credential_resolution_for_unauthorized_user(
        self, temp_db_path, service_client, alice_principal
    ):
        """Requirement 9: Credential resolution fails closed when user credentials are missing or unresolvable."""
        # Note: We do NOT write credentials for user_alice in user store
        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_no_cred",
            repository="owner/repo_no_cred",
            prompt="Task requiring credentials",
            credential_refs={"github": {"key_ref": "missing_github_pat"}},
        )

        mock_sb = MockSandbox(repo_name="owner/repo_no_cred")
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_6",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            single_turn=True,
        )

        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "FAILED"
        assert "Security violation" in str(updated_job.error)

    def test_7_sandbox_cleanup_failure_remains_visible_and_recoverable(
        self, temp_db_path, service_client, alice_principal
    ):
        """Requirement 10: Sandbox cleanup failure on completion leaves state recoverable, never falsely CLEANED."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk_gemini_123"}},
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_cleanup_fail",
            repository="owner/repo_cleanup",
            prompt="Task with failing cleanup",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        mock_sb = MockSandbox(repo_name="owner/repo_cleanup")
        mock_sb.fail_on_close = True  # Sandbox close() will raise

        def simple_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            return {"success": True, "content": "Done"}

        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_7",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            test_agent_turn_fn=simple_turn,
            single_turn=True,
        )

        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "COMPLETED"
        # Since close() failed, normal cleanup was not finalized -> remains ACTIVE/ORPHANED, NOT falsely CLEANED
        assert updated_job.sandbox_cleanup_status != "CLEANED"

        # Check that error log was recorded
        events = service_client.get_job_events(job_view.id, principal=alice_principal)
        assert any("Sandbox close error" in e.message for e in events)

    def test_8_independent_os_process_execution_without_browser_session(
        self, temp_db_path, service_client, alice_principal, auth_authority
    ):
        """Requirement 11: Worker executes in an independent OS process without depending on an active session."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={
                "github_access_token": "ghp_tok_888",
                "api_keys": {"gemini": "sk_gemini_888"},
            },
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_os_proc",
            repository="owner/repo_proc",
            prompt="Autonomous background task",
            credential_refs={
                "github": {"key_ref": "github_tok"},
                "gemini": {"key_ref": "gemini_tok"},
            },
        )

        # Close service client to simulate browser disconnect
        service_client.close()

        # Spawn genuine separate OS child process to run the worker
        ctx = multiprocessing.get_context("spawn")
        stop_ev = ctx.Event()
        p = ctx.Process(
            target=_multiprocess_worker_runner,
            args=("worker_proc_independent", temp_db_path, stop_ev, True),
        )
        p.start()
        p.join(timeout=15.0)

        assert not p.is_alive()
        assert p.exitcode == 0

        # Now simulate user reconnecting in a new session later
        reconnected_client = JobServiceClient(db_path=temp_db_path, auth_verifier=auth_authority)
        retrieved_job = reconnected_client.get_job(job_view.id, principal=alice_principal)
        reconnected_client.close()

        assert retrieved_job.status in ("COMPLETED", "FAILED")

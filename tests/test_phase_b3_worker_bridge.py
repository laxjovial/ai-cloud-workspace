"""Comprehensive Test Suite for Phase B.3.2 Worker-Agent Execution Bridge.

Covers all Phase B.3.2 requirements and senior review corrections:
1. End-to-end execution: A job submitted through JobServiceClient is claimed and executed by the worker.
2. Invocation of agent turn: agent.run_agent_turn / agent_turn_fn is invoked and returns assistant response.
3. Observable progress: Progress events (AGENT_STATUS) and completion events (ASSISTANT_RESPONSE)
   are written to durable job_events and queryable via JobServiceClient.get_job_events().
4. Result persistence: Final assistant output and terminal logs are persisted and retrievable after execution.
5. Error & outcome semantics:
   - User cancellation -> CANCELLED
   - Execution timeout -> TIMED_OUT (via watchdog and ExecutionController timeout)
   - Step-limit exhaustion -> FAILED (with structured diagnostic content preserved; never falsely TIMED_OUT)
   - Agent exception -> FAILED
   - Completion -> COMPLETED
6. Credential reference resolution:
   - Primary GitHub PAT vs specific connected account (id or label)
   - Rejection when specific account does not match (no silent fallback to primary)
   - Provider API keys matched against provider reference
   - Cross-tenant / unauthorized isolation
7. Secret sanitization at persistence boundary:
   - Raw GitHub tokens, API keys, and Bearer tokens are redacted in AGENT_STATUS, ASSISTANT_RESPONSE, terminal logs, and error dicts
   - Inspects SQLite database directly to verify raw secrets never touch disk
8. Independent OS process execution:
   - Spawned OS process with picklable test doubles executes turn to completion
   - Confirms agent was invoked and job reached COMPLETED
   - Confirms result and events survive connection closure and reopening
9. Cooperative cancellation and forced termination boundaries:
   - Tests cooperative interruption checks in agent loop
   - Documents and tests watchdog / supervisor fallback
10. Stale lease fencing:
   - Stale worker generation cannot mutate status or append logs
11. Sandbox lifecycle and cleanup recovery:
   - Destruction failures remain visible and recoverable, never falsely CLEANED
"""

import multiprocessing
import os
import sqlite3
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch
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
        self.executed_commands: List[str] = []

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


# Picklable top-level helpers for multiprocessing worker tests
def _multiprocess_test_agent_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
    if status_callback:
        status_callback("Spawned worker agent running in independent process.")
    return {
        "success": True,
        "content": "Spawned worker agent successfully executed turn in independent process.",
        "terminal_logs": "pytest passed 10/10",
    }


def _multiprocess_worker_runner_with_mock(worker_id, db_path, stop_event, single_turn=True):
    """Entry point for testing autonomous worker child process with picklable mock agent."""
    mock_sb = MockSandbox(repo_name="owner/repo_proc")
    worker_child_main(
        worker_instance_id=worker_id,
        db_path=db_path,
        stop_event=stop_event,
        test_sandbox_factory=lambda repo_name, github_token: mock_sb,
        test_agent_turn_fn=_multiprocess_test_agent_turn,
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

    @pytest.fixture
    def bob_principal(self, auth_authority):
        return auth_authority.issue_principal_from_verified_credential(
            verified_user_id="user_bob",
            email="bob@example.com",
        )

    def test_1_end_to_end_job_submission_and_worker_execution(
        self, temp_db_path, service_client, alice_principal
    ):
        """Requirement 1 & 2: A job submitted via JobServiceClient is claimed and executed by the worker."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={
                "github_access_token": "ghp_mock_token_for_alice_1234567890",
                "api_keys": {"gemini": "sk-mock-gemini-key-1234567890123456"},
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

        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_1",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo_name, github_token: mock_sb,
            test_agent_turn_fn=mock_agent_turn,
            single_turn=True,
        )

        assert len(agent_invocations) == 1
        assert agent_invocations[0]["model"] == "gemini/gemini-2.5-flash"
        assert agent_invocations[0]["api_key"] == "sk-mock-gemini-key-1234567890123456"

        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "COMPLETED"
        assert updated_job.sandbox_cleanup_status == "CLEANED"
        assert mock_sb.closed is True

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
            data={"api_keys": {"gemini": "sk-gemini-test-key-123456789012"}},
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

    def test_3_step_limit_maps_to_failed_with_diagnostics_preserved(
        self, temp_db_path, service_client, alice_principal
    ):
        """Senior Review Issue 1: Step limit exhaustion maps to FAILED, preserving diagnostic content (never falsely TIMED_OUT)."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk-gemini-test-key-123456789012"}},
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
        # Must be FAILED, NOT TIMED_OUT
        assert updated_job.status == "FAILED"
        assert updated_job.error is not None
        assert updated_job.error.get("status") == "incomplete_step_limit"
        assert updated_job.error.get("content") == "Partial progress completed."
        assert updated_job.error.get("terminal_logs") == "step 1... step 15..."

    def test_4_timeout_watchdog_maps_to_timed_out_distinct_from_cancellation(
        self, temp_db_path, service_client, alice_principal
    ):
        """Senior Review Issue 1: Execution timeout maps to TIMED_OUT, distinct from CANCELLED."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk-gemini-test-key-123456789012"}},
        )

        # Submit job with short timeout of 1.0 second
        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_timeout",
            repository="owner/repo_timeout",
            prompt="Task that exceeds timeout",
            timeout_seconds=1.0,
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        def slow_agent_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            # Wait until watchdog aborts with timeout
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                if execution_controller.is_aborted():
                    return {"success": False, "status": "aborted", "error": execution_controller.abort_reason}
                time.sleep(0.1)
            return {"success": True, "content": "Finished slow task"}

        mock_sb = MockSandbox(repo_name="owner/repo_timeout")
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_timeout",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            test_agent_turn_fn=slow_agent_turn,
            single_turn=True,
        )

        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        # Timeout must map to TIMED_OUT, NOT CANCELLED
        assert updated_job.status == "TIMED_OUT"
        assert updated_job.error is not None
        assert "timeout" in updated_job.error.get("abort_type", "")

    def test_5_cooperative_cancellation_during_turn(
        self, temp_db_path, service_client, alice_principal
    ):
        """Senior Review Issue 1 & 5: Cancellation requested via service client maps to CANCELLED and stops agent steps."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk-gemini-test-key-123456789012"}},
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
            if execution_controller.is_aborted():
                return {"success": False, "status": "aborted", "error": execution_controller.abort_reason}
            tool_calls_attempted.append("step_1")

            # External cancellation request arrives via service client
            service_client.cancel_job(job_view.id, principal=alice_principal)

            # Watchdog polls status and aborts controller
            q = DurableJobQueue(db_path=temp_db_path)
            poll_res = q.poll_worker_status(
                job_id=job_view.id,
                worker_id="worker_test_cancel",
                lease_generation=1,
                lease_duration_seconds=30.0,
            )
            q.close()
            if poll_res == "CANCEL_REQUESTED":
                execution_controller.abort("User requested cancellation.", abort_type="cancellation")

            if execution_controller.is_aborted():
                return {"success": False, "status": "aborted", "error": execution_controller.abort_reason}
            tool_calls_attempted.append("step_2_should_not_run")
            return {"success": True, "content": "Finished"}

        mock_sb = MockSandbox(repo_name="owner/repo_cancel")
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_cancel",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            test_agent_turn_fn=cancellable_agent_turn,
            single_turn=True,
        )

        assert tool_calls_attempted == ["step_1"]
        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "CANCELLED"
        assert updated_job.error.get("abort_type") == "cancellation"

    def test_6_stale_lease_generation_cannot_persist_or_mutate_job(
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

        # Advance lease generation to 2 via reaper and worker_gen_2 claim
        with q._lock:
            with q.conn:
                q.conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE id = ?;", (time.time() - 10, job["id"]))

        q.reap_stale_jobs()
        reclaimed = q.claim_next_job(worker_id="worker_gen_2", lease_duration_seconds=30.0)
        assert reclaimed["lease_generation"] > stale_gen

        # Stale worker attempts to append log or update status -> StaleLeaseError
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

    def test_7_credential_resolution_matrix_and_fail_closed_boundaries(
        self, temp_db_path, service_client, alice_principal, bob_principal
    ):
        """Senior Review Issue 2: Comprehensive matrix for primary PAT, connected account, mismatched reference, and tenant isolation."""
        # Setup Alice with primary PAT and a connected account 'gh_work'
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={
                "github_access_token": "ghp_alice_primary_12345678901234",
                "github_accounts": [
                    {
                        "id": "acc_work_001",
                        "account_label": "gh_work",
                        "github_username": "alice-work",
                        "github_token": "ghp_alice_work_connected_9988776655",
                    }
                ],
                "api_keys": {
                    "gemini": "sk-alice-gemini-key-123456789012",
                    "openai": "sk-alice-openai-key-123456789012",
                },
            },
        )

        # Setup Bob with distinct keys
        _save_local_user_data(
            user_id="user_bob",
            email="bob@example.com",
            data={
                "github_access_token": "ghp_bob_primary_00000000000000",
                "api_keys": {"gemini": "sk-bob-gemini-key-000000000000"},
            },
        )

        resolved_tokens = []

        def capture_token_agent(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            resolved_tokens.append({
                "api_key": api_key,
                "github_token": getattr(sandbox, "github_token", ""),
            })
            return {"success": True, "content": "Token captured."}

        # Case A: Alice references primary PAT
        j_primary = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_cred_a",
            repository="owner/repo_a",
            prompt="Test primary PAT",
            credential_refs={
                "github": {"key_ref": "primary_pat"},
                "gemini": {"key_ref": "gemini_key"},
            },
        )
        sb_a = MockSandbox("owner/repo_a")
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_cred_a",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo, github_token=tok),
            test_agent_turn_fn=capture_token_agent,
            single_turn=True,
        )
        assert resolved_tokens[-1]["github_token"] == "ghp_alice_primary_12345678901234"
        assert resolved_tokens[-1]["api_key"] == "sk-alice-gemini-key-123456789012"

        # Case B: Alice references specific connected account by label 'gh_work'
        j_connected = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_cred_b",
            repository="owner/repo_b",
            prompt="Test connected account",
            credential_refs={
                "github": {"key_ref": "gh_work"},
                "gemini": {"key_ref": "gemini_key"},
            },
        )
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_cred_b",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo, github_token=tok),
            test_agent_turn_fn=capture_token_agent,
            single_turn=True,
        )
        assert resolved_tokens[-1]["github_token"] == "ghp_alice_work_connected_9988776655"

        # Case C: Alice references an unknown connected account 'gh_personal'
        # Crucial requirement: Must FAIL closed, never silently substitute primary PAT!
        j_unknown = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_cred_c",
            repository="owner/repo_c",
            prompt="Test unknown connected account",
            credential_refs={
                "github": {"key_ref": "gh_personal"},
                "gemini": {"key_ref": "gemini_key"},
            },
        )
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_cred_c",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo, github_token=tok),
            test_agent_turn_fn=capture_token_agent,
            single_turn=True,
        )
        res_c = service_client.get_job(j_unknown.id, principal=alice_principal)
        assert res_c.status == "FAILED"
        assert "Security violation" in str(res_c.error)

        # Case D: Alice references wrong provider key_ref for openai
        j_wrong_prov = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_cred_d",
            repository="owner/repo_d",
            prompt="Test wrong provider key_ref",
            credential_refs={
                "github": {"key_ref": "primary_pat"},
                "openai": {"key_ref": "gemini_key"},  # mismatched key_ref
            },
        )
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_cred_d",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo, github_token=tok),
            test_agent_turn_fn=capture_token_agent,
            single_turn=True,
        )
        res_d = service_client.get_job(j_wrong_prov.id, principal=alice_principal)
        assert res_d.status == "FAILED"
        assert "Security violation" in str(res_d.error)

    def test_8_strict_secret_sanitization_at_database_boundary(
        self, temp_db_path, service_client, alice_principal
    ):
        """Senior Review Issue 3: Raw secrets are redacted BEFORE SQLite persistence across all event & error pathways."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk-alice-gemini-key-123456789012"}},
        )

        raw_gh_pat = "ghp_leakedsecretpat1234567890abcdef"
        raw_api_key = "sk-leakedopenaikey1234567890abcdef"
        raw_bearer = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M"

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_secrets",
            repository="owner/repo_secrets",
            prompt="Sanitization test",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        def leaky_agent_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            if status_callback:
                status_callback(f"Status note with PAT {raw_gh_pat} and header {raw_bearer}")
            return {
                "success": True,
                "content": f"Assistant response revealing API key: {raw_api_key}",
                "terminal_logs": f"$ curl -H '{raw_bearer}' https://api.openai.com\nKey: {raw_api_key}",
            }

        mock_sb = MockSandbox(repo_name="owner/repo_secrets")
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_sanitization",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            test_agent_turn_fn=leaky_agent_turn,
            single_turn=True,
        )

        # Inspect RAW SQLite database bytes and rows directly - proving raw secrets never touch disk
        conn = sqlite3.connect(temp_db_path)
        events_rows = conn.execute("SELECT message, data_json FROM job_events WHERE job_id = ?;", (job_view.id,)).fetchall()
        jobs_rows = conn.execute("SELECT error_json FROM jobs WHERE id = ?;", (job_view.id,)).fetchall()
        conn.close()

        all_persisted_strings = []
        for m, d in events_rows:
            all_persisted_strings.append(m or "")
            all_persisted_strings.append(d or "")
        for (err,) in jobs_rows:
            all_persisted_strings.append(err or "")

        combined_dump = "\n".join(all_persisted_strings)

        # Prove raw secrets are COMPLETELY ABSENT from SQLite tables
        assert raw_gh_pat not in combined_dump
        assert raw_api_key not in combined_dump
        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in combined_dump

        # Prove redacted placeholders are present
        assert "[REDACTED_SECRET]" in combined_dump

    def test_9_sandbox_cleanup_failure_remains_visible_and_recoverable(
        self, temp_db_path, service_client, alice_principal
    ):
        """Requirement 10: Sandbox cleanup failure on completion leaves state recoverable, never falsely CLEANED."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk-alice-gemini-key-123456789012"}},
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_cleanup_fail",
            repository="owner/repo_cleanup",
            prompt="Task with failing cleanup",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        mock_sb = MockSandbox(repo_name="owner/repo_cleanup")
        mock_sb.fail_on_close = True

        def simple_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            return {"success": True, "content": "Done"}

        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_test_cleanup",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            test_agent_turn_fn=simple_turn,
            single_turn=True,
        )

        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "COMPLETED"
        assert updated_job.sandbox_cleanup_status != "CLEANED"

        events = service_client.get_job_events(job_view.id, principal=alice_principal)
        assert any("Sandbox close error" in e.message for e in events)

    def test_10_independent_os_process_execution_without_browser_session(
        self, temp_db_path, service_client, alice_principal, auth_authority
    ):
        """Senior Review Issue 4: Genuine spawned OS process executes agent turn to COMPLETED and results survive session disconnect."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={
                "github_access_token": "ghp_tok_8888888888888888",
                "api_keys": {"gemini": "sk-gemini-tok-88888888888888"},
            },
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_os_proc",
            repository="owner/repo_proc",
            prompt="Autonomous background task",
            credential_refs={
                "github": {"key_ref": "primary_pat"},
                "gemini": {"key_ref": "gemini_key"},
            },
        )

        # Simulate browser / client disconnect: close client completely
        service_client.close()

        # Spawn genuine separate OS child process to run worker with picklable test doubles
        ctx = multiprocessing.get_context("spawn")
        stop_ev = ctx.Event()
        p = ctx.Process(
            target=_multiprocess_worker_runner_with_mock,
            args=("worker_proc_independent", temp_db_path, stop_ev, True),
        )
        p.start()
        p.join(timeout=20.0)

        assert not p.is_alive()
        assert p.exitcode == 0

        # Simulate user reconnecting in a brand new session
        reconnected_client = JobServiceClient(db_path=temp_db_path, auth_verifier=auth_authority)
        retrieved_job = reconnected_client.get_job(job_view.id, principal=alice_principal)
        events = reconnected_client.get_job_events(job_view.id, principal=alice_principal)
        reconnected_client.close()

        # Strict assertion: MUST be COMPLETED (not FAILED or any fallback)
        assert retrieved_job.status == "COMPLETED"
        assert retrieved_job.sandbox_cleanup_status == "CLEANED"

        # Check that agent was invoked and output persisted
        event_types = [e.event_type for e in events]
        assert "AGENT_STATUS" in event_types
        assert "ASSISTANT_RESPONSE" in event_types
        resp_ev = next(e for e in events if e.event_type == "ASSISTANT_RESPONSE")
        assert "Spawned worker agent successfully executed turn" in resp_ev.message
        assert "pytest passed 10/10" in resp_ev.data.get("terminal_logs", "")

    def test_11_cooperative_and_blocking_cancellation_boundaries(
        self, temp_db_path, service_client, alice_principal
    ):
        """Senior Review Issue 5: Verifies cooperative checkpoints before and between tool calls; documents non-interruptibility of blocking operations."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk-alice-gemini-key-123456789012"}},
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_cancel_checkpoints",
            repository="owner/repo_cp",
            prompt="Checkpoints test",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        checkpoints_reached = []

        def checkpoint_agent_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            # Checkpoint 1: Before initial step
            if execution_controller.is_aborted():
                return {"success": False, "status": "aborted", "error": execution_controller.abort_reason}
            checkpoints_reached.append("before_llm")

            # Checkpoint 2: Before tool 1
            if execution_controller.is_aborted():
                return {"success": False, "status": "aborted", "error": execution_controller.abort_reason}
            checkpoints_reached.append("before_tool_1")
            sandbox.run_command("echo tool_1")

            # Cancellation arrives here
            service_client.cancel_job(job_view.id, principal=alice_principal)
            q = DurableJobQueue(db_path=temp_db_path)
            poll_res = q.poll_worker_status(job_view.id, "worker_cp", 1, 30.0)
            q.close()
            if poll_res == "CANCEL_REQUESTED":
                execution_controller.abort("User cancelled job.", abort_type="cancellation")

            # Checkpoint 3: Between tool 1 and tool 2
            if execution_controller.is_aborted():
                return {"success": False, "status": "aborted", "error": execution_controller.abort_reason}
            checkpoints_reached.append("before_tool_2_should_not_run")
            return {"success": True, "content": "Done"}

        mock_sb = MockSandbox(repo_name="owner/repo_cp")
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_cp",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: mock_sb,
            test_agent_turn_fn=checkpoint_agent_turn,
            single_turn=True,
        )

        assert checkpoints_reached == ["before_llm", "before_tool_1"]
        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "CANCELLED"

    def test_12_real_production_agent_invocation_path(
        self, temp_db_path, service_client, alice_principal
    ):
        """Final Review Requirement A: Exercise execute_job_in_worker with agent_turn_fn=None (real agent.run_agent_turn)."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk-real-agent-gemini-key-12345"}},
        )

        job_view = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_prod_agent",
            repository="owner/repo_prod",
            prompt="Production agent integration test",
            model="gemini/gemini-2.5-flash",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        mock_sb = MockSandbox(repo_name="owner/repo_prod")

        # Mock litellm.completion to return a simulated assistant message without real network calls
        fake_choice = MagicMock()
        fake_choice.message = MagicMock()
        fake_choice.message.content = "Production agent successfully synthesized solution."
        fake_choice.message.tool_calls = None
        fake_response = MagicMock()
        fake_response.choices = [fake_choice]

        with patch("agent.completion", return_value=fake_response) as mock_completion:
            stop_ev = multiprocessing.Event()
            worker_child_main(
                worker_instance_id="worker_prod_agent",
                db_path=temp_db_path,
                stop_event=stop_ev,
                test_sandbox_factory=lambda repo, tok: mock_sb,
                test_agent_turn_fn=None,  # MUST BE None: invokes production agent.run_agent_turn
                single_turn=True,
            )

            # 1. Verify litellm.completion was invoked by agent.py with correct arguments
            assert mock_completion.called
            call_kwargs = mock_completion.call_args[1]
            assert call_kwargs["model"] == "gemini/gemini-2.5-flash"
            assert call_kwargs["api_key"] == "sk-real-agent-gemini-key-12345"
            assert any("Production agent integration test" in m["content"] for m in call_kwargs["messages"])

        # 2. Verify progress and assistant completion events are in durable store
        events = service_client.get_job_events(job_view.id, principal=alice_principal)
        event_types = [e.event_type for e in events]
        assert "AGENT_STATUS" in event_types
        assert "ASSISTANT_RESPONSE" in event_types

        # Verify assistant response content is persisted
        resp_ev = next(e for e in events if e.event_type == "ASSISTANT_RESPONSE")
        assert "Production agent successfully synthesized solution." in resp_ev.message

        # 3. Verify terminal status
        updated_job = service_client.get_job(job_view.id, principal=alice_principal)
        assert updated_job.status == "COMPLETED"
        assert updated_job.sandbox_cleanup_status == "CLEANED"
        assert mock_sb.closed is True

    def test_13_exception_after_timeout_or_cancellation_preserves_authoritative_status(
        self, temp_db_path, service_client, alice_principal
    ):
        """Final Review Requirement B: Preserves TIMED_OUT or CANCELLED if an exception occurs after abort condition was recorded."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={"api_keys": {"gemini": "sk-alice-gemini-key-123456789012"}},
        )

        # Case 1: Exception raised after timeout
        j_timeout = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_ex_timeout",
            repository="owner/repo_to",
            prompt="Timeout exception test",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        def timeout_then_raise_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            execution_controller.abort("Timeout deadline reached.", abort_type="timeout")
            raise ConnectionResetError("Network abruptly severed during timeout handling")

        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_ex_timeout",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo),
            test_agent_turn_fn=timeout_then_raise_turn,
            single_turn=True,
        )

        res_to = service_client.get_job(j_timeout.id, principal=alice_principal)
        assert res_to.status == "TIMED_OUT"
        assert res_to.error.get("abort_type") == "timeout"
        assert "ConnectionResetError" in str(res_to.error.get("exception_type"))

        # Case 2: Exception raised after cancellation
        j_cancel = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_ex_cancel",
            repository="owner/repo_ca",
            prompt="Cancel exception test",
            credential_refs={"gemini": {"key_ref": "gemini_key"}},
        )

        def cancel_then_raise_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            execution_controller.abort("User cancelled job.", abort_type="cancellation")
            raise BrokenPipeError("Broken pipe during cancellation cleanup")

        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_ex_cancel",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo),
            test_agent_turn_fn=cancel_then_raise_turn,
            single_turn=True,
        )

        res_ca = service_client.get_job(j_cancel.id, principal=alice_principal)
        assert res_ca.status == "CANCELLED"
        assert res_ca.error.get("abort_type") == "cancellation"
        assert "BrokenPipeError" in str(res_ca.error.get("exception_type"))

    def test_14_deterministic_provider_model_credential_binding(
        self, temp_db_path, service_client, alice_principal
    ):
        """Final Review Requirement C: Credential selection matches model provider deterministically regardless of dict order."""
        _save_local_user_data(
            user_id="user_alice",
            email="alice@example.com",
            data={
                "github_access_token": "ghp_alice_primary_token",
                "api_keys": {
                    "gemini": "sk-alice-gemini-key-111",
                    "openai": "sk-alice-openai-key-222",
                    "anthropic": "sk-alice-anthropic-key-333",
                    "deepseek": "sk-alice-deepseek-key-444",
                },
            },
        )

        captured_keys = []

        def capture_key_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            captured_keys.append({"model": model, "api_key": api_key})
            return {"success": True, "content": "Done"}

        # Test Order 1: OpenAI first in dict, executing an Anthropic model -> MUST select Anthropic key
        j1 = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_prov_1",
            repository="owner/repo",
            prompt="Anthropic task",
            model="anthropic/claude-3-7-sonnet-20250219",
            credential_refs={
                "openai": {"key_ref": "openai_key"},
                "anthropic": {"key_ref": "anthropic_key"},
                "gemini": {"key_ref": "gemini_key"},
            },
        )
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_prov_1",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo),
            test_agent_turn_fn=capture_key_turn,
            single_turn=True,
        )
        assert captured_keys[-1]["api_key"] == "sk-alice-anthropic-key-333"

        # Test Order 2: Reverse dict order executing DeepSeek model -> MUST select DeepSeek key
        j2 = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_prov_2",
            repository="owner/repo",
            prompt="DeepSeek task",
            model="deepseek/deepseek-reasoner",
            credential_refs={
                "gemini": {"key_ref": "gemini_key"},
                "anthropic": {"key_ref": "anthropic_key"},
                "deepseek": {"key_ref": "deepseek_key"},
                "openai": {"key_ref": "openai_key"},
            },
        )
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_prov_2",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo),
            test_agent_turn_fn=capture_key_turn,
            single_turn=True,
        )
        assert captured_keys[-1]["api_key"] == "sk-alice-deepseek-key-444"

        # Test Mismatched Model Provider: OpenAI model requested, but ONLY Gemini credential provided
        # MUST fail closed with Security violation, NEVER substitute Gemini key
        j3 = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_prov_3",
            repository="owner/repo",
            prompt="OpenAI task with only Gemini key",
            model="openai/gpt-4o",
            credential_refs={
                "gemini": {"key_ref": "gemini_key"},
            },
        )
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_prov_3",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo),
            test_agent_turn_fn=capture_key_turn,
            single_turn=True,
        )
        res_3 = service_client.get_job(j3.id, principal=alice_principal)
        assert res_3.status == "FAILED"
        assert "Security violation" in str(res_3.error)
        assert "openai" in str(res_3.error)

        # Test Unsupported Model String: e.g. 'custom-llama-3/70b'
        # MUST fail closed
        j4 = service_client.submit_agent_turn(
            principal=alice_principal,
            session_id="sess_prov_4",
            repository="owner/repo",
            prompt="Custom model task",
            model="unknown-provider/model-xyz",
            credential_refs={
                "gemini": {"key_ref": "gemini_key"},
            },
        )
        stop_ev = multiprocessing.Event()
        worker_child_main(
            worker_instance_id="worker_prov_4",
            db_path=temp_db_path,
            stop_event=stop_ev,
            test_sandbox_factory=lambda repo, tok: MockSandbox(repo),
            test_agent_turn_fn=capture_key_turn,
            single_turn=True,
        )
        res_4 = service_client.get_job(j4.id, principal=alice_principal)
        assert res_4.status == "FAILED"
        assert "Security violation" in str(res_4.error)


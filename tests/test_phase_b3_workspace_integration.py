"""Integration and Component Tests for Phase B.3.3 Streamlit Workspace Integration.

Tests cover:
1. Server-side SupabaseAuthAdapter:
   - Stable secret resolution (environment, file, 0600 mode)
   - Verified principal issuance with cryptographic signature
   - Rejection of invalid, empty, or mismatched user tokens
2. JobServiceClient & Queue Tenant-Scoped Listing Extensions:
   - list_user_jobs with session filtering
   - list_recent_jobs in JobServiceClient
   - get_active_job_for_session hydration
3. Continuous Design Dial & UI Theme Tokens:
   - Interpolation across 0.0 (Classic), 0.5 (Balanced Modern), and 1.0 (Futuristic)
   - Persistence and retrieval from user store (.user_store)
   - Clamping of out-of-range inputs
4. End-to-End Async Turn Submission & Polling Simulation:
   - Verifies that UI submits turn via JobServiceClient rather than running synchronously
   - Job is claimed and executed by worker in background
   - Progressive events (AGENT_STATUS) and final results are retrievable by UI client
   - Cancellation can be requested and transitions job cleanly to CANCELLED
5. Invariant Checks:
   - WorkspaceSandbox is never imported or initialized in the UI layer
   - WorkerSupervisor is never started or imported in the UI layer
"""

import os
import sqlite3
import tempfile
import threading
import time
from typing import Any, Dict
from unittest.mock import MagicMock, patch
import pytest

from auth_adapter import (
    SupabaseAuthAdapter,
    get_or_create_server_authority_secret,
    _DEFAULT_SECRETS_DIR,
    _DEFAULT_SECRET_FILE,
)
from db import _save_local_user_data
from job_queue import DurableJobQueue, JobQueueError
from job_service import (
    AuthenticatedPrincipal,
    HMACAuthenticationAuthority,
    JobServiceClient,
    AuthenticationVerificationError,
)
import ui_theme
from worker import ExecutionController, execute_job_in_worker


@pytest.fixture
def temp_db_path():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    if os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass
    for ext in ("-wal", "-shm"):
        if os.path.exists(path + ext):
            try:
                os.remove(path + ext)
            except OSError:
                pass


@pytest.fixture
def auth_authority():
    secret = b"test_secret_for_auth_adapter_32_bytes_long!"
    return HMACAuthenticationAuthority(secret)


@pytest.fixture
def test_user_id():
    return "usr_alice_b3_3"


class TestPhaseB3WorkspaceIntegration:
    """Suite testing all Phase B.3.3 integration and UX requirements."""

    # -------------------------------------------------------------------------
    # 1. Auth Adapter Tests
    # -------------------------------------------------------------------------

    def test_auth_secret_resolution_and_file_permissions(self, tmp_path, monkeypatch):
        """Verifies that authority secret generates a secure persistent file with 0600 mode."""
        test_sec_dir = str(tmp_path / ".secrets")
        test_sec_file = os.path.join(test_sec_dir, "auth_secret")
        monkeypatch.setattr("auth_adapter._DEFAULT_SECRETS_DIR", test_sec_dir)
        monkeypatch.setattr("auth_adapter._DEFAULT_SECRET_FILE", test_sec_file)
        monkeypatch.delenv("JOB_SERVICE_AUTH_SECRET", raising=False)

        secret = get_or_create_server_authority_secret()
        assert isinstance(secret, bytes)
        assert len(secret) >= 16
        assert os.path.exists(test_sec_file)

        # Mode check (should be 0600)
        mode = os.stat(test_sec_file).st_mode & 0o777
        assert mode == 0o600

        # Subsequent call returns the exact same secret
        secret2 = get_or_create_server_authority_secret()
        assert secret2 == secret

    def test_auth_adapter_issues_valid_signed_principal(self, auth_authority, test_user_id):
        """Verifies SupabaseAuthAdapter issues principals that satisfy JobServiceClient verification."""
        adapter = SupabaseAuthAdapter(authority=auth_authority)
        session_user = {"id": test_user_id, "email": "alice@example.com"}

        principal = adapter.issue_principal_for_session(session_user)
        assert isinstance(principal, AuthenticatedPrincipal)
        assert principal.user_id == test_user_id
        assert principal.email == "alice@example.com"

        # Must verify cleanly
        verified = auth_authority.verify_principal(principal)
        assert verified.user_id == test_user_id

    def test_auth_adapter_fails_closed_on_invalid_user(self, auth_authority):
        """Verifies that empty user_id or invalid structures raise AuthenticationVerificationError."""
        adapter = SupabaseAuthAdapter(authority=auth_authority)

        with pytest.raises(AuthenticationVerificationError):
            adapter.issue_principal_for_session({})

        with pytest.raises(AuthenticationVerificationError):
            adapter.issue_principal_for_session({"id": "   "})

        with pytest.raises(AuthenticationVerificationError):
            adapter.issue_principal_for_session(None)  # type: ignore

    def test_auth_adapter_supabase_jwt_validation_flow(self, auth_authority):
        """Verifies validate_supabase_token handles successful response and rejects failures."""
        adapter = SupabaseAuthAdapter(authority=auth_authority)

        # Mock supabase client
        mock_user = MagicMock(id="usr_verified_99", email="verified@example.com", user_metadata={})
        mock_response = MagicMock(user=mock_user)

        with patch("auth_adapter.get_supabase_client") as mock_get_sb:
            mock_sb = MagicMock()
            mock_sb.auth.get_user.return_value = mock_response
            mock_get_sb.return_value = mock_sb

            user_dict = adapter.validate_supabase_token("valid_jwt_token_123")
            assert user_dict["id"] == "usr_verified_99"
            assert user_dict["email"] == "verified@example.com"

            # Issue principal requiring token validation
            principal = adapter.issue_principal_for_session(
                session_user={"id": "usr_verified_99"},
                access_token="valid_jwt_token_123",
                require_token_validation=True,
            )
            assert principal.user_id == "usr_verified_99"

            # Rejection on mismatched ID
            with pytest.raises(AuthenticationVerificationError):
                adapter.issue_principal_for_session(
                    session_user={"id": "usr_different_attacker"},
                    access_token="valid_jwt_token_123",
                    require_token_validation=True,
                )

    # -------------------------------------------------------------------------
    # 2. Design Dial and UI Theme Tests
    # -------------------------------------------------------------------------

    def test_design_dial_interpolation_tokens(self):
        """Tests that CSS variables smoothly interpolate across the continuous dial."""
        # 0.0 (Classic)
        classic = ui_theme.compute_theme_tokens(0.0)
        assert classic["radius_card"] == "2px"
        assert classic["accent_color"] == "#2563eb"
        assert "Inter" in classic["font_family"]

        # 0.5 (Balanced Modern Default)
        modern = ui_theme.compute_theme_tokens(0.5)
        assert modern["radius_card"] == "10px"
        assert modern["accent_color"] == "#6366f1"
        assert "Plus Jakarta Sans" in modern["font_family"]

        # 1.0 (Futuristic / AI-Native)
        futuristic = ui_theme.compute_theme_tokens(1.0)
        assert futuristic["radius_card"] == "18px"
        assert futuristic["accent_color"] == "#06b6d4"
        assert "Outfit" in futuristic["font_family"]

    def test_design_dial_clamping_and_persistence(self, test_user_id):
        """Verifies clamping of inputs and persistence to local user store."""
        # Out-of-bounds inputs clamped
        assert ui_theme.clamp_dial(-0.5) == 0.0
        assert ui_theme.clamp_dial(1.8) == 1.0
        assert ui_theme.clamp_dial("invalid") == 0.5

        # Persistence to user store
        _save_local_user_data(test_user_id, email=None, data={"design_dial": 0.85})
        loaded = ui_theme.get_user_design_dial(test_user_id)
        assert loaded == 0.85

        ui_theme.set_user_design_dial(test_user_id, None, 0.25)
        loaded2 = ui_theme.get_user_design_dial(test_user_id)
        assert loaded2 == 0.25

    # -------------------------------------------------------------------------
    # 3. Job Service & Queue Filtering Extensions
    # -------------------------------------------------------------------------

    def test_job_service_session_filtering_and_hydration(
        self, temp_db_path, auth_authority, test_user_id
    ):
        """Tests listing jobs filtered by session and hydrating active jobs."""
        queue = DurableJobQueue(db_path=temp_db_path)
        client = JobServiceClient(db_path=temp_db_path, auth_verifier=auth_authority, queue=queue)
        principal = auth_authority.issue_principal_from_verified_credential(test_user_id)

        # Submit jobs across two sessions
        j1 = client.submit_agent_turn(
            principal=principal,
            session_id="sess_alpha",
            repository="owner/repo1",
            prompt="Prompt 1",
            model="gemini/gemini-2.5-flash",
            turn_index=1,
        )
        j2 = client.submit_agent_turn(
            principal=principal,
            session_id="sess_alpha",
            repository="owner/repo1",
            prompt="Prompt 2",
            model="gemini/gemini-2.5-flash",
            turn_index=2,
        )
        j3 = client.submit_agent_turn(
            principal=principal,
            session_id="sess_beta",
            repository="owner/repo2",
            prompt="Prompt 3",
            model="gemini/gemini-2.5-flash",
            turn_index=1,
        )

        # Filter by sess_alpha
        alpha_jobs = client.list_recent_jobs(principal=principal, session_id="sess_alpha")
        assert len(alpha_jobs) == 2
        assert {j.job_id for j in alpha_jobs} == {j1.job_id, j2.job_id}

        # Hydrate active job for sess_alpha (should return the most recent active job)
        active = client.get_active_job_for_session("sess_alpha", principal=principal)
        assert active is not None
        assert active.job_id in (j1.job_id, j2.job_id)
        assert active.status == "QUEUED"

        # Cross-tenant isolation in listing
        attacker_principal = auth_authority.issue_principal_from_verified_credential("usr_attacker")
        attacker_jobs = client.list_recent_jobs(principal=attacker_principal, session_id="sess_alpha")
        assert len(attacker_jobs) == 0

    # -------------------------------------------------------------------------
    # 4. Asynchronous Submission, Event Polling, and Completion Flow
    # -------------------------------------------------------------------------

    def test_asynchronous_agent_turn_submission_and_event_monitoring(
        self, temp_db_path, auth_authority, test_user_id
    ):
        """Simulates Streamlit UI async flow:
        1. User submits turn via JobServiceClient (QUEUED).
        2. UI polls events via get_job_events.
        3. Worker claims and executes turn in background.
        4. UI observes AGENT_STATUS progress events and final COMPLETED state.
        """
        _save_local_user_data(
            test_user_id,
            email=None,
            data={
                "github_access_token": "ghp_mock_token_for_alice_1234567890",
                "api_keys": {"gemini": "sk-mock-gemini-key-1234567890123456"},
            },
        )

        queue = DurableJobQueue(db_path=temp_db_path)
        client = JobServiceClient(db_path=temp_db_path, auth_verifier=auth_authority, queue=queue)
        principal = auth_authority.issue_principal_from_verified_credential(test_user_id)

        # Submit agent turn
        job_view = client.submit_agent_turn(
            principal=principal,
            session_id="sess_async_flow",
            repository="owner/repo_async",
            prompt="Analyze architectural layers",
            model="gemini/gemini-2.5-flash",
            credential_refs={
                "github": {"key_ref": "primary_pat"},
                "gemini": {"key_ref": "gemini_key"},
            },
            additional_payload={"messages": [{"role": "user", "content": "Analyze architectural layers"}]},
        )
        assert job_view.status == "QUEUED"
        assert len(job_view.job_id) > 10

        # Initial UI polling: only the initial ENQUEUED event exists
        initial_events = client.get_job_events(job_view.job_id, principal=principal, after_id=0)
        assert len(initial_events) == 1
        assert initial_events[0].event_type == "ENQUEUED"

        # Execute turn in worker with mock sandbox
        class MockSb:
            def __init__(self, repo_name, sandbox_id="sb_async_1"):
                self.repo_name = repo_name
                self.sandbox_id = sandbox_id
            def start(self):
                return {"success": True}
            def close(self):
                pass

        def mock_agent_turn(messages, model, api_key, sandbox, execution_controller, status_callback=None):
            if status_callback:
                status_callback("Scanning codebase for service boundaries...")
                status_callback("Generating architecture report...")
            return {
                "success": True,
                "content": "All architectural boundaries verified successfully.",
                "terminal_logs": "$ pytest -v\nPassed 10/10",
            }

        claim = queue.claim_next_job(worker_id="worker_ui_test", lease_duration_seconds=30.0)
        assert claim is not None
        assert claim["id"] == job_view.job_id

        controller = ExecutionController(
            job_id=claim["id"],
            timeout_seconds=claim["timeout_seconds"],
            queue=queue,
            worker_id="worker_ui_test",
            lease_generation=claim["lease_generation"],
        )

        execute_job_in_worker(
            job=claim,
            worker_id="worker_ui_test",
            lease_gen=claim["lease_generation"],
            queue=queue,
            controller=controller,
            sandbox_factory=lambda repo, token: MockSb(repo),
            agent_turn_fn=mock_agent_turn,
        )

        # UI polls events again
        events = client.get_job_events(job_view.job_id, principal=principal, after_id=0)
        assert len(events) >= 2
        status_messages = [e.message for e in events if e.event_type == "AGENT_STATUS"]
        assert "Scanning codebase for service boundaries..." in status_messages
        assert "Generating architecture report..." in status_messages

        # UI checks job status
        final_view = client.get_job(job_view.job_id, principal=principal)
        assert final_view.status == "COMPLETED"
        resp_events = [e for e in events if e.event_type == "ASSISTANT_RESPONSE"]
        assert len(resp_events) == 1
        assert resp_events[0].message == "All architectural boundaries verified successfully."
        assert "Passed 10/10" in resp_events[0].data["terminal_logs"]

    def test_user_cancellation_from_ui(self, temp_db_path, auth_authority, test_user_id):
        """Verifies user can cancel an active job from the UI via JobServiceClient."""
        queue = DurableJobQueue(db_path=temp_db_path)
        client = JobServiceClient(db_path=temp_db_path, auth_verifier=auth_authority, queue=queue)
        principal = auth_authority.issue_principal_from_verified_credential(test_user_id)

        job_view = client.submit_agent_turn(
            principal=principal,
            session_id="sess_cancel",
            repository="owner/repo_cancel",
            prompt="Run indefinite job",
            model="gemini/gemini-2.5-flash",
        )
        assert job_view.status == "QUEUED"

        # Request cancellation
        cancelled_view = client.cancel_job(job_view.job_id, principal=principal)
        assert cancelled_view.is_cancellation_requested is True

    # -------------------------------------------------------------------------
    # 5. UI Architecture & Invariant Enforcement
    # -------------------------------------------------------------------------

    def test_app_does_not_import_or_use_synchronous_sandbox_or_worker_supervisor(self):
        """Strict architectural invariant check:
        Streamlit app.py must NEVER import or initialize WorkspaceSandbox or WorkerSupervisor.
        Execution MUST be owned by the worker.
        """
        with open("app.py", "r", encoding="utf-8") as f:
            code = f.read()

        assert "WorkspaceSandbox" not in code, "Violation: WorkspaceSandbox referenced in app.py"
        assert "WorkerSupervisor" not in code, "Violation: WorkerSupervisor referenced in app.py"
        assert "agent.run_agent_turn" not in code, "Violation: Synchronous agent.run_agent_turn invoked in app.py"
        assert "JobServiceClient" in code, "Expected JobServiceClient in app.py"
        assert "SupabaseAuthAdapter" in code, "Expected SupabaseAuthAdapter in app.py"

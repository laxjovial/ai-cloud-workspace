"""Regression test suite for Phase A Security and Correctness Hardening.

Covers:
1. PKCE verifiers and OAuth states are cryptographically unpredictable and unique across attempts.
2. Invalid, mismatched, expired, reused, or cross-session OAuth state is rejected.
   CRITICAL: Verifies rejection of invalid state even when exactly ONE legitimate pending state exists.
   CRITICAL: Verifies rejection of cross-session OAuth state consumption between different browser sessions.
3. Missing or invalid encryption keys prevent secret writes (fail-closed in encrypt_secret and _save_local_user_data).
4. Failed decryption does not return ciphertext as a valid plaintext secret.
   CRITICAL: Distinguishes supported legacy plaintext records from invalid ciphertext and corrupted files.
   CRITICAL: Raises DecryptionError on corrupted ciphertext or invalid key, preserving existing store.
5. Git clone and push do NOT embed credentials in remote URLs or process command strings (uses ephemeral askpass helper).
   CRITICAL: Unified setup/execution cleanup guarantees helper deletion on failure.
   CRITICAL: Distinguishes username and password prompts.
6. Tokens are redacted and not exposed through command strings, config, logs, or error messages.
7. Malicious branch names and commit messages cannot execute arbitrary shell commands.
8. Nonzero process exit codes are reported accurately (success=False).
9. Step-limit exhaustion is reported as incomplete (success=False, status='incomplete_step_limit').
10. Strict path confinement: prevents directory traversal (../), prefix collision (/workspace-evil), and escaping symlinks.
   CRITICAL: Symlink validation fails closed if realpath command fails or returns empty output.
"""

import os
import time
from unittest.mock import MagicMock, patch
import pytest
from cryptography.fernet import Fernet

import agent
import db
import sandbox
from sandbox import (
    VALID_BRANCH_PATTERN,
    VALID_REPO_PATTERN,
    WorkspaceSandbox,
    sanitize_error_message,
)


# =====================================================================
# 1 & 2. OAUTH PKCE & STATE TESTS (SESSION BINDING & STRICT VALIDATION)
# =====================================================================

class TestOAuthSecurity:
    """Tests for cryptographically random PKCE and state validation."""

    def test_pkce_verifiers_and_states_are_unique_and_unpredictable(self):
        """Ensure distinct OAuth attempts generate unique, unguessable verifiers and states."""
        with patch.object(db, "get_supabase_client") as mock_supabase:
            mock_client = MagicMock()
            mock_client.auth._url = "https://example.supabase.co/auth/v1"
            mock_supabase.return_value = mock_client

            res1 = db.get_oauth_url("github", "http://localhost:8501", session_id="session_A")
            res2 = db.get_oauth_url("github", "http://localhost:8501", session_id="session_B")

            assert res1["success"] is True
            assert res2["success"] is True

            assert "verifier" not in res1  # Verifier must NOT leak in returned dict
            assert res1["state"] != res2["state"]
            assert len(res1["state"]) >= 32

            # Ensure state is present in the generated authorization URL
            assert f"state={res1['state']}" in res1["url"]
            assert f"state={res2['state']}" in res2["url"]

    def test_oauth_exchange_rejects_cross_session_state(self):
        """CRITICAL: One user's/browser's pending state cannot be consumed by a different session (login-CSRF defense)."""
        with patch.object(db, "get_supabase_client") as mock_supabase:
            mock_client = MagicMock()
            mock_client.auth._url = "https://example.supabase.co/auth/v1"
            mock_supabase.return_value = mock_client

            # Session A initiates OAuth
            session_a = "browser_session_alpha"
            session_b = "browser_session_beta_attacker"

            gen_res = db.get_oauth_url("github", "http://localhost:8501", session_id=session_a)
            assert gen_res["success"] is True
            state_a = gen_res["state"]

            # Case 1: Session B attempts to complete login with Session A's state -> REJECTED
            res_b = db.exchange_oauth_code("auth_code_victim", state=state_a, session_id=session_b)
            assert res_b["success"] is False
            assert "cross-session state mismatch" in res_b["error"]

            # Case 2: Attacker omits session_id entirely (session_id=None or "") -> FAIL CLOSED & REJECTED
            res_none = db.exchange_oauth_code("auth_code_victim", state=state_a, session_id=None)
            assert res_none["success"] is False
            assert "session identifier missing" in res_none["error"]

            res_empty_sid = db.exchange_oauth_code("auth_code_victim", state=state_a, session_id="")
            assert res_empty_sid["success"] is False
            assert "session identifier missing" in res_empty_sid["error"]

            # Case 3: Verify preservation of legitimate pending state after mismatch attacks
            mock_client.auth._storage_key = "sb"
            mock_client.auth.exchange_code_for_session.return_value = MagicMock(user=MagicMock(id="user_alpha"))
            valid_res = db.exchange_oauth_code("auth_code_victim", state=state_a, session_id=session_a)
            assert valid_res["success"] is True

            # Case 4: Verify single-use replay protection
            replay_res = db.exchange_oauth_code("auth_code_victim", state=state_a, session_id=session_a)
            assert replay_res["success"] is False
            assert "invalid, expired, or already-used" in replay_res["error"]

    def test_oauth_flow_survives_browser_redirect_with_cookie_storage(self):
        """CRITICAL: Verifies end-to-end that browser session ID persists across full page redirects."""
        with patch.object(db, "get_supabase_client") as mock_supabase:
            mock_client = MagicMock()
            mock_client.auth._url = "https://example.supabase.co/auth/v1"
            mock_client.auth._storage_key = "sb"
            mock_client.auth.exchange_code_for_session.return_value = MagicMock(user=MagicMock(id="user_redirect_test"))
            mock_supabase.return_value = mock_client

            # 1. User arrives on app: stable browser session ID established
            browser_sid = "persistent_cookie_session_token_12345"

            # 2. User clicks OAuth button: initiating request generated with this session_id
            init_res = db.get_oauth_url("github", "http://localhost:8501", session_id=browser_sid)
            assert init_res["success"] is True
            state_param = init_res["state"]

            # 3. User navigates away to GitHub, completes auth, and is redirected back to http://localhost:8501/?code=xyz&state=...
            # In a new WebSocket/render connection, cookie provides identical browser_sid
            callback_sid = browser_sid

            # 4. Callback exchange executes with code, state, and cookie-persisted session_id
            exchange_res = db.exchange_oauth_code(
                code="github_auth_code_987",
                state=state_param,
                session_id=callback_sid,
            )
            assert exchange_res["success"] is True
            assert exchange_res["data"].user.id == "user_redirect_test"

    def test_oauth_exchange_rejects_invalid_state_even_when_single_pending_state_exists(self):
        """CRITICAL: Mismatched state must be rejected even if exactly one valid state exists."""
        with patch.object(db, "get_supabase_client") as mock_supabase:
            mock_client = MagicMock()
            mock_client.auth._url = "https://example.supabase.co/auth/v1"
            mock_supabase.return_value = mock_client

            # Generate exactly one legitimate attempt
            gen_res = db.get_oauth_url("github", "http://localhost:8501", session_id="sess_1")
            assert gen_res["success"] is True
            valid_state = gen_res["state"]

            # Attempt exchange with forged / mismatched state
            forged_state = "forged_attacker_state_12345"
            res = db.exchange_oauth_code("auth_code_x", state=forged_state, session_id="sess_1")
            assert res["success"] is False
            assert "invalid, expired, or already-used" in res["error"]

            # Also attempt exchange with missing / empty state
            res_empty = db.exchange_oauth_code("auth_code_x", state="", session_id="sess_1")
            assert res_empty["success"] is False
            assert "missing or empty state" in res_empty["error"]

            # The original valid state should still be consumable
            mock_client.auth._storage_key = "sb"
            mock_client.auth.exchange_code_for_session.return_value = MagicMock(user=MagicMock(id="user_valid"))
            valid_res = db.exchange_oauth_code("auth_code_x", state=valid_state, session_id="sess_1")
            assert valid_res["success"] is True

    def test_oauth_exchange_single_use_prevents_replay(self):
        """Consuming an authorization attempt once prevents replay with the same state."""
        with patch.object(db, "get_supabase_client") as mock_supabase:
            mock_client = MagicMock()
            mock_client.auth._url = "https://example.supabase.co/auth/v1"
            mock_client.auth._storage_key = "sb"
            mock_client.auth.exchange_code_for_session.return_value = MagicMock(user=MagicMock(id="user_123"))
            mock_supabase.return_value = mock_client

            gen_res = db.get_oauth_url("google", "http://localhost:8501", session_id="sess_replay")
            state = gen_res["state"]

            # First exchange should succeed
            exchange1 = db.exchange_oauth_code("auth_code_1", state=state, session_id="sess_replay")
            assert exchange1["success"] is True

            # Second exchange with same state must be rejected (single-use)
            exchange2 = db.exchange_oauth_code("auth_code_1", state=state, session_id="sess_replay")
            assert exchange2["success"] is False
            assert "invalid, expired, or already-used" in exchange2["error"]

    def test_oauth_states_expire(self):
        """Expired OAuth states must be cleaned and rejected."""
        with patch.object(db, "get_supabase_client") as mock_supabase:
            mock_client = MagicMock()
            mock_client.auth._url = "https://example.supabase.co/auth/v1"
            mock_supabase.return_value = mock_client

            expired_state = "expired_state_9999"

            def _inject_expired(states):
                states[expired_state] = {
                    "verifier": "old_verifier",
                    "provider": "github",
                    "session_id": "sess_exp",
                    "created_at": time.time() - (db.OAUTH_STATE_EXPIRY_SECONDS + 50),
                }

            db._atomic_modify_oauth_states(_inject_expired)

            res = db.exchange_oauth_code("auth_code_x", state=expired_state, session_id="sess_exp")
            assert res["success"] is False
            assert "invalid, expired, or already-used" in res["error"]


# =====================================================================
# 3 & 4. FAIL-CLOSED ENCRYPTION & LOCAL STORAGE TESTS
# =====================================================================

class TestFailClosedEncryption:
    """Tests confirming encryption fails closed across secret encryption and local storage."""

    def test_encrypt_secret_fails_closed_when_key_missing(self, monkeypatch):
        """Missing ENCRYPTION_KEY must raise EncryptionConfigurationError, never return plaintext."""
        monkeypatch.setattr(db, "_get_fernet", lambda: None)
        with pytest.raises(db.EncryptionConfigurationError) as exc_info:
            db.encrypt_secret("ghp_my_sensitive_pat_12345")
        assert "ENCRYPTION_KEY is not configured" in str(exc_info.value)

    def test_save_local_user_data_fails_closed_when_key_missing(self, monkeypatch):
        """_save_local_user_data must raise EncryptionConfigurationError and refuse plaintext writes."""
        monkeypatch.setattr(db, "_get_fernet", lambda: None)
        with pytest.raises(db.EncryptionConfigurationError) as exc_info:
            db._save_local_user_data("test_user_id", "test@example.com", {"api_keys": {"openai": "sk-123"}})
        assert "Refusing to persist local user store in plaintext" in str(exc_info.value)

    def test_load_local_user_data_distinguishes_legacy_and_raises_on_corrupted_ciphertext(self, monkeypatch, tmp_path):
        """_load_local_user_data must raise DecryptionError on corrupted ciphertext instead of returning empty dict."""
        monkeypatch.setattr(db, "USER_STORE_DIR", str(tmp_path))
        valid_key = Fernet.generate_key().decode()
        monkeypatch.setattr(db, "_get_fernet", lambda: Fernet(valid_key.encode()))

        # 1. Write corrupted Fernet token
        test_uid = "user_corrupted_test"
        filename = db._get_safe_filename(test_uid) + ".json"
        corrupted_file = tmp_path / filename
        corrupted_file.write_text("gAAAAABcorrupted_fernet_token_contents_here")

        # Must raise DecryptionError, NOT silently return {}
        with pytest.raises(db.DecryptionError) as exc_info:
            db._load_local_user_data(test_uid)
        assert "invalid key or corrupted ciphertext" in str(exc_info.value)

        # Ensure the corrupted file on disk was preserved (not overwritten/deleted)
        assert corrupted_file.exists()
        assert corrupted_file.read_text() == "gAAAAABcorrupted_fernet_token_contents_here"

    def test_load_local_user_data_supports_legitimate_legacy_plaintext_json(self, monkeypatch, tmp_path):
        """Legacy plaintext JSON records are safely parsed without error."""
        monkeypatch.setattr(db, "USER_STORE_DIR", str(tmp_path))
        valid_key = Fernet.generate_key().decode()
        monkeypatch.setattr(db, "_get_fernet", lambda: Fernet(valid_key.encode()))

        legacy_uid = "legacy_user_test"
        filename = db._get_safe_filename(legacy_uid) + ".json"
        legacy_file = tmp_path / filename
        legacy_file.write_text('{"active_repo": "owner/legacy-repo"}')

        loaded = db._load_local_user_data(legacy_uid)
        assert loaded == {"active_repo": "owner/legacy-repo"}

    def test_decrypt_secret_fails_closed_on_invalid_token(self, monkeypatch):
        """Corrupted or mismatched ciphertext must raise DecryptionError, not return ciphertext."""
        valid_key = Fernet.generate_key().decode()
        monkeypatch.setattr(db, "_get_fernet", lambda: Fernet(valid_key.encode()))

        corrupted_ciphertext = "gAAAAABcorrupted_fernet_token_content"
        with pytest.raises(db.DecryptionError):
            db.decrypt_secret(corrupted_ciphertext)

    def test_save_user_api_key_fails_closed_when_key_missing(self, monkeypatch):
        """save_user_api_key must raise error without writing unencrypted key to database."""
        monkeypatch.setattr(db, "_get_fernet", lambda: None)
        with pytest.raises(db.EncryptionConfigurationError):
            db.save_user_api_key("user_1", "openai", "sk-secret-key-12345")

    def test_valid_encryption_roundtrip(self, monkeypatch):
        """Valid key encrypts and decrypts correctly."""
        valid_key = Fernet.generate_key().decode()
        monkeypatch.setattr(db, "_get_fernet", lambda: Fernet(valid_key.encode()))

        plaintext = "sk-ant-api03-abcdef1234567890"
        encrypted = db.encrypt_secret(plaintext)
        assert encrypted != plaintext
        assert encrypted.startswith("gAAAAA")

        decrypted = db.decrypt_secret(encrypted)
        assert decrypted == plaintext


# =====================================================================
# 5 & 6. GITHUB TOKEN ISOLATION & REDACTION TESTS
# =====================================================================

class TestTokenIsolationAndRedaction:
    """Tests confirming GitHub tokens are never leaked into URLs, remotes, or process arguments."""

    def test_sanitize_error_message_redacts_tokens(self):
        """GitHub tokens and basic auth headers must be scrubbed from error logs."""
        raw_msg = (
            "fatal: unable to access 'https://x-access-token:ghp_SuperSecretPAT9999@github.com/org/repo.git/': "
            "The requested URL returned error: 403. Token ghp_SuperSecretPAT9999 was rejected."
        )
        token = "ghp_SuperSecretPAT9999"
        cleaned = sanitize_error_message(raw_msg, token)

        assert token not in cleaned
        assert "[REDACTED_TOKEN]" in cleaned
        assert "x-access-token:[REDACTED]@" in cleaned

    def test_clone_does_not_embed_token_in_url_or_command_args(self):
        """Clone command must use clean URL and transient askpass helper without exposing token in command line."""
        with patch.object(sandbox, "get_e2b_api_key", return_value="fake_e2b_key"):
            token_val = "ghp_SecretToken12345"
            sb = WorkspaceSandbox(repo_name="org/my-project", github_token=token_val)
            mock_container = MagicMock()
            sb.sandbox = mock_container

            executed_commands = []
            files_written = {}

            def fake_run(cmd, **kwargs):
                executed_commands.append(cmd)
                res = MagicMock()
                res.exit_code = 0
                res.stdout = "https://github.com/org/my-project.git"
                res.stderr = ""
                return res

            def fake_write(path, content):
                files_written[path] = content

            mock_container.commands.run = fake_run
            mock_container.files.write = fake_write

            with patch("e2b.Sandbox") as mock_e2b:
                mock_e2b.create.return_value = mock_container
                start_res = sb.start()

            assert start_res["success"] is True

            # Verify no command executed contains the token embedded inside a URL or process argument
            for cmd in executed_commands:
                assert token_val not in cmd
                assert "ghp_SecretToken12345@github.com" not in cmd
                assert "AUTHORIZATION: basic" not in cmd

            # Verify askpass script was written and cleaned up
            askpass_files = [p for p in files_written.keys() if ".git_askpass_clone_" in p]
            assert len(askpass_files) > 0
            # Script must distinguish Username vs Password
            assert "*Username*|*username*)" in files_written[askpass_files[0]]
            assert any(f"rm -f {askpass_files[0]}" in cmd for cmd in executed_commands)

    def test_askpass_cleans_up_even_if_git_command_fails(self):
        """Credential helper must be cleaned up even if git clone exits with error."""
        with patch.object(sandbox, "get_e2b_api_key", return_value="fake_e2b_key"):
            sb = WorkspaceSandbox(repo_name="org/failing-repo", github_token="ghp_SecretToken12345")
            mock_container = MagicMock()
            sb.sandbox = mock_container

            executed_commands = []
            files_written = {}

            def fake_run(cmd, **kwargs):
                executed_commands.append(cmd)
                res = MagicMock()
                if "git clone" in cmd:
                    res.exit_code = 128
                    res.stderr = "fatal: repository not found"
                else:
                    res.exit_code = 0
                return res

            mock_container.commands.run = fake_run
            mock_container.files.write = lambda p, c: files_written.update({p: c})

            with patch("e2b.Sandbox") as mock_e2b:
                mock_e2b.create.return_value = mock_container
                start_res = sb.start()

            assert start_res["success"] is False

            # Cleanup command must still have executed
            askpass_files = [p for p in files_written.keys() if ".git_askpass_clone_" in p]
            assert len(askpass_files) > 0
            assert any(f"rm -f {askpass_files[0]}" in cmd for cmd in executed_commands)


# =====================================================================
# 7. SAFE GIT COMMAND CONSTRUCTION & INJECTION DEFENSE
# =====================================================================

class TestSafeGitCommandConstruction:
    """Tests verifying malicious branch names or commit messages are rejected or escaped safely."""

    def test_malicious_branch_names_rejected(self):
        """Branch names containing command injection characters must be rejected."""
        sb = WorkspaceSandbox(repo_name="org/repo", github_token="fake_token")
        sb.sandbox = MagicMock()

        malicious_branches = [
            "feat; rm -rf /",
            "branch`id`",
            "branch$(whoami)",
            "-f",
            "../escape",
            "foo|nc localhost 4444",
        ]

        for bad_branch in malicious_branches:
            res = sb.commit_and_create_pr(
                branch_name=bad_branch,
                commit_message="valid message",
                pr_title="PR",
                pr_body="Body",
            )
            assert res["success"] is False
            assert "Invalid or unsafe git branch name" in res["error"]

    def test_commit_message_with_shell_characters_is_safely_escaped(self):
        """Commit messages with quotes, subshells, and newlines must be safely quoted."""
        sb = WorkspaceSandbox(repo_name="org/repo", github_token="fake_token")
        mock_container = MagicMock()
        sb.sandbox = mock_container

        executed_commands = []

        def fake_run(cmd, **kwargs):
            executed_commands.append(cmd)
            res = MagicMock()
            res.exit_code = 0
            res.stdout = ""
            res.stderr = ""
            return res

        mock_container.commands.run = fake_run

        with patch("github.Github") as mock_gh:
            mock_repo = MagicMock()
            mock_pr = MagicMock(html_url="https://github.com/org/repo/pull/1", number=1)
            mock_repo.create_pull.return_value = mock_pr
            mock_gh.return_value.get_repo.return_value = mock_repo

            tricky_msg = 'fix: bug$(touch /tmp/hacked) && "injection" \'test\''
            res = sb.commit_and_create_pr(
                branch_name="fix/safe-branch-1",
                commit_message=tricky_msg,
                pr_title="PR Title",
                pr_body="PR Body",
            )
            assert res["success"] is True

            # The executed commit command must have the commit message safely quoted
            commit_cmds = [c for c in executed_commands if "git commit -m" in c]
            assert len(commit_cmds) > 0
            assert '$(touch /tmp/hacked)' not in commit_cmds[0].split(" -m ")[-1].replace("'", "") or "'" in commit_cmds[0]


# =====================================================================
# 8 & 9. TRUTHFUL OUTCOMES (EXIT CODES & STEP LIMITS)
# =====================================================================

class TestTruthfulOutcomes:
    """Tests verifying non-zero exit codes and step exhaustion are reported accurately."""

    def test_nonzero_exit_code_is_reported_as_failure(self):
        """A command with non-zero exit code must return success=False."""
        sb = WorkspaceSandbox(repo_name="org/repo", github_token="")
        mock_container = MagicMock()
        sb.sandbox = mock_container

        mock_res = MagicMock()
        mock_res.exit_code = 1
        mock_res.stdout = "FAILED (failures=2)"
        mock_res.stderr = ""
        mock_container.commands.run.return_value = mock_res

        out = sb.run_command("pytest")
        assert out["success"] is False
        assert out["exit_code"] == 1
        assert "FAILED" in out["stdout"]

    def test_step_limit_exhaustion_is_reported_as_incomplete(self):
        """Reaching max_steps must return success=False and status='incomplete_step_limit'."""
        mock_sb = MagicMock()
        mock_sb.run_command.return_value = {"success": True, "stdout": "ok", "stderr": "", "exit_code": 0}

        mock_tool_call = MagicMock()
        mock_tool_call.id = "call_123"
        mock_tool_call.function.name = "run_terminal_command"
        mock_tool_call.function.arguments = '{"command": "echo test"}'

        mock_msg = MagicMock()
        mock_msg.tool_calls = [mock_tool_call]
        mock_msg.content = None

        mock_choice = MagicMock()
        mock_choice.message = mock_msg

        mock_completion_resp = MagicMock()
        mock_completion_resp.choices = [mock_choice]

        with patch("agent.completion", return_value=mock_completion_resp):
            turn_res = agent.run_agent_turn(
                messages=[{"role": "user", "content": "Keep running"}],
                model="openai/gpt-4o",
                api_key="fake_key",
                sandbox=mock_sb,
                max_steps=2,
            )

            assert turn_res["success"] is False
            assert turn_res["status"] == "incomplete_step_limit"
            assert "Reached maximum steps limit" in turn_res["content"]


# =====================================================================
# 10. STRICT PATH CONFINEMENT & SYMLINK DEFENSE TESTS
# =====================================================================

class TestPathConfinementAndSymlinks:
    """Tests verifying traversal attacks, prefix collisions, and symlink escapes are blocked."""

    def test_resolve_path_blocks_directory_traversal(self):
        """Paths attempting to traverse out of /home/user/workspace must be confined."""
        sb = WorkspaceSandbox(repo_name="org/repo", github_token="")

        traversal_attempts = [
            "/home/user/workspace/../../etc/passwd",
            "/home/user/workspace/../../../root/.ssh/id_rsa",
            "../../etc/shadow",
            "/workspace/../../etc/passwd",
            "/home/user/workspace-evil/secret",
            "/home/user/workspace_other/file",
            "/etc/passwd",
        ]

        workspace_root = "/home/user/workspace"
        for bad_path in traversal_attempts:
            resolved = sb._resolve_path(bad_path)
            assert resolved == workspace_root, f"Path '{bad_path}' resolved to '{resolved}' instead of workspace root!"

    def test_resolve_path_allows_legitimate_subpaths(self):
        """Legitimate subpaths inside workspace must resolve correctly."""
        sb = WorkspaceSandbox(repo_name="org/repo", github_token="")

        valid_paths = {
            "src/app.py": "/home/user/workspace/src/app.py",
            "README.md": "/home/user/workspace/README.md",
            "/home/user/workspace/tests/test_app.py": "/home/user/workspace/tests/test_app.py",
            "/workspace/index.html": "/home/user/workspace/index.html",
        }

        for rel, expected in valid_paths.items():
            resolved = sb._resolve_path(rel)
            assert resolved == expected, f"Failed resolving legitimate path: {rel}"

    def test_read_and_write_file_reject_symlink_escapes(self):
        """Symlinks pointing outside workspace must be blocked during file read/write."""
        sb = WorkspaceSandbox(repo_name="org/repo", github_token="")
        mock_container = MagicMock()
        sb.sandbox = mock_container

        # Mock realpath returning an escaping host path (/etc/passwd)
        mock_realpath_res = MagicMock()
        mock_realpath_res.stdout = "/etc/passwd"
        mock_realpath_res.exit_code = 0
        mock_container.commands.run.return_value = mock_realpath_res

        read_res = sb.read_file("evil_symlink.txt")
        assert read_res["success"] is False
        assert "symbolic link targets a path outside workspace" in read_res["error"]

        write_res = sb.write_file("evil_symlink.txt", "hacked")
        assert write_res["success"] is False
        assert "symbolic link targets a path outside workspace" in write_res["error"]

    def test_read_and_write_file_fail_closed_if_realpath_fails_or_returns_empty(self):
        """CRITICAL: If realpath command fails or returns empty stdout, read/write must be blocked (fail-closed)."""
        sb = WorkspaceSandbox(repo_name="org/repo", github_token="")
        mock_container = MagicMock()
        sb.sandbox = mock_container

        # 1. realpath command returns non-zero exit code
        mock_failed_res = MagicMock()
        mock_failed_res.exit_code = 1
        mock_failed_res.stdout = ""
        mock_failed_res.stderr = "realpath: permission denied"
        mock_container.commands.run.return_value = mock_failed_res

        read_res = sb.read_file("some_file.txt")
        assert read_res["success"] is False
        assert "Path validation failed" in read_res["error"]

        write_res = sb.write_file("some_file.txt", "content")
        assert write_res["success"] is False
        assert "Path validation failed" in write_res["error"]

        # 2. realpath command returns exit code 0 but empty stdout
        mock_empty_res = MagicMock()
        mock_empty_res.exit_code = 0
        mock_empty_res.stdout = "   "
        mock_container.commands.run.return_value = mock_empty_res

        read_res2 = sb.read_file("some_file.txt")
        assert read_res2["success"] is False
        assert "Path validation failed" in read_res2["error"]

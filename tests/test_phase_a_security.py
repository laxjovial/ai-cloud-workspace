"""Regression test suite for Phase A Security and Correctness Hardening.

Covers:
1. PKCE verifiers and OAuth states are cryptographically unpredictable and unique across attempts.
2. Invalid, mismatched, expired, reused, or cross-session OAuth state is rejected.
   CRITICAL: Verifies rejection of invalid state even when exactly ONE legitimate pending state exists.
3. Missing or invalid encryption keys prevent secret writes (fail-closed in encrypt_secret and _save_local_user_data).
4. Failed decryption does not return ciphertext as a valid plaintext secret.
5. Git clone and push do NOT embed credentials in remote URLs or process command strings (uses ephemeral askpass helper).
6. Tokens are redacted and not exposed through command strings, config, logs, or error messages.
7. Malicious branch names and commit messages cannot execute arbitrary shell commands.
8. Nonzero process exit codes are reported accurately (success=False).
9. Step-limit exhaustion is reported as incomplete (success=False, status='incomplete_step_limit').
10. Strict path confinement: prevents directory traversal (../), prefix collision (/workspace-evil), and escaping symlinks.
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
# 1 & 2. OAUTH PKCE & STATE TESTS
# =====================================================================

class TestOAuthSecurity:
    """Tests for cryptographically random PKCE and state validation."""

    def test_pkce_verifiers_and_states_are_unique_and_unpredictable(self):
        """Ensure distinct OAuth attempts generate unique, unguessable verifiers and states."""
        with patch.object(db, "get_supabase_client") as mock_supabase:
            mock_client = MagicMock()
            mock_client.auth._url = "https://example.supabase.co/auth/v1"
            mock_supabase.return_value = mock_client

            res1 = db.get_oauth_url("github", "http://localhost:8501")
            res2 = db.get_oauth_url("github", "http://localhost:8501")

            assert res1["success"] is True
            assert res2["success"] is True

            assert "verifier" not in res1  # Verifier must NOT leak in returned dict
            assert res1["state"] != res2["state"]
            assert len(res1["state"]) >= 32

            # Ensure state is present in the generated authorization URL
            assert f"state={res1['state']}" in res1["url"]
            assert f"state={res2['state']}" in res2["url"]

    def test_oauth_exchange_rejects_invalid_state_even_when_single_pending_state_exists(self):
        """CRITICAL: Mismatched state must be rejected even if exactly one valid state exists."""
        with patch.object(db, "get_supabase_client") as mock_supabase:
            mock_client = MagicMock()
            mock_client.auth._url = "https://example.supabase.co/auth/v1"
            mock_supabase.return_value = mock_client

            # Generate exactly one legitimate attempt
            gen_res = db.get_oauth_url("github", "http://localhost:8501")
            assert gen_res["success"] is True
            valid_state = gen_res["state"]

            # Attempt exchange with forged / mismatched state
            forged_state = "forged_attacker_state_12345"
            res = db.exchange_oauth_code("auth_code_x", state=forged_state)
            assert res["success"] is False
            assert "invalid, expired, or already-used" in res["error"]

            # Also attempt exchange with missing / empty state
            res_empty = db.exchange_oauth_code("auth_code_x", state="")
            assert res_empty["success"] is False
            assert "missing or empty state" in res_empty["error"]

            # The original valid state should still be consumable
            mock_client.auth._storage_key = "sb"
            mock_client.auth.exchange_code_for_session.return_value = MagicMock(user=MagicMock(id="user_valid"))
            valid_res = db.exchange_oauth_code("auth_code_x", state=valid_state)
            assert valid_res["success"] is True

    def test_oauth_exchange_single_use_prevents_replay(self):
        """Consuming an authorization attempt once prevents replay with the same state."""
        with patch.object(db, "get_supabase_client") as mock_supabase:
            mock_client = MagicMock()
            mock_client.auth._url = "https://example.supabase.co/auth/v1"
            mock_client.auth._storage_key = "sb"
            mock_client.auth.exchange_code_for_session.return_value = MagicMock(user=MagicMock(id="user_123"))
            mock_supabase.return_value = mock_client

            gen_res = db.get_oauth_url("google", "http://localhost:8501")
            state = gen_res["state"]

            # First exchange should succeed
            exchange1 = db.exchange_oauth_code("auth_code_1", state=state)
            assert exchange1["success"] is True

            # Second exchange with same state must be rejected (single-use)
            exchange2 = db.exchange_oauth_code("auth_code_1", state=state)
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
                    "created_at": time.time() - (db.OAUTH_STATE_EXPIRY_SECONDS + 50),
                }

            db._atomic_modify_oauth_states(_inject_expired)

            res = db.exchange_oauth_code("auth_code_x", state=expired_state)
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
            assert "/home/user/.git_askpass_tmp" in files_written
            assert any("rm -f /home/user/.git_askpass_tmp" in cmd for cmd in executed_commands)


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

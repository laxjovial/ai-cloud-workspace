"""E2B Cloud Sandbox Engine for running terminal commands, tests, git operations, and live web previews."""

import os
import re
import shlex
from typing import Any, Dict, Optional
import streamlit as st


def get_e2b_api_key() -> str:
    """Retrieves the E2B API Key from Streamlit secrets or environment."""
    key = ""
    try:
        key = st.secrets.get("E2B_API_KEY", "")
    except Exception:
        pass
    return key or os.getenv("E2B_API_KEY", "")


WORKSPACE_DIR = "/home/user/workspace"
VALID_REPO_PATTERN = re.compile(r"^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+$")
VALID_BRANCH_PATTERN = re.compile(r"^[a-zA-Z0-9_./-]+$")


def sanitize_error_message(msg: str, token: str = "") -> str:
    """Redacts GitHub tokens or credentials from error strings."""
    if not msg:
        return ""
    cleaned = msg
    if token and len(token) > 4:
        cleaned = cleaned.replace(token, "[REDACTED_TOKEN]")
    # Redact common token patterns (ghp_, gho_, ghu_, etc.)
    cleaned = re.sub(r"gh[pousr]_[A-Za-z0-9_]+", "[REDACTED_TOKEN]", cleaned)
    cleaned = re.sub(r"x-access-token:[^@]+@", "x-access-token:[REDACTED]@", cleaned)
    return cleaned


class WorkspaceSandbox:
    """Manages an ephemeral cloud Linux container for a user's repository."""

    def __init__(self, repo_name: str, github_token: str):
        self.repo_name = repo_name.strip()
        self.github_token = (github_token or "").strip()
        self.api_key = get_e2b_api_key()
        self.sandbox: Optional[Any] = None

    def start(self) -> Dict[str, Any]:
        """Boots the E2B sandbox and clones the repository into /home/user/workspace."""
        if not self.api_key:
            return {
                "success": False,
                "error": "E2B_API_KEY not found in secrets or environment. Get a free key at https://e2b.dev",
            }

        # Validate repo name format (owner/repo)
        if not VALID_REPO_PATTERN.match(self.repo_name):
            return {
                "success": False,
                "error": f"Invalid repository name format: '{self.repo_name}'. Expected 'owner/repo'.",
            }

        try:
            from e2b import Sandbox

            # Initialize sandbox using modern E2B API
            if hasattr(Sandbox, "create"):
                self.sandbox = Sandbox.create(api_key=self.api_key)
            else:
                self.sandbox = Sandbox(api_key=self.api_key)

            # Public repository URL without embedded credentials
            clean_repo_url = f"https://github.com/{self.repo_name}.git"

            # Clean destination directory
            self.sandbox.commands.run(f"rm -rf {WORKSPACE_DIR}")

            # Clone using Git client or temporary git credential helper
            # Token is never embedded in clone URL or persistent origin URL
            if self.github_token:
                # Use git -c http.extraHeader to pass authorization for clone without writing to .git/config
                import base64
                auth_val = base64.b64encode(f"x-access-token:{self.github_token}".encode()).decode()
                clone_cmd = (
                    f"git -c http.extraHeader=\"AUTHORIZATION: basic {auth_val}\" "
                    f"clone --depth 50 {clean_repo_url} {WORKSPACE_DIR}"
                )
            else:
                clone_cmd = f"git clone --depth 50 {clean_repo_url} {WORKSPACE_DIR}"

            try:
                result = self.sandbox.commands.run(clone_cmd)
                exit_code = getattr(result, "exit_code", 0)
                if exit_code != 0:
                    raw_err = getattr(result, 'stderr', '') or getattr(result, 'stdout', '')
                    return {
                        "success": False,
                        "error": f"Failed to clone repository: {sanitize_error_message(raw_err, self.github_token)}",
                    }
            except Exception as cmd_err:
                err_msg = str(cmd_err)
                if hasattr(cmd_err, "stderr") and getattr(cmd_err, "stderr"):
                    err_msg = getattr(cmd_err, "stderr")
                elif hasattr(cmd_err, "stdout") and getattr(cmd_err, "stdout"):
                    err_msg = getattr(cmd_err, "stdout")
                return {
                    "success": False,
                    "error": f"Failed to clone repository: {sanitize_error_message(err_msg, self.github_token)}",
                }

            # Double-check that .git/config does NOT contain credentials
            verify_res = self.sandbox.commands.run(f"git -C {WORKSPACE_DIR} remote get-url origin")
            origin_url = getattr(verify_res, "stdout", "").strip()
            if self.github_token and self.github_token in origin_url:
                # Sanitize immediate remote URL
                self.sandbox.commands.run(f"git -C {WORKSPACE_DIR} remote set-url origin {clean_repo_url}")

            return {
                "success": True,
                "message": f"Successfully cloned {self.repo_name} into sandbox environment.",
            }
        except Exception as e:
            return {"success": False, "error": sanitize_error_message(str(e), self.github_token)}

    def _resolve_path(self, file_path: str) -> str:
        """Resolves relative or legacy /workspace paths to /home/user/workspace and confines to workspace."""
        p = file_path.strip()
        if p.startswith(WORKSPACE_DIR):
            resolved = p
        elif p.startswith("/workspace"):
            p = p[len("/workspace"):].lstrip("/")
            resolved = f"{WORKSPACE_DIR}/{p}"
        elif p.startswith("/"):
            resolved = os.path.normpath(f"{WORKSPACE_DIR}/{p.lstrip('/')}")
        else:
            resolved = os.path.normpath(os.path.join(WORKSPACE_DIR, p))

        # Confine path: must not traverse outside WORKSPACE_DIR
        norm_workspace = os.path.normpath(WORKSPACE_DIR)
        if not resolved.startswith(norm_workspace):
            return norm_workspace
        return resolved

    def run_command(self, command: str, cwd: str = WORKSPACE_DIR) -> Dict[str, Any]:
        """Runs any bash command inside the sandbox with truthful exit_code and success flags."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running.", "exit_code": -1}

        target_cwd = self._resolve_path(cwd) if cwd != WORKSPACE_DIR else WORKSPACE_DIR
        try:
            res = self.sandbox.commands.run(command, cwd=target_cwd)
            stdout = getattr(res, "stdout", "") or ""
            stderr = getattr(res, "stderr", "") or ""
            exit_code = getattr(res, "exit_code", 0)

            # Truthful reporting: command success is True ONLY if exit_code == 0
            is_success = (exit_code == 0)
            return {
                "success": is_success,
                "stdout": sanitize_error_message(stdout, self.github_token),
                "stderr": sanitize_error_message(stderr, self.github_token),
                "exit_code": exit_code,
            }
        except Exception as e:
            # Handle commands that exited with non-zero code
            if hasattr(e, "exit_code"):
                code = getattr(e, "exit_code", 1)
                return {
                    "success": (code == 0),
                    "stdout": sanitize_error_message(getattr(e, "stdout", "") or "", self.github_token),
                    "stderr": sanitize_error_message(getattr(e, "stderr", "") or getattr(e, "error", ""), self.github_token),
                    "exit_code": code,
                }
            return {
                "success": False,
                "error": sanitize_error_message(str(e), self.github_token),
                "exit_code": -1,
            }

    def start_dev_server(self, command: str, port: int) -> Dict[str, Any]:
        """Starts a background dev server and returns a public URL accessible from any mobile or desktop browser."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        try:
            # Run command in the background
            self.sandbox.commands.run(command, background=True, cwd=WORKSPACE_DIR)
            # Expose port as public URL
            host = self.sandbox.get_host(port)
            preview_url = f"https://{host}"
            return {
                "success": True,
                "preview_url": preview_url,
                "port": port,
                "message": f"Dev server running on port {port}. Live mobile/browser preview: {preview_url}",
            }
        except Exception as e:
            return {"success": False, "error": sanitize_error_message(str(e), self.github_token)}

    def read_file(self, file_path: str) -> Dict[str, Any]:
        """Reads a file from the repository inside the sandbox."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        full_path = self._resolve_path(file_path)
        try:
            content = self.sandbox.files.read(full_path)
            return {"success": True, "content": content}
        except Exception as e:
            return {"success": False, "error": sanitize_error_message(str(e), self.github_token)}

    def write_file(self, file_path: str, content: str) -> Dict[str, Any]:
        """Writes or updates a file inside the repository in the sandbox."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        full_path = self._resolve_path(file_path)
        try:
            self.sandbox.files.write(full_path, content)
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": sanitize_error_message(str(e), self.github_token)}

    def commit_and_create_pr(
        self,
        branch_name: str,
        commit_message: str,
        pr_title: str,
        pr_body: str,
        base_branch: str = "main",
    ) -> Dict[str, Any]:
        """Creates a git branch, commits changes, pushes to GitHub using safe parameterization, and opens a Pull Request."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        clean_branch = branch_name.strip()
        if not VALID_BRANCH_PATTERN.match(clean_branch) or ".." in clean_branch or clean_branch.startswith("-"):
            return {
                "success": False,
                "error": f"Invalid or unsafe git branch name: '{clean_branch}'.",
            }

        clean_base = base_branch.strip()
        if not VALID_BRANCH_PATTERN.match(clean_base) or ".." in clean_base or clean_base.startswith("-"):
            return {
                "success": False,
                "error": f"Invalid or unsafe base branch name: '{clean_base}'.",
            }

        try:
            # Safely quote arguments to prevent shell injection
            q_branch = shlex.quote(clean_branch)
            q_commit_msg = shlex.quote(commit_message.strip() or "AI code update")

            # Checkout branch and commit changes
            setup_cmds = [
                f"git checkout -b {q_branch}",
                'git config user.name "AI Cloud Workspace"',
                'git config user.email "bot@ai-cloud.workspace"',
                "git add -A",
                f"git commit -m {q_commit_msg}",
            ]
            setup_script = " && ".join(setup_cmds)
            res = self.sandbox.commands.run(setup_script, cwd=WORKSPACE_DIR)
            if getattr(res, "exit_code", 0) != 0:
                raw_err = getattr(res, "stderr", "") or getattr(res, "stdout", "")
                return {
                    "success": False,
                    "error": f"Git commit failed: {sanitize_error_message(raw_err, self.github_token)}",
                }

            # Push branch using transient header without exposing credentials to remote URL
            if self.github_token:
                import base64
                auth_val = base64.b64encode(f"x-access-token:{self.github_token}".encode()).decode()
                push_cmd = f"git -c http.extraHeader=\"AUTHORIZATION: basic {auth_val}\" push -u origin {q_branch}"
            else:
                push_cmd = f"git push -u origin {q_branch}"

            push_res = self.sandbox.commands.run(push_cmd, cwd=WORKSPACE_DIR)
            if getattr(push_res, "exit_code", 0) != 0:
                raw_err = getattr(push_res, "stderr", "") or getattr(push_res, "stdout", "")
                return {
                    "success": False,
                    "error": f"Git push failed: {sanitize_error_message(raw_err, self.github_token)}",
                }

            from github import Github

            gh = Github(self.github_token)
            repo = gh.get_repo(self.repo_name)
            pr = repo.create_pull(
                title=pr_title.strip() or "AI Pull Request",
                body=pr_body or "",
                head=clean_branch,
                base=clean_base,
            )

            return {
                "success": True,
                "pr_url": pr.html_url,
                "pr_number": pr.number,
            }
        except Exception as e:
            return {"success": False, "error": sanitize_error_message(str(e), self.github_token)}

    def close(self):
        """Terminates and destroys the ephemeral sandbox."""
        if self.sandbox:
            try:
                self.sandbox.kill()
            except Exception:
                pass
            self.sandbox = None



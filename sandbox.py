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

    def _run_git_with_askpass(
        self,
        command_args: str,
        cwd: Optional[str] = None,
        helper_prefix: str = "askpass",
    ) -> Dict[str, Any]:
        """Runs a Git command using an ephemeral GIT_ASKPASS credential helper with unified setup/execution cleanup.
        
        Distinguishes username versus password/token prompts.
        Guarantees cleanup of the helper script on both success and failure.
        """
        import uuid
        askpass_id = uuid.uuid4().hex[:8]
        askpass_path = f"/home/user/.git_askpass_{helper_prefix}_{askpass_id}"

        # Script distinguishes username vs password prompts
        # For HTTPS GitHub clone/push, username can be 'x-access-token' and password is the token
        askpass_content = (
            "#!/bin/sh\n"
            "case \"$1\" in\n"
            "  *Username*|*username*) exec echo \"x-access-token\" ;;\n"
            f"  *) exec echo {shlex.quote(self.github_token)} ;;\n"
            "esac\n"
        )

        try:
            self.sandbox.files.write(askpass_path, askpass_content)
            self.sandbox.commands.run(f"chmod 700 {askpass_path}")

            full_cmd = f"GIT_ASKPASS={askpass_path} GIT_TERMINAL_PROMPT=0 {command_args}"
            target_cwd = cwd or WORKSPACE_DIR
            res = self.sandbox.commands.run(full_cmd, cwd=target_cwd)
            return {
                "success": getattr(res, "exit_code", 0) == 0,
                "stdout": getattr(res, "stdout", "") or "",
                "stderr": getattr(res, "stderr", "") or "",
                "exit_code": getattr(res, "exit_code", 0),
            }
        finally:
            try:
                self.sandbox.commands.run(f"rm -f {askpass_path}")
            except Exception:
                pass

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

            # Clone using Git client with transient credential helper
            if self.github_token:
                clone_res = self._run_git_with_askpass(
                    f"git clone --depth 50 {clean_repo_url} {WORKSPACE_DIR}",
                    cwd="/home/user",
                    helper_prefix="clone",
                )
                if not clone_res["success"]:
                    raw_err = clone_res.get("stderr") or clone_res.get("stdout") or ""
                    return {
                        "success": False,
                        "error": f"Failed to clone repository: {sanitize_error_message(raw_err, self.github_token)}",
                    }
            else:
                clone_cmd = f"git clone --depth 50 {clean_repo_url} {WORKSPACE_DIR}"
                result = self.sandbox.commands.run(clone_cmd, cwd="/home/user")
                if getattr(result, "exit_code", 0) != 0:
                    raw_err = getattr(result, 'stderr', '') or getattr(result, 'stdout', '')
                    return {
                        "success": False,
                        "error": f"Failed to clone repository: {sanitize_error_message(raw_err, self.github_token)}",
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
        """Resolves relative or legacy /workspace paths to /home/user/workspace and confines strictly to workspace."""
        norm_workspace = os.path.normpath(WORKSPACE_DIR)
        p = file_path.strip()
        if not p:
            return norm_workspace

        if p.startswith("/workspace"):
            sub = p[len("/workspace"):].lstrip("/")
            target = os.path.normpath(os.path.join(norm_workspace, sub))
        elif p.startswith("/"):
            normalized_abs = os.path.normpath(p)
            if normalized_abs == norm_workspace or normalized_abs.startswith(norm_workspace + "/"):
                target = normalized_abs
            else:
                # Any absolute path outside workspace is rejected/confined
                return norm_workspace
        else:
            target = os.path.normpath(os.path.join(norm_workspace, p))

        # Check path containment using commonpath to prevent prefix collisions and traversal
        try:
            common = os.path.commonpath([norm_workspace, target])
        except Exception:
            return norm_workspace

        if common != norm_workspace:
            return norm_workspace
        return target

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

    def _check_realpath_confinement(self, full_path: str) -> Dict[str, Any]:
        """Validates that a path does not escape the workspace via symlinks.
        
        Fails closed: requires realpath exit_code == 0, non-empty stdout, and strict containment.
        """
        norm_workspace = os.path.normpath(WORKSPACE_DIR)
        try:
            realpath_check = self.sandbox.commands.run(f"realpath -m {shlex.quote(full_path)}")
            exit_code = getattr(realpath_check, "exit_code", 1)
            resolved_real = getattr(realpath_check, "stdout", "").strip()

            # Fail closed: must successfully execute and return non-empty resolved path
            if exit_code != 0 or not resolved_real:
                err_detail = getattr(realpath_check, "stderr", "") or "realpath command returned empty or non-zero status"
                return {
                    "valid": False,
                    "error": f"Path validation failed: unable to verify file containment ({err_detail}).",
                }

            # Check containment
            if not (resolved_real == norm_workspace or resolved_real.startswith(norm_workspace + "/")):
                return {
                    "valid": False,
                    "error": "Security violation: symbolic link targets a path outside workspace.",
                }

            return {"valid": True, "realpath": resolved_real}
        except Exception as e:
            return {
                "valid": False,
                "error": f"Path validation failed: {sanitize_error_message(str(e), self.github_token)}",
            }

    def read_file(self, file_path: str) -> Dict[str, Any]:
        """Reads a file from the repository inside the sandbox with strict path and symlink checks."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        full_path = self._resolve_path(file_path)
        norm_workspace = os.path.normpath(WORKSPACE_DIR)

        # Disallow reading root workspace dir directly as file
        if full_path == norm_workspace:
            return {"success": False, "error": "Invalid file path: path points outside workspace or to workspace root."}

        # Check for escaping symlinks via realpath check in sandbox (fails closed)
        containment = self._check_realpath_confinement(full_path)
        if not containment["valid"]:
            return {"success": False, "error": containment["error"]}

        try:
            content = self.sandbox.files.read(full_path)
            return {"success": True, "content": content}
        except Exception as e:
            return {"success": False, "error": sanitize_error_message(str(e), self.github_token)}

    def write_file(self, file_path: str, content: str) -> Dict[str, Any]:
        """Writes or updates a file inside the repository in the sandbox with strict path and symlink checks."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        full_path = self._resolve_path(file_path)
        norm_workspace = os.path.normpath(WORKSPACE_DIR)

        if full_path == norm_workspace:
            return {"success": False, "error": "Invalid file path: path points outside workspace or to workspace root."}

        # Check for escaping symlinks via realpath check in sandbox (fails closed)
        containment = self._check_realpath_confinement(full_path)
        if not containment["valid"]:
            return {"success": False, "error": containment["error"]}

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

            # Push branch using transient askpass helper without exposing credentials to command args or remote URL
            if self.github_token:
                push_res = self._run_git_with_askpass(
                    f"git push -u origin {q_branch}",
                    cwd=WORKSPACE_DIR,
                    helper_prefix="push",
                )
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



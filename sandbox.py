"""E2B Cloud Sandbox Engine for running terminal commands, tests, git operations, and live web previews."""

import os
from typing import Any, Dict, Optional
import streamlit as st


def get_e2b_api_key() -> str:
    """Retrieves the E2B API Key from Streamlit secrets or environment."""
    return st.secrets.get("E2B_API_KEY", os.getenv("E2B_API_KEY", ""))


class WorkspaceSandbox:
    """Manages an ephemeral cloud Linux container for a user's repository."""

    def __init__(self, repo_name: str, github_token: str):
        self.repo_name = repo_name
        self.github_token = github_token
        self.api_key = get_e2b_api_key()
        self.sandbox: Optional[Any] = None

    def start(self) -> Dict[str, Any]:
        """Boots the E2B sandbox and clones the repository into /workspace."""
        if not self.api_key:
            return {
                "success": False,
                "error": "E2B_API_KEY not found in secrets or environment. Get a free key at https://e2b.dev",
            }

        try:
            from e2b import Sandbox

            # Initialize sandbox
            self.sandbox = Sandbox(api_key=self.api_key)

            # Clone repository
            auth_repo_url = f"https://x-access-token:{self.github_token}@github.com/{self.repo_name}.git"
            clone_cmd = f"git clone {auth_repo_url} /workspace"

            result = self.sandbox.commands.run(clone_cmd)
            if result.exit_code != 0:
                return {
                    "success": False,
                    "error": f"Failed to clone repository: {result.stderr or result.stdout}",
                }

            return {
                "success": True,
                "message": f"Successfully cloned {self.repo_name} into sandbox environment.",
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def run_command(self, command: str, cwd: str = "/workspace") -> Dict[str, Any]:
        """Runs any bash command (e.g. npm test, pytest, go run, cargo) inside the sandbox."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        try:
            res = self.sandbox.commands.run(command, cwd=cwd)
            return {
                "success": True,
                "stdout": res.stdout,
                "stderr": res.stderr,
                "exit_code": res.exit_code,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def start_dev_server(self, command: str, port: int) -> Dict[str, Any]:
        """Starts a background dev server and returns a public URL accessible from any mobile or desktop browser."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        try:
            # Run command in the background
            self.sandbox.commands.run(command, background=True, cwd="/workspace")
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
            return {"success": False, "error": str(e)}

    def read_file(self, file_path: str) -> Dict[str, Any]:
        """Reads a file from the repository inside the sandbox."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        full_path = (
            file_path
            if file_path.startswith("/workspace")
            else f"/workspace/{file_path}"
        )
        try:
            content = self.sandbox.files.read(full_path)
            return {"success": True, "content": content}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def write_file(self, file_path: str, content: str) -> Dict[str, Any]:
        """Writes or updates a file inside the repository in the sandbox."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        full_path = (
            file_path
            if file_path.startswith("/workspace")
            else f"/workspace/{file_path}"
        )
        try:
            self.sandbox.files.write(full_path, content)
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def commit_and_create_pr(
        self,
        branch_name: str,
        commit_message: str,
        pr_title: str,
        pr_body: str,
        base_branch: str = "main",
    ) -> Dict[str, Any]:
        """Creates a git branch, commits changes, pushes to GitHub, and opens a Pull Request."""
        if not self.sandbox:
            return {"success": False, "error": "Sandbox is not running."}

        try:
            commands = [
                f"git checkout -b {branch_name}",
                'git config user.name "AI Cloud Workspace"',
                'git config user.email "bot@ai-cloud.workspace"',
                "git add .",
                f'git commit -m "{commit_message}"',
                f"git push -u origin {branch_name}",
            ]

            full_script = " && ".join(commands)
            res = self.sandbox.commands.run(full_script, cwd="/workspace")
            if res.exit_code != 0:
                return {
                    "success": False,
                    "error": f"Git operations failed: {res.stderr or res.stdout}",
                }

            from github import Github

            gh = Github(self.github_token)
            repo = gh.get_repo(self.repo_name)
            pr = repo.create_pull(
                title=pr_title, body=pr_body, head=branch_name, base=base_branch
            )

            return {
                "success": True,
                "pr_url": pr.html_url,
                "pr_number": pr.number,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def close(self):
        """Terminates and destroys the ephemeral sandbox."""
        if self.sandbox:
            try:
                self.sandbox.kill()
            except Exception:
                pass
            self.sandbox = None

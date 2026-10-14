"""Autonomous AI Coding Agent supporting BYOK (Gemini, Claude, OpenAI, DeepSeek) with Sandbox tools."""

import json
from typing import Any, Callable, Dict, List, Optional
import litellm
from litellm import completion
from sandbox import WorkspaceSandbox

# Suppress verbose LiteLLM logging
litellm.suppress_debug_info = True

AGENT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "run_terminal_command",
            "description": "Execute any bash shell command in the repository workspace (e.g. npm test, pytest, go run, cargo build, python, ls, git status).",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The exact shell command to run.",
                    }
                },
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "start_dev_server",
            "description": "Start a long-running web dev server in the background (e.g. npm run dev, python -m http.server 8000, flask run) and return a live public URL to view the app on phone or desktop browser.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The command to run in background (e.g. npm run dev -- --port 3000, python3 app.py).",
                    },
                    "port": {
                        "type": "integer",
                        "description": "The port the dev server listens on (e.g. 3000, 5000, 8000, 8080).",
                    },
                },
                "required": ["command", "port"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read the contents of a file in the workspace repository.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to file relative to repo root.",
                    }
                },
                "required": ["file_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Create or overwrite a file in the workspace repository.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to file relative to repo root.",
                    },
                    "content": {
                        "type": "string",
                        "description": "Full file content to write.",
                    },
                },
                "required": ["file_path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_pull_request",
            "description": "Commit changes, push to GitHub, and open a Pull Request.",
            "parameters": {
                "type": "object",
                "properties": {
                    "branch_name": {
                        "type": "string",
                        "description": "Name for the git branch (e.g. fix/auth-tests).",
                    },
                    "commit_message": {
                        "type": "string",
                        "description": "Git commit message.",
                    },
                    "pr_title": {"type": "string", "description": "Title of the PR."},
                    "pr_body": {
                        "type": "string",
                        "description": "Detailed markdown explanation of the fix and test verification.",
                    },
                },
                "required": [
                    "branch_name",
                    "commit_message",
                    "pr_title",
                    "pr_body",
                ],
            },
        },
    },
]

SYSTEM_PROMPT = """You are an expert autonomous software engineer working in a live Linux sandbox on a user's GitHub repository.
You have full access to terminal execution, live dev server hosting, file viewing/editing, and GitHub Pull Request creation.

Your Capabilities:
1. Multi-language support: You can execute Python, Node.js, Go, Rust, Ruby, PHP, Java, Shell, and any Linux command.
2. Web previews: If the user asks to "run this project", inspect the repo (e.g., package.json, requirements.txt, go.mod), start the appropriate dev server using `start_dev_server`, and provide the live public URL so the user can open it on their mobile phone or desktop browser.
3. Automated fixes: When asked to fix bugs or test, run tests in terminal, inspect errors, modify code, re-test until clean, and open a PR.

Always be concise, precise, and transparent about commands executed."""


def run_agent_turn(
    messages: List[Dict[str, Any]],
    model: str,
    api_key: str,
    sandbox: WorkspaceSandbox,
    status_callback: Optional[Callable[[str], None]] = None,
    max_steps: int = 10,
) -> Dict[str, Any]:
    """Runs a multi-turn agent execution loop with sandbox tool calling."""

    working_messages = [
        {"role": "system", "content": SYSTEM_PROMPT}
    ] + messages

    accumulated_logs = []

    for step in range(max_steps):
        if status_callback:
            status_callback(f"Agent thinking (Step {step + 1}/{max_steps})...")

        try:
            response = completion(
                model=model,
                messages=working_messages,
                tools=AGENT_TOOLS,
                tool_choice="auto",
                api_key=api_key,
                temperature=0.2,
            )
        except Exception as e:
            return {
                "success": False,
                "error": f"LLM API Error: {str(e)}",
                "terminal_logs": "\n".join(accumulated_logs),
            }

        response_message = response.choices[0].message
        working_messages.append(response_message)

        tool_calls = getattr(response_message, "tool_calls", None)

        if not tool_calls:
            return {
                "success": True,
                "content": response_message.content or "Completed task.",
                "terminal_logs": "\n".join(accumulated_logs),
            }

        for tool_call in tool_calls:
            func_name = tool_call.function.name
            args = json.loads(tool_call.function.arguments)

            if status_callback:
                status_callback(f"Executing `{func_name}` in sandbox...")

            tool_result = ""

            if func_name == "run_terminal_command":
                cmd = args.get("command", "")
                log_entry = f"$ {cmd}"
                accumulated_logs.append(log_entry)

                res = sandbox.run_command(cmd)
                stdout = res.get("stdout", "")
                stderr = res.get("stderr", "")
                exit_code = res.get("exit_code", 0)

                output_str = stdout if stdout else ""
                if stderr:
                    output_str += f"\n[stderr]: {stderr}"
                output_str += f"\n[Exit code: {exit_code}]"

                accumulated_logs.append(output_str)
                tool_result = json.dumps(res)

            elif func_name == "start_dev_server":
                cmd = args.get("command", "")
                port = int(args.get("port", 3000))
                res = sandbox.start_dev_server(cmd, port)
                accumulated_logs.append(
                    f"$ {cmd} (Dev server started on port {port})\nPreview URL: {res.get('preview_url')}"
                )
                tool_result = json.dumps(res)

            elif func_name == "read_file":
                path = args.get("file_path", "")
                res = sandbox.read_file(path)
                tool_result = json.dumps(res)
                accumulated_logs.append(f"Read file: {path}")

            elif func_name == "write_file":
                path = args.get("file_path", "")
                content = args.get("content", "")
                res = sandbox.write_file(path, content)
                tool_result = json.dumps(res)
                accumulated_logs.append(f"Updated file: {path}")

            elif func_name == "create_pull_request":
                res = sandbox.commit_and_create_pr(
                    branch_name=args.get("branch_name", "ai-fix"),
                    commit_message=args.get("commit_message", "AI code update"),
                    pr_title=args.get("pr_title", "AI Pull Request"),
                    pr_body=args.get("pr_body", ""),
                )
                tool_result = json.dumps(res)
                if res.get("success"):
                    accumulated_logs.append(f"Created PR: {res.get('pr_url')}")
                else:
                    accumulated_logs.append(
                        f"Failed to create PR: {res.get('error')}"
                    )

            working_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": func_name,
                    "content": tool_result,
                }
            )

    return {
        "success": True,
        "content": "Reached maximum steps limit. Please review the terminal logs above.",
        "terminal_logs": "\n".join(accumulated_logs),
    }

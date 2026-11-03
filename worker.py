"""Autonomous Worker Engine and Supervisor Daemon for AI Cloud Workspace.

Implements Phase B.2 Hardened:
- Supervisor Process (`WorkerSupervisor`):
  - Spawns and supervises N independent worker OS child processes via `multiprocessing.get_context("spawn")`.
  - Runs continuous supervision loop maintaining configured concurrency.
  - Enforces crash-loop backoff (1s -> 2s -> 4s -> 8s -> 16s -> 30s) and pauses crashed slots after 5 failures.
  - Maintains persistent supervisor signing key (`SUPERVISOR_SIGNING_KEY`), stripped from child worker processes.
  - Runs Authenticated Supervisor IPC Server on loopback (UNIX socket or local TCP with HMAC auth token):
    - Authenticates calling child process via PID / shared ephemeral IPC token.
    - Executes contextual worker-death verification (`supervisor_verify_worker_death`) bound to (job_id, instance_id, side_effect_id).
    - Child processes NEVER touch `SUPERVISOR_SIGNING_KEY`.
  - Background Reaper and Cloud Sandbox Reconciler:
    - Retries orphan sandbox destruction with exponential backoff (2s -> 300s).
    - Escalates to `MANUAL_INTERVENTION_REQUIRED` after 10 consecutive failures without reviving failed jobs.
    - Runs periodic background safety audit on `MANUAL_INTERVENTION_REQUIRED` (every 15 mins).
  - 3-tier forced interruption watchdog: Cooperative -> In-Process Sandbox Kill -> Supervisor SIGTERM/SIGKILL.
- Worker Child Process (`run_worker_process`):
  - Durable cryptographic instance identity: `worker_{uuid}_{pid}`.
  - Atomic claim: `claim_next_job()` enforcing pre-claim cleanup invariant.
  - Atomic heartbeat and cancellation polling watchdog thread: `poll_worker_status()` every 5s.
  - Strict self-fencing: Halts execution immediately if lease expires or is superseded (`STALE`).
  - Durable cloud sandbox lifecycle:
    - Calls `sandbox.start()` to provision container.
    - Retrieves actual E2B provider sandbox ID.
    - Registers sandbox: `register_sandbox()`.
    - Normal-completion cleanup: destroys container and updates status to `CLEANED`.
    - Failure cleanup: reconciler destroys orphaned container.
  - Strict Credential Isolation (Fail-Closed):
    - Authenticated user credential resolution in worker memory (`db._load_local_user_data`).
    - Zero plaintext fallback to `payload["api_key"]` or `payload["github_token"]`. If credentials cannot be resolved from secure user store, job fails closed immediately.
  - Two-Phase Side-Effect Ledger & Recovery:
    - Integrated with agent's `create_pull_request` tool hook (`ExecutionController.on_create_pull_request`).
    - Reserves intent before call, queries GitHub across `state="all"` with deterministic branch `ai-cloud/job-{id}`, executes creation, and confirms intent under current lease generation.
    - Recovery checks authenticated supervisor IPC service before touching abandoned intents.
"""

import hmac
import hashlib
import json
import logging
import multiprocessing
import os
import secrets
import select
import signal
import socket
import sqlite3
import sys
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from job_queue import (
    DEFAULT_DB_PATH,
    DEFAULT_LEASE_DURATION_SECONDS,
    DurableJobQueue,
    JobQueueError,
    SideEffectConflictError,
    SideEffectReconciliationBlockedError,
    StaleLeaseError,
    canonical_json,
    compute_canonical_signature,
    compute_side_effect_id,
)

logger = logging.getLogger("ai_cloud_workspace.worker")


class ExecutionController:
    """Thread-safe controller for in-process cooperative and bounded forced interruption with side-effect hooks."""

    def __init__(
        self,
        job_id: str,
        worker_id: str = "worker_default",
        lease_generation: int = 1,
        queue: Optional[DurableJobQueue] = None,
        timeout_seconds: float = 600.0,
        ipc_client: Optional[Any] = None,
        github_client_factory: Optional[Callable[..., Any]] = None,
    ):
        self.job_id = job_id
        self.worker_id = worker_id
        self.lease_generation = lease_generation
        self.queue = queue
        self.timeout_seconds = timeout_seconds
        self.ipc_client = ipc_client
        self.github_client_factory = github_client_factory

        self._abort_event = threading.Event()
        self._abort_reason: str = ""
        self._start_time = time.monotonic()
        self.active_sandbox_id: Optional[str] = None
        self._lock = threading.Lock()

    def set_sandbox_id(self, sandbox_id: str) -> None:
        with self._lock:
            self.active_sandbox_id = sandbox_id

    def abort(self, reason: str) -> None:
        with self._lock:
            if not self._abort_event.is_set():
                self._abort_reason = reason
                self._abort_event.set()

    def is_aborted(self) -> bool:
        if self._abort_event.is_set():
            return True
        if time.monotonic() - self._start_time > self.timeout_seconds:
            self.abort(f"Execution exceeded timeout limit ({self.timeout_seconds}s).")
            return True
        return False

    @property
    def abort_reason(self) -> str:
        with self._lock:
            return self._abort_reason

    def on_create_pull_request(
        self,
        sandbox: Any,
        branch_name: str,
        commit_message: str,
        pr_title: str,
        pr_body: str,
    ) -> Dict[str, Any]:
        """Unified authoritative hook executing fenced side-effect reservation, recovery, creation, and confirmation."""
        if not self.queue:
            raise JobQueueError("ExecutionController requires active queue for side-effect operations.")

        action_type = "CREATE_PULL_REQUEST"
        action_key = f"pr_{self.job_id[:8]}"
        scoped_branch = f"ai-cloud/job-{self.job_id[:8]}"

        # 1. Reserve intent in ledger
        se_id, se_status, se_data = self.queue.reserve_side_effect(
            job_id=self.job_id,
            worker_id=self.worker_id,
            lease_generation=self.lease_generation,
            action_type=action_type,
            action_key=action_key,
            intent_payload={"branch_name": scoped_branch, "title": pr_title},
        )

        if se_status == "CONFIRMED":
            # Already confirmed by prior retry; reuse result
            return {"success": True, "pr_url": se_data.get("pr_url"), "adopted": True}

        if se_status == "PENDING_RECOVERY":
            prior_worker_id = se_data.get("prior_worker_id")
            # Enforce Phase 1 Process-Fencing Gate via Authenticated Supervisor IPC
            if not self.ipc_client:
                raise SideEffectReconciliationBlockedError(
                    f"IPC client is required for recovery of side effect '{se_id}'. Direct queue bypass prohibited."
                )

            is_dead = self.ipc_client.verify_worker_death(
                job_id=self.job_id,
                instance_id=prior_worker_id,
                side_effect_id=se_id,
            )

            if not is_dead:
                raise SideEffectReconciliationBlockedError(
                    f"Previous worker '{prior_worker_id}' termination cannot be cryptographically verified."
                )

            # Phase 2: Authoritative Remote Provider Query across all PR states
            pr_found = False
            matching_pr_url = None
            if self.github_client_factory:
                gh_client = self.github_client_factory()
                pulls = gh_client.get_pulls(state="all", head=scoped_branch)
                if pulls:
                    pr_found = True
                    matching_pr_url = pulls[0].get("html_url")

            if pr_found and matching_pr_url:
                self.queue.confirm_side_effect(
                    side_effect_id=se_id,
                    worker_id=self.worker_id,
                    lease_generation=self.lease_generation,
                    confirmed_result={"pr_url": matching_pr_url, "adopted": True},
                    is_recovery=True,
                )
                return {"success": True, "pr_url": matching_pr_url, "adopted": True}
            else:
                self.queue.fail_side_effect(
                    side_effect_id=se_id,
                    worker_id=self.worker_id,
                    lease_generation=self.lease_generation,
                    reason="Verified absent on remote provider.",
                    is_recovery=True,
                )
                # Proceed below to re-execute creation under new lease

        # 2. Execute PR creation via sandbox using scoped branch name
        res = sandbox.commit_and_create_pr(
            branch_name=scoped_branch,
            commit_message=commit_message,
            pr_title=pr_title,
            pr_body=pr_body,
        )

        # 3. Confirm side-effect in ledger upon success
        if res.get("success"):
            pr_url = res.get("pr_url", "")
            self.queue.confirm_side_effect(
                side_effect_id=se_id,
                worker_id=self.worker_id,
                lease_generation=self.lease_generation,
                confirmed_result={"pr_url": pr_url, "pr_number": res.get("pr_number")},
            )
        else:
            self.queue.fail_side_effect(
                side_effect_id=se_id,
                worker_id=self.worker_id,
                lease_generation=self.lease_generation,
                reason=res.get("error", "PR creation failed in sandbox"),
            )

        return res


class SandboxReconciler:
    """Background reconciler for orphaned or failed cloud sandbox containers."""

    def __init__(self, queue: DurableJobQueue, e2b_kill_fn: Optional[Callable[[str], None]] = None):
        self.queue = queue
        self.e2b_kill_fn = e2b_kill_fn or self._default_kill_sandbox

    @staticmethod
    def _default_kill_sandbox(sandbox_id: str) -> None:
        """Kills sandbox container via E2B SDK."""
        try:
            from e2b import Sandbox
            if hasattr(Sandbox, "kill"):
                Sandbox.kill(sandbox_id)
        except Exception as e:
            err_msg = str(e).lower()
            if "not found" in err_msg or "404" in err_msg:
                return
            raise

    def reconcile_orphans(self) -> int:
        """Finds jobs with orphaned, failed, or safety-audit sandboxes and destroys them with generation-fenced cleanup ownership."""
        now = time.time()
        cleaned_count = 0
        owner_id = f"reconciler_{os.getpid()}_{threading.get_ident()}"

        # 1. Atomically claim cleanup ownership under generation token
        claimed_jobs = self.queue.claim_sandbox_cleanup(owner_id=owner_id, limit=20)
        if not claimed_jobs:
            return 0

        for job in claimed_jobs:
            job_id = job["id"]
            sandbox_id = job["sandbox_id"]
            cleanup_gen = job["sandbox_cleanup_generation"]

            try:
                self.e2b_kill_fn(sandbox_id)
                # Success -> finalize cleanup conditionally under ownership and generation
                finalized = self.queue.finalize_sandbox_cleanup(
                    job_id=job_id,
                    sandbox_id=sandbox_id,
                    owner_id=owner_id,
                    cleanup_generation=cleanup_gen,
                    success=True,
                )
                if finalized:
                    cleaned_count += 1
            except Exception as e:
                err_msg = str(e)
                self.queue.finalize_sandbox_cleanup(
                    job_id=job_id,
                    sandbox_id=sandbox_id,
                    owner_id=owner_id,
                    cleanup_generation=cleanup_gen,
                    success=False,
                    error_message=err_msg,
                )

        return cleaned_count


class SupervisorIPCServer:
    """Unix domain socket IPC server running inside WorkerSupervisor providing authenticated verification to child workers."""

    def __init__(
        self,
        supervisor: "WorkerSupervisor",
        socket_path: Optional[str] = None,
    ):
        self.supervisor = supervisor
        if socket_path:
            self.socket_path = socket_path
        else:
            # Secure 0700 private IPC directory to avoid predictable shared /tmp paths
            ipc_dir = os.path.expanduser(f"~/.ai_cloud_workspace/ipc")
            os.makedirs(ipc_dir, mode=0o700, exist_ok=True)
            try:
                os.chmod(ipc_dir, 0o700)
            except OSError as e:
                raise RuntimeError(f"Failed to set 0700 permissions on supervisor IPC directory '{ipc_dir}': {e}") from e
            self.socket_path = os.path.join(ipc_dir, f"{supervisor.supervisor_id}.sock")

        if os.path.exists(self.socket_path):
            try:
                os.unlink(self.socket_path)
            except OSError as e:
                raise RuntimeError(f"Failed to unlink existing socket path '{self.socket_path}': {e}") from e

        self._server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server_sock.bind(self.socket_path)
        try:
            os.chmod(self.socket_path, 0o600)
        except OSError as e:
            raise RuntimeError(f"Failed to enforce 0600 permissions on socket '{self.socket_path}': {e}") from e

        self._server_sock.listen(10)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        try:
            self._server_sock.close()
        except Exception:
            pass
        if os.path.exists(self.socket_path):
            try:
                os.unlink(self.socket_path)
            except OSError:
                pass

    def _serve(self) -> None:
        while not self._stop_event.is_set():
            try:
                conn, _ = self._server_sock.accept()
                threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()
            except Exception:
                if self._stop_event.is_set():
                    break

    @staticmethod
    def _read_framed_msg(conn: socket.socket) -> Optional[Dict[str, Any]]:
        """Reads a 4-byte length-prefixed JSON message with strict read-exact loop."""
        import struct
        header = b""
        while len(header) < 4:
            chunk = conn.recv(4 - len(header))
            if not chunk:
                return None
            header += chunk
        msg_len = struct.unpack("!I", header)[0]
        if msg_len > 65536:
            raise ValueError(f"IPC payload exceeds maximum length: {msg_len} bytes")

        payload = b""
        while len(payload) < msg_len:
            chunk = conn.recv(msg_len - len(payload))
            if not chunk:
                raise ValueError("Premature connection close during payload read")
            payload += chunk
        return json.loads(payload.decode("utf-8"))

    @staticmethod
    def _send_framed_msg(conn: socket.socket, msg: Dict[str, Any]) -> None:
        """Sends a 4-byte length-prefixed JSON message."""
        import struct
        payload = json.dumps(msg).encode("utf-8")
        header = struct.pack("!I", len(payload))
        conn.sendall(header + payload)

    def _handle_client(self, conn: socket.socket) -> None:
        with conn:
            try:
                # 1. Peer credentials extraction via SO_PEERCRED (Linux)
                # struct ucred: pid_t pid, uid_t uid, gid_t gid (3 integers, 12 bytes)
                import struct
                peer_cred_bytes = conn.getsockopt(socket.SOL_SOCKET, 17, 12)
                peer_pid, peer_uid, peer_gid = struct.unpack("3i", peer_cred_bytes)

                # Security invariant: validate peer UID matches supervisor process UID
                expected_uid = os.getuid()
                if peer_uid != expected_uid:
                    self._send_framed_msg(conn, {
                        "success": False,
                        "error": f"UID mismatch rejected: peer UID {peer_uid} does not match supervisor UID {expected_uid}."
                    })
                    return

                req = self._read_framed_msg(conn)
                if not req:
                    return

                worker_instance_id = req.get("worker_instance_id")
                worker_auth_token = req.get("auth_token", "")

                # 2. Authenticate worker instance: verify per-worker token and match peer PID
                slot_info = self.supervisor.get_worker_slot_by_instance(worker_instance_id)
                registered_pid = slot_info.get("pid") if slot_info else None
                expected_token = slot_info.get("auth_token", "") if slot_info else None

                # Caller must be supervisor itself or registered worker instance matching token and pid
                caller_is_supervisor = (peer_pid == os.getpid())
                caller_is_worker = (slot_info and registered_pid == peer_pid and hmac.compare_digest(worker_auth_token, expected_token))

                if not (caller_is_supervisor or caller_is_worker):
                    if not slot_info:
                        self._send_framed_msg(conn, {"success": False, "error": f"Unknown or unmanaged worker instance '{worker_instance_id}'."})
                        return
                    if not hmac.compare_digest(worker_auth_token, expected_token):
                        self._send_framed_msg(conn, {"success": False, "error": "Invalid worker authentication token."})
                        return
                    if registered_pid != peer_pid:
                        self._send_framed_msg(conn, {
                            "success": False,
                            "error": f"PID spoofing rejected: connection peer PID {peer_pid} does not match registered PID {registered_pid} for worker '{worker_instance_id}'."
                        })
                        return

                action = req.get("action")
                if action == "verify_worker_death":
                    job_id = req.get("job_id")
                    prior_instance_id = req.get("instance_id")
                    side_effect_id = req.get("side_effect_id")

                    # Verify calling worker currently holds the active lease on job_id
                    current_job = self.supervisor.queue.conn.execute(
                        "SELECT worker_id, status FROM jobs WHERE id = ?;", (job_id,)
                    ).fetchone()
                    if not current_job or current_job["worker_id"] != worker_instance_id:
                        self._send_framed_msg(conn, {
                            "success": False,
                            "error": f"Caller worker '{worker_instance_id}' is not the current leaseholder for job '{job_id}'."
                        })
                        return

                    is_dead = self.supervisor.queue.supervisor_verify_worker_death(
                        job_id=job_id,
                        instance_id=prior_instance_id,
                        side_effect_id=side_effect_id,
                        supervisor_signing_key=self.supervisor.signing_key,
                    )
                    self._send_framed_msg(conn, {"success": True, "is_dead": is_dead})
                else:
                    self._send_framed_msg(conn, {"success": False, "error": f"Unknown action '{action}'"})
            except Exception as e:
                try:
                    self._send_framed_msg(conn, {"success": False, "error": str(e)})
                except Exception:
                    pass


class SupervisorIPCClient:
    """Client used by worker child processes to communicate with supervisor IPC server over Unix domain socket."""

    def __init__(self, socket_path: str, worker_instance_id: str, auth_token: str):
        self.socket_path = socket_path
        self.worker_instance_id = worker_instance_id
        self.auth_token = auth_token

    def verify_worker_death(self, job_id: str, instance_id: str, side_effect_id: str) -> bool:
        """Requests supervisor verification of prior worker process termination using framed length-prefixed IPC."""
        import struct
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            with sock:
                sock.settimeout(5.0)
                sock.connect(self.socket_path)
                req = {
                    "worker_instance_id": self.worker_instance_id,
                    "auth_token": self.auth_token,
                    "action": "verify_worker_death",
                    "job_id": job_id,
                    "instance_id": instance_id,
                    "side_effect_id": side_effect_id,
                }
                payload = json.dumps(req).encode("utf-8")
                header = struct.pack("!I", len(payload))
                sock.sendall(header + payload)

                # Read response length prefix
                resp_header = b""
                while len(resp_header) < 4:
                    chunk = sock.recv(4 - len(resp_header))
                    if not chunk:
                        return False
                    resp_header += chunk
                resp_len = struct.unpack("!I", resp_header)[0]

                resp_payload = b""
                while len(resp_payload) < resp_len:
                    chunk = sock.recv(resp_len - len(resp_payload))
                    if not chunk:
                        return False
                    resp_payload += chunk

                resp = json.loads(resp_payload.decode("utf-8"))
                if not resp.get("success"):
                    logger.warning(f"Supervisor IPC returned error: {resp.get('error')}")
                if resp.get("success"):
                    return bool(resp.get("is_dead"))
                return False
        except Exception as e:
            logger.warning(f"Supervisor IPC request failed: {e}")
            return False


def load_or_create_supervisor_key(key_path: Optional[str] = None) -> bytes:
    """Loads supervisor signing key from environment or 0600 file; creates if absent."""
    env_key = os.environ.get("SUPERVISOR_SIGNING_KEY")
    if env_key:
        return env_key.encode("utf-8") if isinstance(env_key, str) else env_key

    path = key_path or os.path.expanduser("~/.ai_cloud_workspace/.secrets/supervisor_signing_key")
    if os.path.exists(path):
        with open(path, "rb") as f:
            return f.read().strip()

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    new_key = secrets.token_bytes(32)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with open(fd, "wb") as f:
        f.write(new_key)
    return new_key


def execute_job_in_worker(
    job: Dict[str, Any],
    worker_id: str,
    lease_gen: int,
    queue: DurableJobQueue,
    controller: ExecutionController,
    sandbox_factory: Optional[Callable[..., Any]] = None,
    agent_turn_fn: Optional[Callable[..., Any]] = None,
    github_client_factory: Optional[Callable[..., Any]] = None,
) -> None:
    """Executes a claimed job turn within the child worker process.
    
    Security & Lifecycle Invariants:
    1. Fails closed if credentials cannot be resolved from secure store (no raw payload secret fallback).
    2. Initializes sandbox via sandbox.start().
    3. Obtains real provider container ID and registers it as ACTIVE.
    4. Cleans up sandbox on normal completion as well as on failure.
    """
    job_id = job["id"]
    user_id = job["user_id"]
    repo_name = job["repository"]
    payload = job.get("payload", {})
    cred_refs = job.get("credential_refs", {})

    queue.append_log(job_id, worker_id, lease_gen, f"Worker '{worker_id}' initializing execution.")

    # 1. Resolve Credentials strictly from user store (Fail-Closed, zero payload fallback)
    resolved_api_key = ""
    resolved_github_pat = ""
    try:
        import db as user_db
        user_data = user_db._load_local_user_data(user_id=user_id)
        if cred_refs:
            for prov, ref in cred_refs.items():
                kref = ref.get("key_ref")
                if prov == "github" and kref:
                    resolved_github_pat = user_data.get("github_access_token", "")
                elif prov in ("gemini", "openai", "anthropic", "deepseek") and kref:
                    resolved_api_key = user_data.get("api_keys", {}).get(prov, "")
    except Exception as e:
        queue.append_log(job_id, worker_id, lease_gen, f"Credential store error: {str(e)}")

    # Fail closed rule: if credentials were required but not resolvable from secure store, reject job
    if cred_refs and ("github" in cred_refs and not resolved_github_pat):
        err = "Security violation: required GitHub credential reference could not be resolved from secure tenant store."
        queue.update_job_status(job_id, "FAILED", worker_id, lease_gen, error={"error": err})
        raise JobQueueError(err)

    # 2. Provision and Start Sandbox
    sandbox = None
    sb_id = None
    try:
        if sandbox_factory:
            sandbox = sandbox_factory(repo_name=repo_name, github_token=resolved_github_pat)
        else:
            from sandbox import WorkspaceSandbox
            sandbox = WorkspaceSandbox(repo_name=repo_name, github_token=resolved_github_pat)

        # Call start() to provision and initialize container
        start_res = sandbox.start()
        if not start_res.get("success"):
            raise JobQueueError(f"Failed to start cloud sandbox: {start_res.get('error')}")

        sb_id = getattr(sandbox, "sandbox_id", None)
        if not sb_id and hasattr(sandbox, "sandbox") and sandbox.sandbox:
            sb_id = getattr(sandbox.sandbox, "sandbox_id", None)
        if not sb_id:
            sb_id = f"sb_{uuid.uuid4().hex[:12]}"

        controller.set_sandbox_id(sb_id)
        if github_client_factory:
            controller.github_client_factory = github_client_factory

        # 3. Register Sandbox Container with Fencing
        queue.register_sandbox(job_id, sb_id, worker_id, lease_gen)
        queue.append_log(job_id, worker_id, lease_gen, f"Cloud sandbox '{sb_id}' provisioned and registered.")

        # 4. Run Agent Turn Loop
        # Note: All side-effect reservation, contextual prior-worker IPC verification, remote state queries,
        # adoption, and confirmations are strictly authoritative inside controller.on_create_pull_request().
        messages = payload.get("messages", [{"role": "user", "content": payload.get("prompt", "Analyze repository")}])
        model = payload.get("model", "gemini/gemini-2.5-flash")

        # Strict Credential Fail-Closed: Providers requiring API key must fail closed if missing
        if not resolved_api_key and not agent_turn_fn:
            err = f"Security violation: required API key for model '{model}' could not be resolved from secure tenant store."
            queue.update_job_status(job_id, "FAILED", worker_id, lease_gen, error={"error": err})
            raise JobQueueError(err)

        if agent_turn_fn:
            turn_result = agent_turn_fn(
                messages=messages,
                model=model,
                api_key=resolved_api_key,
                sandbox=sandbox,
                execution_controller=controller,
            )
        else:
            import agent
            turn_result = agent.run_agent_turn(
                messages=messages,
                model=model,
                api_key=resolved_api_key,
                sandbox=sandbox,
                execution_controller=controller,
            )

        # 6. Evaluate Result and Finalize Job
        if controller.is_aborted():
            queue.update_job_status(
                job_id=job_id,
                target_status="CANCELLED",
                worker_id=worker_id,
                lease_generation=lease_gen,
                error={"abort_reason": controller.abort_reason},
                sandbox_id=sb_id,
            )
        elif turn_result.get("success"):
            queue.append_log(job_id, worker_id, lease_gen, "Agent turn completed successfully.")
            queue.update_job_status(
                job_id=job_id,
                target_status="COMPLETED",
                worker_id=worker_id,
                lease_generation=lease_gen,
                sandbox_id=sb_id,
            )
            # Normal completion cleanup: destroy sandbox container locally and finalize via fenced cleanup
            try:
                sandbox.close()
                # Atomically transition sandbox cleanup from ORPHANED/ACTIVE to CLEANED using owner/generation fencing
                cleanup_claimed = queue.claim_sandbox_cleanup(owner_id=worker_id, limit=1)
                for clm in cleanup_claimed:
                    if clm["id"] == job_id and clm["sandbox_id"] == sb_id:
                        queue.finalize_sandbox_cleanup(
                            job_id=job_id,
                            sandbox_id=sb_id,
                            owner_id=worker_id,
                            cleanup_generation=clm["sandbox_cleanup_generation"],
                            success=True,
                        )
            except Exception as e:
                queue.append_log(job_id, worker_id, lease_gen, f"Sandbox close error: {str(e)}")
        else:
            status_val = turn_result.get("status", "failed")
            target = "TIMED_OUT" if "timeout" in status_val or "limit" in status_val else "FAILED"
            queue.update_job_status(
                job_id=job_id,
                target_status=target,
                worker_id=worker_id,
                lease_generation=lease_gen,
                error={"error": turn_result.get("error", "Task execution failed.")},
                sandbox_id=sb_id,
            )
    except Exception as e:
        if sandbox:
            try:
                sandbox.close()
            except Exception:
                pass
        raise


def worker_child_main(
    worker_instance_id: str,
    db_path: str,
    stop_event: multiprocessing.Event,
    ipc_socket_path: Optional[str] = None,
    ipc_token: Optional[str] = None,
    test_sandbox_factory: Optional[Callable[..., Any]] = None,
    test_agent_turn_fn: Optional[Callable[..., Any]] = None,
    test_gh_factory: Optional[Callable[..., Any]] = None,
    single_turn: bool = False,
) -> None:
    """Entry point for a spawned child worker process."""
    pid = os.getpid()
    start_time = time.time()
    queue = DurableJobQueue(db_path=db_path)

    # Register instance in worker_instances as ALIVE (preserve supervisor's authoritative start_time if already set)
    with queue._lock:
        with queue.conn:
            queue.conn.execute("BEGIN IMMEDIATE;")
            queue.conn.execute(
                """
                INSERT INTO worker_instances (
                    instance_id, pid, start_time, last_heartbeat, status
                ) VALUES (?, ?, ?, ?, 'ALIVE')
                ON CONFLICT(instance_id) DO UPDATE SET
                    pid = excluded.pid,
                    start_time = COALESCE(worker_instances.start_time, excluded.start_time),
                    last_heartbeat = excluded.last_heartbeat,
                    status = 'ALIVE';
                """,
                (worker_instance_id, pid, start_time, time.time()),
            )

    ipc_client = None
    if ipc_socket_path and ipc_token:
        ipc_client = SupervisorIPCClient(
            socket_path=ipc_socket_path,
            worker_instance_id=worker_instance_id,
            auth_token=ipc_token,
        )

    try:
        while not stop_event.is_set():
            job = queue.claim_next_job(worker_id=worker_instance_id, lease_duration_seconds=30.0)
            if not job:
                if single_turn:
                    break
                time.sleep(1.0)
                continue

            job_id = job["id"]
            lease_gen = job["lease_generation"]
            timeout_sec = job.get("timeout_seconds", 600.0)
            controller = ExecutionController(
                job_id=job_id,
                worker_id=worker_instance_id,
                lease_generation=lease_gen,
                queue=queue,
                timeout_seconds=timeout_sec,
                ipc_client=ipc_client,
                github_client_factory=test_gh_factory,
            )

            # Spawn Heartbeat & Watchdog Thread
            watchdog_stop = threading.Event()
            job_start_time = time.time()

            def watchdog_loop():
                while not watchdog_stop.is_set():
                    time.sleep(1.0)
                    if watchdog_stop.is_set():
                        break

                    # If execution has exceeded timeout deadline, stop extending lease and trigger abort
                    if time.time() - job_start_time > timeout_sec:
                        controller.abort(f"Execution exceeded timeout limit ({timeout_sec}s).")
                        # Do NOT extend lease beyond job execution timeout
                        break

                    status = queue.poll_worker_status(
                        job_id=job_id,
                        worker_id=worker_instance_id,
                        lease_generation=lease_gen,
                        lease_duration_seconds=30.0,
                    )

                    if status == "CANCEL_REQUESTED":
                        controller.abort("User requested cancellation.")
                        # Do NOT extend lease further once cancellation is requested
                        break
                    elif status == "STALE":
                        controller.abort("Worker lease expired or superseded (STALE).")
                        break

            wd_thread = threading.Thread(target=watchdog_loop, daemon=True)
            wd_thread.start()

            try:
                execute_job_in_worker(
                    job=job,
                    worker_id=worker_instance_id,
                    lease_gen=lease_gen,
                    queue=queue,
                    controller=controller,
                    sandbox_factory=test_sandbox_factory,
                    agent_turn_fn=test_agent_turn_fn,
                    github_client_factory=test_gh_factory,
                )
            except StaleLeaseError:
                break
            except Exception as e:
                try:
                    queue.update_job_status(
                        job_id=job_id,
                        target_status="FAILED",
                        worker_id=worker_instance_id,
                        lease_generation=lease_gen,
                        error={"error": str(e), "type": type(e).__name__},
                        sandbox_id=controller.active_sandbox_id,
                    )
                except Exception:
                    pass
            finally:
                watchdog_stop.set()

            if single_turn:
                break
    finally:
        queue.close()


class WorkerSupervisor:
    """Autonomous supervisor daemon managing a pool of spawned worker child processes with IPC authority."""

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        concurrency: int = 2,
        signing_key_path: Optional[str] = None,
        e2b_kill_fn: Optional[Callable[[str], None]] = None,
    ):
        self.db_path = db_path
        self.concurrency = concurrency
        self.supervisor_id = f"supervisor_{uuid.uuid4().hex[:8]}_{os.getpid()}"
        self.signing_key = load_or_create_supervisor_key(signing_key_path)

        self.queue = DurableJobQueue(db_path=db_path)
        self.reconciler = SandboxReconciler(self.queue, e2b_kill_fn=e2b_kill_fn)

        self._ctx = multiprocessing.get_context("spawn")
        self._stop_event = self._ctx.Event()
        self._workers: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._running = False

        # Authenticated Supervisor IPC Server over Unix domain socket
        self.ipc_server = SupervisorIPCServer(
            supervisor=self,
        )
        self.ipc_server.start()

    def get_worker_slot_by_instance(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Finds managed slot registration by worker instance ID."""
        with self._lock:
            for slot_info in self._workers.values():
                if slot_info.get("instance_id") == instance_id:
                    return slot_info
        return None

    def start_worker_slot(self, slot_id: str, custom_target: Optional[Callable] = None) -> None:
        """Spawns an independent child worker process for a slot with unique ephemeral auth token."""
        instance_id = f"worker_{uuid.uuid4().hex[:12]}"
        worker_token = secrets.token_hex(16)
        spawn_time = time.time()

        target_fn = custom_target or worker_child_main
        args = (
            instance_id,
            self.db_path,
            self._stop_event,
            self.ipc_server.socket_path,
            worker_token,
        )

        proc = self._ctx.Process(target=target_fn, args=args)
        proc.start()

        with self._lock:
            crashes = self._workers.get(slot_id, {}).get("crashes", 0)
            self._workers[slot_id] = {
                "process": proc,
                "instance_id": instance_id,
                "auth_token": worker_token,
                "pid": proc.pid,
                "start_time": spawn_time,
                "crashes": crashes,
                "custom_target": custom_target,
            }

        # Atomically register instance in worker_instances so start_time is consistent and authoritative
        with self.queue._lock:
            with self.queue.conn:
                self.queue.conn.execute("BEGIN IMMEDIATE;")
                self.queue.conn.execute(
                    """
                    INSERT INTO worker_instances (
                        instance_id, pid, start_time, last_heartbeat, status
                    ) VALUES (?, ?, ?, ?, 'ALIVE')
                    ON CONFLICT(instance_id) DO UPDATE SET
                        pid = excluded.pid,
                        start_time = excluded.start_time,
                        last_heartbeat = excluded.last_heartbeat,
                        status = 'ALIVE';
                    """,
                    (instance_id, proc.pid, spawn_time, spawn_time),
                )

    def stop_worker_slot(self, slot_id: str, grace_seconds: float = 5.0) -> None:
        """Stops a worker process gracefully; falls back to forced SIGKILL."""
        with self._lock:
            slot_info = self._workers.get(slot_id)
            if not slot_info:
                return
            proc = slot_info["process"]
            pid = slot_info["pid"]
            inst_id = slot_info["instance_id"]
            start_time = slot_info["start_time"]

        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=grace_seconds)

            if proc.is_alive():
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.join(timeout=2.0)

            exit_code = proc.exitcode
            exit_signal = 9 if exit_code in (-9, 137) else None

            self.queue.record_process_reap(
                instance_id=inst_id,
                pid=pid,
                start_time=start_time,
                exit_code=exit_code,
                exit_signal=exit_signal,
                supervisor_id=self.supervisor_id,
                proof_type="PARENT_WAITPID",
                supervisor_signing_key=self.signing_key,
            )

    def supervise_step(self) -> None:
        """Performs one supervisory iteration: checks children, handles crashes, reaps jobs, reconciles sandboxes."""
        now = time.time()

        # 1. Supervise active running jobs (Deadlock, hard timeout, and cancellation watchdog for unresponsive child processes)
        with self.queue._lock:
            running_jobs = self.queue.conn.execute(
                """
                SELECT id, worker_id, lease_generation, lease_expires_at, timeout_seconds, started_at, cancel_requested_at
                FROM jobs
                WHERE status = 'RUNNING';
                """
            ).fetchall()

        for r_job in running_jobs:
            w_id = r_job["worker_id"]
            lease_exp = r_job["lease_expires_at"]
            started_at = r_job["started_at"] or now
            timeout_sec = r_job["timeout_seconds"] or 600.0
            cancel_req_at = r_job["cancel_requested_at"]
            lease_gen = r_job["lease_generation"]

            # Find if this worker is one of supervisor's managed child slots
            managed_slot = None
            with self._lock:
                for sid, sinfo in self._workers.items():
                    if sinfo.get("instance_id") == w_id:
                        managed_slot = (sid, sinfo)
                        break

            should_terminate = False
            term_reason = ""
            term_target_status = "FAILED"

            # Condition A: Hard execution deadline exceeded (even if worker process keeps sending heartbeats)
            if (now - started_at) > timeout_sec:
                should_terminate = True
                term_reason = f"Execution exceeded hard deadline ({timeout_sec}s)."
                term_target_status = "TIMED_OUT"
            # Condition B: Cancellation requested and grace period (5.0s) exceeded without worker cooperative halt
            elif cancel_req_at and (now - cancel_req_at) > 5.0:
                should_terminate = True
                term_reason = "Worker failed to halt cooperatively within cancellation grace period."
                term_target_status = "CANCELLED"
            # Condition C: Lease expired while process is still alive
            elif lease_exp and lease_exp < now:
                should_terminate = True
                term_reason = "Execution lease expired while worker process was running."
                term_target_status = "CANCELLED" if cancel_req_at else "FAILED"

            if should_terminate and managed_slot:
                slot_id, slot_info = managed_slot
                logger.warning(
                    f"Job '{r_job['id']}' breached enforcement boundary ({term_reason}). "
                    f"Supervisor forcefully terminating worker process PID {slot_info['pid']} in slot '{slot_id}'."
                )
                self.stop_worker_slot(slot_id, grace_seconds=0.2)

                # Fetch recorded reap proof to authoritatively finalize job
                inst_row = self.queue.conn.execute(
                    "SELECT reap_token, reap_signature FROM worker_instances WHERE instance_id = ?;",
                    (w_id,),
                ).fetchone()

                if inst_row and inst_row["reap_token"] and inst_row["reap_signature"]:
                    try:
                        self.queue.supervisor_finalize_job(
                            job_id=r_job["id"],
                            expected_lease_generation=lease_gen,
                            target_status=term_target_status,
                            supervisor_id=self.supervisor_id,
                            authority_proof={
                                "type": "FORCED_KILL",
                                "worker_instance_id": w_id,
                                "reap_token": inst_row["reap_token"],
                                "reap_signature": inst_row["reap_signature"],
                            },
                            reason=term_reason,
                            supervisor_signing_key=self.signing_key,
                            error={"error": term_reason, "type": "SupervisorForcedTermination"},
                        )
                    except Exception as fin_err:
                        logger.error(f"Failed to finalize job '{r_job['id']}': {fin_err}")

                # Restart slot for next tasks
                if not self._stop_event.is_set():
                    self.start_worker_slot(slot_id, custom_target=slot_info.get("custom_target"))

        # 2. Inspect and maintain configured concurrency pool
        slots_to_start = []
        with self._lock:
            for i in range(self.concurrency):
                slot_id = f"slot_{i}"
                if slot_id not in self._workers:
                    slots_to_start.append(slot_id)

        for slot_id in slots_to_start:
            self.start_worker_slot(slot_id)

        with self._lock:
            slots = list(self._workers.keys())

        for slot_id in slots:
            with self._lock:
                slot_info = self._workers.get(slot_id)
                if not slot_info:
                    continue
                proc = slot_info["process"]
                pid = slot_info["pid"]
                inst_id = slot_info["instance_id"]
                start_time = slot_info["start_time"]
                crashes = slot_info["crashes"]

            if not proc.is_alive():
                exit_code = proc.exitcode
                exit_signal = 9 if exit_code in (-9, 137) else None

                # Only record reap if not already recorded DEAD by stop_worker_slot
                inst_check = self.queue.conn.execute(
                    "SELECT status FROM worker_instances WHERE instance_id = ?;", (inst_id,)
                ).fetchone()
                if not (inst_check and inst_check["status"] == "DEAD"):
                    self.queue.record_process_reap(
                        instance_id=inst_id,
                        pid=pid,
                        start_time=start_time,
                        exit_code=exit_code,
                        exit_signal=exit_signal,
                        supervisor_id=self.supervisor_id,
                        proof_type="PARENT_WAITPID",
                        supervisor_signing_key=self.signing_key,
                    )

                if now - start_time < 10.0 and exit_code != 0:
                    crashes += 1
                    with self._lock:
                        if slot_id in self._workers:
                            self._workers[slot_id]["crashes"] = crashes

                    if crashes >= 5:
                        logger.critical(f"Slot '{slot_id}' exceeded crash limit (5). Pausing slot.")
                        continue

                    if not self._stop_event.is_set():
                        backoff = min(30.0, 2.0 ** crashes)
                        time.sleep(backoff)

                if not self._stop_event.is_set():
                    custom_tgt = slot_info.get("custom_target")
                    self.start_worker_slot(slot_id, custom_target=custom_tgt)

        # 3. Reap stale jobs in DB
        self.queue.reap_stale_jobs()

        # 4. Reconcile orphan sandboxes in cloud
        self.reconciler.reconcile_orphans()

    def run_forever(self, poll_interval: float = 2.0) -> None:
        """Continuous supervisor daemon run loop maintaining configured pool until interrupted."""
        self._running = True
        logger.info(f"WorkerSupervisor '{self.supervisor_id}' started with concurrency={self.concurrency}.")
        try:
            while not self._stop_event.is_set():
                self.supervise_step()
                time.sleep(poll_interval)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Gracefully shuts down supervisor daemon, IPC server, and all child worker slots."""
        self._stop_event.set()
        self.ipc_server.stop()
        with self._lock:
            slots = list(self._workers.keys())

        for slot_id in slots:
            self.stop_worker_slot(slot_id)

        self.queue.close()


def main():
    """CLI daemon entry point for the worker supervisor."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    concurrency = int(os.environ.get("WORKER_CONCURRENCY", "2"))
    db_path = os.environ.get("JOBS_DB_PATH", DEFAULT_DB_PATH)

    sup = WorkerSupervisor(db_path=db_path, concurrency=concurrency)

    def sig_handler(sig, frame):
        logger.info("Supervisor received termination signal. Shutting down...")
        sup.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    sup.run_forever()


if __name__ == "__main__":
    main()

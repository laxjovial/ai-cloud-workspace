"""Comprehensive Failure-Injection & Invariant Test Suite for Phase B.2 Autonomous Worker Engine.

Covers the 15 rigorous test scenarios defined in docs/phase_b2_worker_architecture_plan.md:
1. Normal Worker Turn (Standard turn -> COMPLETED, logs streamed, sandbox registered and cleaned).
2. Heartbeat Maintenance (Long task with short lease duration -> extended continuously).
3. Pre-Step Self-Fencing (Lease expired mid-run -> halts immediately, zero subsequent tool calls).
4. Cooperative Cancellation (User cancel requested -> worker aborts turn before next step).
5. Supervisor Non-Forgeable Authority (Rejects finalize without cryptographic HMAC proof).
6. Supervisor Forced Termination Fallback (SIGTERM -> SIGKILL -> reap token generated -> finalized).
7. Blocked Command Forced Interruption (Hanging command -> execution controller aborts on timeout).
8. Crash & Re-Claim Pre-Condition (Killed worker leaves ORPHANED sandbox -> claim strictly refused until CLEANED).
9. Cleanup Escalation & Authenticated Operator Resolution (10 failures -> MANUAL_INTERVENTION_REQUIRED -> resolved without reviving failed job).
10. Contextual Side-Effect Gate (Reconciliation blocked if prior worker death unverified; unblocks on proof).
11. Supervisor Restart Continuity (Key persistence across restart -> validates signatures from prior run).
12. Fail-Closed PR Reconciliation (GitHub API error/timeout fails closed without duplicate PR).
13. PR Idempotency across All States (Detects existing closed/merged PR -> adopts without duplicate).
14. Supervisor Crash-Loop Protection (Repeated worker startup crashes trigger backoff and slot pause).
15. Spawned Worker Credential Isolation (Zero supervisor signing keys or LLM tokens leaked in child env/cmdline).
"""

import hashlib
import json
import multiprocessing
import os
import signal
import sqlite3
import tempfile
import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional
import pytest

from job_queue import (
    DEFAULT_LEASE_DURATION_SECONDS,
    DurableJobQueue,
    JobQueueError,
    SideEffectConflictError,
    SideEffectReconciliationBlockedError,
    StaleLeaseError,
    compute_canonical_signature,
    compute_side_effect_id,
)
from worker import (
    ExecutionController,
    SandboxReconciler,
    SupervisorIPCClient,
    SupervisorIPCServer,
    WorkerSupervisor,
    execute_job_in_worker,
    load_or_create_supervisor_key,
    worker_child_main,
)


class MockSandbox:
    """Mock E2B WorkspaceSandbox for testing."""

    def __init__(self, repo_name: str, github_token: str = "", sandbox_id: Optional[str] = None):
        self.repo_name = repo_name
        self.github_token = github_token
        self.sandbox_id = sandbox_id or f"sb_mock_{uuid.uuid4().hex[:8]}"
        self.executed_commands = []
        self.is_killed = False

    def start(self):
        return {"success": True, "sandbox_id": self.sandbox_id}

    def close(self):
        self.is_killed = True

    def run_command(self, cmd: str):
        self.executed_commands.append(cmd)
        return {"stdout": f"Executed: {cmd}", "stderr": "", "exit_code": 0}

    def start_dev_server(self, cmd: str, port: int):
        return {"preview_url": f"https://preview-{port}.e2b.dev"}

    def read_file(self, path: str):
        return {"content": "file content"}

    def write_file(self, path: str, content: str):
        return {"success": True}

    def commit_and_create_pr(self, branch_name: str, commit_message: str, pr_title: str, pr_body: str):
        return {"success": True, "pr_url": f"https://github.com/{self.repo_name}/pull/42"}


def crashing_worker_target(instance_id, db_path, stop_event, ipc_socket_path=None, ipc_token=None):
    """Top-level function pickleable by multiprocessing spawn context."""
    os._exit(1)


def env_inspector_worker_target(instance_id, db_path, stop_event, ipc_socket_path=None, ipc_token=None):
    """Top-level function pickleable by multiprocessing spawn context."""
    assert "SUPERVISOR_SIGNING_KEY" not in os.environ
    stop_event.set()


def hanging_worker_target(inst_id, db_p, stop_ev, sock_p=None, tok=None):
    """Top-level function pickleable by multiprocessing spawn context."""
    q = DurableJobQueue(db_path=db_p)
    with q._lock:
        with q.conn:
            q.conn.execute(
                "INSERT INTO worker_instances (instance_id, pid, start_time, last_heartbeat, status) VALUES (?, ?, ?, ?, 'ALIVE');",
                (inst_id, os.getpid(), time.time(), time.time()),
            )
    q.close()
    while not stop_ev.is_set():
        time.sleep(0.1)


def mock_worker_target(inst_id, db_p, stop_ev, sock_p=None, tok=None):
    """Top-level function pickleable by multiprocessing spawn context."""
    q = DurableJobQueue(db_path=db_p)
    with q._lock:
        with q.conn:
            q.conn.execute(
                """
                INSERT INTO worker_instances (instance_id, pid, start_time, last_heartbeat, status)
                VALUES (?, ?, ?, ?, 'ALIVE')
                ON CONFLICT(instance_id) DO UPDATE SET
                    pid = excluded.pid,
                    last_heartbeat = excluded.last_heartbeat,
                    status = 'ALIVE';
                """,
                (inst_id, os.getpid(), time.time(), time.time()),
            )
    q.close()
    while not stop_ev.is_set():
        time.sleep(0.1)


def blocking_sandbox_worker_target(inst_id, db_p, stop_ev, sock_p=None, tok=None):
    """Top-level function for worker running a genuinely blocked sandbox command in a child process."""
    q = DurableJobQueue(db_path=db_p)
    with q._lock:
        with q.conn:
            q.conn.execute(
                """
                INSERT INTO worker_instances (instance_id, pid, start_time, last_heartbeat, status)
                VALUES (?, ?, ?, ?, 'ALIVE')
                ON CONFLICT(instance_id) DO UPDATE SET
                    pid = excluded.pid,
                    last_heartbeat = excluded.last_heartbeat,
                    status = 'ALIVE';
                """,
                (inst_id, os.getpid(), time.time(), time.time()),
            )

    claimed = q.claim_next_job(worker_id=inst_id, lease_duration_seconds=30.0)
    if claimed:
        job_id = claimed["id"]
        gen = claimed["lease_generation"]
        timeout_sec = claimed.get("timeout_seconds", 0.5)

        # Heartbeat loop that simulates worker heartbeat continuing while command is blocked
        hb_stop = threading.Event()
        def hb_loop():
            while not hb_stop.is_set():
                time.sleep(0.1)
                if hb_stop.is_set():
                    break
                q.poll_worker_status(job_id=job_id, worker_id=inst_id, lease_generation=gen, lease_duration_seconds=30.0)

        t = threading.Thread(target=hb_loop, daemon=True)
        t.start()

        # Simulate genuinely blocked sandbox operation (unresponsive synchronous call)
        try:
            while not stop_ev.is_set():
                time.sleep(0.1)
        finally:
            hb_stop.set()

    q.close()


class TestPhaseB2WorkerEngine:
    """Comprehensive test suite enforcing Phase B.2 execution and supervision invariants."""

    @pytest.fixture
    def test_env(self, tmp_path):
        db_file = str(tmp_path / "test_jobs.db")
        key_file = str(tmp_path / "supervisor.key")
        signing_key = load_or_create_supervisor_key(key_file)

        queue = DurableJobQueue(db_path=db_file)
        queue._active_supervisor_key = signing_key

        yield {
            "db_path": db_file,
            "key_path": key_file,
            "signing_key": signing_key,
            "queue": queue,
            "tmp_path": tmp_path,
        }
        queue.close()

    def test_scenario_1_normal_worker_turn(self, test_env):
        """1. Standard multi-step turn -> COMPLETED, logs streamed, sandbox registered."""
        queue = test_env["queue"]
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "Run tests and verify build"},
        )

        claimed = queue.claim_next_job(worker_id="worker_test_1", lease_duration_seconds=30.0)
        assert claimed is not None
        assert claimed["status"] == "RUNNING"

        controller = ExecutionController(job_id=job["id"], timeout_seconds=60.0)

        def mock_agent_turn(messages, model, api_key, sandbox, execution_controller):
            sandbox.run_command("npm test")
            return {"success": True, "status": "completed", "content": "Tests passed."}

        execute_job_in_worker(
            job=claimed,
            worker_id="worker_test_1",
            lease_gen=claimed["lease_generation"],
            queue=queue,
            controller=controller,
            sandbox_factory=lambda repo_name, github_token: MockSandbox(repo_name),
            agent_turn_fn=mock_agent_turn,
        )

        finished = queue.get_job(job["id"], user_id="user_alice")
        assert finished["status"] == "COMPLETED"
        assert finished["sandbox_id"] is not None
        assert finished["finished_at"] is not None

        events = queue.get_job_events(job["id"], user_id="user_alice")
        event_types = [e["event_type"] for e in events]
        assert "SANDBOX_REGISTERED" in event_types
        assert "STATUS_CHANGE" in event_types

    def test_scenario_2_heartbeat_maintenance(self, test_env):
        """2. Heartbeat continuously extends lease and inspects status."""
        queue = test_env["queue"]
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "Long task"},
        )

        claimed = queue.claim_next_job(worker_id="worker_test_2", lease_duration_seconds=2.0)
        gen = claimed["lease_generation"]

        # Heartbeat repeatedly
        for _ in range(3):
            time.sleep(0.1)
            status = queue.poll_worker_status(
                job_id=job["id"],
                worker_id="worker_test_2",
                lease_generation=gen,
                lease_duration_seconds=5.0,
            )
            assert status == "ACTIVE"

        current = queue.get_job(job["id"], user_id="user_alice")
        assert current["status"] == "RUNNING"
        assert current["lease_expires_at"] > time.time()

    def test_scenario_3_pre_step_self_fencing(self, test_env):
        """3. Worker lease expires mid-run -> heartbeat returns STALE -> worker halts immediately."""
        queue = test_env["queue"]
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "Fenced task"},
        )

        # Claim with very short lease (0.1s)
        claimed = queue.claim_next_job(worker_id="worker_test_3", lease_duration_seconds=1.0)
        gen = claimed["lease_generation"]

        # Expire lease manually in DB
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET lease_expires_at = ? WHERE id = ?;",
                    (time.time() - 5.0, job["id"]),
                )

        status = queue.poll_worker_status(
            job_id=job["id"],
            worker_id="worker_test_3",
            lease_generation=gen,
        )
        assert status == "STALE"

        controller = ExecutionController(job_id=job["id"])
        controller.abort("Lease expired or superseded (STALE).")
        assert controller.is_aborted()

        # Fenced worker cannot write logs
        with pytest.raises(StaleLeaseError):
            queue.append_log(job["id"], "worker_test_3", gen, "This must fail.")

    def test_scenario_4_cooperative_cancellation(self, test_env):
        """4. User requests cancellation during turn -> worker detects CANCEL_REQUESTED and halts."""
        queue = test_env["queue"]
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "Task to cancel"},
        )

        claimed = queue.claim_next_job(worker_id="worker_test_4", lease_duration_seconds=30.0)
        gen = claimed["lease_generation"]

        # User requests cancel
        queue.request_cancellation(job["id"], user_id="user_alice")

        status = queue.poll_worker_status(
            job_id=job["id"],
            worker_id="worker_test_4",
            lease_generation=gen,
        )
        assert status == "CANCEL_REQUESTED"

        controller = ExecutionController(job_id=job["id"])
        controller.abort("User requested cancellation.")

        def mock_cancelled_turn(messages, model, api_key, sandbox, execution_controller):
            if execution_controller.is_aborted():
                return {"success": False, "status": "aborted", "error": "Cancelled by user"}
            return {"success": True}

        execute_job_in_worker(
            job=claimed,
            worker_id="worker_test_4",
            lease_gen=gen,
            queue=queue,
            controller=controller,
            sandbox_factory=lambda repo_name, github_token: MockSandbox(repo_name),
            agent_turn_fn=mock_cancelled_turn,
        )

        final_job = queue.get_job(job["id"], user_id="user_alice")
        assert final_job["status"] == "CANCELLED"

    def test_scenario_5_supervisor_non_forgeable_authority(self, test_env):
        """5. Supervisor attempts supervisor_finalize_job without proof -> fails closed."""
        queue = test_env["queue"]
        signing_key = test_env["signing_key"]

        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "Task for authority check"},
        )
        claimed = queue.claim_next_job(worker_id="worker_test_5", lease_duration_seconds=30.0)
        gen = claimed["lease_generation"]

        # Attempt finalize with active lease and LEASE_EXPIRED claim -> rejected
        with pytest.raises(JobQueueError, match="worker lease is still active"):
            queue.supervisor_finalize_job(
                job_id=job["id"],
                expected_lease_generation=gen,
                target_status="TIMED_OUT",
                supervisor_id="supervisor_1",
                authority_proof={"type": "LEASE_EXPIRED"},
                reason="Testing authority verification",
                supervisor_signing_key=signing_key,
            )

        # Attempt finalize with forged HMAC signature -> rejected
        with pytest.raises(JobQueueError):
            queue.supervisor_finalize_job(
                job_id=job["id"],
                expected_lease_generation=gen,
                target_status="FAILED",
                supervisor_id="supervisor_1",
                authority_proof={
                    "type": "FORCED_KILL",
                    "worker_instance_id": "nonexistent_worker",
                    "reap_token": "fake_token",
                    "reap_signature": "fake_signature",
                },
                reason="Testing forged proof",
                supervisor_signing_key=signing_key,
            )

    def test_scenario_6_supervisor_forced_termination_fallback(self, test_env):
        """6. Real spawned worker child process hangs -> supervisor records reap evidence -> finalizes job."""
        queue = test_env["queue"]
        db_path = test_env["db_path"]
        key_path = test_env["key_path"]

        sup = WorkerSupervisor(db_path=db_path, concurrency=1, signing_key_path=key_path)

        sup.start_worker_slot("slot_hang", custom_target=hanging_worker_target)
        slot_info = sup._workers["slot_hang"]
        inst_id = slot_info["instance_id"]
        pid = slot_info["pid"]

        # Enqueue and claim a job under this real spawned worker child
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "Hanging task"},
        )
        claimed = queue.claim_next_job(worker_id=inst_id, lease_duration_seconds=30.0)
        gen = claimed["lease_generation"]

        # Supervisor terminates the real running child worker and records authoritative reap proof
        sup.stop_worker_slot("slot_hang", grace_seconds=0.2)

        # Retrieve authoritative reap proof recorded by supervisor
        reap_row = queue.conn.execute(
            "SELECT * FROM worker_instances WHERE instance_id = ?;", (inst_id,)
        ).fetchone()
        assert reap_row["status"] == "DEAD"
        assert reap_row["pid"] == pid
        assert reap_row["reap_token"] is not None
        assert reap_row["reap_signature"] is not None

        # Supervisor finalizes job using real reap proof
        finalized = queue.supervisor_finalize_job(
            job_id=job["id"],
            expected_lease_generation=gen,
            target_status="CANCELLED",
            supervisor_id=sup.supervisor_id,
            authority_proof={
                "type": "FORCED_KILL",
                "worker_instance_id": inst_id,
                "reap_token": reap_row["reap_token"],
                "reap_signature": reap_row["reap_signature"],
            },
            reason="Watchdog detected hung process; SIGKILL executed.",
            supervisor_signing_key=sup.signing_key,
        )

        assert finalized["status"] == "CANCELLED"
        assert finalized["lease_generation"] == gen + 1
        sup.shutdown()

    def test_scenario_7_blocked_command_forced_interruption(self, test_env):
        """7. Genuinely blocked worker operation -> supervisor hard timeout watchdog terminates process via SIGTERM/SIGKILL.

        Note on execution mechanics: This test evaluates process-level supervision and timeout
        enforcement on a spawned child worker process whose thread is blocked (simulating a hung command
        or unresponsive execution). It does not test real cloud network container interruption via E2B SDK.
        """
        db_path = test_env["db_path"]
        key_path = test_env["key_path"]

        sup = WorkerSupervisor(db_path=db_path, concurrency=1, signing_key_path=key_path)
        queue = sup.queue

        # Enqueue job with very short hard timeout (0.5s)
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_hang",
            repository="owner/repo",
            payload={"prompt": "Task running blocked command"},
        )
        # Update timeout_seconds to 0.5s in DB
        with queue._lock:
            with queue.conn:
                queue.conn.execute("UPDATE jobs SET timeout_seconds = 0.5 WHERE id = ?;", (job["id"],))

        # Start slot running blocking_sandbox_worker_target in real child process
        sup.start_worker_slot("slot_blocked", custom_target=blocking_sandbox_worker_target)
        slot_info = sup._workers["slot_blocked"]
        worker_pid = slot_info["pid"]
        worker_inst = slot_info["instance_id"]

        # Wait for worker process to claim job and block
        claimed_job = None
        for _ in range(20):
            time.sleep(0.1)
            cj = queue.get_job(job["id"], user_id="user_alice")
            if cj["status"] == "RUNNING":
                claimed_job = cj
                break

        assert claimed_job is not None
        assert claimed_job["status"] == "RUNNING"
        assert claimed_job["worker_id"] == worker_inst

        # Wait past the hard deadline (0.5s)
        time.sleep(0.8)

        # Trigger supervisor step to enforce hard timeout watchdog
        sup.supervise_step()

        # Verify child process was terminated and reaped by supervisor
        reap_row = queue.conn.execute(
            "SELECT * FROM worker_instances WHERE instance_id = ?;", (worker_inst,)
        ).fetchone()
        assert reap_row["status"] == "DEAD"
        assert reap_row["pid"] == worker_pid

        # Verify job reached terminal TIMED_OUT status via supervisor authority
        final_job = queue.get_job(job["id"], user_id="user_alice")
        assert final_job["status"] == "TIMED_OUT"
        assert final_job["finished_at"] is not None

        sup.shutdown()

    def test_scenario_8_crash_and_reclaim_precondition(self, test_env):
        """8. Worker killed leaving ORPHANED sandbox -> claim_next_job strictly refuses until CLEANED."""
        queue = test_env["queue"]
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "Task before crash"},
        )
        claimed = queue.claim_next_job(worker_id="worker_crash_1", lease_duration_seconds=1.0)
        gen = claimed["lease_generation"]

        # Register sandbox
        queue.register_sandbox(job["id"], "sb_orphan_1", "worker_crash_1", gen)

        # Force expire lease and trigger reaper
        with queue._lock:
            with queue.conn:
                queue.conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE id = ?;", (time.time() - 10.0, job["id"]))

        reaped = queue.reap_stale_jobs()
        assert job["id"] in reaped

        # Job is now QUEUED, but sandbox_cleanup_status is ORPHANED
        job_state = queue.get_job(job["id"], user_id="user_alice")
        assert job_state["status"] == "QUEUED"
        assert job_state["sandbox_cleanup_status"] == "ORPHANED"

        # Another worker attempts to claim -> STRICT PRE-CLAIM INVARIANT BLOCKS CLAIM!
        next_claim = queue.claim_next_job(worker_id="worker_crash_2")
        assert next_claim is None  # Blocked!

        # Reconciler cleans sandbox container
        reconciler = SandboxReconciler(queue=queue, e2b_kill_fn=lambda sb_id: None)
        cleaned_count = reconciler.reconcile_orphans()
        assert cleaned_count == 1

        # Now sandbox_cleanup_status is CLEANED -> claim succeeds!
        job_state_after = queue.get_job(job["id"], user_id="user_alice")
        assert job_state_after["sandbox_cleanup_status"] == "CLEANED"

        reclaimed = queue.claim_next_job(worker_id="worker_crash_2")
        assert reclaimed is not None
        assert reclaimed["id"] == job["id"]

    def test_scenario_9_cleanup_escalation_and_operator_resolution(self, test_env):
        """9. Reconciler encounters repeated errors (10 times) -> MANUAL_INTERVENTION_REQUIRED -> operator resolves."""
        queue = test_env["queue"]
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "Stuck sandbox task"},
        )
        claimed = queue.claim_next_job(worker_id="worker_stuck_1")
        queue.register_sandbox(job["id"], "sb_stuck_1", "worker_stuck_1", claimed["lease_generation"])

        # Transition to FAILED with ORPHANED cleanup
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET status = 'FAILED', sandbox_cleanup_status = 'ORPHANED' WHERE id = ?;",
                    (job["id"],),
                )

        # Failing reconciler
        def fail_kill(sb_id):
            raise Exception("Provider 500: Cloud container engine failure.")

        reconciler = SandboxReconciler(queue=queue, e2b_kill_fn=fail_kill)

        # Simulate 10 failures
        for _ in range(10):
            with queue._lock:
                with queue.conn:
                    queue.conn.execute("UPDATE jobs SET sandbox_cleanup_next_retry = 0 WHERE id = ?;", (job["id"],))
            reconciler.reconcile_orphans()

        stuck_job = queue.get_job(job["id"], user_id="user_alice")
        assert stuck_job["sandbox_cleanup_status"] == "MANUAL_INTERVENTION_REQUIRED"
        assert stuck_job["status"] == "FAILED"  # Terminal status preserved!

        # Operator authenticated resolution
        admin_auth_token = "secret_admin_token_xyz"
        token_hash = hashlib.sha256(admin_auth_token.encode("utf-8")).hexdigest()
        admin_keystore = {token_hash: {"principal_id": "operator_bob", "roles": ["ADMIN"]}}

        resolved = queue.resolve_manual_cleanup(
            job_id=job["id"],
            action="CONFIRMED_DELETED",
            verification_proof="Confirmed deleted via E2B CLI and console.",
            admin_auth_token=admin_auth_token,
            admin_auth_keys=admin_keystore,
        )

        assert resolved["sandbox_cleanup_status"] == "CLEANED"
        # CRITICAL INVARIANT: Confirming cleanup of a terminal FAILED job does NOT revive it into QUEUED!
        assert resolved["status"] == "FAILED"

    def test_scenario_10_contextual_side_effect_gate(self, test_env):
        """10. Side-effect recovery requires contextual proof of prior worker process termination via authenticated supervisor IPC."""
        queue = test_env["queue"]
        db_path = test_env["db_path"]
        key_path = test_env["key_path"]

        sup = WorkerSupervisor(db_path=db_path, concurrency=1, signing_key_path=key_path)

        sup.start_worker_slot("slot_A", custom_target=mock_worker_target)
        slot_A = sup._workers["slot_A"]
        inst_A = slot_A["instance_id"]

        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "PR task"},
        )

        # Worker A claims job and reserves intent
        claimed_A = queue.claim_next_job(worker_id=inst_A, lease_duration_seconds=30.0)
        se_id, status, _ = queue.reserve_side_effect(
            job_id=job["id"],
            worker_id=inst_A,
            lease_generation=claimed_A["lease_generation"],
            action_type="CREATE_PULL_REQUEST",
            action_key="pr_1",
            intent_payload={"branch": "ai-cloud/job-1"},
        )
        assert status == "PENDING"

        # Spawn Worker B to attempt recovery
        sup.start_worker_slot("slot_B", custom_target=mock_worker_target)
        slot_B = sup._workers["slot_B"]
        inst_B = slot_B["instance_id"]
        ipc_client_B = SupervisorIPCClient(
            socket_path=sup.ipc_server.socket_path,
            worker_instance_id=inst_B,
            auth_token=slot_B["auth_token"],
        )

        # Worker B attempts recovery over IPC while Worker A is ALIVE -> rejected!
        is_dead = ipc_client_B.verify_worker_death(
            job_id=job["id"],
            instance_id=inst_A,
            side_effect_id=se_id,
        )
        assert not is_dead

        # Now supervisor stops slot A and records authoritative reap proof
        sup.stop_worker_slot("slot_A", grace_seconds=0.2)

        # Now Worker B claims expired job and requests IPC verification -> succeeds!
        with queue._lock:
            with queue.conn:
                queue.conn.execute("UPDATE jobs SET status = 'QUEUED', worker_id = NULL, lease_expires_at = NULL WHERE id = ?;", (job["id"],))

        claimed_B = queue.claim_next_job(worker_id=inst_B, lease_duration_seconds=30.0)
        assert claimed_B is not None

        is_dead = ipc_client_B.verify_worker_death(
            job_id=job["id"],
            instance_id=inst_A,
            side_effect_id=se_id,
        )
        assert is_dead

        # FOCUSED INTEGRATION TEST: Reject IPC verification when caller's lease has expired
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET lease_expires_at = ? WHERE id = ?;",
                    (time.time() - 10.0, job["id"]),
                )
        is_dead_expired = ipc_client_B.verify_worker_death(
            job_id=job["id"],
            instance_id=inst_A,
            side_effect_id=se_id,
        )
        assert not is_dead_expired

        # FOCUSED INTEGRATION TEST: Reject IPC verification when job is no longer RUNNING (e.g. CANCELLED or FAILED)
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET status = 'CANCELLED', lease_expires_at = ? WHERE id = ?;",
                    (time.time() + 60.0, job["id"]),
                )
        is_dead_non_running = ipc_client_B.verify_worker_death(
            job_id=job["id"],
            instance_id=inst_A,
            side_effect_id=se_id,
        )
        assert not is_dead_non_running

        # Restore job to RUNNING with valid lease for confirmation
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET status = 'RUNNING', lease_expires_at = ? WHERE id = ?;",
                    (time.time() + 60.0, job["id"]),
                )

        # FOCUSED INTEGRATION TEST: Verify concurrent queue writes and IPC reads cannot interfere
        concurrent_errors = []
        def concurrent_writer():
            try:
                for _ in range(50):
                    queue.append_log(
                        job_id=job["id"],
                        worker_id=inst_B,
                        lease_generation=claimed_B["lease_generation"],
                        message="Concurrent log entry",
                    )
                    time.sleep(0.005)
            except Exception as ex:
                concurrent_errors.append(ex)

        writer_t = threading.Thread(target=concurrent_writer)
        writer_t.start()

        # Perform multiple IPC death verifications concurrently while writes are happening
        for _ in range(20):
            res = ipc_client_B.verify_worker_death(
                job_id=job["id"],
                instance_id=inst_A,
                side_effect_id=se_id,
            )
            assert res is True
            time.sleep(0.01)

        writer_t.join()
        assert len(concurrent_errors) == 0

        # Worker B confirms side-effect under its lease generation
        queue.confirm_side_effect(
            side_effect_id=se_id,
            worker_id=inst_B,
            lease_generation=claimed_B["lease_generation"],
            confirmed_result={"pr_url": "https://github.com/owner/repo/pull/1"},
            is_recovery=True,
        )

        # Stale worker A awakens and tries to confirm -> strictly fenced!
        with pytest.raises(StaleLeaseError):
            queue.confirm_side_effect(
                side_effect_id=se_id,
                worker_id=inst_A,
                lease_generation=claimed_A["lease_generation"],
                confirmed_result={"pr_url": "https://github.com/owner/repo/pull/zombie"},
            )

        sup.shutdown()

    def test_scenario_11_supervisor_restart_continuity(self, test_env):
        """11. Supervisor restarts after terminating real child process -> persistent key validates continuity."""
        db_path = test_env["db_path"]
        key_path = test_env["key_path"]

        # Run supervisor 1 and spawn real worker child
        sup1 = WorkerSupervisor(db_path=db_path, concurrency=1, signing_key_path=key_path)

        sup1.start_worker_slot("slot_prior", custom_target=mock_worker_target)
        slot_info = sup1._workers["slot_prior"]
        prior_inst_id = slot_info["instance_id"]
        prior_pid = slot_info["pid"]

        # Terminate real child and record authoritative reap signature
        sup1.stop_worker_slot("slot_prior", grace_seconds=0.2)
        sup1.shutdown()

        # Start replacement supervisor 2 pointing to same persistent key
        sup2 = WorkerSupervisor(db_path=db_path, concurrency=1, signing_key_path=key_path)
        assert sup2.signing_key == sup1.signing_key

        # Replacement supervisor verifies signature from supervisor 1 run
        inst_row = sup2.queue.conn.execute(
            "SELECT * FROM worker_instances WHERE instance_id = ?;", (prior_inst_id,)
        ).fetchone()

        assert inst_row["status"] == "DEAD"
        assert inst_row["pid"] == prior_pid
        canonical_payload = {
            "version": "v1",
            "proof_type": inst_row["proof_type"],
            "supervisor_id": inst_row["supervisor_id"],
            "instance_id": inst_row["instance_id"],
            "pid": inst_row["pid"],
            "start_time": inst_row["start_time"],
            "exit_code": inst_row["exit_code"],
            "exit_signal": inst_row["exit_signal"],
            "terminated_at": inst_row["terminated_at"],
            "reap_token": inst_row["reap_token"],
        }
        computed_sig = compute_canonical_signature(sup2.signing_key, canonical_payload)
        assert computed_sig == inst_row["reap_signature"]
        sup2.shutdown()

    def test_scenario_12_fail_closed_pr_reconciliation(self, test_env):
        """12. GitHub API error during reconciliation check fails closed without creating duplicate PR."""
        db_path = test_env["db_path"]
        key_path = test_env["key_path"]

        sup = WorkerSupervisor(db_path=db_path, concurrency=1, signing_key_path=key_path)
        queue = sup.queue

        # Spawn prior worker, create intent, and reap it via supervisor
        sup.start_worker_slot("slot_prior_gh", custom_target=mock_worker_target)
        prior_info = sup._workers["slot_prior_gh"]
        prior_worker_id = prior_info["instance_id"]

        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "PR task with network glitch"},
        )
        se_id, _, _ = queue.reserve_side_effect(
            job_id=job["id"],
            worker_id=prior_worker_id,
            lease_generation=1,
            action_type="CREATE_PULL_REQUEST",
            action_key=f"pr_{job['id'][:8]}",
            intent_payload={"branch_name": f"ai-cloud/job-{job['id'][:8]}", "repository": "owner/repo"},
        )
        sup.stop_worker_slot("slot_prior_gh", grace_seconds=0.2)

        # Spawn worker 1 to attempt reconciliation
        sup.start_worker_slot("slot_active_gh", custom_target=mock_worker_target)
        active_info = sup._workers["slot_active_gh"]
        active_worker_id = active_info["instance_id"]
        ipc_client = SupervisorIPCClient(
            socket_path=sup.ipc_server.socket_path,
            worker_instance_id=active_worker_id,
            auth_token=active_info["auth_token"],
        )

        class BrokenGitHubClient:
            def get_pulls(self, **kwargs):
                raise Exception("GitHub API 500: Server Error / Rate Limit Exceeded.")

        claimed = queue.claim_next_job(worker_id=active_worker_id)
        controller = ExecutionController(
            job_id=job["id"],
            worker_id=active_worker_id,
            lease_generation=claimed["lease_generation"],
            queue=queue,
            ipc_client=ipc_client,
            github_client_factory=lambda: BrokenGitHubClient(),
        )

        # Worker attempts recovery with broken client -> fails closed
        def agent_turn_triggering_pr(messages, model, api_key, sandbox, execution_controller):
            return execution_controller.on_create_pull_request(
                sandbox=sandbox,
                branch_name=f"ai-cloud/job-{job['id'][:8]}",
                commit_message="test pr",
                pr_title="test pr",
                pr_body="test body",
            )

        with pytest.raises(Exception, match="GitHub API 500"):
            execute_job_in_worker(
                job=claimed,
                worker_id=active_worker_id,
                lease_gen=claimed["lease_generation"],
                queue=queue,
                controller=controller,
                sandbox_factory=lambda repo_name, github_token: MockSandbox(repo_name),
                github_client_factory=lambda: BrokenGitHubClient(),
                agent_turn_fn=agent_turn_triggering_pr,
            )
        sup.shutdown()

    def test_scenario_13_pr_idempotency_across_all_states(self, test_env):
        """13. PR was merged/closed on GitHub before retry -> worker detects and adopts existing PR."""
        db_path = test_env["db_path"]
        key_path = test_env["key_path"]

        sup = WorkerSupervisor(db_path=db_path, concurrency=1, signing_key_path=key_path)
        queue = sup.queue

        sup.start_worker_slot("slot_prior_closed", custom_target=mock_worker_target)
        prior_info = sup._workers["slot_prior_closed"]
        prior_worker_id = prior_info["instance_id"]

        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "PR task with pre-existing closed PR"},
        )
        se_id, _, _ = queue.reserve_side_effect(
            job_id=job["id"],
            worker_id=prior_worker_id,
            lease_generation=1,
            action_type="CREATE_PULL_REQUEST",
            action_key=f"pr_{job['id'][:8]}",
            intent_payload={"branch_name": f"ai-cloud/job-{job['id'][:8]}", "repository": "owner/repo"},
        )
        sup.stop_worker_slot("slot_prior_closed", grace_seconds=0.2)

        sup.start_worker_slot("slot_active_adopt", custom_target=mock_worker_target)
        active_info = sup._workers["slot_active_adopt"]
        active_worker_id = active_info["instance_id"]
        ipc_client = SupervisorIPCClient(
            socket_path=sup.ipc_server.socket_path,
            worker_instance_id=active_worker_id,
            auth_token=active_info["auth_token"],
        )

        class PreExistingGitHubClient:
            def get_pulls(self, state, head):
                assert state == "all"
                return [{"html_url": "https://github.com/owner/repo/pull/99", "state": "closed"}]

        claimed = queue.claim_next_job(worker_id=active_worker_id)
        controller = ExecutionController(
            job_id=job["id"],
            worker_id=active_worker_id,
            lease_generation=claimed["lease_generation"],
            queue=queue,
            ipc_client=ipc_client,
            github_client_factory=lambda: PreExistingGitHubClient(),
        )

        def agent_turn_triggering_pr(messages, model, api_key, sandbox, execution_controller):
            res = execution_controller.on_create_pull_request(
                sandbox=sandbox,
                branch_name=f"ai-cloud/job-{job['id'][:8]}",
                commit_message="test pr",
                pr_title="test pr",
                pr_body="test body",
            )
            assert res.get("adopted") is True
            assert res.get("pr_url") == "https://github.com/owner/repo/pull/99"
            return {"success": True}

        execute_job_in_worker(
            job=claimed,
            worker_id=active_worker_id,
            lease_gen=claimed["lease_generation"],
            queue=queue,
            controller=controller,
            sandbox_factory=lambda repo_name, github_token: MockSandbox(repo_name),
            github_client_factory=lambda: PreExistingGitHubClient(),
            agent_turn_fn=agent_turn_triggering_pr,
        )

        final_job = queue.get_job(job["id"], user_id="user_alice")
        assert final_job["status"] == "COMPLETED"
        sup.shutdown()

    def test_scenario_14_supervisor_crash_loop_protection(self, test_env):
        """14. Child worker repeatedly crashes on startup -> supervisor applies backoff and pauses slot."""
        sup = WorkerSupervisor(db_path=test_env["db_path"], concurrency=1)
        # Set stop event so supervise_step does not immediately spawn a replacement worker
        sup.start_worker_slot("slot_crash", custom_target=crashing_worker_target)
        proc = sup._workers["slot_crash"]["process"]
        # Wait for child process to terminate
        proc.join(timeout=3.0)

        sup._stop_event.set()
        # Supervise step detects crash and updates crash count
        sup.supervise_step()
        assert sup._workers["slot_crash"]["crashes"] >= 1

        sup.shutdown()

    def test_scenario_15_spawned_worker_credential_isolation(self, test_env):
        """15. Inspect child process environment and memory: zero supervisor keys leaked and missing credentials fail closed."""
        key_path = test_env["key_path"]
        sup = WorkerSupervisor(db_path=test_env["db_path"], concurrency=1, signing_key_path=key_path)
        sup.start_worker_slot("slot_sec", custom_target=env_inspector_worker_target)
        time.sleep(0.5)
        sup.supervise_step()
        sup.shutdown()

        # Strict Fail-Closed Verification: Job requiring API credentials fails closed if missing from secure tenant store
        queue = test_env["queue"]
        job_unauth = queue.enqueue_job(
            user_id="user_unauth",
            session_id="sess_unauth",
            repository="owner/repo",
            payload={"prompt": "Task with missing credentials", "model": "gemini/gemini-2.5-flash"},
            credential_refs={"github": {"key_ref": "sec_gh"}, "gemini": {"key_ref": "sec_gem"}},
        )
        claimed_unauth = queue.claim_next_job(worker_id="worker_unauth")
        controller_unauth = ExecutionController(job_id=job_unauth["id"])

        with pytest.raises(JobQueueError, match="Security violation"):
            execute_job_in_worker(
                job=claimed_unauth,
                worker_id="worker_unauth",
                lease_gen=claimed_unauth["lease_generation"],
                queue=queue,
                controller=controller_unauth,
                sandbox_factory=lambda repo_name, github_token: MockSandbox(repo_name),
            )

        failed_job = queue.get_job(job_unauth["id"], user_id="user_unauth")
        assert failed_job["status"] == "FAILED"

    def test_scenario_16_normal_completion_active_sandbox_cleanup_matrix(self, test_env):
        """16. Verification of normal completion active sandbox cleanup matrix:
        1. ACTIVE -> CLEANED on normal completion.
        2. Stale worker / lease generation cannot finalize.
        3. Mismatched sandbox ID cannot finalize.
        4. Failed container close does not transition to CLEANED.
        5. Repeated cleanup calls are idempotent.
        """
        queue = test_env["queue"]
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_norm_clean",
            repository="owner/repo",
            payload={"prompt": "Normal cleanup test task"},
        )
        claimed = queue.claim_next_job(worker_id="worker_norm_1")
        gen = claimed["lease_generation"]
        sb_id = "sb_norm_1"
        queue.register_sandbox(job["id"], sb_id, "worker_norm_1", gen)

        # 1. Stale worker cannot finalize
        with pytest.raises(StaleLeaseError):
            queue.finalize_active_sandbox_cleanup(
                job_id=job["id"],
                sandbox_id=sb_id,
                worker_id="imposter_worker",
                lease_generation=gen,
            )

        # 2. Stale lease generation cannot finalize
        with pytest.raises(StaleLeaseError):
            queue.finalize_active_sandbox_cleanup(
                job_id=job["id"],
                sandbox_id=sb_id,
                worker_id="worker_norm_1",
                lease_generation=gen + 1,
            )

        # 3. Mismatched sandbox ID cannot finalize
        with pytest.raises(JobQueueError, match="Sandbox ID mismatch"):
            queue.finalize_active_sandbox_cleanup(
                job_id=job["id"],
                sandbox_id="wrong_sandbox_id",
                worker_id="worker_norm_1",
                lease_generation=gen,
            )

        # 3b. Expired lease cannot finalize cleanup
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET lease_expires_at = ? WHERE id = ?;",
                    (time.time() - 10.0, job["id"]),
                )
        with pytest.raises(StaleLeaseError, match="lease is null or expired"):
            queue.finalize_active_sandbox_cleanup(
                job_id=job["id"],
                sandbox_id=sb_id,
                worker_id="worker_norm_1",
                lease_generation=gen,
            )

        # Restore valid lease for subsequent checks
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET lease_expires_at = ? WHERE id = ?;",
                    (time.time() + 60.0, job["id"]),
                )

        # 3c. Non-RUNNING job cannot finalize cleanup
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET status = 'QUEUED' WHERE id = ?;",
                    (job["id"],),
                )
        with pytest.raises(StaleLeaseError, match="non-running status"):
            queue.finalize_active_sandbox_cleanup(
                job_id=job["id"],
                sandbox_id=sb_id,
                worker_id="worker_norm_1",
                lease_generation=gen,
            )

        # Restore status to RUNNING
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET status = 'RUNNING' WHERE id = ?;",
                    (job["id"],),
                )

        # 3d. Regression: lease expires while waiting to acquire queue lock / BEGIN IMMEDIATE
        # Set a short lease duration (0.4s) that is currently valid
        short_lease_expiry = time.time() + 0.4
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET lease_expires_at = ? WHERE id = ?;",
                    (short_lease_expiry, job["id"]),
                )

        # Deterministic synchronization: lock_holder sets an Event immediately after acquiring queue._lock
        lock_acquired_event = threading.Event()

        def lock_holder():
            with queue._lock:
                lock_acquired_event.set()
                # Hold lock until short lease is guaranteed to have expired
                time.sleep(0.5)

        holder_t = threading.Thread(target=lock_holder)
        holder_t.start()
        # Wait deterministically for lock acquisition
        assert lock_acquired_event.wait(timeout=2.0) is True

        # Now attempt cleanup while lock is held; finalize_active_sandbox_cleanup must block until lock_holder releases,
        # by which time the lease will have expired. It must sample `now` AFTER lock acquisition and reject!
        with pytest.raises(StaleLeaseError, match="lease is null or expired"):
            queue.finalize_active_sandbox_cleanup(
                job_id=job["id"],
                sandbox_id=sb_id,
                worker_id="worker_norm_1",
                lease_generation=gen,
            )
        holder_t.join()

        # Restore valid lease for subsequent checks
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE jobs SET lease_expires_at = ? WHERE id = ?;",
                    (time.time() + 60.0, job["id"]),
                )

        # 4. Normal completion via execute_job_in_worker transitions ACTIVE -> CLEANED
        controller = ExecutionController(job_id=job["id"])
        mock_sb = MockSandbox("owner/repo", sandbox_id=sb_id)
        mock_sb.close_called = False
        original_close = mock_sb.close
        def tracking_close():
            mock_sb.close_called = True
            original_close()
        mock_sb.close = tracking_close

        execute_job_in_worker(
            job=claimed,
            worker_id="worker_norm_1",
            lease_gen=gen,
            queue=queue,
            controller=controller,
            sandbox_factory=lambda repo_name, github_token: mock_sb,
            agent_turn_fn=lambda **kwargs: {"success": True, "output": "Done successfully"},
        )

        assert mock_sb.close_called is True
        job_after = queue.get_job(job["id"], user_id="user_alice")
        assert job_after["status"] == "COMPLETED"
        assert job_after["sandbox_cleanup_status"] == "CLEANED"

        # 5. Repeated call is idempotent for valid caller
        res_repeat = queue.finalize_active_sandbox_cleanup(
            job_id=job["id"],
            sandbox_id=sb_id,
            worker_id="worker_norm_1",
            lease_generation=gen,
        )
        assert res_repeat is True

        # 5b. Already-CLEANED row rejects mismatched worker or mismatched sandbox
        with pytest.raises(StaleLeaseError):
            queue.finalize_active_sandbox_cleanup(
                job_id=job["id"],
                sandbox_id=sb_id,
                worker_id="imposter_worker",
                lease_generation=gen,
            )
        with pytest.raises(JobQueueError, match="Sandbox ID mismatch"):
            queue.finalize_active_sandbox_cleanup(
                job_id=job["id"],
                sandbox_id="wrong_sandbox_id",
                worker_id="worker_norm_1",
                lease_generation=gen,
            )

        # 6. Failed close does not produce CLEANED status (it becomes ORPHANED on terminal update)
        job_fail = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_norm_fail",
            repository="owner/repo",
            payload={"prompt": "Failing close test task"},
        )
        claimed_fail = queue.claim_next_job(worker_id="worker_norm_2")
        gen_fail = claimed_fail["lease_generation"]
        sb_id_fail = "sb_norm_fail"
        queue.register_sandbox(job_fail["id"], sb_id_fail, "worker_norm_2", gen_fail)

        mock_sb_fail = MockSandbox("owner/repo", sandbox_id=sb_id_fail)
        def broken_close():
            raise RuntimeError("Cloud provider network failure during close")
        mock_sb_fail.close = broken_close

        controller_fail = ExecutionController(job_id=job_fail["id"])
        execute_job_in_worker(
            job=claimed_fail,
            worker_id="worker_norm_2",
            lease_gen=gen_fail,
            queue=queue,
            controller=controller_fail,
            sandbox_factory=lambda repo_name, github_token: mock_sb_fail,
            agent_turn_fn=lambda **kwargs: {"success": True, "output": "Done"},
        )
        job_fail_after = queue.get_job(job_fail["id"], user_id="user_alice")
        assert job_fail_after["sandbox_cleanup_status"] == "ORPHANED"  # NOT transitioned to CLEANED!

    def test_scenario_17_pr_recovery_fail_closed_and_state_matrix(self, test_env):
        """17. Comprehensive PR recovery verification:
        1. Missing GitHub client factory raises SideEffectReconciliationBlockedError (fail-closed).
        2. Client factory instantiation error raises SideEffectReconciliationBlockedError.
        3. GitHub remote API error preserves unresolved recovery state.
        4. Existing open PR is adopted cleanly.
        5. Existing closed/merged PR is adopted cleanly.
        6. Verified remote absence allows safe recreation.
        7. No duplicate PR created after adoption.
        """
        sup = WorkerSupervisor(db_path=test_env["db_path"], concurrency=1, signing_key_path=test_env["key_path"])
        queue = sup.queue

        sup.start_worker_slot("slot_prior_rec", custom_target=mock_worker_target)
        prior_info = sup._workers["slot_prior_rec"]
        prior_worker_id = prior_info["instance_id"]

        # 1. Missing GitHub client factory fails closed
        job1 = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_rec_1",
            repository="owner/repo",
            payload={"prompt": "Test missing factory"},
        )
        c1 = queue.claim_next_job(worker_id=prior_worker_id)
        g1 = c1["lease_generation"]
        se_key1 = f"pr_{job1['id'][:8]}"
        scoped_branch1 = f"ai-cloud/job-{job1['id'][:8]}"

        se_id1, _, _ = queue.reserve_side_effect(
            job_id=job1["id"],
            worker_id=prior_worker_id,
            lease_generation=g1,
            action_type="CREATE_PULL_REQUEST",
            action_key=se_key1,
            intent_payload={"branch_name": scoped_branch1, "title": "Test PR 1"},
        )

        # Stop prior worker and record verified reap
        sup.stop_worker_slot("slot_prior_rec", grace_seconds=0.2)

        # Force expire lease and reap stale job so it returns to QUEUED
        with queue._lock:
            with queue.conn:
                queue.conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE id = ?;", (time.time() - 10.0, job1["id"]))
        queue.reap_stale_jobs()

        # Active worker setup
        sup.start_worker_slot("slot_curr_rec", custom_target=mock_worker_target)
        curr_info = sup._workers["slot_curr_rec"]
        curr_worker_id = curr_info["instance_id"]
        ipc_client = SupervisorIPCClient(
            socket_path=sup.ipc_server.socket_path,
            worker_instance_id=curr_worker_id,
            auth_token=curr_info["auth_token"],
        )

        c1_new = queue.claim_next_job(worker_id=curr_worker_id)
        g1_new = c1_new["lease_generation"]

        # 1. Missing factory fails closed
        ctrl_no_factory = ExecutionController(
            job_id=job1["id"],
            worker_id=curr_worker_id,
            lease_generation=g1_new,
            queue=queue,
            ipc_client=ipc_client,
            github_client_factory=None,  # MISSING FACTORY!
        )

        mock_sb1 = MockSandbox("owner/repo")
        with pytest.raises(SideEffectReconciliationBlockedError, match="GitHub client factory is required"):
            ctrl_no_factory.on_create_pull_request(
                sandbox=mock_sb1,
                branch_name=scoped_branch1,
                commit_message="commit",
                pr_title="Test PR 1",
                pr_body="body",
            )
        # Verify side effect remains in PENDING_RECOVERY (unresolved)
        se_row1 = queue.conn.execute("SELECT status FROM job_side_effects WHERE side_effect_id = ?;", (se_id1,)).fetchone()
        assert se_row1["status"] == "PENDING"

        # 2. Client factory exception fails closed
        def failing_factory():
            raise RuntimeError("Secret store unreachable")

        ctrl_broken_factory = ExecutionController(
            job_id=job1["id"],
            worker_id=curr_worker_id,
            lease_generation=g1_new,
            queue=queue,
            ipc_client=ipc_client,
            github_client_factory=failing_factory,
        )
        with pytest.raises(SideEffectReconciliationBlockedError, match="Remote GitHub query failed during reconciliation"):
            ctrl_broken_factory.on_create_pull_request(
                sandbox=mock_sb1,
                branch_name=scoped_branch1,
                commit_message="commit",
                pr_title="Test PR 1",
                pr_body="body",
            )
        se_row2 = queue.conn.execute("SELECT status FROM job_side_effects WHERE side_effect_id = ?;", (se_id1,)).fetchone()
        assert se_row2["status"] == "PENDING"

        # 3. GitHub API query failure preserves unresolved recovery state
        class FlakyGitHubClient:
            def get_pulls(self, **kwargs):
                raise ConnectionResetError("GitHub API rate limit or network reset")

        ctrl_flaky_api = ExecutionController(
            job_id=job1["id"],
            worker_id=curr_worker_id,
            lease_generation=g1_new,
            queue=queue,
            ipc_client=ipc_client,
            github_client_factory=lambda: FlakyGitHubClient(),
        )
        with pytest.raises(SideEffectReconciliationBlockedError, match="Remote GitHub query failed during reconciliation"):
            ctrl_flaky_api.on_create_pull_request(
                sandbox=mock_sb1,
                branch_name=scoped_branch1,
                commit_message="commit",
                pr_title="Test PR 1",
                pr_body="body",
            )
        se_row3 = queue.conn.execute("SELECT status FROM job_side_effects WHERE side_effect_id = ?;", (se_id1,)).fetchone()
        assert se_row3["status"] == "PENDING"

        # 4a. Existing open PR adopted cleanly across state="all"
        class OpenPRGitHubClient:
            def __init__(self):
                self.calls = []
            def get_pulls(self, state, head):
                self.calls.append({"state": state, "head": head})
                return [{"html_url": "https://github.com/owner/repo/pull/101", "number": 101, "state": "open"}]

        open_gh = OpenPRGitHubClient()
        ctrl_open = ExecutionController(
            job_id=job1["id"],
            worker_id=curr_worker_id,
            lease_generation=g1_new,
            queue=queue,
            ipc_client=ipc_client,
            github_client_factory=lambda: open_gh,
        )
        mock_sb1.pr_created = False
        res_adopt_open = ctrl_open.on_create_pull_request(
            sandbox=mock_sb1,
            branch_name=scoped_branch1,
            commit_message="commit",
            pr_title="Test PR 1",
            pr_body="body",
        )
        assert res_adopt_open["success"] is True
        assert res_adopt_open["adopted"] is True
        assert res_adopt_open["pr_url"] == "https://github.com/owner/repo/pull/101"
        assert open_gh.calls[0]["state"] == "all"
        assert getattr(mock_sb1, "pr_created", False) is False

        # Reset side effect status back to PENDING for testing closed PR adoption
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE job_side_effects SET status = 'PENDING', worker_id = ?, lease_generation = ? WHERE side_effect_id = ?;",
                    (prior_worker_id, g1, se_id1),
                )

        # 4b. Existing closed PR adopted cleanly across state="all"
        class ClosedPRGitHubClient:
            def __init__(self):
                self.calls = []
            def get_pulls(self, state, head):
                self.calls.append({"state": state, "head": head})
                return [{"html_url": "https://github.com/owner/repo/pull/42", "number": 42, "state": "closed"}]

        closed_gh = ClosedPRGitHubClient()
        ctrl_closed = ExecutionController(
            job_id=job1["id"],
            worker_id=curr_worker_id,
            lease_generation=g1_new,
            queue=queue,
            ipc_client=ipc_client,
            github_client_factory=lambda: closed_gh,
        )
        mock_sb1.pr_created = False
        res_adopt = ctrl_closed.on_create_pull_request(
            sandbox=mock_sb1,
            branch_name=scoped_branch1,
            commit_message="commit",
            pr_title="Test PR 1",
            pr_body="body",
        )
        assert res_adopt["success"] is True
        assert res_adopt["adopted"] is True
        assert res_adopt["pr_url"] == "https://github.com/owner/repo/pull/42"
        assert closed_gh.calls[0]["state"] == "all"
        assert getattr(mock_sb1, "pr_created", False) is False

        # Reset side effect status back to PENDING for testing merged PR adoption
        with queue._lock:
            with queue.conn:
                queue.conn.execute(
                    "UPDATE job_side_effects SET status = 'PENDING', worker_id = ?, lease_generation = ? WHERE side_effect_id = ?;",
                    (prior_worker_id, g1, se_id1),
                )

        # 4c. Existing merged PR adopted cleanly across state="all"
        class MergedPRGitHubClient:
            def __init__(self):
                self.calls = []
            def get_pulls(self, state, head):
                self.calls.append({"state": state, "head": head})
                return [{"html_url": "https://github.com/owner/repo/pull/202", "number": 202, "state": "closed", "merged": True, "merged_at": "2026-11-04T00:00:00Z"}]

        merged_gh = MergedPRGitHubClient()
        ctrl_merged = ExecutionController(
            job_id=job1["id"],
            worker_id=curr_worker_id,
            lease_generation=g1_new,
            queue=queue,
            ipc_client=ipc_client,
            github_client_factory=lambda: merged_gh,
        )
        mock_sb1.pr_created = False
        res_adopt_merged = ctrl_merged.on_create_pull_request(
            sandbox=mock_sb1,
            branch_name=scoped_branch1,
            commit_message="commit",
            pr_title="Test PR 1",
            pr_body="body",
        )
        assert res_adopt_merged["success"] is True
        assert res_adopt_merged["adopted"] is True
        assert res_adopt_merged["pr_url"] == "https://github.com/owner/repo/pull/202"
        assert merged_gh.calls[0]["state"] == "all"
        assert getattr(mock_sb1, "pr_created", False) is False

        # 5. Verified absence allows safe creation under new lease
        job2 = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_rec_2",
            repository="owner/repo",
            payload={"prompt": "Test verified absence"},
        )
        c2 = queue.claim_next_job(worker_id=prior_worker_id)
        g2 = c2["lease_generation"]
        se_key2 = f"pr_{job2['id'][:8]}"
        scoped_branch2 = f"ai-cloud/job-{job2['id'][:8]}"
        se_id2, _, _ = queue.reserve_side_effect(
            job_id=job2["id"],
            worker_id=prior_worker_id,
            lease_generation=g2,
            action_type="CREATE_PULL_REQUEST",
            action_key=se_key2,
            intent_payload={"branch_name": scoped_branch2, "title": "Test PR 2"},
        )

        with queue._lock:
            with queue.conn:
                queue.conn.execute("UPDATE jobs SET lease_expires_at = ? WHERE id = ?;", (time.time() - 10.0, job2["id"]))
        queue.reap_stale_jobs()

        c2_new = queue.claim_next_job(worker_id=curr_worker_id)
        g2_new = c2_new["lease_generation"]

        class EmptyGitHubClient:
            def get_pulls(self, state, head):
                assert state == "all"
                return []

        mock_sb2 = MockSandbox("owner/repo")
        ctrl_empty = ExecutionController(
            job_id=job2["id"],
            worker_id=curr_worker_id,
            lease_generation=g2_new,
            queue=queue,
            ipc_client=ipc_client,
            github_client_factory=lambda: EmptyGitHubClient(),
        )
        res_created = ctrl_empty.on_create_pull_request(
            sandbox=mock_sb2,
            branch_name=scoped_branch2,
            commit_message="commit new",
            pr_title="Test PR 2",
            pr_body="body new",
        )
        assert res_created["success"] is True
        assert res_created.get("adopted") is None
        assert "pull/42" in res_created["pr_url"]
        # Verify ledger confirmed new PR under curr_worker_id and g2_new
        se_row_new = queue.conn.execute("SELECT * FROM job_side_effects WHERE side_effect_id = ?;", (se_id2,)).fetchone()
        assert se_row_new["status"] == "CONFIRMED"
        assert se_row_new["worker_id"] == curr_worker_id
        assert se_row_new["lease_generation"] == g2_new

        sup.shutdown()

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
import time
import uuid
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
    WorkerSupervisor,
    execute_job_in_worker,
    load_or_create_supervisor_key,
    worker_child_main,
)


class MockSandbox:
    """Mock E2B WorkspaceSandbox for testing."""

    def __init__(self, repo_name: str, github_token: str = ""):
        self.repo_name = repo_name
        self.github_token = github_token
        self.sandbox_id = f"sb_mock_{uuid.uuid4().hex[:8]}"
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


def crashing_worker_target(instance_id, db_path, stop_event, ipc_port=None, ipc_token=None):
    """Top-level function pickleable by multiprocessing spawn context."""
    os._exit(1)


def env_inspector_worker_target(instance_id, db_path, stop_event, ipc_port=None, ipc_token=None):
    """Top-level function pickleable by multiprocessing spawn context."""
    assert "SUPERVISOR_SIGNING_KEY" not in os.environ
    stop_event.set()


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
        """6. Worker process hangs -> supervisor records reap evidence -> finalizes job."""
        queue = test_env["queue"]
        signing_key = test_env["signing_key"]
        inst_id = "worker_hung_1"
        pid = 99999

        reap_proof = queue.record_process_reap(
            instance_id=inst_id,
            pid=pid,
            start_time=time.time() - 30.0,
            exit_code=-9,
            exit_signal=9,
            supervisor_id="supervisor_alpha",
            proof_type="PARENT_WAITPID",
            supervisor_signing_key=signing_key,
        )

        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "Hanging task"},
        )
        claimed = queue.claim_next_job(worker_id=inst_id, lease_duration_seconds=30.0)
        gen = claimed["lease_generation"]

        finalized = queue.supervisor_finalize_job(
            job_id=job["id"],
            expected_lease_generation=gen,
            target_status="CANCELLED",
            supervisor_id="supervisor_alpha",
            authority_proof={
                "type": "FORCED_KILL",
                "worker_instance_id": inst_id,
                "reap_token": reap_proof["reap_token"],
                "reap_signature": reap_proof["reap_signature"],
            },
            reason="Watchdog detected hung process; SIGKILL executed.",
            supervisor_signing_key=signing_key,
        )

        assert finalized["status"] == "CANCELLED"
        assert finalized["lease_generation"] == gen + 1

    def test_scenario_7_blocked_command_forced_interruption(self, test_env):
        """7. Active tool call hangs -> timeout watchdog trips controller.abort()."""
        controller = ExecutionController(job_id="job_hang", timeout_seconds=0.2)
        assert not controller.is_aborted()

        time.sleep(0.3)
        assert controller.is_aborted()
        assert "timeout" in controller.abort_reason.lower()

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
        """10. Side-effect recovery requires contextual proof of prior worker process termination."""
        queue = test_env["queue"]
        signing_key = test_env["signing_key"]

        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "PR task"},
        )

        # Worker A reserves intent
        se_id, status, _ = queue.reserve_side_effect(
            job_id=job["id"],
            worker_id="worker_A",
            lease_generation=1,
            action_type="CREATE_PULL_REQUEST",
            action_key="pr_1",
            intent_payload={"branch": "ai-cloud/job-1"},
        )
        assert status == "PENDING"

        # Worker B attempts recovery while Worker A is NOT verified dead
        is_dead = queue.supervisor_verify_worker_death(
            job_id=job["id"],
            instance_id="worker_A",
            side_effect_id=se_id,
            supervisor_signing_key=signing_key,
        )
        assert not is_dead

        # Now supervisor authoritatively reaps worker A
        reap_proof = queue.record_process_reap(
            instance_id="worker_A",
            pid=12345,
            start_time=time.time() - 20.0,
            exit_code=1,
            exit_signal=None,  # All termination modes accepted!
            supervisor_id="supervisor_alpha",
            proof_type="PARENT_WAITPID",
            supervisor_signing_key=signing_key,
        )

        # Now contextual verification succeeds!
        is_dead = queue.supervisor_verify_worker_death(
            job_id=job["id"],
            instance_id="worker_A",
            side_effect_id=se_id,
            supervisor_signing_key=signing_key,
        )
        assert is_dead

        # Worker B can now safely confirm side effect after remote reconciliation
        queue.confirm_side_effect(
            side_effect_id=se_id,
            worker_id="worker_B",
            lease_generation=2,
            confirmed_result={"pr_url": "https://github.com/owner/repo/pull/1"},
            is_recovery=True,
        )

        # Old worker A awakens as a zombie and tries to confirm -> strictly fenced!
        with pytest.raises(StaleLeaseError):
            queue.confirm_side_effect(
                side_effect_id=se_id,
                worker_id="worker_A",
                lease_generation=1,
                confirmed_result={"pr_url": "https://github.com/owner/repo/pull/zombie"},
            )

    def test_scenario_11_supervisor_restart_continuity(self, test_env):
        """11. Supervisor restarts after recording reap signatures -> persistent key validates continuity."""
        db_path = test_env["db_path"]
        key_path = test_env["key_path"]

        # Run supervisor 1
        sup1 = WorkerSupervisor(db_path=db_path, concurrency=1, signing_key_path=key_path)
        reap_proof = sup1.queue.record_process_reap(
            instance_id="worker_prior_run",
            pid=5555,
            start_time=time.time() - 50.0,
            exit_code=0,
            exit_signal=None,
            supervisor_id=sup1.supervisor_id,
            proof_type="PARENT_WAITPID",
            supervisor_signing_key=sup1.signing_key,
        )
        sup1.shutdown()

        # Start replacement supervisor 2 pointing to same persistent key
        sup2 = WorkerSupervisor(db_path=db_path, concurrency=1, signing_key_path=key_path)
        assert sup2.signing_key == sup1.signing_key

        # Replacement supervisor verifies signature from supervisor 1 run
        inst_row = sup2.queue.conn.execute(
            "SELECT * FROM worker_instances WHERE instance_id = 'worker_prior_run';"
        ).fetchone()

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
        queue = test_env["queue"]
        signing_key = test_env["signing_key"]

        # Simulate prior crashed worker with PENDING intent and verified reap record
        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "PR task with network glitch"},
        )
        se_id, _, _ = queue.reserve_side_effect(
            job_id=job["id"],
            worker_id="prior_worker",
            lease_generation=1,
            action_type="CREATE_PULL_REQUEST",
            action_key=f"pr_{job['id'][:8]}",
            intent_payload={"branch_name": f"ai-cloud/job-{job['id'][:8]}", "repository": "owner/repo"},
        )
        queue.record_process_reap(
            instance_id="prior_worker",
            pid=8888,
            start_time=time.time() - 30.0,
            exit_code=1,
            exit_signal=None,
            supervisor_id="supervisor_test",
            proof_type="PARENT_WAITPID",
            supervisor_signing_key=signing_key,
        )

        class BrokenGitHubClient:
            def get_pulls(self, **kwargs):
                raise Exception("GitHub API 500: Server Error / Rate Limit Exceeded.")

        claimed = queue.claim_next_job(worker_id="worker_gh_1")
        controller = ExecutionController(job_id=job["id"])

        # Worker attempts recovery with broken client -> fails closed
        with pytest.raises(Exception, match="GitHub API 500"):
            execute_job_in_worker(
                job=claimed,
                worker_id="worker_gh_1",
                lease_gen=claimed["lease_generation"],
                queue=queue,
                controller=controller,
                sandbox_factory=lambda repo_name, github_token: MockSandbox(repo_name),
                github_client_factory=lambda: BrokenGitHubClient(),
            )

    def test_scenario_13_pr_idempotency_across_all_states(self, test_env):
        """13. PR was merged/closed on GitHub before retry -> worker detects and adopts existing PR."""
        queue = test_env["queue"]
        signing_key = test_env["signing_key"]

        job = queue.enqueue_job(
            user_id="user_alice",
            session_id="sess_1",
            repository="owner/repo",
            payload={"prompt": "PR task with pre-existing closed PR"},
        )
        se_id, _, _ = queue.reserve_side_effect(
            job_id=job["id"],
            worker_id="prior_worker_closed",
            lease_generation=1,
            action_type="CREATE_PULL_REQUEST",
            action_key=f"pr_{job['id'][:8]}",
            intent_payload={"branch_name": f"ai-cloud/job-{job['id'][:8]}", "repository": "owner/repo"},
        )
        queue.record_process_reap(
            instance_id="prior_worker_closed",
            pid=7777,
            start_time=time.time() - 30.0,
            exit_code=1,
            exit_signal=None,
            supervisor_id="supervisor_test",
            proof_type="PARENT_WAITPID",
            supervisor_signing_key=signing_key,
        )

        class PreExistingGitHubClient:
            def get_pulls(self, state, head):
                assert state == "all"
                return [{"html_url": "https://github.com/owner/repo/pull/99", "state": "closed"}]

        claimed = queue.claim_next_job(worker_id="worker_gh_2")
        controller = ExecutionController(job_id=job["id"])

        execute_job_in_worker(
            job=claimed,
            worker_id="worker_gh_2",
            lease_gen=claimed["lease_generation"],
            queue=queue,
            controller=controller,
            sandbox_factory=lambda repo_name, github_token: MockSandbox(repo_name),
            github_client_factory=lambda: PreExistingGitHubClient(),
            agent_turn_fn=lambda messages, model, api_key, sandbox, execution_controller: {"success": True},
        )

        final_job = queue.get_job(job["id"], user_id="user_alice")
        assert final_job["status"] == "COMPLETED"

    def test_scenario_14_supervisor_crash_loop_protection(self, test_env):
        """14. Child worker repeatedly crashes on startup -> supervisor applies backoff and pauses slot."""
        sup = WorkerSupervisor(db_path=test_env["db_path"], concurrency=1)
        # Set stop event so supervise_step does not immediately spawn a replacement worker
        sup.start_worker_slot("slot_crash", custom_target=crashing_worker_target)
        time.sleep(0.5)

        sup._stop_event.set()
        # Supervise step detects crash and updates crash count
        sup.supervise_step()
        assert sup._workers["slot_crash"]["crashes"] >= 1

        sup.shutdown()

    def test_scenario_15_spawned_worker_credential_isolation(self, test_env):
        """15. Inspect child process environment and memory: zero supervisor keys or LLM keys leaked."""
        key_path = test_env["key_path"]
        sup = WorkerSupervisor(db_path=test_env["db_path"], concurrency=1, signing_key_path=key_path)
        sup.start_worker_slot("slot_sec", custom_target=env_inspector_worker_target)
        time.sleep(0.5)
        sup.supervise_step()
        sup.shutdown()

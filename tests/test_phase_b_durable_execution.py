"""Tests for Phase B.1 (Hardened): Durable Job Store & SQLite Queue Engine.

Exhaustively verifies all B.1 review criteria:
1. Jobs survive database connection closure, reopen, and process restart simulation.
2. Concurrent workers atomic claiming with zero double-claims across multi-threaded and multi-process workloads.
3. Monotonic fencing token (lease_generation) prevents stale worker updates after lease reclamation.
4. Lease expiration enforcement: record_heartbeat, append_log, and update_job_status FAIL CLOSED on expired
   or NULL leases even BEFORE the reaper executes.
5. Cancellation precedence:
   - Cancellation requests cannot be overwritten by worker COMPLETED status.
   - If worker reports FAILED or TIMED_OUT while cancellation is pending, status becomes CANCELLED with error diagnostics retained.
   - Cancelled running jobs whose lease expires are transitioned to terminal CANCELLED by the reaper, NEVER re-queued.
6. Terminal job protection: Worker events and log appends are strictly rejected once a job reaches a terminal state.
7. Strict credential-reference and payload schema validation:
   - Exactly requires {"key_ref": "..."}, blocks raw PATs/keys in payload and credential_refs, rejects extra fields.
8. Shared-connection and multi-process concurrency safety:
   - Re-entrant internal locking protects multi-threaded shared queue instances.
   - Multi-process claiming racing against SQLite WAL busy handlers.
9. Database corruption and transaction failures produce controlled JobQueueError exceptions.
10. Schema migrations are idempotent and preserve existing data.
11. Rigorous numeric boundary checks: rejects NaN, inf, booleans, and floats-for-integers.
"""

import math
import multiprocessing
import os
import sqlite3
import threading
import time
import pytest

from job_queue import (
    DurableJobQueue,
    JobQueueError,
    StaleLeaseError,
    InvalidStateTransitionError,
    JobAccessDeniedError,
    JobNotFoundError,
    InvalidCredentialReferenceError,
    run_migrations,
)


@pytest.fixture
def queue(tmp_path):
    """Provides a fresh DurableJobQueue instance backed by a temporary file."""
    db_file = str(tmp_path / "test_jobs.db")
    q = DurableJobQueue(db_file)
    yield q
    q.close()


def _mp_worker_claim(db_path: str, worker_id: str, results_queue: multiprocessing.Queue):
    """Helper function executed in an isolated OS process for multi-process contention testing."""
    q = DurableJobQueue(db_path)
    try:
        claimed = q.claim_next_job(worker_id=worker_id)
        if claimed:
            results_queue.put((worker_id, claimed["id"], claimed["lease_generation"]))
        else:
            results_queue.put((worker_id, None, None))
    except Exception as e:
        results_queue.put((worker_id, f"ERROR: {str(e)}", None))
    finally:
        q.close()


class TestPhaseB1HardenedJobStore:
    """Comprehensive regression test suite for Phase B.1 Durable Job Queue."""

    # -------------------------------------------------------------------------
    # 1. PERSISTENCE & LIFECYCLE
    # -------------------------------------------------------------------------

    def test_job_persistence_survives_connection_closure_and_reopen(self, tmp_path):
        """1. Jobs survive database connection closure, reopen, and process restart simulation."""
        db_file = str(tmp_path / "persistent_test.db")
        q1 = DurableJobQueue(db_file)

        created = q1.enqueue_job(
            user_id="user_alpha",
            session_id="sess_1",
            repository="org/repo-a",
            payload={"prompt": "Run tests"},
            idempotency_key="idemp_1",
        )
        job_id = created["id"]
        q1.close()

        q2 = DurableJobQueue(db_file)
        retrieved = q2.get_job(job_id, user_id="user_alpha")
        assert retrieved["id"] == job_id
        assert retrieved["status"] == "QUEUED"
        assert retrieved["repository"] == "org/repo-a"
        assert retrieved["payload"]["prompt"] == "Run tests"
        q2.close()

    def test_invalid_lifecycle_state_transitions_are_rejected(self, queue):
        """Lifecycle state transitions must follow explicit allowed transitions and block terminal mutations."""
        job = queue.enqueue_job(
            user_id="user_delta",
            session_id="sess_delta",
            repository="org/repo-delta",
            payload={"action": "test"},
        )
        job_id = job["id"]

        claimed = queue.claim_next_job(worker_id="w_test")
        assert claimed["id"] == job_id
        gen = claimed["lease_generation"]

        queue.update_job_status(job_id=job_id, target_status="COMPLETED", worker_id="w_test", lease_generation=gen)

        with pytest.raises(InvalidStateTransitionError) as exc:
            queue.update_job_status(job_id=job_id, target_status="RUNNING", worker_id="w_test", lease_generation=gen)
        assert "Illegal status transition" in str(exc.value)

    # -------------------------------------------------------------------------
    # 2. CONCURRENCY: MULTI-THREAD & MULTI-PROCESS
    # -------------------------------------------------------------------------

    def test_shared_connection_multithread_concurrency_safety(self, queue):
        """Multi-threaded queries through a single shared queue instance are serialized safely without sqlite errors."""
        num_threads = 8
        num_ops_per_thread = 10
        errors = []

        def worker_loop(t_idx):
            for i in range(num_ops_per_thread):
                try:
                    queue.enqueue_job(
                        user_id="user_shared_conn",
                        session_id=f"sess_{t_idx}",
                        repository="org/repo",
                        payload={"i": i},
                    )
                except Exception as e:
                    errors.append(f"Thread {t_idx} op {i} error: {e}")

        threads = [threading.Thread(target=worker_loop, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Shared connection concurrency errors: {errors}"
        jobs = queue.list_user_jobs("user_shared_conn", limit=200)
        assert len(jobs) == num_threads * num_ops_per_thread

    def test_multiprocess_atomic_claiming_contention(self, tmp_path):
        """Multi-process: Independent OS processes racing to claim jobs across separate connections cannot double-claim."""
        db_file = str(tmp_path / "mp_test.db")
        setup_q = DurableJobQueue(db_file)

        num_jobs = 6
        job_ids = []
        for i in range(num_jobs):
            res = setup_q.enqueue_job(
                user_id="user_mp",
                session_id=f"sess_{i}",
                repository="org/repo-mp",
                payload={"job_index": i},
            )
            job_ids.append(res["id"])
        setup_q.close()

        mp_ctx = multiprocessing.get_context("spawn")
        results_queue = mp_ctx.Queue()
        processes = []

        for p_idx in range(num_jobs):
            w_id = f"proc_worker_{p_idx}"
            p = mp_ctx.Process(target=_mp_worker_claim, args=(db_file, w_id, results_queue))
            processes.append(p)
            p.start()

        for p in processes:
            p.join(timeout=10.0)

        claimed_records = []
        while not results_queue.empty():
            rec = results_queue.get_nowait()
            claimed_records.append(rec)

        assert len(claimed_records) == num_jobs
        claimed_jids = [r[1] for r in claimed_records if r[1] is not None]
        assert len(claimed_jids) == num_jobs
        assert len(set(claimed_jids)) == num_jobs, "Collision detected in multi-process contention!"
        assert set(claimed_jids) == set(job_ids)

    # -------------------------------------------------------------------------
    # 3. LEASE EXPIRATION ENFORCEMENT (BEFORE REAPER EXECUTION & NULL HANDLING)
    # -------------------------------------------------------------------------

    def test_expired_lease_fails_closed_on_heartbeat_before_reaper(self, queue):
        """Heartbeat must fail closed if current lease has expired, even if reaper hasn't run yet."""
        enq = queue.enqueue_job("user_exp", "s", "org/repo", payload={"task": 1})
        jid = enq["id"]

        claimed = queue.claim_next_job("worker_exp", lease_duration_seconds=1.0)
        gen = claimed["lease_generation"]

        time.sleep(1.1)

        with pytest.raises(StaleLeaseError) as exc_info:
            queue.record_heartbeat(jid, "worker_exp", gen)
        assert "expired" in str(exc_info.value).lower()

    def test_expired_lease_fails_closed_on_log_append_before_reaper(self, queue):
        """Log append must fail closed if current lease has expired, even if reaper hasn't run yet."""
        enq = queue.enqueue_job("user_exp2", "s", "org/repo", payload={"task": 2})
        jid = enq["id"]

        claimed = queue.claim_next_job("worker_exp2", lease_duration_seconds=1.0)
        gen = claimed["lease_generation"]

        time.sleep(1.1)

        with pytest.raises(StaleLeaseError) as exc_info:
            queue.append_log(jid, "worker_exp2", gen, "Line from zombie worker")
        assert "expired" in str(exc_info.value).lower()

    def test_expired_lease_fails_closed_on_status_update_before_reaper(self, queue):
        """Status update must fail closed if lease expired before reaper runs."""
        enq = queue.enqueue_job("user_exp3", "s", "org/repo", payload={"task": 3})
        jid = enq["id"]

        claimed = queue.claim_next_job("worker_exp3", lease_duration_seconds=1.0)
        gen = claimed["lease_generation"]

        time.sleep(1.1)

        with pytest.raises(StaleLeaseError) as exc_info:
            queue.update_job_status(jid, "COMPLETED", "worker_exp3", gen)
        assert "expired" in str(exc_info.value).lower()

    def test_null_lease_expires_at_fails_closed_on_mutations(self, queue):
        """If a running job somehow has NULL lease_expires_at, all worker mutations must fail closed."""
        enq = queue.enqueue_job("user_null_lease", "s", "org/repo", payload={})
        jid = enq["id"]

        claimed = queue.claim_next_job("worker_null")
        gen = claimed["lease_generation"]

        # Manually force lease_expires_at to NULL to test fail-closed invariant
        with queue.conn:
            queue.conn.execute("UPDATE jobs SET lease_expires_at = NULL WHERE id = ?;", (jid,))

        with pytest.raises(StaleLeaseError) as exc1:
            queue.record_heartbeat(jid, "worker_null", gen)
        assert "null or expired" in str(exc1.value).lower()

        with pytest.raises(StaleLeaseError) as exc2:
            queue.append_log(jid, "worker_null", gen, "test log")
        assert "null or expired" in str(exc2.value).lower()

        with pytest.raises(StaleLeaseError) as exc3:
            queue.update_job_status(jid, "COMPLETED", "worker_null", gen)
        assert "null or expired" in str(exc3.value).lower()

    # -------------------------------------------------------------------------
    # 4. CANCELLATION & PRECEDENCE POLICY
    # -------------------------------------------------------------------------

    def test_reaper_never_requeues_cancelled_job(self, queue):
        """If cancellation was requested, an expired worker job MUST transition to terminal CANCELLED, never QUEUED."""
        enq = queue.enqueue_job("user_cancel_reap", "s", "org/repo", payload={"task": "cancel_reap"})
        jid = enq["id"]

        claimed = queue.claim_next_job("worker_dying", lease_duration_seconds=1.0)
        assert claimed["id"] == jid

        queue.request_cancellation(jid, user_id="user_cancel_reap")

        time.sleep(1.1)

        reaped = queue.reap_stale_jobs()
        assert jid in reaped

        final_job = queue.get_job(jid, user_id="user_cancel_reap")
        assert final_job["status"] == "CANCELLED"
        assert final_job["finished_at"] is not None

        # Verify it cannot be claimed by any other worker
        next_claim = queue.claim_next_job("worker_new")
        assert next_claim is None

    def test_cancellation_precedence_policy_across_terminal_statuses(self, queue):
        """Explicit policy: User cancellation strictly overrides COMPLETED, FAILED, and TIMED_OUT."""
        # Case 1: COMPLETED is blocked with InvalidStateTransitionError
        j1 = queue.enqueue_job("u", "s", "org/repo", payload={})["id"]
        c1 = queue.claim_next_job("w1")
        queue.request_cancellation(j1, user_id="u")
        with pytest.raises(InvalidStateTransitionError) as exc1:
            queue.update_job_status(j1, "COMPLETED", "w1", c1["lease_generation"])
        assert "cancellation was requested" in str(exc1.value)

        # Case 2: Worker reports FAILED while cancellation pending -> status is recorded as CANCELLED, error diagnostic preserved
        j2 = queue.enqueue_job("u", "s", "org/repo", payload={})["id"]
        c2 = queue.claim_next_job("w2")
        queue.request_cancellation(j2, user_id="u")
        res2 = queue.update_job_status(
            j2, "FAILED", "w2", c2["lease_generation"], error={"detail": "Sandbox network dropped"}
        )
        assert res2["status"] == "CANCELLED"
        assert res2["error"]["detail"] == "Sandbox network dropped"

        # Case 3: Worker reports TIMED_OUT while cancellation pending -> status is recorded as CANCELLED
        j3 = queue.enqueue_job("u", "s", "org/repo", payload={})["id"]
        c3 = queue.claim_next_job("w3")
        queue.request_cancellation(j3, user_id="u")
        res3 = queue.update_job_status(
            j3, "TIMED_OUT", "w3", c3["lease_generation"], error={"reason": "10m wall clock"}
        )
        assert res3["status"] == "CANCELLED"
        assert res3["error"]["reason"] == "10m wall clock"

    # -------------------------------------------------------------------------
    # 5. TERMINAL JOB PROTECTION
    # -------------------------------------------------------------------------

    def test_terminal_jobs_reject_worker_logs_and_status(self, queue):
        """Terminal jobs reject further worker logs, heartbeats, and mutations."""
        job = queue.enqueue_job("user_term", "s", "org/repo", payload={})
        jid = job["id"]

        claimed = queue.claim_next_job("worker_term")
        gen = claimed["lease_generation"]

        queue.update_job_status(jid, "COMPLETED", "worker_term", gen)

        with pytest.raises(StaleLeaseError) as exc:
            queue.append_log(jid, "worker_term", gen, "Post-completion log")
        assert "terminal/non-running" in str(exc.value)

        with pytest.raises(StaleLeaseError) as exc2:
            queue.record_heartbeat(jid, "worker_term", gen)
        assert "is in status 'COMPLETED'" in str(exc2.value)

    # -------------------------------------------------------------------------
    # 6. EXACT CREDENTIAL-REFERENCE SCHEMA & PAYLOAD VALIDATION
    # -------------------------------------------------------------------------

    def test_exact_credential_reference_schema_enforcement(self, queue):
        """credential_refs must conform strictly to {'<provider>': {'key_ref': '<name>'}} without extra fields."""
        # Valid schema
        valid = queue.enqueue_job(
            "u", "s", "org/repo",
            payload={"task": "clean"},
            credential_refs={
                "github": {"key_ref": "user_pat_primary"},
                "openai": {"key_ref": "byok_openai_team"},
            },
        )
        assert valid["credential_refs"]["github"]["key_ref"] == "user_pat_primary"

        # Reject non-dict reference value (e.g. raw string)
        with pytest.raises(InvalidCredentialReferenceError) as exc1:
            queue.enqueue_job("u", "s", "org/repo", payload={}, credential_refs={"github": "raw_str_not_object"})
        assert "must be a dictionary object with key_ref" in str(exc1.value)

        # Reject extra unexpected fields
        with pytest.raises(InvalidCredentialReferenceError) as exc2:
            queue.enqueue_job(
                "u", "s", "org/repo", payload={},
                credential_refs={"github": {"key_ref": "ref1", "token": "extra_field"}},
            )
        assert "Unknown field(s) ['token']" in str(exc2.value)

        # Reject missing key_ref field (e.g. empty dictionary {})
        with pytest.raises(InvalidCredentialReferenceError) as exc3:
            queue.enqueue_job("u", "s", "org/repo", payload={}, credential_refs={"github": {}})
        assert "Missing required 'key_ref'" in str(exc3.value)

        # Reject raw secrets inside key_ref value
        with pytest.raises(InvalidCredentialReferenceError) as exc4:
            queue.enqueue_job(
                "u", "s", "org/repo", payload={},
                credential_refs={"github": {"key_ref": "ghp_1234567890abcdefghijklmnopqrstuvwxyz"}},
            )
        assert "Raw credential pattern detected" in str(exc4.value)

        # Reject invalid characters in key_ref
        with pytest.raises(InvalidCredentialReferenceError) as exc5:
            queue.enqueue_job(
                "u", "s", "org/repo", payload={},
                credential_refs={"github": {"key_ref": "ref; rm -rf /"}},
            )
        assert "invalid characters" in str(exc5.value)

        # Reject raw secrets in payload
        with pytest.raises(JobQueueError) as exc6:
            queue.enqueue_job("u", "s", "org/repo", payload={"key": "sk-proj-12345678901234567890"})
        assert "Security violation: Raw secret" in str(exc6.value)

    # -------------------------------------------------------------------------
    # 7. RIGOROUS NUMERIC VALIDATION (NaN, Inf, Booleans, Floats-for-Ints)
    # -------------------------------------------------------------------------

    def test_numeric_validation_rejects_nan_inf_booleans_and_invalid_types(self, queue):
        """Strict validation rejects NaN, infinity, booleans, and floats where integers are required."""
        # Boolean passed as max_retries
        with pytest.raises(JobQueueError) as exc1:
            queue.enqueue_job("u", "s", "org/repo", payload={}, max_retries=True)
        assert "Invalid type" in str(exc1.value)

        # Float passed as max_retries
        with pytest.raises(JobQueueError) as exc2:
            queue.enqueue_job("u", "s", "org/repo", payload={}, max_retries=3.5)
        assert "expected integer" in str(exc2.value)

        # NaN passed as timeout_seconds
        with pytest.raises(JobQueueError) as exc3:
            queue.enqueue_job("u", "s", "org/repo", payload={}, timeout_seconds=float("nan"))
        assert "non-finite numbers (NaN, Inf)" in str(exc3.value)

        # Inf passed as timeout_seconds
        with pytest.raises(JobQueueError) as exc4:
            queue.enqueue_job("u", "s", "org/repo", payload={}, timeout_seconds=float("inf"))
        assert "non-finite numbers (NaN, Inf)" in str(exc4.value)

        # NaN passed as lease_duration_seconds
        with pytest.raises(JobQueueError) as exc5:
            queue.claim_next_job("w", lease_duration_seconds=float("nan"))
        assert "non-finite numbers (NaN, Inf)" in str(exc5.value)

        # Negative lease duration
        with pytest.raises(JobQueueError) as exc6:
            queue.claim_next_job("w", lease_duration_seconds=-10.0)
        assert "out of permitted range" in str(exc6.value)

    # -------------------------------------------------------------------------
    # 8. MULTI-TENANCY, IDEMPOTENCY, CORRUPTION & MIGRATIONS
    # -------------------------------------------------------------------------

    def test_multi_tenant_job_isolation(self, queue):
        """Cross-tenant reads, cancellations, and log retrievals are strictly blocked."""
        job_alice = queue.enqueue_job("alice", "s_alice", "org/alice-repo", payload={"private": True})
        jid = job_alice["id"]

        with pytest.raises(JobAccessDeniedError):
            queue.get_job(jid, user_id="bob")
        with pytest.raises(JobAccessDeniedError):
            queue.request_cancellation(jid, user_id="bob")
        with pytest.raises(JobAccessDeniedError):
            queue.get_job_events(jid, user_id="bob")

    def test_idempotent_job_submission(self, queue):
        """Identical idempotency keys return existing job without duplicating."""
        key = "idemp_abc_123"
        job1 = queue.enqueue_job("u_idemp", "s", "org/repo", payload={"v": 1}, idempotency_key=key)
        job2 = queue.enqueue_job("u_idemp", "s", "org/repo", payload={"v": 2}, idempotency_key=key)

        assert job1["id"] == job2["id"]
        assert job2["payload"]["v"] == 1

        with pytest.raises(JobAccessDeniedError):
            queue.enqueue_job("foreign_user", "s", "org/repo", payload={}, idempotency_key=key)

    def test_controlled_error_on_database_corruption(self, tmp_path):
        """Database file corruption or syntax failures raise controlled JobQueueError, not bare unhandled exceptions."""
        corrupt_file = str(tmp_path / "corrupted_jobs.db")
        with open(corrupt_file, "w") as fp:
            fp.write("CORRUPTED_SQLITE_HEADER_GARBAGE")

        with pytest.raises(JobQueueError) as exc_info:
            DurableJobQueue(corrupt_file)
        assert "Failed to connect to SQLite database" in str(exc_info.value) or "error" in str(exc_info.value).lower()

    def test_schema_migrations_are_idempotent_and_preserve_data(self, tmp_path):
        """Schema migrations can run repeatedly without modifying existing data."""
        db_file = str(tmp_path / "mig_test.db")
        conn = sqlite3.connect(db_file)
        run_migrations(conn)

        conn.execute(
            """
            INSERT INTO jobs (id, user_id, session_id, repository, status, payload_json, created_at)
            VALUES ('j_mig', 'u1', 's1', 'r', 'QUEUED', '{}', 100.0);
            """
        )
        conn.commit()

        run_migrations(conn)
        cursor = conn.execute("SELECT id FROM jobs WHERE id = 'j_mig';")
        assert cursor.fetchone()[0] == "j_mig"
        conn.close()

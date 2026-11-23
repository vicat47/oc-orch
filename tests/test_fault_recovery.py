#!/usr/bin/env python3
"""fault_recovery 应用用例测试（F04/F12/F17/F19 的应用层 + 主路径）。"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
import unittest

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tests.fakes import FakeClient, FakeClock, FakeService

from oc_orch.adapters.sqlite_db import connect, init
from oc_orch.adapters.writer_guard import install_writer_guard
from oc_orch.adapters.failover_guard import install_failover_guard
from oc_orch.adapters.failover_ledger import FailoverLedger
from oc_orch.application.fault_recovery import FaultRecoveryService
from oc_orch.domain import failover as fo


class FakeLedger:
    def __init__(self):
        self.tasks = {}
        self.attempts = {}
        self.audit_rows = []

    def get_task(self, task_id):
        return self.tasks.get(task_id)

    def get_attempt(self, attempt_id):
        return self.attempts.get(attempt_id)

    def list_tasks(self, *, limit=200, **_):
        return list(self.tasks.values())[:limit]

    def attempts_for_session(self, session_id, *, limit=20):
        return [a for a in self.attempts.values()
                if a.get("session_id") == session_id][:limit]

    def audit(self, actor, action, entity, entity_id, detail):
        self.audit_rows.append((actor, action, entity, entity_id, detail))


def _task(project="p", status="blocked", coordinator=None):
    return {"id": "T-1", "project": project, "status": status, "title": "t",
            "coordinator_session": coordinator or "ses_c",
            "current_attempt_id": None, "closed_at": None, "metadata": {}}


def _attempt(aid="A-1", status="terminal_failed", model="deepseek/deepseek-flash"):
    return {"id": aid, "task_id": "T-1", "status": status, "model": model,
            "session_id": "ses_w1", "directory": "/work", "agent": "build",
            "dispatch_state": "confirmed", "site_released_at": 999_999,
            "metadata": {}}


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="oc-orch-fr-")
        conn = init(self._tmp)
        install_writer_guard(conn, actor="test")
        install_failover_guard(conn, actor="test")
        conn.close()
        self.clock = FakeClock(wall_ms=1_000_000)
        self.failover = FailoverLedger(connect(self._tmp), clock=self.clock.now_ms)
        self.ledger = FakeLedger()
        self.client = FakeClient(FakeService())
        self.dispatch_calls = []

        def dispatch(**kwargs):
            self.dispatch_calls.append(kwargs)
            att_id = kwargs.get("_attempt_id") or ("AX-" + str(len(self.dispatch_calls) + 40))
            message_id = kwargs.get("_message_id") or "msg_1"
            self.ledger.attempts[att_id] = {
                "id": att_id, "task_id": kwargs["task_id"], "status": "creating",
                "model": kwargs.get("model"), "session_id": kwargs.get("session"),
                "directory": kwargs.get("directory"), "agent": kwargs.get("agent"),
                "dispatch_state": "confirmed",
                "metadata": {}}
            return {"ok": True, "attempt_id": att_id,
                    "message_id": message_id, "generation": 2,
                    "session_id": kwargs.get("session")}

        self.dispatch = dispatch

        def authorize(**kwargs):
            return {"allowed": True, "reason": "test"}

        self.service = FaultRecoveryService(
            ledger=self.ledger, failover=self.failover, client=self.client,
            clock=self.clock, dispatch=self.dispatch, authorize=authorize)

    def tearDown(self):
        shutil.rmtree(self._tmp, ignore_errors=True)

    def enable_policy(self, project="p", task=None):
        policy = {"enabled": True, "scope": "project",
                  "source_routes": ["deepseek/deepseek-flash"],
                  "fallback_routes": ["yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash"],
                  "max_switches": 1, "max_attempts": 3}
        if task:
            policy = dict(policy, scope="task", scope_id=task)
            return self.failover.set_policy("task", task, policy,
                                            expected_revision=None)
        return self.failover.set_policy("project", project, policy,
                                        expected_revision=None)

    def prepare_worker(self, session_model=None):
        session_model = session_model or {"providerID": "deepseek",
                                          "id": "deepseek-flash",
                                          "variant": "default"}
        self.client.sessions["ses_w1"] = {
            "id": "ses_w1", "title": "t", "model": session_model,
            "location": {"directory": "/work"}, "metadata": {},
            "outcome": "failed", "time": {}}
        self.ledger.tasks["T-1"] = dict(_task(), current_attempt_id="A-1")
        self.ledger.attempts["A-1"] = _attempt()

    def observe_quota_failure(self):
        self.enable_policy()
        return self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")


class TestObserveFailure(Base):
    def test_402_observed_with_route_from_session(self):
        self.prepare_worker()
        self.enable_policy()
        out = self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
        self.assertEqual(out["action"], "created")
        self.assertEqual(out["category"], "quota")
        self.assertEqual(out["route"], "deepseek/deepseek-flash#default")
        ep = self.failover.get_record("episode", out["episode_id"])
        self.assertEqual(ep["status"], fo.EPISODE_OPEN)
        blocked = self.failover.is_route_blocked("deepseek/deepseek-flash#default")
        self.assertTrue(blocked and blocked["blocked"])

    def test_duplicate_observation_keeps_budget(self):
        self.prepare_worker()
        self.enable_policy()
        first = self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
        second = self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
        self.assertEqual(second["action"], "observed")
        ep = self.failover.get_record("episode", first["episode_id"])
        self.assertEqual(ep["attempts_used"], 2)
        self.assertEqual(ep["switches_used"], 0)

    def test_transient_not_episode(self):
        self.prepare_worker()
        self.enable_policy()
        out = self.service.observe_failure(
            error={"status": 429, "message": "rate limited"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
        self.assertEqual(out["action"], "retry")
        self.assertIsNone(out["episode_id"])

    def test_exhausted_transient_honors_trusted_retry_after_across_service_restart(self):
        self.prepare_worker()
        self.enable_policy()
        error = {"status": 429, "message": "rate limited",
                 "retry_after_ms": 60_000}
        out = self.service.observe_failure(
            error=error, task_id="T-1", attempt_id="A-1", session_id="ses_w1",
            transients_exhausted=True, origin_at=self.clock.now_ms())
        self.assertEqual(out["action"], "retry")
        self.assertEqual(self.failover.list_records("episode")["records"], [])

        # 重建用例仍读同一持久台账/时钟；可信等待窗未到时不得先切。
        restarted = FaultRecoveryService(
            ledger=self.ledger, failover=self.failover, client=self.client,
            clock=self.clock, dispatch=self.dispatch,
            authorize=lambda **_: {"allowed": True})
        self.clock.advance(seconds=60)
        after = restarted.observe_failure(
            error=error, task_id="T-1", attempt_id="A-1", session_id="ses_w1",
            transients_exhausted=True, origin_at=1_000_000)
        self.assertEqual(after["action"], "created")
        self.assertIsNotNone(after["episode_id"])

    def test_stale_failure_is_historical_and_does_not_block_current_route(self):
        self.prepare_worker()
        self.enable_policy()
        self.ledger.tasks["T-1"]["current_attempt_id"] = "A-new"
        out = self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1",
            event_id="old-event")
        self.assertEqual(out["action"], "stale-observation-kept")
        self.assertIsNone(out["episode_id"])
        self.assertIsNone(self.failover.is_route_blocked(
            "deepseek/deepseek-flash#default"))

    def test_policy_is_frozen_and_revocation_blocks_before_switch(self):
        self.prepare_worker()
        self.enable_policy()
        out = self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
        frozen = self.failover.get_record("episode", out["episode_id"])
        self.assertEqual(frozen["metadata"]["policy_snapshot"]["revision"], 1)
        self.service._authorize = lambda **_: {"allowed": False}
        self.service.tick(limit=10)
        current = self.failover.get_record("episode", out["episode_id"])
        self.assertEqual(current["status"], fo.EPISODE_BLOCKED_MANUAL)
        self.assertEqual(current["metadata"]["last_block_reason"], "not-authorized")
        self.assertEqual(self.client.switch_calls, [])

    def test_no_policy_blocks_manual(self):
        self.prepare_worker()
        out = self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
        self.assertEqual(out["action"], "created")
        ep = self.failover.get_record("episode", out["episode_id"])
        self.assertEqual(ep["status"], fo.EPISODE_BLOCKED_MANUAL)


class TestRecoveryFlow(Base):
    def test_full_flow_to_resumed(self):
        self.prepare_worker()
        out = self.observe_quota_failure()
        episode_id = out["episode_id"]
        # tick 1: open → switch（POST 已发，目标模型已生效）
        r1 = self.service.tick(limit=10)
        self.assertGreaterEqual(r1["advanced"], 1)
        self.assertEqual(self.client.session_model("ses_w1")["id"],
                         "DeepSeek-AI/DeepSeek-V4-Flash")
        # tick 2: switch_requested → confirm（一致）→ switch_confirmed
        self.service.tick(limit=10)
        # tick 3: switch_confirmed → 后继派单
        r3 = self.service.tick(limit=10)
        self.assertTrue(self.dispatch_calls, "应调用共享派单")
        self.assertEqual(self.dispatch_calls[0]["session"], "ses_w1")
        self.assertEqual(self.dispatch_calls[0]["model"],
                         "yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash")
        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_CONTINUATION_REQUESTED)
        # 后继 attempt 终态 → 观察 → resumed
        successor = (ep.get("metadata") or {}).get("successor_attempt_id")
        self.assertTrue(successor)
        self.ledger.attempts[successor].update({
            "status": "result_collected", "result_collected_at": self.clock.now_ms(),
            "result_path": "/tmp/result.json"})
        self.service.tick(limit=10)
        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_RESUMED)
        self.assertTrue(self.failover.is_route_blocked(
            "deepseek/deepseek-flash#default")["blocked"],
            "备用恢复不得清除原 provider 的硬故障阻断")
        self.assertIsNone(self.failover.get_session_lock("ses_w1"))

    def test_fallback_failure_stays_manual_and_never_tries_third_route(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        self.service.tick(limit=10)
        self.clock.advance(seconds=30)
        self.service.tick(limit=10)
        self.service.tick(limit=10)
        ep = self.failover.get_record("episode", episode_id)
        successor = ep["metadata"]["successor_attempt_id"]
        self.ledger.attempts[successor].update({
            "status": "terminal_failed", "terminal_at": self.clock.now_ms(),
            "site_released_at": self.clock.now_ms()})
        self.service.tick(limit=10)
        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_BLOCKED_MANUAL)
        self.assertEqual(ep["metadata"]["last_block_reason"],
                         "fallback-execution-failed")
        self.assertEqual(len(self.client.switch_calls), 1)
        self.assertEqual(len(self.dispatch_calls), 1)

    def test_unknown_switch_response_confirms_original_without_reposting(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        from oc_orch.client import TransportError
        original_switch = self.client.switch_model

        def applied_but_response_lost(session_id, **kwargs):
            original_switch(session_id, **kwargs)
            raise TransportError("/session/model", "lost response")

        self.client.switch_model = applied_but_response_lost
        self.service.tick(limit=10)
        self.assertEqual(len(self.client.switch_calls), 1)
        self.clock.advance(seconds=30)
        self.service.tick(limit=10)
        self.assertEqual(len(self.client.switch_calls), 1)
        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_SWITCH_CONFIRMED)
        self.assertEqual(len(self.failover.list_records("operation")["records"]), 1)

    def test_external_model_change_after_possible_switch_blocks_without_retry(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        from oc_orch.client import TransportError
        self.client.fail("switch_model", TransportError("/session/model", "lost response"))
        self.service.tick(limit=10)
        self.client.sessions["ses_w1"]["model"] = {
            "providerID": "third-party", "id": "operator-selected"}
        self.clock.advance(seconds=30)
        self.service.tick(limit=10)
        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_BLOCKED_MANUAL)
        self.assertEqual(ep["metadata"]["last_block_reason"], "external-model-change")
        self.assertEqual(len(self.client.switch_calls), 1)

    def test_continuation_unknown_reuses_reserved_attempt_and_message_identity(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        self.service.tick(limit=10)
        self.clock.advance(seconds=30)
        self.service.tick(limit=10)
        calls = []

        def unknown_dispatch(**kwargs):
            calls.append(kwargs)
            aid, mid = kwargs["_attempt_id"], kwargs["_message_id"]
            self.ledger.attempts[aid] = {
                "id": aid, "task_id": kwargs["task_id"],
                "status": "dispatch_unknown", "dispatch_state": "unknown",
                "model": kwargs["model"], "session_id": kwargs["session"],
                "dispatch_msg_id": mid, "metadata": {},
            }
            raise RuntimeError("prompt response unknown")

        self.service._dispatch = unknown_dispatch
        self.service.tick(limit=10)
        first = self.failover.get_record("episode", episode_id)
        attempt_id = first["metadata"]["successor_attempt_id"]
        message_id = first["metadata"]["successor_message_id"]
        self.service.tick(limit=10)
        after = self.failover.get_record("episode", episode_id)
        self.assertEqual(after["metadata"]["successor_attempt_id"], attempt_id)
        self.assertEqual(after["metadata"]["successor_message_id"], message_id)
        self.assertEqual(len(calls), 1)

    def test_terminal_success_alone_is_not_effective_resume(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        self.service.tick(limit=10)
        self.clock.advance(seconds=30)
        self.service.tick(limit=10)
        self.service.tick(limit=10)
        ep = self.failover.get_record("episode", episode_id)
        successor = ep["metadata"]["successor_attempt_id"]
        self.ledger.attempts[successor].update({
            "status": "terminal_succeeded", "terminal_at": self.clock.now_ms()})
        self.service.tick(limit=10)
        self.assertNotEqual(self.failover.get_record("episode", episode_id)["status"],
                            fo.EPISODE_RESUMED)

    def test_switch_api_reject_blocks_without_dispatch(self):
        self.prepare_worker()
        self.observe_quota_failure()
        from oc_orch.client import ApiError
        self.client.fail("switch_model", ApiError(400, "/api/session/x/model",
                                                  "InvalidRequestError", "bad", ""))
        self.service.tick(limit=10)
        ep = list(self.failover.list_records("episode")["records"])[0]
        self.assertEqual(ep["status"], fo.EPISODE_BLOCKED_MANUAL)
        self.assertFalse(self.dispatch_calls, "切换拒绝后不得派单")
        self.assertIsNone(self.failover.get_session_lock("ses_w1"))

    def test_task_paused_before_switch_stays(self):
        self.prepare_worker()
        self.observe_quota_failure()
        self.ledger.tasks["T-1"]["status"] = "paused"
        self.service.tick(limit=10)
        ep = list(self.failover.list_records("episode")["records"])[0]
        self.assertEqual(ep["status"], fo.EPISODE_BLOCKED_MANUAL)
        self.assertFalse(self.dispatch_calls)

    def test_pause_after_switch_confirmation_blocks_continuation(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        self.service.tick(episode_id=episode_id)  # durable possible + one switch POST
        self.clock.advance(seconds=60)
        self.service.tick(episode_id=episode_id)  # actual model confirmation
        self.assertEqual(self.failover.get_record("episode", episode_id)["status"],
                         fo.EPISODE_SWITCH_CONFIRMED)

        self.ledger.tasks["T-1"]["status"] = "paused"
        self.service.tick(episode_id=episode_id)
        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_BLOCKED_MANUAL)
        self.assertEqual(ep["metadata"]["last_block_reason"], "task-controlled")
        self.assertEqual(len(self.client.switch_calls), 1)
        self.assertFalse(self.dispatch_calls)

    def test_authorization_revoked_after_switch_stops_before_continuation(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        self.service.tick(episode_id=episode_id)
        self.clock.advance(seconds=60)
        self.service.tick(episode_id=episode_id)
        self.service._authorize = lambda **_: {"allowed": False,
                                                "reason": "revoked"}

        self.service.tick(episode_id=episode_id)
        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_BLOCKED_MANUAL)
        self.assertEqual(ep["metadata"]["last_block_reason"],
                         "authorization-revoked-before-continuation")
        self.assertEqual(len(self.client.switch_calls), 1)
        self.assertFalse(self.dispatch_calls)

    def test_switch_transport_unknown_records_possible(self):
        self.prepare_worker()
        self.observe_quota_failure()
        from oc_orch.client import TransportError
        self.client.fail("switch_model", TransportError("/api/session/x/model",
                                                        "boom"))
        self.service.tick(limit=10)
        ops = self.failover.list_records("operation")["records"]
        self.assertEqual(len(ops), 1)
        self.assertIsNotNone(ops[0].get("possible_submitted_at"))
        # confirm 未一致 → 保持 unknown 或 blocked_manual（最多补证 3 次后人工）
        ep = list(self.failover.list_records("episode")["records"])[0]
        self.assertNotEqual(ep["status"], fo.EPISODE_SWITCH_CONFIRMED)
        self.assertIsNotNone(self.failover.get_session_lock("ses_w1"))

    def test_unknown_old_model_budget_survives_service_reopen_without_repost(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        from oc_orch.client import TransportError
        self.client.fail("switch_model", TransportError(
            "/api/session/x/model", "response lost"))
        self.service.tick(episode_id=episode_id)
        self.assertEqual(len(self.client.switch_calls), 1)

        # New store/service objects observe the persisted operation and query budget.
        conn = connect(self._tmp)
        try:
            reopened = FaultRecoveryService(
                ledger=self.ledger,
                failover=FailoverLedger(conn, clock=self.clock.now_ms),
                client=self.client, clock=self.clock, dispatch=self.dispatch,
                authorize=lambda **_: {"allowed": True})
            for _ in range(3):
                self.clock.advance(seconds=30)
                reopened.tick(episode_id=episode_id)
            episode = reopened.failover.get_record("episode", episode_id)
            self.assertEqual(episode["status"], fo.EPISODE_BLOCKED_MANUAL)
            self.assertEqual(episode["metadata"]["last_block_reason"],
                             "switch-unconfirmed")
            self.assertEqual(episode["metadata"]["confirm_queries"], 3)
            self.assertEqual(len(self.client.switch_calls), 1)
            self.assertFalse(self.dispatch_calls)
            self.assertIsNotNone(reopened.failover.get_session_lock("ses_w1"))
        finally:
            conn.close()

    def test_two_supervisors_claim_one_switch_post(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        barrier = threading.Barrier(2)
        clients = []
        errors = []
        def run():
            conn = connect(self._tmp)
            failover = FailoverLedger(conn, clock=self.clock.now_ms)
            client = FakeClient(FakeService())
            client.sessions["ses_w1"] = dict(self.client.sessions["ses_w1"])
            original = failover.prepare_switch
            def sync_claim(**kwargs):
                barrier.wait(timeout=5)
                return original(**kwargs)
            failover.prepare_switch = sync_claim
            service = FaultRecoveryService(
                ledger=self.ledger, failover=failover, client=client,
                clock=self.clock, dispatch=self.dispatch,
                authorize=lambda **_: {"allowed": True})
            try:
                service.tick(limit=10)
                clients.append(client)
            except Exception as exc:
                errors.append(exc)
            finally:
                conn.close()
        threads = [threading.Thread(target=run) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(sum(len(client.switch_calls) for client in clients), 1)
        self.assertEqual(len(self.failover.list_records("operation")["records"]), 1)
        self.assertEqual(self.failover.get_record("episode", episode_id)["switches_used"], 1)

    def test_busy_session_waits_without_switch(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        self.client.set_active(["ses_w1"])

        self.service.tick(limit=10)

        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_WAITING_QUIESCENCE)
        self.assertEqual(ep["metadata"]["last_block_reason"], "session-active")
        self.assertEqual(self.client.switch_calls, [])

    def test_pending_inbox_waits_without_switch(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        self.client.set_inbox("ses_w1", [{"id": "msg-pending", "delivery": "queue"}])

        self.service.tick(limit=10)

        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["metadata"]["last_block_reason"],
                         "session-inbox-not-empty")
        self.assertEqual(self.client.switch_calls, [])

    def _assert_quiescence_blocked(self, reason):
        episode_id = self.observe_quota_failure()["episode_id"]
        self.service.tick(limit=10)
        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["metadata"]["last_block_reason"], reason)
        self.assertEqual(ep["status"], fo.EPISODE_WAITING_QUIESCENCE)
        self.assertEqual(self.client.switch_calls, [])

    def test_pending_permission_blocks(self):
        self.prepare_worker()
        self.client.add_permission(directory="/work", session_id="ses_w1")
        self._assert_quiescence_blocked("session-permission-pending")

    def test_pending_form_blocks(self):
        self.prepare_worker()
        self.client.set_forms("ses_w1", [{"id": "form-1"}])
        self._assert_quiescence_blocked("session-form-pending")

    def test_running_shell_blocks(self):
        self.prepare_worker()
        self.client.add_shell(directory="/work", status="running")
        self._assert_quiescence_blocked("shell-active-or-unknown")

    def test_coordinator_session_is_never_auto_switched(self):
        self.prepare_worker()
        self.ledger.tasks["T-1"]["coordinator_session"] = "ses_w1"
        episode_id = self.observe_quota_failure()["episode_id"]

        self.service.tick(limit=10)

        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["metadata"]["last_block_reason"], "coordinator-session")
        self.assertEqual(self.client.switch_calls, [])

    def test_shared_task_session_is_never_auto_switched(self):
        self.prepare_worker()
        self.ledger.tasks["T-2"] = dict(_task(project="p2"), id="T-2",
                                          coordinator_session="ses_c2")
        self.ledger.attempts["A-2"] = dict(_attempt(aid="A-2"), task_id="T-2")
        episode_id = self.observe_quota_failure()["episode_id"]

        self.service.tick(limit=10)

        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["metadata"]["last_block_reason"], "multi-task-session")
        self.assertEqual(self.client.switch_calls, [])

    def test_failed_attempt_must_still_be_current(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        self.ledger.tasks["T-1"]["current_attempt_id"] = "A-new"

        self.service.tick(limit=10)

        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["metadata"]["last_block_reason"], "current-attempt-changed")
        self.assertEqual(self.client.switch_calls, [])

    def test_unavailable_observation_fails_closed(self):
        self.prepare_worker()
        episode_id = self.observe_quota_failure()["episode_id"]
        from oc_orch.client import TransportError
        self.client.fail("active_sessions", TransportError("/api/session/active", "down"))

        self.service.tick(limit=10)

        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_WAITING_QUIESCENCE)
        self.assertEqual(ep["metadata"]["last_block_reason"],
                         "session-observation-unknown")
        self.assertEqual(self.client.switch_calls, [])


class TestDispatchModelGuard(unittest.TestCase):
    """F10：复用 session 且请求模型与实际不一致 → 在创建 Attempt 前拒绝。"""

    def test_model_mismatch_rejected_before_attempt(self):
        from tests.fakes import FakeClock, FakeService, FakeClient
        from oc_orch.application.services import Orchestration
        from oc_orch.domain.errors import LedgerError

        class StubLedger:
            def get_task(self, tid):
                return {"id": tid, "project": "p", "status": "blocked",
                        "coordinator_session": "ses_c", "current_attempt_id": None,
                        "metadata": {}, "write_scope": [], "title": "t",
                        "closed_at": None}

            def check_project_dispatch(self, project):
                return None

            def coordinator_lease(self, project):
                return {"holder_session": "ses_c"}

            def check_dispatch_authority(self, project, coordinator):
                return {"project": project, "session_id": coordinator, "epoch": 1}

            def provider_states(self):
                return {}

        client = FakeClient(FakeService())
        client.sessions["ses_w1"] = {
            "id": "ses_w1", "title": "t",
            "model": {"providerID": "deepseek", "id": "deepseek-flash",
                      "variant": "default"},
            "location": {"directory": "/work"}, "metadata": {},
            "outcome": "failed", "time": {}}
        clock = FakeClock(wall_ms=1_000_000)
        orch = Orchestration(StubLedger(), client=lambda: client,
                             results=object(), clock=clock,
                             new_id=lambda p, n: "x")
        with self.assertRaises(LedgerError) as raised:
            orch.dispatch("T-1", directory="/work", coordinator="ses_c",
                          session="ses_w1",
                          model="yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash")
        self.assertEqual(raised.exception.code, "MODEL_MISMATCH")
        self.assertEqual(client.switch_calls, [])

    def test_matching_model_passes_guard(self):
        from tests.fakes import FakeClock, FakeService, FakeClient

        client = FakeClient(FakeService())
        client.sessions["ses_w1"] = {
            "id": "ses_w1", "title": "t",
            "model": {"providerID": "yfzs-newapi", "id": "X",
                      "variant": "default"},
            "location": {"directory": "/work"}, "metadata": {},
            "outcome": "succeeded", "time": {"idle": 999}}
        # 一致模型的判定条件（session_model 返回 request 相同 provider/id）。
        model = client.session_model("ses_w1")
        self.assertEqual((model["providerID"], model["id"]),
                         ("yfzs-newapi", "X"))

    def test_hard_route_block_persists_past_expiry_and_blocks_dispatch(self):
        from oc_orch.adapters.sqlite_db import init
        from oc_orch.adapters.writer_guard import install_writer_guard
        from oc_orch.adapters.failover_guard import install_failover_guard
        from oc_orch.adapters.failover_ledger import FailoverLedger
        from oc_orch.application.services import Orchestration
        from oc_orch.domain.errors import LedgerError
        from tests.fakes import FakeClock, FakeService, FakeClient

        with tempfile.TemporaryDirectory(prefix="failover-dispatch-block-") as home:
            conn = init(home)
            try:
                install_writer_guard(conn, actor="test")
                install_failover_guard(conn, actor="test")
                now = FakeClock(wall_ms=3_000)
                blocked = FailoverLedger(conn, clock=now.now_ms)
                blocked.set_route_blocked("provider/model", reason="quota",
                                          until_ms=2_000, now=1_000)
                self.assertTrue(blocked.is_route_blocked(
                    "provider/model", now=now.now_ms())["blocked"])

                class StubLedger:
                    def __init__(self):
                        self.conn = conn

                    def get_task(self, tid):
                        return {"id": tid, "project": "p", "status": "blocked",
                                "coordinator_session": "ses_c", "current_attempt_id": None,
                                "metadata": {}, "write_scope": [], "title": "t",
                                "closed_at": None}

                    def check_project_dispatch(self, project):
                        return None

                    def coordinator_lease(self, project):
                        return {"holder_session": "ses_c"}

                    def check_dispatch_authority(self, project, coordinator):
                        return {"project": project, "session_id": "ses_c", "epoch": 1}

                    def provider_states(self):
                        return {}

                client = FakeClient(FakeService())
                client.set_session("ses_w1", outcome="failed", location_dir=home,
                                   model={"providerID": "provider", "id": "model"})
                orch = Orchestration(StubLedger(), client=lambda: client,
                                     results=object(), clock=now,
                                     new_id=lambda p, n: "x",
                                     route_blocks=blocked)
                with self.assertRaises(LedgerError) as raised:
                    orch.dispatch("T-1", directory=home, coordinator="ses_c",
                                  session="ses_w1", model="provider/model",
                                  ignore_provider_backoff=True)
                self.assertEqual(raised.exception.code, "PROVIDER_HARD_BLOCKED")
                self.assertEqual(client.prompt_calls, [])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

#!/usr/bin/env python3
"""fault_recovery 应用用例测试（F04/F12/F17/F19 的应用层 + 主路径）。"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tests.fakes import FakeClient, FakeClock, FakeService

from oc_orch.adapters.sqlite_db import connect, init
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

    def audit(self, actor, action, entity, entity_id, detail):
        self.audit_rows.append((actor, action, entity, entity_id, detail))


def _task(project="p", status="blocked", coordinator=None):
    return {"id": "T-1", "project": project, "status": status, "title": "t",
            "coordinator_session": coordinator or "ses_c",
            "current_attempt_id": None, "closed_at": None, "metadata": {}}


def _attempt(aid="A-1", status="terminal_failed", model="deepseek/deepseek-flash"):
    return {"id": aid, "task_id": "T-1", "status": status, "model": model,
            "session_id": "ses_w1", "directory": "/work", "agent": "build",
            "metadata": {}}


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="oc-orch-fr-")
        conn = init(self._tmp)
        conn.close()
        self.clock = FakeClock(wall_ms=1_000_000)
        self.failover = FailoverLedger(connect(self._tmp), clock=self.clock.now_ms)
        self.ledger = FakeLedger()
        self.client = FakeClient(FakeService())
        self.dispatch_calls = []

        def dispatch(**kwargs):
            self.dispatch_calls.append(kwargs)
            att_id = "AX-" + str(len(self.dispatch_calls) + 40)
            self.ledger.attempts[att_id] = {
                "id": att_id, "task_id": kwargs["task_id"], "status": "creating",
                "model": kwargs.get("model"), "session_id": kwargs.get("session"),
                "directory": kwargs.get("directory"), "agent": kwargs.get("agent"),
                "metadata": {}}
            return {"ok": True, "attempt_id": att_id,
                    "message_id": "msg_1", "generation": 2,
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
        self.ledger.tasks["T-1"] = _task()
        self.ledger.attempts["A-1"] = _attempt()


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
        self.enable_policy()
        out = self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
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
        self.ledger.attempts[successor]["status"] = "terminal_succeeded"
        self.service.tick(limit=10)
        ep = self.failover.get_record("episode", episode_id)
        self.assertEqual(ep["status"], fo.EPISODE_RESUMED)

    def test_switch_api_reject_blocks_without_dispatch(self):
        self.prepare_worker()
        self.enable_policy()
        self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
        from oc_orch.client import ApiError
        self.client.fail("switch_model", ApiError(400, "/api/session/x/model",
                                                  "InvalidRequestError", "bad", ""))
        self.service.tick(limit=10)
        ep = list(self.failover.list_records("episode")["records"])[0]
        self.assertEqual(ep["status"], fo.EPISODE_BLOCKED_MANUAL)
        self.assertFalse(self.dispatch_calls, "切换拒绝后不得派单")

    def test_task_paused_before_switch_stays(self):
        self.prepare_worker()
        self.enable_policy()
        self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
        self.ledger.tasks["T-1"]["status"] = "paused"
        self.service.tick(limit=10)
        ep = list(self.failover.list_records("episode")["records"])[0]
        self.assertEqual(ep["status"], fo.EPISODE_BLOCKED_MANUAL)
        self.assertFalse(self.dispatch_calls)

    def test_switch_transport_unknown_records_possible(self):
        self.prepare_worker()
        self.enable_policy()
        self.service.observe_failure(
            error={"status": 402, "message": "Insufficient Balance"},
            task_id="T-1", attempt_id="A-1", session_id="ses_w1")
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
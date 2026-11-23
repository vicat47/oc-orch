#!/usr/bin/env python3
"""make_fault_recovery 装配冒烟 + supervisor 观察路径（F01/F25 的运行时冒烟）。

构造与真实 Supervisor 相同的依赖（真实 SQLite 台账 + FakeClient + 共享派单
与资源授权），验证空调谐不炸、策略与路线阻断在同一 meta 命名空间共享。
"""

from __future__ import annotations

import shutil
import tempfile
import unittest
from unittest.mock import patch

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fakes import FakeClient, FakeClock, FakeService

from oc_orch.composition import init
from oc_orch.composition import make_fault_recovery
from oc_orch.adapters.failover_ledger import FailoverLedger
from oc_orch.adapters.writer_guard import install_writer_guard
from oc_orch.adapters.failover_guard import install_failover_guard
from oc_orch.domain import failover as fo
from oc_orch.supervisor import Supervisor


class TestCompositionSmoke(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="oc-orch-frc-")
        conn = init(self._tmp)
        install_writer_guard(conn, actor="test")
        install_failover_guard(conn, actor="test")
        conn.close()
        self.clock = FakeClock(wall_ms=1_000_000)
        self.client = FakeClient(FakeService())

    def tearDown(self):
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _svc(self):
        return make_fault_recovery(
            init(self._tmp), self._tmp, client=lambda: self.client,
            clock=self.clock)

    def test_no_policy_no_crash_zero_network(self):
        # 不预置授权/策略/任务：装配与空调谐应 fail-closed，不抛、零网络。
        svc = self._svc()
        out = svc.tick(limit=5)
        self.assertIsInstance(out, dict)
        self.assertEqual(out.get("scanned", 0), 0)
        self.assertEqual(self.client.switch_calls, [])

    def test_shared_namespace_policy_and_route_block(self):
        conn = init(self._tmp)
        try:
            failover = FailoverLedger(conn, clock=self.clock.now_ms)
            failover.set_policy(
                "project", "p9",
                {"enabled": True, "scope": "project",
                 "source_routes": ["deepseek/deepseek-flash"],
                 "fallback_routes": ["yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash"],
                 "max_switches": 1, "max_attempts": 3},
                expected_revision=None)
            blocked = failover.set_route_blocked(
                "deepseek/deepseek-flash", reason="quota", until_ms=None, now=0)
            self.assertTrue(blocked["route"])
            self.assertTrue(failover.is_route_blocked(
                "deepseek/deepseek-flash", now=1000)["blocked"])
            self.assertEqual(
                failover.get_policy("project", "p9")["fallback_routes"],
                ["yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash"])
            guard = failover.guard_valid()
            self.assertTrue(guard["valid"])
        finally:
            conn.close()

    def test_email_bridge_replays_after_cursor_failure_without_model_or_chat_call(self):
        sup = Supervisor(self._tmp, client=self.client, clock=self.clock)
        self.addCleanup(sup.conn.close)
        task = sup.ledger.create_task(project="bridge-project", title="bridge test")
        chat_notice = sup.ledger.enqueue_notification(
            kind="execution_failed", target_session="ses-coordinator",
            dedupe_key="execution-failed:bridge-test",
            payload={"summary": "provider failed"}, project="bridge-project",
            task_id=task["id"])
        self.assertEqual(sup.ledger.ack_notifications(
            [chat_notice["id"]], actor="acceptance-ack"), 1)
        failover = FailoverLedger(sup.conn, clock=self.clock.now_ms)
        episode = {
            "episode_id": "EP-bridge-replay", "project": "bridge-project",
            "task_id": task["id"], "session_id": "ses-worker",
            "route": "deepseek/deepseek-flash", "category": "quota",
            "status": fo.EPISODE_BLOCKED_MANUAL, "switches_used": 0,
            "attempts_used": 1, "old_attempt_id": "A-failed",
            "created_at": self.clock.now_ms(), "updated_at": self.clock.now_ms(),
            "metadata": {"error_message": "provider quota exhausted"},
        }
        failover.upsert_record("episode", episode["episode_id"], episode,
                               expected_revision=None)

        # Model the crash window after outbox enqueue but before bridge cursor commit.
        with patch.object(FailoverLedger, "set_cursor",
                          side_effect=RuntimeError("cursor commit lost")):
            first = sup.bridge_failover_events(now=self.clock.now_ms())
        self.assertEqual(first["scanned"], 1)
        second = sup.bridge_failover_events(now=self.clock.now_ms())
        self.assertEqual(second["scanned"], 1)
        self.assertEqual(second["enqueued"], 0)
        items = sup._failover_email_service().list_items()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["status"], "pending-auth")
        self.assertEqual(items[0]["incident_type"], "provider-fault")
        self.assertEqual(self.client.switch_calls, [])
        self.assertEqual(self.client.prompt_calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)

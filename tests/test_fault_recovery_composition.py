#!/usr/bin/env python3
"""make_fault_recovery 装配冒烟 + supervisor 观察路径（F01/F25 的运行时冒烟）。

构造与真实 Supervisor 相同的依赖（真实 SQLite 台账 + FakeClient + 共享派单
与资源授权），验证空调谐不炸、策略与路线阻断在同一 meta 命名空间共享。
"""

from __future__ import annotations

import shutil
import tempfile
import unittest

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fakes import FakeClient, FakeClock, FakeService

from oc_orch.composition import init
from oc_orch.composition import make_fault_recovery
from oc_orch.adapters.failover_ledger import FailoverLedger


class TestCompositionSmoke(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="oc-orch-frc-")
        init(self._tmp)
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
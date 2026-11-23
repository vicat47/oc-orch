"""manual-gate 显式解锁 CLI：缺省只清人工中断、拒绝 provider 冷却。

对应 RUNBOOK 的“人工中断不自动复活”：恢复必须是一次可见、受审计的运维动作，
不能靠 runtime 自动清除。2026-11-19 用户授权新增 ``--allow-provider``：operator
可显式对 provider 闸门“恢复调度”（复位为 probe-ready；保留错误证据守卫，新错误
仍重新冷却），缺省仍拒绝，向后兼容。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from oc_orch.adapters.clock import SystemClock
from oc_orch.adapters.sqlite_db import init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.target_gate import TargetGate


class ManualGateUnlockCli(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="manual-unlock-", dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)
        self.project = "/tmp/opencode/project-manual-unlock"
        self.sid = "ses_manual_unlock"
        self.ledger.claim_coordinator(
            project=self.project, session_id=self.sid, actor="test")
        self.gate = TargetGate(self.conn, SystemClock())

    def run_cli(self, *args):
        env = dict(
            os.environ,
            PYTHONPATH=str(ROOT / "src"),
            OC_ORCH_HOME=self.home,
            OC_ORCH_CONFIG_JSON="{}",
            OC_ORCH_SERVICE_CONFIG=str(Path(self.home) / "no-service.json"),
        )
        return subprocess.run(
            [sys.executable, "-B", "-m", "oc_orch.cli", "--home", self.home,
             *args],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=20,
        )

    def test_unlock_clears_manual_resurfaces_pending_and_acks_alert(self):
        self.gate.block_manual(self.sid, reason="coordinator-interrupted")
        now = self.gate._now_ms()
        notification = self.ledger.enqueue_notification(
            kind="result_ready",
            target_session=self.sid,
            dedupe_key="manual-unlock:result",
            payload={"task": "T-test"},
            next_attempt_at=now + 3600 * 1000,
        )
        alert = self.ledger.add_alert(
            severity="high",
            kind="target_gate_manual",
            title="目标被人工中断",
            detail="manual",
            dedupe_key=f"wake-gate:manual:{self.sid}",
        )

        result = self.run_cli(
            "coordinator", "unlock", "--project", self.project,
            "--reason", "operator-confirmed", "--json")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        out = json.loads(result.stdout)
        self.assertTrue(out["cleared"])
        self.assertEqual(out["resurfaced"], 1)
        self.assertEqual(out["acked_alerts"], [alert["id"]])
        self.assertIsNone(self.gate.get(self.sid))
        refreshed = self.ledger.get_notification(notification["id"])
        self.assertLessEqual(refreshed["next_attempt_at"], self.gate._now_ms() + 5000)
        self.assertEqual(self.ledger.get_alert(alert["id"])["state"], "acked")
        rows = self.conn.execute(
            "SELECT action FROM audit WHERE entity='session' AND entity_id=? "
            "AND action='target-gate.clear-manual'", (self.sid,)).fetchall()
        self.assertEqual(len(rows), 1, "解锁必须留下审计记录")

    def test_explicit_block_sets_persistent_pause_gate(self):
        blocked = self.run_cli(
            "coordinator", "block", "--project", self.project,
            "--reason", "operator-pause", "--json")
        self.assertEqual(blocked.returncode, 0,
                         blocked.stdout + blocked.stderr)
        snap = self.gate.get(self.sid)
        self.assertEqual(snap["state"], "manual-blocked")
        self.assertEqual(snap["reason"], "coordinator-paused")

        unlocked = self.run_cli(
            "coordinator", "unlock", "--project", self.project,
            "--reason", "operator-resume", "--json")
        self.assertEqual(unlocked.returncode, 0,
                         unlocked.stdout + unlocked.stderr)
        self.assertIsNone(self.gate.get(self.sid))

    def test_unlock_refuses_provider_cooldown(self):
        classification = {
            "category": "quota", "transient": False, "status": 429,
            "message": "limit", "action": "block-and-alert",
            "limit_kind": "subscription", "code": "provider.limit",
        }
        self.gate.observe_error(
            self.sid, classification, evidence_key="manual-unlock:e1",
            observed_at=self.gate._now_ms())
        result = self.run_cli(
            "coordinator", "unlock", "--project", self.project, "--json")
        self.assertEqual(result.returncode, 7, result.stdout + result.stderr)
        out = json.loads(result.stdout)
        self.assertEqual(out["code"], "gate")
        self.assertEqual(out["detail"]["reason"], "not-manual")
        self.assertEqual(self.gate.get(self.sid)["state"], "blocked")

    def test_unlock_without_gate_is_noop(self):
        result = self.run_cli(
            "coordinator", "unlock", "--project", self.project, "--json")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        out = json.loads(result.stdout)
        self.assertFalse(out["cleared"])
        self.assertEqual(out["reason"], "no-active-gate")

    def test_unlock_provider_with_allow_provider_flag(self):
        classification = {
            "category": "quota", "transient": False, "status": 429,
            "message": "limit", "action": "block-and-alert",
            "limit_kind": "subscription", "code": "provider.limit",
        }
        self.gate.observe_error(
            self.sid, classification, evidence_key="unlock-provider:e1",
            observed_at=self.gate._now_ms())
        now = self.gate._now_ms()
        self.ledger.enqueue_notification(
            kind="result_ready", target_session=self.sid,
            dedupe_key="unlock-provider:result",
            payload={"task": "T-test"}, next_attempt_at=now + 3600 * 1000)
        alert = self.ledger.add_alert(
            severity="high", kind="foreign_input_blocked",
            title="唤醒被阻塞", detail="foreign",
            dedupe_key=f"wake-block:foreign_input_blocked:{self.sid}")
        self.assertEqual(self.gate.get(self.sid)["state"], "blocked")

        result = self.run_cli(
            "coordinator", "unlock", "--project", self.project,
            "--reason", "operator-confirmed-credits-restored",
            "--allow-provider", "--json")

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        out = json.loads(result.stdout)
        self.assertTrue(out["cleared"])
        self.assertEqual(out["cleared_via"], "provider")
        self.assertEqual(out["resurfaced"], 1)
        self.assertEqual(out["acked_alerts"], [alert["id"]])
        snap = self.gate.get(self.sid)
        self.assertIsNotNone(snap, "provider 清门保留记录并复位，而不是删除")
        self.assertEqual(snap["state"], "probe-ready")
        self.assertEqual(snap["source"], "provider")
        rows = self.conn.execute(
            "SELECT action FROM audit WHERE entity='session' AND entity_id=? "
            "AND action='target-gate.clear-provider'", (self.sid,)).fetchall()
        self.assertEqual(len(rows), 1, "provider 解锁必须留下审计记录")
        raw = json.loads(self.conn.execute(
            "SELECT value FROM meta WHERE key=?",
            (f"target-gate:{self.sid}",)).fetchone()[0])
        self.assertIn("unlock-provider:e1", raw["recent_errors"],
                      "保留错误证据守卫，避免同源重复错误绕过冷却")
        self.assertNotIn("probe_observation", raw,
                         "复位后移除只读补证，避免与原 probe 归属不一致")
        # 新错误仍重新登记冷却（fail-closed 不变量不变，不是持久绕过）
        self.gate.observe_error(
            self.sid, classification, evidence_key="unlock-provider:e2",
            observed_at=self.gate._now_ms())
        self.assertEqual(self.gate.get(self.sid)["state"], "blocked")

    def test_clear_provider_refuses_manual_and_no_gate(self):
        self.gate.block_manual(self.sid, reason="coordinator-interrupted")
        r = self.gate.clear_provider(self.sid, reason="op")
        self.assertFalse(r["cleared"])
        self.assertEqual(r["reason"], "not-provider")
        self.assertEqual(self.gate.get(self.sid)["state"], "manual-blocked")
        r = self.gate.clear_provider("ses_no_gate", reason="op")
        self.assertFalse(r["cleared"])
        self.assertEqual(r["reason"], "no-active-gate")

    def test_unlock_rejects_non_holder_session(self):
        self.gate.block_manual(self.sid, reason="coordinator-interrupted")
        result = self.run_cli(
            "coordinator", "unlock", "--project", self.project,
            "--session", "ses_other", "--json")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        out = json.loads(result.stdout)
        self.assertEqual(out["code"], "usage")
        self.assertEqual(self.gate.get(self.sid)["state"], "manual-blocked")


if __name__ == "__main__":
    unittest.main(verbosity=2)

#!/usr/bin/env python3
"""显式回合恢复用例：运维解除 hold / 登记已完成用户回合。

覆盖共享 application 层而不依赖 Web/真实 OpenCode：
- 只接受项目当前租约 holder 与匹配 epoch；
- 成功时重置协调失败、提前 pending 通知、ACK interrupted 告警；
- 失败（无 hold/旧 CAS/barrier/未核验）不写控制状态、不冒充已恢复。
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from oc_orch.adapters.coordinator_control import CoordinatorControlLedger
from oc_orch.adapters.sqlite_db import init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.application.coordinator_control import CoordinatorControlService
from oc_orch.application.manual_recovery import (
    register_completed_user_turn,
    release_turn_hold,
)
from oc_orch.domain.errors import ActionError
from tests.fakes import FakeClock


class ManualTurnRecoveryTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="turn-recovery-",
                                          dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.ledger = Ledger(self.conn, self.home)
        self.control = CoordinatorControlLedger(self.conn, self.clock)
        self.service = CoordinatorControlService(self.control, self.clock)
        self.project = "/tmp/opencode/project-turn-recovery"
        self.sid = "ses_turn_recovery"
        self.lease = self.ledger.claim_coordinator(
            project=self.project, session_id=self.sid, actor="test", ttl=3600)

    def hold(self, *, at=None, reason="coordinator-interrupted"):
        at = at if at is not None else self.clock.now_ms()
        result = self.service.hold_session(
            self.sid, event_id=f"ev-{at}", origin_at=at, reason=reason)
        self.assertTrue(result["executed"])
        return at

    def pending_notification(self):
        return self.ledger.enqueue_notification(
            kind="result_ready", target_session=self.sid,
            dedupe_key=f"turn-recovery:{self.clock.now_ms()}",
            payload={"summary": "pending"},
            project=self.project,
            next_attempt_at=self.clock.now_ms() + 3600 * 1000)

    def test_release_resets_failure_resurfaces_and_acks_alert(self):
        held_at = self.hold()
        notification = self.pending_notification()
        alert = self.ledger.add_alert(
            severity="high", kind="coordinator_interrupted",
            title="主协调 interrupted", detail=json.dumps(
                {"project": self.project, "session_id": self.sid}),
            dedupe_key=f"coord-failure:{self.project}:{held_at}")
        rev = self.service.get_session(self.sid)["revision"]
        out = release_turn_hold(
            self.service, self.ledger, project=self.project,
            session=self.sid, operation_id="op-release",
            reason="operator-resume", expected_revision=rev,
            expected_epoch=self.lease["epoch"], actor="test",
            now=self.clock.now_ms())
        self.assertTrue(out["ok"])
        result = out["result"]
        self.assertTrue(result["executed"])
        self.assertTrue(result["applied"])
        self.assertEqual(out["resurfaced"], 1)
        self.assertEqual(out["acked_alerts"], [alert["id"]])
        self.assertIsNone(self.service.get_session(self.sid)["hold"])
        refreshed = self.ledger.get_notification(notification["id"])
        self.assertLessEqual(refreshed["next_attempt_at"],
                             self.clock.now_ms())
        self.assertEqual(self.ledger.get_alert(alert["id"])["state"], "acked")
        lease = self.ledger.coordinator_lease(self.project)
        self.assertEqual(lease["failure_count"], 0)

    def test_release_rejects_wrong_holder_and_epoch_without_write(self):
        self.hold()
        rev = self.service.get_session(self.sid)["revision"]
        with self.assertRaises(ActionError):
            release_turn_hold(
                self.service, self.ledger, project=self.project,
                session="ses_other", operation_id="op-release",
                reason="operator-resume", expected_revision=rev,
                expected_epoch=self.lease["epoch"], actor="test",
                now=self.clock.now_ms())
        with self.assertRaises(ActionError):
            release_turn_hold(
                self.service, self.ledger, project=self.project,
                session=self.sid, operation_id="op-release",
                reason="operator-resume", expected_revision=rev,
                expected_epoch=self.lease["epoch"] + 1, actor="test",
                now=self.clock.now_ms())
        self.assertIsNotNone(self.service.get_session(self.sid)["hold"])

    def test_release_refuses_barrier(self):
        held_at = self.hold()
        self.service.begin_user_turn(
            self.sid, message_id="msg-new", origin_at=held_at + 10,
            operation_id="op-turn", trusted=True)
        rev = self.service.get_session(self.sid)["revision"]
        out = release_turn_hold(
            self.service, self.ledger, project=self.project,
            session=self.sid, operation_id="op-release",
            reason="operator-resume", expected_revision=rev,
            expected_epoch=self.lease["epoch"], actor="test",
            now=self.clock.now_ms())
        self.assertFalse(out["ok"])
        self.assertFalse(out["result"]["executed"])
        self.assertEqual(out["result"]["refused"], "turn-active")
        self.assertIsNotNone(self.service.get_session(self.sid)["barrier"])

    def test_register_completed_turn_uses_verified_evidence(self):
        held_at = self.hold()
        notification = self.pending_notification()
        verified = {"finished": True, "reason": "completed-user-turn-verified",
                    "message_id": "msg-user-done", "origin_at": held_at + 10,
                    "idle_id": "idle-done", "idle_at": held_at + 30,
                    "outcome": "succeeded", "coverage": {"complete": True}}
        rev = self.service.get_session(self.sid)["revision"]
        out = register_completed_user_turn(
            self.service, self.ledger,
            lambda sid, mid: verified if mid == "msg-user-done"
            else {"finished": False, "reason": "not-candidate"},
            project=self.project, session=self.sid,
            operation_id="op-register", reason="operator-registered",
            expected_revision=rev, expected_epoch=self.lease["epoch"],
            message_id="msg-user-done", actor="test",
            now=self.clock.now_ms())
        self.assertTrue(out["ok"])
        self.assertTrue(out["result"]["executed"])
        self.assertIsNone(self.service.get_session(self.sid)["hold"])
        self.assertIsNone(self.service.get_session(self.sid)["barrier"])
        self.assertEqual(out["resurfaced"], 1)
        refreshed = self.ledger.get_notification(notification["id"])
        self.assertLessEqual(refreshed["next_attempt_at"],
                             self.clock.now_ms())

    def test_register_refuses_unverified_without_write(self):
        self.hold()
        rev = self.service.get_session(self.sid)["revision"]
        out = register_completed_user_turn(
            self.service, self.ledger,
            lambda sid, mid: {"finished": False,
                              "reason": "ambiguous-user-window"},
            project=self.project, session=self.sid,
            operation_id="op-register", reason="operator-registered",
            expected_revision=rev, expected_epoch=self.lease["epoch"],
            message_id="msg-user-done", actor="test",
            now=self.clock.now_ms())
        self.assertFalse(out["ok"])
        self.assertEqual(out["refused"], "ambiguous-user-window")
        snap = self.service.get_session(self.sid)
        self.assertIsNotNone(snap["hold"])
        self.assertIsNone(snap["barrier"])
        self.assertEqual(snap["revision"], rev)


if __name__ == "__main__":
    unittest.main(verbosity=2)

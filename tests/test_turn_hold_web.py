#!/usr/bin/env python3
"""回合 hold 的 Web 入口：只读展示、候选预览、登记完成回合、运维强制解除。

只使用临时 SQLite + 真实 WebServer + 假 session 客户端；不访问真实
OpenCode/邮箱/配置。验证 Web 只做结构化转发：确认/类型/CAS/CSRF/read_only
与共享用例拒绝都如实返回，不冒充已恢复。
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from oc_orch.adapters.coordinator_control import CoordinatorControlLedger
from oc_orch.adapters.sqlite_db import init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.util import now_ms
from oc_orch.web import WebServer
from tests.fakes import FakeClock
from tests.test_continuity_web import ContinuityWebBase

SID = "ses_coord"
PROJECT = "p"
T0 = 1_000_000


class FakeSessionClient:
    """最小 OpenCode V2 session 客户端：固定窗口与终态。"""

    def __init__(self):
        self.session = {
            "id": SID, "outcome": "succeeded",
            "time": {"created": T0 - 100, "busy": T0 - 100,
                     "idle": T0 + 30, "updated": T0 + 30},
        }
        self.rows = [
            {"id": "msg_user_done", "type": "user",
             "time": {"created": T0 + 10}},
            {"id": "msg_assistant", "type": "assistant",
             "time": {"created": T0 + 20}},
            {"id": "idle_done", "type": "idle", "outcome": "succeeded",
             "time": {"created": T0 + 30}},
        ]
        self.active = {}

    def get_session(self, session_id):
        return dict(self.session) if session_id == SID else None

    def messages(self, session_id, *, limit=50, order="desc"):
        rows = list(self.rows)
        rows.sort(key=lambda m: m["time"]["created"], reverse=(order == "desc"))
        return rows[:limit]

    def message_page(self, session_id, *, limit=50, order="asc", cursor=None):
        rows = list(self.rows)
        rows.sort(key=lambda m: m["time"]["created"])
        return {"data": rows, "cursor": {"previous": None, "next": None}}

    def active_sessions(self):
        return dict(self.active)

    def inbox(self, session_id):
        return []


class TurnHoldWebBase(ContinuityWebBase):
    def setUp(self):
        super().setUp()
        self.fake = FakeSessionClient()
        self.server.client_factory = lambda: self.fake
        self.control = CoordinatorControlLedger(
            self.conn, FakeClock(wall_ms=T0))
        self.sid = SID
        self.project = PROJECT

    def hold(self, *, reason="coordinator-interrupted", at=T0):
        result = self.control.hold_session(
            self.sid, event_id=f"ev-hold-{at}", origin_at=at, reason=reason)
        self.assertTrue(result["executed"])
        return result

    def revision(self):
        return self.control.get_session(self.sid)["revision"]

    def release_body(self, **overrides):
        body = {
            "project": self.project, "session_id": self.sid,
            "operation_id": "web-release-1",
            "reason": "web:operator-forced-resume",
            "expected_revision": self.revision(),
            "expected_epoch": self.lease["epoch"],
            "confirm": self.project,
        }
        body.update(overrides)
        return body

    def register_body(self, **overrides):
        body = {
            "project": self.project, "session_id": self.sid,
            "operation_id": "web-register-1",
            "reason": "web:operator-registered-completed-turn",
            "message_id": "msg_user_done",
            "expected_revision": self.revision(),
            "expected_epoch": self.lease["epoch"],
            "confirm": self.project,
        }
        body.update(overrides)
        return body


class TurnHoldWebTests(TurnHoldWebBase):
    def test_detail_exposes_hold_and_barrier(self):
        self.hold()
        status, data = self.get(f"/api/coordinator/{self.sid}")
        self.assertEqual(status, 200, data)
        control = data.get("session_control") or {}
        self.assertTrue(control.get("ok"))
        self.assertTrue(control.get("held"))
        self.assertEqual(control["hold"]["reason"], "coordinator-interrupted")
        self.assertIsNone(control.get("barrier"))
        self.assertEqual(control.get("revision"), self.revision())

    def test_detail_survives_control_state_error(self):
        conn = init(self.tmp)
        try:
            conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?)",
                ("coordinator-control:session:" + self.sid, "{bad json"))
            conn.commit()
        finally:
            conn.close()
        status, data = self.get(f"/api/coordinator/{self.sid}")
        self.assertEqual(status, 200, data)
        self.assertFalse(data["session_control"]["ok"])

    def test_turn_candidate_requires_exact_scope_and_holder(self):
        status, _ = self.get("/api/coordinator/turn-candidate?project=p")
        self.assertEqual(status, 400)
        status, _ = self.get(
            "/api/coordinator/turn-candidate?project=other&session_id=" + SID)
        self.assertEqual(status, 400)

    def test_turn_candidate_eligible_after_completed_user_turn(self):
        self.hold()
        status, data = self.get(
            f"/api/coordinator/turn-candidate?project={PROJECT}&session_id={SID}")
        self.assertEqual(status, 200, data)
        self.assertTrue(data["eligible"])
        self.assertEqual(data["message_id"], "msg_user_done")
        self.assertEqual(data["idle_id"], "idle_done")
        self.assertEqual(data["revision"], self.revision())

    def test_turn_candidate_refuses_auto_wake_message(self):
        self.hold()
        # 把一个自动唤醒输入登记到台账并绑定候选消息身份：候选必须拒绝，
        # 不能把自动消息当真人。
        reserved = self.ledger.reserve_target_input(
            self.sid, owner="test", notification_ids=[],
            body="", source="automatic_wake", now=T0 + 10)
        self.conn.execute(
            "UPDATE session_inputs SET message_id=? WHERE input_id=?",
            ("msg_user_done", reserved["input"]["input_id"]))
        self.conn.commit()
        status, data = self.get(
            f"/api/coordinator/turn-candidate?project={PROJECT}&session_id={SID}")
        self.assertEqual(status, 200, data)
        self.assertFalse(data["eligible"])
        self.assertEqual(data["reason"], "known-automatic-input")

    def test_register_requires_confirm_and_revision(self):
        self.hold()
        status, data = self.post(
            "/api/coordinator/register-turn",
            self.register_body(confirm="wrong"))
        self.assertEqual(status, 400, data)
        self.assertEqual(data["code"], "CONFIRM_REQUIRED")
        status, data = self.post(
            "/api/coordinator/register-turn",
            self.register_body(expected_revision=self.revision() + 1))
        self.assertEqual(status, 200, data)
        self.assertFalse(data["ok"])
        self.assertEqual(data["result"]["refused"], "revision-changed")
        self.assertIsNotNone(self.control.get_session(self.sid)["hold"])

    def test_register_success_clears_hold_and_resurfaces(self):
        self.hold()
        notification = self.ledger.enqueue_notification(
            kind="result_ready", target_session=self.sid,
            dedupe_key="register-web-pending",
            payload={"summary": "pending"}, project=PROJECT,
            next_attempt_at=now_ms() + 3600 * 1000)
        status, data = self.post(
            "/api/coordinator/register-turn", self.register_body())
        self.assertEqual(status, 200, data)
        self.assertTrue(data["ok"], data)
        self.assertTrue(data["result"]["executed"])
        self.assertIsNone(self.control.get_session(self.sid)["hold"])
        self.assertIsNone(self.control.get_session(self.sid)["barrier"])
        refreshed = self.ledger.get_notification(notification["id"])
        self.assertLessEqual(refreshed["next_attempt_at"], now_ms() + 5000)

    def test_release_hold_success_then_barrier_refused(self):
        self.hold()
        status, data = self.post(
            "/api/coordinator/release-hold", self.release_body())
        self.assertEqual(status, 200, data)
        self.assertTrue(data["ok"], data)
        self.assertTrue(data["result"]["applied"])
        self.assertIsNone(self.control.get_session(self.sid)["hold"])

        self.hold(at=T0 + 100)
        self.control.begin_user_turn(
            self.sid, message_id="msg_new", origin_at=T0 + 110,
            operation_id="op-turn", trusted=True)
        status, data = self.post(
            "/api/coordinator/release-hold", self.release_body(
                operation_id="web-release-2",
                expected_revision=self.revision()))
        self.assertEqual(status, 200, data)
        self.assertFalse(data["ok"])
        self.assertEqual(data["result"]["refused"], "turn-active")
        self.assertIsNotNone(self.control.get_session(self.sid)["barrier"])

    def test_release_hold_requires_reason_types_and_csrf(self):
        self.hold()
        status, data = self.post(
            "/api/coordinator/release-hold",
            self.release_body(expected_revision=True))
        self.assertEqual(status, 400, data)
        status, data = self.post(
            "/api/coordinator/release-hold", self.release_body(reason=""))
        self.assertEqual(status, 400, data)
        status, data = self.post(
            "/api/coordinator/release-hold", self.release_body(),
            headers={"Origin": "https://evil.invalid"})
        self.assertEqual(status, 403, data)

    def test_read_only_server_rejects_release_and_register(self):
        self.hold()
        ro_server = WebServer(self.tmp, port=0, read_only=True)
        ro_server.client_factory = lambda: self.fake
        port = ro_server.start()
        self.addCleanup(ro_server.stop)
        for path, body in (
                ("/api/coordinator/release-hold", self.release_body()),
                ("/api/coordinator/register-turn", self.register_body())):
            import urllib.error
            import urllib.request
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}{path}",
                data=json.dumps(body).encode(), method="POST",
                headers={"Content-Type": "application/json",
                         "X-OC-Orch-Web": "1"})
            try:
                urllib.request.urlopen(req, timeout=15)
                self.fail("read_only 写动作必须被拒绝")
            except urllib.error.HTTPError as exc:
                self.assertIn(exc.code, (400, 403))


if __name__ == "__main__":
    unittest.main(verbosity=2)

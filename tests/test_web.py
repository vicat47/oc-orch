#!/usr/bin/env python3
"""Web 控制面最小集成测试：无 token、loopback、CSRF、只读、动作走 CLI。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.composition import SystemClock, make_target_gate  # noqa: E402
from oc_orch.config import load_config  # noqa: E402
from oc_orch.db import init  # noqa: E402
from oc_orch.ledger import Ledger  # noqa: E402
from oc_orch.adapters.writer_guard import install_writer_guard  # noqa: E402
from oc_orch.adapters.failover_guard import install_failover_guard  # noqa: E402
from oc_orch.adapters.failover_ledger import FailoverLedger  # noqa: E402
from oc_orch.results import collect_attempt, scan_deliverable_baseline  # noqa: E402
from oc_orch.util import now_ms  # noqa: E402
from oc_orch.web import WebServer  # noqa: E402


class _FakeClient:
    """提供可归属的 V2 完成窗口，不触网；不手写结果 schema。"""

    def __init__(self, attempt):
        self.attempt = attempt
        self.ended = attempt["terminal_at"]
        self.rows = [
            {"id": attempt["dispatch_msg_id"], "type": "user", "text": "execute task",
             "time": {"created": self.ended - 5}},
            {"id": "msg_web_answer", "type": "assistant", "finish": "stop",
             "time": {"created": self.ended - 4, "completed": self.ended - 1},
             "content": [{"type": "text", "text": "web collected"}]},
            {"id": "msg_web_idle", "type": "idle", "outcome": "succeeded",
             "time": {"created": self.ended}},
        ]

    def messages(self, session_id=None, limit=None, order=None):
        return self.rows

    def message_page(self, session_id, *, limit=100, order="asc", cursor=None):
        rows = sorted(self.rows, key=lambda m: (m["time"]["created"], m["id"]),
                      reverse=order == "desc")
        return {"data": rows, "cursor": {"next": None, "previous": None}}

    def get_message(self, session_id, message_id):
        return next((m for m in self.rows if m["id"] == message_id), None)

    def inbox(self, session_id):
        return []

    def active_sessions(self):
        return {}

    def get_session(self, session_id):
        return {"id": session_id, "outcome": "succeeded",
                "time": {"busy": self.ended - 5, "idle": self.ended}}


class _FakeTitlesClient:
    def get_session(self, session_id):
        if session_id == "ses_titles":
            return {"id": session_id, "title": "协调者甲"}
        return None


class TestWeb(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="oc-orch-web-")
        self.conn = init(self.tmp)
        self.ledger = Ledger(self.conn, self.tmp)
        self.server = WebServer(self.tmp, port=0)
        self.port = self.server.start()

    def tearDown(self):
        self.server.stop()
        self.conn.close()

    def req(self, path: str, *, method="GET", body=None, headers=None):
        data = None
        h = dict(headers or {})
        if body is not None:
            data = json.dumps(body).encode()
            h.setdefault("Content-Type", "application/json")
        r = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}",
                                   data=data, headers=h, method=method)
        try:
            with urllib.request.urlopen(r, timeout=10) as resp:
                return resp.status, resp.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    def test_dashboard_and_status(self):
        status, body = self.req("/")
        self.assertEqual(status, 200)
        self.assertIn("oc-orch", body)
        status, body = self.req("/api/status")
        self.assertEqual(status, 200)
        data = json.loads(body)
        self.assertTrue(data["ok"])
        self.assertNotIn("dispatch", body)  # 不泄漏完整 prompt
        self.assertIn("read_only", data["snapshot"])

    def test_task_detail_shows_failover_without_remote_calls_or_private_error(self):
        task = self.ledger.create_task(project="/tmp/p-failover",
                                       title="failover projection")
        install_writer_guard(self.conn, actor="test")
        install_failover_guard(self.conn, actor="test")
        failover = FailoverLedger(self.conn, clock=now_ms)
        failover.upsert_record("episode", "EP-web-readonly", {
            "episode_id": "EP-web-readonly", "project": task["project"],
            "task_id": task["id"], "session_id": "ses-web-readonly",
            "route": "provider/model", "category": "quota", "status": "open",
            "switches_used": 0, "attempts_used": 1, "created_at": now_ms(),
            "updated_at": now_ms(),
            "metadata": {"error_message": "private provider response body"},
        }, expected_revision=None)
        calls = []
        self.server.client_factory = lambda: calls.append("client")

        status, body = self.req(f"/api/task/{task['id']}")

        self.assertEqual(status, 200, body)
        data = json.loads(body)
        self.assertTrue(data["failover"]["read_only"])
        self.assertEqual(data["failover"]["network_calls"], 0)
        self.assertEqual(data["failover"]["episodes"][0]["episode_id"],
                         "EP-web-readonly")
        self.assertNotIn("private provider response body", body)
        self.assertEqual(calls, [])

    def test_coordinator_titles(self):
        self.server.client_factory = lambda: _FakeTitlesClient()
        self.ledger.create_task(project="p", title="t", coordinator_session="ses_titles")
        status, body = self.req("/api/coordinator-titles")
        self.assertEqual(status, 200, body)
        data = json.loads(body)
        self.assertEqual(data["titles"], {"ses_titles": "协调者甲"})

    def test_csrf_and_ack(self):
        n = self.ledger.enqueue_notification(
            kind="test", target_session="ses_web", dedupe_key="web:1",
            payload={"summary": "test"},
        )
        status, _ = self.req("/api/notify/ack", method="POST",
                             body={"ids": [n["id"]]})
        self.assertEqual(status, 403)
        status, body = self.req(
            "/api/notify/ack", method="POST", body={"ids": [n["id"]]},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 200, body)
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "acked")

    def test_manual_gate_unlock_and_status(self):
        sid, project = "ses_web_coord", "/tmp/opencode/web-project"
        self.ledger.claim_coordinator(project=project, session_id=sid, actor="test")
        gate = make_target_gate(self.conn, SystemClock())
        gate.block_manual(sid, reason="interrupted")
        note = self.ledger.enqueue_notification(
            kind="result_ready", target_session=sid, dedupe_key="web:unlock",
            payload={"task": "T-web"}, next_attempt_at=now_ms() + 3600 * 1000,
        )
        alert = self.ledger.add_alert(
            severity="high", kind="target_gate_manual", title="manual",
            detail="manual", dedupe_key=f"wake-gate:manual:{sid}",
        )

        status, body = self.req("/api/status")
        self.assertEqual(status, 200, body)
        gates = json.loads(body)["snapshot"]["target_gates"]
        self.assertEqual(next(g for g in gates if g["session_id"] == sid)["state"],
                         "manual-blocked")

        status, body = self.req(
            "/api/coordinator/unlock", method="POST",
            body={"project": project, "session": sid},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 400, body)
        self.assertEqual(json.loads(body)["code"], "CONFIRM_REQUIRED")
        self.assertIsNotNone(gate.get(sid))

        status, body = self.req(
            "/api/coordinator/unlock", method="POST",
            body={"project": project, "session": sid, "reason": "web",
                  "confirm": "UNLOCK"},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 200, body)
        result = json.loads(body)["result"]
        self.assertTrue(result["cleared"])
        self.assertEqual(result["resurfaced"], 1)
        self.assertEqual(result["acked_alerts"], [alert["id"]])
        self.assertIsNone(gate.get(sid))
        self.assertLessEqual(
            self.ledger.get_notification(note["id"])["next_attempt_at"],
            now_ms() + 5000)
        self.assertEqual(self.ledger.get_alert(alert["id"])["state"], "acked")

    def test_manual_gate_unlock_refuses_provider_gate(self):
        sid, project = "ses_web_provider", "/tmp/opencode/web-provider"
        self.ledger.claim_coordinator(project=project, session_id=sid, actor="test")
        gate = make_target_gate(self.conn, SystemClock())
        gate.observe_error(
            sid,
            {"category": "quota", "transient": False, "status": 429,
             "message": "limit", "action": "block-and-alert",
             "limit_kind": "subscription", "code": "provider.limit"},
            evidence_key="web:e1", observed_at=now_ms(),
        )
        status, body = self.req(
            "/api/coordinator/unlock", method="POST",
            body={"project": project, "session": sid, "confirm": "UNLOCK"},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 400, body)
        self.assertEqual(json.loads(body)["code"], "gate")
        self.assertEqual(gate.get(sid)["state"], "blocked")

    def test_provider_gate_unlock_with_allow_provider(self):
        sid, project = "ses_web_provider_v2", "/tmp/opencode/web-provider-v2"
        self.ledger.claim_coordinator(project=project, session_id=sid, actor="test")
        gate = make_target_gate(self.conn, SystemClock())
        gate.observe_error(
            sid,
            {"category": "quota", "transient": False, "status": 429,
             "message": "limit", "action": "block-and-alert",
             "limit_kind": "subscription", "code": "provider.limit"},
            evidence_key="web:pv2:e1", observed_at=now_ms(),
        )
        note = self.ledger.enqueue_notification(
            kind="result_ready", target_session=sid,
            dedupe_key="web:pv2:note", payload={"task": "T-pv2"},
            next_attempt_at=now_ms() + 3600 * 1000)
        self.assertEqual(gate.get(sid)["state"], "blocked")

        # provider 清门是 operator 动作：缺理由拒绝
        status, body = self.req(
            "/api/coordinator/unlock", method="POST",
            body={"project": project, "session": sid,
                  "allow_provider": True, "confirm": "UNLOCK"},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 400, body)
        self.assertEqual(json.loads(body)["code"], "BAD_REQUEST")
        self.assertEqual(gate.get(sid)["state"], "blocked")

        status, body = self.req(
            "/api/coordinator/unlock", method="POST",
            body={"project": project, "session": sid, "reason": "limits restored",
                  "allow_provider": True, "confirm": "UNLOCK"},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 200, body)
        result = json.loads(body)["result"]
        self.assertTrue(result["cleared"])
        self.assertEqual(result["cleared_via"], "provider")
        self.assertEqual(result["resurfaced"], 1)
        snap = gate.get(sid)
        self.assertIsNotNone(snap)
        self.assertEqual(snap["state"], "probe-ready")
        self.assertEqual(snap["source"], "provider")
        self.assertLessEqual(
            self.ledger.get_notification(note["id"])["next_attempt_at"],
            now_ms() + 5000)
        rows = self.conn.execute(
            "SELECT action FROM audit WHERE entity='session' AND entity_id=? "
            "AND action='target-gate.clear-provider'", (sid,)).fetchall()
        self.assertEqual(len(rows), 1, "Web provider 解锁必须写审计")

    def test_task_cancel_confirmation(self):
        t = self.ledger.create_task(project="p", title="web cancel")
        status, body = self.req(
            f"/api/task/{t['id']}/cancel", method="POST",
            body={"reason": "web", "confirm": "wrong"},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 400, body)
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "draft")
        status, body = self.req(
            f"/api/task/{t['id']}/cancel", method="POST",
            body={"reason": "web", "confirm": t["id"]},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 200, body)
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "cancelled")

    def test_origin_rejected(self):
        n = self.ledger.enqueue_notification(
            kind="test", target_session="ses_web", dedupe_key="web:2",
            payload={"summary": "test"},
        )
        status, body = self.req(
            "/api/notify/ack", method="POST", body={"ids": [n["id"]]},
            headers={"X-OC-Orch-Web": "1", "Origin": "http://evil.example"},
        )
        self.assertEqual(status, 403, body)

    def test_verify_confirmation_and_gate(self):
        t = self.ledger.create_task(project="p", title="web verify")
        a = self.ledger.create_attempt(
            t["id"], directory=self.tmp, status="dispatched",
            dispatch_state="confirmed", session_id="ses_web_verify",
            dispatch_msg_id="msg_web_input",
        )
        baseline = scan_deliverable_baseline(self.ledger, a["id"], captured_at=now_ms())
        self.ledger.seal_execution(a["id"], mode="new", session_before={}, baseline=baseline)
        started = now_ms()
        self.ledger.record_execution_started(a["id"], started_at=started,
                                             event_id="start-web")
        terminal = self.ledger.record_terminal(
            a["id"], outcome="succeeded", source="sse", event_id="end-web",
            candidate={"session_id": a["session_id"], "source": "sse",
                       "outcome": "succeeded", "ended_at": started + 5,
                       "input_in_inbox": False, "input_owner": "unique"})
        self.assertEqual(terminal["result"], "accepted", terminal)
        # 显式释放现场：CLI 后置 ensure 早退、绝不触网；保留 Origin/confirm 检查。
        self.ledger.release_site(a["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        # 1) confirm 不匹配：400，不进入验收
        status, body = self.req(
            f"/api/attempt/{a['id']}/verify", method="POST",
            body={"pass": True, "confirm": "wrong", "evidence": ["x"]},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 400, body)
        self.assertNotEqual(self.ledger.get_task(t["id"])["status"], "accepted")
        # 2) confirm 正确但尚未 collect：必须 400/code=gate 且无 accepted/verification
        #    残留（旧实现 200+accepted=红；统一门禁实现=绿）
        status, body = self.req(
            f"/api/attempt/{a['id']}/verify", method="POST",
            body={"pass": True, "confirm": a["id"], "evidence": ["web-test"]},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 400, body)
        data = json.loads(body)
        self.assertEqual(data.get("code"), "gate")
        self.assertNotEqual(self.ledger.get_task(t["id"])["status"], "accepted")
        self.assertNotEqual(self.ledger.get_attempt(a["id"])["status"], "accepted")
        self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"])
        # 3) 真实 collect：基于原输入与完成区间产生真实结果文件
        collect_attempt(self.ledger, _FakeClient(self.ledger.get_attempt(a["id"])), a["id"],
                        load_config(self.tmp), auto=False)
        # 4) collect 后再验收：200，task/attempt accepted，verification 落库
        status, body = self.req(
            f"/api/attempt/{a['id']}/verify", method="POST",
            body={"pass": True, "confirm": a["id"], "evidence": ["web-test"]},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 200, body)
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "accepted")
        self.assertEqual(self.ledger.get_attempt(a["id"])["status"], "accepted")
        self.assertEqual(
            self.ledger.get_attempt(a["id"])["verification"]["result"], "pass")
        self.assertEqual(
            self.ledger.get_attempt(a["id"])["verification"].get("classification"),
            "normal_effective")

    def test_verify_force_reason_passthrough(self):
        """force reason 透传：空白 reason 必须 400/code=usage 且无验收写入
        （旧实现 200=红）；明确非空 reason 允许例外通过，
        result.verification.forced/force_reason/missing 与例外分类保留。"""
        t = self.ledger.create_task(project="p", title="web force")
        a = self.ledger.create_attempt(
            t["id"], directory=self.tmp, status="dispatched",
            dispatch_state="confirmed", deliverables=["out.txt"],
        )
        self.ledger.transition_attempt(a["id"], "terminal_succeeded")
        self.ledger.release_site(a["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        # 1) force + 空白 reason：400/code=usage，无 verification 写入、不 accepted
        status, body = self.req(
            f"/api/attempt/{a['id']}/verify", method="POST",
            body={"pass": True, "confirm": a["id"],
                  "force": True, "reason": "   "},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 400, body)
        data = json.loads(body)
        self.assertEqual(data.get("code"), "usage")
        self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"])
        self.assertNotEqual(self.ledger.get_task(t["id"])["status"], "accepted")
        # 2) force + 明确非空 reason：200，forced=true、force_reason/missing 保留
        status, body = self.req(
            f"/api/attempt/{a['id']}/verify", method="POST",
            body={"pass": True, "confirm": a["id"],
                  "force": True, "reason": "例外通过", "evidence": ["force-ev"]},
            headers={"X-OC-Orch-Web": "1"},
        )
        self.assertEqual(status, 200, body)
        data = json.loads(body)
        self.assertTrue(data.get("ok"))
        result = data["result"]
        self.assertEqual(result["task"]["status"], "accepted")
        verif = result["verification"]
        self.assertTrue(verif["forced"])
        self.assertEqual(verif.get("classification"), "forced")
        self.assertEqual(verif.get("actor"), "coordinator")
        self.assertTrue(verif.get("unmet"))
        self.assertEqual(verif["force_reason"], "例外通过")
        self.assertIn("out.txt", verif["missing"])


class TestWebReadOnly(unittest.TestCase):
    def test_read_only(self):
        tmp = tempfile.mkdtemp(prefix="oc-orch-web-ro-")
        conn = init(tmp)
        ledger = Ledger(conn, tmp)
        n = ledger.enqueue_notification(
            kind="test", target_session="ses_web", dedupe_key="web:ro",
            payload={"summary": "test"},
        )
        server = WebServer(tmp, port=0, read_only=True)
        port = server.start()
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/notify/ack",
                data=json.dumps({"ids": [n["id"]]}).encode(),
                headers={"Content-Type": "application/json",
                         "X-OC-Orch-Web": "1"},
                method="POST",
            )
            try:
                urllib.request.urlopen(req, timeout=10)
                self.fail("should be forbidden")
            except urllib.error.HTTPError as e:
                self.assertEqual(e.code, 403)
            self.assertEqual(ledger.get_notification(n["id"])["state"], "pending")
        finally:
            server.stop()
            conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

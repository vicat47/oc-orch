#!/usr/bin/env python3
"""R2 Web/API 契约：业务健康投影、本地人工待办、项目暂停/继续（共享应用用例）。

- GET /api/health：composition.make_continuity_status 本地只读投影；进程健康仅
  展示、不推进 runtime 游标、不构造 Client；scope 参数错误稳定 400。
- GET /api/incidents：OperatorIncidentService 脱敏只读；不 reconcile、不发邮件。
- POST /api/coordinator/pause|resume：ConversationControl 共享用例；必填
  project/session_id/operation_id/reason/expected_revision/expected_epoch +
  confirm 精确=project；CAS/旧 holder 守卫；请求不得自带 trusted/attestation
  提权；read_only / CSRF / Origin 仍被拒。
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from oc_orch import composition  # noqa: E402
from oc_orch.ledger import Ledger  # noqa: E402
from oc_orch.web import WebServer  # noqa: E402


class _NoClient:
    """只读/控制路径绝不得惰性构造 Client；被构造即失败。"""

    def __init__(self):
        self.called = False

    def __call__(self):
        self.called = True
        raise AssertionError("health/incidents/pause-resume 不应构造 Client")


class ContinuityWebBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="continuity-web-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.conn = composition.init(self.tmp)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.tmp)
        self.lease = self.ledger.claim_coordinator(
            project="p", session_id="ses_coord", actor="test", ttl=3600)
        self.task = self.ledger.create_task(
            project="p", title="task", coordinator_session="ses_coord")
        self.client = _NoClient()
        self.server = WebServer(self.tmp, port=0, read_only=False)
        self.server.client_factory = self.client
        self.port = self.server.start()

    def tearDown(self):
        self.server.stop()

    def get(self, path):
        r = urllib.request.Request("http://127.0.0.1:%d%s" % (self.port, path))
        try:
            with urllib.request.urlopen(r, timeout=15) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def post(self, path, body, *, headers=None):
        h = {"Content-Type": "application/json", "X-OC-Orch-Web": "1"}
        h.update(headers or {})
        r = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path),
            data=json.dumps(body).encode(), method="POST", headers=h)
        try:
            with urllib.request.urlopen(r, timeout=15) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def _meta_cursor(self):
        conn = composition.connect(self.tmp)
        try:
            return conn.execute(
                "SELECT value FROM meta WHERE key='sweep-cursor:business-health-tasks'"
            ).fetchone()
        finally:
            conn.close()


class ContinuityWebHealthTests(ContinuityWebBase):
    def test_health_shape_process_and_runtime(self):
        status, data = self.get("/api/health")
        self.assertEqual(status, 200, data)
        self.assertEqual(data["schema"], "oc-orch.continuity-health.v1")
        self.assertEqual(data["semantics"], "process-health-not-business-acceptance")
        for key in ("healthy", "connected", "heartbeat_age_s", "last_cycle_at"):
            self.assertIn(key, data["process"])
        self.assertEqual(data["runtime"]["email_default_enabled"], False,
                         "运行时默认邮件关闭是发布事实，不自行声称启用")
        self.assertEqual(data["runtime"]["native_trusted_chat_enabled"], False)
        self.assertEqual([t["task_id"] for t in data["tasks"]], [self.task["id"]])
        c = data["tasks"][0]["control"]
        self.assertEqual(c["project_intent"], "running")
        self.assertEqual(c["holder_session"], "ses_coord")
        self.assertIsInstance(c["epoch"], int)
        self.assertEqual(c["project_revision"], 0)
        self.assertFalse(self.client.called, "健康 GET 不得构造 Client")

    def test_health_readonly_does_not_advance_cursor_or_write(self):
        conn = composition.connect(self.tmp)
        try:
            before = list(conn.iterdump())
        finally:
            conn.close()
        for _ in range(2):
            status, _ = self.get("/api/health")
            self.assertEqual(status, 200)
        conn = composition.connect(self.tmp)
        try:
            self.assertEqual(list(conn.iterdump()), before)
            self.assertIsNone(self._meta_cursor())
        finally:
            conn.close()

    def test_health_invalid_scope_and_limit_are_stable_400(self):
        for path, code in [
            ("/api/health?limit=0", "INVALID_LIMIT"),
            ("/api/health?limit=201", "INVALID_LIMIT"),
            ("/api/health?limit=abc", "INVALID_LIMIT"),
            ("/api/health?project=", "INVALID_SCOPE"),
            ("/api/health?session_id=", "INVALID_SCOPE"),
            ("/api/health?after_id=", "INVALID_SCOPE"),
        ]:
            with self.subTest(path=path):
                status, data = self.get(path)
                self.assertEqual(status, 400, (path, data))
                self.assertEqual(data["code"], code, path)

    def test_health_session_filter_and_pagination(self):
        for i in range(3):
            self.ledger.create_task(project="other", title=f"other{i}",
                                    task_id=f"T-0{i}", coordinator_session="ses_other")
        self.ledger.create_task(project="p", title="second",
                                task_id="T-second", coordinator_session="ses_coord")
        status, data = self.get("/api/health?session_id=ses_coord&limit=1")
        self.assertEqual(status, 200, data)
        self.assertEqual([t["task_id"] for t in data["tasks"]], [self.task["id"]])
        self.assertFalse(data["coverage"]["complete"])
        self.assertIsNotNone(data["coverage"]["next_after_id"])
        status2, data2 = self.get(
            "/api/health?session_id=ses_coord&limit=1&after_id=%s"
            % data["coverage"]["next_after_id"])
        self.assertEqual(status2, 200, data2)
        self.assertEqual(len(data2["tasks"]), 1)
        ids1 = {t["task_id"] for t in data["tasks"]}
        ids2 = {t["task_id"] for t in data2["tasks"]}
        self.assertTrue(ids1 and ids2 and not ids1 & ids2, "续页不得重复同任务")
        self.assertTrue(all(t["project"] == "p" for t in data2["tasks"]))

    def test_health_process_healthy_never_means_business_progress(self):
        status, data = self.get("/api/health")
        self.assertEqual(status, 200, data)
        item = next(t for t in data["tasks"] if t["task_id"] == self.task["id"])
        self.assertIn("process_healthy", item)
        self.assertNotEqual(self.ledger.get_task(self.task["id"])["status"],
                            "accepted")


class ContinuityWebIncidentsTests(ContinuityWebBase):
    def test_incidents_readonly_shape_and_no_mail(self):
        status, data = self.get("/api/incidents?limit=50")
        self.assertEqual(status, 200, data)
        self.assertTrue(data["ok"])
        self.assertIsInstance(data["incidents"], list)
        st = data["status"]
        self.assertIn("total", st)
        self.assertIn("by_state", st)
        self.assertIn("by_type", st)
        self.assertEqual(st["channel"], "not-configured")
        self.assertIs(st["email_configured"], False)
        self.assertIs(st["email_policy_enabled"], False)
        self.assertEqual(st["recipient_refs"], [])
        self.assertFalse(self.client.called)
        # 只读路径不得调用 reconcile / 发送
        conn = composition.connect(self.tmp)
        try:
            self.assertIsNone(conn.execute(
                "SELECT value FROM meta WHERE key='mail:last-instance'").fetchone())
        finally:
            conn.close()

    def test_incidents_do_not_secret_leak(self):
        # 通过责任台账登记一条等待人工 → incidents 只读列表不得含正文/证据
        from oc_orch.composition import make_responsibilities
        store = make_responsibilities(self.ledger)
        from oc_orch.util import now_ms as _now_ms
        store.open_responsibility(
            "operator", "p", self.task["id"], state="waiting_human",
            wait_kind="decision", wait_condition="explicit decision",
            next_action="confirm owner", next_check_at=_now_ms() + 30_000,
            deadline=_now_ms() + 60_000, max_budget=3,
            condition_key="decision", target_session="ses_coord",
            evidence={"body": "SECRET-EVIDENCE-BODY"})
        status, data = self.get("/api/incidents")
        self.assertEqual(status, 200, data)
        blob = json.dumps(data, ensure_ascii=False)
        self.assertNotIn("SECRET-EVIDENCE-BODY", blob)


class ContinuityWebControlTests(ContinuityWebBase):
    def _control_body(self, action, **overrides):
        body = {
            "project": "p", "session_id": "ses_coord",
            "operation_id": "test-op-%s" % action,
            "reason": "web 测试", "expected_revision": 0,
            "expected_epoch": self.lease["epoch"], "confirm": "p",
        }
        body.update(overrides)
        return body

    def test_pause_then_health_shows_paused_then_resume(self):
        status, data = self.post("/api/coordinator/pause",
                                 self._control_body("pause"))
        self.assertEqual(status, 200, data)
        self.assertTrue(data["ok"])
        self.assertIs(data["executed"], True)
        self.assertEqual(data["project"], "p")
        self.assertEqual(data["session_id"], "ses_coord")
        self.assertEqual(data["action"], "pause")

        _, health = self.get("/api/health")
        item = next(t for t in health["tasks"] if t["task_id"] == self.task["id"])
        paused_rev = item["control"]["project_revision"]
        self.assertEqual(item["control"]["project_intent"], "paused")
        self.assertGreaterEqual(paused_rev, 1)

        status, resume = self.post("/api/coordinator/resume", self._control_body(
            "resume", expected_revision=paused_rev))
        self.assertEqual(status, 200, resume)
        self.assertIs(resume["executed"], True)
        _, health2 = self.get("/api/health")
        item2 = next(t for t in health2["tasks"] if t["task_id"] == self.task["id"])
        self.assertEqual(item2["control"]["project_intent"], "running")
        self.assertFalse(self.client.called)

    def test_confirm_must_match_project_exactly(self):
        for confirm in ("/p", "P", ""):
            with self.subTest(confirm=confirm):
                body = self._control_body("pause")
                body["confirm"] = confirm
                status, data = self.post("/api/coordinator/pause", body)
                self.assertEqual(status, 400, (confirm, data))
                self.assertEqual(data["code"], "CONFIRM_REQUIRED")

    def test_missing_required_fields_are_400(self):
        cases = [
            {},  # 全缺
            {"project": "p", "session_id": "ses_coord", "operation_id": "o",
             "reason": "r", "expected_revision": 0, "expected_epoch": 1,
             "confirm": "p"},  # 全部满足 → 200；仅做对照
        ]
        fields = ["project", "session_id", "operation_id", "reason",
                  "expected_revision", "expected_epoch", "confirm"]
        for drop in fields:
            body = self._control_body("pause")
            body.pop(drop, None)
            status, data = self.post("/api/coordinator/pause", body)
            self.assertEqual(status, 400, (drop, data))
        self.assertEqual(cases[1]["confirm"], "p")

    def test_stale_cas_epoch_rejected(self):
        status, data = self.post("/api/coordinator/pause", self._control_body(
            "pause", expected_epoch=self.lease["epoch"] + 1))
        self.assertEqual(status, 400, data)
        self.assertEqual(data["code"], "control_refused")

    def test_non_holder_session_rejected(self):
        body = self._control_body("pause", session_id="ses_other",
                                  confirm="p")
        status, data = self.post("/api/coordinator/pause", body)
        self.assertEqual(status, 400, data)

    def test_request_cannot_carry_trusted_or_attestation_escalation(self):
        # 请求体内即使带 trusted/attestation/user 字段，也只能按固定本地 operator
        # 标签执行，不能自报真人/可信；成功结果与不带这些字段一致。
        body = self._control_body("pause")
        body["trusted"] = True
        body["attestation"] = "user-claimed"
        body["source"] = "user"
        status, data = self.post("/api/coordinator/pause", body)
        self.assertEqual(status, 200, data)
        self.assertIs(data["executed"], True)
        # 恢复请求同理；继续也正常工作
        _, health = self.get("/api/health")
        rev = next(t for t in health["tasks"]
                   if t["task_id"] == self.task["id"])["control"]["project_revision"]
        body2 = self._control_body("resume", expected_revision=rev)
        body2["trusted"] = False
        body2["attestation"] = "http-claimed"
        status2, data2 = self.post("/api/coordinator/resume", body2)
        self.assertEqual(status2, 200, data2)
        self.assertIs(data2["executed"], True)

    def test_read_only_blocks_pause(self):
        ro = WebServer(self.tmp, port=0, read_only=True)
        ro.client_factory = self.client
        port = ro.start()
        try:
            h = {"Content-Type": "application/json", "X-OC-Orch-Web": "1"}
            r = urllib.request.Request(
                "http://127.0.0.1:%d/api/coordinator/pause" % port,
                data=json.dumps(self._control_body("pause")).encode(),
                method="POST", headers=h)
            with self.assertRaises(urllib.error.HTTPError) as cm:
                urllib.request.urlopen(r, timeout=15)
            self.assertEqual(cm.exception.code, 403)
            self.assertEqual(json.loads(cm.exception.read().decode())["code"],
                             "READ_ONLY")
        finally:
            ro.stop()

    def test_csrf_missing_header_and_bad_origin_rejected(self):
        status, data = self.post("/api/coordinator/pause",
                                 self._control_body("pause"),
                                 headers={"X-OC-Orch-Web": "0"})
        self.assertEqual(status, 403, data)
        self.assertEqual(data["code"], "CSRF")
        status, data = self.post("/api/coordinator/pause",
                                 self._control_body("pause"),
                                 headers={"Origin": "https://evil.example"})
        self.assertEqual(status, 403, data)
        self.assertEqual(data["code"], "BAD_ORIGIN")
        # 拒绝后不应产生任何控制写入
        _, health = self.get("/api/health")
        item = next(t for t in health["tasks"] if t["task_id"] == self.task["id"])
        self.assertEqual(item["control"]["project_intent"], "running")


if __name__ == "__main__":
    unittest.main(verbosity=2)
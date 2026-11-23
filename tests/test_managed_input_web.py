#!/usr/bin/env python3
"""S4-D Web 只读诊断：/api/inputs、/api/input-watches 与页面受管输入/白名单表格。

只读约束：GET 只查本地台账（list_inputs/list_watches），绝不构造 Client、绝不
触发 observe/reconcile 等远端操作；页面不增加写按钮；所有远端值经 esc() 渲染。
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
from contextlib import contextmanager
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from oc_orch.db import init  # noqa: E402
from oc_orch.ledger import Ledger  # noqa: E402
from oc_orch.web import WebServer  # noqa: E402


class _FakeManagedInputs:
    """C 用例替身：只实现列表接口；故意带正文探测 Web 不泄露。"""

    def __init__(self):
        self.calls = []

    def list_inputs(self, **kw):
        self.calls.append(("list_inputs", kw))
        return {"ok": True, "inputs": [{
            "input_id": "I-w1", "session_id": "ses_web", "message_id": "msg_w",
            "intent": "execute", "resume": True, "owner": "ops", "revision": 2,
            "admission": "delivered", "delivery": "delivered",
            "execution": "succeeded", "business_ack": "acknowledged",
            "pending_age_ms": 0, "next_action": "close",
            "text": "web 列表不应返回正文",
        }]}

    def list_watches(self, **kw):
        self.calls.append(("list_watches", kw))
        return {"ok": True, "watches": [
            {"session_id": "ses_w", "owner": "ops", "project": "p",
             "max_pending_age_ms": 3600000, "enabled": True, "revision": 1,
             "pending_count": 1, "next_action": "alert",
             "last_observed_at": 1777000000000,
             "last_observation": {
                 "session_id": "ses_w", "owner": "ops", "project": "p",
                 "trusted": True, "pending": 1, "oldest_age_ms": 120000,
                 "aged_out": False, "blocking_reason": None,
                 "next_action": "等待：继续定点观察"}},
            {"session_id": "ses_w2", "owner": "ops", "project": "p2",
             "max_pending_age_ms": 60000, "enabled": False, "revision": 4,
             "last_observation": None},
        ]}


class _FakeResponsibilities:
    def __init__(self):
        self.calls = []

    def list_open_responsibilities(self, **kw):
        self.calls.append(("list", kw))
        return [{
            "id": "R-w1", "state": "awaiting_review", "task_id": "T-w",
            "attempt_id": "A-w", "generation": 2, "result_version": "rv-1",
            "owner": "coord", "target_session": "ses_coord",
            "wait_kind": "review", "wait_condition": "business-report-unknown",
            "next_action": "协调者核收", "next_check_at": 1777000000000,
            "deadline": 1777100000000, "budget_used": 1, "max_budget": 3,
            "revision": 4,
        }]


class _NoClient:
    """只读诊断绝不允许惰性构造 Client；被构造即失败。"""

    def __init__(self):
        self.called = False

    def __call__(self):
        self.called = True
        raise AssertionError("只读 GET 不应构造 Client")


class ManagedInputWebTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="managed-web-", dir="/tmp/opencode")
        self.addCleanup(lambda: os.path.isdir(self.tmp) and __import__(
            "shutil").rmtree(self.tmp, ignore_errors=True))
        self.conn = init(self.tmp)
        self.ledger = Ledger(self.conn, self.tmp)
        self.fake = _FakeManagedInputs()
        self.resp = _FakeResponsibilities()
        self.boom_client = _NoClient()

        @contextmanager
        def opened(home, *, client_factory=None):
            yield self.fake

        @contextmanager
        def opened_resp(home):
            yield self.resp

        self.opener = opened
        self.server = WebServer(self.tmp, port=0, read_only=True)
        self.server.client_factory = self.boom_client
        # 保持 patch 在整个测试生命周期有效：HTTP 处理线程在 req() 时才调用
        # web._open_managed_inputs，不能只在 start() 期间打补丁。
        self._patch = patch("oc_orch.entrypoints.web._open_managed_inputs",
                            self.opener)
        self._patch.start()
        self.addCleanup(self._patch.stop)
        self._patch_resp = patch("oc_orch.entrypoints.web._open_responsibilities",
                                 opened_resp)
        self._patch_resp.start()
        self.addCleanup(self._patch_resp.stop)
        self.port = self.server.start()

    def tearDown(self):
        self.server.stop()
        self.conn.close()

    def req(self, path, method="GET"):
        r = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path), method=method)
        try:
            with urllib.request.urlopen(r, timeout=10) as resp:
                return resp.status, resp.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    def test_api_inputs_read_only_no_client(self):
        status, body = self.req("/api/inputs?limit=5")
        self.assertEqual(status, 200, body)
        data = json.loads(body)
        self.assertTrue(data["ok"])
        self.assertEqual(data["inputs"][0]["input_id"], "I-w1")
        self.assertEqual(self.fake.calls,
                         [("list_inputs", {"session_id": None, "limit": 5})])
        self.assertFalse(self.boom_client.called,
                         "只读 GET 不得惰性构造 Client")

    def test_api_inputs_session_filter(self):
        status, body = self.req("/api/inputs?session=ses_web&limit=3")
        self.assertEqual(status, 200, body)
        self.assertEqual(self.fake.calls,
                         [("list_inputs", {"session_id": "ses_web", "limit": 3})])

    def test_api_inputs_limit_clamped(self):
        status, _ = self.req("/api/inputs?limit=99999")
        self.assertEqual(status, 200)
        self.assertEqual(self.fake.calls,
                         [("list_inputs", {"session_id": None, "limit": 200})])

    def test_api_watches_no_client_and_list_only(self):
        status, body = self.req("/api/input-watches")
        self.assertEqual(status, 200, body)
        data = json.loads(body)
        self.assertTrue(data["ok"])
        self.assertEqual(data["watches"][0]["project"], "p")
        self.assertIs(data["watches"][0]["enabled"], True)
        self.assertIs(data["watches"][1]["enabled"], False)
        self.assertEqual(data["watches"][1]["revision"], 4)
        self.assertEqual(data["watches"][0]["last_observation"]["pending"], 1)
        self.assertEqual(data["watches"][0]["last_observation"]["trusted"], True)
        self.assertIsNone(data["watches"][1]["last_observation"])
        self.assertEqual(self.fake.calls, [("list_watches", {"limit": 50})])
        self.assertFalse(self.boom_client.called)
        # 只读路径不得触发 observe/reconcile 等远端操作
        self.assertEqual([c[0] for c in self.fake.calls],
                         ["list_watches"])

    def test_api_responsibilities_read_only_no_client(self):
        status, body = self.req("/api/responsibilities?task=T-w&limit=5")
        self.assertEqual(status, 200, body)
        data = json.loads(body)
        self.assertTrue(data["ok"])
        self.assertEqual(data["responsibilities"][0]["id"], "R-w1")
        self.assertEqual(self.resp.calls,
                         [("list", {"task_id": "T-w", "owner": None, "limit": 5})])
        self.assertFalse(self.boom_client.called)

    def test_page_and_assets_contract_no_server_side_row_data(self):
        status, body = self.req("/")
        self.assertEqual(status, 200)
        self.assertIn("oc-orch 控制面", body)
        self.assertIn("/assets/app.js", body)
        self.assertIn("/assets/app.css", body)
        self.assertNotIn('src="http', body, "无外部脚本 CDN 引用")
        self.assertNotIn('href="http', body, "无外部样式 CDN 引用")
        status, js = self.req("/assets/app.js")
        self.assertEqual(status, 200, js)
        self.assertNotIn("unsafeHTML", js, "禁止 unsafeHTML 字符串式渲染")
        # 服务端页面/资源绝不内插列表正文或行数据（正文展示与转义证据在真实 DOM 测试）
        for needle in ("I-w1", "web 列表不应返回正文", "msg_w"):
            self.assertNotIn(needle, body, needle)
            self.assertNotIn(needle, js, needle)
        self.assertEqual(self.fake.calls, [], "页面/资源 GET 本身不触发服务读")

    def test_page_does_not_embed_api_body_or_inline_script(self):
        status, body = self.req("/")
        self.assertEqual(status, 200)
        self.assertNotIn("<script>", body, "外壳不应含无 src 的内联脚本")
        for needle in ("web 列表不应返回正文", "I-w1", "msg_w"):
            self.assertNotIn(needle, body, "服务端 HTML 不得内插列表正文/行数据")

class _WriteFake:
    def __init__(self):
        self.calls = []
        self._record = lambda m, **kw: self.calls.append((m, kw))
        self.four_state = {"admission": "prepared", "delivery": "pending",
                           "execution": "unknown", "business_ack": "unacknowledged"}

    def send(self, session_id, **kw):
        self._record("send", session_id=session_id, **kw)
        return {"ok": True, "input": {"input_id": "I-web1", "session_id": session_id,
                                      "message_id": "msg_w1",
                                      "tracking": "tracked" if kw.get("intent") == "execute" else "untracked",
                                      "task_id": kw.get("task_id"),
                                      "responsibility_id": "R-web", **self.four_state}}

    def flush(self, input_id, **kw):
        self._record("flush", input_id=input_id, **kw)
        return {"ok": True, "input": {"input_id": input_id, "admission": "cancelled"}}

    def cancel(self, input_id, **kw):
        self._record("cancel", input_id=input_id, **kw)
        return {"ok": True, "input": {"input_id": input_id, "admission": "cancelled"}}

    def acknowledge(self, input_id, **kw):
        self._record("acknowledge", input_id=input_id, **kw)
        return {"ok": True, "changed": bool(kw.get("value")),
                "input": {"input_id": input_id,
                          "business_ack": "acknowledged" if kw.get("value") else "unacknowledged"}}


class ManagedInputWebWriteContract(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="managed-web-write-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        init(self.tmp)
        self.fake = _WriteFake()

        @contextmanager
        def opened(home, *, client_factory=None):
            yield self.fake

        self.opener = opened
        self.server = WebServer(self.tmp, port=0, read_only=False)
        self._patch = patch("oc_orch.entrypoints.web._open_managed_inputs", opened)
        self._patch.start()
        self.addCleanup(self._patch.stop)
        self.port = self.server.start()

    def tearDown(self):
        self.server.stop()

    def post(self, path, body, *, headers=None):
        h = {"Content-Type": "application/json", "X-OC-Orch-Web": "1"}
        h.update(headers or {})
        r = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path),
            data=json.dumps(body).encode(), method="POST", headers=h)
        try:
            with urllib.request.urlopen(r, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def test_post_send_execute_passes_tracking_and_four_states(self):
        status, data = self.post("/api/input/send", {
            "session": "ses_w", "intent": "execute", "text": "go",
            "owner": "ops", "request_id": "r1", "project": "p",
            "task": "T-1", "attempt": "A-1", "target_session": "ses_c",
            "deadline": 1777000000123, "work_kind": "observe",
            "materials": [{"ref": "base"}]})
        self.assertEqual(status, 200, data)
        kws = self.fake.calls[0][1]
        self.assertEqual(kws["task_id"], "T-1")
        self.assertEqual(kws["attempt_id"], "A-1")
        self.assertEqual(kws["target_session"], "ses_c")
        self.assertEqual(kws["deadline"], 1777000000123)
        self.assertEqual(kws["work_kind"], "observe")
        inp = data["out"]["input"]
        for key in ("admission", "delivery", "execution", "business_ack"):
            self.assertIn(key, inp)

    def test_post_send_missing_text_or_owner_is_400_no_business_call(self):
        status, data = self.post("/api/input/send", {"session": "s", "intent": "execute"})
        self.assertEqual(status, 400)
        self.assertIn("text", data["message"])
        self.assertEqual(self.fake.calls, [])

    def test_post_flush_cancel_ack_share_methods(self):
        for path, body, method in [
            ("/api/input/I-1/flush",
             {"owner": "o", "confirm": "I-1", "expected_revision": 2, "reason": "c"},
             "flush"),
            ("/api/input/I-1/cancel",
             {"owner": "o", "confirm": "I-1", "expected_revision": 2, "reason": "c"},
             "cancel"),
            ("/api/input/I-1/ack",
             {"owner": "o", "confirm": "I-1", "expected_revision": 2, "reason": "ok", "value": True},
             "acknowledge")]:
            with self.subTest(path=path):
                status, data = self.post(path, body)
                self.assertEqual(status, 200, data)
                self.assertEqual(self.fake.calls[-1][0], method)

    def test_post_ack_requires_explicit_bool(self):
        status, data = self.post("/api/input/I-1/ack", {
            "owner": "o", "confirm": "I-1", "expected_revision": 1, "reason": "x"})
        self.assertEqual(status, 400)
        self.assertIn("bool", data["message"])
        self.assertEqual(self.fake.calls, [])

    def test_post_csrf_missing_header_rejected(self):
        status, data = self.post("/api/input/send", {
            "session": "s", "intent": "record", "text": "x",
            "owner": "o", "request_id": "r"}, headers={"X-OC-Orch-Web": "0"})
        self.assertEqual(status, 403)
        self.assertEqual(data["code"], "CSRF")
        self.assertEqual(self.fake.calls, [])

    def test_post_read_only_blocks_write(self):
        ro = WebServer(self.tmp, port=0, read_only=True)
        with patch("oc_orch.entrypoints.web._open_managed_inputs", self.opener):
            port = ro.start()
            try:
                h = {"Content-Type": "application/json", "X-OC-Orch-Web": "1"}
                r = urllib.request.Request(
                    "http://127.0.0.1:%d/api/input/send" % port,
                    data=b'{}', method="POST", headers=h)
                with urllib.request.urlopen(r, timeout=10) as resp:
                    self.fail("read_only 不应允许写")
            except urllib.error.HTTPError as e:
                self.assertEqual(e.code, 403)
                body = json.loads(e.read().decode())
                self.assertEqual(body["code"], "READ_ONLY")
            finally:
                ro.stop()
        self.assertEqual(self.fake.calls, [])


class ManagedInputWebRealLedgerContract(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="managed-web-real-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        init(self.tmp)
        from tests.fakes import FakeClient, FakeClock
        self._client_calls = 0
        self.client = FakeClient()
        self.clock = FakeClock()

        def factory():
            self._client_calls += 1
            return self.client

        self.factory = factory

        @contextmanager
        def opened(home, *, client_factory=None):
            conn = init(home)
            ledger = Ledger(conn, home, clock=self.clock)
            from oc_orch.composition import make_managed_inputs, make_responsibilities
            try:
                yield make_managed_inputs(
                    conn, home, client=self.factory, clock=self.clock,
                    responsibilities=make_responsibilities(ledger))
            finally:
                conn.close()

        self.server = WebServer(self.tmp, port=0, read_only=False)
        self.server.client_factory = self.factory
        self._patch = patch("oc_orch.entrypoints.web._open_managed_inputs", opened)
        self._patch.start()
        self.addCleanup(self._patch.stop)
        self.port = self.server.start()
        conn = init(self.tmp)
        try:
            self.ledger = Ledger(conn, self.tmp, clock=self.clock)
            self.task = self.ledger.create_task(
                project="p", title="managed", coordinator_session="ses_c",
                write_scope=[self.tmp])
            self.attempt = self.ledger.create_attempt(
                self.task["id"], directory=self.tmp, session_id="ses_w",
                status="running", dispatch_state="confirmed")
        finally:
            conn.close()
        self.client.set_session("ses_w")

    def tearDown(self):
        self.server.stop()

    def post(self, path, body):
        h = {"Content-Type": "application/json", "X-OC-Orch-Web": "1"}
        r = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path),
            data=json.dumps(body).encode(), method="POST", headers=h)
        try:
            with urllib.request.urlopen(r, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def test_record_does_not_construct_client(self):
        status, data = self.post("/api/input/send", {
            "session": "ses_w", "intent": "record", "text": "log",
            "owner": "ops", "request_id": "r-rec"})
        self.assertEqual(status, 200, data)
        self.assertEqual(data["out"]["input"]["intent"], "record")
        self.assertEqual(self._client_calls, 0, "record 绝不得构造 Client")

    def test_park_requires_flush_condition_and_expires_at(self):
        status, data = self.post("/api/input/send", {
            "session": "ses_w", "intent": "park", "text": "hold",
            "owner": "ops", "request_id": "r-park"})
        self.assertEqual(status, 400)

    def test_execute_default_tracked_and_sealed_write_rejects(self):
        status, data = self.post("/api/input/send", {
            "session": "ses_w", "intent": "execute", "text": "continue",
            "owner": "ops", "request_id": "r-ex", "task": self.task["id"],
            "work_kind": "write", "target_session": "ses_c"})
        self.assertEqual(status, 200, data)
        self.assertEqual(data["out"]["input"]["tracking"], "tracked")
        conn = init(self.tmp)
        try:
            Ledger(conn, self.tmp, clock=self.clock).conn.execute(
                "UPDATE attempts SET status='terminal_succeeded', "
                "site_released_at=1 WHERE id=?", (self.attempt["id"],))
        finally:
            conn.close()
        status, data = self.post("/api/input/send", {
            "session": "ses_w", "intent": "execute", "text": "fix",
            "owner": "ops", "request_id": "r-write", "task": self.task["id"],
            "work_kind": "write", "target_session": "ses_c"})
        self.assertEqual(status, 400)
        self.assertIn("rework", data["message"])

    def test_foreign_queue_blocks_execute_send(self):
        self.client.set_inbox("ses_w", [{"id": "foreign-1", "text": "other work"}])
        status, data = self.post("/api/input/send", {
            "session": "ses_w", "intent": "execute", "text": "go",
            "owner": "ops", "request_id": "r-foreign", "task": self.task["id"],
            "work_kind": "observe", "target_session": "ses_c"})
        self.assertEqual(status, 400)
        self.assertIn("queue", data["message"])


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""OpenCode V2 HTTP/SSE 适配器契约测试（离线；loopback 假 HTTP/受控传输）。

对应 OpenSpec modularize-orchestrator 任务 5.1。契约维度：

- 超时：HTTP 读超时 → TransportError；SSE 读超时 → StreamError("read-timeout")
- EOF：HTTP 截断体（IncompleteRead）→ TransportError；SSE 直接 EOF → StreamError("EOF")
- 错误体：HTTP 错误 → ApiError（status/path/_tag/message/body/code）；
  非 JSON 错误体回退 raw 文本（tag=None → code=HTTP_<status>）；404 语义（get_session → None）
- 401 重新发现：GET 401 → reload_service() 后 raise ApiError（不重试）；
  POST 401 → 明确拒绝（不 reload、不重试）；非 401（503 等）→ 不 reload
- 帧解析：多行 data、空行、: 注释、event:/id: 忽略、CRLF、空 data 帧、
  坏 JSON 帧跳过、字符串型 JSON、非 dict JSON（列表/标量丢弃）
- POST 不盲重试：传输错 1 次即抛 TransportError，绝不自动重放（401 已明确拒绝 ↔ 超时未知 均不重试）
- GET 重试约束：传输错严格单次重试（reload 后），不无限重试
- V2 消息契约：get_message 404→None、HTTP 错误→ApiError、缺字段/坏形状→
  MessageProtocolError（不静默降级）；message_page 游标翻页 cursor 与
  order 互斥、空页/终页为有效语义、重复 cursor 原样返回（消费者负责发现）

固定分类：ApiError/TransportError/StreamError（定义迁移至 domain.errors，client.py 兼容导出）。
有意语义修正（与旧 urllib 行为差异，逐项单列于文档/测试）：
  SC-A 截断响应体 IncompleteRead/BadStatusLine 也归并为 TransportError（原先裸抛 http.client 异常）。
  SC-B GET 重试的第二次尝试若得到明确 HTTP 错误，以 ApiError 为准（原先被吞成 TransportError）。
"""

from __future__ import annotations

import http.server
import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))
FIXTURE_ROOT = os.path.join(HERE, "fixtures")

from oc_orch.client import (  # noqa: E402
    ApiError,
    Client,
    MessageProtocolError,
    OrchError,
    Service,
    StreamError,
    TransportError,
)


# ---------------------------------------------------------------- 假 HTTP 服务

class _Handler(http.server.BaseHTTPRequestHandler):
    """每个请求消费一个脚本步（step）；无步时用 default 步。"""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # 静默，避免测试噪音
        pass

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_DELETE(self):
        self._handle("DELETE")

    def _handle(self, method: str) -> None:
        srv = self.server
        with srv.lock:
            srv.seen.append((method, self.path))
            step = srv.steps.pop(0) if srv.steps else dict(srv.default)
        clen = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(clen) if clen else b""
        with srv.lock:
            srv.bodies.append(body)

        if step.get("close"):  # 接受连接后直接断开：无响应 → RemoteDisconnected/连接重置
            self.close_connection = True
            self.connection.close()
            return

        status = step.get("status", 200)
        payload = step.get("payload", b"")
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        self.send_response_only(status, step.get("reason", ""))
        for k, v in step.get("headers", {}).items():
            self.send_header(k, v)
        if step.get("hang_headers"):  # 只发部分内容后悬挂 → 客户端读超时
            self.send_header("Content-Length", str(len(payload) + 1024))
            self.end_headers()
            if payload:
                self.wfile.write(payload)
                self.wfile.flush()
            time.sleep(step["hang_headers"])
            return
        declared = step.get("content_length", len(payload))
        self.send_header("Content-Length", str(declared))
        if step.get("finish"):
            self.send_header("Connection", "close")
        self.end_headers()
        if payload:
            self.wfile.write(payload)
            self.wfile.flush()
        if step.get("finish"):
            self.close_connection = True


class ScriptedServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, steps=None, default=None):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.lock = threading.Lock()
        self.steps = list(steps or [])
        self.default = dict(default or {})
        self.seen = []  # [(method, path), ...]
        self.bodies = []
        self._serve_thread = threading.Thread(target=self.serve_forever,
                                              name="oc-orch-fake-srv", daemon=True)
        self._serve_thread.start()

    @property
    def port(self) -> int:
        return self.server_address[1]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def service(self, password: str = "pw") -> Service:
        return Service(base=self.url, host="127.0.0.1", port=self.port,
                       password=password)

    def count(self, method: str | None = None, path_contains: str | None = None) -> int:
        total = 0
        for m, p in self.seen:
            if method is not None and m != method:
                continue
            if path_contains is not None and path_contains not in p:
                continue
            total += 1
        return total


# ---------------------------------------------------------------- 基座

class _ContractBase(unittest.TestCase):
    """每个测试：独立临时 service.json + 一个或多个 loopback 假服务。"""

    def setUp(self):
        self._saved_env = os.environ.copy()
        self._tmp = tempfile.mkdtemp(prefix="oc-orch-contract-")
        self.cfg_path = os.path.join(self._tmp, "service.json")
        self._servers: list[ScriptedServer] = []
        self._dead: list[socket.socket] = []
        os.environ["OC_ORCH_SERVICE_CONFIG"] = self.cfg_path

    def tearDown(self):
        for srv in self._servers:
            try:
                srv.shutdown()
            except Exception:
                pass
            try:
                srv.server_close()
            except Exception:
                pass
        for s in self._dead:
            try:
                s.close()
            except OSError:
                pass
        shutil.rmtree(self._tmp, ignore_errors=True)
        os.environ.clear()
        os.environ.update(self._saved_env)

    # ---- 构造辅助 ----
    def server(self, steps=None, default=None) -> ScriptedServer:
        srv = ScriptedServer(steps=steps, default=default)
        self._servers.append(srv)
        return srv

    def write_config(self, port: int, password: str = "pw") -> None:
        cfg = {"url": f"http://127.0.0.1:{port}", "hostname": "127.0.0.1",
               "port": str(port), "password": password}
        with open(self.cfg_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f)
        os.chmod(self.cfg_path, 0o600)

    def client_to(self, port: int, timeout: float = 0.15) -> Client:
        self.write_config(port)
        return Client(timeout=timeout)

    def dead_port(self) -> int:
        """绑定但未 listen 的端口：连接必被拒（与 check_offline 的占位技巧一致）。"""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        self._dead.append(s)
        return s.getsockname()[1]

    def load_bytes(self, *parts) -> bytes:
        with open(os.path.join(FIXTURE_ROOT, *parts), "rb") as f:
            return f.read()

    # ---- 断言辅助 ----
    def assert_api_error(self, cm, status, path, tag=None, code_fallback=None):
        err = cm.exception
        self.assertIsInstance(err, ApiError)
        self.assertEqual(err.status, status)
        self.assertEqual(err.path, path)
        self.assertEqual(err.tag, tag)
        if tag is not None:
            self.assertEqual(err.code, tag)
        elif code_fallback is not None:
            self.assertEqual(err.code, code_fallback)
        return err


# ---------------------------------------------------------------- HTTP 契约

class TestHttpContract(_ContractBase):
    """5.1 HTTP 维度：成功/空/文本、错误体分类、超时、EOF、401 重新发现、重试约束。"""

    # ---- 成功与解码 ----
    def test_non_object_error_json_keeps_api_classification(self):
        for payload in ('[]', 'null', '"denied"', '{"message":{"reason":"denied"}}'):
            with self.subTest(payload=payload):
                srv = self.server([{"status": 403, "payload": payload}])
                with self.assertRaises(ApiError) as raised:
                    self.client_to(srv.port)._post("/prompt", {})
                self.assertEqual(raised.exception.status, 403)
                self.assertIsInstance(raised.exception.message, str)
                self.assertEqual(srv.count(), 1)

    def test_truncated_error_body_keeps_known_status(self):
        srv = self.server([{"status": 403, "payload": "denied", "content_length": 100,
                            "finish": True}])
        with self.assertRaises(ApiError) as raised:
            self.client_to(srv.port)._post("/prompt", {})
        self.assertEqual(raised.exception.status, 403)
        self.assertEqual(raised.exception.body, "denied")
        self.assertEqual(srv.count(), 1)

    def test_401_reload_failure_keeps_auth_status(self):
        srv = self.server([{"status": 401, "payload": '{"message":"denied"}'}])
        client = self.client_to(srv.port)
        os.unlink(self.cfg_path)
        with self.assertRaises(ApiError) as raised:
            client._get("/api/info")
        self.assertEqual(raised.exception.status, 401)
        self.assertEqual(srv.count(), 1)

    def test_post_cannot_opt_into_transport_retry(self):
        srv = self.server([{"close": True}, {"payload": '{"unexpected":"retry"}'}])
        client = self.client_to(srv.port)
        with self.assertRaises(TransportError):
            client._request("POST", "/prompt", {}, retry=True)
        self.assertEqual(srv.count(), 1)

    def test_get_json_success(self):
        srv = self.server([{"payload": '{"data":{"ok":true}}'}])
        client = self.client_to(srv.port)
        out = client._get("/api/info")
        self.assertEqual(out, {"data": {"ok": True}})
        self.assertEqual(srv.count("GET"), 1)

    def test_empty_200_returns_none(self):
        srv = self.server([{"payload": ""}])
        client = self.client_to(srv.port)
        self.assertIsNone(client._get("/api/info"))

    def test_non_json_200_returns_text(self):
        srv = self.server([{"payload": "plain-text"}])
        client = self.client_to(srv.port)
        self.assertEqual(client._get("/api/info"), "plain-text")

    def test_get_session_404_returns_none(self):
        srv = self.server([{"status": 404, "payload": self.load_bytes(
            "http", "error_plain.txt")}])
        client = self.client_to(srv.port)
        self.assertIsNone(client.get_session("ses_missing"))
        self.assertEqual(srv.count("GET"), 1)

    def test_delete_no_retry(self):
        srv = self.server([{"close": True}])
        client = self.client_to(srv.port)
        with self.assertRaises(TransportError):
            client._request("DELETE", "/api/session/ses_x", retry=False)
        self.assertEqual(srv.count(), 1)

    # ---- 409 冲突分类（既有 classify_http_error 依赖）----
    def test_409_conflict_is_api_error(self):
        srv = self.server([{"status": 409, "payload": json.dumps(
            {"_tag": "ConflictError", "message": "session busy"}).encode()}])
        client = self.client_to(srv.port)
        with self.assertRaises(ApiError) as cm:
            client._get("/api/session/ses_x")
        err = self.assert_api_error(cm, 409, "/api/session/ses_x", tag="ConflictError")
        self.assertIn("session busy", err.message)

    # ---- 错误体分类（fixture 脱敏样本）----
    def test_json_error_body_parses_tag_and_message(self):
        srv = self.server([{"status": 401, "payload": self.load_bytes(
            "http", "error_unauthorized.json")}])
        client = self.client_to(srv.port)
        with self.assertRaises(ApiError) as cm:
            client._get("/api/info")
        err = self.assert_api_error(cm, 401, "/api/info", tag="UnauthorizedError")
        self.assertIn("authentication failed", err.message)
        self.assertIn('"message"', err.body)

    def test_forbidden_error_fixture(self):
        srv = self.server([{"status": 403, "payload": self.load_bytes(
            "http", "error_forbidden.json")}])
        client = self.client_to(srv.port)
        with self.assertRaises(ApiError) as cm:
            client._get("/api/info")
        self.assert_api_error(cm, 403, "/api/info", tag="ForbiddenError")

    def test_rate_limited_error_fixture(self):
        srv = self.server([{"status": 429, "payload": self.load_bytes(
            "http", "error_rate_limited.json")}])
        client = self.client_to(srv.port)
        with self.assertRaises(ApiError) as cm:
            client._get("/api/info")
        self.assert_api_error(cm, 429, "/api/info", tag="RateLimitError")

    def test_plain_error_body_fallback(self):
        srv = self.server([{"status": 500, "payload": self.load_bytes(
            "http", "error_plain.txt")}])
        client = self.client_to(srv.port)
        with self.assertRaises(ApiError) as cm:
            client._get("/api/info")
        err = self.assert_api_error(cm, 500, "/api/info", code_fallback="HTTP_500")
        self.assertTrue(err.message.startswith("<!"))
        self.assertEqual(err.message, err.body[:500])

    def test_error_taxonomy_classes(self):
        self.assertTrue(issubclass(ApiError, OrchError))
        self.assertTrue(issubclass(TransportError, OrchError))
        self.assertTrue(issubclass(StreamError, OrchError))
        self.assertEqual(TransportError("/p", "x").code, "TRANSPORT")
        self.assertEqual(StreamError("EOF").code, "STREAM")

    # ---- 超时（读超时 → TransportError）----
    def test_read_timeout_post_not_retried(self):
        srv = self.server([{"status": 200, "payload": '{"ok":',
                            "hang_headers": 0.35}])
        client = self.client_to(srv.port, timeout=0.1)
        with self.assertRaises(TransportError):
            client._post("/api/session/ses_x/prompt", {"id": "msg_1", "text": "x"})
        self.assertEqual(srv.count("POST"), 1)
        self.assertEqual(client.service.port, srv.port)

    def test_read_timeout_get_single_retry(self):
        srv = self.server([
            {"status": 200, "payload": '{"ok":', "hang_headers": 0.35},
            {"status": 200, "payload": '{"ok":', "hang_headers": 0.35},
        ])
        client = self.client_to(srv.port, timeout=0.1)
        with self.assertRaises(TransportError) as cm:
            client._get("/api/info")
        self.assertIn("/api/info", str(cm.exception))
        self.assertEqual(srv.count("GET"), 2)

    def test_connection_refused_get_transport(self):
        port = self.dead_port()
        client = self.client_to(port)
        with self.assertRaises(TransportError) as cm:
            client._get("/api/info")
        self.assertIn("ConnectionRefusedError", repr(cm.exception.message))

    # ---- EOF：截断响应体（SC-A 修正项）----
    def test_truncated_body_get_is_transport_error(self):
        # SC-A：Content-Length 声明大于实际送达并关闭 → http.client.IncompleteRead，
        # 旧实现裸抛（不在 OrchError 分类内）；契约要求归并为 TransportError。
        srv = self.server([
            {"status": 200, "payload": b'{"ok":', "content_length": 100,
             "finish": True},
            {"status": 200, "payload": b'{"ok":', "content_length": 100,
             "finish": True},
        ])
        client = self.client_to(srv.port)
        with self.assertRaises(TransportError):
            client._get("/api/info")
        self.assertEqual(srv.count("GET"), 2)

    def test_truncated_body_post_is_transport_error(self):
        # SC-A：POST 不重试；截断体只出现一次即抛 TransportError。
        srv = self.server([
            {"status": 200, "payload": b'{"ok":', "content_length": 100,
             "finish": True},
        ])
        client = self.client_to(srv.port)
        with self.assertRaises(TransportError):
            client._post("/api/session/ses_x/prompt", {"id": "msg_1", "text": "x"})
        self.assertEqual(srv.count("POST"), 1)

    # ---- GET 重试约束 ----
    def test_get_transport_single_retry_after_reload(self):
        srv = self.server([
            {"close": True},
            {"payload": '{"data":{"ok":true}}'},
        ])
        client = self.client_to(srv.port)
        out = client._get("/api/info")
        self.assertEqual(out, {"data": {"ok": True}})
        self.assertEqual(srv.count("GET"), 2)  # 恰好一次 reload 后重试，不无限重试

    def test_post_transport_never_retried(self):
        srv = self.server([{"close": True}])
        client = self.client_to(srv.port)
        with self.assertRaises(TransportError):
            client._post("/api/session/ses_x/prompt", {"id": "msg_1", "text": "x"})
        self.assertEqual(srv.count("POST"), 1)  # 不自动重放：派单结果未知，保留原 body 由调用方对账

    def test_get_retry_second_attempt_api_error_wins(self):
        # SC-B：第二次尝试到达服务并得到明确 HTTP 错误 → 报 ApiError（原实现吞成 TransportError）。
        srv = self.server([
            {"close": True},
            {"status": 503, "payload": self.load_bytes("http", "error_plain.txt")},
        ])
        client = self.client_to(srv.port)
        with self.assertRaises(ApiError) as cm:
            client._get("/api/info")
        self.assert_api_error(cm, 503, "/api/info", code_fallback="HTTP_503")
        self.assertEqual(srv.count("GET"), 2)

    # ---- 401 重新发现 ----
    def test_get_401_reload_config_and_raise_without_retry(self):
        srv_a = self.server([{"status": 401, "payload": self.load_bytes(
            "http", "error_unauthorized.json")}])
        srv_b = self.server([{"payload": '{"data":{"ok":true,"endpoint":"B"}}'}])
        client = self.client_to(srv_a.port)
        self.write_config(srv_b.port)  # 服务配置已切换（模拟服务重配）
        with self.assertRaises(ApiError) as cm:
            client._get("/api/info")
        self.assert_api_error(cm, 401, "/api/info", tag="UnauthorizedError")
        self.assertEqual(srv_a.count("GET"), 1)      # 401 不重试（明确拒绝）
        self.assertEqual(srv_b.count(), 0)           # 401 路径下不重放
        # reload 后 client 已指向新端点；下一请求走 B 并成功
        self.assertEqual(client.service.port, srv_b.port)
        out = client._get("/api/info")
        self.assertEqual(out["data"]["endpoint"], "B")
        self.assertEqual(srv_b.count("GET"), 1)

    def test_post_401_rejected_no_reload_no_retry(self):
        srv = self.server([{"status": 401, "payload": self.load_bytes(
            "http", "error_unauthorized.json")}])
        client = self.client_to(srv.port)
        with self.assertRaises(ApiError) as cm:
            client._post("/api/session/ses_x/prompt", {"id": "msg_1", "text": "x"})
        self.assert_api_error(cm, 401, "/api/session/ses_x/prompt",
                              tag="UnauthorizedError")
        self.assertEqual(srv.count("POST"), 1)
        # POST 401 = 明确拒绝：不 reload（服务端点仍为 A）、不重试
        self.assertEqual(client.service.port, srv.port)

    def test_non_401_does_not_reload(self):
        srv = self.server([{"status": 503, "payload": self.load_bytes(
            "http", "error_plain.txt")}])
        client = self.client_to(srv.port)
        with self.assertRaises(ApiError) as cm:
            client._get("/api/info")
        self.assert_api_error(cm, 503, "/api/info", code_fallback="HTTP_503")
        self.assertEqual(client.service.port, srv.port)  # 未 reload、未换端点


# ---------------------------------------------------------------- V2 消息契约

class TestMessageHttpContract(_ContractBase):
    """get_message / message_page 的 V2 固封形状契约（OpenCode 2.2.5 消息 schema）。

    - 坏响应 vs 真实不存在：HTTP 404 → None（唯一合法“不存在”）；其他 HTTP
      错误 → ApiError 原样抛出；响应体/字段违反固封 schema → MessageProtocolError
      （code=MESSAGE_PROTOCOL，窄 reason），三者可区分、均不静默降级。
    - 游标查询参数：cursor 有值（非空字符串）绝不传 order；无/空 cursor 传
      order；均携带 limit。
    - 空页（data=[] 与 next=null）是有效语义，由调用方结合游标判断终止；
      重复 cursor 的发现属消费者责任（适配器原样返回）。
    """

    def _message_step(self, *fixture, status: int = 200, payload=None):
        if payload is None:
            payload = self.load_bytes("http", *fixture)
        return {"status": status, "payload": payload}

    # ---- get_message ----
    def test_get_message_valid_user_sample(self):
        srv = self.server([self._message_step("get_message_user.json")])
        client = self.client_to(srv.port)
        msg = client.get_message("ses_a", "msg_user_1")
        self.assertEqual(msg["id"], "msg_user_1")
        self.assertEqual(msg["type"], "user")
        self.assertEqual(msg["text"], "完成测试任务：产出 report.md 并按原格式回报。")
        self.assertEqual(msg["time"]["created"], 1794008072447)
        self.assertIn("msg_user_1", srv.seen[0][1])

    def test_get_message_404_returns_none(self):
        srv = self.server([self._message_step("get_message_404.json", status=404)])
        client = self.client_to(srv.port)
        self.assertIsNone(client.get_message("ses_a", "msg_missing"))
        self.assertEqual(srv.count("GET"), 1)

    def test_get_message_http_error_is_api_error_not_none(self):
        # 500 错误体 ≠ 消息不存在：必须抛 ApiError，不得吞成 None。
        srv = self.server([self._message_step("error_plain.txt", status=500)])
        client = self.client_to(srv.port)
        with self.assertRaises(ApiError) as cm:
            client.get_message("ses_a", "msg_x")
        self.assertEqual(cm.exception.status, 500)

    def test_get_message_missing_data_is_protocol_error(self):
        srv = self.server([self._message_step("get_message_missing_data.json")])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.get_message("ses_a", "msg_x")
        self.assertEqual(cm.exception.reason, "data_missing")
        self.assertEqual(cm.exception.code, "MESSAGE_PROTOCOL")

    def test_get_message_empty_body_is_protocol_error(self):
        # 200 空响应体：与 404（真实不存在）必须可区分。
        srv = self.server([{"payload": ""}])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.get_message("ses_a", "msg_x")
        self.assertEqual(cm.exception.reason, "empty_body")

    def test_get_message_non_object_data_is_protocol_error(self):
        srv = self.server([{"payload": '{"data": ["not", "an", "object"]}'}])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.get_message("ses_a", "msg_x")
        self.assertEqual(cm.exception.reason, "data_wrong_type")

    # ---- message_page：有效形状 ----
    def test_message_page_valid_first_page(self):
        srv = self.server([self._message_step("message_page_valid.json")])
        client = self.client_to(srv.port)
        page = client.message_page("ses_a", limit=50, order="asc")
        self.assertEqual([m["id"] for m in page["data"]],
                         ["msg_user_1", "msg_idle_1"])
        self.assertEqual(page["data"][1]["type"], "idle")
        self.assertEqual(page["data"][1]["outcome"], "succeeded")
        self.assertEqual(page["cursor"]["next"], "cur_next_page_1")
        self.assertIsNone(page["cursor"]["previous"])
        # 无 cursor 的首页：传 order=asc，不携带 cursor
        path = srv.seen[0][1]
        self.assertIn("order=asc", path)
        self.assertIn("limit=", path)
        self.assertNotIn("cursor=", path)

    def test_message_page_desc_first_page_params(self):
        srv = self.server([self._message_step("message_page_valid.json")])
        client = self.client_to(srv.port)
        client.message_page("ses_a", limit=5, order="desc")
        path = srv.seen[0][1]
        self.assertIn("order=desc", path)
        self.assertNotIn("cursor=", path)

    def test_message_page_valid_empty_page(self):
        # 空页是有效语义：正常返回，不抛协议错误（终止判断在消费侧）。
        srv = self.server([self._message_step("message_page_empty.json")])
        client = self.client_to(srv.port)
        page = client.message_page("ses_a", cursor="cur_next_page_1")
        self.assertEqual(page["data"], [])
        self.assertEqual(page["cursor"]["previous"], "cur_next_page_1")
        self.assertIsNone(page["cursor"]["next"])

    def test_message_page_cursor_excludes_order_param(self):
        # 服务契约：沿 cursor 翻页绝不组合 order。
        srv = self.server([self._message_step("message_page_valid.json")])
        client = self.client_to(srv.port)
        client.message_page("ses_a", cursor="cur_next_page_1")
        path = srv.seen[0][1]
        self.assertIn("cursor=cur_next_page_1", path)
        self.assertNotIn("order=", path)
        self.assertIn("limit=", path)

    def test_message_page_empty_string_cursor_follows_order(self):
        # 空字符串游标视为无游标：按 order 请求首页（不带 cursor 参数）。
        srv = self.server([self._message_step("message_page_valid.json")])
        client = self.client_to(srv.port)
        client.message_page("ses_a", order="desc", cursor="")
        path = srv.seen[0][1]
        self.assertIn("order=desc", path)
        self.assertNotIn("cursor=", path)

    def test_message_page_repeated_cursor_returned_verbatim(self):
        # 服务重复返回同一 cursor：适配器原样返回两页，不做消重/静默终止；
        # 重复 cursor 的有界发现和终止判定属消费者责任（对接 read_message_window）。
        step = self._message_step("message_page_valid.json")
        srv = self.server([step, dict(step)])
        client = self.client_to(srv.port)
        p1 = client.message_page("ses_a", cursor="cur_next_page_1")
        p2 = client.message_page("ses_a", cursor="cur_next_page_1")
        self.assertEqual(p1, p2)
        both = [p for p in (p1, p2) if p["cursor"]["next"] == "cur_next_page_1"]
        self.assertEqual(len(both), 2)
        self.assertEqual(srv.count("GET"), 2)

    # ---- message_page：协议违规（P2；不静默降级为空页/完整覆盖）----
    def test_message_page_missing_cursor_is_protocol_error(self):
        srv = self.server([self._message_step("message_page_cursor_missing.json")])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_a", order="asc")
        self.assertEqual(cm.exception.reason, "cursor_missing")

    def test_message_page_data_wrong_type_is_protocol_error(self):
        srv = self.server([self._message_step("message_page_data_wrong_type.json")])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_a", order="asc")
        self.assertEqual(cm.exception.reason, "data_wrong_type")

    def test_message_page_cursor_wrong_type_is_protocol_error(self):
        srv = self.server([self._message_step("message_page_cursor_wrong_type.json")])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_a", order="asc")
        self.assertEqual(cm.exception.reason, "cursor_wrong_type")

    def test_message_page_body_not_object_is_protocol_error(self):
        srv = self.server([self._message_step("message_page_body_not_object.txt")])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_a", order="asc")
        self.assertEqual(cm.exception.reason, "body_not_object")

    def test_message_page_empty_body_is_protocol_error(self):
        srv = self.server([{"payload": ""}])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_a", order="asc")
        self.assertEqual(cm.exception.reason, "empty_body")

    def test_message_page_cursor_field_wrong_type_is_protocol_error(self):
        # cursor.next 非字符串/非 null（schema：anyOf string|null）。
        srv = self.server([{"payload":
                            '{"data": [], "cursor": {"next": 5, "previous": null}}'}])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_a", order="asc")
        self.assertEqual(cm.exception.reason, "cursor_field_wrong_type")
        self.assertEqual(cm.exception.detail["field"], "next")

    def test_message_page_data_item_not_object_is_protocol_error(self):
        srv = self.server([{"payload":
                            '{"data": [{"id": "m1"}, "oops"], '
                            '"cursor": {"previous": null, "next": null}}'}])
        client = self.client_to(srv.port)
        with self.assertRaises(MessageProtocolError) as cm:
            client.message_page("ses_a", order="asc")
        self.assertEqual(cm.exception.reason, "data_item_not_object")

    def test_message_protocol_error_taxonomy(self):
        # 协议错误是 OrchError 的独立窄分类，与 ApiError/TransportError 可区分。
        self.assertTrue(issubclass(MessageProtocolError, OrchError))
        err = MessageProtocolError("cursor_missing", "/api/session/s/message")
        self.assertEqual(err.code, "MESSAGE_PROTOCOL")
        self.assertEqual(err.reason, "cursor_missing")
        self.assertEqual(err.path, "/api/session/s/message")
        self.assertIn("cursor_missing", str(err))
        self.assertNotIsInstance(err, ApiError)
        self.assertNotIsInstance(err, TransportError)
        try:
            raise ApiError(404, "/p", None, "x")
        except ApiError as e:
            self.assertNotIsInstance(e, MessageProtocolError)


# ---------------------------------------------------------------- SSE 契约

class TestSseContract(_ContractBase):
    """5.1 SSE 维度：帧解析（多行/空行/注释/CRLF/空 data/坏 JSON/字符串型/非 dict）、
    超时、EOF、HTTP 状态、连接拒绝。"""

    def _stream_step(self, payload: bytes, hang: float | None = None):
        step = {"status": 200,
                "headers": {"Content-Type": "text/event-stream",
                            "Cache-Control": "no-cache"}}
        if hang is not None:
            step["payload"] = payload
            step["hang_headers"] = hang
        else:
            step["payload"] = payload
            step["content_length"] = len(payload)
            step["finish"] = True
        return step

    def _collect(self, payload, *, socket_timeout=0.25, hang=None):
        step = self._stream_step(payload, hang=hang)
        srv = self.server([step])
        client = self.client_to(srv.port)
        events = []
        with self.assertRaises(StreamError) as cm:
            for ev in client.iter_events(["session.execution.succeeded"],
                                         socket_timeout=socket_timeout):
                events.append(ev)
        return events, cm.exception, srv

    def test_happy_path_fixture_frames(self):
        payload = self.load_bytes("sse", "connected_and_events.stream")
        events, err, srv = self._collect(payload)  # noqa: F841 (err below)
        self.assertIn("EOF", str(err))
        self.assertEqual(
            [e.get("type") for e in events],
            ["server.connected", "session.execution.started",
             "session.tool.called", "session.text.delta", "permission.asked",
             "session.execution.succeeded", "session.execution.failed"],
        )
        self.assertEqual(srv.count("GET"), 1)

    def test_multiline_data_joined(self):
        payload = self.load_bytes("sse", "connected_and_events.stream")
        events, err, _ = self._collect(payload)
        started = [e for e in events if e["type"] == "session.execution.started"][0]
        self.assertEqual(started["data"]["sessionID"], "ses_<<REDACTED_SESSION>>")

    def test_comments_and_event_id_lines_ignored(self):
        # 注释行帧、event:/id: 字段、多空行均不产出事件
        payload = self.load_bytes("sse", "connected_and_events.stream")
        events, _, _ = self._collect(payload)
        self.assertNotIn("comment", [e.get("type") for e in events])
        # server.connected 仍是第一帧（V2 客户端语义：过滤订阅也收到 connected）
        self.assertEqual(events[0]["type"], "server.connected")

    def test_string_typed_json_payload_double_parsed(self):
        payload = self.load_bytes("sse", "connected_and_events.stream")
        events, _, _ = self._collect(payload)
        asked = [e for e in events if e["type"] == "permission.asked"]
        self.assertEqual(len(asked), 1)
        self.assertEqual(asked[0]["data"]["id"], "per_<<REDACTED>>")

    def test_non_dict_json_frames_dropped(self):
        payload = self.load_bytes("sse", "connected_and_events.stream")
        events, _, _ = self._collect(payload)
        self.assertFalse(any(e == [1, 2, 3] or e == 42 for e in events))
        # 列表帧不产生 dict 事件
        self.assertTrue(all(isinstance(e, dict) for e in events))

    def test_bad_json_frame_skipped_stream_continues(self):
        payload = self.load_bytes("sse", "bad_json_resilient.stream")
        events, err, _ = self._collect(payload)
        self.assertIn("EOF", str(err))
        self.assertEqual([e.get("type") for e in events],
                         ["server.connected", "session.created"])

    def test_empty_data_frame_skipped(self):
        stream = b'data:\n\ndata: {"type":"server.connected","data":{}}\n\n'
        events, err, _ = self._collect(stream)
        self.assertIn("EOF", str(err))
        self.assertEqual([e.get("type") for e in events], ["server.connected"])

    def test_crlf_line_endings(self):
        stream = (b'data: {"type":"server.connected","data":{}}\r\n\r\n'
                  b'data: {"type":"session.created","data":{"id":"ses_X"}}\r\n\r\n')
        events, err, _ = self._collect(stream)
        self.assertIn("EOF", str(err))
        self.assertEqual([e.get("type") for e in events],
                         ["server.connected", "session.created"])
        self.assertEqual(events[1]["data"]["id"], "ses_X")

    def test_eof_immediately_after_headers(self):
        step = self._stream_step(b"")
        srv = self.server([step])
        client = self.client_to(srv.port)
        with self.assertRaises(StreamError) as cm:
            for _ in client.iter_events(["session.created"], socket_timeout=0.25):
                pass
        self.assertIn("EOF", str(cm.exception))

    def test_unterminated_frame_is_not_dispatched_at_eof(self):
        events, error, _ = self._collect(b'data: {"type":"session.execution.succeeded"}\n')
        self.assertEqual(events, [])
        self.assertIn("EOF", str(error))

    def test_read_timeout_is_stream_error(self):
        payload = b'data: {"type":"server.connected","data":{}}\n\n'
        step = self._stream_step(payload, hang=0.4)  # 发送后悬挂 → 读超时
        srv = self.server([step])
        client = self.client_to(srv.port)
        with self.assertRaises(StreamError) as cm:
            for _ in client.iter_events(["session.created"], socket_timeout=0.15):
                pass
        self.assertIn("read-timeout", str(cm.exception))

    def test_non_200_status_stream_error(self):
        srv = self.server([{"status": 401, "payload": self.load_bytes(
            "http", "error_unauthorized.json")}])
        client = self.client_to(srv.port)
        with self.assertRaises(StreamError) as cm:
            for _ in client.iter_events(["session.created"], socket_timeout=0.25):
                pass
        self.assertIn("HTTP 401", str(cm.exception))

    def test_connection_refused_stream_error(self):
        port = self.dead_port()
        client = self.client_to(port)
        with self.assertRaises(StreamError) as cm:
            for _ in client.iter_events(["session.created"], socket_timeout=0.25):
                pass
        self.assertIn("connection", str(cm.exception))

    def test_events_not_triggered_by_finish(self):
        # EOF/断开只抛 StreamError，不产生"伪造"事件（V2 live-only：不重放、不补全）
        step = self._stream_step(b"")
        srv = self.server([step])
        client = self.client_to(srv.port)
        with self.assertRaises(StreamError):
            for _ in client.iter_events(["session.created"], socket_timeout=0.25):
                self.fail("SSE 不应在 EOF 时伪造事件")


if __name__ == "__main__":
    unittest.main(verbosity=2)

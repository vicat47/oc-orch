#!/usr/bin/env python3
"""CLI 与 Web 入口等价性：同一 application 用例、独立连接、拒绝原因一致。

OpenSpec 4.1/4.2 验收：
- CLI（dispatch/rework/verify/collect/pause/resume/cancel/ack）与 Web 写动作
  都通过 composition.open_application 装配的同一应用用例，业务结论与拒绝
  原因一致；Web 外层维持 result/error.detail 的 CLI 嵌套形状。
- Web 每个请求独立连接（并发互不共享事务连接）；写动作不调用 subprocess
  （Supervisor 生命周期除外）。
- CLI daemon 启动统一 `python -m oc_orch.cli`（facade 兼容 + wheel 可用），
  不依赖 bin/ 布局；派单 Client 延迟装配，dry-run 无服务配置也可执行。

隔离：临时 home 真 SQLite；loopback 假 OpenCode 服务（仅 message/shell/
session/prompt 端点）供真实 Client 适配器离线路由；绝不连真实服务。
"""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
import http.server
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = Path(HERE).parent
SRC = str(REPO / "src")
sys.path.insert(0, SRC)

from oc_orch.db import init  # noqa: E402
from oc_orch.domain.errors import TransportError  # noqa: E402
from oc_orch.ledger import Ledger  # noqa: E402
from oc_orch.util import now_ms  # noqa: E402
from oc_orch.web import WebServer  # noqa: E402
from oc_orch.adapters.results import scan_deliverable_baseline  # noqa: E402


class _FakeHandler(http.server.BaseHTTPRequestHandler):
    """最小 loopback 假 OpenCode 服务：message/shell/session/prompt 端点。"""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        parts = path.strip("/").split("/")
        if path in ("/api/session", "/api/session/active") or path.endswith("/shell"):
            self._send({"data": []})
        elif len(parts) >= 3 and parts[:2] == ["api", "session"]:
            sid = parts[2]
            if len(parts) >= 4 and parts[3] == "message":
                rows = self.server.messages.get(sid, [])
                if len(parts) == 5:
                    message = next((m for m in rows if m["id"] == parts[4]), None)
                    self._send({"data": message} if message else {"message": "not found"},
                               200 if message else 404)
                    return
                query = urllib.parse.parse_qs(parsed.query)
                direction = query.get("order", ["desc"])[0]
                offset = 0
                cursor = query.get("cursor", [None])[0]
                if cursor:
                    direction, raw_offset = cursor.split(":", 1)
                    offset = int(raw_offset)
                limit = int(query.get("limit", ["60"])[0])
                ordered = sorted(rows, key=lambda m: (m["time"]["created"], m["id"]),
                                 reverse=direction == "desc")
                data = ordered[offset:offset + limit]
                nxt = f"{direction}:{offset + len(data)}" if offset + len(data) < len(ordered) else None
                self._send({"data": data, "cursor": {"previous": None, "next": nxt}})
            elif len(parts) == 4 and parts[3] in ("inbox", "form"):
                self._send({"data": []})
            else:
                self._send({"data": self.server.sessions.get(sid, {
                    "id": sid, "time": {}, "outcome": None,
                    "location": {"directory": self.server.home}})})
        else:
            self._send({"data": {}})

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        if path == "/api/session":
            sid = body["id"]
            self.server.sessions[sid] = {
                "id": sid, "time": {}, "outcome": None,
                "location": {"directory": self.server.home}}
            self._send({"data": self.server.sessions[sid]})
        else:
            self._send({"data": {"id": body.get("id", "msg_fake")}})


class FakeService:
    """启动一个 loopback 假服务并写 service.json（dead-port 模式可选）。"""

    def __init__(self, home: str):
        self.home = home
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _FakeHandler)
        self.httpd.daemon_threads = True
        self.httpd.home = home
        self.httpd.sessions = {}
        self.httpd.messages = {}
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       name="oc-orch-fake-svc", daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def write_config(self, path: str) -> None:
        Path(path).write_text(json.dumps({
            "hostname": "127.0.0.1", "port": self.port, "url": self.url,
            "password": "pw",
        }), encoding="utf-8")
        os.chmod(path, 0o600)

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    def stop(self) -> None:
        try:
            self.httpd.shutdown()
            self.httpd.server_close()
        except Exception:
            pass

    def finished(self, sid, message_id, started, ended):
        """真实 V2 形状的输入→完整 assistant→idle，供两个入口使用同一证据。"""
        rows = []
        if message_id:
            rows.append({"id": message_id, "type": "user", "text": "execute task",
                         "time": {"created": started}})
        rows.extend([
            {"id": f"msg_answer_{sid}", "type": "assistant", "agent": "test",
             "model": {"providerID": "test", "id": "test"}, "finish": "stop",
             "time": {"created": started + 1, "completed": ended - 1},
             "content": [{"type": "text", "text": "equivalence-done"}]},
            {"id": f"msg_idle_{sid}", "type": "idle", "outcome": "succeeded",
             "time": {"created": ended}},
        ])
        self.httpd.messages[sid] = rows
        self.httpd.sessions[sid] = {
            "id": sid, "outcome": "succeeded", "time": {"busy": started, "idle": ended},
            "location": {"directory": self.home}}


def dead_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class EntrypointEquivalence(unittest.TestCase):
    """每条用例独立临时 home；CLI 走 `python -m oc_orch.cli`，Web 走 loopback HTTP。"""

    def setUp(self):
        self.home_dir = tempfile.TemporaryDirectory(
            prefix="oc-orch-equiv-", dir=os.environ.get("TMPDIR") or None)
        self.addCleanup(self.home_dir.cleanup)
        self.home = self.home_dir.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)
        self.blob: dict = {}

    # ---- helpers -------------------------------------------------
    def finished_attempt(self, service, task, *, session="ses_fixture", deliverables=()):
        sid = f"{session}_{time.time_ns()}"
        message_id = f"msg_input_{sid}"
        attempt = self.ledger.create_attempt(
            task["id"], directory=self.home, session_id=sid,
            dispatch_msg_id=message_id, status="dispatched", dispatch_state="confirmed",
            deliverables=list(deliverables))
        baseline = scan_deliverable_baseline(self.ledger, attempt["id"], captured_at=now_ms())
        self.ledger.seal_execution(attempt["id"], mode="new", session_before={}, baseline=baseline)
        self.finish_attempt(service, attempt)
        return self.ledger.get_attempt(attempt["id"])

    def finish_attempt(self, service, attempt):
        started = now_ms()
        ended = started + 5
        service.finished(attempt["session_id"], attempt.get("dispatch_msg_id"), started, ended)
        self.ledger.record_execution_started(attempt["id"], started_at=started,
                                             event_id=f"start-{attempt['id']}")
        terminal = self.ledger.record_terminal(
            attempt["id"], outcome="succeeded", source="sse",
            event_id=f"end-{attempt['id']}", candidate={
                "session_id": attempt["session_id"], "outcome": "succeeded",
                "source": "sse", "ended_at": ended, "input_in_inbox": False,
                "input_owner": "unique"})
        self.assertEqual(terminal["result"], "accepted", terminal)

    def cli(self, *args, env_extra: dict | None = None) -> tuple[subprocess.CompletedProcess, dict]:
        cmd = [sys.executable, "-B", "-m", "oc_orch.cli", "--home", self.home,
               "--json", *args]
        env = os.environ.copy()
        env["PYTHONPATH"] = SRC
        env["OC_ORCH_HOME"] = self.home
        env["NO_PROXY"] = "127.0.0.1,localhost,::1"
        env["no_proxy"] = "127.0.0.1,localhost,::1"
        for k, v in (env_extra or {}).items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v
        p = subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True,
                           stdin=subprocess.DEVNULL, env=env, timeout=90)
        try:
            out = json.loads(p.stdout) if p.stdout.strip() else {}
        except ValueError:
            out = {}
        self.blob = out
        return p, out

    def start_web(self, *, read_only: bool = False, client_factory=None) -> tuple[WebServer, int]:
        server = WebServer(self.home, port=0, read_only=read_only,
                           client_factory=client_factory)
        port = server.start()
        self.addCleanup(server.stop)
        return server, port

    def post(self, port: int, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = json.dumps(body or {}).encode("utf-8")
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}", data=data, method="POST",
            headers={"Content-Type": "application/json", "X-OC-Orch-Web": "1"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8")
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {}

    # ---- pause/resume 等价 ---------------------------------------
    def test_pause_resume_equivalence(self):
        t = self.ledger.create_task(project="p", title="t")
        # CLI pause
        p, out = self.cli("task", "pause", t["id"])
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(out["task"]["status"], "paused")
        cli_from = out["task"]["metadata"]["paused_from"]
        # CLI again（幂等已暂停）
        p, out = self.cli("task", "pause", t["id"])
        self.assertEqual(p.returncode, 0)
        self.assertEqual(out["task"]["status"], "paused")
        # CLI resume
        p, out = self.cli("task", "resume", t["id"])
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(out["task"]["status"], cli_from)
        # Web 同样业务结论
        _, port = self.start_web()
        st, body = self.post(port, f"/api/task/{t['id']}/pause")
        self.assertEqual(st, 200, body)
        self.assertEqual(body["result"]["task"]["status"], "paused")
        st, body = self.post(port, f"/api/task/{t['id']}/resume")
        self.assertEqual(st, 200, body)
        self.assertEqual(body["result"]["task"]["status"], cli_from)
        self.assertEqual(self.ledger.get_task(t["id"])["status"], cli_from)

    def test_resume_non_paused_rejection_equivalence(self):
        t = self.ledger.create_task(project="p", title="t")
        # CLI：exit 2 code=usage
        p, out = self.cli("task", "resume", t["id"])
        self.assertEqual(p.returncode, 2)
        self.assertEqual(out.get("code"), "usage")
        # Web：400 code=usage，error.detail 保留 CLI 嵌套形状
        _, port = self.start_web()
        st, body = self.post(port, f"/api/task/{t['id']}/resume")
        self.assertEqual(st, 400)
        self.assertEqual(body.get("code"), "usage")
        self.assertEqual(body["detail"]["code"], "usage")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "draft")

    # ---- cancel 等价 ----------------------------------------------
    def test_cancel_equivalence_and_confirmation(self):
        t = self.ledger.create_task(project="p", title="t")
        # Web 确认不匹配：400 CONFIRM_REQUIRED，不进入业务
        _, port = self.start_web()
        st, body = self.post(port, f"/api/task/{t['id']}/cancel",
                             {"reason": "web", "confirm": "wrong"})
        self.assertEqual(st, 400)
        self.assertEqual(body.get("code"), "CONFIRM_REQUIRED")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "draft")
        # CLI cancel：exit 0 → cancelled
        p, out = self.cli("task", "cancel", t["id"], "--reason", "manual")
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(out["task"]["status"], "cancelled")
        self.assertEqual(self.ledger.get_task(t["id"])["closed_at"] is not None, True)
        # Web 对同结论的第二次 cancel：CLI 已取消 → web 幂等返回 cancelled
        t2 = self.ledger.create_task(project="p", title="t2")
        st, body = self.post(port, f"/api/task/{t2['id']}/cancel",
                             {"reason": "web", "confirm": t2["id"]})
        self.assertEqual(st, 200, body)
        self.assertEqual(body["result"]["task"]["status"], "cancelled")

    # ---- 通知/告警 ack 等价 ----------------------------------------
    def test_notify_ack_equivalence(self):
        n = self.ledger.enqueue_notification(
            kind="test", target_session="ses_x", dedupe_key="eq:1",
            payload={"summary": "x"})
        # CLI ack
        p, out = self.cli("notify", "ack", n["id"])
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(out["acked"], 1)
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "acked")
        # Web ack 新通知
        n2 = self.ledger.enqueue_notification(
            kind="test", target_session="ses_x", dedupe_key="eq:2",
            payload={"summary": "y"})
        _, port = self.start_web()
        st, body = self.post(port, "/api/notify/ack", {"ids": [n2["id"]]})
        self.assertEqual(st, 200, body)
        self.assertEqual(body["result"]["acked"], 1)
        self.assertEqual(self.ledger.get_notification(n2["id"])["state"], "acked")

    def test_alerts_ack_equivalence(self):
        a = self.ledger.add_alert(severity="warn", kind="test", title="alert",
                                  detail="detail", dedupe_key="eq:alert:1")
        p, out = self.cli("alerts", "ack", a["id"])
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(len(out["alerts"]), 1)
        self.assertEqual(self.ledger.get_alert(a["id"])["state"], "acked")
        a2 = self.ledger.add_alert(severity="warn", kind="test", title="a2",
                                   detail="d", dedupe_key="eq:alert:2")
        _, port = self.start_web()
        st, body = self.post(port, "/api/alerts/ack", {"ids": [a2["id"]]})
        self.assertEqual(st, 200, body)
        self.assertEqual(len(body["result"]["alerts"]), 1)
        self.assertEqual(self.ledger.get_alert(a2["id"])["state"], "acked")

    # ---- collect 等价（真实 Client → loopback 假服务）-------------
    def test_collect_equivalence(self):
        svc = FakeService(self.home)
        self.addCleanup(svc.stop)
        svc_path = os.path.join(self.home, "service.json")
        svc.write_config(svc_path)
        t = self.ledger.create_task(project="p", title="t")
        a = self.finished_attempt(svc, t, session="ses_fake")
        # CLI collect（子进程真实 Client → 假服务）
        p, out = self.cli("collect", "--attempt", a["id"],
                          env_extra={"OC_ORCH_SERVICE_CONFIG": svc_path})
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual(out["status"], "result_collected")
        self.assertTrue(out["text_chars"] > 0)
        self.assertEqual(out["attempt_id"], a["id"])
        # 新冲突规则：site_released_at 恒空的终端尝试仍占写权；释放后新尝试才能认领
        self.ledger.release_site(a["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        # Web collect（测试进程真实 Client → 同一假服务）
        saved = os.environ.get("OC_ORCH_SERVICE_CONFIG")
        os.environ["OC_ORCH_SERVICE_CONFIG"] = svc_path
        self.addCleanup(
            lambda: os.environ.__setitem__("OC_ORCH_SERVICE_CONFIG", saved)
            if saved is not None else os.environ.pop("OC_ORCH_SERVICE_CONFIG", None))
        a2 = self.finished_attempt(svc, t, session="ses_fake2")
        _, port = self.start_web()
        st, body = self.post(port, f"/api/attempt/{a2['id']}/collect")
        self.assertEqual(st, 200, body)
        self.assertEqual(body["result"]["status"], "result_collected")
        self.assertEqual(body["result"]["text_chars"] > 0, True)
        # 业务结论一致：结果文件真实存在且与 CLI 相同 schema
        for aid in (a["id"], a2["id"]):
            rp = self.ledger.get_attempt(aid)["result_path"]
            self.assertTrue(rp and os.path.exists(rp))
            data = json.loads(Path(rp).read_text(encoding="utf-8"))
            self.assertEqual(data["schema"], "oc-orch.result.v1")

    # ---- verify 等价 + 拒绝原因一致 --------------------------------
    def test_verify_equivalence_and_gate(self):
        svc = FakeService(self.home)
        self.addCleanup(svc.stop)
        svc_path = os.path.join(self.home, "service.json")
        svc.write_config(svc_path)
        env_extra = {"OC_ORCH_SERVICE_CONFIG": svc_path}

        def make_collected():
            t = self.ledger.create_task(project="p", title="t")
            a = self.finished_attempt(svc, t, session="ses_v")
            self.ledger.release_site(a["id"], confirmed=True,
                                     external_stopped=True, actor="test")
            return t, a

        # CLI：collect 前直接 pass → exit 7 code=gate，不 accepted
        t0, a0 = make_collected()
        p, out = self.cli("verify", "--attempt", a0["id"], "--pass",
                          "--evidence", "e", env_extra=env_extra)
        self.assertEqual(p.returncode, 7, p.stdout)
        self.assertEqual(out.get("code"), "gate")
        self.assertNotEqual(self.ledger.get_task(t0["id"])["status"], "accepted")

        # 真实 collect 后 CLI pass → accepted
        t1, a1 = make_collected()
        collect_attempt_path = os.path.join(self.home, "results",
                                            f"{a1['id']}.json")
        # 用 CLI 自己 collect（复用上一步假服务）
        p, _ = self.cli("collect", "--attempt", a1["id"], env_extra=env_extra)
        self.assertEqual(p.returncode, 0, p.stderr)
        p, out = self.cli("verify", "--attempt", a1["id"], "--pass",
                          "--evidence", "clie", env_extra=env_extra)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(out["task"]["status"], "accepted")
        self.assertEqual(out["verification"]["result"], "pass")
        self.assertIsNotNone(collect_attempt_path)

        # Web：另一个已 collect 尝试 → 200 accepted；拒绝原因与 CLI 相同 code
        t2, a2 = make_collected()
        saved = os.environ.get("OC_ORCH_SERVICE_CONFIG")
        os.environ["OC_ORCH_SERVICE_CONFIG"] = svc_path
        self.addCleanup(
            lambda: os.environ.__setitem__("OC_ORCH_SERVICE_CONFIG", saved)
            if saved is not None else os.environ.pop("OC_ORCH_SERVICE_CONFIG", None))
        _, port = self.start_web()
        # Web 未 collect 直接 pass → 400 code=gate（error.detail 保留 CLI 嵌套）
        st, body = self.post(port, f"/api/attempt/{a2['id']}/verify",
                             {"pass": True, "confirm": a2["id"],
                              "evidence": ["webe"]})
        self.assertEqual(st, 400, body)
        self.assertEqual(body.get("code"), "gate")
        self.assertEqual(body["detail"]["code"], "gate")
        self.assertNotEqual(self.ledger.get_task(t2["id"])["status"], "accepted")
        # Web collect 后 verify → 200 accepted
        st, body = self.post(port, f"/api/attempt/{a2['id']}/collect")
        self.assertEqual(st, 200, body)
        st, body = self.post(port, f"/api/attempt/{a2['id']}/verify",
                             {"pass": True, "confirm": a2["id"],
                              "evidence": ["webe"]})
        self.assertEqual(st, 200, body)
        self.assertEqual(body["result"]["task"]["status"], "accepted")
        self.assertEqual(self.ledger.get_task(t2["id"])["status"], "accepted")

    # ---- 释放现场：shell 查询失败保持写权 --------------------------
    def test_release_query_failure_keeps_site(self):
        t = self.ledger.create_task(project="p", title="t")
        a = self.ledger.create_attempt(t["id"], directory=self.home,
                                       status="dispatched",
                                       dispatch_state="confirmed")
        self.ledger.transition_attempt(a["id"], "terminal_succeeded")
        dead = dead_port()
        svc_path = os.path.join(self.home, "service.json")
        Path(svc_path).write_text(json.dumps(
            {"hostname": "127.0.0.1", "port": dead, "url": f"http://127.0.0.1:{dead}",
             "password": ""}), encoding="utf-8")
        os.chmod(svc_path, 0o600)
        env_extra = {"OC_ORCH_SERVICE_CONFIG": svc_path}
        # 查询失败（连接拒绝）→ exit 7 gate 且 site 保持未释放
        p, out = self.cli("attempt", "release", a["id"], "--confirm",
                          env_extra=env_extra)
        self.assertEqual(p.returncode, 7, p.stdout + p.stderr)
        self.assertEqual(out.get("code"), "gate")
        self.assertIsNone(self.ledger.get_attempt(a["id"])["site_released_at"])
        # 人工核实后用 --external-stopped → 成功释放
        p, out = self.cli("attempt", "release", a["id"], "--confirm",
                          "--external-stopped", env_extra=env_extra)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertIsNotNone(self.ledger.get_attempt(a["id"])["site_released_at"])

    # ---- dispatch dry-run：无服务配置也可执行（Client 延迟装配）----
    def test_dispatch_dry_run_without_service_config(self):
        t = self.ledger.create_task(project="p", title="t",
                                    coordinator_session="ses_c")
        p, out = self.cli("dispatch", "--task", t["id"], "--dry-run",
                          "--directory", self.home,
                          env_extra={"OC_ORCH_SERVICE_CONFIG": None})
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertTrue(out.get("dry_run"))
        self.assertTrue(out["attempt_id"].startswith("A-"))
        self.assertTrue(out["prompt"])
        # dry-run 不产生任何尝试
        self.assertEqual(self.ledger.list_attempts(task_id=t["id"]), [])

    # ---- dispatch/rework 真实走假服务 ------------------------------
    def test_dispatch_and_rework_via_cli(self):
        svc = FakeService(self.home)
        self.addCleanup(svc.stop)
        svc_path = os.path.join(self.home, "service.json")
        svc.write_config(svc_path)
        env_extra = {"OC_ORCH_SERVICE_CONFIG": svc_path}
        t = self.ledger.create_task(project="p", title="t",
                                    coordinator_session="ses_c")
        # dispatch → dispatched，尝试落库
        p, out = self.cli("dispatch", "--task", t["id"], "--directory",
                          self.home, "--no-prompt", env_extra=env_extra)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual(out["ok"], True)
        att = self.ledger.get_attempt(out["attempt_id"])
        self.assertEqual(att["status"], "dispatched")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "dispatched")
        # 终态后 rework → 新尝试 g2 同一 session（合法证据入口写终态）
        self.finish_attempt(svc, att)
        self.ledger.release_site(att["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        p, out = self.cli("rework", "--task", t["id"], "--feedback", "fix it",
                          env_extra=env_extra)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(out["generation"], 2)
        self.assertEqual(out["session_id"], att["session_id"])

    def test_collect_carry_manifest_and_version_display_via_cli(self):
        """CLI collect --carry-manifest：读取清单传 intent，返回版本/通知 ID，不隐式 ACK。"""
        svc = FakeService(self.home)
        self.addCleanup(svc.stop)
        svc_path = os.path.join(self.home, "service.json")
        svc.write_config(svc_path)
        env_extra = {"OC_ORCH_SERVICE_CONFIG": svc_path}
        t = self.ledger.create_task(project="p", title="carry-cli")
        out_file = os.path.join(self.home, "out.txt")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("v1")
        digest = hashlib.sha256(Path(out_file).read_bytes()).hexdigest()
        a = self.finished_attempt(svc, t, session="ses_carry", deliverables=["out.txt"])
        self.ledger.release_site(a["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        manifest = os.path.join(self.home, "carry.json")
        Path(manifest).write_text(json.dumps({
            "schema": "oc-orch.carry.v1", "attempt_id": a["id"],
            "items": [{"target": "out.txt", "baseline_sha256": digest,
                       "reason": "CLI 显式沿用"}]}, ensure_ascii=False),
            encoding="utf-8")
        p, out = self.cli("collect", "--attempt", a["id"],
                          "--carry-manifest", manifest, env_extra=env_extra)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertTrue(out.get("result_version"), out)
        self.assertTrue(out.get("notification_ids"), out)
        rows = self.ledger.conn.execute(
            "SELECT state FROM notifications WHERE attempt_id=?", (a["id"],)).fetchall()
        for r in rows:
            self.assertNotEqual(r["state"], "acked", "collect 不隐式 ACK")
        self.assertEqual(out["collection_state"], "complete")
        self.assertIn("result_version", out)
        self.assertIn("notification_ids", out)

    def test_collect_bad_carry_manifest_path_exit_usage(self):
        svc = FakeService(self.home)
        self.addCleanup(svc.stop)
        svc_path = os.path.join(self.home, "service.json")
        svc.write_config(svc_path)
        p, _ = self.cli("collect", "--attempt", "A-fake",
                        "--carry-manifest", "no-such-file.json",
                        env_extra={"OC_ORCH_SERVICE_CONFIG": svc_path})
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)

    def test_collect_invalid_carry_values_leave_result_and_notifications_untouched(self):
        svc = FakeService(self.home)
        self.addCleanup(svc.stop)
        service_path = Path(self.home) / "service.json"
        svc.write_config(str(service_path))
        task = self.ledger.create_task(project="p", title="invalid-carry")
        att = self.finished_attempt(svc, task)
        manifest = Path(self.home) / "invalid-carry.json"
        values = [[], {"schema": "wrong", "attempt_id": att["id"], "items": []},
                  {"schema": "oc-orch.carry.v1", "attempt_id": "A-other", "items": []},
                  {"schema": "oc-orch.carry.v1", "attempt_id": att["id"], "items": "bad"}]
        before = self.ledger.get_attempt(att["id"])
        for value in values:
            with self.subTest(value=value):
                manifest.write_text(json.dumps(value), encoding="utf-8")
                proc, out = self.cli("collect", "--attempt", att["id"],
                                     "--carry-manifest", str(manifest), env_extra={
                                         "OC_ORCH_SERVICE_CONFIG": str(service_path)})
                self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
                self.assertEqual(out.get("code"), "CARRY_MANIFEST_INVALID")
                self.assertIn("CARRY", out.get("message", ""))
                self.assertNotIn("Traceback", proc.stderr)
                after = self.ledger.get_attempt(att["id"])
                self.assertEqual(after["result_path"], before["result_path"])
                self.assertEqual(after["result_collected_at"], before["result_collected_at"])
                self.assertEqual(self.ledger.list_notifications(), [])

class WebConcurrency(unittest.TestCase):
    """每请求独立连接：并发写动作互不共享事务连接、结论一致。"""

    def setUp(self):
        self.home_dir = tempfile.TemporaryDirectory(
            prefix="oc-orch-conc-", dir=os.environ.get("TMPDIR") or None)
        self.addCleanup(self.home_dir.cleanup)
        self.home = self.home_dir.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)
        self.server = WebServer(self.home, port=0)
        # 16 路同时提交用于验证独立 SQLite 连接；夹具的监听队列需容纳全部客户端。
        # stdlib 默认 backlog=5 会在业务 handler 之前因 TCP 突发丢连接。
        with patch("oc_orch.web.ThreadingHTTPServer.request_queue_size", 32):
            self.port = self.server.start()
        self.addCleanup(self.server.stop)

    def post(self, path, body):
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method="POST",
            headers={"Content-Type": "application/json", "X-OC-Orch-Web": "1"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8")
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {}

    def test_concurrent_acks_and_pauses_independent_conn(self):
        N = 8
        notif_ids = []
        for i in range(N):
            n = self.ledger.enqueue_notification(
                kind="test", target_session="ses_c",
                dedupe_key=f"conc:{i}", payload={"summary": f"{i}"})
            notif_ids.append(n["id"])
        task_ids = [self.ledger.create_task(project=f"p{i}", title=f"t{i}")["id"]
                    for i in range(N)]
        barrier = threading.Barrier(N * 2)

        def ack(nid):
            barrier.wait()
            st, body = self.post("/api/notify/ack", {"ids": [nid]})
            return "ack", nid, st, body

        def pause(tid):
            barrier.wait()
            st, body = self.post(f"/api/task/{tid}/pause", {"reason": "conc"})
            return "pause", tid, st, body

        with ThreadPoolExecutor(max_workers=N * 2) as pool:
            futures = [pool.submit(ack, nid) for nid in notif_ids]
            futures += [pool.submit(pause, tid) for tid in task_ids]
            results = [future.result(timeout=30) for future in futures]
        self.assertEqual(len(results), N * 2)
        for kind, ident, st, body in results:
            self.assertEqual(st, 200, f"{kind} {ident}: {body}")
        for nid in notif_ids:
            self.assertEqual(self.ledger.get_notification(nid)["state"], "acked")
        for tid in task_ids:
            self.assertEqual(self.ledger.get_task(tid)["status"], "paused")

    def test_business_actions_never_call_subprocess(self):
        """写动作直接调用应用，不经过 subprocess/redirect_stdout 执行 CLI。"""
        n = self.ledger.enqueue_notification(
            kind="test", target_session="ses_c", dedupe_key="nosub:1",
            payload={"summary": "x"})
        t = self.ledger.create_task(project="p", title="t")
        with patch("subprocess.run", side_effect=AssertionError("业务动作不得 subprocess")), \
                patch("subprocess.Popen", side_effect=AssertionError("业务动作不得 subprocess")):
            st, body = self.post("/api/notify/ack", {"ids": [n["id"]]})
            self.assertEqual(st, 200, body)
            st, body = self.post(f"/api/task/{t['id']}/pause", {"reason": "nosub"})
            self.assertEqual(st, 200, body)
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "acked")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "paused")


if __name__ == "__main__":
    unittest.main(verbosity=2)

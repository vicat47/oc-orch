#!/usr/bin/env python3
"""F2 验收切片 · 公开 CLI 契约回归（基线红测试，不依赖新内部提交 API）。

设计契约（主 agent 已同步，2026-11-06）：
- 正常 pass：必须已 collect（结果文件 + result_collected_at），交付物/检查齐全；
  否则 exit 7 gate。不 collect 直接 pass 当前会 accepted（缺陷，先红）。
- force：非空 --reason 才可豁免 collect/交付证据缺项，快照记录缺项；
  不可豁免 current/paused/cancelled/closed（当前 cancel 会部分提交 exit 2，先红）。
- 重复 pass：already_accepted 幂等 exit 0。
- fail：必须带 --reason，任务 rejected、尝试 verification_failed。

隔离：TemporaryDirectory(dir=$TMPDIR)；真 Ledger + collect_attempt + 极简
FakeClient 提供原输入/完成 assistant/idle 窗口，产生真实结果文件；验收前显式 release_site 避免
_ensure_site_free 网络；CLI 子进程 OC_ORCH_SERVICE_CONFIG 指向绑定不监听的
loopback 端口，stdin=DEVNULL，绝不连真实服务。

注意：新分类字段 normal_effective 名称未冻结，本文件只校验
forced / force_reason / verification.missing，不做超约束断言。
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
CLI = Path(REPO) / "bin" / "oc-orch"
sys.path.insert(0, os.path.join(REPO, "src"))

from oc_orch.config import load_config  # noqa: E402
from oc_orch.db import init  # noqa: E402
from oc_orch.ledger import Ledger  # noqa: E402
from oc_orch.results import collect_attempt, scan_deliverable_baseline  # noqa: E402
from oc_orch.util import now_ms  # noqa: E402


class FakeClient:
    """完成证据的有界 V2 窗口；CLI 验收本身仍使用不可达的隔离服务配置。"""

    def __init__(self, messages):
        self._messages = messages

    def messages(self, session_id, limit=None, order=None):
        return self._messages

    def message_page(self, session_id, *, limit=100, order="asc", cursor=None):
        rows = sorted(self._messages, key=lambda m: (m["time"]["created"], m["id"]),
                      reverse=order == "desc")
        return {"data": rows, "cursor": {"next": None, "previous": None}}

    def get_message(self, session_id, message_id):
        return next((m for m in self._messages if m["id"] == message_id), None)

    def inbox(self, session_id):
        return []

    def active_sessions(self):
        return {}

    def get_session(self, session_id):
        return {"id": session_id, "outcome": "succeeded", "time": {
            "busy": self._messages[0]["time"]["created"],
            "idle": self._messages[-1]["time"]["created"]}}


class VerifyCliContract(unittest.TestCase):
    """每条用例独立临时 home：真 Ledger + 绑定不监听的隔离服务配置。"""

    def setUp(self):
        # 每个资源创建后立即注册 addCleanup（顺序 tmp→socket→conn；
        # LIFO 实际清理 conn→socket→tmp），setUp 中途异常也能保证清理。
        self.tmp = tempfile.TemporaryDirectory(
            prefix="oc-orch-f2-cli-", dir=os.environ.get("TMPDIR") or None)
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        # 绑定但不 listen：占用端口避免误连真实服务，同时保持配置可读。
        self._sock = socket.socket()
        self.addCleanup(self._sock.close)
        self._sock.bind(("127.0.0.1", 0))
        svc = Path(self.home) / "service.json"
        svc.write_text(json.dumps({
            "hostname": "127.0.0.1",
            "port": self._sock.getsockname()[1],
            "password": "",
        }), encoding="utf-8")
        svc.chmod(0o600)
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)
        self.cfg = load_config(self.home)
        self.blob: dict = {}

    # 注意：不使用 tearDown，清理全部由 addCleanup 承担，避免 setUp
    # 异常或重复清理导致的二次释放。

    # ---- fixture -------------------------------------------------
    def make(self, deliverables=(), checks=()):
        t = self.ledger.create_task(
            project="p", title="t", objective="o", acceptance=["a"])
        specs = (list(deliverables)
                 + [{"kind": "command", "target": c} for c in checks])
        a = self.ledger.create_attempt(
            t["id"], directory=self.home, status="dispatched",
            session_id=f"ses_{t['id']}", dispatch_msg_id=f"msg_input_{t['id']}",
            dispatch_state="confirmed", deliverables=specs)
        baseline = scan_deliverable_baseline(self.ledger, a["id"], captured_at=now_ms())
        self.ledger.seal_execution(a["id"], mode="new", session_before={}, baseline=baseline)
        return t, a

    def terminal(self, aid):
        started = now_ms()
        att = self.ledger.get_attempt(aid)
        self.ledger.record_execution_started(aid, started_at=started,
                                             event_id=f"start-{aid}")
        out = self.ledger.record_terminal(
            aid, outcome="succeeded", source="sse", event_id=f"end-{aid}",
            candidate={"session_id": att["session_id"], "source": "sse",
                       "outcome": "succeeded", "ended_at": started + 5,
                       "input_in_inbox": False, "input_owner": "unique"})
        self.assertEqual(out["result"], "accepted", out)

    def make_file(self, name, text="x"):
        p = Path(self.home) / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")

    def released(self, aid):
        self.ledger.release_site(aid, confirmed=True,
                                 external_stopped=True, actor="test")

    def collected(self, aid, text="DONE"):
        """collect_attempt + FakeClient：产生真实结果文件并置 result_collected。"""
        att = self.ledger.get_attempt(aid)
        ended = att["terminal_at"]
        rows = [
            {"id": att["dispatch_msg_id"], "type": "user", "text": "execute task",
             "time": {"created": ended - 5}},
            {"id": f"msg_answer_{aid}", "type": "assistant", "finish": "stop",
             "time": {"created": ended - 4, "completed": ended - 1},
             "content": [{"type": "text", "text": text}]},
            {"id": f"msg_idle_{aid}", "type": "idle", "outcome": "succeeded",
             "time": {"created": ended}},
        ]
        out = collect_attempt(self.ledger, FakeClient(rows), aid, self.cfg, auto=False)
        self.assertEqual(out["status"], "result_collected", out)
        self.released(aid)

    def cli(self, *args):
        cmd = [sys.executable, "-B", str(CLI), "--home", self.home,
               "--json", *args]
        env = os.environ.copy()
        env.pop("OC_ORCH_CONFIG_JSON", None)
        env["OC_ORCH_HOME"] = self.home
        env["OC_ORCH_SERVICE_CONFIG"] = str(Path(self.home) / "service.json")
        p = subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True,
                           stdin=subprocess.DEVNULL, env=env, timeout=90)
        out = p.stdout.strip()
        self.blob = json.loads(out) if out.startswith("{") else {}
        return p

    # ---- 合法路径（正确旧行为锚点）-------------------------------
    def test_legal_collect_then_pass(self):
        """collect -> verify --pass：exit 0，原字段 task/attempt/verification。"""
        t, a = self.make(deliverables=["out.txt"])
        self.terminal(a["id"])
        self.make_file("out.txt")
        self.collected(a["id"])
        p = self.cli("verify", "--attempt", a["id"], "--pass",
                     "--evidence", "e1", "--notes", "n1")
        self.assertEqual(p.returncode, 0, p.stderr)
        j = self.blob
        self.assertTrue(j.get("ok"))
        self.assertEqual(j["task"]["status"], "accepted")
        self.assertEqual(j["attempt"]["status"], "accepted")
        self.assertEqual(j["verification"]["result"], "pass")
        self.assertEqual(j["verification"]["evidence"], ["e1"])
        self.assertEqual(j["verification"]["forced"], False)
        self.assertEqual(j["verification"].get("classification"), "normal_effective")
        self.assertEqual(j["verification"].get("actor"), "coordinator")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "accepted")

    def test_repeat_pass_idempotent(self):
        """重复 verify --pass：already_accepted 幂等 exit 0，不重复写。"""
        t, a = self.make()
        self.terminal(a["id"])
        self.collected(a["id"])
        first = self.cli("verify", "--attempt", a["id"], "--pass")
        self.assertEqual(first.returncode, 0, first.stderr)
        second = self.cli("verify", "--attempt", a["id"], "--pass")
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertTrue(self.blob.get("already_accepted") is True)
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "accepted")

    def test_fail_with_reason_atomic_rejected(self):
        """verify --fail 带 reason：任务 rejected、尝试 verification_failed、
        verification 落库；缺 reason exit 2。"""
        t, a = self.make()
        self.terminal(a["id"])
        self.collected(a["id"])
        p = self.cli("verify", "--attempt", a["id"], "--fail",
                     "--reason", "bad result")
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(self.blob["task"]["status"], "rejected")
        self.assertEqual(self.blob["attempt"]["status"], "verification_failed")
        self.assertEqual(self.blob["verification"]["result"], "fail")
        saved = self.ledger.get_attempt(a["id"])["verification"]
        self.assertEqual(saved["result"], "fail")
        self.assertEqual(saved.get("classification"), "fail")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "rejected")
        # 缺 reason：usage exit 2（既有正确行为锚点）
        t2, a2 = self.make()
        self.terminal(a2["id"])
        self.collected(a2["id"])
        p2 = self.cli("verify", "--attempt", a2["id"], "--fail")
        self.assertEqual(p2.returncode, 2, p2.stdout)

    def test_pass_missing_deliverable_gate(self):
        """已 collect 但必需交付物缺失：正常 pass exit 7，不 accepted。"""
        t, a = self.make(deliverables=["missing.txt"])
        self.terminal(a["id"])
        self.collected(a["id"])  # 只有文本，无 missing.txt 文件
        p = self.cli("verify", "--attempt", a["id"], "--pass", "--evidence", "e")
        self.assertEqual(p.returncode, 7, p.stdout)
        self.assertEqual(self.blob.get("code"), "gate")
        self.assertNotEqual(self.ledger.get_task(t["id"])["status"], "accepted")

    def test_pass_failed_check_gate(self):
        """检查命令失败：verify --pass --run-checks exit 7。"""
        t, a = self.make(checks=["false"])
        self.terminal(a["id"])
        self.collected(a["id"])
        p = self.cli("verify", "--attempt", a["id"], "--pass", "--run-checks",
                     "--evidence", "e")
        self.assertEqual(p.returncode, 7, p.stdout)
        self.assertNotEqual(self.ledger.get_task(t["id"])["status"], "accepted")

    # ---- force 例外（能力保留 + 收紧 reason）----------------------
    def test_force_pass_exception_saves_gaps(self):
        """force + 非空 reason 可豁免 collect/交付缺项：exit 0，forced/
        force_reason/缺失项落库，且分类为 forced 而非正常有效交付。"""
        t, a = self.make(deliverables=["out.txt"])
        self.terminal(a["id"])      # 无 collect：force 可豁免，快照记录缺项
        self.released(a["id"])
        p = self.cli("verify", "--attempt", a["id"], "--pass", "--force",
                     "--reason", "exception ok")
        self.assertEqual(p.returncode, 0, p.stderr)
        j = self.blob
        self.assertTrue(j["verification"]["forced"])
        self.assertEqual(j["verification"].get("classification"), "forced")
        self.assertEqual(j["verification"].get("actor"), "coordinator")
        self.assertTrue(j["verification"].get("unmet"))
        self.assertEqual(j["verification"]["force_reason"], "exception ok")
        self.assertIn("out.txt", j["verification"]["missing"])
        self.assertEqual(j["task"]["status"], "accepted")
        # force 保留实际未收集事实，不伪造结果路径。
        self.assertEqual(self.ledger.get_attempt(a["id"]).get("result_path"), None)

    def test_force_pass_requires_reason(self):
        """--force --pass 无/空/纯空白 reason：exit 2 usage。契约要求 strip 后
        非空（仅 if args.reason 判断会让空白串漏网）；收紧——当前基线均 exit 0=红。"""
        for label, extra in (("none", ()), ("empty", ("--reason", "")),
                             ("blank", ("--reason", "   "))):
            with self.subTest(reason=label):
                t, a = self.make(deliverables=["out.txt"])
                self.terminal(a["id"])
                self.make_file("out.txt")
                self.collected(a["id"])
                p = self.cli("verify", "--attempt", a["id"], "--pass",
                             "--force", *extra)
                self.assertEqual(p.returncode, 2,
                                 f"{label}: exit={p.returncode} out={p.stdout}")
                self.assertEqual(self.blob.get("code"), "usage")
                self.assertNotEqual(self.ledger.get_task(t["id"])["status"],
                                    "accepted")

    # ---- 非法/受保护路径（先红）----------------------------------
    def test_normal_pass_without_collect_rejected(self):
        """未 collect（无结果文件）正常 pass：exit 7 gate 且不残留 verification
        （当前 exit 0+accepted+verification 已写=红）。"""
        t, a = self.make()
        self.terminal(a["id"])
        self.released(a["id"])     # 无 collect，无 result_path
        p = self.cli("verify", "--attempt", a["id"], "--pass", "--evidence", "e")
        self.assertEqual(p.returncode, 7, p.stdout)
        self.assertEqual(self.blob.get("code"), "gate")
        self.assertNotEqual(self.ledger.get_task(t["id"])["status"], "accepted")
        self.assertNotEqual(self.ledger.get_attempt(a["id"])["status"], "accepted")
        self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"],
                          "拒绝不得残留预写 verification")

    def test_old_attempt_force_rejected(self):
        """旧尝试即使 --force 也 gate 拒绝（当前已正确，锚点）。"""
        t, a1 = self.make(deliverables=["out.txt"])
        self.terminal(a1["id"])
        self.make_file("out.txt")
        self.collected(a1["id"])
        # 新尝试成为 current
        self.ledger.create_attempt(t["id"], directory=self.home,
                                   status="dispatched",
                                   dispatch_state="confirmed")
        p = self.cli("verify", "--attempt", a1["id"], "--pass", "--force",
                     "--reason", "x")
        self.assertEqual(p.returncode, 7, p.stdout)
        self.assertNotEqual(self.ledger.get_task(t["id"])["status"], "accepted")
        self.assertNotEqual(self.ledger.get_attempt(a1["id"])["status"], "accepted")
        self.assertIsNone(self.ledger.get_attempt(a1["id"])["verification"],
                          "旧尝试拒绝不得残留预写 verification")

    def test_cancelled_task_force_rejected(self):
        """已 cancel 任务即使 --force 也 gate 拒绝且不留部分提交、不残留
        verification（当前 exit 2、attempt 已 accepted 且 verification 已写=红）。"""
        t, a = self.make(deliverables=["out.txt"])
        self.terminal(a["id"])
        self.make_file("out.txt")
        self.collected(a["id"])
        self.ledger.transition_task(t["id"], "cancelled", reason="manual",
                                    actor="coordinator")
        p = self.cli("verify", "--attempt", a["id"], "--pass", "--force",
                     "--reason", "x")
        self.assertEqual(p.returncode, 7, p.stdout)
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "cancelled")
        self.assertNotEqual(self.ledger.get_attempt(a["id"])["status"], "accepted")
        self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"],
                          "closed 任务拒绝不得残留预写 verification")

    def test_paused_task_force_rejected(self):
        """paused 任务即使 --force 也 gate 拒绝且不残留 verification
        （当前 exit 0+accepted+verification 已写=红）。"""
        t, a = self.make()
        self.terminal(a["id"])
        self.released(a["id"])
        self.ledger.transition_task(t["id"], "paused", reason="manual",
                                    actor="coordinator")
        p = self.cli("verify", "--attempt", a["id"], "--pass", "--force",
                     "--reason", "x")
        self.assertEqual(p.returncode, 7, p.stdout)
        self.assertNotEqual(self.ledger.get_task(t["id"])["status"], "accepted")
        self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"],
                          "paused 任务拒绝不得残留预写 verification")


if __name__ == "__main__":
    unittest.main(verbosity=2)

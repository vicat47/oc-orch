#!/usr/bin/env python3
"""F2 验收门禁回归（OpenSpec modularize-orchestrator 1.2/2.2/2.3）。

- 旧 API 直接把 attempt/task 置 accepted / 伪造 verification 的路径必须被拒绝
  （开头 10 个旧红 subTest；生产收紧后转绿）。
- 统一验收入口：results.prepare_verification / verify_attempt、
  Ledger.submit_verification / verification_snapshot。
- 测试 hash、源 hash、运行退出码与快照由文件外 manifest 维护，不写本文件。
- 原子提交、外部证据快照和人工控制边界均以行为断言固封。
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.db import init
from oc_orch.ledger import Ledger, LedgerError
from oc_orch.results import collect_attempt, prepare_verification, verify_attempt
from oc_orch.util import now_ms

FORGED_VERIFICATION = {
    "verified_at": 1, "result": "pass", "evidence": [], "notes": "",
    "forced": False, "force_reason": None,
    "missing": [], "unverified": [], "deliverables": [],
}

_RESULT_CFG = {"result_max_messages": 50, "result_store_limit": 20000}


class _FakeClient:
    """收集验收契约的最小 V2 形状 fake（user→assistant→idle 消息）。"""

    def __init__(self, messages=None):
        self._messages = messages or []

    def messages(self, session_id, *, limit, order):
        return self._messages[:limit]

    def get_message(self, session_id, message_id):
        for m in self._messages:
            if m.get("id") == message_id:
                return m
        return None

    def message_page(self, session_id, *, limit=50, order="asc", cursor=None):
        def key(m):
            t = m.get("time") or {}
            return (int(t.get("created") or 0), str(m.get("id") or ""))
        msgs = sorted(self._messages, key=key)
        if cursor:
            parts = cursor.split(":", 2)
            if parts[0] == "desc":
                msgs = [m for m in msgs if key(m) < (int(parts[1]), parts[2])]
                msgs.reverse()
            else:
                msgs = [m for m in msgs if key(m) > (int(parts[1]), parts[2])]
        elif order == "desc":
            msgs = list(reversed(msgs))
        data = msgs[:limit]
        nxt = None
        if data and len(msgs) > len(data):
            last = data[-1]
            direction = "desc" if (cursor and cursor.startswith("desc:")) or (
                not cursor and order == "desc") else "asc"
            nxt = f"{direction}:{key(last)[0]}:{key(last)[1]}"
        return {"data": list(data), "cursor": {"next": nxt, "previous": None}}

    def inbox(self, session_id):
        return []

    def deliverable_scan_cap(self):
        return None


def _snippet(text="任务完成"):
    return {"type": "assistant", "time": {"created": now_ms()},
            "content": [{"type": "text", "text": text}]}


def _identities(seq: int) -> tuple[str, str]:
    """每代夹具独立的输入/完成消息 id（按 site 序号推导，稳定可追溯）。"""
    return f"f2-in-{seq}", f"f2-done-{seq}"


class VerificationTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="oc-orch-f2-")
        self.addCleanup(self._tmp.cleanup)
        self.home = self._tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)   # LIFO：先关连接，再删目录
        self.ledger = Ledger(self.conn, self.home)
        self._site_seq = 0

    # ---- 夹具 -----------------------------------------------------
    def _dir(self):
        self._site_seq += 1
        d = os.path.join(self.home, "site", str(self._site_seq))
        os.makedirs(d, exist_ok=True)
        # 本代夹具的输入/完成消息身份（legacy 观察防串扰）
        self._ident = _identities(self._site_seq)
        self._msg_id = self._ident[0]
        return d

    def _task(self):
        return self.ledger.create_task(project="p", title="f2")

    def _attempt(self, task, *, d, **kw):
        base = dict(directory=d, write_scope=[d], status="dispatched",
                    dispatch_state="confirmed", session_id="ses-f2")
        base.update(kw)
        return self.ledger.create_attempt(task["id"], **base)

    def _succeeded(self, *, deliverables=("out.txt",), files=(), text="任务完成"):
        """合法证据入口夹具：sealed 基线 + started + record_terminal 真实区间。

        交付文件在封存基线**之前**写入并作为基线内容（baseline=v0），再由
        _collected 修改文件（v1）形成 new/changed 来源；终态经 record_terminal
        提交真实执行区间（started→ended），不再用通用 transition 伪造成功。
        """
        d = self._dir()
        for name in files:
            with open(os.path.join(d, name), "w", encoding="utf-8") as fh:
                fh.write("baseline")
        t = self._task()
        a = self._attempt(t, d=d, deliverables=list(deliverables),
                          dispatch_msg_id=self._msg_id)
        # 派单前封存基线（D5）：before-baseline 内容 = 现场初始内容
        from oc_orch.adapters.results import scan_deliverable_baseline
        ref = scan_deliverable_baseline(self.ledger, a["id"],
                                        captured_at=now_ms())
        self._last_baseline = ref
        self.ledger.seal_execution(a["id"], mode="new", session_before={},
                                   baseline=ref)
        # 真实执行：started → 终态（record_terminal，真实区间）
        in_id, done_id = self._ident
        started = now_ms()
        self._fixture_messages = [
            {"id": in_id, "type": "user", "text": "go",
             "time": {"created": started}},
            {"id": done_id, "type": "assistant",
             "time": {"created": started + 1},
             "content": [{"type": "text", "text": text}]},
        ]
        self.ledger.record_execution_started(a["id"], started_at=started,
                                             event_id="evt-f2-start")
        out = self.ledger.record_terminal(
            a["id"], outcome="succeeded", source="sse",
            event_id="evt-f2-end",
            candidate={"outcome": "succeeded", "source": "sse",
                       "ended_at": started + 10, "session_id": a["session_id"],
                       "input_in_inbox": False})
        assert out["result"] == "accepted", out
        return t, self.ledger.get_attempt(a["id"])

    def _collected(self, *, deliverables=("out.txt",), files=("out.txt",), text="任务完成"):
        t, a = self._succeeded(deliverables=deliverables, files=files, text=text)
        if files:
            # 现场内容相对基线变化 → new/changed（满足来源门；不触发 STALE）
            for name in files:
                with open(os.path.join(a["directory"], name), "w",
                          encoding="utf-8") as fh:
                    fh.write("collected-v1")
        info = collect_attempt(self.ledger, _FakeClient(self._fixture_messages),
                               a["id"], _RESULT_CFG)
        self.assertEqual(info["status_after"], "result_collected", info)
        return t, self.ledger.get_attempt(a["id"])

    def _clean(self, task_id, attempt_id):
        att = self.ledger.get_attempt(attempt_id)
        task = self.ledger.get_task(task_id)
        return (att["status"], att["verification"], att["result_path"], att["result_collected_at"],
                task["status"], task["current_attempt_id"], task["accepted_at"])

    def _verify_audit(self, task_id, attempt_id):
        return self.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action LIKE 'verify.%' AND "
            "((entity='attempt' AND entity_id=?) OR (entity='task' AND entity_id=?))",
            (attempt_id, task_id)).fetchone()[0]

    # ---- 旧 API 绕过：必须拒绝且无副作用（10 个红基线） ------------
    def _snapshot(self, task_id, attempt_id):
        t = self.ledger.get_task(task_id)
        a = self.ledger.get_attempt(attempt_id)
        n = self.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE to_state='accepted' AND "
            "((entity='attempt' AND entity_id=?) OR (entity='task' AND entity_id=?))",
            (attempt_id, task_id),
        ).fetchone()[0]
        return {
            "attempt_status": a["status"],
            "task_status": t["status"],
            "current_attempt_id": t["current_attempt_id"],
            "task_accepted_at": t["accepted_at"],
            "verification": a["verification"],
            "result_path": a["result_path"],
            "accepted_audit": n,
        }

    def _assert_rejected_clean(self, task_id, attempt_id, op):
        before = self._snapshot(task_id, attempt_id)
        with self.assertRaises(LedgerError):
            op()
        self.assertEqual(self._snapshot(task_id, attempt_id), before,
                         "拒绝后不得留下 attempt/task/verification/审计副作用")
        self.ledger.set_meta("f2.conn_usable", "ok")
        self.assertEqual(self.ledger.get_meta("f2.conn_usable"), "ok")

    def test_attempt_transition_accepted_gated(self):
        for mode, kw in (
            ("plain", {}),
            ("force_no_reason", {"force": True}),
            ("force_with_reason", {"force": True, "reason": "manual-ok"}),
        ):
            with self.subTest(mode=mode):
                t = self._task()
                a = self._attempt(t, d=self._dir())
                self._assert_rejected_clean(
                    t["id"], a["id"],
                    lambda: self.ledger.transition_attempt(a["id"], "accepted", **kw))

    def test_task_transition_accepted_gated(self):
        for mode, kw in (("plain", {}), ("force_with_reason", {"force": True, "reason": "manual-ok"})):
            with self.subTest(mode=mode):
                t = self._task()
                a = self._attempt(t, d=self._dir())
                self._assert_rejected_clean(
                    t["id"], a["id"],
                    lambda: self.ledger.transition_task(t["id"], "accepted", **kw))

    def test_update_attempt_cannot_inject_accepted(self):
        for label, fields in (
            ("status_only", {"status": "accepted"}),
            ("status_plus_forged_verification",
             {"status": "accepted", "verification": dict(FORGED_VERIFICATION)}),
        ):
            with self.subTest(variant=label):
                t = self._task()
                a = self._attempt(t, d=self._dir())
                self._assert_rejected_clean(
                    t["id"], a["id"],
                    lambda: self.ledger.update_attempt(a["id"], fields=dict(fields)))

    def test_extra_cannot_inject_accepted_state(self):
        cases = (
            ("attempt_extra_status",
             lambda led, a, t: led.transition_attempt(
                 a["id"], "running", reason="x", extra={"status": "accepted"})),
            ("task_extra_status",
             lambda led, a, t: led.transition_task(
                 t["id"], "running", reason="x", extra={"status": "accepted"})),
            ("attempt_extra_forged_verification",
             lambda led, a, t: led.transition_attempt(
                 a["id"], "accepted", reason="x",
                 extra={"verification": dict(FORGED_VERIFICATION)})),
        )
        for label, op in cases:
            with self.subTest(case=label):
                t = self._task()
                a = self._attempt(t, d=self._dir())
                self._assert_rejected_clean(t["id"], a["id"],
                                            lambda: op(self.ledger, a, t))

    # ---- 新入口：正常 pass/fail 与前置条件 -------------------------
    def test_invalid_result_rejected(self):
        t, a = self._collected()
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a["id"], result="maybe")
        self.assertEqual(ctx.exception.code, "INVALID_RESULT")

    def test_normal_pass_requires_real_collect(self):
        t1, a1 = self._succeeded()
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a1["id"], result="pass")
        self.assertEqual(ctx.exception.code, "NOT_READY")
        t2, a2 = self._succeeded()
        self.ledger.update_attempt(a2["id"], fields={"status": "result_collected"}, actor="test")
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a2["id"], result="pass")
        self.assertEqual(ctx.exception.code, "NOT_READY")
        # 手工标记不改写任务状态（stay result_pending）；两者都不得进入 accepted
        self.assertEqual(self.ledger.get_task(t1["id"])["status"], "result_pending")
        self.assertEqual(self.ledger.get_task(t2["id"])["status"], "result_pending")
        self.assertNotEqual(self.ledger.get_task(t1["id"])["status"], "accepted")
        self.assertNotEqual(self.ledger.get_task(t2["id"])["status"], "accepted")

    def test_result_file_missing_or_identity_mismatch_rejected(self):
        t, a = self._collected()
        os.unlink(a["result_path"])
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a["id"], result="pass")
        self.assertEqual(ctx.exception.code, "NOT_READY")
        t2, a2 = self._collected()
        with open(a2["result_path"], "w", encoding="utf-8") as fh:
            json.dump({"schema": "oc-orch.result.v1x", "attempt_id": "A-other"}, fh)
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a2["id"], result="pass")
        self.assertEqual(ctx.exception.code, "NOT_READY")
        self.assertEqual(self.ledger.get_attempt(a2["id"])["status"], "result_collected")
        self.assertIsNone(self.ledger.get_attempt(a2["id"])["verification"])
        self.assertEqual(self.ledger.get_task(t2["id"])["status"], "result_received")

    def test_required_deliverable_missing_rejected(self):
        """T35/F5 新契约：收集后必需文件缺失/无摘要 → verify 要求重新收集
        （RESULT_CHANGED）或交付门拒绝（AUTH_GATE_FAIL）；二者都不得通过
        正常 pass 且不留下 accepted/verification 副作用。"""
        t, a = self._collected(deliverables=("out.txt", "report.md"), files=("out.txt",))
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a["id"], result="pass")
        self.assertIn(ctx.exception.code, ("AUTH_GATE_FAIL", "RESULT_CHANGED",
                                           "STALE_GATE_FAIL"))
        self.assertEqual(self._clean(t["id"], a["id"]),
                         ("result_collected", None, a["result_path"], a["result_collected_at"],
                          "result_received", a["id"], None))

    def test_declared_check_failed_or_unrun_rejected(self):
        spec = {"kind": "command", "target": "exit 1", "required": True}
        t, a = self._collected(deliverables=("out.txt", spec), files=("out.txt",))
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a["id"], result="pass")
        self.assertEqual(ctx.exception.code, "AUTH_GATE_FAIL")
        t2, a2 = self._collected(deliverables=("out.txt", dict(spec)), files=("out.txt",))
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a2["id"], result="pass",
                           run_checks=True, check_timeout=30)
        self.assertEqual(ctx.exception.code, "AUTH_GATE_FAIL")

    def test_normal_pass_and_fail(self):
        t, a = self._collected()
        out = verify_attempt(self.ledger, a["id"], result="pass", evidence=["e1"], notes="n")
        self.assertTrue(out["ok"])
        self.assertTrue(out["changed"])
        self.assertFalse(out["already"])
        self.assertEqual(out["result"], "pass")
        self.assertEqual(out["attempt"]["status"], "accepted")
        self.assertEqual(out["task"]["status"], "accepted")
        v = out["verification"]
        self.assertEqual(v["classification"], "normal_effective")
        self.assertFalse(v["forced"])
        self.assertIsNone(v["force_reason"])
        self.assertEqual(v["evidence"], ["e1"])
        self.assertEqual(v["notes"], "n")
        self.assertEqual(v["actor"], "coordinator")
        t2, a2 = self._collected()
        out2 = verify_attempt(self.ledger, a2["id"], result="fail", reason="与交付物不符")
        self.assertTrue(out2["ok"])
        self.assertTrue(out2["changed"])
        self.assertEqual(out2["attempt"]["status"], "verification_failed")
        self.assertEqual(out2["task"]["status"], "rejected")
        self.assertEqual(out2["verification"]["classification"], "fail")

    def test_repeat_pass_idempotent_keeps_evidence(self):
        t, a = self._collected()
        verify_attempt(self.ledger, a["id"], result="pass", notes="first")
        n1 = self._verify_audit(t["id"], a["id"])
        att1 = self.ledger.get_attempt(a["id"])["verification"]
        out = verify_attempt(self.ledger, a["id"], result="pass", notes="ignored")
        self.assertTrue(out["already"])
        self.assertFalse(out["changed"])
        self.assertEqual(out["verification"], att1)
        self.assertEqual(self._verify_audit(t["id"], a["id"]), n1)

    def test_fail_reason_required(self):
        t, a = self._collected()
        for bad in ("", "   "):
            with self.subTest(reason=repr(bad)):
                with self.assertRaises(LedgerError) as ctx:
                    verify_attempt(self.ledger, a["id"], result="fail", reason=bad)
                self.assertEqual(ctx.exception.code, "REASON_REQUIRED")

    # ---- force 语义 -------------------------------------------------
    def test_force_pass_requires_reason(self):
        t, a = self._collected()
        for bad in ("", "   "):
            with self.subTest(reason=repr(bad)):
                with self.assertRaises(LedgerError) as ctx:
                    verify_attempt(self.ledger, a["id"], result="pass", force=True, reason=bad)
                self.assertEqual(ctx.exception.code, "FORCE_REASON_REQUIRED")

    def test_force_pass_records_reason_unmet_actor(self):
        # T35/F5 新契约：verify 前事务外复核声明产物，缺失文件先被
        # RESULT_CHANGED 要求重新收集；本测试焦点是"force 必须有因并完整
        # 留痕"——用真实不满足项（未执行的必需 command 检查）保持 unmet
        # 非空断言，缺失文件场景由 test_required_deliverable_missing_rejected
        # 的 RESULT_CHANGED 断言覆盖。
        command = {"kind": "command", "target": "printf check", "required": True}
        t, a = self._collected(deliverables=("out.txt", command),
                               files=("out.txt",))
        out = verify_attempt(self.ledger, a["id"], result="pass", force=True,
                             reason="人工核实未执行检查可接受",
                             actor="main-coordinator")
        self.assertTrue(out["ok"])
        self.assertTrue(out["changed"])
        v = out["verification"]
        self.assertEqual(v["classification"], "forced")
        self.assertTrue(v["forced"])
        self.assertEqual(v["force_reason"], "人工核实未执行检查可接受")
        self.assertEqual(v["actor"], "main-coordinator")
        self.assertTrue(v.get("unmet"), "force 须持久化未满足项")
        self.assertIn("printf check", json.dumps(v["unmet"]))
        self.assertEqual(self.ledger.get_attempt(a["id"])["status"], "accepted")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "accepted")
        # 收集后缺失文件的 force 前置：先被 RESULT_CHANGED 要求重新收集
        t2, a2 = self._collected(deliverables=("out.txt", "report.md"),
                                 files=("out.txt",))
        with self.assertRaises(LedgerError) as ctx:
            prepare_verification(self.ledger, a2["id"], result="pass", force=True,
                                 reason="x")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")
        self.assertIsNone(self.ledger.get_attempt(a2["id"])["verification"])

    def test_active_shells_block_normal_and_force_marks(self):
        t, a = self._collected()
        att = self.ledger.get_attempt(a["id"])
        meta = dict(att["metadata"])
        meta["active_shells"] = {"sh-x": {"status": "running"}}
        self.ledger.update_attempt(a["id"], fields={"metadata": meta})
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a["id"], result="pass")
        self.assertEqual(ctx.exception.code, "SIDE_EFFECTS_ACTIVE")
        out = verify_attempt(self.ledger, a["id"], result="pass", force=True,
                             reason="人工确认残留 shell 无副作用")
        v = out["verification"]
        self.assertTrue(v["forced"])
        self.assertEqual(v["classification"], "forced")
        self.assertIn("active_shells", json.dumps(v))
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "accepted")

    # ---- 归属 / 人工控制保护（force 不豁免）------------------------
    def test_cancel_pause_protected_even_force(self):
        t, a = self._collected()
        prepared = prepare_verification(self.ledger, a["id"], result="pass", force=True, reason="x")
        self.ledger.close_task(t["id"], reason="user-cancel")
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.submit_verification(a["id"], verification=prepared["prepared"],
                                            result="pass", force=True, reason="x")
        self.assertEqual(ctx.exception.code, "TASK_PROTECTED")
        t2, a2 = self._collected()
        prepared2 = prepare_verification(self.ledger, a2["id"], result="pass", force=True, reason="x")
        self.ledger.transition_task(t2["id"], "paused", reason="manual", actor="coordinator")
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.submit_verification(a2["id"], verification=prepared2["prepared"],
                                            result="pass", force=True, reason="x")
        self.assertEqual(ctx.exception.code, "TASK_PROTECTED")

    def test_old_generation_protected_even_force(self):
        t, a = self._collected()
        prepared = prepare_verification(self.ledger, a["id"], result="pass", force=True, reason="x")
        c2 = init(self.home)
        try:
            led2 = Ledger(c2, self.home)
            led2.release_site(a["id"], confirmed=True, external_stopped=True, actor="test")
            d2 = os.path.join(self.home, "site-2")
            os.makedirs(d2, exist_ok=True)
            led2.create_attempt(t["id"], directory=d2, write_scope=[d2], status="running",
                                dispatch_state="confirmed", session_id="ses-2")
        finally:
            c2.close()
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.submit_verification(a["id"], verification=prepared["prepared"],
                                            result="pass", force=True, reason="x")
        self.assertEqual(ctx.exception.code, "NOT_CURRENT")

    def test_fail_on_accepted_protected(self):
        t, a = self._collected()
        verify_attempt(self.ledger, a["id"], result="pass")
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a["id"], result="fail", reason="迟到质疑")
        self.assertEqual(ctx.exception.code, "TASK_PROTECTED")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "accepted")

    # ---- 事务原子性 / 并发指纹（确定性交错，无概率 sleep）----------
    def test_mid_submit_rollback(self):
        for kind, ddl in (("audit_insert", "BEFORE INSERT ON audit"),
                          ("task_status", "BEFORE UPDATE OF status ON tasks")):
            with self.subTest(trigger=kind):
                t, a = self._collected()
                # 准备阶段允许刷新证据；故障必须发生在 submit 的验收事务内。
                prepared = prepare_verification(self.ledger, a["id"], result="pass", evidence=["x"])
                audit_before = self.conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0]
                trig = "f2_boom_" + kind
                self.conn.execute(
                    f"CREATE TEMP TRIGGER {trig} {ddl} "
                    f"BEGIN SELECT RAISE(ABORT, 'boom-{kind}'); END")
                try:
                    with self.assertRaises(Exception) as ctx:
                        self.ledger.submit_verification(
                            a["id"], verification=prepared["prepared"], result="pass")
                    self.assertIn(f"boom-{kind}", str(ctx.exception))
                finally:
                    self.conn.execute(f"DROP TRIGGER IF EXISTS {trig}")
                self.assertEqual(self.ledger.get_attempt(a["id"])["status"], "result_collected")
                self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"])
                self.assertEqual(self.ledger.get_task(t["id"])["status"], "result_received")
                self.assertIsNone(self.ledger.get_task(t["id"])["accepted_at"])
                self.assertEqual(self._verify_audit(t["id"], a["id"]), 0)
                self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0],
                                 audit_before)
                self.ledger.set_meta("f2.tx.usable", "ok")
                self.assertEqual(self.ledger.get_meta("f2.tx.usable"), "ok")

    def test_race_evidence_change_rejected(self):
        t, a = self._collected()
        prepared = prepare_verification(self.ledger, a["id"], result="pass")
        c2 = init(self.home)
        try:
            led2 = Ledger(c2, self.home)
            d = led2.deliverables(a["id"])[0]
            led2.update_deliverable(d["id"], status="failed", evidence={"tampered": True})
        finally:
            c2.close()
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.submit_verification(a["id"], verification=prepared["prepared"],
                                            result="pass")
        self.assertEqual(ctx.exception.code, "EVIDENCE_CHANGED")
        self.assertEqual(self.ledger.get_attempt(a["id"])["status"], "result_collected")
        self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"])

    def test_race_cancel_rejected(self):
        t, a = self._collected()
        prepared = prepare_verification(self.ledger, a["id"], result="pass")
        c2 = init(self.home)
        try:
            Ledger(c2, self.home).close_task(t["id"], reason="user-cancel")
        finally:
            c2.close()
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.submit_verification(a["id"], verification=prepared["prepared"],
                                            result="pass")
        self.assertEqual(ctx.exception.code, "TASK_PROTECTED")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "cancelled")

    # ---- 直接 submit 不可伪造（DB required 是权威，不信任载荷）-----
    def test_direct_submit_cannot_forge(self):
        t, a = self._collected()
        with self.assertRaises(LedgerError):
            self.ledger.submit_verification(a["id"], verification={"missing": [], "unverified": []},
                                            result="pass")
        t2, a2 = self._collected(deliverables=("out.txt", "report.md"),
                                 files=("out.txt", "report.md"))
        forged = prepare_verification(self.ledger, a2["id"], result="pass",
                                      force=True, reason="仅准备快照")["prepared"]
        forged["guard"]["fingerprint"] = "deadbeef"
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.submit_verification(
                a2["id"], verification=forged, result="pass")
        self.assertEqual(ctx.exception.code, "EVIDENCE_CHANGED")
        t3, a3 = self._collected(
            deliverables=("out.txt", "report.md",
                          {"kind": "command", "target": "exit 1", "required": True}),
            files=("out.txt", "report.md"))
        forged = prepare_verification(self.ledger, a3["id"], result="pass",
                                      force=True, reason="仅准备快照")["prepared"]
        # 保留真实 guard/结果文件快照，只对门禁清单撒谎（真实 command
        # 检查未通过/未执行，谎称全部满足）。
        forged.update(missing=[], unverified=[], unmet=[], forced=False,
                      classification="normal_effective", force_reason=None)
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.submit_verification(
                a3["id"], verification=forged, result="pass")
        self.assertEqual(ctx.exception.code, "AUTH_GATE_FAIL")
        self.assertEqual(self.ledger.get_attempt(a3["id"])["status"], "result_collected")
        self.assertIsNone(self.ledger.get_attempt(a3["id"])["verification"])

    def test_submit_requires_guard_and_complete_result_snapshot(self):
        for change, code in (("guard", "EVIDENCE_CHANGED"),
                             ("result_snapshot", "NOT_READY")):
            with self.subTest(change=change):
                t, a = self._collected()
                payload = prepare_verification(self.ledger, a["id"], result="pass")["prepared"]
                if change == "guard":
                    payload.pop("guard")
                else:
                    payload["result_snapshot"] = {"path": a["result_path"], "exists": True}
                before = self._clean(t["id"], a["id"])
                with self.assertRaises(LedgerError) as ctx:
                    self.ledger.submit_verification(a["id"], verification=payload, result="pass")
                self.assertEqual(ctx.exception.code, code)
                self.assertEqual(self._clean(t["id"], a["id"]), before)

    def test_submit_rechecks_active_children_for_pass_and_fail(self):
        for result in ("pass", "fail"):
            with self.subTest(result=result):
                t, a = self._collected()
                a_meta = a["metadata"] or {}
                self.ledger.update_attempt(
                    a["id"], fields={"metadata": {**a_meta,
                                                  "active_children": ["ses-child"]}})
                payload = prepare_verification(self.ledger, a["id"], result=result,
                                               force=True, reason="准备例外")["prepared"]
                with self.assertRaises(LedgerError) as ctx:
                    self.ledger.submit_verification(a["id"], verification=payload,
                                                    result=result, reason="人工结论")
                self.assertEqual(ctx.exception.code, "SIDE_EFFECTS_ACTIVE")
                self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"])
                forced = self.ledger.submit_verification(a["id"], verification=payload,
                                                         result=result, force=True, reason="人工结论")
                self.assertEqual(forced["verification"]["force_reason"], "人工结论")
                self.assertIn({"code": "side_effects", "active_children": ["ses-child"]},
                              forced["verification"]["unmet"])

    def test_protected_tasks_do_not_execute_declared_checks(self):
        for control in ("cancelled", "paused", "closed", "old"):
            with self.subTest(control=control):
                command = {"kind": "command", "target": "printf checked > marker.txt", "required": True}
                t, a = self._collected(deliverables=(command,), files=())
                if control == "cancelled":
                    self.ledger.close_task(t["id"], reason="用户取消")
                elif control == "paused":
                    self.ledger.transition_task(t["id"], "paused")
                elif control == "closed":
                    # 历史不一致状态：status 已变但 closed_at 保留，仍须保护。
                    self.conn.execute("UPDATE tasks SET closed_at=1 WHERE id=?", (t["id"],))
                else:
                    self.ledger.release_site(a["id"], confirmed=True, external_stopped=True, actor="test")
                    self._attempt(t, d=self._dir())
                with self.assertRaises(LedgerError) as ctx:
                    verify_attempt(self.ledger, a["id"], result="pass", force=True,
                                   reason="不能覆盖控制意图", run_checks=True)
                self.assertEqual(ctx.exception.code, "NOT_CURRENT" if control == "old" else "TASK_PROTECTED")
                self.assertFalse(os.path.exists(os.path.join(a["directory"], "marker.txt")))
                self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"])

    def test_prepare_uses_same_attempt_as_guard(self):
        t, a = self._collected()
        snapshot = self.ledger.verification_snapshot
        def changed_snapshot(aid):
            c2 = init(self.home)
            try:
                c2.execute("UPDATE attempts SET generation=generation+1 WHERE id=?", (aid,))
            finally:
                c2.close()
            return snapshot(aid)
        with mock.patch.object(self.ledger, "verification_snapshot", side_effect=changed_snapshot):
            with self.assertRaises(LedgerError) as ctx:
                verify_attempt(self.ledger, a["id"], result="pass")
        self.assertEqual(ctx.exception.code, "NOT_READY")
        self.assertIsNone(self.ledger.get_attempt(a["id"])["verification"])

    def test_result_hash_matches_bytes_read_before_file_replacement(self):
        t, a = self._collected()
        original_open = open
        with original_open(a["result_path"], "rb") as f:
            original = f.read()
        @contextmanager
        def replacing_open(path, mode="r", *args, **kwargs):
            with original_open(path, mode, *args, **kwargs) as f:
                yield f
            if path == a["result_path"] and mode == "rb":
                with original_open(path, "wb") as f:
                    f.write(b'{"changed_after_read":true}')
        with mock.patch("builtins.open", replacing_open):
            out = verify_attempt(self.ledger, a["id"], result="pass")
        rs = out["verification"]["result_snapshot"]
        self.assertEqual(rs["sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(rs["size"], len(original))

    def test_pass_persists_declared_check_evidence(self):
        command = {"kind": "command", "target": "printf verified", "required": True}
        t, a = self._collected(deliverables=(command,), files=())
        out = verify_attempt(self.ledger, a["id"], result="pass", run_checks=True)
        observed = self.ledger.deliverables(a["id"])[0]
        saved = out["verification"]["deliverables"][0]
        self.assertEqual(saved.get("evidence"), json.loads(observed["evidence"]))
        self.assertEqual(saved.get("checked_at"), observed["checked_at"])
        self.assertEqual(out["verification"]["result_snapshot"]["attempt_id"], a["id"])

    def test_result_missing_with_real_collection_can_pass(self):
        """空收集不占正式标记（新契约）：预算耗尽 → result_missing；正常 pass
        被拒；真实补收集后才能 normal_effective（T24/Proposal BREAKING）。

        F3 口径：空收集也必须能读到输入锚点（窗口可见但本轮无 assistant
        实质输出），覆盖不足的窗口不作为“零条完整结果”计数。"""
        t, a = self._succeeded(deliverables=())
        # 空收集：只保留输入锚点（user 消息可见），本轮无 assistant 实质输出
        empty_window = _FakeClient(
            [m for m in self._fixture_messages if m.get("type") != "assistant"])
        for _ in range(5):  # 补收集预算（默认 5 次）耗尽
            collect_attempt(self.ledger, empty_window, a["id"], _RESULT_CFG)
        self.assertEqual(self.ledger.get_attempt(a["id"])["status"],
                         "result_missing")
        self.assertIsNone(self.ledger.get_attempt(a["id"])["result_collected_at"],
                          "空结果不得占用正式收集完成标记")
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, a["id"], result="pass")
        self.assertEqual(ctx.exception.code, "NOT_READY")
        # 真实证据补收集：形成有效结果后正常验收
        # （补回报挂在真实 execution epoch 内，而非可能更早的 attempt.started_at；
        #  V2 消息必有非空 id，_snippet 默认无 id 需补上）
        in_id, done_id = self._ident
        epoch_start = a["metadata"]["execution"]["epoch"]["started_at"]
        filled = _FakeClient(
            [m for m in self._fixture_messages if m.get("type") != "assistant"]
            + [dict(_snippet("补齐结果"), id=f"{done_id}-late",
                    time={"created": epoch_start + 5})])
        collect_attempt(self.ledger, filled, a["id"], _RESULT_CFG)
        self.assertEqual(self.ledger.get_attempt(a["id"])["status"],
                         "result_collected")
        self.assertEqual(verify_attempt(self.ledger, a["id"], result="pass")
                         ["verification"]["classification"], "normal_effective")

    def test_accepted_late_event_noop_and_force_cannot_reopen(self):
        t, a = self._collected()
        verify_attempt(self.ledger, a["id"], result="pass")
        before = self._clean(t["id"], a["id"])
        self.assertEqual(self.ledger.transition_attempt(a["id"], "running")["status"], "accepted")
        with self.assertRaises(LedgerError):
            self.ledger.transition_attempt(a["id"], "running", force=True, reason="伪造重开")
        with self.assertRaises(LedgerError):
            self.ledger.update_attempt(a["id"], fields={"status": "running"})
        self.assertEqual(self._clean(t["id"], a["id"]), before)

    def test_failure_state_also_requires_atomic_verification(self):
        t, a = self._collected()
        for op in (
            lambda: self.ledger.transition_attempt(a["id"], "verification_failed"),
            lambda: self.ledger.update_attempt(a["id"], fields={"status": "verification_failed"}),
            lambda: self.ledger.transition_task(t["id"], "rejected"),
        ):
            self._assert_rejected_clean(t["id"], a["id"], op)
        # 已有失败验收仍能合法 pause/resume，不重新生成验收记录。
        verify_attempt(self.ledger, a["id"], result="fail", reason="需要返工")
        self.ledger.transition_task(t["id"], "paused")
        self.assertEqual(self.ledger.transition_task(t["id"], "rejected")["status"], "rejected")

    def test_create_attempt_cannot_predeclare_verification(self):
        for status in ("accepted", "rejected", "verification_failed"):
            with self.subTest(status=status):
                t = self._task()
                with self.assertRaises(LedgerError) as ctx:
                    self._attempt(t, d=self._dir(), status=status)
                self.assertEqual(ctx.exception.code, "VERIFY_REQUIRED")
                self.assertEqual(self.ledger.list_attempts(task_id=t["id"]), [])
                self.assertIsNone(self.ledger.get_task(t["id"])["current_attempt_id"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

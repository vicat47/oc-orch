#!/usr/bin/env python3
"""OpenSpec fix-attempt-completion-integrity：P0 事故闭环（T01–T05、T17、T29 等）。

复用 A-a4afbaf046da 的事故毫秒链与 A-63c27e61d537 / A-4b6d285726a4 时间样本，用
FakeClient + FakeClock + 真临时 SQLite 固化"新 started 已到 + REST 旧 succeeded/idle +
旧交付 + 提前释放 + 空收集 + 早产 result_ready/ACK + 晚到真实完成"。

断言覆盖状态、写权、turn 与通知整体，而不是只断言其中一项；pre-fix 期望全部红。

事实来源：openspec/changes/fix-attempt-completion-integrity/evidence.md（生产台账
只读快照，时间值为 epoch ms）。不调用真实服务、不使用概率 sleep。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.composition import application
from oc_orch.config import DEFAULTS
from oc_orch.domain.errors import ActionError, TransportError
from oc_orch.ledger import LedgerError
from oc_orch.supervisor import Supervisor
from unittest.mock import patch

from tests.fakes import FakeClient, FakeClock

# ---- 生产事故时间样本（epoch ms，见 evidence.md §2/§4） -----------------------
# (名称, 旧回合 idle/ended, 新回合 started)
INCIDENT_SAMPLES = [
    ("A-a4", 1794007219265, 1794008072447),
    ("A-63c", 1794004000739, 1794004397804),
    ("A-4b6", 1793992914318, 1793996193595),
    # T04：旧 idle 落在新尝试两秒容差内且与冻结基线相同——时间容差不能覆盖回合身份
    ("T04-2s容差", None, None),
]


def _clock_time_for(sample):
    name, old_idle, new_started = sample
    if old_idle is None:  # T04：old_idle = new_started - 1500ms（2 秒容差内）
        new_started = 1794008072447
        old_idle = new_started - 1500
    return name, old_idle, new_started


class IncidentBase(unittest.TestCase):
    """共享底座：真临时 SQLite + FakeClient + FakeClock + 应用用例装配。"""

    def _fresh(self):
        """每个样本独立世界（subTest 不重跑 setUp，需要在循环内显式重建）。"""
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-incident-")
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.clock = FakeClock(wall_ms=1_000_000)
        self.client = FakeClient()
        self.cfg = dict(DEFAULTS)
        self.cfg["auto_collect"] = True
        self.cfg["collect_retry_interval"] = 15
        self.cfg["collect_max_attempts"] = 5
        self.sup = Supervisor(self.tmp, cfg=self.cfg, client=self.client,
                              clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.app = application(self.ledger, lambda: self.client, self.cfg,
                               clock=self.clock)

    def setUp(self):
        self._fresh()

    # ---- 布置 -----------------------------------------------------
    def write_files(self, contents: dict[str, str], *, at_ms: int | None = None):
        paths = {}
        for rel, text in contents.items():
            path = os.path.join(self.tmp, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            if at_ms is not None:
                os.utime(path, (at_ms / 1000, at_ms / 1000))
            paths[rel] = path
        return paths

    def make_task(self, *, coordinator: str = "ses_c", files: list[str] | None = None):
        return self.ledger.create_task(
            project="p", title="事故任务", objective="目标",
            acceptance=["交付物完整"], coordinator_session=coordinator,
            write_scope=[self.tmp],
            metadata={"deliverables": files or []},
        )

    def start_event(self, sid: str, at: int, event_id: str | None = None):
        return self.sup.ingest_event({
            "id": event_id or f"evt-start-{at}",
            "type": "session.execution.started",
            "created": at,
            "data": {"sessionID": sid},
        })

    def terminal_event(self, sid: str, outcome: str, at: int, event_id: str):
        return self.sup.ingest_event({
            "id": event_id,
            "type": f"session.execution.{outcome}",
            "created": at,
            "data": {"sessionID": sid},
        })

    def completed_session(self, sid: str, *, outcome: str, idle: int, busy: int,
                          active: bool = False, updated: int | None = None):
        session = self.client.set_session(sid, outcome=outcome,
                                          time_={"idle": idle, "busy": busy,
                                                 "updated": updated or idle})
        if active:
            self.client.active[sid] = session
        else:
            self.client.active.pop(sid, None)
        return session

    def assistant_message(self, sid: str, msg_id: str, text: str, at: int,
                          *, tools: list[dict] | None = None):
        content = [{"type": "text", "text": text}] if text else []
        for t in tools or []:
            content.append({"type": "tool", "name": t.get("name", "bash"),
                            "executed": t.get("executed", True),
                            "state": {"status": t.get("status", "completed")}})
        self.client.append_message(sid, {
            "id": msg_id, "type": "assistant", "content": content,
            "time": {"created": at},
        })

    def notifications_for(self, attempt_id: str) -> list[dict]:
        rows = self.ledger.conn.execute(
            "SELECT * FROM notifications WHERE attempt_id=? ORDER BY created_at",
            (attempt_id,),
        ).fetchall()
        return [dict(r, payload=json.loads(r["payload"])) for r in rows]

    def audit_actions(self, entity_id: str) -> list[dict]:
        rows = self.ledger.conn.execute(
            "SELECT * FROM audit WHERE entity_id=? ORDER BY id", (entity_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def turns_of(self, attempt_id: str) -> list[dict]:
        return self.ledger.turns(attempt_id)

    # ---- 事故剧本 -------------------------------------------------
    def setup_reuse_attempt(self, *, old_idle: int, new_started: int,
                            files: dict[str, str], worker: str = "ses_w",
                            coordinator: str = "ses_c"):
        """上一回合真实完成 → rework 复用同 session → prompt 投递 → 新 started。

        返回 (task, old_att, new_att, msg_id)。时钟沿旧回合→新派单推进。
        """
        old_start = old_idle - 600_000
        self.clock._wall = old_start + 1_000  # 旧回合派单窗
        paths = self.write_files(files, at_ms=old_idle - 1_000)
        rels = sorted(files)
        task = self.make_task(coordinator=coordinator, files=rels)

        # 旧尝试：真实完成（started → REST succeeded/idle → 终态、释放现场）
        old_att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id=worker,
            status="creating", dispatch_state="confirmed", model="p/m",
            deliverables=rels, write_scope=[self.tmp],
        )
        self.start_event(worker, old_start, "evt-old-start")
        self.completed_session(worker, outcome="succeeded", idle=old_idle,
                               busy=old_start)
        self.sup.cycle(dispatch=False)
        old_att = self.ledger.get_attempt(old_att["id"])
        assert old_att["status"] in ("terminal_succeeded", "result_collected",
                                     "result_missing"), old_att["status"]
        assert old_att["site_released_at"], "旧尝试应已释放现场"

        # 同 session 返工（事故路径）：默认保留 worker session
        self.clock._wall = new_started - 60_000
        out = self.app.rework(task["id"], feedback="修订报告；其余沿用已有产物")
        new_att = self.ledger.get_attempt(out["attempt_id"])
        # 模拟服务端投递本轮输入并真正开始执行
        self.client.deliver_inbox(worker, at=new_started)
        self.start_event(worker, new_started, "evt-new-start")
        self.clock._wall = new_started + 122  # 事故里 REST 观察发生在 started 后 122ms
        return task, old_att, new_att, out["message_id"]

    # ---- 断言 -----------------------------------------------------
    def assertNoFalseTerminal(self, att_id: str, *, turns_before=None,
                              model: str | None = None):
        att = self.ledger.get_attempt(att_id)
        self.assertIsNone(att["terminal_at"], "不得写假 terminal_at")
        self.assertIsNone(att["result_collected_at"], "不得空收集写 result_collected_at")
        self.assertIsNone(att["site_released_at"], "假终态期间必须保持写权")
        self.assertNotIn(att["status"], (
            "terminal_succeeded", "terminal_failed", "terminal_interrupted",
            "result_collected", "result_missing", "accepted", "rejected",
        ), "假终态不得推进尝试状态")
        # 拒绝场景必须保持 turn 集合完全不变：错误关闭一个顺序正确的 turn 也会被抓
        if turns_before is not None:
            self.assertEqual(self.turns_of(att_id), turns_before,
                             "拒绝场景不得新建/关闭任何 turn（open 保持 open）")
        for t in self.turns_of(att_id):
            if t["ended_at"] is not None and t["started_at"] is not None:
                self.assertLess(t["started_at"], t["ended_at"],
                                "不得存在逆序关闭的 turn")
        for n in self.notifications_for(att_id):
            cls = n["payload"].get("classification") or ""
            self.assertNotIn(cls, (
                "RESULT_READY", "RESULT_MISSING", "DELIVERABLE_MISSING",
                "DELIVERABLE_STALE", "CHECKS_PENDING", "CHECKS_FAILED",
                "COLLECTION_REQUIRED", "COLLECT_FAILED",
                "EXECUTION_FAILED", "EXECUTION_INTERRUPTED",
            ), "拒绝场景不得产生就绪/执行误报类通知")
            if cls.startswith("EXECUTION_") and cls != "EXECUTION_UNCONFIRMED":
                self.fail(f"拒绝场景产生了未确认之外的执行类别通知：{cls!r}")
            if n["kind"].startswith("result") or n["kind"] in (
                    "execution_succeeded", "execution_failed", "execution_interrupted",
                    "result_collect_failed"):
                self.fail(f"拒绝场景产生了成功/结果类通知 kind={n['kind']}")
        if model:
            # 假终态期间不得清除 provider 失败/冷却状态（无成功清理）
            self.assertIsNotNone(self.ledger.provider_states().get(model),
                                 "拒绝场景不得清除 provider 失败/冷却状态")


class TestIncidentT02(IncidentBase):
    """T02/T04：新 started 已到 + REST 旧 outcome/idle —— 不得产生假终态链。"""

    def test_reuse_session_old_terminal_rejected(self):
        for sample in INCIDENT_SAMPLES:
            name, old_idle, new_started = _clock_time_for(sample)
            with self.subTest(sample=name):
                self._fresh()
                task, old_att, new_att, msg_id = self.setup_reuse_attempt(
                    old_idle=old_idle, new_started=new_started,
                    files={"report.md": "上一轮产物", "result.json": "{}"})
                # 假终态拒绝前：provider 冷却/失败状态 + turn 快照
                turns_before = self.turns_of(new_att["id"])
                self.ledger.record_provider_state(
                    "p/m", category="rate_limit", status=429,
                    until_ms=self.clock.now_ms() + 60_000, reason="t02")
                # 事故条件：worker 仍在执行（active），REST 仍返回旧 outcome/idle
                self.completed_session("ses_w", outcome="succeeded", idle=old_idle,
                                       busy=new_started, active=True)
                self.sup.cycle(dispatch=False)
                new_id = new_att["id"]
                self.assertNoFalseTerminal(new_id, turns_before=turns_before,
                                           model="p/m")
                # 拒绝必须可追踪：审计中存在终态拒绝记录
                rejects = [a for a in self.audit_actions(new_id)
                           if a["action"] == "terminal.rejected"]
                self.assertTrue(rejects, "假终态拒绝必须留下审计")
                # 同范围新认领仍被阻止（写权排他）
                with self.assertRaises(LedgerError) as ctx:
                    self.ledger.create_attempt(
                        task["id"], directory=self.tmp, session_id="ses_w2",
                        status="creating", write_scope=[self.tmp])
                self.assertEqual(ctx.exception.code, "SITE_BUSY")

    def test_old_outcome_variants_all_rejected(self):
        """T01：旧 succeeded/failed/interrupted 一律不得推进新尝试。"""
        for outcome in ("succeeded", "failed", "interrupted"):
            with self.subTest(old_outcome=outcome):
                self._fresh()
                old_idle, new_started = 1794007219265, 1794008072447
                task, old_att, new_att, msg_id = self.setup_reuse_attempt(
                    old_idle=old_idle, new_started=new_started,
                    files={"report.md": "上一轮产物"})
                # 假终态拒绝前：provider 冷却状态 + turn 快照
                turns_before = self.turns_of(new_att["id"])
                self.ledger.record_provider_state(
                    "p/m", category="rate_limit", status=429,
                    until_ms=self.clock.now_ms() + 60_000, reason="t01")
                # 注意：旧尝试上一轮真实终态为 succeeded；REST 残留为其他结果
                self.completed_session("ses_w", outcome=outcome, idle=old_idle,
                                       busy=new_started, active=True)
                self.sup.cycle(dispatch=False)
                self.assertNoFalseTerminal(new_att["id"], turns_before=turns_before,
                                           model="p/m")
                if outcome != "succeeded":
                    # 残留 failed 也不得伪造执行失败通知
                    for n in self.notifications_for(new_att["id"]):
                        self.assertNotEqual(n["payload"].get("classification"),
                                            "EXECUTION_FAILED")


class TestIncidentT03(IncidentBase):
    """T03：outcome 有值但 idle 缺失/null/非法——不能用 now 补造终态。"""

    def test_idle_missing_variants(self):
        for variant, time_ in (
            ("null-idle", {"idle": None, "busy": 1794008072447}),
            ("no-idle", {"busy": 1794008072447}),
            ("invalid-type", {"idle": "not-a-time", "busy": 1794008072447}),
        ):
            with self.subTest(variant=variant):
                self._fresh()
                old_idle, new_started = 1794007219265, 1794008072447
                task, old_att, new_att, msg_id = self.setup_reuse_attempt(
                    old_idle=old_idle, new_started=new_started,
                    files={"report.md": "上一轮产物"})
                turns_before = self.turns_of(new_att["id"])
                # 会话不活动（候选终端时间缺失是无效候选，不让活动门/新 busy 顶替原因）
                self.client.set_session("ses_w", outcome="succeeded", active=False,
                                        time_=time_)
                self.sup.cycle(dispatch=False)
                self.assertNoFalseTerminal(new_att["id"], turns_before=turns_before)
                att = self.ledger.get_attempt(new_att["id"])
                self.assertLess(att["terminal_at"] or 0, 1,
                                "非法 idle 不得产生 terminal_at")
                # 稳定拒绝原因 + 下一动作 + 审计 + 无终端副作用（T03 缺口）
                unconf = (att["metadata"].get("execution") or {}).get("unconfirmed") or {}
                self.assertEqual(unconf.get("reason"), "missing-end-time")
                self.assertTrue(unconf.get("next_action"),
                                "missing-end-time 必须提供下一动作")
                self.assertEqual(len(self.turns_of(new_att["id"])), len(turns_before),
                                 "拒绝不得增删 turn")
                rejects = [a for a in self.audit_actions(new_att["id"])
                           if a["action"] == "terminal.rejected"]
                self.assertEqual(len(rejects), 1,
                                 "缺失 idle 拒绝必须留下 terminal.rejected 审计")
                accepts = [a for a in self.audit_actions(new_att["id"])
                           if a["action"] == "terminal.accepted"]
                self.assertEqual(accepts, [], "缺失 idle 不得被接受")
                for n in self.notifications_for(new_att["id"]):
                    cls = n["payload"].get("classification") or ""
                    self.assertNotIn(cls, (
                        "RESULT_READY", "RESULT_MISSING", "DELIVERABLE_MISSING",
                        "DELIVERABLE_STALE", "CHECKS_PENDING", "COLLECTION_REQUIRED",
                        "COLLECT_FAILED", "EXECUTION_FAILED", "EXECUTION_INTERRUPTED"),
                        "缺失 idle 拒绝不得产生成功/结果类通知")
                    if cls.startswith("EXECUTION_") and cls != "EXECUTION_UNCONFIRMED":
                        self.fail(f"缺失 idle 产生了未确认之外的执行类别：{cls!r}")


class TestIncidentT10(IncidentBase):
    """T10：新输入仍在 inbox、旧执行在新尝试创建之后结束——不得归属新输入。"""

    def test_undelivered_input_not_completed(self):
        old_idle, new_started = 1794007219265, 1794008072447
        later_done = new_started + 5_000
        old_start = old_idle - 600_000
        self.clock._wall = old_start + 1_000
        self.write_files({"report.md": "上一轮产物"})
        task = self.make_task(files=["report.md"])
        old_att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="creating", dispatch_state="confirmed",
            deliverables=["report.md"], write_scope=[self.tmp])
        self.start_event("ses_w", old_start, "evt-old-start")
        self.completed_session("ses_w", outcome="succeeded", idle=old_idle,
                               busy=old_start)
        self.sup.cycle(dispatch=False)
        self.clock._wall = new_started - 60_000
        out = self.app.rework(task["id"], feedback="继续")
        new_att = self.ledger.get_attempt(out["attempt_id"])
        # 输入仍在 inbox（未投递），旧回合之后又一轮执行结束（时间更新）
        self.completed_session("ses_w", outcome="succeeded", idle=later_done,
                               busy=new_started, active=False)
        self.clock._wall = later_done + 2_000
        self.sup.cycle(dispatch=False)
        self.assertNoFalseTerminal(new_att["id"])


class TestIncidentT05(IncidentBase):
    """T05：假终态被拒后，真实完成只提交一次终态、一个有效版本和通知。"""

    def _run(self, *, outcome: str, assistant_text: str, new_contents: dict | None,
             files0: dict):
        new_started = 1794008072447
        old_idle = 1794007219265
        real_done = new_started + 600_000
        task, old_att, new_att, msg_id = self.setup_reuse_attempt(
            old_idle=old_idle, new_started=new_started, files=files0)
        # 假终态窗口：REST 旧 outcome → 拒绝
        self.completed_session("ses_w", outcome="succeeded", idle=old_idle,
                               busy=new_started, active=True)
        self.sup.cycle(dispatch=False)
        self.assertNoFalseTerminal(new_att["id"])
        # 真实完成：新交付内容 + 实质回报
        if new_contents:
            self.write_files(new_contents)
        if assistant_text:
            self.assistant_message("ses_w", "msg_real_done", assistant_text,
                                   real_done - 1)
        self.completed_session("ses_w", outcome=outcome, idle=real_done,
                               busy=new_started, active=False)
        self.clock._wall = real_done + 2_000
        self.sup.cycle(dispatch=False)
        return new_att["id"], outcome, assistant_text, new_contents

    def test_real_success_closes_loop_once(self):
        att_id, *_ = self._run(outcome="succeeded", assistant_text="真实完成回报：报告已更新。",
                               new_contents={"report.md": "新内容 v2"}, files0={"report.md": "旧内容"})
        att = self.ledger.get_attempt(att_id)
        self.assertEqual(att["status"], "result_collected")
        self.assertEqual(att["terminal_kind"], "succeeded")
        self.assertEqual(att["terminal_at"], 1794008072447 + 600_000,
                         "terminal_at 使用服务端真实完成时间")
        # 只有一个结果版本通知，携带版本与不可变路径
        result_notifs = [n for n in self.notifications_for(att_id)
                         if n["payload"].get("result_version")]
        self.assertEqual(len(result_notifs), 1)
        payload = result_notifs[0]["payload"]
        self.assertEqual(payload["classification"], "RESULT_READY")
        self.assertIn("versions", payload["result_path"])
        self.assertTrue(os.path.isfile(payload["result_path"]),
                        "通知必须指向已存在的不可变版本文件")
        with open(payload["result_path"], "r", encoding="utf-8") as f:
            snapshot = json.load(f)
        self.assertEqual(snapshot["attempt_id"], att_id)
        self.assertEqual(snapshot["result_version"], payload["result_version"])
        # 重复周期不产生第二条逻辑结果通知
        self.clock.advance(seconds=30)
        self.sup.cycle(dispatch=False)
        again = [n for n in self.notifications_for(att_id)
                 if n["payload"].get("result_version")]
        self.assertEqual(len(again), 1)
        # 终态只提交一次（accepted 审计只有一条）
        accepts = [a for a in self.audit_actions(att_id)
                   if a["action"] == "terminal.accepted"]
        self.assertEqual(len(accepts), 1)

    def test_real_failed_and_interrupted_keep_semantics(self):
        for outcome, expect_status, expect_notif in (
            ("failed", "terminal_failed", "EXECUTION_FAILED"),
            ("interrupted", "terminal_interrupted", "INTERRUPTED"),
        ):
            with self.subTest(outcome=outcome):
                self._fresh()
                att_id, _, _, _ = self._run(
                    outcome=outcome, assistant_text="x", new_contents=None,
                    files0={"report.md": "旧内容"})
                att = self.ledger.get_attempt(att_id)
                self.assertEqual(att["terminal_kind"], outcome)
                self.assertGreaterEqual(att["terminal_at"], 1794008072447)
                self.assertNotEqual(att["status"], "result_collected",
                                    "失败/中断不得发布成功结果")
                classifications = [n["payload"].get("classification")
                                   for n in self.notifications_for(att_id)]
                self.assertIn(expect_notif, classifications)
                self.assertNotIn("RESULT_READY", classifications)


class TestIncidentT17(IncidentBase):
    """T17/T18：空回报 + 旧文件 + 只 touch/工具进度 —— 不得形成有效收集。"""

    def test_empty_text_old_files_no_result(self):
        new_started = 1794008072447
        old_idle = 1794007219265
        real_done = new_started + 600_000
        task, old_att, new_att, msg_id = self.setup_reuse_attempt(
            old_idle=old_idle, new_started=new_started,
            files={"report.md": "旧内容", "manifest.json": "{}"})
        # 假终态拒绝
        self.completed_session("ses_w", outcome="succeeded", idle=old_idle,
                               busy=new_started, active=True)
        self.sup.cycle(dispatch=False)
        self.assertNoFalseTerminal(new_att["id"])
        # 真实完成但本轮只有工具活动（空文本）且文件未变化（仅 touch）
        path = os.path.join(self.tmp, "report.md")
        os.utime(path, (real_done / 1000, real_done / 1000))
        self.assistant_message(
            "ses_w", "msg_tool_only", "", real_done - 1,
            tools=[{"name": "bash", "status": "running"}])
        self.completed_session("ses_w", outcome="succeeded", idle=real_done,
                               busy=new_started, active=False)
        self.clock._wall = real_done + 2_000
        self.sup.cycle(dispatch=False)
        att = self.ledger.get_attempt(new_att["id"])
        self.assertEqual(att["terminal_kind"], "succeeded",
                         "真实完成仍应被确认")
        self.assertNotEqual(att["status"], "result_collected",
                            "空文本+全旧文件不得形成有效收集")
        task_row = self.ledger.get_task(att["task_id"])
        self.assertNotEqual(task_row["status"], "result_received")
        for n in self.notifications_for(new_att["id"]):
            self.assertNotIn(n["payload"].get("classification"), (
                "RESULT_READY", "DELIVERABLE_MISSING", "DELIVERABLE_STALE"),
                "无效收集不得发布结果就绪")
        # 补收集预算（15s × 5 次内无新证据）→ 有界耗尽，标 result_missing
        for i in range(5):
            self.clock.advance(seconds=self.cfg["collect_retry_interval"])
            self.sup.cycle(dispatch=False)
        att = self.ledger.get_attempt(new_att["id"])
        self.assertEqual(att["status"], "result_missing")
        classifications = [n["payload"].get("classification")
                           for n in self.notifications_for(new_att["id"])]
        self.assertIn("RESULT_MISSING", classifications)
        # 稍后真实证据可见（服务端消息可见性延迟；created 仍在执行窗口内）
        self.assistant_message("ses_w", "msg_late_report", "迟到但真实的实质回报",
                               real_done - 5)
        out = self.app.collect(new_att["id"])
        att = self.ledger.get_attempt(new_att["id"])
        self.assertEqual(att["status"], "result_collected")
        self.assertTrue(out.get("result_version"), "补收集必须返回结果版本")
        self.assertTrue(out.get("notification_ids"), "补收集必须形成新通知责任")


class TestIncidentT29(IncidentBase):
    """T29：旧实现空结果 + 真实坏 terminal_at + 逆序 turn + 已 ACK —— 显式
    collect 经 B 的 collection.recovery_execution 补证恢复，产生真实新版本。

    事故核心：旧实现把假终态（terminal_at=坏值）当真实成功并提前收集/ACK。
    本样本**保留**坏 terminal_at/turn/旧 ACK，不重开、不 rewrite；恢复由
    application.collection `_recovery_evidence` 用后来真实 REST 区间证明，
    恢复证据随正式结果同事务落库（collection.recovery_execution）。
    """

    def test_legacy_premature_state_recovered_by_explicit_collect(self):
        new_started = 1794008072447
        bad_terminal_at = new_started + 122  # 旧实现早产假终态
        real_done = new_started + 600_000
        files = {"report.md": "上一轮产物", "result.json": "{}"}
        self.write_files(files)
        task = self.make_task(files=sorted(files))
        # 历史损坏样本：旧实现的空结果 + 已 ACK + 逆序坏 turn + 坏 terminal_at
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_legacy",
            deliverables=sorted(files), write_scope=[self.tmp])
        turn_started, fake_end = new_started, new_started - 853_182  # 旧 idle 逆序
        self.ledger.conn.execute(
            "INSERT INTO turns(attempt_id, start_event_id, end_event_id, "
            "started_at, ended_at, outcome) VALUES(?,?,?,?,?,?)",
            (att["id"], "evt-real-start", "obs-fake", turn_started,
             fake_end, "succeeded"))
        self.ledger.conn.execute(
            "UPDATE attempts SET status='result_collected', terminal_at=?, "
            "terminal_kind='succeeded', result_collected_at=?, result_path=? "
            "WHERE id=?",
            (bad_terminal_at, new_started + 300,
             os.path.join(self.tmp, "legacy-empty-result.json"), att["id"]))
        with open(os.path.join(self.tmp, "legacy-empty-result.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"schema": "oc-orch.result.v1", "attempt_id": att["id"],
                       "task_id": task["id"], "generation": att["generation"],
                       "session_id": "ses_w", "assistant_message_count": 0,
                       "text_chars": 0, "text": "", "collected_at":
                       new_started + 300, "auto": True,
                       "terminal_kind": "succeeded"}, f)
        legacy_notif = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c",
            dedupe_key=f"result-ready:{att['id']}",
            payload={"task_id": task["id"], "attempt_id": att["id"],
                     "classification": "RESULT_READY", "summary": "执行回合成功结束"},
            project="p", task_id=task["id"], attempt_id=att["id"])
        self.ledger.ack_notifications([legacy_notif["id"]], actor="coordinator:old")
        acked_at = self.ledger.get_notification(legacy_notif["id"])["acked_at"]
        self.assertIsNotNone(acked_at)

        # 尚无真实成功区间：坏终态/空结果不能直接收集（B 门禁）
        with self.assertRaises(ActionError):
            self.app.collect(att["id"])

        # 真实完成证据（B 的 _recovery_evidence 所需）：输入锚点 + REST 成功/idle
        # + 会话不活动 + 输入不在 pending inbox；不驱动 supervisor 覆盖坏终态。
        self.client.append_message("ses_w", {"id": "msg_legacy", "type": "user",
                                             "texts": "原输入",
                                             "time": {"created": new_started}})
        self.write_files({"report.md": "真实修订内容"})
        self.assistant_message("ses_w", "msg_real_final", "真实完成：全部交付物就绪",
                               real_done - 1)
        # 匹配 outcome/end 的 idle 消息：B 的 _recovery_evidence 按唯一 idle 区间补证
        self.client.add_idle("ses_w", outcome="succeeded", at=real_done)
        self.completed_session("ses_w", outcome="succeeded", idle=real_done,
                               busy=new_started, active=False)

        out = self.app.collect(att["id"])
        self.assertTrue(out.get("result_version"), "恢复收集必须形成新版本")
        self.assertTrue(out.get("notification_ids"))
        self.assertEqual(out.get("collection_state"), "complete")
        att2 = self.ledger.get_attempt(att["id"])
        self.assertEqual(att2["status"], "result_collected")
        # 坏 terminal_at 与坏 turn 保持原样，不被 collect 改写
        self.assertEqual(att2["terminal_at"], bad_terminal_at,
                         "历史坏终态保持可审计，不 rewrite")
        collection = (att2["metadata"] or {}).get("collection") or {}
        recovery = collection.get("recovery_execution")
        self.assertIsInstance(recovery, dict, "恢复证据必须随结果落库")
        self.assertEqual(recovery.get("schema"),
                         "oc-orch.collection.recovery.v1")
        self.assertEqual(recovery.get("real_end_at"), real_done)
        self.assertEqual(recovery.get("legacy_terminal", {}).get("at"),
                         bad_terminal_at, "记录原损坏引用而非篡改")
        # 旧 ACK 保持；新通知是新的待确认责任
        old_row = self.ledger.get_notification(legacy_notif["id"])
        self.assertEqual(old_row["state"], "acked")
        self.assertEqual(old_row["acked_at"], acked_at)
        new_ids = out["notification_ids"]
        self.assertNotIn(legacy_notif["id"], new_ids)
        for nid in new_ids:
            row = self.ledger.get_notification(nid)
            self.assertIn(row["state"], ("pending", "sent"),
                          "collect 不隐式 ACK，新通知保持待确认")
        turns = self.turns_of(att["id"])
        bad = [t for t in turns if t["end_event_id"] == "obs-fake"]
        self.assertEqual(len(bad), 1, "历史异常 turn 保留可审计")
        self.assertEqual(bad[0]["ended_at"], fake_end, "坏 turn 时间不被改写")


class TestIncidentT07(IncidentBase):
    """T07：终态先于 started 到达、中间重启 —— 候选不丢失、只提交一次。"""

    def _fresh_with_sealed_attempt(self):
        self._fresh()
        t0 = self.clock.now_ms()
        task = self.make_task(coordinator="ses_c")
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            dispatch_msg_id="msg_anchor",
            deliverables=[], write_scope=[self.tmp])
        # 派单前执行边界（reuse/new 语义相同），epoch 尚缺
        self.ledger.seal_execution(
            att["id"], mode="new", session_before={}, baseline={},
            allow_send=True, actor="coordinator", reason="test-seal")
        # 真实原输入（已投递消息）：started 必须与本输入绑定，不能只靠 started 补证
        self.client.append_message("ses_w", {"id": "msg_anchor", "type": "user",
                                             "texts": "原输入",
                                             "time": {"created": t0}})
        # 会话快照存在（busy <= idle，无更晚 busy）：活动门可读最新 session
        self.client.set_session("ses_w", outcome=None,
                                time_={"busy": t0, "idle": t0, "updated": t0},
                                active=False)
        return task, self.ledger.get_attempt(att["id"]), t0

    def test_terminal_before_started_survives_restart(self):
        task, att, t0 = self._fresh_with_sealed_attempt()
        started_at = t0 + 10_000
        ended_at = started_at + 50_000
        # 1) 先收到 terminal（started 尚未到达）：候选落 events，归属不足被拒
        action = self.sup.ingest_event({
            "id": "evt_term_first", "type": "session.execution.succeeded",
            "created": ended_at, "data": {"sessionID": "ses_w", "error": None},
        })
        # 事件先被保存；终态因无 started/区间证据保持未确认（不推进状态）
        att1 = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att1["terminal_at"])
        self.assertTrue(self.ledger.event_seen("evt_term_first"))
        unconf = (att1["metadata"].get("execution") or {}).get("unconfirmed")
        self.assertIn(unconf.get("reason"), ("no-attributable-start",
                                             "legacy-no-baseline",
                                             "ended-before-start"))
        # 2) 重启：新 Supervisor 复用同一台账与 fake 服务（独立连接读取持久状态）
        sup2 = Supervisor(self.tmp, cfg=self.cfg, client=self.client,
                          clock=self.clock)
        self.addCleanup(sup2.conn.close)
        # started 迟到达（真实开始时间早于 terminal）→ 归属补齐 → 补交终态
        self.clock._wall = started_at + 1
        action2 = sup2.ingest_event({
            "id": "evt_start_later", "type": "session.execution.started",
            "created": started_at, "data": {"sessionID": "ses_w"},
        })
        self.assertEqual(action2, "started")
        att2 = sup2.ledger.get_attempt(att["id"])
        self.assertEqual(att2["status"], "terminal_succeeded")
        self.assertEqual(att2["terminal_at"], ended_at,
                         "补证沿用原始 terminal 事件的时间")
        # 只提交一次终态；迟到 started 不产生第二个 open turn
        accepts = [a for a in self.audit_actions(att["id"])
                   if a["action"] == "terminal.accepted"]
        self.assertEqual(len(accepts), 1)
        open_turns = [t for t in self.turns_of(att["id"])
                      if t["end_event_id"] is None]
        self.assertEqual(open_turns, [], "不允许残留第二个 open turn")
        closed = [t for t in self.turns_of(att["id"])
                  if t["end_event_id"] is not None]
        self.assertEqual(len(closed), 1)
        first = closed[0]
        self.assertEqual(first["started_at"], started_at)
        self.assertEqual(first["ended_at"], ended_at)


class TestIncidentT09(IncidentBase):
    """T09：真实执行开始/结束早于 prompt HTTP 回执 —— 合法完成不被本地时间误伤。"""

    def test_execution_finished_before_http_ack(self):
        new_started = 1794008072447
        real_done = new_started + 300_000
        self.clock._wall = new_started - 120_000
        self.write_files({"report.md": "上一轮产物"})
        task = self.make_task(files=["report.md"])

        # 首次 prompt HTTP 响应丢失（未入队 → dispatch_unknown；原 IDs 保留）
        self.client.fail("prompt", TransportError("/prompt", "response-lost"))
        with self.assertRaises(ActionError) as ctx:
            self.app.dispatch(task["id"], directory=self.tmp, coordinator="ses_c",
                              prompt="完成任务", deliverables=["report.md"])
        self.assertEqual(ctx.exception.code, "dispatch_unknown")
        att_id, sid, msg_id = (ctx.exception.detail["attempt_id"],
                               ctx.exception.detail["session_id"],
                               ctx.exception.detail["message_id"])
        att = self.ledger.get_attempt(att_id)
        self.assertEqual(att["dispatch_state"], "unknown")
        self.assertEqual(att["dispatch_msg_id"], msg_id)

        # 真正交错：supervisor 对账重发原 ID 的 HTTP 确认回调期间，
        # 真实执行已投递并完成（漏 SSE）——本地确认时间晚于真实结束。
        def prompt_and_finish(*args, **kwargs):
            out = FakeClient.prompt(self.client, *args, **kwargs)  # 消息入队（回调内）
            self.clock._wall = new_started
            self.client.deliver_inbox(args[0], at=new_started)  # 输入已投递执行
            self.client.add_idle(args[0], outcome="succeeded", at=real_done)
            self.client.append_message(args[0], {
                "id": "msg_real_t09", "type": "assistant",
                "time": {"created": real_done - 1},
                "content": [{"type": "text", "text": "已完成：report 更新"}]})
            self.write_files({"report.md": "真实修订"})
            self.client.set_session(args[0], outcome="succeeded",
                                    time_={"idle": real_done, "busy": new_started,
                                           "updated": real_done},
                                    active=False)
            self.clock._wall = real_done + 2_000
            return out

        with patch.object(self.client, "prompt", side_effect=prompt_and_finish):
            self.sup.cycle(dispatch=False)

        att = self.ledger.get_attempt(att_id)
        self.assertEqual(att["dispatch_state"], "confirmed",
                         "回调内已完成：对账确认原 ID 已受理")
        self.assertEqual(att["dispatch_msg_id"], msg_id, "绝不换 ID 重投")
        self.assertEqual(att["terminal_at"], real_done,
                         "真实结束发生在本地确认之前：不得以确认时间误伤")
        self.assertEqual(len(self.client.prompt_calls), 2,
                         "首次响应丢失 + 对账补发原 ID 一次；不重复发送已受理消息")


class TestIncidentT11(IncidentBase):
    """T11：busy 晚于候选 idle 且 session 活动 —— 保留未确认；事实刷新后确认。"""

    def test_newer_busy_conflict_then_refreshed_facts_confirm(self):
        started, idle0, busy1 = 1000000, 1000100, 1000200
        task = self.make_task(coordinator="ses_c")
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_x", deliverables=[], write_scope=[self.tmp])
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
                                   baseline={}, allow_send=True,
                                   actor="coordinator", reason="test")
        # 真实原输入（已投递）：epoch/started 必须与本输入绑定
        self.client.append_message("ses_w", {"id": "msg_x", "type": "user",
                                             "texts": "原输入",
                                             "time": {"created": started}})
        self.start_event("ses_w", started, "evt_start1")
        # 候选：idle=1000100 但 busy=1000200（更晚 busy）且 session 活动
        self.completed_session("ses_w", outcome="succeeded", idle=idle0,
                               busy=busy1, active=True)
        self.clock._wall = busy1 + 500
        self.sup.cycle(dispatch=False)
        att1 = self.ledger.get_attempt(att["id"])
        self.assertNoFalseTerminal(att1["id"])
        self.assertEqual((att1["metadata"]["execution"]["unconfirmed"]["reason"]),
                         "newer-busy-after-idle")
        unconf_notifs = [n for n in self.notifications_for(att1["id"])
                         if n["payload"].get("classification")
                         == "EXECUTION_UNCONFIRMED"]
        self.assertGreaterEqual(len(unconf_notifs), 1,
                                "未确认必须留下可操作告警/通知")
        self.assertTrue(all(n["payload"].get("next_action")
                            for n in unconf_notifs))
        self.assertIsNone(att1["site_released_at"], "冲突期间写权保持")
        # 事实刷新：session 最终 idle 晚于 busy、不活动 → 完成被确认
        real_end = busy1 + 400_000
        self.completed_session("ses_w", outcome="succeeded", idle=real_end,
                               busy=busy1, active=False)
        self.clock._wall = real_end + 1
        self.sup.cycle(dispatch=False)
        att2 = self.ledger.get_attempt(att["id"])
        self.assertEqual(att2["terminal_at"], real_end,
                         "刷新后成功复核可推进；旧 active 样本不造成永久等待")


class TestIncidentT15(IncidentBase):
    """T15：no_prompt 附着运行中回合可 REST 完成；仅历史 idle 不消费过去成功。"""

    def test_attach_busy_interval_completes_via_rest(self):
        task = self.make_task(coordinator="ses_c")
        busy = 999_000
        # 正在运行的 busy 区间：busy(999000) 晚于 idle(900000)，无更晚 idle
        self.client.set_session("ses_w", active=True,
                                time_={"busy": busy, "idle": 900_000},
                                location_dir=self.tmp)
        out = self.app.dispatch(task["id"], directory=self.tmp,
                                coordinator="ses_c", session="ses_w",
                                no_prompt=True, prompt="附着")
        self.assertEqual(self.client.prompt_calls, [],
                         "no_prompt 附着不得发送 prompt")
        self.assertIsNone(out.get("message_id"),
                          "T15：no_prompt 不伪造派单消息 ID")
        att = self.ledger.get_attempt(out["attempt_id"])
        self.assertEqual(att["dispatch_state"], "confirmed")
        self.assertIsNone(att["dispatch_msg_id"],
                          "no_prompt 附着不得生成虚构 dispatch message")
        execution = att["metadata"]["execution"]
        self.assertEqual(execution["session_before"]["busy"], busy)
        self.assertEqual(execution["epoch"]["started_at"], busy,
                         "正在运行的 busy 区间必须冻结为可归属执行开始")
        # 全程漏 SSE：附着区间结束 → REST idles
        self.client.clear_active()
        real_end = 1_001_000
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"busy": busy, "idle": real_end},
                                active=False)
        self.clock._wall = real_end + 2_000
        self.sup.cycle(dispatch=False)
        att = self.ledger.get_attempt(out["attempt_id"])
        self.assertEqual(att["terminal_at"], real_end,
                         "attach 的 busy 区间后续结束可由 REST 归属完成")
        self.assertEqual(att["terminal_kind"], "succeeded")

    def test_attach_history_idle_waits_for_new_facts(self):
        task = self.make_task(coordinator="ses_c")
        # 真实历史已完成 busy：busy(800000) 早于 idle(900000) —— 不能当新 epoch
        self.client.set_session("ses_w", active=False,
                                time_={"busy": 800_000, "idle": 900_000},
                                location_dir=self.tmp)
        out = self.app.dispatch(task["id"], directory=self.tmp,
                                coordinator="ses_c", session="ses_w",
                                no_prompt=True, prompt="附着")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertIsNone(out.get("message_id"), "不伪造派单消息")
        att = self.ledger.get_attempt(out["attempt_id"])
        self.assertIsNone(att["dispatch_msg_id"])
        # 历史已完成 busy 不当新执行开始
        self.assertEqual((att["metadata"]["execution"]["epoch"] or {}).get(
            "started_at"), None, "历史 busy/idle 不能当新 epoch")
        # 新的 REST 完成也不立即归属（无 epoch/锚点身份）：等待真实执行事实
        self.completed_session("ses_w", outcome="succeeded", idle=950_000,
                               busy=920_000, active=False)
        self.clock._wall = 951_000
        self.sup.cycle(dispatch=False)
        att = self.ledger.get_attempt(out["attempt_id"])
        self.assertIsNone(att["terminal_at"],
                          "历史 idle 的旧成功是基线，不推进新附着尝试")


class TestIncidentT16(IncidentBase):
    """T16：多个未结尝试共享 session —— 无法唯一归属时 unconfirmed，不 latest+2s。"""

    def test_multiple_open_attempts_shared_session_unconfirmed(self):
        t0 = self.clock.now_ms()
        task = self.make_task(coordinator="ses_c")
        a1 = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_a1", deliverables=[], write_scope=[self.tmp])
        self.ledger.seal_execution(a1["id"], mode="new", session_before={},
                                   baseline={}, allow_send=True,
                                   actor="coordinator", reason="test")
        # 第二个未结尝试：不同写范围（无 SITE_BUSY 冲突）但共享同一 session
        other_tmp = tempfile.TemporaryDirectory(prefix="oc-other-")
        self.addCleanup(other_tmp.cleanup)
        other = other_tmp.name
        a2 = self.ledger.create_attempt(
            task["id"], directory=other, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_a2", deliverables=[], write_scope=[other])
        self.ledger.seal_execution(a2["id"], mode="new", session_before={},
                                   baseline={}, allow_send=True,
                                   actor="coordinator", reason="test")
        # 两个 attempt 的输入都在同一 session，REST 终态无法唯一归属
        self.client.append_message("ses_w", {"id": "msg_a1", "type": "user",
                                             "time": {"created": t0}})
        self.client.append_message("ses_w", {"id": "msg_a2", "type": "user",
                                             "time": {"created": t0}})
        self.completed_session("ses_w", outcome="succeeded", idle=t0 + 1000,
                               busy=t0, active=False)
        self.sup.observe_attempt(self.ledger.get_attempt(a1["id"]), {})
        self.sup.observe_attempt(self.ledger.get_attempt(a2["id"]), {})
        for att in (self.ledger.get_attempt(a1["id"]),
                    self.ledger.get_attempt(a2["id"])):
            self.assertIsNone(att["terminal_at"],
                              "多尝试共享区间不得按 latest 猜测归属")
            unconf = (att["metadata"].get("execution") or {}).get("unconfirmed")
            self.assertEqual(unconf.get("reason"),
                             "input-ownership-ambiguous")
            self.assertIsNone(att["site_released_at"])
        # 事件归属同样不确定：terminal 事件不猜测具体 attempt
        self.assertIsNone(self.sup._attempt_for_event("ses_w", t0 + 1000))

    def _shared_session_pair(self, *, delta):
        """两个共享 session 的未结尝试，a2 创建时间较 a1 偏移 delta（重叠窗口）。"""
        self._fresh()
        t0 = self.clock.now_ms()
        task = self.make_task(coordinator="ses_c")
        other_tmp = tempfile.TemporaryDirectory(prefix="oc-ov-")
        self.addCleanup(other_tmp.cleanup)
        other = other_tmp.name
        a1 = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_ov1", write_scope=[self.tmp])
        self.ledger.seal_execution(a1["id"], mode="new", session_before={},
                                   baseline={}, allow_send=True,
                                   actor="coordinator", reason="t16")
        self.clock._wall = t0 + delta
        a2 = self.ledger.create_attempt(
            task["id"], directory=other, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_ov2", write_scope=[other])
        self.ledger.seal_execution(a2["id"], mode="new", session_before={},
                                   baseline={}, allow_send=True,
                                   actor="coordinator", reason="t16")
        return self.ledger.get_attempt(a1["id"]), self.ledger.get_attempt(a2["id"])

    def test_parametrized_overlapping_window_late_event_not_guessed(self):
        """T16 参数化：最近尝试窗口重叠 + 事件迟到 → 不按 latest+2s 兜底猜测。"""
        for delta in (0, 1500, 1900):
            with self.subTest(delta=delta):
                a1, a2 = self._shared_session_pair(delta=delta)
                t0 = self.clock.now_ms()
                # 双方输入均已投递；输入身份决定归属（不按 latest+2s 兜底）
                self.client.append_message("ses_w", {"id": "msg_ov1", "type": "user",
                                                     "time": {"created": t0}})
                self.client.append_message("ses_w", {"id": "msg_ov2", "type": "user",
                                                     "time": {"created": t0 + delta}})
                ended = t0 + delta + 1000
                self.client.add_idle("ses_w", outcome="succeeded", at=ended)
                self.completed_session("ses_w", outcome="succeeded",
                                       idle=ended, busy=t0, active=False)
                # 迟到事件（时间远超两窗口）在双方仍 open 时不得猜具体 attempt
                # （不按 latest+2s 或唯一 open 兜底）
                self.assertIsNone(
                    self.sup._attempt_for_event("ses_w", ended + 50_000),
                    "重叠窗口+迟到事件不得凭时间窗口/最新尝试兜底归属")
                if delta == 0:
                    # 同一毫秒双输入：同区间不可拆 → 双方都 unconfirmed
                    for aid in (a1["id"], a2["id"]):
                        self.sup.observe_attempt(self.ledger.get_attempt(aid), {})
                        a = self.ledger.get_attempt(aid)
                        self.assertIsNone(a["terminal_at"],
                                          "同毫秒双输入同区间：不得猜测归属")
                        self.assertEqual(
                            a["metadata"]["execution"]["unconfirmed"]["reason"],
                            "input-ownership-ambiguous")
                        self.assertIsNone(a["site_released_at"])
                else:
                    # 迟到输入（a2）唯一锚定最终区间 → a2 唯一归属；a1 区间被 a2 输入
                    # 占用 → ambiguous（不按 latest+2s 猜测 a1）
                    self.sup.observe_attempt(self.ledger.get_attempt(a1["id"]), {})
                    a1o = self.ledger.get_attempt(a1["id"])
                    self.assertIsNone(a1o["terminal_at"])
                    self.assertEqual(a1o["metadata"]["execution"]["unconfirmed"]["reason"],
                                     "input-ownership-ambiguous")
                    self.assertIsNone(a1o["site_released_at"])
                    self.sup.observe_attempt(self.ledger.get_attempt(a2["id"]), {})
                    a2o = self.ledger.get_attempt(a2["id"])
                    self.assertEqual(a2o["terminal_at"], ended,
                                      "后续输入的独特区间可由输入身份唯一归属（非 latest 猜测）")
                    # 后代完成后仅剩旧代仍 open，同样不能把迟到事件兜底归到旧代。
                    for event_at in (ended, ended + 1, ended + 50_000):
                        self.assertIsNone(self.sup._attempt_for_event("ses_w", event_at))

    def test_unique_input_anchors_only_attribution(self):
        """T16：两个重叠尝试共享 session，仅 A 输入已落入真实区间 → A 唯一归属；
        B 输入仍 pending → 不归属（排队未执行）。"""
        a1, a2 = self._shared_session_pair(delta=0)
        t0 = self.clock.now_ms()
        # A 输入已投递；B 输入仍在 inbox（pending，未执行）
        self.client.append_message("ses_w", {"id": "msg_ov1", "type": "user",
                                             "time": {"created": t0}})
        self.client.set_inbox("ses_w", [{"id": "msg_ov2"}])
        self.client.add_idle("ses_w", outcome="succeeded", at=t0 + 1000)
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": t0 + 1000, "busy": t0,
                                       "updated": t0 + 1000}, active=False)
        self.clock._wall = t0 + 1_100
        self.sup.observe_attempt(self.ledger.get_attempt(a1["id"]), {})
        a1o = self.ledger.get_attempt(a1["id"])
        self.assertEqual(a1o["terminal_at"], t0 + 1000,
                         "唯一输入锚点身份：不按 latest+2s 也能唯一归属 A")
        self.sup.observe_attempt(self.ledger.get_attempt(a2["id"]), {})
        a2o = self.ledger.get_attempt(a2["id"])
        self.assertIsNone(a2o["terminal_at"], "B 输入仍 pending，不得归属")
        self.assertEqual(a2o["metadata"]["execution"]["unconfirmed"]["reason"],
                         "input-undelivered")


class TestIncidentT11RealGates(IncidentBase):
    """T11 三种真实活动门（审查2）：session-active 冲突、active 查询失败、旧 active
    样本在同一观察内刷新为 inactive 后放行；全部经实现自身刷新，不靠测试手动清。"""

    def _sealed(self, *, msg="msg_act", started=1_000_000):
        self._fresh()
        task = self.make_task(coordinator="ses_c")
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id=msg, deliverables=[], write_scope=[self.tmp])
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
                                   baseline={}, allow_send=True,
                                   actor="coordinator", reason="test")
        return task, att

    def test_session_active_but_busy_le_idle_holds(self):
        """active 包含 worker 且 busy<=idle：会话仍活动 → session-active 保持。"""
        task, att = self._sealed()
        ended, busy = 1_001_000, 1_000_000
        # 真实原输入已投递
        self.client.append_message("ses_w", {"id": "msg_act", "type": "user",
                                             "time": {"created": 1_000_000}})
        self.start_event("ses_w", 1_000_000, "evt_act_start")
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": ended, "busy": busy,
                                       "updated": ended})
        sess = self.client.sessions["ses_w"]
        self.client.active["ses_w"] = sess
        # 刷新后 active 仍包含 worker（不能把活动当作已完成）
        with patch.object(self.client, "active_sessions",
                          return_value={"ses_w": sess}):
            self.sup.observe_attempt(self.ledger.get_attempt(att["id"]),
                                     {"ses_w": sess}, active_ok=True)
        att1 = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att1["terminal_at"],
                          "worker 仍活动（busy<=idle 一致）不能确认终态")
        unconf = (att1["metadata"].get("execution") or {}).get("unconfirmed")
        self.assertEqual(unconf.get("reason"), "session-active")
        self.assertIsNone(att1["site_released_at"], "冲突期间写权保持")
        for n in self.notifications_for(att1["id"]):
            self.assertNotEqual(n["payload"].get("classification"), "RESULT_READY")

    def test_active_query_failure_keeps_unconfirmed_then_resolves(self):
        """active 查询失败 = unknown（不得当无活动）；恢复后仍须真实不活动才确认。"""
        task, att = self._sealed(msg="msg_q")
        self.client.append_message("ses_w", {"id": "msg_q", "type": "user",
                                             "time": {"created": 1_000_000}})
        self.start_event("ses_w", 1_000_000, "evt_q_start")
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": 1_001_000, "busy": 1_000_000})
        with patch.object(self.client, "active_sessions",
                          side_effect=Exception("active unavailable")):
            self.sup.observe_attempt(self.ledger.get_attempt(att["id"]),
                                     {}, active_ok=False)
        att1 = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att1["terminal_at"],
                          "查询失败=unknown：不得以空集合当无活动而确认终态")
        unconf = (att1["metadata"].get("execution") or {}).get("unconfirmed")
        self.assertEqual(unconf.get("reason"), "activity-unknown")
        self.assertIsNone(att1["site_released_at"])
        # 恢复：active 查询成功且不包含 worker；同一后续观察确认真实完成
        self.client.active.pop("ses_w", None)
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {},
                                 active_ok=True)
        att2 = self.ledger.get_attempt(att["id"])
        self.assertEqual(att2["terminal_at"], 1_001_000,
                         "活动查询恢复且 worker 不活动后合法完成可确认")

    def test_stale_active_refreshes_in_single_observation(self):
        """旧 active 样本在同一观察内刷新为 inactive：实现自行刷新，不得永久等待。"""
        task, att = self._sealed(msg="msg_r")
        self.client.append_message("ses_w", {"id": "msg_r", "type": "user",
                                             "time": {"created": 1_000_000}})
        self.start_event("ses_w", 1_000_000, "evt_r_start")
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": 1_001_000, "busy": 1_000_000})
        sess = self.client.sessions["ses_w"]
        stale = {"ses_w": sess}  # 旧 active 快照（stale）
        real_active = {}        # 刷新后的真实事实：worker 已停止
        with patch.object(self.client, "active_sessions",
                          return_value=real_active):
            self.sup.observe_attempt(self.ledger.get_attempt(att["id"]),
                                     stale, active_ok=True)
        att4 = self.ledger.get_attempt(att["id"])
        self.assertEqual(att4["terminal_at"], 1_001_000,
                         "旧 active 样本经实现自身刷新为 inactive 后放行")

    def test_pending_input_blocks_epoch_and_reobserved_old_idle(self):
        """补充：SSE started 已到（epoch）但原输入仍在 pending inbox —— 不能确认；
        投递后旧 idle 重观测仍是基线（旧回合），不得归属本尝试。"""
        old_idle = 1_000_000
        new_started = 2_000_000
        task = self.make_task(coordinator="ses_c")
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_p", deliverables=[], write_scope=[self.tmp])
        # reuse：冻结旧回合基线（旧 idle）
        self.ledger.seal_execution(
            att["id"], mode="reuse", session_before={"outcome": "succeeded",
                                                     "idle": old_idle,
                                                     "busy": old_idle - 1000},
            baseline={}, allow_send=True, actor="coordinator", reason="test")
        # 新 started 已到（epoch），但新输入仍在 inbox 排队（未执行）
        self.client.set_inbox("ses_w", [{"id": "msg_p"}])
        self.start_event("ses_w", new_started, "evt_p_start")
        # 旧回合 idle 重观测
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": old_idle, "busy": old_idle - 1000},
                                active=False)
        self.clock._wall = old_idle + 1
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {})
        att1 = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att1["terminal_at"],
                          "新输入尚 pending：started 存在也不能确认")
        unconf = (att1["metadata"].get("execution") or {}).get("unconfirmed")
        self.assertEqual(unconf.get("reason"), "input-undelivered")
        # 投递后旧 idle 仍重观测：属于冻结基线（旧回合），不得归属本尝试
        self.client.deliver_inbox("ses_w", at=new_started)
        self.clock._wall = old_idle + 2
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {})
        att2 = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att2["terminal_at"],
                          "投递后旧 idle 重观测仍是基线，不得归属新尝试")

    def test_unknown_persists_across_period_deduped(self):
        """T11 边界（协调者澄清）：未知观察跨周期保留稳定 first_at、不重置/不重复逻辑
        通知、有下一动作；事实恢复后可推进。无独立超时定时器：Supervisor 每周期重观察，
        即时去重 EXECUTION_UNCONFIRMED 告警（稳定 dedupe key）持续可操作。"""
        task, att = self._sealed(msg="msg_ez")
        self.client.append_message("ses_w", {"id": "msg_ez", "type": "user",
                                             "time": {"created": 1_000_000}})
        self.start_event("ses_w", 1_000_000, "evt_ez_start")
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": 1_001_000, "busy": 1_000_000})

        def fail_active():
            raise Exception("active unavailable")

        dedupe = f"terminal-unconfirmed:{att['id']}:activity-unknown"
        with patch.object(self.client, "active_sessions", side_effect=fail_active):
            self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {},
                                     active_ok=False)
            first = self.ledger.get_attempt(att["id"])
            un1 = (first["metadata"]["execution"]["unconfirmed"])
            self.assertEqual(un1["reason"], "activity-unknown")
            first_at = un1["first_at"]
            self.assertEqual(un1["occurrences"], 1)
            self.assertTrue(un1["next_action"])
            self.assertEqual(
                len([n for n in self.ledger.list_notifications(limit=100)
                     if n["dedupe_key"] == dedupe]), 1,
                "第一次未知只产生一条逻辑通知")
            # 推进时间越过观察周期后重复未知
            self.clock.advance(seconds=3600)
            self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {},
                                     active_ok=False)
            second = self.ledger.get_attempt(att["id"])
            un2 = (second["metadata"]["execution"]["unconfirmed"])
            self.assertEqual(un2["first_at"], first_at, "重复未知不得重置首见时间")
            self.assertEqual(un2["occurrences"], 2, "重复未知只累计次数")
            self.assertEqual(
                len([n for n in self.ledger.list_notifications(limit=100)
                     if n["dedupe_key"] == dedupe]), 1,
                "跨周期重复未知不新建/复活多余逻辑通知")
            self.assertIsNone(second["terminal_at"])
            self.assertIsNone(second["site_released_at"])
        # 事实恢复：active 查询成功且 worker 不活动 → 推进
        self.client.active.pop("ses_w", None)
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {}, active_ok=True)
        last = self.ledger.get_attempt(att["id"])
        self.assertEqual(last["terminal_at"], 1_001_000, "事实恢复后可推进")




class TestIntervalSingleRound(IncidentBase):
    """审查1 第三轮：区间归属必须回合唯一——coverage complete 不能替代唯一回合。

    锚点与匹配 idle 之间夹有旧 idle（其他回合）或后续 user 输入 → 多回合归属
    不明，不 proven；锚点**前**的旧回合 idle 属于基线区域，允许。
    """

    def _interval_world(self, *, msg="msg_i", started=1_000_000, ended=1_001_000):
        self._fresh()
        task = self.make_task(coordinator="ses_c")
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id=msg, deliverables=[], write_scope=[self.tmp])
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
                                   baseline={}, allow_send=True,
                                   actor="coordinator", reason="test")
        self.client.append_message("ses_w", {"id": msg, "type": "user",
                                             "texts": "原输入",
                                             "time": {"created": started}})
        return task, self.ledger.get_attempt(att["id"])

    def test_multiple_idles_in_interval_not_proven(self):
        """旧 idle（时间/outcome 不匹配）夹在锚点与匹配 idle 之间 → 多回合拒绝。"""
        task, att = self._interval_world()
        # 旧回合 idle 落在 (input_at, ended] 内，但时间不是候选 ended
        self.client.add_idle("ses_w", outcome="succeeded", at=1_000_500)
        # 匹配 idle 在 ended
        self.client.add_idle("ses_w", outcome="succeeded", at=1_001_000)
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": 1_001_000, "busy": 1_000_000,
                                       "updated": 1_001_000}, active=False)
        self.clock._wall = 1_001_200
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {})
        att1 = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att1["terminal_at"],
                          "coverage complete 不能替代回合唯一：旧 idle 夹中间属多回合")
        unconf = (att1["metadata"].get("execution") or {}).get("unconfirmed")
        self.assertEqual(unconf.get("reason"), "no-attributable-start")
        self.assertIsNone(att1["site_released_at"])

    def test_later_user_input_in_interval_not_proven(self):
        """锚点后、匹配 idle 前出现后续 user 输入 → 后续回合，不 proven。"""
        task, att = self._interval_world()
        self.client.append_message("ses_w", {"id": "later_input", "type": "user",
                                             "texts": "后续输入",
                                             "time": {"created": 1_000_700}})
        self.client.add_idle("ses_w", outcome="succeeded", at=1_001_000)
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": 1_001_000, "busy": 1_000_000,
                                       "updated": 1_001_000}, active=False)
        self.clock._wall = 1_001_200
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {})
        att1 = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att1["terminal_at"],
                          "锚点后后续 user 输入：区间不唯一，不得归属本轮")
        unconf = (att1["metadata"].get("execution") or {}).get("unconfirmed")
        self.assertEqual(unconf.get("reason"), "no-attributable-start")
        self.assertIsNone(att1["site_released_at"])

    def test_old_idle_before_anchor_still_proven(self):
        """锚点**前**的旧回合 idle（基线区域）不破坏本轮区间归属。"""
        task, att = self._interval_world()
        # 旧回合 idle 在锚点之前（< input_at）：允许
        self.client.add_idle("ses_w", outcome="succeeded", at=999_000)
        self.client.add_idle("ses_w", outcome="succeeded", at=1_001_000)
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": 1_001_000, "busy": 1_000_000,
                                       "updated": 1_001_000}, active=False)
        self.clock._wall = 1_001_200
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {})
        att1 = self.ledger.get_attempt(att["id"])
        # 本函数 _rest 观察应形成正式终态（区间唯一：锚点前旧回合是基线）
        self.assertEqual(att1["terminal_at"], 1_001_000,
                         "锚点前旧回合 idle 是基线；本轮区间仍唯一可归属")
        self.assertEqual(att1["terminal_kind"], "succeeded")




class TestIncidentT04(IncidentBase):
    """T04 边界：同毫秒基线（start=end）旧 idle 仍拒；无 seq/区间不能猜；有充分
    输入→idle 区间的新真实完成可过（不被同毫秒基线永久挡住）。"""

    def _reuse_sealed(self, *, old_idle, msg="msg_t04"):
        self._fresh()
        task = self.make_task(coordinator="ses_c", files=["report.md"])
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed", dispatch_msg_id=msg,
            deliverables=["report.md"], write_scope=[self.tmp])
        self.ledger.seal_execution(
            att["id"], mode="reuse",
            session_before={"outcome": "succeeded", "idle": old_idle,
                            "busy": old_idle - 600_000},
            baseline={}, allow_send=True, actor="coordinator", reason="t04")
        return task, self.ledger.get_attempt(att["id"])

    def test_same_ms_baseline_rejected_then_interval_passes(self):
        # 同毫秒：候选 ended == 冻结基线 idle（start=end 完全相等）→ 旧回合身份优先
        old_idle = 1794008072447
        task, att = self._reuse_sealed(old_idle=old_idle)
        self.client.append_message("ses_w", {"id": "msg_t04", "type": "user",
                                             "time": {"created": old_idle}})
        turns_before = self.turns_of(att["id"])
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": old_idle, "busy": old_idle - 1000},
                                active=False)
        self.clock._wall = old_idle + 100
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {})
        att1 = self.ledger.get_attempt(att["id"])
        self.assertNoFalseTerminal(att1["id"], turns_before=turns_before)
        unconf = (att1["metadata"].get("execution") or {}).get("unconfirmed")
        self.assertEqual(unconf.get("reason"), "baseline-outcome-equal",
                         "同毫秒基线（start=end）必须按旧回合身份拒绝（无时间容差）")
        # 有充分输入→idle 区间的新真实完成：不被同毫秒基线永久挡住
        # （新回合 busy 必须在基线 idle 之后，否则仍是旧 epoch 残留）
        new_busy = old_idle + 1_000
        new_done = old_idle + 600_000
        self.client.add_idle("ses_w", outcome="succeeded", at=new_done)
        self.client.append_message("ses_w", {"id": "msg_t04b", "type": "assistant",
                                             "time": {"created": new_done - 1},
                                             "content": [{"type": "text",
                                                          "text": "继续完成"}]})
        self.completed_session("ses_w", outcome="succeeded", idle=new_done,
                               busy=new_busy, active=False)
        self.clock._wall = new_done + 1_000
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {})
        att2 = self.ledger.get_attempt(att["id"])
        self.assertEqual(att2["terminal_at"], new_done,
                         "充分输入→idle 区间的新真实完成可正常确认")
        self.assertEqual(att2["terminal_kind"], "succeeded")

    def test_no_open_turn_no_interval_not_guessed(self):
        """新 session 无 epoch、无区间证据：不能猜；不得构造 start=end 的 turn。"""
        self._fresh()
        task = self.make_task(coordinator="ses_c")
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_t04c", deliverables=[], write_scope=[self.tmp])
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
                                   baseline={}, allow_send=True,
                                   actor="coordinator", reason="t04")
        self.client.append_message("ses_w", {"id": "msg_t04c", "type": "user",
                                             "time": {"created": 1_000_000}})
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": 1_001_000, "busy": 1_000_000},
                                active=False)
        self.clock._wall = 1_001_100
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {})
        att1 = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att1["terminal_at"], "无 open turn/区间证据不得猜测")
        unconf = (att1["metadata"].get("execution") or {}).get("unconfirmed")
        self.assertEqual(unconf.get("reason"), "no-attributable-start")
        self.assertEqual(self.turns_of(att["id"]), [],
                         "不得构造 start=end 绕过归属校验")


class TestIncidentT06(IncidentBase):
    """T06：复用 session 全程漏 SSE —— 旧 busy/idle 封存后，原 user + 新 idle 真实
    区间闭合 REST（保持原 session/msg，无 prompt 重发；T09 只是间接覆盖）。"""

    def test_reuse_session_full_sse_miss_rest_closed_loop(self):
        old_idle = 1794007219265
        new_started = old_idle + 60_000
        real_done = new_started + 300_000
        self.write_files({"report.md": "上一轮产物"})
        task = self.make_task(files=["report.md"])
        # 旧尝试真实完成并释放（SSE started + REST idle + 区间）
        old_start = old_idle - 600_000
        self.clock._wall = old_start + 1_000
        old_att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="creating", dispatch_state="confirmed",
            deliverables=["report.md"], write_scope=[self.tmp])
        self.start_event("ses_w", old_start, "evt_old_start")
        self.client.add_idle("ses_w", outcome="succeeded", at=old_idle)
        self.completed_session("ses_w", outcome="succeeded", idle=old_idle,
                               busy=old_start, active=False)
        self.clock._wall = old_idle + 100
        self.sup.cycle(dispatch=False)
        old2 = self.ledger.get_attempt(old_att["id"])
        self.assertEqual(old2["status"], "terminal_succeeded")
        self.assertIsNotNone(old2["site_released_at"])
        # 复用返工：只发一次 prompt；随后 **全程漏 SSE**
        self.clock._wall = new_started - 60_000
        out = self.app.rework(task["id"], feedback="继续")
        new_att = self.ledger.get_attempt(out["attempt_id"])
        self.assertEqual(len(self.client.prompt_calls), 1, "rework 只发一次 prompt")
        # 服务端投递原输入（user 消息可见）；无任何 SSE started/terminal 事件
        self.client.deliver_inbox("ses_w", at=new_started)
        self.write_files({"report.md": "真实修订"})
        self.assistant_message("ses_w", "msg_real_t06", "继续完成", real_done - 1)
        self.client.add_idle("ses_w", outcome="succeeded", at=real_done)
        self.completed_session("ses_w", outcome="succeeded", idle=real_done,
                               busy=new_started, active=False)
        self.clock._wall = real_done + 2_000
        self.sup.cycle(dispatch=False)
        n2 = self.ledger.get_attempt(new_att["id"])
        self.assertEqual(n2["dispatch_state"], "confirmed")
        self.assertEqual(n2["dispatch_msg_id"], out["message_id"], "保留原 message ID")
        self.assertEqual(n2["session_id"], "ses_w", "保留原 session")
        self.assertEqual(n2["terminal_at"], real_done,
                         "复用 session 全漏 SSE：原 user + 新 idle 真实区间由 REST 闭合")
        self.assertEqual(n2["terminal_kind"], "succeeded")
        self.assertEqual(len(self.client.prompt_calls), 1,
                         "supervisor 对账不得重发 prompt")


class TestIncidentT11Expiry(IncidentBase):
    """T11 边界（协调者澄清）：未知观察跨周期保留稳定 first_at、不重置/不重复逻辑
    通知、有下一动作；事实恢复后可推进。没有独立超时定时器：Supervisor 每周期重观察，
    即时去重的 EXECUTION_UNCONFIRMED 告警（稳定 dedupe key）持续可操作。"""

    def test_unknown_persists_across_period_deduped_then_recovers(self):
        self._fresh()
        task = self.make_task(coordinator="ses_c")
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_e", deliverables=[], write_scope=[self.tmp])
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
                                   baseline={}, allow_send=True,
                                   actor="coordinator", reason="t11")
        self.client.append_message("ses_w", {"id": "msg_e", "type": "user",
                                             "time": {"created": 1_000_000}})
        self.start_event("ses_w", 1_000_000, "evt_e_start")
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": 1_001_000, "busy": 1_000_000})

        def fail_active():
            raise Exception("active unavailable")

        with patch.object(self.client, "active_sessions", side_effect=fail_active):
            self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {},
                                     active_ok=False)
            first = self.ledger.get_attempt(att["id"])
            un1 = (first["metadata"]["execution"]["unconfirmed"])
            self.assertEqual(un1["reason"], "activity-unknown")
            first_at = un1["first_at"]
            self.assertEqual(un1["occurrences"], 1)
            self.assertTrue(un1["next_action"])
            dedupe = f"terminal-unconfirmed:{att['id']}:activity-unknown"
            rows1 = [n for n in self.ledger.list_notifications(limit=100)
                     if n["dedupe_key"] == dedupe]
            self.assertEqual(len(rows1), 1, "第一次未知只产生一条逻辑通知")
            self.assertTrue(rows1[0]["payload"].get("next_action"))
            # 推进时间越过既有观察周期后重复未知：first_at 保留、只累计次数、不新建通知
            self.clock.advance(seconds=3600)
            self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {},
                                     active_ok=False)
            second = self.ledger.get_attempt(att["id"])
            un2 = (second["metadata"]["execution"]["unconfirmed"])
            self.assertEqual(un2["first_at"], first_at, "重复未知不得重置首见时间")
            self.assertEqual(un2["occurrences"], 2, "重复未知只累计次数")
            rows2 = [n for n in self.ledger.list_notifications(limit=100)
                     if n["dedupe_key"] == dedupe]
            self.assertEqual(len(rows2), 1, "跨周期重复未知不新建/复活多余逻辑通知")
            self.assertIsNone(second["terminal_at"], "未知持续期间不得推进终态")
            self.assertIsNone(second["site_released_at"], "未知持续期间写权保持")
        # 事实恢复：active 查询成功且 worker 不活动 → 推进
        self.client.active.pop("ses_w", None)
        self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {}, active_ok=True)
        last = self.ledger.get_attempt(att["id"])
        self.assertEqual(last["terminal_at"], 1_001_000, "事实恢复后可推进")




if __name__ == "__main__":
    unittest.main()

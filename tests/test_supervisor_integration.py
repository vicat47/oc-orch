#!/usr/bin/env python3
"""OpenSpec modularize-orchestrator 3.1/3.2：Supervisor 离线集成层回归。

用 FakeClient + 可控时钟 + 真临时 SQLite 覆盖：
- 窄端口装配（client/clock 注入），构造不读取个人服务配置；
- observe/reconcile/stall 分支：权限、表单、长工具、deadline、429/403、dispatch_unknown；
- SSE 只触发 REST 对账：断流期间的终态由 REST 发现；
- G1 闭环：task create → application.dispatch(fake) → observe terminal → collect →
  verify accepted；G4 旧代次晚到、G5 pause/cancel 晚事件。

不调用真实模型、不连接真实服务；所有时间边界由 FakeClock 推进。
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.client import ApiError, TransportError
from oc_orch.composition import application
from oc_orch.config import DEFAULTS
from oc_orch.ledger import LedgerError
from oc_orch.supervisor import Supervisor
from oc_orch.util import now_ms
from unittest.mock import patch

from tests.fakes import FakeClient, FakeClock


def _cfg(**overrides) -> dict:
    cfg = dict(DEFAULTS)
    cfg["auto_collect"] = False  # 让 collect/verify 由测试显式驱动
    cfg.update(overrides)
    return cfg


class SupervisorBase(unittest.TestCase):
    """共享底座：真临时 SQLite + FakeClient + FakeClock 装配 Supervisor。"""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-sup-")
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.clock = FakeClock(wall_ms=1_000_000)
        self.client = FakeClient()
        self.cfg = _cfg()
        self.sup = Supervisor(self.tmp, cfg=self.cfg, client=self.client,
                              clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger

    def new_supervisor(self, **cfg_overrides):
        cfg = _cfg(**cfg_overrides)
        sup = Supervisor(self.tmp, cfg=cfg, client=self.client, clock=self.clock)
        self.addCleanup(sup.conn.close)
        return sup

    def make_task(self, *, coordinator: str = "ses_c", **kw):
        base = dict(project="p", title="任务", objective="目标",
                    acceptance=["交付物完整"], coordinator_session=coordinator)
        base.update(kw)
        return self.ledger.create_task(**base)

    def make_attempt(self, task_id, **kw):
        base = dict(directory=self.tmp, status="running",
                    dispatch_state="confirmed")
        base.update(kw)
        return self.ledger.create_attempt(task_id, **base)

    def notification(self, kind: str, *, dedupe: str, state: str = "pending",
                     payload: dict | None = None, task_id=None, attempt_id=None,
                     target: str = "ses_c", max_attempts: int = 3,
                     priority: int = 5):
        return self.ledger.enqueue_notification(
            kind=kind, target_session=target, dedupe_key=dedupe,
            payload=payload or {"summary": "x"}, project="p",
            task_id=task_id, attempt_id=attempt_id, priority=priority,
            max_attempts=max_attempts)


class TestAssembly(SupervisorBase):
    def test_clock_injection_reaches_ledger(self):
        """3.1：注入时钟后 supervisor 与 ledger 时间同源，持久期限可控。"""
        t0 = self.clock.now_ms()
        self.client.set_session("ses_a", outcome="succeeded",
                                time_={"updated": t0, "idle": t0, "busy": t0})
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_a")
        # 归属证据（epoch started）后，REST 对账的终态才可被确认
        self.sup.ingest_event({
            "id": "evt_clock_start", "type": "session.execution.started",
            "created": t0 - 1, "data": {"sessionID": "ses_a"},
        })
        self.clock.advance(seconds=5)
        self.sup.cycle(dispatch=False)
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["status"], "terminal_succeeded")
        # 所有持久时间戳来自 fake clock
        self.assertEqual(att["last_observed_at"], self.clock.now_ms())

    def test_default_assembly_no_service_config_read(self):
        """3.1：默认装配不依赖测试构造；注入装配不读真实服务配置。"""
        # 构造 FakeClient 过程不读任何配置；这里显式回归端口属性可用。
        self.assertTrue(hasattr(self.sup, "ledger"))
        self.assertIs(self.sup.client, self.client)
        self.assertIs(self.sup.ledger.clock, self.clock)


class TestObserveTerminal(SupervisorBase):
    def test_rest_reconcile_detects_terminal_without_sse(self):
        """OpenSpec：SSE 断流期间执行结束 → REST 对账发现终态并恢复责任。"""
        t0 = self.clock.now_ms()
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_a",
                                status="dispatched",
                                dispatch_msg_id="msg_anchor_a")
        # T06 合法证据：原输入已作为消息可见（已投递执行），
        # 全程漏 SSE 时由输入→idle 区间确认归属
        self.client.append_message("ses_a", {
            "id": "msg_anchor_a", "type": "user", "texts": "go",
            "time": {"created": t0 - 1},
        })
        # 没有任何 SSE 事件：终态只出现在 REST session 上；同时存在与结束时刻
        # 一致的 idle 消息与已投递的原输入，才能证明输入→结束完整区间（T26）。
        self.client.add_idle("ses_a", outcome="succeeded", at=t0)
        self.client.set_session("ses_a", outcome="succeeded",
                                time_={"updated": t0, "idle": t0, "busy": t0},
                                active=False)
        summary = self.sup.cycle(dispatch=True)
        self.assertGreaterEqual(summary["observed"], 1)
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["status"], "terminal_succeeded")
        # auto_collect=False（本套件配置）→ COLLECTION_REQUIRED，不写无版本 RESULT_READY
        notif = [n for n in self.ledger.list_notifications(limit=50)
                 if n["kind"] == "execution_succeeded"]
        self.assertEqual(len(notif), 1)
        self.assertEqual(notif[0]["attempt_id"], att["id"])
        self.assertEqual(notif[0]["payload"].get("classification"),
                         "COLLECTION_REQUIRED")
        # 未发送任何 prompt（对账不重投）；只有 dispatch 阶段的一次已发生
        # ——本测试直接创建尝试，因此 supervisor 不应再 prompt
        self.assertEqual(self.client.prompt_calls, [])

    def test_stream_terminal_triggers_cycle_and_duplicate_suppressed(self):
        """SSE 事件只触发 REST 对账：终态事件入账，重复事件为 no-op。"""
        t0 = self.clock.now_ms()
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_a",
                                status="dispatched")
        self.client.set_session("ses_a", time_={"updated": t0, "idle": t0, "busy": t0})
        # 归属证据：先收到本轮 started（epoch），终态事件的归属可校验
        started = self.sup.ingest_event({
            "id": "evt_start_1", "type": "session.execution.started",
            "created": t0 - 10, "data": {"sessionID": "ses_a"},
        })
        self.assertEqual(started, "started")
        env = {
            "id": "evt_term_1", "type": "session.execution.succeeded",
            "created": t0, "data": {"sessionID": "ses_a", "error": None},
        }
        urgent = self.sup.handle_stream_item({"kind": "event", "event": env,
                                              "at": t0})
        self.assertTrue(urgent, "终态事件应触发立即对账")
        self.assertEqual(self.ledger.get_attempt(att["id"])["status"],
                         "terminal_succeeded")
        # 重复投递：record_event 判重，不产生新业务
        again = self.sup.handle_stream_item({"kind": "event", "event": env,
                                             "at": t0})
        self.assertFalse(again, "重复事件不得再次触发对账")
        self.assertEqual(len(self.ledger.list_notifications(limit=50)), 1)
        # connected 项目触发对账
        self.assertTrue(self.sup.handle_stream_item({"kind": "connected", "at": t0}))


class TestObserveBlocked(SupervisorBase):
    def test_permission_blocked_via_rest(self):
        """REST 观察到权限等待（SSE 漏事件）→ blocked_permission + 唤醒。"""
        t0 = self.clock.now_ms()
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_a",
                                status="running")
        self.client.set_session("ses_a", time_={"updated": t0, "idle": t0})
        self.client.add_permission(directory=att["directory"],
                                   session_id="ses_a", pid="per_1",
                                   action="shell", resources=["rm -rf /"])
        att = self.ledger.get_attempt(att["id"])
        self.sup.observe_attempt(att, {})
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["status"], "blocked_permission")
        notif = [n for n in self.ledger.list_notifications(limit=50)
                 if n["kind"] == "blocked_permission"]
        self.assertEqual(len(notif), 1)
        self.assertEqual(notif[0]["payload"].get("permission_id"), "per_1")
        self.assertEqual(notif[0]["priority"], 10)

    def test_form_blocked_via_rest(self):
        """REST 观察到表单等待 → blocked_form + 唤醒。"""
        t0 = self.clock.now_ms()
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_a",
                                status="running")
        self.client.set_session("ses_a", time_={"updated": t0, "idle": t0})
        self.client.set_forms("ses_a", [{"id": "f1", "title": "确认范围",
                                         "fields": [{"key": "ok"}]}])
        att = self.ledger.get_attempt(att["id"])
        self.sup.observe_attempt(att, {})
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["status"], "blocked_form")
        notif = [n for n in self.ledger.list_notifications(limit=50)
                 if n["kind"] == "blocked_form"]
        self.assertEqual(len(notif), 1)
        self.assertEqual(notif[0]["payload"].get("form_id"), "f1")


class TestStall(SupervisorBase):
    def _running(self, task_id, *, started=None, tool_started=None,
                 deadline_at=None):
        t0 = started if started is not None else self.clock.now_ms()
        att = self.make_attempt(task_id, session_id="ses_a", status="running",
                                deadline_at=deadline_at)
        meta = {}
        if tool_started is not None:
            meta["active_tool"] = {"started_at": tool_started, "name": "shell"}
        self.ledger.update_attempt(att["id"], fields={"metadata": meta},
                                   actor="test", reason="stall-setup")
        self.client.set_session("ses_a", time_={"updated": t0, "idle": t0,
                                                "busy": t0}, active=True)
        return self.ledger.get_attempt(att["id"])

    def test_long_tool_overrun_stalls(self):
        """长工具超过 tool_expected_grace → stalled + LONG_TOOL_OVERRUN。"""
        self.sup = self.new_supervisor(stall_after=30, startup_grace=5,
                                       tool_expected_grace=20)
        self.ledger = self.sup.ledger
        task = self.make_task()
        t0 = self.clock.now_ms()
        att = self._running(task["id"], started=t0, tool_started=t0)
        self.clock.advance(seconds=40)  # > startup_grace 且 > tool_grace
        self.sup.observe_attempt(att, {"ses_a": {}}, active_ok=True)
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["status"], "stalled")
        stall = [n for n in self.ledger.list_notifications(limit=50)
                 if n["kind"] == "stall"]
        self.assertEqual(len(stall), 1)
        self.assertEqual(stall[0]["payload"]["classification"], "LONG_TOOL_OVERRUN")

    def test_deadline_exceeded_stalls(self):
        """超过显式 deadline → stalled + DEADLINE_EXCEEDED。"""
        self.sup = self.new_supervisor(stall_after=100, startup_grace=100)
        self.ledger = self.sup.ledger
        task = self.make_task()
        t0 = self.clock.now_ms()
        att = self._running(task["id"], started=t0,
                            deadline_at=t0 + 20 * 1000)
        self.clock.advance(seconds=40)
        self.sup.observe_attempt(att, {"ses_a": {}}, active_ok=True)
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["status"], "stalled")
        stall = [n for n in self.ledger.list_notifications(limit=50)
                 if n["kind"] == "stall"]
        self.assertEqual(stall[0]["payload"]["classification"], "DEADLINE_EXCEEDED")

    def test_startup_grace_no_stall(self):
        """冷启动窗口内无进展不算停滞。"""
        self.sup = self.new_supervisor(stall_after=5, startup_grace=30)
        self.ledger = self.sup.ledger
        task = self.make_task()
        t0 = self.clock.now_ms()
        att = self._running(task["id"], started=t0)
        self.clock.advance(seconds=10)  # < startup_grace 30
        self.sup.observe_attempt(att, {"ses_a": {}}, active_ok=True)
        self.assertEqual(self.ledger.get_attempt(att["id"])["status"], "running")
        self.assertEqual([n for n in self.ledger.list_notifications(limit=50)
                          if n["kind"] == "stall"], [])

    def test_idle_unconfirmed_no_inbox(self):
        """不活跃且无终态无 inbox → idle_unconfirmed，grace 后 enqueue_idle。"""
        self.sup = self.new_supervisor(dispatch_unknown_grace=30,
                                       stall_after=3600, startup_grace=5)
        self.ledger = self.sup.ledger
        task = self.make_task()
        t0 = self.clock.now_ms()
        att = self.make_attempt(task["id"], session_id="ses_a",
                                status="dispatched")
        self.client.set_session("ses_a", time_={"updated": t0, "idle": t0,
                                                "busy": t0})
        att = self.ledger.get_attempt(att["id"])
        self.sup.observe_attempt(att, {})
        self.assertEqual(self.ledger.get_attempt(att["id"])["status"],
                         "idle_unconfirmed")
        self.clock.advance(seconds=40)
        att = self.ledger.get_attempt(att["id"])
        self.sup.observe_attempt(att, {})
        idle = [n for n in self.ledger.list_notifications(limit=50)
                if n["kind"] == "idle_unconfirmed"]
        self.assertEqual(len(idle), 1)


class TestReconcileDispatch(SupervisorBase):
    def _att(self, task_id, *, planned="ses_planned", msg="msg_1", **kw):
        meta = {"dispatch": {"session_id_planned": planned, "prompt": "开始",
                             "create": {"session_id": planned,
                                        "directory": self.tmp, "title": "t"}}}
        return self.ledger.create_attempt(
            task_id, directory=self.tmp, session_id=planned,
            status="creating", dispatch_state="prepared", dispatch_msg_id=msg,
            metadata=meta, **kw)

    def test_no_planned_session_lost(self):
        task = self.make_task()
        att = self.ledger.create_attempt(
            task["id"], directory=self.tmp, status="creating",
            dispatch_state="unknown", metadata={})
        self.assertIsNone(self.sup.reconcile_dispatch(att))
        self.assertEqual(self.ledger.get_attempt(att["id"])["status"], "lost")

    def test_confirmed_via_inbox_without_resend(self):
        """同 ID 已受理（inbox 命中）→ 直接 confirmed，绝不换 ID 重投。"""
        task = self.make_task()
        att = self._att(task["id"])
        self.client.set_session("ses_planned", time_={})
        self.client.set_inbox("ses_planned", [{"id": "msg_1"}])
        out = self.sup.reconcile_dispatch(att)
        self.assertEqual(out["dispatch_state"], "confirmed")
        self.assertEqual(out["status"], "dispatched")
        self.assertEqual(self.client.prompt_calls, [], "已受理不得重复 prompt")

    def test_creates_missing_session_then_confirms(self):
        task = self.make_task()
        att = self._att(task["id"])
        # session 不存在，但有 create 规格 → 同 ID 创建后确认
        out = self.sup.reconcile_dispatch(att)
        self.assertEqual(self.client.create_calls[0]["id"], "ses_planned")
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertEqual(out["dispatch_state"], "confirmed")
        self.assertEqual(out["status"], "dispatched")

    def test_conflict_then_confirmed_after_recheck(self):
        """prompt 409 冲突：再查原 ID 已受理 → confirmed，不双发。"""
        task = self.make_task()
        att = self._att(task["id"])
        self.client.set_session("ses_planned", time_={})
        self.client.fail("prompt", ApiError(409, "/prompt", None, "conflict"))
        self.client.prompt_conflict_admits = True
        out = self.sup.reconcile_dispatch(att)
        self.assertEqual(out["dispatch_state"], "confirmed")
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_no_prompt_attach_confirms_without_prompt(self):
        """no_prompt 附件恢复：只确认/创建原 session，不允许真派单。"""
        # 场景 A：session 已存在（create 后崩溃在 confirmed 之前）
        task = self.make_task()
        att = self._att(task["id"], msg="msg_attach")
        meta = dict(att["metadata"])
        meta["dispatch"]["no_prompt"] = True
        self.ledger.update_attempt(att["id"], fields={"metadata": meta},
                                   actor="test", reason="no-prompt")
        self.client.set_session("ses_planned", time_={})
        out = self.sup.reconcile_dispatch(self.ledger.get_attempt(att["id"]))
        self.assertEqual(out["dispatch_state"], "confirmed")
        self.assertEqual(out["status"], "dispatched")
        self.assertEqual(self.client.prompt_calls, [])
        # 场景 B：session 不存在 → attach 不能对不存在的 session 发明归属/补创建，
        # 保持待补证不发送、不建立执行边界（不能“换个 ID 或新 session”）。
        self.ledger.release_site(att["id"], confirmed=True, external_stopped=True,
                                 actor="test", note="fake-no-prompt-scenario-finished")
        task2 = self.make_task(title="任务2")
        att2 = self._att(task2["id"], planned="ses_planned2", msg="msg_attach2")
        meta2 = dict(att2["metadata"])
        meta2["dispatch"]["no_prompt"] = True
        self.ledger.update_attempt(att2["id"], fields={"metadata": meta2},
                                   actor="test", reason="no-prompt")
        before = self.client.create_calls[:]
        out2 = self.sup.reconcile_dispatch(self.ledger.get_attempt(att2["id"]))
        self.assertIsNone(out2, "attach 目标 session 不存在：保留尝试不发送/不创建")
        self.assertEqual(self.client.create_calls, before, "不发明/创建 session")
        self.assertEqual(self.client.prompt_calls, [])

    def test_transport_error_marks_dispatch_unknown(self):
        """传输错误 → dispatch_unknown，保留原 ID 与 retry 计数。"""
        self.sup = self.new_supervisor(dispatch_max_retries=10)
        self.ledger = self.sup.ledger
        task = self.make_task()
        att = self._att(task["id"])
        self.client.set_session("ses_planned", time_={})
        self.client.fail("prompt", TransportError("/prompt", "boom"), times=2)
        out = self.sup.reconcile_dispatch(att)
        self.assertIsNone(out)
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["dispatch_state"], "unknown")
        self.assertEqual(att["status"], "dispatch_unknown")
        self.assertEqual(att["metadata"]["dispatch_retry"]["count"], 1)

    def test_retry_exhaustion_enqueues_dispatch_unknown_notification(self):
        """重试耗尽 → dispatch_unknown 通知（业务不换 ID 重投）。"""
        self.sup = self.new_supervisor(dispatch_max_retries=2)
        self.ledger = self.sup.ledger
        task = self.make_task()
        att = self._att(task["id"])
        self.client.set_session("ses_planned", time_={})
        self.client.fail("prompt", ApiError(429, "/prompt", None, "rate"),
                         times=10)
        for _ in range(3):
            self.sup.reconcile_dispatch(self.ledger.get_attempt(att["id"]))
            retry = self.ledger.get_attempt(att["id"])["metadata"]["dispatch_retry"]
            self.clock.advance(seconds=max(0, retry["next_at"] - self.clock.now_ms()) / 1000)
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["metadata"]["dispatch_retry"]["count"], 2)
        self.assertEqual(len(self.client.prompt_calls), 2, "耗尽后只对账，不再发送")
        notif = [n for n in self.ledger.list_notifications(limit=50)
                 if n["kind"] == "dispatch_unknown"]
        self.assertEqual(len(notif), 1)
        self.assertEqual(notif[0]["dedupe_key"], f"dispatch-unknown:{att['id']}")
        # 没有换新 message/id：dispatch_msg_id 保持不变
        self.assertTrue(all(p["message_id"] == att["dispatch_msg_id"]
                            for p in self.client.prompt_calls))
        self.assertEqual(self.ledger.get_attempt(att["id"])["session_id"],
                         "ses_planned")


class TestProviderRetryEvents(SupervisorBase):
    def _retry_event(self, status: int, message: str) -> dict:
        return {
            "id": "evt_r" + str(hash((status, message)) & 0xFFFF),
            "type": "session.retry.scheduled",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_a",
                     "error": {"status": status, "message": message}},
        }

    def test_429_transient_records_provider_backoff(self):
        """429 → provider state（受控 until），不切阻塞状态。"""
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_a",
                                status="running", model="p/m")
        self.client.set_session("ses_a", time_={})
        action = self.sup.ingest_event(self._retry_event(429, "rate limit"))
        self.assertEqual(action, "retry")
        att = self.ledger.get_attempt(att["id"])
        pr = att["metadata"]["provider_retry"]
        self.assertEqual(pr["count"], 1)
        self.assertEqual(pr["last_error"]["category"], "rate_limit")
        state = self.ledger.provider_states()["p/m"]
        self.assertEqual(state["category"], "rate_limit")
        # 首次重试 count=1 → delay = 30 * 2^min(1,5) = 60s
        self.assertEqual(state["until"], self.clock.now_ms() + 60 * 1000)
        self.assertEqual(att["status"], "running")

    def test_403_blocks_provider_and_alerts(self):
        """403 → provider_retry 阻塞 + provider_blocked 通知 + 告警。"""
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_a",
                                status="running", model="p/m")
        self.client.set_session("ses_a", time_={})
        action = self.sup.ingest_event(
            self._retry_event(403, "model_permission_denied"))
        self.assertEqual(action, "retry")
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["status"], "provider_retry")
        notif = [n for n in self.ledger.list_notifications(limit=50)
                 if n["kind"] == "provider_blocked"]
        self.assertEqual(len(notif), 1)
        self.assertEqual(notif[0]["payload"]["category"], "auth")
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=50)
                  if a["kind"] == "provider_auth"]
        self.assertEqual(len(alerts), 1)


class TestClosedLoopWithAppServices(SupervisorBase):
    def test_g1_dispatch_observe_collect_verify_accepted(self):
        """G1 闭环：task → application.dispatch(fake) → observe terminal →
        collect → verify accepted，持久通知/审计完整。"""
        site = os.path.join(self.tmp, "site")
        os.makedirs(site, exist_ok=True)
        task = self.ledger.create_task(
            project="p", title="闭环", objective="产出 out.txt",
            acceptance=["out.txt 存在"], coordinator_session="ses_c",
            write_scope=[site])
        app = application(self.ledger, lambda: self.client, self.cfg,
                          clock=self.clock)
        out = app.dispatch(task["id"], directory=site, coordinator="ses_c",
                           prompt="完成测试任务", deliverables=["out.txt"])
        self.assertTrue(out["ok"])
        attempt_id, session_id = out["attempt_id"], out["session_id"]
        self.assertEqual(self.client.prompt_calls[0]["message_id"],
                         out["message_id"])

        # 执行完成的事实只来自 REST；先投递本轮输入并收到 started（合法归属证据）
        t0 = self.clock.now_ms()
        self.client.deliver_inbox(session_id, at=t0)
        self.sup.ingest_event({
            "id": "evt_g1_start", "type": "session.execution.started",
            "created": t0, "data": {"sessionID": session_id},
        })
        t1 = self.clock.now_ms()
        self.client.set_session(session_id, outcome="succeeded",
                                time_={"updated": t1, "idle": t1, "busy": t0})
        self.sup.cycle(dispatch=True)
        att = self.ledger.get_attempt(attempt_id)
        self.assertEqual(att["status"], "terminal_succeeded")

        with open(os.path.join(site, "out.txt"), "w", encoding="utf-8") as f:
            f.write("ok")
        got = app.collect(attempt_id)
        self.assertEqual(got["status"], "result_collected")
        v = app.verify(attempt_id, passed=True)
        self.assertTrue(v["ok"])
        att = self.ledger.get_attempt(attempt_id)
        task = self.ledger.get_task(task["id"])
        self.assertEqual(att["status"], "accepted")
        self.assertEqual(task["status"], "accepted")
        self.assertEqual(v["verification"]["classification"], "normal_effective")

        # 持久通知：auto_collect=False → COLLECTION_REQUIRED 入队，验收后 ack
        notif = [n for n in self.ledger.list_notifications(limit=50)
                 if n["kind"] == "execution_succeeded" and n["task_id"] == task["id"]]
        self.assertEqual(len(notif), 1)
        self.assertEqual(notif[0]["state"], "acked")
        # 审计闭环
        rows = self.ledger.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action='verify.pass' "
            "AND entity='attempt' AND entity_id=?", (attempt_id,)).fetchone()[0]
        self.assertEqual(rows, 1)
        rows_task = self.ledger.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action='verify.pass' "
            "AND entity='task' AND entity_id=?", (task["id"],)).fetchone()[0]
        self.assertEqual(rows_task, 1)
        rows_observe = self.ledger.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action='terminal.accepted' "
            "AND entity_id=?", (attempt_id,)).fetchone()[0]
        self.assertGreaterEqual(rows_observe, 1)


class TestSideEffectReleaseGate(SupervisorBase):
    """D4/T12 + F8：accepted/rejected/cancelled/lost 等业务终态也不能单独证明
    外部执行停止；session/shell/子 session 三态全部 inactive 才允许自动释放。
    """

    def _terminal_accepted_with_site_held(self):
        """复用 G1 流程到 accepted，但写权仍在：终态发生时已有后台 shell/活动
        session（_after_terminal 三态门拒绝释放），验收经 verify_attempt 完成
        （不调用 services.verify 的自动释放；side effects 用有因 force 记录）。
        """
        site = os.path.join(self.tmp, "site")
        os.makedirs(site, exist_ok=True)
        task = self.ledger.create_task(
            project="p", title="side-release", objective="产出 out.txt",
            acceptance=["out.txt 存在"], coordinator_session="ses_c",
            write_scope=[site])
        app = application(self.ledger, lambda: self.client, self.cfg,
                          clock=self.clock)
        out = app.dispatch(task["id"], directory=site, coordinator="ses_c",
                           prompt="完成测试任务", deliverables=["out.txt"])
        attempt_id, session_id = out["attempt_id"], out["session_id"]
        t0 = self.clock.now_ms()
        self.client.deliver_inbox(session_id, at=t0)
        self.sup.ingest_event({
            "id": "evt_gate_start", "type": "session.execution.started",
            "created": t0, "data": {"sessionID": session_id},
        })
        t1 = self.clock.now_ms()
        self.client.set_session(session_id, outcome="succeeded",
                                time_={"updated": t1, "idle": t1, "busy": t0})
        # 终态确认要求 worker session 不活动；用“目录内 running shell”让写权门
        # 保持（三态之一 active → 不释放），验收后才由 finalize 复核。
        self.client.add_shell(directory=site,
                              metadata={"sessionID": session_id})
        self.sup.cycle(dispatch=True)
        att = self.ledger.get_attempt(attempt_id)
        self.assertEqual(att["status"], "terminal_succeeded")
        self.assertIsNone(att["site_released_at"],
                          "后台 shell 活动时终态不自动释放写现场")
        with open(os.path.join(site, "out.txt"), "w", encoding="utf-8") as f:
            f.write("ok")
        app.collect(attempt_id)
        att = self.ledger.get_attempt(attempt_id)
        self.assertEqual(att["status"], "result_collected")
        # 验收（有因 force 记录 side effects 未满足项；services.verify 的自动
        # 释放不走，保证 accepted 后写权仍由三态门决定）。
        v = app.results.verify(attempt_id, result="pass", force=True,
                               reason="后台 shell 运行中，验收仅记录未满足项")
        self.assertTrue(v["ok"])
        att = self.ledger.get_attempt(attempt_id)
        self.assertEqual(att["status"], "accepted")
        self.assertIsNone(att["site_released_at"],
                          "后台 shell 未确认停止，验收也不自动释放写现场")
        return attempt_id, session_id, site

    def test_accepted_with_running_shell_and_active_session_kept(self):
        """F8：accepted 分支不得绕过三态检查直接释放。"""
        attempt_id, session_id, site = self._terminal_accepted_with_site_held()
        # accepted 分支同样必须三态检查：session 活动 + shell 运行 → 保持写权
        self.client.active[session_id] = self.client.sessions[session_id]
        self.sup.finalize_terminals(self.client.active_sessions())
        att = self.ledger.get_attempt(attempt_id)
        self.assertIsNone(att["site_released_at"],
                          "accepted 不证明外部停止：session/shell 活动时保持写权")
        # 全部停止事实确认后才自动释放
        self.client.clear_active()
        self.client._shells_store.clear()
        self.sup.finalize_terminals(self.client.active_sessions())
        att = self.ledger.get_attempt(attempt_id)
        self.assertIsNotNone(att["site_released_at"],
                             "三态全部 inactive 后才允许自动释放")

    def test_lost_with_active_children_kept(self):
        """lost 也不自动释放：活动子 session 未确认停止。"""
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_loss",
                                status="running")
        self.client.set_active(["ses_loss"])
        child = {"id": "ses_child1", "outcome": None, "time": {},
                 "location": {"directory": self.tmp}}
        self.client.children["ses_loss"] = [child]
        self.client.active["ses_child1"] = child
        self.ledger.transition_attempt(att["id"], "lost", reason="session-404",
                                       actor="supervisor")
        self.sup.finalize_terminals(self.client.active_sessions())
        att = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att["site_released_at"],
                          "活动子 session 存在时 lost 不得自动释放")
        self.client.active.pop("ses_child1", None)
        self.client.clear_active()
        self.client.children.pop("ses_loss", None)
        self.sup.finalize_terminals(self.client.active_sessions())
        att = self.ledger.get_attempt(att["id"])
        self.assertIsNotNone(att["site_released_at"],
                             "子 session 停止事实确认后 lost 可释放")

    def test_cancelled_with_unknown_query_kept(self):
        """T12：查询失败是 unknown 而非无活动，保留写权并有告警。"""
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_cx",
                                status="running")
        self.ledger.transition_attempt(att["id"], "cancelled", reason="manual")
        self.client.fail("active_sessions",
                         Exception("active endpoint unavailable"))
        self.sup.finalize_terminals({})  # refresh_side_effects 的首次 active 查询失败
        att = self.ledger.get_attempt(att["id"])
        self.assertIsNone(att["site_released_at"],
                          "外部活动查询失败=unknown，不得释放写权")
        side = (att["metadata"] or {}).get("side_effect_checks") or {}
        self.assertEqual(side.get("session", {}).get("state"), "unknown")
        unconf = [n for n in self.ledger.list_notifications(limit=50)
                  if n["kind"] == "execution_unconfirmed"]
        self.assertTrue(any("side-effects-unknown" in n["dedupe_key"]
                            for n in unconf), "unknown 必须留下去重告警")


    def test_side_effect_observation_preserves_concurrent_metadata(self):
        """审查6：refresh_side_effects 经 record_side_effect_observation 窄 merge，
        不擦除 I/O 期间另一连接并发写入的 collection/stop 等非观察子字段。"""
        from oc_orch.db import init as _db_init
        from oc_orch.ledger import Ledger
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_b",
                                status="result_collected")
        att_id = att["id"]
        # 第二连接作为并发写入者（同一 home，WAL 双连接真实交错）
        ledger2 = Ledger(_db_init(self.tmp), self.tmp, clock=self.clock)
        self.addCleanup(ledger2.conn.close)

        def shells_with_concurrent_write(directory):
            # 观察 I/O 期间另一进程写入 collection 恢复证据与 stop 事实
            fresh = ledger2.get_attempt(att_id)
            meta = {**fresh["metadata"],
                    "collection": {"state": "complete",
                                   "recovery_execution": {"schema": "r", "x": 1}}}
            ledger2.update_attempt(att_id, fields={"metadata": meta,
                                                   "last_observed_at":
                                                   self.clock.now_ms()},
                                   actor="concurrent-writer", reason="collect-io")
            return []

        with patch.object(self.client, "shells",
                          side_effect=shells_with_concurrent_write):
            self.sup.finalize_terminals({})
        att_after = self.ledger.get_attempt(att_id)
        meta = att_after["metadata"]
        self.assertEqual(
            meta.get("collection", {}).get("recovery_execution", {}).get("x"), 1,
            "并发写入的 collection 子字段不得被观察整块覆盖")
        self.assertIsNotNone(meta.get("side_effect_checks"),
                             "观察子字段必须已持久化")
        self.assertIn("active_shells", meta)
        # 审计证明 narrow merge 只写白名单观察子字段
        rows = self.ledger.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action="
            "'attempt.side-effect-observation' AND entity_id=?", (att_id,)).fetchone()
        self.assertGreaterEqual(rows[0], 1)


class TestLateEventsDoNotOverwrite(SupervisorBase):
    def test_g4_old_attempt_late_terminal_and_verify_rejected(self):
        """G4：旧代次晚到结果不覆盖当前尝试，验收被 NOT_CURRENT 拒绝。"""
        task = self.make_task()
        a1 = self.make_attempt(task["id"], session_id="ses_a",
                               status="running")
        self.ledger.transition_attempt(a1["id"], "terminal_succeeded",
                                       reason="first")
        self.ledger.release_site(a1["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        a2 = self.make_attempt(task["id"], session_id="ses_b",
                               status="dispatched")
        self.ledger.transition_attempt(a2["id"], "running", reason="new-worker-started")
        self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"],
                         a2["id"])

        # REST 观察到旧 session 终态晚到：不改当前尝试
        t0 = self.clock.now_ms()
        self.client.set_session("ses_a", outcome="succeeded",
                                time_={"updated": t0, "idle": t0, "busy": t0})
        self.sup.observe_attempt(self.ledger.get_attempt(a1["id"]), {})
        a1b = self.ledger.get_attempt(a1["id"])
        self.assertEqual(a1b["status"], "terminal_succeeded")
        self.assertEqual(self.ledger.get_attempt(a2["id"])["status"], "running")
        self.assertEqual(self.ledger.get_task(task["id"])["status"], "running")
        self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"], a2["id"])
        self.assertIsNone(self.ledger.get_attempt(a2["id"])["site_released_at"])

        # 旧代次验收必须被拒绝
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.submit_verification(
                a1["id"], verification={}, result="pass", reason="late")
        self.assertEqual(ctx.exception.code, "NOT_CURRENT")

    def test_g5_cancel_and_pause_late_events_keep_intent(self):
        """G5：取消/暂停后晚到运行/终态事件不复活任务、不建新尝试。"""
        task = self.make_task()
        att = self.make_attempt(task["id"], session_id="ses_a",
                                status="running")
        self.client.set_session("ses_a", time_={"idle": 0, "updated": 0})
        self.ledger.close_task(task["id"], reason="user-cancel")
        t0 = self.clock.now_ms()
        self.sup.ingest_event({
            "id": "evt_start_late", "type": "session.execution.started",
            "created": t0, "data": {"sessionID": "ses_a"},
        })
        self.sup.ingest_event({
            "id": "evt_term_late", "type": "session.execution.succeeded",
            "created": t0, "data": {"sessionID": "ses_a", "error": None},
        })
        task = self.ledger.get_task(task["id"])
        self.assertEqual(task["status"], "cancelled")
        self.assertEqual(len(self.ledger.list_attempts(task_id=task["id"])), 1)
        # 新契约（D9）：人工取消下不产生正常成功通知（入队即抑制，审计留痕）；
        # 抑制后没有待投递的责任被取消任务消耗
        active_after = [n for n in self.ledger.list_notifications(limit=50)
                        if n["state"] in ("pending", "sent", "inflight")
                        and n["task_id"] == task["id"]]
        self.assertEqual(active_after, [], "已取消任务不得产生新的正常成功通知")
        suppressed = self.ledger.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action="
            "'notification.suppressed-manual-control' AND entity_id=?",
            (att["id"],)).fetchone()[0]
        self.assertGreaterEqual(suppressed, 1, "抑制必须留审计")
        self.sup.dispatch_notifications()
        active = [n for n in self.ledger.list_notifications(limit=50)
                  if n["state"] in ("pending", "sent", "inflight")]
        self.assertEqual(active, [], "已取消任务的通知应被 ack，不继续投递")

        # pause 后晚到事件同样不推进任务
        task2 = self.make_task(title="pause-任务")
        att2 = self.make_attempt(task2["id"], session_id="ses_b",
                                 status="running")
        self.client.set_session("ses_b", time_={"idle": 0, "updated": 0})
        self.ledger.transition_task(task2["id"], "paused", reason="manual")
        self.sup.ingest_event({
            "id": "evt_term_pause", "type": "session.execution.succeeded",
            "created": t0, "data": {"sessionID": "ses_b", "error": None},
        })
        self.assertEqual(self.ledger.get_task(task2["id"])["status"], "paused")


if __name__ == "__main__":
    unittest.main(verbosity=2)

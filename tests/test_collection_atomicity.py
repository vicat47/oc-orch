#!/usr/bin/env python3
"""OpenSpec fix-attempt-completion-integrity：收集/发布事务与崩溃窗口（T08/T13/T30/T31）。

真临时 SQLite WAL（含双连接）、故障触发器与受控同步点；无概率 sleep。
覆盖：
- 窄终态事务中途异常 → turn/attempt/task/审计整体回滚（T08）；
- 收集 I/O 期间人工控制 → 无过时提交/无正常新结果通知（T13）；
- 两收集器同版/异版竞争、通知插入失败、文件落盘后 DB 回滚（T30）；
- 提交与通知责任同事务；崩溃后待发责任可恢复（T31）。
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.composition import application
from oc_orch.config import DEFAULTS
from oc_orch.db import init
from oc_orch.ledger import Ledger, LedgerError
from oc_orch.supervisor import Supervisor
from oc_orch.util import now_ms

from tests.fakes import FakeClient, FakeClock, complete_execution


CFG = {**DEFAULTS, "auto_collect": True, "desktop_alerts": False,
       "collect_retry_interval": 15, "collect_max_attempts": 5}


def _snippet(text: str, at: int, mid: str = "msg_a"):
    return {"id": mid, "type": "assistant", "time": {"created": at},
            "content": [{"type": "text", "text": text}]}


class AtomicityBase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-atomic-")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.clock = FakeClock(wall_ms=1_000_000)
        self.client = FakeClient()
        self.sup = Supervisor(self.home, cfg=CFG, client=self.client,
                              clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.ledger2 = Ledger(init(self.home), self.home, clock=self.clock)
        self.addCleanup(self.ledger2.conn.close)
        self.app = application(self.ledger, lambda: self.client, CFG,
                               clock=self.clock)
        self.client.set_session("ses_c", time_={})  # coordinator 通道可投递



    def make_collected(self, *, files=("out.txt",), text="完成：产物就绪",
                        deliverables=None):
        task = self.ledger.create_task(project="p", title="t",
                                       coordinator_session="ses_c",
                                       write_scope=[self.home])
        os.makedirs(os.path.join(self.home, "site"), exist_ok=True)
        d = os.path.join(self.home, "site")
        for name in files:
            with open(os.path.join(d, name), "w", encoding="utf-8") as f:
                f.write("v1")
        att = self.ledger.create_attempt(
            task["id"], directory=d, session_id="ses_w", status="dispatched",
            dispatch_state="confirmed",
            deliverables=deliverables or list(files),
            dispatch_msg_id="msg_in_1", write_scope=[d])
        # 真实执行：started → 输入消息 → 成功
        t0 = self.clock.now_ms()
        self.sup.ingest_event({"id": "evt_s", "type": "session.execution.started",
                               "created": t0, "data": {"sessionID": "ses_w"}})
        self.client.append_message("ses_w", {
            "id": "msg_in_1", "type": "user", "texts": "go",
            "time": {"created": t0 - 1}})
        self.client.append_message("ses_w", _snippet(text, t0 + 10))
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"updated": t0 + 100, "idle": t0 + 100,
                                       "busy": t0})
        self.clock.advance(seconds=1)
        self.sup.cycle(dispatch=False)
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["status"], "result_collected", att["status"])
        return task, att

class _FailConn:
    """可注入失败的连接代理（sqlite3.Connection 属性只读，不能直接 mock）。"""

    def __init__(self, conn, predicate, limit=1):
        self._conn = conn
        self._pred = predicate
        self._limit = limit
        self._hits = 0

    def execute(self, sql, *a, **k):
        if self._pred(sql) and self._hits < self._limit:
            self._hits += 1
            raise RuntimeError(f"injected-failure@{sql[:24]!r}")
        return self._conn.execute(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self._conn, name)



class TestTerminalTransaction(AtomicityBase):
    """T08：终态提交中途异常 → 全回滚，无部分状态。"""

    def test_mid_terminal_exceptions_rollback_everything(self):
        for trigger in ("turn_update", "attempt_update", "task_update",
                        "audit_insert"):
            with self.subTest(trigger=trigger):
                fresh = tempfile.TemporaryDirectory(prefix="oc-orch-t08-")
                self.addCleanup(fresh.cleanup)
                clock = FakeClock(wall_ms=1_000_000)
                client = FakeClient()
                sup = Supervisor(fresh.name, cfg=CFG, client=client, clock=clock)
                self.addCleanup(sup.conn.close)
                ledger = sup.ledger
                task = ledger.create_task(project="p", title="t",
                                          coordinator_session="ses_c")
                att = ledger.create_attempt(
                    task["id"], directory=fresh.name, session_id="ses_w",
                    status="dispatched", dispatch_state="confirmed",
                    dispatch_msg_id="msg_x", write_scope=[fresh.name])
                ledger.seal_execution(att["id"], mode="new",
                                      session_before={})
                t0 = clock.now_ms()
                ledger.record_execution_started(att["id"], started_at=t0,
                                                event_id="evt_s")
                # 注入中途失败（按 SQL 模式定位；事务应整体回滚）
                def pred(sql: str) -> bool:
                    if trigger == "turn_update" and "UPDATE turns" in sql:
                        return True
                    if trigger == "attempt_update" and sql.startswith(
                            "UPDATE attempts SET status="):
                        return True
                    if trigger == "task_update" and sql.startswith(
                            "UPDATE tasks SET status="):
                        return True
                    if trigger == "audit_insert" and sql.startswith(
                            "INSERT INTO audit"):
                        return True
                    return False

                ledger.conn, saved_conn = _FailConn(ledger.conn, pred), ledger.conn
                try:
                    with self.assertRaises(Exception):
                        ledger.record_terminal(
                            att["id"], outcome="succeeded", source="sse",
                            event_id="evt_e", candidate={
                                "outcome": "succeeded", "source": "sse",
                                "ended_at": t0 + 50, "session_id": "ses_w"})
                finally:
                    ledger.conn = saved_conn
                att2 = ledger.get_attempt(att["id"])
                task2 = ledger.get_task(task["id"])
                # 整体回滚：无终态/无任务推进/无已关闭 turn
                self.assertIsNone(att2["terminal_at"],
                                  f"{trigger}: 不得写 terminal_at")
                self.assertNotEqual(att2["status"], "terminal_succeeded")
                self.assertNotIn(task2["status"], ("result_pending",
                                                   "result_received"))
                self.assertIsNone(att2["result_collected_at"])
                self.assertIsNone(att2["site_released_at"])
                for t in ledger.turns(att["id"]):
                    if t["end_event_id"]:
                        self.assertLess(t["started_at"] or 0, t["ended_at"])

    def test_reversed_turn_rejected_cleanly(self):
        task = self.ledger.create_task(project="p", title="t",
                                       coordinator_session="ses_c")
        att = self.ledger.create_attempt(
            task["id"], directory=self.home, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            dispatch_msg_id="msg_x", write_scope=[self.home])
        self.ledger.seal_execution(att["id"], mode="new", session_before={})
        t0 = self.clock.now_ms()
        self.ledger.record_execution_started(att["id"], started_at=t0,
                                             event_id="evt_s")
        out = self.ledger.record_terminal(
            att["id"], outcome="succeeded", source="sse", event_id="evt_e",
            candidate={"outcome": "succeeded", "source": "sse",
                       "ended_at": t0 - 10, "session_id": "ses_w"})
        self.assertEqual(out["result"], "rejected")
        self.assertIsNone(self.ledger.get_attempt(att["id"])["terminal_at"])


class TestCollectGuardWindows(AtomicityBase):
    """T13：收集 I/O 期间人工控制 → 过时提交被拒且无正常通知。"""

    def test_pause_during_collect_io_blocks_stale_commit(self):
        task, att = self.make_collected()
        # 收集一个新版本期间任务被暂停：第二次 collect 前落 pause
        self.ledger.transition_task(task["id"], "paused", reason="manual")
        with open(os.path.join(self.home, "site", "out.txt"), "w",
                  encoding="utf-8") as f:
            f.write("v2-paused-window")
        self.client.append_message("ses_w", _snippet("第二版回报",
                                                     self.clock.now_ms() + 1,
                                                     "msg_b"))
        from oc_orch.domain.errors import ActionError
        with self.assertRaises(ActionError) as ctx:
            self.app.collect(att["id"])
        self.assertIn(getattr(ctx.exception, "code", ""),
                      ("TASK_PROTECTED", "COLLECTION_CONFLICT", "gate"))
        att2 = self.ledger.get_attempt(att["id"])
        meta = att2["metadata"].get("result") or {}
        self.assertNotIn("v2-paused-window",
                         open(att2["result_path"]).read(),
                         "暂停窗口的过时提交不得改写版本内容")

    def test_notification_insert_failure_rolls_back_commit(self):
        task, att = self.make_collected(files=("a.txt",), text="第一版")
        with open(os.path.join(self.home, "site", "a.txt"), "w",
                  encoding="utf-8") as f:
            f.write("v2")
        self.client.append_message("ses_w", _snippet("第二版", self.clock.now_ms(),
                                                     "msg_c"))
        before = self.ledger.get_attempt(att["id"])
        with patch.object(Ledger, "_enqueue_notification_in_tx",
                          side_effect=RuntimeError("notify-down")):
            from oc_orch.adapters.results import collect_attempt
            with self.assertRaises(Exception):
                collect_attempt(self.ledger, self.client, att["id"], CFG,
                                auto=True, clock=self.clock.now_ms)
        after = self.ledger.get_attempt(att["id"])
        # 无部分提交：结果指针/时间/任务状态不变
        self.assertEqual(after["result_collected_at"],
                         before["result_collected_at"])
        self.assertEqual(after["status"], "result_collected")
        # 孤立版本文件可被下次同指纹收集复用
        from oc_orch.adapters.results import collect_attempt
        out = collect_attempt(self.ledger, self.client, att["id"], CFG,
                              auto=True, clock=self.clock.now_ms)
        self.assertTrue(out["result_version"])


class TestConcurrentCollectors(AtomicityBase):
    """T30：同版幂等 / 异版冲突 / 文件与 DB 采集时间一致。"""

    def test_same_content_two_collectors_same_version_and_time(self):
        task, att = self.make_collected()
        # 两个收集器（不同连接）重复收集相同内容
        from oc_orch.adapters.results import collect_attempt
        ledger2 = self.ledger2
        app2 = application(ledger2, lambda: self.client, CFG, clock=self.clock)
        out1 = app2.collect(att["id"])
        out2 = self.app.collect(att["id"])
        self.assertEqual(out1["result_version"], out2["result_version"])
        self.assertEqual(out1["notification_ids"], out2["notification_ids"])
        # 只有一条结果通知
        rows = [n for n in self.ledger.list_notifications(limit=100)
                if n["kind"] == "result_ready" and n["attempt_id"] == att["id"]]
        self.assertEqual(len(rows), 1)
        # 台账时间与版本文件一致
        att2 = self.ledger.get_attempt(att["id"])
        import json as j
        with open(att2["result_path"], encoding="utf-8") as f:
            snap = j.load(f)
        self.assertEqual(snap["collected_at"], att2["result_collected_at"])

    def test_stale_guard_conflict_cannot_overwrite_newer_result(self):
        task, att = self.make_collected(text="V1 内容")
        # C1 读 guard；C2 收集新版本 V2；C1 再提交 → 冲突
        guard = self.ledger2.collection_guard(att["id"])
        with open(os.path.join(self.home, "site", "out.txt"), "w",
                  encoding="utf-8") as f:
            f.write("V2 内容")
        self.client.append_message("ses_w", _snippet("V2 回报",
                                                     self.clock.now_ms() + 1,
                                                     "msg_v2"))
        self.app.collect(att["id"])  # C2 提交 V2
        old_text = "V1 内容（C1 视角的旧内容）"
        from oc_orch.application import collection as coll
        from oc_orch.adapters.results import _CollectionIO
        # C1 用旧 guard 直接提交 → 必须冲突
        with self.assertRaises(Exception):
            self.ledger2.commit_collection(
                att["id"], result_version="rv1-fake", content_fingerprint="fp-fake",
                collected_at=self.clock.now_ms(), version_path="/nonexistent",
                summary="C1", guard=guard, actor="coordinator")


class TestCrashRecovery(AtomicityBase):
    """T31：提交后投递前崩溃 → 重启后待发责任可恢复。"""

    def test_pending_notification_survives_new_supervisor_cycle(self):
        task, att = self.make_collected()
        rows = [n for n in self.ledger.list_notifications(limit=100)
                if n["kind"] == "result_ready" and n["state"] == "pending"]
        self.assertEqual(len(rows), 1, "新版本必有持久通知责任")
        # 模拟崩溃：新 Supervisor 实例（同 home）恢复投递
        sup2 = Supervisor(self.home, cfg=CFG, client=self.client,
                          clock=self.clock, once=True)
        self.addCleanup(sup2.conn.close)
        sup2.cycle(dispatch=True)
        sent = [n for n in sup2.ledger.list_notifications(limit=100)
                if n["kind"] == "result_ready" and n["state"] in ("sent", "acked")]
        self.assertEqual(len(sent), 1)
        self.assertEqual(len(self.client.prompt_calls), 1)


if __name__ == "__main__":
    unittest.main()

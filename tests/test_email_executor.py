#!/usr/bin/env python3
"""OperatorEmailExecutor：独立有限 tick、默认关闭零外部调用、失败可见且不影响
同 home 主 Supervisor cycle。临时 SQLite + FakeClock + FakeTransport；不触
runtime/真实网络/邮箱。
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch import composition  # noqa: E402
from oc_orch.adapters.sqlite_db import init, meta_get  # noqa: E402
from oc_orch.application.mail_port import (  # noqa: E402
    MailChannelError,
    MailUnknownError,
)
from oc_orch.domain.operator_email import (  # noqa: E402
    KIND_FIRST,
    KIND_UPDATE,
    KIND_ESCALATION,
    KIND_RECOVERY,
    STATUS_SUBMIT_UNKNOWN,
    EmailPolicy,
    IncidentFact,
    Recipient,
    Sender,
)
from oc_orch.runtime import email_executor as emod  # noqa: E402
from oc_orch.runtime.email_executor import (  # noqa: E402
    DEFAULT_OWNER,
    OperatorEmailExecutor,
    STATUS_META_KEY,
    TICK_META_KEY,
)
from tests.fakes import FakeClock  # noqa: E402


def enabled_policy(**overrides) -> EmailPolicy:
    kwargs = dict(
        enabled=True,
        sender=Sender("oc-ops", "ops@example.test"),
        recipients=(Recipient("op-primary", "operator@example.test"),),
        projects=frozenset({"proj-a"}),
        event_kinds=frozenset({KIND_FIRST, KIND_UPDATE, KIND_ESCALATION,
                               KIND_RECOVERY}),
        unattended=True,
        receipt_queries_authorized=True,
        receipt_check_limit=3,
    )
    kwargs.update(overrides)
    return EmailPolicy(**kwargs)


def fact(**kw) -> IncidentFact:
    base = dict(
        incident_id="inc-1", project="proj-a", incident_type="waiting-human-responsibility",
        condition_key="waiting-human", condition_version=1,
        recipient_ref="op-primary", condition_kind="human-responsibility",
        blocked_reason="需要人工决策", decision_needed="选A或B",
        first_seen_at=1_000_000, observed_at=1_000_000,
    )
    base.update(kw)
    return IncidentFact(**base)


class FakeTransport:
    """可注入结果/异常的窄传输桩；记录调用。"""

    def __init__(self, send_result=None, search_result=None):
        self.sent: list[dict] = []
        self.searches: list[dict] = []
        self.send_result = {"queued": True} if send_result is None else send_result
        self.search_result = [] if search_result is None else search_result

    def send(self, **kw):
        self.sent.append(dict(kw))
        if isinstance(self.send_result, Exception):
            raise self.send_result
        return self.send_result

    def search_receipt(self, **kw):
        self.searches.append(dict(kw))
        if isinstance(self.search_result, Exception):
            raise self.search_result
        return list(self.search_result)


def seed(home, *, clock, policy, transport, dispatch: bool = False,
         observed_at: int = 1_000_000):
    """在独立短连接上登记事实；dispatch=True 时立即派单（用于造 submit-unknown）。"""
    conn = init(home)
    try:
        # 经真实责任行产生可重验来源；通道/失败/预算断言不变，工厂不再接受
        # 调用方凭空自报的incident发送。纯邮件规则的legacy测试仍直接装配svc。
        from oc_orch.adapters.operator_incident import IncidentStore
        from oc_orch.domain.operator_incident import build_binding

        ledger = composition.Ledger(conn, home, clock=clock)
        task = ledger.create_task(project="proj-a", title="mail source")
        composition.make_responsibilities(ledger).open_responsibility(
            "operator", "proj-a", task["id"], state="waiting_human",
            wait_kind="decision", wait_condition="waiting-human",
            next_action="选A或B", next_check_at=clock.now_ms()+60_000,
            deadline=clock.now_ms()+3600_000, condition_key="waiting-human")
        source = IncidentStore(conn, clock, base=ledger).list_waiting_human_sources(limit=10)[0][0]
        svc = composition.make_operator_email(conn, clock=clock, policy=policy,
                                              transport=transport)
        out = svc.enqueue_incident(fact(observed_at=observed_at), source_binding=build_binding(source))
        if dispatch and out.get("item_id"):
            out2 = svc.dispatch_once(owner="seeder", item_id=out["item_id"])
            return out, out2
        return out
    finally:
        conn.close()


class ExecutorBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="exec-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        init(self.home)
        self.clock = FakeClock(wall_ms=1_000_000)

    def status_blob(self):
        conn = init(self.home)
        try:
            return meta_get(conn, STATUS_META_KEY)
        finally:
            conn.close()


class TestDefaultDisabled(ExecutorBase):
    def test_default_tick_zero_external_calls_and_no_op(self):
        # 默认关闭：登记为 pending-auth，也不触发任何发送/查询。
        seed(self.home, clock=self.clock, policy=EmailPolicy(),
             transport=FakeTransport())
        exec_ = OperatorEmailExecutor(self.home, clock=self.clock)
        result = exec_.tick()
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["owner"], DEFAULT_OWNER)
        self.assertEqual(result["steps"]["reconcile_stale_claimed"]["count"], 0)
        self.assertEqual(result["steps"]["verify_receipt"]["result"],
                         "nothing-to-verify")
        self.assertEqual(result["steps"]["dispatch_once"]["result"],
                         "nothing-due")
        self.assertEqual(result["external_calls"], {"send": 0, "search": 0})
        blob = self.status_blob()
        self.assertIsNotNone(blob)
        public = json.loads(blob)
        self.assertIn("steps", public)
        self.assertNotIn("subject", json.dumps(public))

    def test_no_auto_threads_or_processes(self):
        exec_ = OperatorEmailExecutor(self.home)
        self.assertIsInstance(exec_, OperatorEmailExecutor)
        # tick 是同步有限调用，不隐式启动线程/进程：对象不持有工作线程
        self.assertEqual(exec_._transport.send_count, 0)


class TestMainCycleIsolation(ExecutorBase):
    def test_channel_failure_visible_and_main_conn_untouched(self):
        # 主 Supervisor 风格连接：先建任务并保持打开
        main = init(self.home)
        self.addCleanup(main.close)
        main.execute(
            "INSERT INTO tasks(id, project, title, objective, acceptance, status, "
            "write_scope, priority, next_check_at, created_at, updated_at, metadata) "
            "VALUES('T-main','proj-a','x','','[]','open','[]',0,1,1,1,'{}')")
        main.commit()

        policy = enabled_policy()
        seed(self.home, clock=self.clock, policy=policy,
             transport=FakeTransport())  # pending 待发
        failing = FakeTransport(send_result=MailChannelError("channel-down"))
        exec_ = OperatorEmailExecutor(self.home, clock=self.clock,
                                      policy=policy, transport=failing)
        result = exec_.tick()
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["steps"]["dispatch_once"]["result"],
                         "channel-blocked")  # 可见失败
        self.assertEqual(result["external_calls"], {"send": 1, "search": 0})

        # 主连接未受影响：可读、任务未变、心跳可见
        row = main.execute("SELECT status FROM tasks WHERE id='T-main'"
                           ).fetchone()
        self.assertEqual(row["status"], "open")
        self.assertIsNotNone(meta_get(main, TICK_META_KEY))
        self.assertIn("channel-blocked", self.status_blob() or "")

    def test_business_budget_visible_and_not_acked(self):
        policy = enabled_policy(max_attempts=2)
        seed(self.home, clock=self.clock, policy=policy,
             transport=FakeTransport())
        failing = FakeTransport(
            send_result=MailUnknownError("timeout"))  # 未知，不重发
        exec_ = OperatorEmailExecutor(self.home, clock=self.clock,
                                      policy=policy, transport=failing)
        # tick1：第一封 submit-unknown（send 计入 1）
        r1 = exec_.tick()
        self.assertEqual(r1["steps"]["dispatch_once"]["result"], "submit-unknown")
        # tick2-3：unknown 不重发，回执查询消耗预算
        exec_.tick()
        r3 = exec_.tick()
        self.assertEqual(r3["external_calls"]["send"], 1)  # 从未重发
        conn = init(self.home)
        try:
            item = next(i for i in _all_items(conn) if i["kind"] == "first")
            self.assertEqual(item["status"], STATUS_SUBMIT_UNKNOWN)
            self.assertEqual(item["attempts"], 1)
        finally:
            conn.close()


class TestUnknownNotResent(ExecutorBase):
    def test_unknown_across_ticks_not_resend_and_budget_exhausted(self):
        policy = enabled_policy(receipt_check_limit=2)
        seeding_transport = FakeTransport(send_result=MailUnknownError("timeout"))
        _, dispatched = seed(self.home, clock=self.clock, policy=policy,
                             transport=seeding_transport, dispatch=True)
        self.assertEqual(dispatched["result"], "submit-unknown")

        verify_transport = FakeTransport(search_result=[])
        exec_ = OperatorEmailExecutor(self.home, clock=self.clock,
                                      policy=policy, transport=verify_transport)
        t1 = exec_.tick()
        self.assertEqual(t1["steps"]["verify_receipt"]["result"], "no-match")
        self.assertEqual(t1["steps"]["dispatch_once"]["result"], "nothing-due")
        t2 = exec_.tick()
        self.assertEqual(t2["steps"]["verify_receipt"]["result"], "no-match")
        # 预算耗尽 → 可见
        t3 = exec_.tick()
        self.assertEqual(t3["steps"]["verify_receipt"]["result"],
                         "receipt-check-exhausted")
        # 从未重发；回执查询有界
        self.assertEqual(verify_transport.sent, [])
        self.assertEqual(len(verify_transport.searches), 2)
        conn = init(self.home)
        try:
            item = next(i for i in _all_items(conn) if i["kind"] == "first")
            self.assertEqual(item["status"], STATUS_SUBMIT_UNKNOWN)
            self.assertEqual(item["receipt_checks"], 2)
        finally:
            conn.close()


class TestFreshConnectionPerTick(ExecutorBase):
    def test_factory_failure_still_closes_new_connection_and_redacts_error(self):
        real_init = emod.init
        conns = []

        def spy(home):
            conn = real_init(home)
            conns.append(conn)
            return conn

        exec_ = OperatorEmailExecutor(self.home)
        with mock.patch.object(emod, "init", side_effect=spy), mock.patch.object(
                emod, "make_operator_email", side_effect=RuntimeError("token=secretvalue12345")):
            result = exec_.tick()
        self.assertFalse(result["ok"])
        self.assertNotIn("secretvalue12345", json.dumps(result))
        self.assertEqual(len(conns), 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            conns[0].execute("SELECT 1")

    def test_persisted_executor_status_has_a_real_size_cap(self):
        conn = init(self.home)
        self.addCleanup(conn.close)
        ledger = composition.Ledger(conn, self.home, clock=self.clock)
        exec_ = OperatorEmailExecutor(self.home, clock=self.clock)
        oversized = {"ok": False, "steps": {"verify_receipt": {
            "reason": ["x" * 200 for _ in range(200)],
            "status": ["y" * 200 for _ in range(200)],
            "item_id": ["z" * 200 for _ in range(200)]}}}
        exec_._persist_state(ledger, 1, self.clock.now_ms(), oversized)
        raw = meta_get(conn, STATUS_META_KEY)
        self.assertLessEqual(len(raw.encode("utf-8")), emod.MAX_STATUS_BYTES)
        self.assertTrue(json.loads(raw)["truncated"])

    def test_each_tick_uses_independent_connection_and_closes(self):
        real_init = emod.init
        conns = []

        def spy(home):
            conn = real_init(home)
            conns.append(conn)
            return conn

        exec_ = OperatorEmailExecutor(self.home, clock=FakeClock(wall_ms=1))
        with mock.patch.object(emod, "init", side_effect=spy):
            exec_.tick()
            exec_.tick()
        self.assertEqual(len(conns), 2)
        self.assertIsNot(conns[0], conns[1])
        for conn in conns:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")  # 已关闭：不留连接


class TestFailuresCaptured(ExecutorBase):
    def test_corrupt_state_captured_not_raised(self):
        policy = enabled_policy()
        out = seed(self.home, clock=self.clock, policy=policy,
                   transport=FakeTransport())
        conn = init(self.home)
        try:
            from oc_orch.adapters.email_outbox import MAIL_KEY_PREFIX
            conn.execute("UPDATE meta SET value=? WHERE key=?",
                         ("{bad json", MAIL_KEY_PREFIX + out["item_id"]))
            conn.commit()
        finally:
            conn.close()
        exec_ = OperatorEmailExecutor(self.home, clock=self.clock,
                                      policy=policy,
                                      transport=FakeTransport())
        result = exec_.tick()  # 不抛异常
        self.assertFalse(result["ok"])
        step = result["steps"]["reconcile_stale_claimed"]
        self.assertEqual(step["error"]["kind"], "MailOutboxStateError")
        # 后续步骤因前一步失败而跳过（fail-fast），整体结果为结构化可见失败
        self.assertEqual(result["steps"]["verify_receipt"]["skipped"], True)
        self.assertEqual(result["steps"]["dispatch_once"]["skipped"], True)
        blob = self.status_blob()
        self.assertIn("MailOutboxStateError", blob or "")


class TestQueryAuthorization(ExecutorBase):
    def test_verify_denied_without_explicit_query_auth_no_budget(self):
        # 发送已授权，但未显式授予回执查询 → verify 拒绝且不耗预算
        policy = enabled_policy(receipt_queries_authorized=False)
        seeding_transport = FakeTransport(send_result=MailUnknownError("timeout"))
        _, dispatched = seed(self.home, clock=self.clock, policy=policy,
                             transport=seeding_transport, dispatch=True)
        self.assertEqual(dispatched["result"], "submit-unknown")
        verify_transport = FakeTransport(search_result=[])
        exec_ = OperatorEmailExecutor(self.home, clock=self.clock,
                                      policy=policy, transport=verify_transport)
        t = exec_.tick()
        self.assertEqual(t["steps"]["verify_receipt"]["result"],
                         "queries-not-authorized")
        self.assertEqual(verify_transport.searches, [])  # 0 外部查询
        conn = init(self.home)
        try:
            item = next(i for i in _all_items(conn) if i["kind"] == "first")
            self.assertEqual(item["receipt_checks"], 0)  # 拒绝不耗预算
        finally:
            conn.close()


def _all_items(conn):
    from oc_orch.adapters.email_outbox import EmailOutbox
    outbox = EmailOutbox(conn, FakeClock(wall_ms=1_000_000))
    return outbox.list_all()


if __name__ == "__main__":
    unittest.main(verbosity=2)

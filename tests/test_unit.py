#!/usr/bin/env python3
"""oc-orch 单元测试：纯分类、台账状态机、去重、写现场、租约。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.classify import classify_event, classify_provider_error
from oc_orch.db import init
from oc_orch.ledger import Ledger, LedgerError
from oc_orch.results import (
    collect_attempt,
    extract_assistant_text,
    scan_deliverable_baseline,
    verification_gate,
    verify_attempt,
)
from oc_orch.util import now_ms
from oc_orch.wake_policy import should_suppress_wake

_RESULT_CFG = {"result_max_messages": 50, "result_store_limit": 20000}


class _FakeClient:
    """只供 collect_attempt 读取 assistant 消息；不触碰真实服务。"""

    def __init__(self, messages=None):
        self._messages = messages or []

    def messages(self, session_id, *, limit, order):
        return self._messages

    def message_page(self, session_id, *, limit=50, order="asc", cursor=None):
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


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="oc-orch-test-")
        self.conn = init(self.tmp)
        self.ledger = Ledger(self.conn, self.tmp)

    def tearDown(self):
        self.conn.close()

    def task(self, **kw):
        base = dict(project="p", title="t", objective="o", acceptance=["a"])
        base.update(kw)
        return self.ledger.create_task(**base)

    def attempt(self, task_id, **kw):
        base = dict(directory=self.tmp, status="dispatched",
                    dispatch_state="confirmed")
        base.update(kw)
        return self.ledger.create_attempt(task_id, **base)


class TestClassify(unittest.TestCase):
    def test_terminal_and_nested_session(self):
        ev = {"id": "evt_1", "type": "session.execution.succeeded",
              "created": 123, "data": {"sessionID": "ses_a"}}
        out = classify_event(ev)
        self.assertEqual(out["action"], "terminal")
        self.assertEqual(out["terminal"], "succeeded")
        self.assertEqual(out["session_id"], "ses_a")

    def test_content_never_kept(self):
        ev = {"type": "session.text.delta", "data": {"sessionID": "ses_a",
                                                     "delta": "secret"}}
        out = classify_event(ev)
        self.assertEqual(out["action"], "ignored")
        self.assertTrue(out["content"])
        self.assertNotIn("secret", json.dumps(out))

    def test_permission_and_retry(self):
        ev = {"type": "permission.asked",
              "data": {"id": "per_1", "sessionID": "ses_a", "action": "shell",
                       "resources": ["rm -rf /"]}}
        out = classify_event(ev)
        self.assertEqual(out["action"], "permission")
        self.assertEqual(out["session_id"], "ses_a")

        ev = {"type": "session.retry.scheduled",
              "data": {"sessionID": "ses_a",
                       "error": {"status": 429, "message": "rate limit"}}}
        out = classify_event(ev)
        self.assertEqual(out["action"], "retry")
        self.assertEqual(classify_provider_error(out["error"])["category"], "rate_limit")

    def test_provider_errors(self):
        self.assertEqual(classify_provider_error(
            {"status": 403, "message": "model_permission_denied"})["category"], "auth")
        self.assertFalse(classify_provider_error(
            {"status": 403, "message": "x"})["transient"])
        self.assertEqual(classify_provider_error(
            {"status": 503, "message": "No available channel"})["category"],
            "model_unavailable")
        self.assertEqual(classify_provider_error(
            {"status": 429, "message": "too many requests"})["category"],
            "rate_limit")


class TestLedger(Base):
    def test_task_attempt_distinct_and_gate(self):
        t = self.task()
        a = self.attempt(t["id"], deliverables=["out.txt"])
        self.assertEqual(a["task_id"], t["id"])
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "dispatched")
        # 执行终态成功 ≠ 任务验收
        self.ledger.transition_attempt(a["id"], "terminal_succeeded", reason="event")
        task = self.ledger.get_task(t["id"])
        self.assertEqual(task["status"], "result_pending")
        self.assertNotEqual(task["status"], "accepted")
        gate = verification_gate(self.ledger, a["id"])
        self.assertEqual([d["target"] for d in gate["missing"]], ["out.txt"])
        # 交付物缺失时业务状态不能 accepted
        self.assertNotEqual(self.ledger.get_task(t["id"])["status"], "accepted")

    def test_result_collected_then_accept(self):
        # 合法验收走统一入口：真实 collect（FakeClient）→ verify_attempt
        t = self.task()
        a = self.attempt(t["id"], deliverables=["out.txt"], session_id="ses_t",
                         dispatch_msg_id="msg_t")
        baseline = scan_deliverable_baseline(self.ledger, a["id"], captured_at=now_ms())
        self.ledger.seal_execution(a["id"], mode="new", session_before={}, baseline=baseline)
        out_file = os.path.join(self.tmp, "out.txt")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("done")
        started = now_ms()
        self.ledger.record_execution_started(a["id"], started_at=started, event_id="start-t")
        terminal = self.ledger.record_terminal(
            a["id"], outcome="succeeded", source="sse", event_id="end-t",
            candidate={"session_id": "ses_t", "source": "sse", "outcome": "succeeded",
                       "ended_at": started + 5, "input_in_inbox": False,
                       "input_owner": "unique"})
        self.assertEqual(terminal["result"], "accepted", terminal)
        client = _FakeClient([
            {"id": "msg_t", "type": "user", "text": "execute task",
             "time": {"created": started}},
            {"id": "msg_answer_t", "type": "assistant", "finish": "stop",
             "time": {"created": started + 1, "completed": started + 4},
             "content": [{"type": "text", "text": "任务完成"}]},
            {"id": "msg_idle_t", "type": "idle", "outcome": "succeeded",
             "time": {"created": started + 5}},
        ])
        info = collect_attempt(self.ledger, client, a["id"], _RESULT_CFG)
        self.assertEqual(info["status_after"], "result_collected")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "result_received")
        out = verify_attempt(self.ledger, a["id"], result="pass", evidence=["e1"])
        self.assertTrue(out["ok"])
        self.assertTrue(out["changed"])
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "accepted")
        self.assertEqual(self.ledger.get_attempt(a["id"])["status"], "accepted")
        self.assertEqual(out["verification"]["result"], "pass")
        self.assertEqual(out["verification"]["classification"], "normal_effective")

    def test_duplicate_event(self):
        self.ledger.record_event(event_id="evt_1", type_="x", session_id="ses_a",
                                 task_id=None, attempt_id=None, seq=1, version=1,
                                 created_ms=now_ms(), payload={})
        again = self.ledger.record_event(event_id="evt_1", type_="x", session_id="ses_a",
                                         task_id=None, attempt_id=None, seq=1,
                                         version=1, created_ms=now_ms(), payload={})
        self.assertTrue(again["duplicate"])

    def test_notification_dedupe_and_ack(self):
        n1 = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c",
            dedupe_key="result-ready:A1", payload={"summary": "one"},
            task_id=None, attempt_id=None,
        )
        n2 = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c",
            dedupe_key="result-ready:A1", payload={"summary": "two", "extra": 1},
            task_id=None, attempt_id=None,
        )
        self.assertEqual(n1["id"], n2["id"])
        self.assertEqual(n2["payload"]["summary"], "two")
        self.ledger.ack_notifications([n1["id"]], actor="test")
        n3 = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c",
            dedupe_key="result-ready:A1", payload={"summary": "three"},
            task_id=None, attempt_id=None,
        )
        self.assertEqual(n3["id"], n1["id"])
        self.assertEqual(n3["state"], "acked")

    def test_site_conflict_and_release(self):
        t1 = self.task(title="one")
        a1 = self.attempt(t1["id"], write_scope=[self.tmp])
        t2 = self.task(title="two")
        with self.assertRaises(LedgerError) as ctx:
            self.attempt(t2["id"], write_scope=[self.tmp])
        self.assertEqual(ctx.exception.code, "SITE_BUSY")
        self.ledger.release_site(a1["id"], confirmed=True, external_stopped=True,
                                 actor="test")
        a2 = self.attempt(t2["id"], write_scope=[self.tmp])
        self.assertEqual(a2["task_id"], t2["id"])

    def test_old_attempt_cannot_advance_current(self):
        t = self.task()
        a1 = self.attempt(t["id"], write_scope=[self.tmp])
        self.ledger.transition_attempt(a1["id"], "terminal_succeeded")
        self.ledger.confirm_attempt_stopped(a1["id"], actor="test")
        a2 = self.attempt(t["id"], write_scope=[self.tmp])
        self.ledger.transition_attempt(a2["id"], "running")
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.transition_attempt(a1["id"], "accepted", reason="late")
        self.assertEqual(ctx.exception.code, "NOT_CURRENT")
        self.assertEqual(self.ledger.get_attempt(a1["id"])["status"],
                         "terminal_succeeded")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "running")
        self.assertEqual(self.ledger.get_task(t["id"])["current_attempt_id"], a2["id"])

    def test_coordinator_takeover(self):
        t = self.task(coordinator_session="ses_old")
        lease = self.ledger.claim_coordinator(project="p", session_id="ses_old",
                                              actor="test")
        self.assertEqual(lease["epoch"], 1)
        with self.assertRaises(LedgerError):
            self.ledger.claim_coordinator(project="p", session_id="ses_new",
                                          actor="test", old_session_active=False)
        lease = self.ledger.claim_coordinator(
            project="p", session_id="ses_new", actor="test", takeover=True,
            old_confirmed_stopped=True, old_session_active=False,
        )
        self.assertEqual(lease["epoch"], 2)
        self.assertEqual(self.ledger.get_task(t["id"])["coordinator_session"], "ses_new")

    def test_old_worker_stop_request_is_not_stop(self):
        t = self.task()
        a1 = self.attempt(t["id"], write_scope=[self.tmp])
        self.ledger.stop_attempt(a1["id"], actor="test")  # 仅受理停止请求
        with self.assertRaises(LedgerError) as ctx:
            self.attempt(t["id"], write_scope=[self.tmp], generation=2)
        self.assertEqual(ctx.exception.code, "SITE_BUSY")
        self.ledger.confirm_attempt_stopped(a1["id"], actor="test")
        a2 = self.attempt(t["id"], write_scope=[self.tmp], generation=2)
        self.assertEqual(a2["generation"], 2)

    def test_stale_event_marked(self):
        t = self.task()
        a = self.attempt(t["id"])
        # started_at 是当前时间；构造更早的事件
        rec = self.ledger.record_event(
            event_id="evt_old", type_="session.execution.succeeded",
            session_id=a["session_id"], task_id=t["id"], attempt_id=a["id"],
            seq=1, version=1, created_ms=(a["started_at"] or now_ms()) - 60000,
            payload={},
        )
        self.assertTrue(rec["stale"])
        self.assertEqual(self.ledger.get_attempt(a["id"])["status"], "dispatched")

    def test_event_duplicate_and_order(self):
        rec1 = self.ledger.record_event(
            event_id="evt_x", type_="session.execution.succeeded",
            session_id="ses_x", task_id=None, attempt_id=None,
            seq=5, version=1, created_ms=1000, payload={},
        )
        rec2 = self.ledger.record_event(
            event_id="evt_x", type_="session.execution.succeeded",
            session_id="ses_x", task_id=None, attempt_id=None,
            seq=5, version=1, created_ms=1000, payload={},
        )
        self.assertFalse(rec1["duplicate"])
        self.assertTrue(rec2["duplicate"])

    def test_acked_notification_not_recreated(self):
        n = self.ledger.enqueue_notification(
            kind="idle_unconfirmed", target_session="ses_c",
            dedupe_key="idle:A1:same", payload={"summary": "idle"},
        )
        self.ledger.ack_notifications([n["id"]], actor="test")
        again = self.ledger.enqueue_notification(
            kind="idle_unconfirmed", target_session="ses_c",
            dedupe_key="idle:A1:same", payload={"summary": "idle again"},
        )
        self.assertEqual(again["id"], n["id"])
        self.assertEqual(again["state"], "acked")
        rows = self.ledger.list_notifications(target_session="ses_c", limit=10)
        self.assertEqual(len(rows), 1)

    def test_dead_notification_not_recreated(self):
        n = self.ledger.enqueue_notification(
            kind="idle_unconfirmed", target_session="ses_c",
            dedupe_key="idle:A2:same", payload={"summary": "idle"},
        )
        self.ledger.mark_notification_dead(n["id"], error="max", owner="test")
        again = self.ledger.enqueue_notification(
            kind="idle_unconfirmed", target_session="ses_c",
            dedupe_key="idle:A2:same", payload={"summary": "idle again"},
        )
        self.assertEqual(again["id"], n["id"])
        self.assertEqual(again["state"], "dead")

    def test_sent_notification_not_reset_by_merge(self):
        n = self.ledger.enqueue_notification(
            kind="provider_blocked", target_session="ses_c",
            dedupe_key="provider:A1:auth", payload={"summary": "one"},
        )
        future = now_ms() + 900000
        self.ledger.mark_notification_sent(
            n["id"], message_id="msg_1", next_attempt_at=future, owner="test"
        )
        again = self.ledger.enqueue_notification(
            kind="provider_blocked", target_session="ses_c",
            dedupe_key="provider:A1:auth", payload={"summary": "two"},
        )
        self.assertEqual(again["state"], "sent")
        self.assertEqual(again["next_attempt_at"], future)
        self.assertEqual(again["payload"]["summary"], "two")

    def test_provider_state(self):
        self.ledger.record_provider_state(
            "p/m", category="rate_limit", status=429,
            until_ms=now_ms() + 60000, reason="429",
        )
        st = self.ledger.provider_states()["p/m"]
        self.assertEqual(st["category"], "rate_limit")
        self.ledger.clear_provider_state("p/m", reason="test")
        self.assertNotIn("p/m", self.ledger.provider_states())

    def test_pause_blocks_acceptance(self):
        t = self.task()
        a = self.attempt(t["id"])
        self.ledger.transition_task(t["id"], "paused", reason="manual")
        self.ledger.transition_attempt(a["id"], "terminal_succeeded")
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "paused")


class TestWakePolicy(unittest.TestCase):
    def test_inbox_pending_suppresses(self):
        ok, why = should_suppress_wake(
            outstanding=[], inbox=[{"id": "msg_1"}], is_active=False,
            now_ms=1000, ack_timeout_ms=10000,
        )
        self.assertTrue(ok)
        self.assertEqual(why, "inbox-pending")

    def test_recent_unacked_suppresses(self):
        ok, why = should_suppress_wake(
            outstanding=[{"sent_at": 9000}], inbox=[], is_active=False,
            now_ms=10000, ack_timeout_ms=5000,
        )
        self.assertTrue(ok)
        self.assertEqual(why, "unacked-recent")

    def test_active_with_unacked_suppresses(self):
        ok, why = should_suppress_wake(
            outstanding=[{"sent_at": 1000}], inbox=[], is_active=True,
            now_ms=10000, ack_timeout_ms=5000,
        )
        self.assertTrue(ok)
        self.assertEqual(why, "target-active-with-unacked")

    def test_idle_old_unacked_allows_nudge(self):
        ok, why = should_suppress_wake(
            outstanding=[{"sent_at": 1000}], inbox=[], is_active=False,
            now_ms=10000, ack_timeout_ms=5000,
        )
        self.assertFalse(ok)
        self.assertEqual(why, "")


class TestExtract(unittest.TestCase):
    def test_assistant_text(self):
        msgs = [
            {"type": "user", "time": {"created": 100}},
            {"type": "assistant", "time": {"created": 200}, "content": [
                {"type": "text", "text": "hello"},
                {"type": "tool", "name": "shell", "executed": True,
                 "state": {"status": "completed"}},
            ]},
            {"type": "assistant", "time": {"created": 50}, "content": [
                {"type": "text", "text": "old"},
            ]},
        ]
        text, tools, count = extract_assistant_text(msgs, 100)
        self.assertEqual(text, "hello")
        self.assertEqual(count, 1)
        self.assertEqual(tools[0]["name"], "shell")


if __name__ == "__main__":
    unittest.main(verbosity=2)

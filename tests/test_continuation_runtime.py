#!/usr/bin/env python3
"""S4 执行后接续与责任连续性运行时回归（C 组）。

覆盖：
- 新 kind（continuation_ready / responsibility_missing）明确登记且任务级放行；
- 本地 park 到期/责任 deadline 先于 service 健康门，服务不可用也不跳过；
- 原 input 定点 + 有界分页 history 建立真实执行区间：旧 idle/busy 不算证据，
  同区间其他输入 → ambiguous，不吞并别的输入执行；
- 执行结束且无结构化业务回报 → awaiting_review + 原子发布 continuation_ready；
- attempt 已 terminal 仍从责任/输入注册表观察（不重开旧 attempt）；
- 旧通知 ACK 不吞新接续事实；
- 已托管未完成 Task 的有限缺失责任检查（两次 reconcile 后稳定诊断）；
- 暂停/cancel 遵守控制，deadline/预算跨连接保存。
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from oc_orch.adapters.responsibility_ledger import ResponsibilityStore  # noqa: E402
from oc_orch.config import DEFAULTS  # noqa: E402
from oc_orch.domain.errors import LedgerError  # noqa: E402
from oc_orch.domain.notification_policy import (  # noqa: E402
    ALL_KINDS,
    DEFER,
    SEND,
    SUPPRESS,
    kind_category,
    notification_decision,
)
from oc_orch.supervisor import (  # noqa: E402
    _CONTINUATION_KIND,
    _RESPONSIBILITY_MISSING_KIND,
    _RESP_WAITING_HUMAN,
)
from oc_orch.supervisor import Supervisor  # noqa: E402
from tests.fakes import FakeClient, FakeClock  # noqa: E402


def _note(kind, *, task_id=None, attempt_id=None, payload=None):
    return {"id": "N-x", "kind": kind, "task_id": task_id,
            "attempt_id": attempt_id, "payload": dict(payload or {}),
            "state": "pending", "dedupe_key": f"{kind}:x", "project": "p",
            "target_session": "ses_coord", "created_at": 1000,
            "next_attempt_at": 1000, "priority": 5, "max_attempts": 3,
            "metadata": {}, "attempts": 0, "sent_at": None,
            "acked_at": None, "message_id": None}


def _task(status="result_received", *, current="A-1"):
    return {"id": "T-1", "status": status, "current_attempt_id": current,
            "closed_at": None, "accepted_at": None, "metadata": {},
            "project": "p"}


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="continuation-",
                                               dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_coord")
        self.client.set_session("ses_worker")
        self.sup = Supervisor(self.home, cfg=dict(DEFAULTS),
                              client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.store = ResponsibilityStore(self.ledger)
        self._managed = None

    def managed(self):
        if self._managed is None:
            from oc_orch.composition import make_managed_inputs
            self._managed = make_managed_inputs(
                self.sup.conn, self.home, client=lambda: self.client,
                clock=self.clock, responsibilities=self.store)
        return self._managed

    def send_managed(self, task, attempt, *, message_id="msg_in",
                     work_kind="observe", text="continue"):
        out = self.managed().send_tracked(
            "ses_worker", text=text, message_id=message_id,
            owner="coordinator", task_id=task["id"], attempt_id=attempt["id"],
            target_session="ses_coord",
            deadline=self.clock.now_ms() + 3600 * 1000,
            work_kind=work_kind, materials={"ref": "baseline-current"})
        resp = out.get("responsibility")
        if isinstance(resp, dict) and isinstance(resp.get("responsibility"), dict):
            resp = resp["responsibility"]
        rid = (resp or {}).get("id") or (resp or {}).get("responsibility_id")
        return self.store.get_responsibility(rid)

    def managed_receipt(self, input_id):
        return self.managed().get(input_id)["input"]

    def deliver_inbox(self, *, at=None):
        at = self.clock.now_ms() if at is None else at
        self.client.deliver_inbox("ses_worker", at=at)
        return at

    def append_user(self, message_id, *, at):
        self.client.append_message("ses_worker", {
            "id": message_id, "type": "user", "text": "other",
            "time": {"created": at}})
        return at

    def managed_work(self, *, coordinator="ses_coord", status="running"):
        task = self.ledger.create_task(
            project="p", title="continuation", coordinator_session=coordinator)
        attempt = self.ledger.create_attempt(
            task["id"], directory=self.home, session_id="ses_worker",
            status=status, dispatch_state="confirmed")
        return task, attempt

    def open_resp(self, task, attempt, *, input_id="I-1",
                  message_id="msg_in", state="waiting_input",
                  wait_kind="input_execution",
                  wait_condition="input:msg_in:execution",
                  condition_key="execution-ended:I-1",
                  next_check_at=None, deadline=None, max_budget=3):
        now = self.clock.now_ms()
        opened = self.store.open_responsibility(
            owner="coordinator", project=task["project"], task_id=task["id"],
            attempt_id=attempt["id"], generation=attempt["generation"],
            input_id=input_id, session_id=attempt.get("session_id") or "ses_worker",
            message_id=message_id, target_session="ses_coord", state=state,
            wait_kind=wait_kind, wait_condition=wait_condition,
            next_action="等待执行结束并核收；ACK 不关闭本责任",
            next_check_at=now if next_check_at is None else next_check_at,
            deadline=(now + 60000) if deadline is None else deadline,
            condition_key=condition_key, evidence={}, max_budget=max_budget,
            actor="test")["responsibility"]
        return opened

    def deliver_user(self, message_id="msg_in", *, at=None):
        at = self.clock.now_ms() if at is None else at
        self.client.append_message("ses_worker", {
            "id": message_id, "type": "user", "text": "do",
            "time": {"created": at}})
        return at


class NotificationPolicyKindTests(_Base):
    def test_new_kinds_are_registered_not_unknown(self):
        for kind in (_CONTINUATION_KIND, _RESPONSIBILITY_MISSING_KIND):
            with self.subTest(kind=kind):
                self.assertIn(kind, ALL_KINDS)
                self.assertEqual(kind_category(kind), "current_only")

    def test_task_level_continuation_sends_only_while_task_live(self):
        live = notification_decision(
            _note(_CONTINUATION_KIND, task_id="T-1"), task=_task("running"))
        self.assertEqual(live["action"], SEND)
        self.assertEqual(live["reason"], "responsibility-live")
        paused = notification_decision(
            _note(_CONTINUATION_KIND, task_id="T-1"), task=_task("paused"))
        self.assertEqual(paused["action"], DEFER)
        for status in ("accepted", "cancelled"):
            with self.subTest(status=status):
                done = notification_decision(
                    _note(_RESPONSIBILITY_MISSING_KIND, task_id="T-1"),
                    task=_task(status))
                self.assertEqual(done["action"], SUPPRESS)
        unknown = notification_decision(
            _note(_RESPONSIBILITY_MISSING_KIND, task_id="T-1"),
            task=_task("not-a-status"))
        self.assertEqual(unknown["action"], DEFER)

    def test_superseded_attempt_continuation_is_suppressed(self):
        task = _task("running", current="A-2")
        attempt = {"id": "A-1", "task_id": "T-1", "generation": 1,
                   "status": "terminal_succeeded", "site_released_at": 5,
                   "metadata": {}}
        decision = notification_decision(
            _note(_CONTINUATION_KIND, task_id="T-1", attempt_id="A-1"),
            task=task, attempt=attempt)
        self.assertEqual(decision["action"], SUPPRESS)
        self.assertEqual(decision["reason"], "superseded")


class WaitingDependencyTests(_Base):
    def test_send_result_unknown_reconciles_original_id_only(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        # B 的发送结果未知责任：只按原 ID 补证，不换身份重投。
        updated = self.store.update_responsibility(
            resp["id"], expected_revision=resp["revision"],
            fields={"state": "waiting_dependency",
                    "wait_kind": "send_result_unknown",
                    "wait_condition": "send-result-unknown",
                    "next_check_at": self.clock.now_ms()}, actor="test")
        calls_before = len(self.client.prompt_calls)
        out = self.sup.tick_continuation(allow_remote=True)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["budget_used"], 1,
                         "未知发送只烧原 ID 补证预算")
        self.assertEqual(row["wait_condition"], "send-result-unknown")
        self.assertEqual(len(self.client.prompt_calls), calls_before,
                         "补证不得追加 prompt/换 ID 重投")
        self.assertNotEqual(row["state"], _RESP_WAITING_HUMAN)
        # 投递被确认后转入执行等待（不自动关闭责任、不自动 ACK）。
        self.deliver_inbox()
        self.clock.advance(seconds=31)
        self.sup.tick_continuation(allow_remote=True)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["wait_kind"], "input_execution")
        self.assertNotEqual(row["state"], _RESP_WAITING_HUMAN)

    def test_trusted_cancel_event_matches_original_message_only(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        receipt_before = self.managed_receipt(resp["input_id"])
        self.assertEqual(receipt_before["delivery"], "pending")
        # 会话不匹配：不得取消本输入。
        self.sup.handle_stream_item({"kind": "event", "event": {
            "id": "evt-cancel-wrong", "type": "session.inbox.cancelled",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_other", "inboxID": resp["message_id"]}}})
        self.assertEqual(self.managed_receipt(resp["input_id"])["delivery"],
                         "pending")
        # 原 session + 原 message 精确匹配：可信取消，业务 ACK 保持独立。
        self.sup.handle_stream_item({"kind": "event", "event": {
            "id": "evt-cancel-ok", "type": "session.inbox.cancelled",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_worker", "inboxID": resp["message_id"]}}})
        receipt = self.managed_receipt(resp["input_id"])
        self.assertEqual(receipt["delivery"], "cancelled")
        self.assertEqual(receipt["business_ack"], "unacknowledged")
        self.assertEqual(len(self.client.prompt_calls), 1)


class BoundedScanFairnessTests(_Base):
    def test_missing_check_still_reaches_task_beyond_first_200(self):
        task = self.ledger.create_task(
            project="p", title="tail-target", coordinator_session="ses_coord")
        self.ledger.create_attempt(
            task["id"], directory=self.home, session_id="ses_worker",
            status="terminal_succeeded", dispatch_state="confirmed")
        self.ledger.conn.execute(
            "UPDATE tasks SET status='result_received' WHERE id=?", (task["id"],))
        # 205 个后更新 draft 任务把目标压到 list_tasks 末尾（旧 limit=200 会漏）。
        for i in range(205):
            self.clock.advance(wall=1)
            self.ledger.create_task(project="p", title=f"noise-{i}")
        self.sup.check_missing_responsibilities(now=self.clock.now_ms())
        self.sup.check_missing_responsibilities(now=self.clock.now_ms())
        rows = self.store.list_open_responsibilities(task_id=task["id"])
        self.assertEqual(len(rows), 1,
                         "尾部 Task 也必须进入有界缺失责任检查，不能只看头 200")
        self.assertEqual(rows[0]["condition_key"], "missing-responsibility")

    def test_terminal_scan_reaches_responsibility_beyond_first_200(self):
        target = self.ledger.create_task(
            project="p", title="tail-responsibility",
            coordinator_session="ses_coord")
        target_attempt = self.ledger.create_attempt(
            target["id"], directory=self.home, session_id="ses_worker",
            status="running", dispatch_state="confirmed")
        self.clock.advance(wall=1)
        target_resp = self.open_resp(
            target, target_attempt, condition_key="tail-close",
            next_check_at=self.clock.now_ms() + 3600 * 1000)
        # 205 个更晚更新的任务/责任，把目标责任压到遍历尾部。
        for i in range(205):
            self.clock.advance(wall=1)
            noise = self.ledger.create_task(
                project="p", title=f"noise-{i}", coordinator_session="ses_coord")
            noise_attempt = self.ledger.create_attempt(
                noise["id"], directory=self.home,
                session_id=f"ses_noise_{i}", status="running",
                dispatch_state="confirmed",
                write_scope=[f"/tmp/opencode/noise-site-{i}"])
            self.open_resp(noise, noise_attempt,
                           condition_key=f"noise-close-{i}",
                           next_check_at=self.clock.now_ms() + 3600 * 1000)
        self.ledger.conn.execute(
            "UPDATE tasks SET status='accepted', accepted_at=? WHERE id=?",
            (self.clock.now_ms(), target["id"]))
        deadline_ticks = (len(self.store.list_open_responsibilities(limit=500))
                          // 200) + 2
        for _ in range(deadline_ticks):
            self.sup.tick_continuation(allow_remote=False)
        row = self.store.get_responsibility(target_resp["id"])
        self.assertIsNotNone(row["closed_at"],
                             "尾部责任也必须被终态扫描关闭（keyset 游标回绕）")
        self.assertEqual(row["close_reason"], "task-accepted")



    def _task_attempt(self, *, generation=2, version=None, status="running",
                      current="A-cur"):
        meta = {}
        if version is not None:
            meta["result"] = {"version": version}
        attempt = {"id": current, "task_id": "T-1", "generation": generation,
                   "status": status, "site_released_at": None, "metadata": meta}
        task = _task("running", current=current)
        return task, attempt

    def test_continuation_result_version_change_is_suppressed_before_send(self):
        task, attempt = self._task_attempt(version="v1")
        decision = notification_decision(
            _note(_CONTINUATION_KIND, task_id="T-1", attempt_id="A-cur",
                  payload={"result_version": "v2", "generation": 2}),
            task=task, attempt=attempt)
        self.assertEqual(decision["action"], SUPPRESS)
        self.assertEqual(decision["reason"], "result-version-changed")

    def test_continuation_matching_version_is_still_send(self):
        task, attempt = self._task_attempt(version="v2")
        decision = notification_decision(
            _note(_CONTINUATION_KIND, task_id="T-1", attempt_id="A-cur",
                  payload={"result_version": "v2", "next_action": "x"}),
            task=task, attempt=attempt)
        self.assertEqual(decision["action"], SEND)

    def test_continuation_current_version_unknown_is_defer_not_send(self):
        task, attempt = self._task_attempt(version=None)
        decision = notification_decision(
            _note(_CONTINUATION_KIND, task_id="T-1", attempt_id="A-cur",
                  payload={"result_version": "v2"}),
            task=task, attempt=attempt)
        self.assertEqual(decision["action"], DEFER)
        self.assertEqual(decision["reason"], "current-result-version-unresolved")

    def test_continuation_generation_change_is_suppressed(self):
        task, attempt = self._task_attempt(generation=2, version="v1")
        decision = notification_decision(
            _note(_CONTINUATION_KIND, task_id="T-1", attempt_id="A-cur",
                  payload={"result_version": "v1", "generation": 3}),
            task=task, attempt=attempt)
        self.assertEqual(decision["action"], SUPPRESS)
        self.assertEqual(decision["reason"], "generation-changed")

    def test_s1_result_ready_is_unaffected_by_version_guard(self):
        # 版本对照只针对两个任务级责任 kind；S1 kind 保持原 current 证明。
        task, attempt = self._task_attempt(version="v1")
        decision = notification_decision(
            _note("result_ready", task_id="T-1", attempt_id="A-cur",
                  payload={"result_version": "v2"}),
            task=task, attempt=attempt)
        self.assertEqual(decision["action"], SEND)

class ExecutionIntervalTests(_Base):
    def _end_to_end(self, *, work_kind="observe"):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt, work_kind=work_kind)
        at = self.deliver_inbox()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 5000)
        self.clock.advance(seconds=30)
        return task, attempt, resp

    def test_execution_end_becomes_awaiting_review_and_publishes_once(self):
        task, attempt, resp = self._end_to_end()
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 1)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["state"], "awaiting_review")
        self.assertEqual(row["wait_condition"], "business-report-unknown")
        notes = self.ledger.list_notifications(target_session="ses_coord")
        self.assertEqual([n["kind"] for n in notes], [_CONTINUATION_KIND])
        self.assertEqual(notes[0]["payload"]["input_id"], resp["input_id"])
        self.assertIn("auto_accept", notes[0]["payload"])
        self.assertFalse(notes[0]["payload"]["auto_accept"])
        # 同一事实重复观察/重复 RPC：幂等，不新建通知、不重复派单。
        self.store.update_responsibility(
            resp["id"], expected_revision=row["revision"],
            fields={"next_check_at": self.clock.now_ms()}, actor="test")
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 0)
        self.assertEqual(len(self.ledger.list_notifications(
            target_session="ses_coord")), 1)

    def test_execution_fact_is_written_to_input_receipt_independently(self):
        task, attempt, resp = self._end_to_end()
        self.sup.tick_continuation(allow_remote=True)
        receipt = self.managed_receipt(resp["input_id"])
        self.assertEqual(receipt["delivery"], "delivered")
        self.assertEqual(receipt["execution"], "succeeded")
        self.assertEqual(receipt["business_ack"], "unacknowledged",
                         "执行事实不得冒充业务 ACK")

    def test_old_idle_before_input_is_not_execution_evidence(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        now = self.clock.now_ms()
        self.client.add_idle("ses_worker", outcome="succeeded", at=now - 1000)
        self.deliver_inbox(at=now)
        self.clock.advance(seconds=30)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 0)
        row = self.store.get_responsibility(resp["id"])
        self.assertNotEqual(row["state"], "awaiting_review")
        self.assertEqual(row["evidence"]["execution"]["state"], "running")
        self.assertEqual(self.ledger.list_notifications(
            target_session="ses_coord"), [])

    def test_other_input_in_same_interval_is_ambiguous_not_swallowed(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        at = self.deliver_inbox()
        self.append_user("msg_other", at=at + 2000)
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 4000)
        self.clock.advance(seconds=30)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 0)
        self.assertEqual(out["ambiguous"], 1)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["state"], "waiting_execution")
        self.assertEqual(row["wait_condition"], "execution-interval-ambiguous")
        self.assertEqual(
            row["evidence"]["execution"]["reason"], "other-input-in-interval")
        self.assertEqual(self.ledger.list_notifications(
            target_session="ses_coord"), [])
        # 别的输入不被吞并：本输入 receipt 不能写成 succeeded。
        receipt = self.managed_receipt(resp["input_id"])
        self.assertEqual(receipt["execution"], "unknown")

    def test_incomplete_coverage_is_unknown_not_executed(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        at = self.deliver_inbox()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 5000)
        self.client.message_scan_cap = 1  # 只能看到最新 idle，锚点不可见
        self.clock.advance(seconds=30)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 0)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["evidence"]["execution"]["state"], "unknown")
        self.assertIn(row["evidence"]["execution"]["reason"],
                      ("anchor-unreadable", "coverage-incomplete",
                       "anchor-identity-mismatch", "query-failed"))

    def test_observation_continues_after_attempt_terminal(self):
        task, attempt = self.managed_work(status="terminal_succeeded")
        self.ledger.conn.execute(
            "UPDATE tasks SET status='result_received' WHERE id=?", (task["id"],))
        resp = self.send_managed(task, attempt, work_kind="read_only")
        at = self.deliver_inbox()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 5000)
        self.clock.advance(seconds=30)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 1)
        self.assertEqual(self.store.get_responsibility(resp["id"])["state"],
                         "awaiting_review")
        self.assertEqual(self.ledger.get_attempt(attempt["id"])["status"],
                         "terminal_succeeded")

    def test_sse_execution_end_only_promotes_rest_recheck(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        now = self.clock.now_ms()
        updated = self.store.update_responsibility(
            resp["id"], expected_revision=resp["revision"],
            fields={"next_check_at": now + 60_000}, actor="test")
        item = {"kind": "event", "event": {
            "id": "evt-1", "type": "session.execution.succeeded",
            "created": now + 1000,
            "data": {"sessionID": "ses_worker"}}}
        urgent = self.sup.handle_stream_item(item)
        self.assertTrue(urgent)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["next_check_at"], now,
                         "SSE 应触发本轮 REST 补证，而不是把事件当执行证据")
        self.assertEqual(row["state"], "waiting_execution")

    def test_restart_reuses_persisted_responsibility_and_observes(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        at = self.deliver_inbox()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 5000)
        self.clock.advance(seconds=30)
        restarted = Supervisor(self.home, cfg=dict(DEFAULTS),
                               client=self.client, clock=self.clock)
        self.addCleanup(restarted.conn.close)
        restarted.tick_continuation(allow_remote=True)
        final = ResponsibilityStore(restarted.ledger).get_responsibility(
            resp["id"])
        self.assertEqual(final["state"], "awaiting_review")
        self.assertEqual(final["budget_used"], 0,
                         "正常执行等待不消耗失败/未知补证预算")
        self.assertEqual(len(restarted.ledger.list_notifications(
            target_session="ses_coord")), 1)

    def test_superseded_generation_cannot_advance_old_responsibility(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        at = self.deliver_inbox()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 5000)
        # 新代次认领：不同写范围避免 site 冲突；旧责任 generation 已过期。
        self.ledger.create_attempt(
            task["id"], directory=self.home, session_id="ses_worker2",
            status="running", dispatch_state="confirmed",
            write_scope=["/tmp/opencode/other-site"])
        self.clock.advance(seconds=30)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 0)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["state"], "waiting_human",
                         "stale 不能只 audit：转人工并保原身份")
        self.assertEqual(row["wait_condition"], "stale-attribution")
        alerts = self.ledger.list_alerts(state="new", limit=50)
        self.assertTrue(any(a["kind"] == "responsibility_blocked"
                            for a in alerts))
        self.assertEqual(self.ledger.list_notifications(
            target_session="ses_coord"), [])

    def test_awaiting_review_backoff_avoids_per_cycle_republish(self):
        task, attempt, resp = self._end_to_end()
        self.sup.tick_continuation(allow_remote=True)
        row = self.store.get_responsibility(resp["id"])
        self.assertGreater(row["next_check_at"], self.clock.now_ms(),
                           "已发布事实应退避下一检查")
        before = len(self.ledger.list_notifications(target_session="ses_coord"))
        self.clock.advance(seconds=10)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 0)
        self.assertEqual(
            len(self.ledger.list_notifications(target_session="ses_coord")),
            before, "退避期间不得每轮重复 publish")
        deadline = row["deadline"]
        self.ledger.ack_notifications(
            [n["id"] for n in self.ledger.list_notifications(
                target_session="ses_coord")], actor="test")
        self.clock.advance(seconds=400)
        self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(self.store.get_responsibility(resp["id"])["deadline"],
                         deadline)

    def test_running_first_observation_honors_next_check_not_budget0(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        at = self.deliver_inbox()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 2 * 3600 * 1000)
        self.clock.advance(seconds=5)
        # 首次观察：无 evidence.execution 才允许（不依赖 budget0）
        self.sup.tick_continuation(allow_remote=True)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["evidence"]["execution"]["state"], "running")
        self.assertGreater(row["next_check_at"], self.clock.now_ms())
        # 首次观察后未到期：不得再读 REST（正常 running 不每轮绕 next_check）
        from unittest.mock import patch
        with patch.object(self.client, "message_page",
                          wraps=self.client.message_page) as mp:
            self.sup.tick_continuation(allow_remote=True)
            self.assertEqual(mp.call_count, 0,
                             "首次观察未到期不得每轮重读原 ID history")
        # 到期后才读一次
        self.clock.advance(seconds=31)
        with patch.object(self.client, "message_page",
                          wraps=self.client.message_page) as mp:
            self.sup.tick_continuation(allow_remote=True)
            self.assertEqual(mp.call_count, 1, "到期后才做一次有界 REST 补证")
        # SSE 是唯一例外：started 事件把 next_check 提前到 now
        fresh = self.store.get_responsibility(resp["id"])
        self.store.update_responsibility(
            resp["id"], expected_revision=fresh["revision"],
            fields={"next_check_at": self.clock.now_ms() + 3600 * 1000},
            actor="test")
        self.sup.handle_stream_item({"kind": "event", "event": {
            "id": "evt-promote", "type": "session.execution.started",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_worker"}}})
        with patch.object(self.client, "message_page",
                          wraps=self.client.message_page) as mp:
            self.sup.tick_continuation(allow_remote=True)
            self.assertEqual(mp.call_count, 1, "SSE promote 例外允许立即补证")

    def test_old_notification_ack_does_not_swallow_new_continuation(self):
        task, attempt = self.managed_work()
        old = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord",
            dedupe_key="old-result", payload={"summary": "旧结果"},
            project="p", task_id=task["id"], attempt_id=attempt["id"])
        self.ledger.ack_notifications([old["id"]], actor="test")
        resp = self.send_managed(task, attempt)
        at = self.deliver_inbox()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 5000)
        self.clock.advance(seconds=30)
        self.sup.tick_continuation(allow_remote=True)
        pending = self.ledger.list_notifications(
            target_session="ses_coord", state="pending")
        self.assertEqual(len(pending), 1)
        self.assertNotEqual(pending[0]["id"], old["id"])
        calls_before = len(self.client.prompt_calls)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), calls_before + 1)
        self.assertIn("执行结束", self.client.prompt_calls[-1]["text"])
        self.assertEqual(self.ledger.get_notification(old["id"])["state"], "acked")


class LocalFirstAndDeadlineTests(_Base):
    def test_responsibility_deadline_runs_when_service_is_down(self):
        task, attempt = self.managed_work()
        now = self.clock.now_ms()
        resp = self.open_resp(task, attempt, deadline=now - 1)
        self.client.fail("info", RuntimeError("offline"))
        self.clock.advance(seconds=5)
        out = self.sup.cycle(dispatch=False)
        self.assertEqual(out["continuation"]["deadline"], 1)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["state"], "waiting_human")
        self.assertEqual(row["wait_condition"], "deadline-exhausted")
        alerts = self.ledger.list_alerts(state="new", limit=50)
        self.assertTrue(any(a["kind"] == "responsibility_blocked"
                            for a in alerts))
        self.assertEqual(self.client.prompt_calls, [])

    def test_service_down_does_not_consume_execution_budget(self):
        task, attempt = self.managed_work()
        resp = self.open_resp(task, attempt)
        at = self.deliver_user()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 5000)
        self.client.fail("info", RuntimeError("offline"))
        self.clock.advance(seconds=30)
        self.sup.cycle(dispatch=False)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["state"], "waiting_input")
        self.assertEqual(row["budget_used"], 0)
        self.assertEqual(row["next_check_at"], resp["next_check_at"])

    def test_budget_exhaustion_transfers_to_human(self):
        task, attempt = self.managed_work()
        now = self.clock.now_ms()
        resp = self.open_resp(task, attempt, max_budget=0)
        self.clock.advance(seconds=1)
        self.sup.tick_continuation(allow_remote=True)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["state"], "waiting_human")
        self.assertEqual(row["wait_condition"], "budget-exhausted")
        # 预算/期限跨连接保存，不被重启重置。
        reopened = ResponsibilityStore(self.ledger)
        final = reopened.get_responsibility(resp["id"])
        self.assertEqual(final["max_budget"], 0)
        self.assertEqual(final["deadline"], resp["deadline"])

    def test_closed_at_nonterminal_is_visible_not_treated_as_accepted(self):
        task, attempt = self.managed_work()
        resp = self.open_resp(task, attempt)
        self.ledger.conn.execute(
            "UPDATE tasks SET closed_at=?, status='running' WHERE id=?",
            (self.clock.now_ms(), task["id"]))
        self.clock.advance(seconds=1)
        self.sup.tick_continuation(allow_remote=False)
        row = self.store.get_responsibility(resp["id"])
        self.assertIsNone(row["closed_at"],
                          "closed_at 非验收/取消不得关闭推进责任")
        alerts = self.ledger.list_alerts(state="new", limit=50)
        self.assertTrue(any(a["kind"] == "task_closed_nonterminal"
                            for a in alerts))

    def test_registered_park_input_gets_bounded_original_id_reconcile(self):
        self.managed().register_watch(
            "ses_worker", owner="coordinator", project="p",
            max_pending_age_ms=60000)
        now = self.clock.now_ms()
        out = self.managed().send(
            "ses_worker", intent="park", text="hold",
            owner="coordinator", request_id="park-reconcile", project="p",
            expires_at=now + 3600 * 1000,
            flush_condition="manual-confirm")
        input_id = out["input"]["input_id"]
        self.assertEqual(self.managed_receipt(input_id)["delivery"], "pending")
        self.deliver_inbox()
        self.clock.advance(seconds=61)
        self.sup.tick_continuation(allow_remote=True)
        receipt = self.managed_receipt(input_id)
        self.assertEqual(receipt["delivery"], "delivered",
                         "outstanding=0 已登记输入需按原 ID 有限补证 delivery")
        self.assertEqual(receipt["execution"], "unknown",
                         "park/纯观察不能伪造执行")
        self.assertFalse(self.store.list_open_responsibilities(
            task_id="none"), "补证不得创建假责任/Task")
        self.assertEqual(self.ledger.list_tasks(limit=10), [],
                         "只读补证不得创建假 Task")

    def test_paused_task_never_publishes_or_clears_control(self):
        task, attempt = self.managed_work()
        resp = self.open_resp(task, attempt)
        at = self.deliver_user()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 5000)
        self.ledger.conn.execute(
            "UPDATE tasks SET status='paused' WHERE id=?", (task["id"],))
        self.clock.advance(seconds=30)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 0)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["state"], "waiting_human")
        self.assertEqual(row["wait_condition"], "task-paused")
        self.assertEqual(self.ledger.list_notifications(
            target_session="ses_coord"), [])

    def test_accepted_task_closes_responsibility_without_waiting_next_check(self):
        task, attempt = self.managed_work()
        now = self.clock.now_ms()
        resp = self.open_resp(task, attempt, next_check_at=now + 3600 * 1000)
        self.ledger.conn.execute(
            "UPDATE tasks SET status='accepted', accepted_at=? WHERE id=?",
            (now, task["id"]))
        self.sup.tick_continuation(allow_remote=False)
        row = self.store.get_responsibility(resp["id"])
        self.assertIsNotNone(row["closed_at"])
        self.assertEqual(row["close_reason"], "task-accepted")
        # 旧 attempt 事实不被关闭责任改写。
        self.assertEqual(self.ledger.get_attempt(attempt["id"])["status"],
                         "running")

    def test_waiting_human_is_not_revived_by_repeated_cycles(self):
        task, attempt = self.managed_work()
        now = self.clock.now_ms()
        resp = self.open_resp(
            task, attempt, state="waiting_human", wait_kind="human",
            wait_condition="operator-decision",
            condition_key="manual-decision", next_check_at=now,
            deadline=now + 3600 * 1000)
        for _ in range(3):
            self.clock.advance(seconds=31)
            self.sup.tick_continuation(allow_remote=True)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["state"], "waiting_human")
        self.assertEqual(row["wait_condition"], "operator-decision")
        self.assertEqual(row["budget_used"], 0)
        self.assertEqual(self.ledger.list_notifications(
            target_session="ses_coord"), [])

    def test_ledger_error_in_due_tick_is_visible_and_cycle_survives(self):
        task, attempt = self.managed_work()
        now = self.clock.now_ms()
        self.open_resp(task, attempt, state="waiting_dependency",
                       wait_kind="dependency", wait_condition="dep",
                       condition_key="dep-hold", next_check_at=now)
        store = self.sup._responsibility_store()

        def boom(*args, **kwargs):
            raise LedgerError("SIMULATED_GUARD", "boom-detail")

        # 旧字符串依赖现转明确人工，不再只顺延next_check；在真正的新写边界
        # 注入同一拒绝，保留“必须有可见诊断且主循环存活”的原断言。
        store.escalate_dependency_human = boom
        out = self.sup.cycle(dispatch=False)  # 不得二次崩溃
        self.assertNotIn("error", out)
        rejected = [a for a in self.ledger.list_audit(50)
                    if a.get("action") == "responsibility.tick-rejected"]
        self.assertTrue(rejected, "LedgerError 拒绝必须留下可见审计诊断")
        self.assertIn("SIMULATED_GUARD", rejected[0]["detail"])
        self.assertIn("boom-detail", rejected[0]["detail"])

    def test_cancelled_task_closes_business_responsibility(self):
        task, attempt = self.managed_work()
        resp = self.open_resp(task, attempt)
        self.ledger.conn.execute(
            "UPDATE tasks SET status='cancelled' WHERE id=?", (task["id"],))
        self.clock.advance(seconds=1)
        self.sup.tick_continuation(allow_remote=False)
        row = self.store.get_responsibility(resp["id"])
        self.assertIsNotNone(row["closed_at"])
        self.assertEqual(row["close_reason"], "task-cancelled")


class ContinuationReportTests(_Base):
    def _report(self, resp, **overrides):
        report = {
            "schema": "oc-orch.continuation.v1",
            "kind": "needs_material",
            "next_action": "提供 build baseline 后继续；不自动重派",
            "input_id": resp["input_id"],
            "session_id": resp["session_id"],
            "message_id": resp["message_id"],
            "task_id": resp["task_id"],
            "attempt_id": resp["attempt_id"],
            "generation": resp["generation"],
            "result_version": resp["result_version"],
            "materials": [{"ref": "current-baseline"}],
            "missing_materials": ["build-baseline"],
            "decision": {"needed": "baseline"},
            "evidence_refs": ["log:1"],
        }
        report.update(overrides)
        return report

    def _finish_with_text(self, resp, text):
        at = self.deliver_inbox()
        self.client.append_message("ses_worker", {
            "id": "msg_assistant_report", "type": "assistant",
            "time": {"created": at + 1000},
            "content": [{"type": "text", "text": text}]})
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 3000)
        self.clock.advance(seconds=30)
        return self.sup.tick_continuation(allow_remote=True)

    def test_strict_fenced_report_binds_and_classifies(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        report = self._report(resp)
        text = ("worker 汇报：\n```json\n"
                + json.dumps(report, ensure_ascii=False) + "\n```\n请协调者处理。")
        out = self._finish_with_text(resp, text)
        self.assertEqual(out["published"], 1)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["state"], "awaiting_review")
        self.assertEqual(row["wait_condition"], "business-report:needs_material")
        continuation = row["evidence"]["continuation"]
        self.assertEqual(continuation["kind"], "needs_material")
        self.assertEqual(continuation["source_message_id"],
                         "msg_assistant_report")
        self.assertEqual(continuation["missing_materials"], ["build-baseline"])
        notes = self.ledger.list_notifications(target_session="ses_coord")
        self.assertEqual(notes[0]["payload"]["classification"], "NEEDS_MATERIAL")
        self.assertFalse(notes[0]["payload"]["auto_accept"])
        receipt = self.managed_receipt(resp["input_id"])
        self.assertEqual(receipt["business_ack"], "unacknowledged",
                         "结构化报告不得自动业务 ACK")

    def test_metadata_continuation_report_accepted(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        report = self._report(resp, kind="needs_decision",
                              missing_materials=[])
        at = self.deliver_inbox()
        self.client.append_message("ses_worker", {
            "id": "msg_assistant_meta", "type": "assistant",
            "time": {"created": at + 1000},
            "metadata": {"continuation": report},
            "content": [{"type": "text", "text": "需要协调者决定"}]})
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 3000)
        self.clock.advance(seconds=30)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["published"], 1)
        note = self.ledger.list_notifications(target_session="ses_coord")[0]
        self.assertEqual(note["payload"]["classification"], "NEEDS_DECISION")

    def test_unknown_schema_or_keyword_text_stays_awaiting_review(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        report = self._report(resp)
        report["schema"] = "oc-orch.unknown.v9"
        text = "```json\n" + json.dumps(report) + "\n```\nNEEDS_MATERIAL 完成"
        out = self._finish_with_text(resp, text)
        self.assertEqual(out["published"], 1)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["wait_condition"], "business-report-unknown")
        note = self.ledger.list_notifications(target_session="ses_coord")[0]
        self.assertEqual(note["payload"]["classification"], "AWAITING_REVIEW")
        self.assertEqual(row["evidence"].get("continuation"), None)
        self.assertEqual(row["evidence"].get("continuation_error"),
                         "no-structured-report")

    def test_report_binding_mismatch_is_not_trusted(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt)
        report = self._report(resp, attempt_id="A-other")
        text = json.dumps(report)
        out = self._finish_with_text(resp, text)
        self.assertEqual(out["published"], 1)
        row = self.store.get_responsibility(resp["id"])
        self.assertEqual(row["wait_condition"], "business-report-unknown")
        self.assertEqual(row["evidence"].get("continuation_error"),
                         "report-binding-mismatch")

    def test_sequential_inputs_each_own_idle_are_attributable(self):
        task, attempt = self.managed_work()
        first = self.send_managed(task, attempt, message_id="msg_first")
        at = self.deliver_inbox()
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 1000)
        second = self.send_managed(task, attempt, message_id="msg_second")
        at2 = self.deliver_inbox(at=at + 2000)
        self.client.add_idle("ses_worker", outcome="succeeded", at=at2 + 1000)
        self.clock.advance(seconds=30)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["ambiguous"], 0)
        self.assertEqual(out["published"], 2)
        self.assertEqual(
            self.store.get_responsibility(first["id"])["state"],
            "awaiting_review")
        self.assertEqual(
            self.store.get_responsibility(second["id"])["state"],
            "awaiting_review")

    def test_same_interval_multi_input_is_conservative_unknown(self):
        task, attempt = self.managed_work()
        resp = self.send_managed(task, attempt, message_id="msg_a")
        at = self.deliver_inbox()
        self.append_user("msg_b", at=at + 1000)
        self.client.add_idle("ses_worker", outcome="succeeded", at=at + 2000)
        self.clock.advance(seconds=30)
        out = self.sup.tick_continuation(allow_remote=True)
        self.assertEqual(out["ambiguous"], 1)
        evidence = self.store.get_responsibility(resp["id"])["evidence"]["execution"]
        self.assertEqual(evidence["reason"], "other-input-in-interval")
        self.assertIn("保守 unknown", evidence["explanation"])


class MissingResponsibilityTests(_Base):
    def _terminal_attempt_task(self):
        task = self.ledger.create_task(
            project="p", title="missing", coordinator_session="ses_coord")
        attempt = self.ledger.create_attempt(
            task["id"], directory=self.home, session_id="ses_worker",
            status="terminal_succeeded", dispatch_state="confirmed")
        self.ledger.conn.execute(
            "UPDATE tasks SET status='result_received' WHERE id=?", (task["id"],))
        return task, attempt

    def test_requires_two_reconciles_then_stable_single_diagnostic(self):
        task, attempt = self._terminal_attempt_task()
        first = self.sup.check_missing_responsibilities(now=self.clock.now_ms())
        self.assertEqual((first["created"], first["suspected"]), (0, 1))
        self.assertEqual(self.ledger.list_notifications(
            target_session="ses_coord"), [])
        second = self.sup.check_missing_responsibilities(now=self.clock.now_ms())
        self.assertEqual(second["created"], 1)
        rows = self.store.list_open_responsibilities(task_id=task["id"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["condition_key"], "missing-responsibility")
        notes = self.ledger.list_notifications(
            target_session="ses_coord", state="pending")
        self.assertEqual([n["kind"] for n in notes],
                         [_RESPONSIBILITY_MISSING_KIND])
        # 第三轮不新建责任/通知（条件唯一 + 已有 open 责任）。
        third = self.sup.check_missing_responsibilities(now=self.clock.now_ms())
        self.assertEqual(third["created"], 0)
        self.assertEqual(len(self.ledger.list_notifications(
            target_session="ses_coord")), 1)
        self.assertEqual(len(self.store.list_open_responsibilities(
            task_id=task["id"])), 1)

    def test_pending_notification_defers_missing_diagnostic(self):
        task, attempt = self._terminal_attempt_task()
        pending = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord",
            dedupe_key="pending-todo", payload={"summary": "待投递"},
            project="p", task_id=task["id"], attempt_id=attempt["id"])
        for _ in range(2):
            self.assertEqual(
                self.sup.check_missing_responsibilities(
                    now=self.clock.now_ms())["created"], 0)
        self.assertEqual(self.store.list_open_responsibilities(
            task_id=task["id"]), [])
        self.ledger.ack_notifications([pending["id"]], actor="test")
        for _ in range(2):
            self.sup.check_missing_responsibilities(now=self.clock.now_ms())
        rows = self.store.list_open_responsibilities(task_id=task["id"])
        self.assertEqual(len(rows), 1,
                         "通知投递中不另发 missing；ACK 后才登记诊断")

    def test_active_task_with_open_attempt_is_not_nagged(self):
        task, attempt = self.managed_work(status="running")
        for _ in range(3):
            out = self.sup.check_missing_responsibilities(
                now=self.clock.now_ms())
            self.assertEqual(out["created"], 0)
        self.assertEqual(self.store.list_open_responsibilities(
            task_id=task["id"]), [])

    def test_existing_open_responsibility_suppresses_diagnostic(self):
        task, attempt = self.managed_work(status="terminal_succeeded")
        self.ledger.conn.execute(
            "UPDATE tasks SET status='result_received' WHERE id=?", (task["id"],))
        self.open_resp(task, attempt, condition_key="worker-followup")
        for _ in range(3):
            out = self.sup.check_missing_responsibilities(
                now=self.clock.now_ms())
            self.assertEqual(out["created"], 0)
        self.assertEqual(len(self.store.list_open_responsibilities(
            task_id=task["id"])), 1)

    def test_paused_task_is_not_flagged_and_unrouted_task_warns_human(self):
        task, attempt = self._terminal_attempt_task()
        self.ledger.conn.execute(
            "UPDATE tasks SET status='paused' WHERE id=?", (task["id"],))
        for _ in range(3):
            self.assertEqual(self.sup.check_missing_responsibilities(
                now=self.clock.now_ms())["created"], 0)
        self.ledger.conn.execute(
            "UPDATE tasks SET status='result_received', coordinator_session=NULL "
            "WHERE id=?", (task["id"],))
        self.sup.check_missing_responsibilities(now=self.clock.now_ms())
        self.sup.check_missing_responsibilities(now=self.clock.now_ms())
        rows = self.store.list_open_responsibilities(task_id=task["id"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["state"], "waiting_human")
        self.assertEqual(rows[0]["owner"], "operator")
        alerts = self.ledger.list_alerts(state="new", limit=50)
        self.assertTrue(any(a["kind"] == "responsibility_unowned"
                            for a in alerts))
        self.assertEqual(self.ledger.list_notifications(
            target_session="ses_coord"), [])


if __name__ == "__main__":
    unittest.main()

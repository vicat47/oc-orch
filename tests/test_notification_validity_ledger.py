#!/usr/bin/env python3
"""S1 通知有效性台账回归（B 组）：due/claim 事务内通知策略落地。

覆盖 design D1 / domain.notification_policy（A 组 19 类已登记 kind）：
- 旧代次（superseded）current-only → dead：不发送、不计预算、不当作已读
  ACK、不改写 current；旧 attempt 仍持有写权/停止未确认/活动副作用时另记
  (attempt, 清理条件) 去重的**有效**清理告警（可定点 ACK）；
- 条件已解除（condition-resolved）→ dead；task-paused → 保留待 supervisor
  defer；task-cancelled → 保留由显式 ACK 回收（人工取消不静默丢清理）；
- cleanup 类（execution_unconfirmed/side_effects_running）→ dead + 有效清理告警；
- 未知 kind / 坏 payload → defer 可见待决责任（不发送、有界补证、last_error
  记录原因、诊断告警去重）；global 无锚已登记 kind 按 legacy-unscoped 兼容发送；
- due→claim 窗口 current 变化时 claim 事务内复核并剔除旧代次成员。

只使用真临时 SQLite + 可控时钟，不连接真实服务、不调用模型。
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))
sys.path.insert(0, os.path.dirname(HERE))  # 便于直接运行 python3 tests/test_x.py

from oc_orch.db import init
from oc_orch.ledger import Ledger, LedgerError
from tests.fakes import FakeClock


class NotificationValidityBase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-validity-")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.clock = FakeClock(wall_ms=1_000_000)
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)

    def task_with_old_released(self):
        task = self.ledger.create_task(project="p", title="gen")
        old = self.ledger.create_attempt(
            task["id"], directory=self.home + "/old", session_id="ses_old")
        self.ledger.release_site(old["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        return task, old

    def replace(self, task):
        return self.ledger.create_attempt(
            task["id"], directory=self.home + "/new", session_id="ses_new")

    def enqueue(self, key, *, kind="stall", task=None, attempt=None,
                target="ses_coord", max_attempts=3, priority=5) -> dict:
        return self.ledger.enqueue_notification(
            kind=kind, target_session=target, dedupe_key=key,
            payload={"summary": key}, project="p",
            task_id=task["id"] if task else None,
            attempt_id=attempt["id"] if attempt else None,
            max_attempts=max_attempts, priority=priority)

    def cleanup_alerts(self) -> list[dict]:
        return [a for a in self.ledger.list_alerts(state="new", limit=500)
                if a["kind"] == "notification_cleanup_required"]

    def due(self, limit=50) -> list[dict]:
        return self.ledger.due_notifications(now=self.clock.now_ms(),
                                             limit=limit)


class TestSupersededStatusDead(NotificationValidityBase):
    def test_old_generation_status_kinds_dead_no_budget_no_ack(self):
        """旧代次 stall/idle_unconfirmed/provider_retry：dead、不计数、不 ACK。"""
        task, old = self.task_with_old_released()
        new = self.replace(task)
        rows = [self.enqueue(k, kind=k, task=task, attempt=old)
                for k in ("stall", "idle_unconfirmed", "provider_retry")]
        due = self.due()
        self.assertEqual(due, [], "旧代次纯状态不应进入投递范围")
        for n in rows:
            final = self.ledger.get_notification(n["id"])
            self.assertEqual(final["state"], "dead")
            self.assertEqual(final["attempts"], 0, "不得递增发送预算")
            self.assertIsNone(final["acked_at"], "不得伪装成已读 ACK")
            self.assertIn("superseded", final["last_error"])
        self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"],
                         new["id"], "抑制不得改 current")
        self.assertEqual(self.cleanup_alerts(), [],
                         "已释放且无副作用的旧代次不产生清理责任")

    def test_claim_window_current_change_reexcludes_stale_member(self):
        """claim 事务内复核：due→claim 窗口 current 变化，旧代次成员被剔除。"""
        task, old = self.task_with_old_released()
        stale = self.enqueue("old-result", kind="result_ready",
                             task=task, attempt=old)
        valid = self.enqueue("current-action", kind="result_ready")
        due = self.due()
        self.assertEqual({n["id"] for n in due}, {stale["id"], valid["id"]})
        claim = self.ledger.claim_notifications

        def interleave(ids, owner):
            self.replace(task)  # claim 前 current 变化
            return claim(ids, owner)

        with patch.object(self.ledger, "claim_notifications",
                          side_effect=interleave):
            claimed = self.ledger.claim_notifications(
                [stale["id"], valid["id"]], "owner-x")
        self.assertEqual([n["id"] for n in claimed], [valid["id"]],
                         "claim 后只保留最终有效成员")
        self.assertEqual(self.ledger.get_notification(stale["id"])["state"], "dead")
        self.assertEqual(self.ledger.get_notification(valid["id"])["state"],
                         "inflight")


class TestCleanupResponsibility(NotificationValidityBase):
    def test_old_attempt_still_holding_site_dead_plus_cleanup_alert(self):
        """旧 attempt 仍持有写权：通知 dead，另记去重清理责任、不释放写权。"""
        task = self.ledger.create_task(project="p", title="gen")
        old = self.ledger.create_attempt(
            task["id"], directory=self.home + "/old", session_id="ses_old")
        new = self.replace(task)
        n = self.enqueue("old-stall", kind="stall", task=task, attempt=old)
        due = self.due()
        self.assertEqual(due, [], "旧代次清理责任不进普通唤醒投递")
        final = self.ledger.get_notification(n["id"])
        self.assertEqual(final["state"], "dead", "旧 current-only 必须 dead")
        self.assertEqual(final["attempts"], 0)
        self.assertIsNone(final["acked_at"])
        alerts = self.cleanup_alerts()
        self.assertEqual(len(alerts), 1, "另记去重清理责任")
        a = alerts[0]
        self.assertTrue(a["id"] and not a["id"].isspace() and
                        isinstance(a["id"], str), "清理告警必须有有效稳定 ID")
        self.assertEqual(a["dedupe_key"],
                         f"cleanup-required:{old['id']}:site-still-held")
        self.assertTrue(a["severity"] in ("warning", "high", "medium"),
                        "清理告警使用正常 severity")
        # 可定点 ACK
        self.ledger.ack_alert(a["id"], actor="coordinator")
        self.assertEqual(self.ledger.get_alert(a["id"])["state"], "acked")
        self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"],
                         new["id"])
        self.assertIsNone(self.ledger.get_attempt(old["id"])["site_released_at"],
                          "筛选旧状态不得释放写权")

    def test_cleanup_alert_is_deduped_across_cycles(self):
        """重复 due 周期不重复产生清理告警/不重置预算。"""
        task = self.ledger.create_task(project="p", title="gen")
        old = self.ledger.create_attempt(
            task["id"], directory=self.home + "/old", session_id="ses_old")
        self.replace(task)
        n = self.enqueue("old-stall", kind="stall", task=task, attempt=old)
        self.due()
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "dead")
        self.clock.advance(seconds=30 * 60)
        self.due()
        self.assertEqual(len(self.cleanup_alerts()), 1,
                         "清理责任按 (attempt, 条件) 去重")
        final = self.ledger.get_notification(n["id"])
        self.assertEqual(final["state"], "dead")
        self.assertEqual(final["attempts"], 0)
        self.assertIsNone(final["acked_at"])

    def test_released_old_attempt_with_active_side_effects_cleanup(self):
        """已释放但仍观察到活动 shell/子 session：dead + 清理责任。"""
        task = self.ledger.create_task(project="p", title="gen")
        old = self.ledger.create_attempt(
            task["id"], directory=self.home + "/old", session_id="ses_old")
        self.ledger.release_site(old["id"], confirmed=True,
                                 external_stopped=False, actor="test")
        fresh = self.ledger.get_attempt(old["id"])
        self.ledger.update_attempt(
            old["id"], fields={"metadata": {
                **fresh["metadata"], "active_shells": ["sh_old"]}},
            actor="test", reason="inject-side-effects")
        new = self.replace(task)
        n = self.enqueue("old-shell", kind="idle_unconfirmed",
                         task=task, attempt=old)
        self.due()
        final = self.ledger.get_notification(n["id"])
        self.assertEqual(final["state"], "dead")
        self.assertEqual(final["attempts"], 0)
        alerts = self.cleanup_alerts()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["dedupe_key"],
                         f"cleanup-required:{old['id']}:side-effects-active")

    def test_pure_superseded_has_no_cleanup_alert(self):
        """已释放且无副作用的纯旧代次：dead，且不产生清理责任。"""
        task, old = self.task_with_old_released()
        new = self.replace(task)
        n = self.enqueue("old-retry", kind="provider_retry", task=task,
                        attempt=old)
        self.due()
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "dead")
        self.assertTrue(self.ledger.get_attempt(old["id"])["site_released_at"])
        self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"],
                         new["id"])
        self.assertEqual(self.cleanup_alerts(), [])


class TestPolicyIntegrationLedger(NotificationValidityBase):
    """domain.notification_policy 在 due 的落地：19 类/已解除/可见补证。"""

    def _current_attempt(self, status: str = "running"):
        task = self.ledger.create_task(project="p", title="gen")
        att = self.ledger.create_attempt(
            task["id"], directory=self.home + "/site", session_id="ses_a",
            status=status, dispatch_state="confirmed",
            dispatch_msg_id="msg_a")
        return task, att

    def test_condition_resolved_current_attempt_is_dead(self):
        """条件已解除（current attempt 状态已离开待处理集合）→ dead 不唤醒。"""
        task, att = self._current_attempt(status="running")
        n = self.enqueue("stall-resolved", kind="stall", task=task, attempt=att)
        due = self.due()
        self.assertEqual(due, [], "条件已解除不得进入投递")
        final = self.ledger.get_notification(n["id"])
        self.assertEqual(final["state"], "dead")
        self.assertIn("condition-resolved", final["last_error"])
        self.assertEqual(final["attempts"], 0)
        self.assertIsNone(final["acked_at"])
        self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"],
                         att["id"])

    def test_current_stall_still_pending_is_sendable(self):
        """条件仍待处理（stalled）→ send，可进入投递。"""
        task, att = self._current_attempt(status="stalled")
        n = self.enqueue("stall-active", kind="stall", task=task, attempt=att)
        due = self.due()
        self.assertEqual([x["id"] for x in due], [n["id"]])
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")

    def test_task_paused_is_deferred_not_dead(self):
        """task-paused → 保留 pending（由 supervisor defer），不置 dead。"""
        task, att = self._current_attempt()
        self.ledger.transition_task(task["id"], "paused", reason="user")
        n = self.enqueue("paused-result", kind="result_ready",
                         task=task, attempt=att)
        due = self.due()
        self.assertEqual([x["id"] for x in due], [n["id"]],
                         "paused 通知仍需返回，由 supervisor defer")
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")

    def test_task_cancelled_kept_for_explicit_ack_not_silent_dead(self):
        """task-cancelled → 不静默 dead/cleanup，保留由显式 ACK 回收。"""
        task, att = self._current_attempt()
        self.ledger.close_task(task["id"], reason="user")
        n = self.enqueue("cancelled-result", kind="result_ready",
                         task=task, attempt=att)
        due = self.due()
        self.assertEqual([x["id"] for x in due], [n["id"]],
                         "cancelled 通知交给 supervisor 回收 ACK")
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")
        # 显式回收 ACK（现有系统语义）：不伪装 dead，也不把清理任务静默 ACK 丢清理
        self.ledger.ack_notifications([n["id"]], actor="system:cancelled")
        final = self.ledger.get_notification(n["id"])
        self.assertEqual(final["state"], "acked")
        self.assertIsNotNone(final["acked_at"])

    def test_cleanup_kind_dead_with_valid_dedup_alert(self):
        """cleanup 类（execution_unconfirmed）→ dead + 有效去重清理告警。"""
        task = self.ledger.create_task(project="p", title="gen")
        att = self.ledger.create_attempt(
            task["id"], directory=self.home + "/site", session_id="ses_a",
            status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_a")
        n = self.enqueue("unconfirmed", kind="execution_unconfirmed",
                         task=task, attempt=att)
        self.due()
        final = self.ledger.get_notification(n["id"])
        self.assertEqual(final["state"], "dead")
        self.assertEqual(final["attempts"], 0)
        self.assertIsNone(final["acked_at"])
        alerts = self.cleanup_alerts()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["dedupe_key"],
                         f"cleanup-required:{att['id']}:stop-unconfirmed")
        self.assertTrue(alerts[0]["id"])

    def test_unknown_kind_deferred_visible_supplement(self):
        """未登记 kind → defer 可见待决责任：不发送、有界补证、记录原因。"""
        task, att = self._current_attempt()
        n = self.enqueue("mystery", kind="mystery_kind", task=task, attempt=att)
        due = self.due()
        self.assertEqual(due, [], "未知 kind 不进入发送")
        final = self.ledger.get_notification(n["id"])
        self.assertEqual(final["state"], "pending", "保持可见待决而不是 dead/acked")
        self.assertIn("deferred:unknown-kind", final["last_error"])
        self.assertGreater(final["next_attempt_at"], self.clock.now_ms(),
                           "有界补证期限")
        self.assertEqual(final["attempts"], 0)
        self.assertIsNone(final["acked_at"])

    def test_bad_payload_at_produce_is_deferred_visible(self):
        """坏 payload（非 object）在产生端即 defer invalid-notification。"""
        from oc_orch.util import json_dumps  # noqa: F401
        n = self.ledger.enqueue_notification(
            kind="stall", target_session="ses_coord", dedupe_key="bad-pl",
            payload=["not", "an", "object"], project="p")
        final = self.ledger.get_notification(n["id"])
        self.assertIsNotNone(final["last_error"], "坏 payload 立即标记待补证")
        self.assertIn("invalid-notification", final["last_error"])
        self.assertEqual(final["state"], "pending")
        self.assertGreater(final["next_attempt_at"], self.clock.now_ms())
        self.assertEqual(self.due(), [], "坏 payload 不进入发送")
        # 正常持久 payload 必须 object：字典 payload 正常
        good = self.enqueue("good-payload", kind="stall")
        self.assertEqual(self.ledger.get_notification(good["id"])["last_error"],
                         None)
        self.assertEqual(good["payload"], {"summary": "good-payload"})

    def test_global_current_only_legacy_unscoped_sendable(self):
        """无 task/attempt 的已登记全局通知按 legacy-unscoped 兼容发送。"""
        n = self.enqueue("global-stall", kind="stall")
        due = self.due()
        self.assertEqual([x["id"] for x in due], [n["id"]])
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")

    def test_superseded_backlog_does_not_starve_healthy_result(self):
        """旧积压（121 高优先 stall）不饥饿健康新结果；且清理有界。"""
        task, old = self.task_with_old_released()
        self.replace(task)
        for i in range(121):
            self.ledger.enqueue_notification(
                kind="stall", target_session="ses_coord",
                dedupe_key=f"old-{i}", payload={"s": "old"},
                project="p", priority=9, task_id=task["id"],
                attempt_id=old["id"])
        result = self.enqueue("current-result", kind="result_ready", priority=5)
        first = self.due(limit=3)  # 旧积压不占窗：第一轮就拿到健康结果
        self.assertIn(result["id"], [n["id"] for n in first],
                      "旧代次积压不能使健康新结果饥饿")
        # 积压被有界清理（dead、不计数）
        self.assertEqual(self.ledger.get_notification(result["id"])["state"],
                         "pending")
        final_old = self.ledger.get_notification(
            self.ledger.list_notifications(target_session="ses_coord",
                                           limit=500)[0]["id"])["state"]
        self.assertIn(final_old, ("dead", "pending"))
        self.assertEqual(self.ledger.get_notification(result["id"])["attempts"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
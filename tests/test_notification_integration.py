#!/usr/bin/env python3
"""OpenSpec modularize-orchestrator 3.3：通知集成层回归。

用 FakeClient + 可控时钟 + 真临时 SQLite 覆盖：
- claim/inflight → prompt → sent（ACK 期限正确落库）；
- 重复入队不改 ACK 期限、不产生新业务派单；
- inbox 抑制（目标已有排队输入 → defer，不重复压栈）；
- ACK 超时 → dead + 告警；重试耗尽 → dead + 告警；
- TransportError 不计次数重试、401/403 长退避、404 目标丢失；
- 认领后崩溃 → recover_inflight 恢复待办，重投不产生新通知（业务幂等）。

所有时间边界由 FakeClock 推进；不连接真实服务、不调用模型。
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.client import ApiError, TransportError
from oc_orch.config import DEFAULTS
from oc_orch.supervisor import Supervisor

from tests.fakes import FakeClient, FakeClock


def _cfg(**overrides) -> dict:
    cfg = dict(DEFAULTS)
    cfg.update(overrides)
    return cfg


class NotificationBase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-notif-")
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.clock = FakeClock(wall_ms=1_000_000)
        self.client = FakeClient()
        self.cfg = _cfg()
        self.sup = Supervisor(self.tmp, cfg=self.cfg, client=self.client,
                              clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger

    def enqueue(self, kind: str = "idle_unconfirmed", *, dedupe: str = "dk-1",
                task_id=None, attempt_id=None, max_attempts: int = 3,
                priority: int = 5, payload: dict | None = None,
                target: str = "ses_c") -> dict:
        return self.ledger.enqueue_notification(
            kind=kind, target_session=target, dedupe_key=dedupe,
            payload=payload or {"summary": "x"}, project="p",
            task_id=task_id, attempt_id=attempt_id, priority=priority,
            max_attempts=max_attempts)

    def due(self) -> list[dict]:
        return self.ledger.due_notifications(now=self.clock.now_ms())


class TestDispatchSend(NotificationBase):
    def test_claim_inflight_sent_with_ack_deadline(self):
        """pending → claim/inflight → prompt 成功 → sent，ACK 期限 = now+ack_timeout。"""
        n = self.enqueue()
        self.client.set_session("ses_c", outcome=None)
        summary = self.sup.dispatch_notifications()
        self.assertEqual(summary["due"], 1)
        self.assertEqual(summary["sent"], 1)
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["state"], "sent")
        self.assertEqual(n2["attempts"], 1)
        self.assertTrue(n2["message_id"].startswith("msg_ocorch"))
        self.assertIsNone(n2["claim_owner"])
        self.assertEqual(n2["next_attempt_at"],
                         self.clock.now_ms() + int(self.cfg["ack_timeout"]) * 1000)
        self.assertEqual(len(self.client.prompt_calls), 1)
        wake = self.client.prompt_calls[0]
        self.assertEqual(wake["session_id"], "ses_c")
        self.assertEqual(wake["delivery"], "queue")

    def test_duplicate_enqueue_keeps_ack_deadline(self):
        """同一去重键重复入队：合并信息但保留 ACK 期限，不产生新派单。"""
        n = self.enqueue(dedupe="stall:A1:longtool:1")
        self.client.set_session("ses_c", outcome=None)
        self.sup.dispatch_notifications()
        sent_at = self.clock.now_ms()
        ack_deadline = sent_at + int(self.cfg["ack_timeout"]) * 1000
        # 同一业务事实再次入队（合并有效信息）
        n2 = self.ledger.enqueue_notification(
            kind="stall", target_session="ses_c", dedupe_key="stall:A1:longtool:1",
            payload={"summary": "two"}, project="p", priority=7,
            max_attempts=3)
        self.assertEqual(n2["id"], n["id"])
        self.assertEqual(n2["state"], "sent")
        self.assertEqual(n2["next_attempt_at"], ack_deadline,
                         "sent 通知的 ACK 期限不能被重复入队重置")
        self.assertEqual(n2["payload"]["summary"], "two", "有效信息应合并")
        # ACK 窗口内再对账：不再投递（未到期）
        self.clock.advance(seconds=100)
        self.assertEqual(self.sup.dispatch_notifications()["due"], 0)
        self.assertEqual(len(self.client.prompt_calls), 1, "窗口内不得重复唤醒")

    def test_ack_threshold_before_send_marks_dead(self):
        """pending 且 attempts>=max：发送前判定消耗 → dead + 告警。"""
        n = self.enqueue(max_attempts=1)
        self.ledger.mark_notification_retry(n["id"], error="pre",
                                            next_attempt_at=self.clock.now_ms(),
                                            owner="test", count=True)
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "dead")
        self.ledger.requeue_notification(n["id"], next_attempt_at=self.clock.now_ms(),
                                         actor="test")
        self.client.set_session("ses_c", outcome=None)
        summary = self.sup.dispatch_notifications()
        self.assertEqual(summary["failed"], 1)
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["state"], "dead")
        self.assertIn("max-attempts", n2["last_error"])
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=50)
                  if a["kind"] == "notification_dead"]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["dedupe_key"], f"notif-dead:{n['id']}")


class TestDispatchSuppress(NotificationBase):
    def test_inbox_pending_suppresses_and_defers(self):
        """目标 inbox 已有排队输入 → 抑制唤醒，defer 到 min(300, ack_timeout)。"""
        n = self.enqueue()
        self.client.set_session("ses_c", outcome=None)
        self.client.set_inbox("ses_c", [{"id": "msg_existing"}])
        summary = self.sup.dispatch_notifications()
        self.assertEqual(summary["suppressed"], 1)
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["state"], "pending", "抑制只 defer，不 claim")
        expected = self.clock.now_ms() + min(300, int(self.cfg["ack_timeout"])) * 1000
        self.assertEqual(n2["next_attempt_at"], expected)
        self.assertEqual(len(self.client.prompt_calls), 0, "抑制不得发送")
        audit = [a for a in self.ledger.list_audit(limit=100)
                 if a["action"] == "wake.suppressed"]
        self.assertEqual(len(audit), 1)

    def test_recent_unacked_sent_suppresses(self):
        """存在未确认 sent（ACK 窗口内）→ 抑制，避免重复唤醒压栈。"""
        n = self.enqueue()
        self.client.set_session("ses_c", outcome=None)
        self.sup.dispatch_notifications()  # 首次发送 sent
        self.assertEqual(len(self.client.prompt_calls), 1)
        # 同 session 入队第二条新通知；sent 仍在 ACK 窗口内 → 抑制
        n2 = self.enqueue(dedupe="dk-2")
        summary = self.sup.dispatch_notifications()
        self.assertGreaterEqual(summary["suppressed"], 1)
        self.assertEqual(len(self.client.prompt_calls), 1, "窗口内不得二次发送")
        state = self.ledger.get_notification(n2["id"])["state"]
        self.assertEqual(state, "pending")


class TestDispatchFailures(NotificationBase):
    def _session_or_fail(self):
        self.client.set_session("ses_c", outcome=None)
        return "ses_c"

    def test_transport_error_retries_without_count(self):
        """TransportError：重试但 attempts 不计次数，避免带宽耗尽误判。"""
        n = self.enqueue(max_attempts=3)
        self.client.set_session("ses_c", outcome=None)
        self.client.fail("prompt", TransportError("/x", "connection reset"))
        summary = self.sup.dispatch_notifications()
        self.assertEqual(summary["failed"], 1)
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["state"], "pending")
        self.assertEqual(n2["attempts"], 0, "传输错不计 attempts")
        backoff = min(int(self.cfg["notification_retry_max"]),
                      int(self.cfg["notification_retry_min"]) * (2 ** 0))
        self.assertEqual(n2["next_attempt_at"],
                         self.clock.now_ms() + backoff * 1000)

    def test_http_500_retry_counts_and_backs_off(self):
        """HTTP 5xx：计次数重试，退避增长。"""
        n = self.enqueue(max_attempts=3)
        self.client.set_session("ses_c", outcome=None)
        self.client.fail("prompt", ApiError(500, "/prompt", None, "server"), times=2)
        self.sup.dispatch_notifications()
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["attempts"], 1)
        self.assertEqual(n2["state"], "pending")
        backoff = min(int(self.cfg["notification_retry_max"]),
                      int(self.cfg["notification_retry_min"]))
        self.assertEqual(n2["next_attempt_at"],
                         self.clock.now_ms() + backoff * 1000)
        self.clock.advance(seconds=backoff)
        self.sup.dispatch_notifications()
        n3 = self.ledger.get_notification(n["id"])
        self.assertEqual(n3["attempts"], 2)
        self.assertEqual(n3["state"], "pending")
        self.assertEqual(n3["next_attempt_at"], self.clock.now_ms() + min(
            int(self.cfg["notification_retry_max"]), backoff * 2) * 1000)

    def test_prompt_403_auth_long_backoff_and_alert(self):
        """prompt 401/403：长退避（1h）+ wake_auth 告警。"""
        n = self.enqueue(max_attempts=3)
        self.client.set_session("ses_c", outcome=None)
        self.client.fail("prompt", ApiError(403, "/prompt", None, "forbidden"))
        summary = self.sup.dispatch_notifications()
        self.assertEqual(summary["failed"], 1)
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["state"], "pending")
        self.assertEqual(n2["next_attempt_at"],
                         self.clock.now_ms() + 3600 * 1000)
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=50)
                  if a["kind"] == "wake_auth"]
        self.assertEqual(len(alerts), 1)

    def test_prompt_404_target_lost_dead_alert(self):
        """目标 session 404：标记 dead + wake_target_lost。"""
        n = self.enqueue(max_attempts=3)
        self.client.set_session("ses_c", outcome=None)
        self.client.fail("prompt", ApiError(404, "/prompt", None, "gone"))
        self.sup.dispatch_notifications()
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["state"], "dead")
        self.assertIn("prompt-404", n2["last_error"])
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=50)
                  if a["kind"] == "wake_target_lost"]
        self.assertEqual(len(alerts), 1)

    def test_target_get_session_404_marks_dead(self):
        """get_session(target) 404：发送前判 target 丢失 → dead + 告警。"""
        n = self.enqueue(max_attempts=3)
        self.client.fail("get_session", ApiError(404, "/session/x", None, "gone"))
        summary = self.sup.dispatch_notifications()
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "dead")
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=50)
                  if a["kind"] == "wake_target_lost"]
        self.assertEqual(len(alerts), 1)

    def test_exhaustion_marks_dead_alert(self):
        """重试耗尽：max_attempts 用尽 → dead + notification_dead 告警。"""
        n = self.enqueue(max_attempts=1)
        self.client.set_session("ses_c", outcome=None)
        self.client.fail("prompt", ApiError(503, "/prompt", None, "down"))
        self.sup.dispatch_notifications()
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["state"], "dead")
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=50)
                  if a["kind"] == "notification_dead"]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["dedupe_key"], f"notif-dead:{n['id']}")


class TestAckTimeoutAndAck(NotificationBase):
    def test_ack_timeout_expires_to_dead_with_alert(self):
        """已投递多次未确认：超过 ACK 期限 → dead + notification_unacked。"""
        n = self.enqueue(max_attempts=2)
        t0 = self.clock.now_ms()
        self.ledger.mark_notification_sent(n["id"], message_id="msg_a",
                                           next_attempt_at=t0 + 1000,
                                           owner="test")
        self.clock.advance(seconds=int(self.cfg["ack_timeout"]) + 5)
        self.ledger.mark_notification_sent(n["id"], message_id="msg_b",
                                           next_attempt_at=self.clock.now_ms() + 1000,
                                           owner="test")
        self.clock.advance(seconds=int(self.cfg["ack_timeout"]) + 5)
        self.sup.expire_notifications()
        self.sup.expire_notifications()  # 重复对账不产生第二份告警。
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["state"], "dead")
        self.assertEqual(n2["last_error"], "unacked-timeout")
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=50)
                  if a["kind"] == "notification_unacked"]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["dedupe_key"], f"notif-expired:{n['id']}")

    def test_paused_task_defers_cancelled_acks(self):
        """暂停任务的通知 defer；已取消任务的通知 ack（保留人工意图）。"""
        task_p = self.ledger.create_task(project="p", title="paused")
        task_c = self.ledger.create_task(project="p", title="cancelled")
        self.ledger.transition_task(task_p["id"], "paused", reason="manual")
        self.ledger.close_task(task_c["id"], reason="user")
        np_ = self.enqueue(dedupe="paused-dk", task_id=task_p["id"])
        nc = self.enqueue(dedupe="cancelled-dk", task_id=task_c["id"])
        self.client.set_session("ses_c", outcome=None)
        summary = self.sup.dispatch_notifications()
        self.assertGreaterEqual(summary["suppressed"], 2)
        self.assertEqual(self.ledger.get_notification(np_["id"])["state"],
                         "pending")
        self.assertEqual(self.ledger.get_notification(nc["id"])["state"],
                         "acked")
        self.assertEqual(len(self.client.prompt_calls), 0, "暂停/取消不发送")


class TestCrashRecovery(NotificationBase):
    def test_late_send_result_cannot_overwrite_concurrent_ack(self):
        """网络窗口内 ACK 先落库；晚到成功/错误均不能重开这份责任。"""
        from unittest.mock import patch

        cases = [None, TransportError("/prompt", "lost"),
                 ApiError(500, "/prompt", None, "server"),
                 ApiError(403, "/prompt", None, "auth"),
                 ApiError(404, "/prompt", None, "missing")]
        for index, error in enumerate(cases):
            with self.subTest(error=type(error).__name__ if error else "success"):
                target = f"ses_c_{index}"
                self.client.set_session(target, outcome=None)
                n = self.enqueue(dedupe=f"ack-during-send:{index}", target=target)

                def response(*args, **kwargs):
                    self.ledger.ack_notifications([n["id"]], actor="concurrent-web")
                    if error:
                        raise error
                    return {"data": {"id": kwargs["message_id"]}}

                with patch.object(self.client, "prompt", side_effect=response):
                    self.sup.dispatch_notifications()
                final = self.ledger.get_notification(n["id"])
                self.assertEqual(final["state"], "acked")
                self.assertIsNotNone(final["acked_at"])
                self.assertEqual(self.due(), [])

    def test_lost_response_inbox_confirmation_keeps_identity_and_ack_deadline(self):
        n = self.enqueue()
        self.client.set_session("ses_c", outcome=None)
        self.client.fail("prompt", TransportError("/prompt", "response lost"))
        self.sup.dispatch_notifications()
        pending = self.ledger.get_notification(n["id"])
        msg = pending["message_id"]
        self.assertEqual(pending["state"], "pending")
        self.assertEqual(msg, self.client.prompt_calls[0]["message_id"])
        event = {"data": {"inboxID": msg, "sessionID": "ses_c"}}
        self.clock.advance(seconds=1)
        self.sup.handle_inbox_event(event)
        sent = self.ledger.get_notification(n["id"])
        self.assertEqual(sent["state"], "sent")
        self.assertEqual(sent["message_id"], msg)
        self.assertEqual(sent["attempts"], 1)
        self.clock.advance(seconds=1)
        self.sup.handle_inbox_event(event)
        self.assertEqual(self.ledger.get_notification(n["id"]), sent)
        self.ledger.ack_notifications([n["id"]], actor="test")
        self.sup.handle_inbox_event(event)
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "acked")
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_inbox_confirmation_racing_ack_cannot_reopen_notification(self):
        from unittest.mock import patch

        n = self.enqueue()
        self.ledger.claim_notifications([n["id"]], "test")
        self.ledger.set_notification_message(n["id"], message_id="msg_pending")
        lookup = self.ledger.notification_for_message

        def interleave(message_id):
            row = lookup(message_id)
            self.ledger.ack_notifications([n["id"]], actor="concurrent-web")
            return row

        with patch.object(self.ledger, "notification_for_message", side_effect=interleave):
            self.sup.handle_inbox_event({"data": {"inboxID": "msg_pending"}})
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "acked")

    def test_recover_inflight_resumes_responsibility_idempotently(self):
        """认领后崩溃：recover_inflight 恢复待办；重投同通知不产生新行。"""
        n = self.enqueue(dedupe="crash-dk")
        claimed = self.ledger.claim_notifications([n["id"]], "sup-1")
        self.assertEqual(len(claimed), 1)
        self.assertEqual(self.ledger.get_notification(n["id"])["state"],
                         "inflight")
        # 模拟崩溃后新实例启动：recover_inflight 需要 claim_at 超过窗口
        self.clock.advance(seconds=130)
        self.client.set_session("ses_c", outcome=None)
        recovered = self.ledger.recover_inflight()
        self.assertEqual(recovered, 1)
        n2 = self.ledger.get_notification(n["id"])
        self.assertEqual(n2["state"], "pending")
        self.assertIsNone(n2["claim_owner"])
        self.assertIn("recovered", n2["last_error"])
        # 恢复后重投：同一通知 id、同一消息语义，不新增通知行
        summary = self.sup.dispatch_notifications()
        self.assertEqual(summary["sent"], 1)
        rows = self.ledger.list_notifications(target_session="ses_c", limit=10)
        self.assertEqual(len(rows), 1, "崩溃恢复不得产生重复通知")
        n3 = self.ledger.get_notification(n["id"])
        self.assertEqual(n3["state"], "sent")
        self.assertEqual(n3["dedupe_key"], "crash-dk")
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_acked_notification_not_resurrected_by_duplicate_enqueue(self):
        """已 ack 通知：同一去重键再次入队不得复活。"""
        n = self.enqueue(dedupe="ack-dk")
        self.ledger.ack_notifications([n["id"]], actor="test")
        again = self.ledger.enqueue_notification(
            kind="idle_unconfirmed", target_session="ses_c",
            dedupe_key="ack-dk",
            payload={"summary": "later"}, project="p", max_attempts=3)
        self.assertEqual(again["id"], n["id"])
        self.assertEqual(again["state"], "acked")
        rows = self.ledger.list_notifications(target_session="ses_c", limit=10)
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)

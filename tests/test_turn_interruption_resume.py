"""回合中断恢复边界：替换已被新规格否定的“陌生输入即清门”假设。

直接聊天仍可用，但API无可信真人来源。只有显式登记的新用户回合完成才
具备解除新hold的条件；历史manual保持原样，不能猜测导入或自动删除。
"""

from __future__ import annotations

import tempfile
import unittest

from oc_orch.config import DEFAULTS
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock


class TurnInterruptionResumeTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="turn-resume-", dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_coord", outcome=None)
        self.sup = Supervisor(self.home, cfg=dict(DEFAULTS),
                              client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.ledger.create_task(project="p", title="coord",
                                coordinator_session="ses_coord")

    def interrupt(self):
        self.sup.handle_coordinator_terminal(
            "ses_coord", "interrupted", "user",
            {"id": "evt-interrupt", "created": self.clock.now_ms(),
             "data": {"sessionID": "ses_coord"}})

    def external_inbox(self, *, message_id="msg_user"):
        self.sup.ingest_event({
            "id": "evt-inbox-" + message_id,
            "type": "session.inbox.enqueued",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "inboxID": message_id},
        })

    def enqueue_pending(self):
        return self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord",
            dedupe_key="turn-resume-pending",
            payload={"summary": "pending"},
            project="p", next_attempt_at=self.clock.now_ms() + 3600 * 1000)

    def test_turn_interrupt_does_not_clear_on_untrusted_external_input(self):
        self.interrupt()
        snap = self.sup.coordinator_control.get_session("ses_coord")
        self.assertEqual(snap["hold"]["reason"], "coordinator-interrupted")
        notification = self.enqueue_pending()
        alert = self.ledger.add_alert(
            severity="high", kind="target_gate_manual", title="manual",
            detail="manual", dedupe_key="wake-gate:manual:ses_coord")

        self.external_inbox()

        self.assertEqual(self.sup.coordinator_control.get_session("ses_coord"), snap)
        refreshed = self.ledger.get_notification(notification["id"])
        self.assertGreater(refreshed["next_attempt_at"], self.clock.now_ms())
        self.assertEqual(self.ledger.get_alert(alert["id"])["state"], "new")
        rows = self.sup.conn.execute(
            "SELECT action FROM audit WHERE entity='session' AND entity_id=? "
            "AND action='target-gate.clear-turn-interrupt'",
            ("ses_coord",)).fetchall()
        self.assertEqual(len(rows), 0, "不可信输入不能造自动恢复审计")

    def test_explicit_project_pause_is_not_auto_cleared(self):
        self.sup.target_gate.block_manual(
            "ses_coord", reason="coordinator-paused")
        self.external_inbox()
        snap = self.sup.target_gate.get("ses_coord")
        self.assertEqual(snap["state"], "manual-blocked")
        self.assertEqual(snap["reason"], "coordinator-paused")

    def test_late_turn_interrupt_does_not_downgrade_project_pause(self):
        self.sup.target_gate.block_manual(
            "ses_coord", reason="coordinator-paused")
        self.interrupt()
        snap = self.sup.target_gate.get("ses_coord")
        self.assertEqual(snap["reason"], "coordinator-paused",
                         "后续回合中断不能把显式项目暂停降级")
        self.external_inbox()
        self.assertEqual(self.sup.target_gate.get("ses_coord")["state"],
                         "manual-blocked")

    def test_stale_external_event_does_not_clear_newer_turn_interrupt(self):
        self.interrupt()
        gate_at = self.sup.coordinator_control.get_session("ses_coord")["hold_origin_at"]
        self.sup.ingest_event({
            "id": "evt-stale-inbox",
            "type": "session.inbox.enqueued",
            "created": gate_at - 1,
            "data": {"sessionID": "ses_coord", "inboxID": "msg_stale"},
        })
        snap = self.sup.coordinator_control.get_session("ses_coord")
        self.assertIsNotNone(snap["hold"],
                         "迟到的旧输入事件不能清除之后才产生的回合中断")

    def test_automatic_wake_input_does_not_clear_turn_interrupt(self):
        self.interrupt()
        reserved = self.ledger.reserve_target_input(
            "ses_coord", owner="sup-test", notification_ids=[],
            body="", source="automatic_wake", now=self.clock.now_ms())
        self.sup.ingest_event({
            "id": "evt-auto-wake",
            "type": "session.inbox.enqueued",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord",
                     "inboxID": reserved["input"]["message_id"]},
        })
        snap = self.sup.coordinator_control.get_session("ses_coord")
        self.assertIsNotNone(snap["hold"])

    def test_resume_then_normal_dispatch_delivers_pending(self):
        self.interrupt()
        notification = self.enqueue_pending()
        self.external_inbox()
        self.clock.advance(seconds=1)
        at = self.clock.now_ms()
        self.client.append_message("ses_coord", {
            "id": "msg_trusted", "type": "user", "time": {"created": at}, "text": "continue"})
        self.sup.coordinator_control.begin_user_turn(
            "ses_coord", message_id="msg_trusted", origin_at=at,
            operation_id="op-trusted", trusted=True)
        self.clock.advance(seconds=1)
        at = self.clock.now_ms()
        self.client.add_idle("ses_coord", outcome="succeeded", at=at)
        self.client.set_session("ses_coord", outcome="succeeded", time_={"idle": at})
        self.sup.observe_coordinators({})
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1,
                         "恢复后必须重新进入正常投递路径")
        self.assertEqual(self.ledger.get_notification(notification["id"])["state"],
                         "sent")

    def test_interrupt_cancels_own_queued_wake_and_keeps_foreign(self):
        notification = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord",
            dedupe_key="turn-cancel-own", payload={"summary": "own"},
            project="p")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        own_message = self.client.prompt_calls[0]["message_id"]
        now = self.clock.now_ms()
        self.client.set_inbox("ses_coord", [
            {"id": own_message, "delivery": "queue",
             "time": {"created": now}},
            {"id": "msg_foreign", "delivery": "queue",
             "time": {"created": now}},
        ])
        self.interrupt()
        # 自有排队唤醒被取消；陌生输入原样保留
        remaining = [i["id"] for i in self.client.inbox("ses_coord")]
        self.assertEqual(remaining, ["msg_foreign"])
        row = self.ledger.get_notification(notification["id"])
        self.assertEqual(row["state"], "pending")
        self.assertGreater(row["next_attempt_at"], now,
                           "中断后通知应 defer，释放 hold 后再投递")
        audits = self.sup.conn.execute(
            "SELECT detail FROM audit WHERE action='wake.cancelled' "
            "AND entity_id='ses_coord' ORDER BY id DESC LIMIT 1").fetchone()
        self.assertIsNotNone(audits)
        self.assertIn(own_message, audits["detail"])
        slot = self.ledger.input_for_receipt("ses_coord", own_message)
        self.assertEqual(slot["admission_state"], "cancelled")
        self.assertFalse(slot["outstanding"],
                         "主动取消必须立即释放槽位，不依赖 SSE 取消事件")


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""独立对抗回归：目标级发送竞态、会话错配和取消事件。"""
import unittest
from unittest.mock import patch

from oc_orch.client import TransportError
from oc_orch.config import DEFAULTS
from oc_orch.supervisor import Supervisor
from tests import test_wake_delivery_acceptance as fixtures


class WakeDeliveryRaceTests(unittest.TestCase):
    setUp = fixtures.WakeDeliveryAcceptanceTests.setUp
    enqueue = fixtures.WakeDeliveryAcceptanceTests.enqueue
    old_attempt = fixtures.WakeDeliveryAcceptanceTests.old_attempt
    replace = fixtures.WakeDeliveryAcceptanceTests.replace

    def test_superseded_backlog_cannot_starve_a_healthy_current_result(self):
        task = self.ledger.create_task(project="acceptance", title="legacy backlog")
        old = self.ledger.create_attempt(
            task["id"], directory=str(self.home / "old"), session_id="ses_old",
            status="stalled", dispatch_state="confirmed")
        count = int(DEFAULTS["notification_batch"]) * 6 + 1
        # 先在合法当前代次积压，再替换代次；不能只测产生端直接拒绝旧通知。
        for i in range(count):
            self.ledger.enqueue_notification(
                kind="stall", target_session="ses_coord", dedupe_key=f"old-{i}",
                payload={"summary": "old"}, project="acceptance", priority=9,
                task_id=task["id"], attempt_id=old["id"],
            )
        self.ledger.release_site(old["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        self.replace(task)
        result = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord", dedupe_key="current-result",
            payload={"summary": "current"}, project="acceptance", priority=5,
        )
        self.sup.dispatch_notifications()
        self.sup.dispatch_notifications()
        self.assertEqual(self.ledger.get_notification(result["id"])["state"], "sent",
                         "旧状态积压不能使健康目标的新结果超过两轮仍未发起投递")

    def lost_response(self):
        n = self.enqueue("response-unknown")
        self.client.fail("prompt", TransportError("/prompt", "response lost"))
        self.sup.dispatch_notifications()
        return n, self.client.prompt_calls[0]["message_id"]

    def test_two_senders_cannot_fill_same_target_in_network_window(self):
        self.sup.cfg["notification_batch"] = 1
        self.enqueue("first")
        self.enqueue("second")
        other = Supervisor(str(self.home), cfg={**DEFAULTS, "notification_batch": 1},
                           client=self.client, clock=self.clock)
        self.addCleanup(other.conn.close)
        prompt = self.client.prompt
        inside = False

        def interleave(*args, **kwargs):
            nonlocal inside
            if not inside:
                inside = True
                other.dispatch_notifications()
            return prompt(*args, **kwargs)

        with patch.object(self.client, "prompt", side_effect=interleave):
            self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1,
                         "首个POST未返回期间第二发送者不能抢出另一条wake")
        rows = self.ledger.list_notifications(target_session="ses_coord")
        self.assertEqual(len(rows), 2)
        self.assertTrue(any(n["state"] == "pending" for n in rows))

    def test_delivered_and_acked_wake_does_not_permanently_hold_the_target(self):
        first = self.enqueue("first-result", kind="result_ready")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        message_id = self.client.prompt_calls[0]["message_id"]
        self.clock.advance(seconds=1)
        self.client.deliver_inbox("ses_coord", at=self.clock.now_ms())
        self.sup.ingest_event({
            "id": "evt_first_delivered", "type": "session.inbox.delivered",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "inboxID": message_id},
        })
        self.ledger.ack_notifications([first["id"]], actor="coordinator")
        second = self.enqueue("second-result", kind="result_ready")
        self.sup.dispatch_notifications()
        self.sup.dispatch_notifications()
        self.assertEqual(self.ledger.get_notification(second["id"])["state"], "sent",
                         "已投递且业务确认的旧输入不能永久占住目标槽")
        self.assertEqual(len(self.client.prompt_calls), 2)

    def test_crash_before_possible_has_bounded_restart_recovery(self):
        notice = self.enqueue("prepared-crash", kind="result_ready")
        with patch.object(self.ledger, "_mark_input_possible_in_tx", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [], "崩溃点在可能发送标记之前")
        records = self.ledger.list_pending_inputs(target_session="ses_coord", outstanding=True)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["admission_state"], "prepared")
        deadline = records[0].get("reconcile_deadline") or (
            self.clock.now_ms() + int(DEFAULTS["ack_timeout"]) * 1000)
        self.clock.advance(wall=max(1, deadline - self.clock.now_ms()) + 1000)
        recovered = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                               client=self.client, clock=self.clock)
        self.addCleanup(recovered.conn.close)
        recovered.ledger.recover_inflight()
        recovered.dispatch_notifications()
        recovered.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1,
                         "确认从未possible的prepared输入不能被死owner永久持有")
        self.assertEqual(self.ledger.get_notification(notice["id"])["state"], "sent")

    def test_fast_restart_before_claim_expiry_eventually_recovers_without_new_notice(self):
        notice = self.enqueue("fast-prepared-crash", kind="result_ready")
        with patch.object(self.ledger, "_mark_input_possible_in_tx", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.sup.dispatch_notifications()
        receipt = self.ledger.list_pending_inputs(target_session="ses_coord", outstanding=True)[0]
        self.clock.advance(seconds=1)
        recovered = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                               client=self.client, clock=self.clock)
        self.addCleanup(recovered.conn.close)
        # 真实常见窗口：重启很快，启动时120s claim宽限尚未过，首次回收为0。
        self.assertEqual(recovered.ledger.recover_inflight(), 0)
        recovered.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        deadline = receipt.get("reconcile_deadline") or (
            receipt["prepared_at"] + int(DEFAULTS["ack_timeout"]) * 1000)
        self.clock.advance(wall=max(1, deadline - self.clock.now_ms()) + 1000)
        # 没有新通知、也不再模拟第二次启动：正常调谐必须在到期后恢复原责任。
        recovered.dispatch_notifications()
        recovered.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1,
                         "快速重启错过首次claim回收后不能让prepared/inflight永久滞留")
        self.assertEqual(self.ledger.get_notification(notice["id"])["state"], "sent")

    def test_admission_evidence_for_another_session_cannot_confirm_this_batch(self):
        n, msg = self.lost_response()
        self.sup.ingest_event({
            "id": "evt_wrong_session", "type": "session.inbox.enqueued",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_foreign", "inboxID": msg},
        })
        final = self.ledger.get_notification(n["id"])
        self.assertEqual(final["state"], "pending")
        self.assertEqual(final["attempts"], 0)

    def test_unknown_response_and_failed_message_query_do_not_blindly_repost(self):
        notice, original = self.lost_response()
        self.clock.advance(seconds=31)
        self.client.fail("get_message", TransportError("/message", "observation unavailable"),
                         times=10)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1,
                         "inbox为空但定点消息查询失败不构成未受理证据，不能盲重投")
        self.assertEqual(self.ledger.get_notification(notice["id"])["message_id"], original)

    def test_cancelled_input_is_not_recorded_as_sent_or_business_ack(self):
        n, msg = self.lost_response()
        self.sup.ingest_event({
            "id": "evt_cancelled", "type": "session.inbox.cancelled",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "inboxID": msg},
        })
        final = self.ledger.get_notification(n["id"])
        self.assertNotIn(final["state"], ("sent", "acked"))
        self.assertIsNone(final["acked_at"])


if __name__ == "__main__":
    unittest.main()

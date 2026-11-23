"""D9 独立发布验收：旧代次抑制、无路由责任和 ACK/新版本交错。"""
import json
from unittest.mock import patch

from tests.test_provenance_remediation import RemediationBase, CFG, _assistant
from tests.fakes import FakeClock
from oc_orch.adapters import results
from oc_orch.db import init
from oc_orch.ledger import Ledger
from oc_orch.supervisor import Supervisor


class PublicationAcceptanceTests(RemediationBase):
    def supervisor(self):
        clock = FakeClock(wall_ms=4_000_000_000_000)
        sup = Supervisor(self.home, cfg=CFG, client=self.client, clock=clock)
        self.addCleanup(sup.conn.close)
        self.client.set_session("ses_c", time_={})
        return sup

    def test_old_generation_pending_result_does_not_wake_current_work(self):
        task, att, first = self._collected(final_contents={"out.txt": "v1"})
        sup = self.supervisor()
        newer = self.ledger.create_attempt(task["id"], directory=self.site + "-next",
                                           session_id="ses_next", write_scope=[self.site + "-next"])
        sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"], newer["id"])
        row = self.ledger.get_notification(first["notification_ids"][0])
        self.assertEqual(row["state"], "dead")
        self.assertIsNone(row["acked_at"])
        self.assertIn("superseded", row["last_error"])
        self.assertEqual(row["attempts"], 0)

    def test_generation_change_before_claim_and_late_response_stay_suppressed(self):
        task, att, first = self._collected(final_contents={"out.txt": "v1"})
        nid = first["notification_ids"][0]
        second = Ledger(init(self.home), self.home)
        self.addCleanup(second.conn.close)
        due = self.ledger.due_notifications(now=4_000_000_000_000)
        self.assertIn(nid, [n["id"] for n in due])
        second.create_attempt(task["id"], directory=self.site + "-next",
                              session_id="ses_next", write_scope=[self.site + "-next"])
        self.assertEqual(self.ledger.claim_notifications([nid], "late-owner"), [])
        suppressed = self.ledger.get_notification(nid)
        self.assertEqual(suppressed["state"], "dead")
        self.ledger.mark_notification_sent(nid, message_id="late-response",
            next_attempt_at=4_000_000_000_001, owner="late-owner")
        self.assertEqual(self.ledger.get_notification(nid), suppressed)

    def test_pause_after_due_before_claim_does_not_burn_or_wake(self):
        task, _, first = self._collected(final_contents={"out.txt": "v1"})
        nid = first["notification_ids"][0]
        self.assertTrue(self.ledger.due_notifications(now=4_000_000_000_000))
        second = Ledger(init(self.home), self.home)
        self.addCleanup(second.conn.close)
        second.transition_task(task["id"], "paused", reason="manual")
        before = self.ledger.get_notification(nid)
        self.assertEqual(self.ledger.claim_notifications([nid], "late-owner"), [])
        self.assertEqual(self.ledger.get_notification(nid), before)

    def test_ack_of_v1_during_v2_collection_does_not_ack_v2(self):
        _, att, first = self._collected(deliverables=(), texts=("V1",))
        second = Ledger(init(self.home), self.home)
        self.addCleanup(second.conn.close)
        self.client._replace_message("ses_w", _assistant("msg_in-a0", "V2", 1000010))
        reader = results._CollectionIO.read_message_window

        def interleave(*args, **kwargs):
            data = reader(*args, **kwargs)
            second.ack_notifications(first["notification_ids"], actor="read-v1")
            return data

        with patch.object(results._CollectionIO, "read_message_window", side_effect=interleave):
            out = results.collect_attempt(self.ledger, self.client, att["id"], CFG)
        self.assertNotEqual(first["result_version"], out["result_version"])
        self.assertEqual(second.get_notification(first["notification_ids"][0])["state"], "acked")
        self.assertEqual(second.get_notification(out["notification_ids"][0])["state"], "pending")

    def test_unrouted_backlog_over_300_is_all_routable_without_spending_budget(self):
        sup = self.supervisor()
        ids = []
        for i in range(321):
            task = self.ledger.create_task(project="backlog", title=f"result-{i}")
            row = self.ledger.enqueue_notification(
                kind="result_ready", target_session=None, dedupe_key=f"result-{i}",
                payload={"result_version": f"rv1-{i}", "classification": "RESULT_READY"},
                project="backlog", task_id=task["id"], metadata={"route_pending": 1})
            ids.append(row["id"])
        sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        self.ledger.claim_coordinator(project="backlog", session_id="ses_c", actor="test")
        sup._fill_pending_routes()
        for nid in ids:
            row = self.ledger.get_notification(nid)
            self.assertEqual(row["target_session"], "ses_c")
            self.assertEqual(row["attempts"], 0)
            self.assertEqual(row["state"], "pending")
            self.assertFalse(row["metadata"].get("route_pending"))

    def test_paused_backlog_does_not_starve_other_due_work(self):
        sup = self.supervisor()
        paused = self.ledger.create_task(project="paused", title="paused")
        self.ledger.transition_task(paused["id"], "paused", reason="manual")
        pending = []
        for i in range(61):
            row = self.ledger.enqueue_notification(
                kind="result_ready", target_session="ses_c", dedupe_key=f"paused:{i}",
                payload={"classification": "RESULT_READY"},
                task_id=paused["id"], project="paused", priority=100)
            pending.append(row["id"])
        active = self.ledger.create_task(project="ready", title="ready")
        # 路由/优先级夹具也需有效执行归属，不能靠绕过缺attempt规则保持可发送。
        ready_attempt = self.ledger.create_attempt(
            active["id"], directory=self.site + "-ready", session_id="ses_ready_worker",
            write_scope=[self.site + "-ready"])
        ready = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c", dedupe_key="ready",
            payload={"classification": "RESULT_READY"},
            task_id=active["id"], attempt_id=ready_attempt["id"], project="ready", priority=1)
        sup.dispatch_notifications()
        self.assertEqual(self.ledger.get_notification(ready["id"])["state"], "sent")
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertEqual(self.client.prompt_calls[0]["metadata"]["oc_orch_notifications"], [ready["id"]])
        for nid in pending:
            row = self.ledger.get_notification(nid)
            self.assertEqual(row["state"], "pending")
            self.assertEqual(row["attempts"], 0)

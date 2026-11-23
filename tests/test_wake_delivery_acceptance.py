"""协调者独立回归：规划 P01–P07 的正确行为，不连接真实服务。"""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from oc_orch.client import TransportError
from oc_orch.config import DEFAULTS
from oc_orch.domain.classify import classify_provider_error
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock


class WakeDeliveryAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="wake-acceptance-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_coord", outcome=None)
        self.sup = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                              client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger

    def enqueue(self, key, *, kind="stall", task=None, attempt=None):
        return self.ledger.enqueue_notification(
            kind=kind, target_session="ses_coord", dedupe_key=key,
            payload={"summary": key}, project="acceptance",
            task_id=task["id"] if task else None,
            attempt_id=attempt["id"] if attempt else None,
        )

    def old_attempt(self):
        task = self.ledger.create_task(project="acceptance", title="generation")
        old = self.ledger.create_attempt(
            task["id"], directory=str(self.home / "old"), session_id="ses_old")
        self.ledger.release_site(old["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        return task, old

    def replace(self, task):
        return self.ledger.create_attempt(
            task["id"], directory=str(self.home / "new"), session_id="ses_new")

    def test_p01_superseded_status_never_wakes_or_acks(self):
        task, old = self.old_attempt()
        new = self.replace(task)
        rows = [self.enqueue(k, kind=k, task=task, attempt=old)
                for k in ("stall", "idle_unconfirmed", "provider_retry")]
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        for n in rows:
            final = self.ledger.get_notification(n["id"])
            self.assertEqual(final["state"], "dead")
            self.assertEqual(final["attempts"], 0)
            self.assertIsNone(final["acked_at"])
        self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"], new["id"])

    def test_p02_subscription_limit_is_not_unknown_or_short_429(self):
        for status in (None, 429):
            with self.subTest(status=status):
                error = {"code": "provider.chatgpt-limit",
                         "message": "Subscription Sharing usage limit exceeded"}
                if status:
                    error["status"] = status
                result = classify_provider_error(error)
                self.assertEqual(result["category"], "quota")
                self.assertFalse(result["transient"])

    def test_p03_unknown_observation_does_not_append_input(self):
        n = self.enqueue("observation-unknown")
        self.client.set_inbox("ses_coord", [{"id": "msg_already_queued"}])
        self.client.fail("active_sessions", TransportError("/active", "probe"))
        self.client.fail("inbox", TransportError("/inbox", "probe"))
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")
        self.assertEqual(len(self.client.inbox("ses_coord")), 1)

    def test_p04_foreign_pending_has_visible_bounded_responsibility(self):
        n = self.enqueue("new-result", kind="result_ready")
        self.client.set_inbox("ses_coord", [{"id": "msg_parked", "delivery": "queue"}])
        self.sup.dispatch_notifications()
        self.clock.advance(seconds=3601)
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [], "不能绕过陌生队头发新输入")
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")
        alerts = self.ledger.list_alerts(state="new", limit=100)
        blocked = [a for a in alerts if "inbox" in a["kind"]
                   or "foreign_input" in a["kind"]]
        self.assertTrue(blocked, "长期抑制必须转为可见诊断责任")
        self.assertIn("ses_coord", str(blocked))

    def test_p05_one_admission_event_confirms_the_entire_batch_once(self):
        rows = [self.enqueue(f"batch-{i}") for i in range(3)]
        self.client.fail("prompt", TransportError("/prompt", "response lost"))
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        msg = self.client.prompt_calls[0]["message_id"]
        self.client.set_inbox("ses_coord", [{"id": msg, "delivery": "queue"}])
        event = {"id": "evt_acceptance_admitted", "type": "session.inbox.enqueued",
                 "created": self.clock.now_ms(),
                 "data": {"sessionID": "ses_coord", "inboxID": msg}}
        self.sup.ingest_event(event)
        confirmed = [self.ledger.get_notification(n["id"]) for n in rows]
        self.assertEqual([r["state"] for r in confirmed], ["sent"] * 3)
        self.assertEqual([r["attempts"] for r in confirmed], [1] * 3)
        self.clock.advance(seconds=1)
        self.sup.ingest_event({**event, "id": "evt_acceptance_duplicate_fact"})
        self.assertEqual([self.ledger.get_notification(n["id"]) for n in rows], confirmed)

    def test_p06_claim_filtered_member_is_absent_from_body_and_metadata(self):
        task, old = self.old_attempt()
        stale = self.enqueue("old-result", kind="result_ready", task=task, attempt=old)
        other = self.enqueue("current-action", kind="result_ready")
        claim = self.ledger.claim_notifications

        def interleave(ids, owner):
            self.replace(task)
            return claim(ids, owner)

        with patch.object(self.ledger, "claim_notifications", side_effect=interleave):
            self.sup.dispatch_notifications()
        self.assertEqual(self.ledger.get_notification(stale["id"])["state"], "dead")
        self.assertEqual(len(self.client.prompt_calls), 1)
        call = self.client.prompt_calls[0]
        self.assertNotIn(stale["id"], call["text"])
        self.assertNotIn(stale["id"], call["metadata"]["oc_orch_notifications"])
        self.assertIn(other["id"], call["metadata"]["oc_orch_notifications"])

    def test_p07_unknown_response_cannot_create_a_new_message_identity(self):
        self.enqueue("unknown-response")
        self.client.fail("prompt", TransportError("/prompt", "response lost"))
        self.sup.dispatch_notifications()
        first = self.client.prompt_calls[0]["message_id"]
        self.clock.advance(seconds=31)
        self.sup.dispatch_notifications()
        self.assertEqual({c["message_id"] for c in self.client.prompt_calls}, {first})


if __name__ == "__main__":
    unittest.main()

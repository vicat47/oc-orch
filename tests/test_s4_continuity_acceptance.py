"""父协调者独立验收：ACK、转交与通知发布不能留下未完成责任空档。"""
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.config import DEFAULTS
from oc_orch.db import init
from oc_orch.domain.errors import LedgerError
from oc_orch.ledger import Ledger
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock


class ContinuityAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="s4-independent-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_coord")
        self.sup = Supervisor(self.home, cfg=dict(DEFAULTS), client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        self.store = ResponsibilityStore(self.ledger)
        self.task = self.ledger.create_task(
            project="s4-independent", title="CONTROL carry",
            coordinator_session="ses_coord")
        self.attempt = self.ledger.create_attempt(
            self.task["id"], directory=self.home, session_id="ses_worker",
            status="running", dispatch_state="confirmed")

    def responsibility(self, key="input-one", *, review=False, **changes):
        args = dict(owner="coordinator", project=self.task["project"],
                    task_id=self.task["id"], attempt_id=self.attempt["id"],
                    generation=self.attempt["generation"], input_id="I-acceptance",
                    session_id="ses_worker", message_id="msg_input",
                    target_session="ses_coord", state="waiting_input",
                    wait_kind="execution", wait_condition="input-one-ended",
                    next_action="核收 worker 回报；缺材料则提供基线引用",
                    next_check_at=self.clock.now_ms(),
                    deadline=self.clock.now_ms() + 60000, condition_key=key,
                    evidence={"material_ref": "baseline-current"}, max_budget=3)
        args.update(changes)
        if review:
            args["state"] = "awaiting_review"
            args["evidence"] = {**args["evidence"], "execution": {
                "input_at": self.clock.now_ms() - 20,
                "ended_at": self.clock.now_ms() - 1,
                "idle_id": "msg_scoped_idle", "idle_outcome": "succeeded",
                "coverage": "complete"}}
        return self.store.open_responsibility(**args)

    def test_ack_old_result_keeps_worker_followup_after_reopen(self):
        resp = self.responsibility()
        old = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord",
            dedupe_key="prior-result", payload={"summary": "旧结果"},
            project=self.task["project"], task_id=self.task["id"],
            attempt_id=self.attempt["id"])
        self.ledger.ack_notifications([old["id"]], actor="test")
        conn = init(self.home)
        self.addCleanup(conn.close)
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        reopened = ResponsibilityStore(Ledger(conn, self.home, clock=self.clock))
        rows = reopened.list_open_responsibilities(task_id=self.task["id"])
        self.assertIn(resp["id"], [r["id"] for r in rows])
        self.assertEqual(reopened.get_responsibility(resp["id"])["target_session"], "ses_coord")
        self.assertEqual(self.ledger.get_notification(old["id"])["state"], "acked")

    def test_notify_insert_failure_cannot_consume_waiting_responsibility(self):
        resp = self.responsibility(review=True)
        self.sup.conn.execute(
            "CREATE TRIGGER fail_continuation BEFORE INSERT ON notifications "
            "BEGIN SELECT RAISE(ABORT, 'simulated-crash'); END")
        with self.assertRaises(sqlite3.DatabaseError):
            self.store.publish_responsibility(
                resp["id"], expected_revision=resp["revision"],
                kind="continuation_ready", payload={"summary": "待协调者核收"})
        final = self.store.get_responsibility(resp["id"])
        self.assertEqual(final["revision"], resp["revision"])
        self.assertEqual(final["state"], resp["state"])
        self.assertFalse(self.ledger.list_notifications(target_session="ses_coord"))

    def test_two_observers_cannot_transfer_the_same_revision_twice(self):
        resp = self.responsibility()
        conn = init(self.home)
        self.addCleanup(conn.close)
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        other = ResponsibilityStore(Ledger(conn, self.home, clock=self.clock))
        successor = {"owner": "reviewer", "target_session": "ses_coord",
                     "state": "awaiting_review", "wait_kind": "review",
                     "wait_condition": "worker-reply",
                     "next_action": "核对材料",
                     "next_check_at": self.clock.now_ms(),
                     "deadline": resp["deadline"], "condition_key": "review-one"}
        self.store.transfer_responsibility(
            resp["id"], expected_revision=resp["revision"],
            successor=successor, reason="worker replied")
        with self.assertRaises(LedgerError):
            other.transfer_responsibility(
                resp["id"], expected_revision=resp["revision"],
                successor={**successor, "condition_key": "duplicate-review"},
                reason="late observer")
        rows = self.store.list_open_responsibilities(task_id=self.task["id"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["owner"], "reviewer")

    def test_result_ack_and_succeeded_are_not_business_completion(self):
        resp = self.responsibility()
        for reason in ("notification-acked", "execution-succeeded"):
            with self.subTest(reason=reason), self.assertRaises(LedgerError):
                self.store.close_responsibility(
                    resp["id"], expected_revision=resp["revision"], reason=reason)
        self.assertIn(resp["id"], [r["id"] for r in
                      self.store.list_open_responsibilities(task_id=self.task["id"])])

    def test_new_continuation_survives_old_notification_ack(self):
        resp = self.responsibility(review=True)
        old = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord",
            dedupe_key="old-notification", payload={"summary": "old"},
            task_id=self.task["id"], attempt_id=self.attempt["id"])
        self.ledger.ack_notifications([old["id"]], actor="test")
        self.store.publish_responsibility(
            resp["id"], expected_revision=resp["revision"],
            kind="continuation_ready",
            payload={"summary": "worker 缺材料，需协调者决策"})
        pending = self.ledger.list_notifications(target_session="ses_coord", state="pending")
        self.assertEqual(len(pending), 1)
        self.assertNotEqual(pending[0]["id"], old["id"])
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertIn("worker 缺材料", self.client.prompt_calls[0]["text"])
        self.assertEqual(self.ledger.get_notification(old["id"])["state"], "acked")

    def test_manual_gate_blocks_continuation_without_losing_it(self):
        resp = self.responsibility(review=True)
        self.store.publish_responsibility(
            resp["id"], expected_revision=resp["revision"],
            kind="continuation_ready", payload={"summary": "review"})
        self.sup.target_gate.block_manual("ses_coord", reason="operator stop")
        self.sup.dispatch_notifications()
        self.assertFalse(self.client.prompt_calls)
        rows = self.ledger.list_notifications(target_session="ses_coord", state="pending")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["attempts"], 0)
        self.assertTrue(self.store.list_open_responsibilities(task_id=self.task["id"]))

    def test_budget_and_deadline_survive_a_new_connection(self):
        resp = self.responsibility()
        updated = self.store.update_responsibility(
            resp["id"], expected_revision=resp["revision"], fields={"budget_used": 2})
        conn = init(self.home)
        self.addCleanup(conn.close)
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        reopened = ResponsibilityStore(Ledger(conn, self.home, clock=self.clock))
        final = reopened.get_responsibility(resp["id"])
        self.assertEqual(final["budget_used"], 2)
        self.assertEqual(final["max_budget"], 3)
        self.assertEqual(final["deadline"], resp["deadline"])
        self.assertEqual(final["revision"], updated["revision"])

    def test_late_observation_after_transfer_cannot_close_successor(self):
        resp = self.responsibility()
        moved = self.store.transfer_responsibility(
            resp["id"], expected_revision=resp["revision"],
            successor={"owner": "new-coordinator",
                       "target_session": "ses_new_coord",
                       "state": "awaiting_review",
                       "wait_kind": "review",
                       "wait_condition": "check-result",
                       "next_action": "核对新材料",
                       "next_check_at": self.clock.now_ms(),
                       "deadline": resp["deadline"],
                       "condition_key": "new-review"}, reason="explicit handoff")
        with self.assertRaises(LedgerError):
            self.store.update_responsibility(
                resp["id"], expected_revision=resp["revision"],
                fields={"state": "done"})
        successor = self.store.get_responsibility(moved["successor"]["id"])
        self.assertEqual(successor["state"], "awaiting_review")
        self.assertEqual(successor["target_session"], "ses_new_coord")

    def test_repeated_same_fact_cannot_publish_another_notification_after_ack(self):
        resp = self.responsibility(review=True)
        self.store.publish_responsibility(
            resp["id"], expected_revision=resp["revision"],
            payload={"summary": "same ended input"})
        rows = self.ledger.list_notifications(target_session="ses_coord")
        self.assertEqual(len(rows), 1)
        self.ledger.ack_notifications([rows[0]["id"]], actor="test")
        fresh = self.store.get_responsibility(resp["id"])
        self.store.publish_responsibility(
            resp["id"], expected_revision=fresh["revision"],
            payload={"summary": "same ended input"})
        after = self.ledger.list_notifications(target_session="ses_coord")
        self.assertEqual(len(after), 1)
        self.assertEqual(after[0]["id"], rows[0]["id"])
        self.assertEqual(after[0]["state"], "acked")

    def test_result_generation_replacement_cannot_wake_old_continuation(self):
        resp = self.responsibility(review=True)
        self.store.publish_responsibility(
            resp["id"], expected_revision=resp["revision"],
            payload={"summary": "old-generation-result"})
        self.ledger.release_site(self.attempt["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        replacement = self.ledger.create_attempt(
            self.task["id"], directory=self.home, session_id="ses_worker_new")
        self.sup.dispatch_notifications()
        self.assertFalse(self.client.prompt_calls)
        self.assertEqual(self.ledger.get_task(self.task["id"])["current_attempt_id"],
                         replacement["id"])
        rows = self.ledger.list_notifications(target_session="ses_coord")
        self.assertTrue(rows)
        self.assertTrue(all(n["state"] != "sent" for n in rows))

    def test_reason_text_cannot_discard_unfinished_task_responsibility(self):
        resp = self.responsibility()
        for reason in ("manual-close", "duplicate", "transferred",
                       "superseded:operator-says-so"):
            with self.subTest(reason=reason), self.assertRaises(LedgerError):
                self.store.close_responsibility(
                    resp["id"], expected_revision=resp["revision"], reason=reason)
        self.assertIsNone(self.store.get_responsibility(resp["id"])["closed_at"])

    def test_automatic_update_cannot_release_manual_wait(self):
        resp = self.responsibility(state="waiting_human", wait_kind="human",
                                   wait_condition="operator-decision")
        with self.assertRaises(LedgerError):
            self.store.update_responsibility(
                resp["id"], expected_revision=resp["revision"],
                fields={"state": "waiting_execution"}, actor="supervisor")
        self.assertEqual(self.store.get_responsibility(resp["id"])["state"],
                         "waiting_human")

    def test_open_cannot_claim_another_tasks_attempt(self):
        other = self.ledger.create_task(project=self.task["project"], title="unrelated")
        with self.assertRaises(LedgerError):
            self.responsibility(task_id=other["id"])
        self.assertEqual(self.store.list_open_responsibilities(task_id=other["id"]), [])

    def test_transfer_cannot_reset_budget_or_jump_to_another_task(self):
        resp = self.responsibility()
        resp = self.store.update_responsibility(
            resp["id"], expected_revision=resp["revision"],
            fields={"budget_used": 2})
        other = self.ledger.create_task(project=self.task["project"], title="other")
        base = dict(owner="new-owner", state="awaiting_review", wait_kind="review",
                    wait_condition="check", next_action="review facts",
                    next_check_at=self.clock.now_ms(), deadline=resp["deadline"],
                    condition_key="handoff")
        for change in ({"budget_used": 0}, {"task_id": other["id"]},
                       {"max_budget": 999}):
            with self.subTest(change=change), self.assertRaises(LedgerError):
                self.store.transfer_responsibility(
                    resp["id"], expected_revision=resp["revision"],
                    successor={**base, **change}, reason="handoff")
        self.assertEqual(self.store.get_responsibility(resp["id"])["budget_used"], 2)


if __name__ == "__main__":
    unittest.main()

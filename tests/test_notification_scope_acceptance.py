"""台账不得绕过纯规则：部分缺归属通知补证，只有真正无锚通知兼容。"""
import tempfile
import unittest

from oc_orch.db import init
from oc_orch.ledger import Ledger, LedgerError
from tests.fakes import FakeClock


class NotificationScopeAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="notification-scope-", dir="/tmp/opencode")
        self.addCleanup(self.temp.cleanup)
        self.clock = FakeClock()
        self.conn = init(self.temp.name)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.temp.name, clock=self.clock)

    def prepare_current(self, *, kind="result_ready", status="running"):
        task = self.ledger.create_task(project="scope", title="current execution")
        attempt = self.ledger.create_attempt(
            task["id"], directory=self.temp.name + "/old", session_id="ses_old",
            status=status, dispatch_state="confirmed")
        notice = self.ledger.enqueue_notification(
            kind=kind, target_session="ses_coord", task_id=task["id"],
            attempt_id=attempt["id"], dedupe_key="current-notice", payload={"summary": "current"})
        receipt = self.ledger.reserve_target_input(
            "ses_coord", notification_ids=[notice["id"]], intent="execute",
            owner="sender", source="automatic_wake", now=self.clock.now_ms())["input"]
        claimed = self.ledger.claim_notifications([notice["id"]], "sender")
        self.assertEqual([n["id"] for n in claimed], [notice["id"]])
        return task, attempt, notice, receipt

    def finalize(self, notice, receipt):
        return self.ledger.finalize_input(
            receipt["input_id"], final_notification_ids=[notice["id"]],
            text_factory=lambda rows: "frozen-current", owner="sender",
            expected_revision=receipt["revision"], now=self.clock.now_ms())["input"]

    def replace_current(self, task):
        return self.ledger.create_attempt(
            task["id"], directory=self.temp.name + "/new", session_id="ses_new")

    def test_task_without_attempt_stays_diagnostic_through_finalization(self):
        task = self.ledger.create_task(project="scope", title="missing execution")
        notice = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord", task_id=task["id"],
            dedupe_key="missing-attempt", payload={"result_version": "rv1-scope"})
        self.assertEqual(self.ledger.due_notifications(now=self.clock.now_ms()), [])
        self.assertEqual(self.ledger.claim_notifications([notice["id"]], "sender"), [])
        row = self.ledger.get_notification(notice["id"])
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["attempts"], 0)
        self.assertIsNone(row["acked_at"])
        self.assertIn("attempt-unresolved", row["last_error"])
        reserved = self.ledger.reserve_target_input(
            "ses_coord", notification_ids=[notice["id"]], intent="execute",
            owner="sender", source="automatic_wake", now=self.clock.now_ms())
        receipt = reserved["input"]
        with self.assertRaises(LedgerError) as error:
            self.ledger.finalize_input(
                receipt["input_id"], final_notification_ids=[notice["id"]],
                text_factory=lambda rows: "must not send", owner="sender",
                expected_revision=receipt["revision"], now=self.clock.now_ms())
        self.assertEqual(error.exception.code, "MEMBERS_NOT_SENDABLE")

    def test_truly_unscoped_legacy_notice_is_still_sendable(self):
        notice = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord", dedupe_key="legacy-global",
            payload={"summary": "explicit legacy notification"})
        due = self.ledger.due_notifications(now=self.clock.now_ms())
        self.assertEqual([n["id"] for n in due], [notice["id"]])

    def test_current_changes_after_claim_block_finalization(self):
        task, old, notice, receipt = self.prepare_current()
        new = self.replace_current(task)
        with self.assertRaises(LedgerError) as error:
            self.finalize(notice, receipt)
        self.assertEqual(error.exception.code, "MEMBERS_NOT_SENDABLE")
        self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"], new["id"])
        self.assertIsNone(self.ledger.get_attempt(old["id"])["site_released_at"])

    def test_current_changes_after_finalize_block_possible(self):
        task, old, notice, receipt = self.prepare_current()
        frozen = self.finalize(notice, receipt)
        self.replace_current(task)
        result = self.ledger.mark_input_possible(
            receipt["input_id"], owner="sender", expected_revision=frozen["revision"],
            now=self.clock.now_ms())
        self.assertFalse(result["changed"])
        self.assertEqual(self.ledger.get_input(receipt["input_id"])["send_count"], 0)
        self.assertIsNone(self.ledger.get_attempt(old["id"])["site_released_at"])

    def test_resolved_condition_after_finalize_block_possible(self):
        task, old, notice, receipt = self.prepare_current(kind="stall", status="stalled")
        frozen = self.finalize(notice, receipt)
        self.ledger.transition_attempt(old["id"], "running", actor="observer", reason="progress")
        result = self.ledger.mark_input_possible(
            receipt["input_id"], owner="sender", expected_revision=frozen["revision"],
            now=self.clock.now_ms())
        self.assertFalse(result["changed"])
        self.assertEqual(self.ledger.get_input(receipt["input_id"])["send_count"], 0)


if __name__ == "__main__":
    unittest.main()

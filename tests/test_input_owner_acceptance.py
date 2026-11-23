"""独立发送权边界：新输入/后继必须有明确owner，NULL不授予任意caller。"""
import unittest

from oc_orch.ledger import LedgerError
from tests import test_notification_scope_acceptance as fixtures


class InputOwnerAcceptanceTests(unittest.TestCase):
    setUp = fixtures.NotificationScopeAcceptanceTests.setUp

    def reserve(self, session="ses_owner", owner="sender"):
        notice = self.ledger.enqueue_notification(
            kind="result_ready", target_session=session, dedupe_key=session,
            payload={"summary": "owner contract"})
        return notice, self.ledger.reserve_target_input(
            session, notification_ids=[notice["id"]], intent="execute", owner=owner,
            source="automatic_wake", now=self.clock.now_ms())["input"]

    def delivered(self, session):
        notice, receipt = self.reserve(session)
        frozen = self.ledger.finalize_input(
            receipt["input_id"], final_notification_ids=[notice["id"]],
            text_factory=lambda rows: "frozen", owner="sender",
            expected_revision=receipt["revision"], now=self.clock.now_ms())["input"]
        self.ledger.mark_input_possible(
            receipt["input_id"], owner="sender", expected_revision=frozen["revision"],
            now=self.clock.now_ms())
        self.ledger.confirm_input_admitted(receipt["input_id"], evidence={"source": "test"},
                                           now=self.clock.now_ms(), actor="observer")
        self.ledger.confirm_input_delivered(receipt["input_id"], evidence={"source": "test"},
                                            now=self.clock.now_ms(), actor="observer")
        return receipt

    def test_reserve_rejects_missing_or_invalid_owner_without_creating_input(self):
        for i, owner in enumerate((None, "", "   ", 123, [], {})):
            with self.subTest(owner=owner):
                session = f"ses_bad_owner_{i}"
                with self.assertRaises(LedgerError):
                    self.reserve(session, owner)
                self.assertEqual(self.ledger.list_pending_inputs(target_session=session), [])

    def test_nudge_cannot_create_ownerless_successor(self):
        for i, owner in enumerate((None, "", "   ", 123)):
            with self.subTest(owner=owner):
                session = f"ses_nudge_owner_{i}"
                receipt = self.delivered(session)
                before = self.ledger.get_input(receipt["input_id"])
                with self.assertRaises(LedgerError):
                    self.ledger.nudge_input(
                        receipt["input_id"], text_factory=lambda rows: "nudge", owner=owner,
                        now=self.clock.now_ms())
                self.assertEqual(self.ledger.get_input(receipt["input_id"]), before)
                self.assertEqual(len(self.ledger.list_pending_inputs(target_session=session)), 1)

    def test_missing_persisted_owner_does_not_authorize_arbitrary_finalizer(self):
        notice, receipt = self.reserve()
        # 显式历史坏形状夹具；不能由“claim_owner为空”推断任何caller都有写权。
        self.conn.execute("UPDATE session_inputs SET claim_owner=NULL WHERE input_id=?",
                          (receipt["input_id"],))
        with self.assertRaises(LedgerError):
            self.ledger.finalize_input(
                receipt["input_id"], final_notification_ids=[notice["id"]],
                text_factory=lambda rows: "must not send", owner="someone-else",
                expected_revision=receipt["revision"], now=self.clock.now_ms())

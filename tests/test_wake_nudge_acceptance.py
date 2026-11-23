"""独立活性验收：仅已投递输入可有限催办，等待不得消耗预算。"""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from oc_orch.composition import make_target_gate
from oc_orch.config import DEFAULTS
from oc_orch.supervisor import Supervisor
from tests import test_wake_delivery_acceptance as fixtures


class WakeNudgeAcceptanceTests(unittest.TestCase):
    setUp = fixtures.WakeDeliveryAcceptanceTests.setUp
    enqueue = fixtures.WakeDeliveryAcceptanceTests.enqueue

    def deliver_latest(self):
        self.clock.advance(seconds=1)
        message_id = self.client.prompt_calls[-1]["message_id"]
        self.client.deliver_inbox("ses_coord", at=self.clock.now_ms())
        self.sup.ingest_event({
            "id": "evt_nudge_delivered_" + message_id,
            "type": "session.inbox.delivered", "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "inboxID": message_id},
        })

    def test_delivered_unacked_has_finite_nudges_with_same_business_identity(self):
        notice = self.enqueue("finite-nudge", kind="result_ready")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        maximum = int(self.sup.cfg["max_wake_attempts"])
        for expected in range(2, maximum + 1):
            self.deliver_latest()
            self.clock.advance(seconds=int(self.sup.cfg["ack_timeout"]) + 1)
            self.sup.dispatch_notifications()
            self.assertEqual(len(self.client.prompt_calls), expected,
                             "已投递但未ACK的空闲目标需要有界催办，不能永久持槽")
            call = self.client.prompt_calls[-1]
            self.assertEqual(call["metadata"]["oc_orch_notifications"], [notice["id"]])
        self.deliver_latest()
        self.clock.advance(seconds=int(self.sup.cfg["ack_timeout"]) + 1)
        self.sup.expire_notifications()
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), maximum)
        self.assertEqual(len({c["message_id"] for c in self.client.prompt_calls}), maximum)
        self.assertIsNone(self.ledger.get_notification(notice["id"])["acked_at"])
        self.assertTrue(self.ledger.list_alerts(state="new"), "预算耗尽必须有可见责任")

    def test_pending_input_never_gets_a_nudge(self):
        self.enqueue("still-inbox", kind="result_ready")
        self.sup.dispatch_notifications()
        self.clock.advance(seconds=int(self.sup.cfg["ack_timeout"]) + 1)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        rows = self.ledger.list_pending_inputs(target_session="ses_coord")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["nudge_count"], 0)

    def test_cooling_target_does_not_spend_nudge_or_create_successor(self):
        self.enqueue("quota-wait", kind="result_ready")
        self.sup.dispatch_notifications()
        self.deliver_latest()
        gate = make_target_gate(self.sup.conn, self.clock)
        gate.observe_error("ses_coord", {"category": "quota", "transient": False,
            "message": "limit", "retry_after_ms": 7200000}, evidence_key="quota-origin",
            observed_at=self.clock.now_ms(), origin_at=self.clock.now_ms())
        self.clock.advance(seconds=int(self.sup.cfg["ack_timeout"]) + 1)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        rows = self.ledger.list_pending_inputs(target_session="ses_coord")
        self.assertEqual(len(rows), 1, "冷却等待不能先创建nudge并烧掉跨代预算")
        self.assertEqual(rows[0]["nudge_count"], 0)

    def test_legacy_unknown_receipt_is_not_replayed_under_new_identity(self):
        fixture = Path(__file__).resolve().parent / "fixtures" / "schema-v1.sql"
        with tempfile.TemporaryDirectory(prefix="legacy-wake-", dir="/tmp/opencode") as home:
            conn = sqlite3.connect(str(Path(home) / "ledger.db"))
            conn.executescript(fixture.read_text())
            conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version','1')")
            conn.execute("INSERT INTO notifications(id,kind,target_session,dedupe_key,payload,"
                "state,attempts,next_attempt_at,message_id,created_at,sent_at,max_attempts) "
                "VALUES(?,?,?,?,?,'sent',1,?,?,?,?,3)",
                ("N-legacy-unknown", "result_ready", "ses_coord", "legacy-unknown",
                 json.dumps({"summary": "legacy"}), 1001000, "msg_legacy_unknown", 1000000, 1000000))
            conn.commit()
            conn.close()
            self.clock.advance(seconds=1001)
            sup = Supervisor(home, cfg=dict(DEFAULTS), client=self.client, clock=self.clock)
            try:
                sup.dispatch_notifications()
                sup.dispatch_notifications()
                self.assertEqual(self.client.prompt_calls, [],
                    "历史回执仍unknown且原正文未知，不能升级后直接换新ID重播")
                self.assertIsNone(sup.ledger.get_notification("N-legacy-unknown")["acked_at"])
                self.assertIsNotNone(sup.ledger.input_for_receipt("ses_coord", "msg_legacy_unknown"))
            finally:
                sup.conn.close()

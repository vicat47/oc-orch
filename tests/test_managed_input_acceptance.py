"""协调者独立S4验收：仅临时账本/FakeClient，不读取或调用真实服务。"""
import tempfile
import unittest
from unittest.mock import patch

from oc_orch.adapters.managed_input_ledger import ManagedInputLedger
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.target_gate import TargetGate
from oc_orch.application.managed_inputs import ManagedInputs
from oc_orch.db import init
from oc_orch.domain.errors import ActionError, LedgerError
from tests.fakes import FakeClient, FakeClock


def make_managed_inputs(conn, home, *, client, clock):
    base = Ledger(conn, home, clock=clock)
    return ManagedInputs(ManagedInputLedger(conn, base), client=client,
                         clock=clock, target_gate=TargetGate(conn, clock))


class ManagedInputAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="managed-input-acceptance-", dir="/tmp/opencode")
        self.addCleanup(self.temp.cleanup)
        self.conn = init(self.temp.name)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_allowed", outcome="succeeded")
        self.service = make_managed_inputs(self.conn, self.temp.name,
                                           client=lambda: self.client, clock=self.clock)

    def register(self, sid="ses_allowed"):
        return self.service.register_watch(sid, owner="operator", project="offline",
                                           max_pending_age_ms=1000)

    def deliver(self, sid="ses_allowed"):
        self.client.deliver_inbox(sid, at=self.clock.now_ms())

    def send(self, *, intent="park", request_id="request-1", **extra):
        fields = {"expires_at": self.clock.now_ms() + 1000,
                  "flush_condition": "explicit-human-confirmation"} if intent == "park" else {}
        fields.update(extra)
        return self.service.send("ses_allowed", intent=intent, text="offline test",
            owner="operator", request_id=request_id, project="offline", **fields)["input"]

    def test_record_never_constructs_client_or_calls_model(self):
        def forbidden():
            self.fail("record留档不得构造Client")
        service = make_managed_inputs(self.conn, self.temp.name, client=forbidden, clock=self.clock)
        result = service.send("ses_offline_record", intent="record", text="local record",
                              owner="operator", request_id="record-1")
        row = service.get(result["input"]["input_id"])["input"]
        self.assertEqual(row["intent"], "record")
        self.assertNotIn(row["execution"], ("succeeded", "executed"))
        self.assertEqual(self.client.prompt_calls, [])

    def test_missing_park_responsibility_is_rejected_before_io(self):
        self.register()
        with patch.object(self.client, "prompt") as prompt, patch.object(self.client, "inbox") as inbox:
            with self.assertRaises((ActionError, LedgerError)):
                self.service.send("ses_allowed", intent="park", text="x", owner="operator",
                                  request_id="bad-park")
        prompt.assert_not_called()
        inbox.assert_not_called()

    def test_execute_and_park_preserve_explicit_resume(self):
        self.register()
        executed = self.send(intent="execute", request_id="execute-1")
        self.assertIs(self.client.prompt_calls[-1]["resume"], True)
        self.assertEqual(executed["message_id"], self.client.prompt_calls[-1]["message_id"])
        parked = self.send(intent="park", request_id="park-1")
        self.assertIs(self.client.prompt_calls[-1]["resume"], False)
        self.assertEqual(parked["message_id"], self.client.prompt_calls[-1]["message_id"])

    def test_same_request_does_not_repeat_prompt_or_change_identity(self):
        self.register()
        first = self.send()
        second = self.send()
        self.assertEqual(first["input_id"], second["input_id"])
        self.assertEqual(first["message_id"], second["message_id"])
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_park_expiry_alerts_without_flush_cancel_or_new_prompt(self):
        self.register()
        row = self.send()
        self.clock.advance(seconds=2)
        with patch.object(self.client, "update_inbox_delivery") as steer, \
                patch.object(self.client, "cancel_inbox") as cancel:
            self.service.observe_registered()
            self.service.observe_registered()
        steer.assert_not_called()
        cancel.assert_not_called()
        self.assertEqual(len(self.client.prompt_calls), 1)
        after = self.service.get(row["input_id"])["input"]
        self.assertNotEqual(after["delivery"], "cancelled")
        alerts = self.conn.execute("SELECT detail FROM alerts WHERE state='new'").fetchall()
        self.assertTrue(any(row["input_id"] in a["detail"] and "operator" in a["detail"]
                            for a in alerts), "到期必须形成owner可见责任")

    def test_empty_whitelist_observation_never_constructs_client(self):
        def forbidden():
            self.fail("空白名单不得构造Client或扫描全局session")
        service = make_managed_inputs(self.conn, self.temp.name, client=forbidden, clock=self.clock)
        service.observe_registered()

    def test_whitelist_observation_never_queries_unregistered_session(self):
        self.register()
        self.client.set_session("ses_unregistered")
        queried = []
        original = self.client.inbox

        def check(sid):
            queried.append(sid)
            self.assertEqual(sid, "ses_allowed")
            return original(sid)

        with patch.object(self.client, "inbox", side_effect=check), \
                patch.object(self.client, "list_sessions", side_effect=AssertionError("禁止全局扫描")), \
                patch.object(self.client, "active_sessions", side_effect=AssertionError("禁止全局active读取")):
            self.service.observe_registered()
        self.assertTrue(queried)

    def test_flush_requires_confirmation_and_rejects_foreign_queue_head(self):
        self.register()
        row = self.send()
        self.client.set_inbox("ses_allowed", [{"id": "msg_foreign", "delivery": "queue"},
            {"id": row["message_id"], "delivery": "queue"}])
        with patch.object(self.client, "update_inbox_delivery") as steer:
            with self.assertRaises((ActionError, LedgerError)):
                self.service.flush(row["input_id"], owner="operator", confirm="wrong",
                    expected_revision=row["revision"], reason="manual")
            fresh = self.service.get(row["input_id"])["input"]
            with self.assertRaises((ActionError, LedgerError)):
                self.service.flush(row["input_id"], owner="operator", confirm=row["input_id"],
                    expected_revision=fresh["revision"], reason="manual")
        steer.assert_not_called()
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_old_session_success_does_not_prove_new_input_execution(self):
        self.register()
        row = self.send()
        self.clock.advance(seconds=1)
        self.deliver("ses_allowed")
        viewed = self.service.reconcile(row["input_id"])["input"]
        self.assertEqual(viewed["delivery"], "delivered")
        self.assertNotIn(viewed["execution"], ("succeeded", "executed"))
        self.assertNotEqual(viewed["business_ack"], "acknowledged")

    def test_delete_noop_with_delivered_message_is_not_cancelled(self):
        self.register()
        row = self.send()
        self.clock.advance(seconds=1)
        self.deliver("ses_allowed")
        current = self.service.get(row["input_id"])["input"]
        viewed = self.service.cancel(row["input_id"], owner="operator", confirm=row["input_id"],
            expected_revision=current["revision"], reason="explicit cancellation check")["input"]
        self.assertEqual(viewed["delivery"], "delivered")
        self.assertNotEqual(viewed["business_ack"], "acknowledged")

    def test_local_expiry_still_works_without_remote_access(self):
        self.register()
        self.send()
        self.clock.advance(seconds=2)
        def forbidden():
            self.fail("本地过期检查不得读取服务")
        local = make_managed_inputs(self.conn, self.temp.name, client=forbidden, clock=self.clock)
        local.observe_registered(allow_remote=False)
        self.assertGreater(self.conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0], 0)

    def test_expiry_condition_does_not_realert_after_alert_ack(self):
        self.register()
        row = self.send()
        self.clock.advance(seconds=2)
        self.service.observe_registered(allow_remote=False)
        matching = self.conn.execute("SELECT id FROM alerts WHERE detail LIKE ?",
                                     ("%" + row["input_id"] + "%",)).fetchall()
        self.assertTrue(matching)
        from oc_orch.composition import Ledger
        ledger = Ledger(self.conn, self.temp.name, clock=self.clock)
        for alert in matching:
            ledger.ack_alert(alert["id"], actor="operator")
        self.service.observe_registered(allow_remote=False)
        after = self.conn.execute("SELECT id FROM alerts WHERE detail LIKE ?",
                                  ("%" + row["input_id"] + "%",)).fetchall()
        self.assertEqual(len(after), len(matching),
                         "同一park到期条件不得因告警ACK后每周期重新制造责任")

    def test_local_lists_do_not_expose_frozen_input_body(self):
        self.register()
        self.send()
        def forbidden():
            self.fail("本地列表不得构造Client")
        local = make_managed_inputs(self.conn, self.temp.name, client=forbidden, clock=self.clock)
        result = local.list_inputs(session_id="ses_allowed")
        self.assertNotIn("offline test", repr(result))
        self.assertEqual(len(result["inputs"]), 1)

    def test_target_resume_is_explicit_and_never_prompts(self):
        self.register()
        gate = TargetGate(self.conn, self.clock)
        gate.block_manual("ses_allowed", reason="operator stopped")
        state = gate.get("ses_allowed")
        with self.assertRaises((ActionError, LedgerError)):
            self.service.resume_target("ses_allowed", owner="operator", confirm="wrong",
                expected_revision=state["revision"], reason="manual resume")
        self.service.resume_target("ses_allowed", owner="operator", confirm="ses_allowed",
            expected_revision=state["revision"], reason="manual resume")
        self.assertEqual(self.client.prompt_calls, [])

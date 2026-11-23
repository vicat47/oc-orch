"""父级补充验收：持久观察查询、撤销晚到与告警条件去重（全离线）。"""
import tempfile
import unittest
from unittest.mock import patch

from oc_orch.adapters.managed_input_ledger import ManagedInputLedger
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.target_gate import TargetGate
from oc_orch.application.managed_inputs import ManagedInputs
from oc_orch.client import TransportError
from oc_orch.db import init
from oc_orch.domain.errors import ActionError, LedgerError
from tests.fakes import FakeClient, FakeClock


def make_managed_inputs(conn, home, *, client, clock):
    base = Ledger(conn, home, clock=clock)
    return ManagedInputs(ManagedInputLedger(conn, base), client=client,
                         clock=clock, target_gate=TargetGate(conn, clock))


class ManagedInputObservationAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="managed-observation-acceptance-", dir="/tmp/opencode")
        self.addCleanup(self.temp.cleanup)
        self.conn = init(self.temp.name)
        self.addCleanup(self.conn.close)
        self.clock, self.client = FakeClock(), FakeClient()
        self.base = Ledger(self.conn, self.temp.name, clock=self.clock)
        self.service = make_managed_inputs(self.conn, self.temp.name,
            client=lambda: self.client, clock=self.clock)

    def watch(self, sid):
        self.client.set_session(sid, outcome=None)
        return self.service.register_watch(sid, owner="operator", project="offline",
            max_pending_age_ms=1000)["watch"]

    def overdue(self, sid="ses_aged"):
        watch = self.watch(sid)
        row = self.service.send(sid, intent="park", text="secret body not in observations",
            owner="operator", request_id="pending-1", project="offline",
            expires_at=self.clock.now_ms() + 600000,
            flush_condition="explicit-human-confirmation")["input"]
        self.client.inboxes[sid][0]["time"]["created"] = self.clock.now_ms() - 10000
        return watch, row

    def test_disabled_prefix_does_not_consume_enabled_observation_budget(self):
        watch = self.watch("ses_a_disabled")
        self.service.unregister_watch("ses_a_disabled", owner="operator",
            confirm="ses_a_disabled", expected_revision=watch["revision"])
        self.watch("ses_b_enabled")
        seen = []
        original = self.client.inbox

        def observe(sid):
            seen.append(sid)
            return original(sid)

        with patch.object(self.client, "inbox", side_effect=observe):
            self.service.observe_registered(limit=1)
        self.assertEqual(seen, ["ses_b_enabled"])

    def test_revocation_during_get_discards_late_observation_and_alert(self):
        watch, _ = self.overdue()
        original = self.client.inbox

        def revoke(sid):
            items = original(sid)
            self.service.unregister_watch(sid, owner="operator", confirm=sid,
                expected_revision=watch["revision"])
            return items

        with patch.object(self.client, "inbox", side_effect=revoke):
            out = self.service.observe_registered()
        self.assertEqual(out["observed"], [])
        self.assertEqual(self.conn.execute("SELECT count(*) FROM alerts").fetchone()[0], 0)
        self.assertFalse(self.service.list_watches()["watches"][0]["enabled"])

    def test_pending_alert_condition_survives_ack_without_age_based_duplicates(self):
        _, row = self.overdue()
        self.service.observe_registered()
        alerts = self.base.list_alerts(state="new", limit=100)
        self.assertEqual(len(alerts), 1)
        self.assertIn("operator", alerts[0]["detail"])
        self.assertIn(row["message_id"], alerts[0]["detail"])
        self.base.ack_alert(alerts[0]["id"], actor="operator")
        for _ in range(3):
            self.clock.advance(seconds=1)
            self.service.observe_registered()
        self.assertEqual(self.conn.execute("SELECT count(*) FROM alerts").fetchone()[0], 1,
            "同一pending责任的动态age不应绕过去重；ACK后也不重复")

    def test_last_observation_is_locally_queryable_without_client_or_body(self):
        self.overdue()
        self.service.observe_registered()

        def forbidden():
            self.fail("查询持久观察不应构造Client")

        local = make_managed_inputs(self.conn, self.temp.name, client=forbidden, clock=self.clock)
        result = local.list_watches()
        observation = result["watches"][0].get("last_observation")
        self.assertIsInstance(observation, dict)
        self.assertEqual(observation["pending"], 1)
        self.assertTrue(observation["trusted"])
        self.assertGreaterEqual(observation["oldest_age_ms"], 10000)
        self.assertIn("next_action", observation)
        self.assertNotIn("secret body not in observations", repr(result))

    def test_unknown_operation_noop_still_checks_owner_and_revision(self):
        _, row = self.overdue()
        with patch.object(self.client, "cancel_inbox",
                          side_effect=TransportError("/inbox", "response lost")):
            self.service.cancel(row["input_id"], owner="operator", confirm=row["input_id"],
                expected_revision=row["revision"], reason="explicit operator request")
        current = self.service.get(row["input_id"])["input"]
        for owner, revision in (("another-owner", current["revision"]),
                                ("operator", current["revision"] - 1)):
            with self.subTest(owner=owner, revision=revision), \
                    self.assertRaises((ActionError, LedgerError)):
                self.service.cancel(row["input_id"], owner=owner, confirm=row["input_id"],
                    expected_revision=revision, reason="must not bypass authority")

    def test_unknown_cancel_cannot_be_replaced_with_flush_operation(self):
        _, row = self.overdue()
        with patch.object(self.client, "cancel_inbox",
                          side_effect=TransportError("/inbox", "response lost")):
            self.service.cancel(row["input_id"], owner="operator", confirm=row["input_id"],
                expected_revision=row["revision"], reason="explicit operator request")
        before = self.service.get(row["input_id"])["input"]
        with patch.object(self.client, "update_inbox_delivery",
                          side_effect=AssertionError("未知cancel不允许换operation发PATCH")) as write:
            self.service.flush(row["input_id"], owner="operator", confirm=row["input_id"],
                expected_revision=before["revision"], reason="must not replace unknown operation")
        self.assertEqual(write.call_count, 0)
        after = self.service.get(row["input_id"])["input"]
        self.assertEqual(after["message_id"], before["message_id"])
        self.assertEqual(after["revision"], before["revision"])

    def test_registered_external_pending_keeps_age_and_owner_without_inventing_intent(self):
        self.watch("ses_external")
        item = {"id": "msg_external_original", "delivery": "queue",
                "time": {"created": self.clock.now_ms() - 10000}}
        self.client.set_inbox("ses_external", [item])
        out = self.service.observe_registered()
        observation = out["observed"][0]
        self.assertEqual(observation["pending"], 1)
        self.assertIsInstance(observation["oldest_age_ms"], int,
            "已登记外部分支的有效排队时间不能因消息非managed来源而丢掉")
        self.assertGreaterEqual(observation["oldest_age_ms"], 10000)
        self.assertTrue(observation["aged_out"])
        self.assertEqual(self.client.inboxes["ses_external"], [item])
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.conn.execute(
            "SELECT count(*) FROM session_inputs WHERE source='managed_input'").fetchone()[0], 0)
        alerts = self.base.list_alerts(state="new", limit=100)
        self.assertEqual(len(alerts), 1)
        self.assertIn("operator", alerts[0]["detail"])
        self.assertIn("msg_external_original", alerts[0]["detail"])

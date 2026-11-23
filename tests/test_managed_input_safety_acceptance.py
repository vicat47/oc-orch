"""父级独立S4用例反例：取消证据、未知操作、防前缀饥饿和撤销读闸门。"""
import tempfile
import unittest
from unittest.mock import patch

from oc_orch.adapters.managed_input_ledger import ManagedInputLedger
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.target_gate import TargetGate
from oc_orch.application.managed_inputs import ManagedInputs
from oc_orch.client import TransportError
from oc_orch.db import init
from oc_orch.application.services import ActionError
from tests.fakes import FakeClient, FakeClock


def make_managed_inputs(conn, home, *, client, clock):
    base = Ledger(conn, home, clock=clock)
    return ManagedInputs(ManagedInputLedger(conn, base), client=client,
                         clock=clock, target_gate=TargetGate(conn, clock))


class ManagedInputSafetyAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="managed-safety-acceptance-", dir="/tmp/opencode")
        self.addCleanup(self.temp.cleanup)
        self.conn = init(self.temp.name)
        self.addCleanup(self.conn.close)
        self.clock, self.client = FakeClock(), FakeClient()
        self.service = make_managed_inputs(self.conn, self.temp.name,
                                           client=lambda: self.client, clock=self.clock)

    def watch(self, sid):
        self.client.set_session(sid, outcome=None)
        self.service.register_watch(sid, owner="operator", project="offline",
                                    max_pending_age_ms=1000)

    def park(self, sid="ses_park"):
        self.watch(sid)
        return self.service.send(sid, intent="park", text="park safely", owner="operator",
            request_id="park-1", project="offline", expires_at=self.clock.now_ms() + 60000,
            flush_condition="explicit-human-confirmation")["input"]

    def manual(self, operation, input_id):
        current = self.service.get(input_id)["input"]
        return getattr(self.service, operation)(input_id, owner="operator", confirm=input_id,
            expected_revision=current["revision"], reason="explicit operator request")

    def test_cancel_message_query_failure_is_unknown_not_cancelled(self):
        row = self.park()
        self.client.fail("get_message", TransportError("/message", "cannot establish delivery"), times=10)
        result = self.manual("cancel", row["input_id"])
        self.assertEqual(result["input"]["delivery"], "unknown")
        self.assertNotEqual(self.service.get(row["input_id"])["input"]["delivery"], "cancelled")

    def test_cancel_noop_with_late_history_visibility_stays_unknown(self):
        row = self.park()

        def concurrently_consume(sid, mid):
            self.client.deliver_inbox(sid, at=self.clock.now_ms())
            # DELETE 已无排队项，实际是no-op；本次历史查询暂时未见，不能冒充取消证据。

        with patch.object(self.client, "cancel_inbox", side_effect=concurrently_consume), \
                patch.object(self.client, "get_message", return_value=None):
            result = self.manual("cancel", row["input_id"])
        self.assertEqual(result["input"]["delivery"], "unknown")

    def test_unknown_flush_is_not_replayed_by_fresh_manual_revision(self):
        row = self.park()
        calls = []

        def lost_response(*args, **kwargs):
            calls.append(args)
            raise TransportError("/inbox", "steer may have happened")

        with patch.object(self.client, "update_inbox_delivery", side_effect=lost_response):
            for _ in range(2):
                try:
                    self.manual("flush", row["input_id"])
                except ActionError as exc:
                    self.assertEqual(exc.code, "unknown_response")
        self.assertEqual(len(calls), 1, "新revision人工确认也不能盲重放未知PATCH")

    def test_unknown_cancel_is_not_replayed_by_fresh_manual_revision(self):
        row = self.park()
        calls = []

        def lost_response(*args, **kwargs):
            calls.append(args)
            raise TransportError("/inbox", "delete may have happened")

        with patch.object(self.client, "cancel_inbox", side_effect=lost_response):
            self.manual("cancel", row["input_id"])
            self.manual("cancel", row["input_id"])
        self.assertEqual(len(calls), 1, "未知DELETE不得因再次人工调用而重放")

    def test_bounded_watch_rounds_cover_all_enabled_targets(self):
        for sid in ("ses_a", "ses_b", "ses_c", "ses_d", "ses_e"):
            self.watch(sid)
        seen = []
        original = self.client.inbox

        def observe(sid):
            seen.append(sid)
            return original(sid)

        with patch.object(self.client, "inbox", side_effect=observe):
            for _ in range(3):
                before = len(seen)
                self.service.observe_registered(limit=2)
                self.assertLessEqual(len(seen) - before, 2)
        self.assertEqual(set(seen), {"ses_a", "ses_b", "ses_c", "ses_d", "ses_e"},
                         "有界观察应轮转，而不是永久只查前缀")

    def test_revoked_watch_in_current_batch_is_not_queried(self):
        self.watch("ses_a")
        self.watch("ses_b")
        target = next(w for w in self.service.list_watches()["watches"] if w["session_id"] == "ses_b")
        seen = []
        original = self.client.inbox

        def revoke_next(sid):
            seen.append(sid)
            if sid == "ses_a":
                self.service.unregister_watch("ses_b", owner="operator", confirm="ses_b",
                                              expected_revision=target["revision"])
            return original(sid)

        with patch.object(self.client, "inbox", side_effect=revoke_next):
            self.service.observe_registered(limit=2)
        self.assertNotIn("ses_b", seen, "仅写回CAS不足，撤销后也不能继续下一个GET")

    def test_overdue_registered_pending_has_persistent_owner_alert(self):
        self.park("ses_overdue")
        self.client.inboxes["ses_overdue"][0]["time"]["created"] = self.clock.now_ms() - 10000
        self.service.observe_registered()
        alerts = self.conn.execute("SELECT detail FROM alerts WHERE state='new'").fetchall()
        self.assertTrue(any("ses_overdue" in a["detail"] and "operator" in a["detail"]
                            for a in alerts), "不能只有瞬时observed而无持久owner责任")

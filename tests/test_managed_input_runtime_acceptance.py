"""白名单观察的Supervisor接线独立验收；不连接真实OpenCode。"""
import tempfile
import os
import sys
import unittest
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from oc_orch.client import TransportError
from oc_orch.config import DEFAULTS
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock


class ManagedInputRuntimeAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="managed-runtime-", dir="/tmp/opencode")
        self.addCleanup(self.temp.cleanup)
        self.clock, self.client = FakeClock(), FakeClient()
        self.client.set_session("ses_watch", outcome=None)
        self.sup = Supervisor(self.temp.name, cfg=dict(DEFAULTS),
                              client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)

    def service(self):
        from oc_orch.composition import make_managed_inputs
        return make_managed_inputs(self.sup.conn, self.temp.name,
                                   client=lambda: self.client, clock=self.clock)

    def test_empty_observer_has_no_remote_reads_or_model_writes(self):
        with patch.object(self.client, "inbox") as inbox, \
                patch.object(self.client, "get_session") as session, \
                patch.object(self.client, "prompt") as prompt:
            self.sup.observe_managed_inputs()
        inbox.assert_not_called()
        session.assert_not_called()
        prompt.assert_not_called()

    def test_service_failure_does_not_skip_local_park_expiry(self):
        service = self.service()
        service.register_watch("ses_watch", owner="operator", project="offline",
                               max_pending_age_ms=1000)
        row = service.send("ses_watch", intent="park", text="do not auto-run",
            owner="operator", request_id="service-down-park", project="offline",
            expires_at=self.clock.now_ms() + 1000,
            flush_condition="manual-confirmation")["input"]
        self.clock.advance(seconds=2)
        self.client.fail("info", TransportError("/info", "offline"))
        with patch.object(self.client, "inbox") as inbox, \
                patch.object(self.client, "get_session") as session, \
                patch.object(self.client, "update_inbox_delivery") as flush, \
                patch.object(self.client, "cancel_inbox") as cancel:
            self.sup.cycle(dispatch=False)
        inbox.assert_not_called()
        session.assert_not_called()
        flush.assert_not_called()
        cancel.assert_not_called()
        self.assertEqual(len(self.client.prompt_calls), 1)
        alerts = self.sup.ledger.list_alerts(state="new", limit=100)
        self.assertTrue(any(row["input_id"] in a["detail"] for a in alerts))

    def test_unknown_observer_never_recovers_or_cancels_input(self):
        service = self.service()
        service.register_watch("ses_watch", owner="operator", project="offline",
                               max_pending_age_ms=1000)
        self.client.fail("inbox", TransportError("/inbox", "unavailable"), times=3)
        with patch.object(self.client, "prompt") as prompt, \
                patch.object(self.client, "update_inbox_delivery") as flush, \
                patch.object(self.client, "cancel_inbox") as cancel:
            self.sup.observe_managed_inputs()
        prompt.assert_not_called()
        flush.assert_not_called()
        cancel.assert_not_called()

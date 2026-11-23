"""父整合：真实责任→本地incident→独立有界执行器，生产/真实邮箱均不访问。"""

import json
import tempfile
import unittest

from oc_orch.composition import make_coordinator_control, make_operator_incident, make_operator_email
from oc_orch.config import DEFAULTS
from oc_orch.runtime.email_executor import OperatorEmailExecutor
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock
from tests.test_email_executor import enabled_policy, FakeTransport, fact


class IncidentRuntimeTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="incident-runtime-", dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.clock = FakeClock()
        self.client = FakeClient()
        self.sup = Supervisor(self.home, cfg=dict(DEFAULTS), client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.task = self.ledger.create_task(project="proj-a", title="needs decision",
                                             coordinator_session="ses_coord")
        self.lease = self.ledger.claim_coordinator(project="proj-a", session_id="ses_coord",
                                                   actor="test", ttl=3600)
        self.responsibility = self.sup._responsibility_store().open_responsibility(
            "operator", "proj-a", self.task["id"], state="waiting_human", wait_kind="decision",
            wait_condition="decision", next_action="choose next action",
            next_check_at=self.clock.now_ms()+60_000,
            deadline=self.clock.now_ms()+3600_000, condition_key="decision",
            target_session="ses_coord")

    def test_main_tick_registers_visible_incident_without_channel_or_model(self):
        before = self.ledger.get_task(self.task["id"])
        out = self.sup.tick_continuation(allow_remote=False)
        self.assertIn("incidents", out)
        items = make_operator_incident(self.ledger, self.clock).list(project="proj-a")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["owner"], "operator")
        self.assertEqual(items[0]["channel"], "not-configured")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_task(self.task["id"]), before)
        self.assertIsNone(self.sup.last_error)

    def test_approved_fake_channel_sends_only_from_independent_executor(self):
        transport = FakeTransport()
        policy = enabled_policy()
        service = make_operator_incident(self.ledger, self.clock, policy=policy,
                                         transport=transport, recipient_refs=("op-primary",))
        out = service.reconcile(limit=10)
        self.assertEqual(out["email_enqueued"], 1, out)
        self.assertEqual(transport.sent, [])
        result = OperatorEmailExecutor(self.home, clock=self.clock, policy=policy,
                                        transport=transport).tick()
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["external_calls"], {"send":1, "search":0})
        self.assertEqual(self.ledger.get_task(self.task["id"])["status"], "draft")
        self.assertEqual(self.client.prompt_calls, [])

    def test_real_factory_refuses_self_reported_incident_without_source(self):
        transport = FakeTransport()
        email = make_operator_email(self.sup.conn, self.clock,
                                    policy=enabled_policy(), transport=transport)
        out = email.enqueue_incident(fact())
        self.assertEqual(out["result"], "source-recheck-failed")
        self.assertEqual(email.dispatch_once(owner="test")["result"], "nothing-due")
        self.assertEqual(transport.sent, [])

    def test_pause_after_enqueue_is_rechecked_by_independent_executor(self):
        transport = FakeTransport()
        policy = enabled_policy()
        service = make_operator_incident(self.ledger, self.clock, policy=policy,
                                         transport=transport, recipient_refs=("op-primary",))
        service.reconcile(limit=10)
        control = make_coordinator_control(self.sup.conn, self.clock)
        self.assertTrue(control.pause(
            "proj-a", session_id="ses_coord", operation_id="pause", reason="user-pause",
            expected_revision=0, expected_epoch=self.lease["epoch"])["executed"])
        result = OperatorEmailExecutor(self.home, clock=self.clock, policy=policy,
                                        transport=transport).tick()
        self.assertTrue(result["ok"], result)
        self.assertEqual(transport.sent, [])
        self.assertTrue(control.get_project("proj-a")["paused"])

    def test_default_independent_tick_never_enables_email(self):
        self.sup.tick_continuation(allow_remote=False)
        result = OperatorEmailExecutor(self.home, clock=self.clock).tick()
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["external_calls"], {"send":0,"search":0})
        self.assertNotIn("@", json.dumps(make_operator_incident(self.ledger, self.clock).status()))


if __name__ == "__main__":
    unittest.main()

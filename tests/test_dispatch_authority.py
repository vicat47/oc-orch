"""父级交叉反例：派单主体/租约冻结及交接后的保事实恢复。"""

from pathlib import Path
import json
import tempfile
import unittest
from unittest.mock import patch

from oc_orch.composition import application, init
from oc_orch.config import DEFAULTS
from oc_orch.domain.errors import ActionError, LedgerError
from oc_orch.ledger import Ledger
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock, complete_execution


class DispatchAuthorityTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="dispatch-authority-", dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.clock = FakeClock()
        self.client = FakeClient()
        self.sup = Supervisor(self.home, cfg=dict(DEFAULTS), client=self.client,
                              clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.task = self.ledger.create_task(project="p", title="work",
                                             coordinator_session="ses_old")
        self.lease = self.ledger.claim_coordinator(
            project="p", session_id="ses_old", actor="test", ttl=3600)
        self.app = application(self.ledger, lambda: self.client, dict(DEFAULTS),
                               clock=self.clock)
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.other = Ledger(conn, self.home, clock=self.clock)

    def dispatch(self, **kwargs):
        return self.app.dispatch(self.task["id"], directory=self.home,
                                 coordinator=kwargs.pop("coordinator", "ses_old"), **kwargs)

    def handoff(self, holder="ses_new"):
        return self.other.claim_coordinator(
            project="p", session_id=holder, actor="test", takeover=True,
            old_confirmed_stopped=True, ttl=3600)

    def prepared(self):
        return self.ledger.create_attempt(
            self.task["id"], directory=self.home, session_id="ses_worker",
            dispatch_msg_id="msg_original", metadata={
                "coordinator": "ses_old", "dispatch": {
                    "session_id_planned": "ses_worker", "prompt": "work",
                    "create": {"directory": self.home}}})

    def test_old_holder_cannot_rebind_task_or_claim_new_attempt(self):
        self.handoff()
        before = self.ledger.get_task(self.task["id"])
        with self.assertRaises((ActionError, LedgerError)):
            self.dispatch()
        self.assertEqual(self.ledger.get_task(self.task["id"]), before)
        self.assertEqual(self.ledger.list_attempts(), [])
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])

    def test_expired_lease_cannot_start_work(self):
        self.clock.advance(seconds=3601)
        with self.assertRaises((ActionError, LedgerError)):
            self.dispatch()
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])

    def test_recovery_blocked_lease_cannot_start_work(self):
        self.other.record_coordinator_failure("p", reason="retired", key="retired")
        self.other.conn.execute(
            "UPDATE coordinator_leases SET recovery_blocked=1 WHERE project='p'")
        with self.assertRaises((ActionError, LedgerError)):
            self.dispatch()
        self.assertEqual(self.client.create_calls, [])

    def test_new_attempt_freezes_dispatch_subject_and_lease(self):
        out = self.dispatch()
        auth = self.ledger.get_attempt(out["attempt_id"])["metadata"]["dispatch_authority"]
        self.assertEqual(auth["session_id"], "ses_old")
        self.assertEqual(auth["epoch"], self.lease["epoch"])
        self.assertIsInstance(auth["lease_event"], int)

    def test_wal_handoff_during_create_blocks_prompt_keeps_original_identity(self):
        original = self.client.create_session

        def create_then_handoff(**kwargs):
            out = original(**kwargs)
            self.handoff()
            return out

        with patch.object(self.client, "create_session", side_effect=create_then_handoff):
            with self.assertRaises((ActionError, LedgerError)):
                self.dispatch()
        self.assertEqual(self.client.prompt_calls, [])
        att = self.ledger.list_attempts()[0]
        self.assertEqual(att["session_id"], self.client.create_calls[0]["id"])
        self.assertTrue(att["dispatch_msg_id"])
        self.assertIsNone(att["site_released_at"])
        self.assertIsNone(att["stop_requested_at"])
        self.assertEqual(self.ledger.get_task(self.task["id"])["coordinator_session"], "ses_new")

    def test_away_and_back_same_holder_cannot_use_old_epoch(self):
        original = self.client.create_session

        def create_then_reacquire(**kwargs):
            out = original(**kwargs)
            self.handoff()
            self.handoff("ses_old")
            return out

        with patch.object(self.client, "create_session", side_effect=create_then_reacquire):
            with self.assertRaises((ActionError, LedgerError)):
                self.dispatch()
        self.assertEqual(self.client.prompt_calls, [])

    def test_release_reclaim_same_millisecond_cannot_aba_old_authority(self):
        original = self.client.create_session

        def create_then_reclaim(**kwargs):
            out = original(**kwargs)
            self.other.release_coordinator("p", actor="test")
            self.other.claim_coordinator(project="p", session_id="ses_old", actor="test", ttl=3600)
            return out

        with patch.object(self.client, "create_session", side_effect=create_then_reclaim):
            with self.assertRaises((ActionError, LedgerError)):
                self.dispatch()
        self.assertEqual(self.client.prompt_calls, [])

    def test_rework_by_old_holder_cannot_release_site(self):
        att = self.prepared()
        complete_execution(self.ledger, att["id"])
        self.handoff()
        with self.assertRaises((ActionError, LedgerError)):
            self.app.rework(self.task["id"], coordinator="ses_old", feedback="fix")
        self.assertIsNone(self.ledger.get_attempt(att["id"])["site_released_at"])
        self.assertEqual(len(self.ledger.list_attempts()), 1)
        self.assertEqual(self.client.prompt_calls, [])

    def test_recovery_after_handoff_may_observe_but_never_post(self):
        att = self.prepared()
        self.client.set_session("ses_worker")
        self.handoff()
        self.sup.reconcile_dispatch(att)
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_attempt(att["id"])["dispatch_msg_id"], "msg_original")

    def test_inflight_admission_can_be_confirmed_after_handoff(self):
        att = self.prepared()
        self.ledger.record_dispatch(att["id"], "unknown")
        self.client.set_session("ses_worker")
        self.client.set_inbox("ses_worker", [{"id": "msg_original"}])
        self.handoff()
        out = self.sup.reconcile_dispatch(att)
        self.assertEqual(out["dispatch_state"], "confirmed")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_task(self.task["id"])["coordinator_session"], "ses_new")

    def test_lease_renewal_does_not_invalidate_dispatch(self):
        original = self.client.create_session

        def create_then_renew(**kwargs):
            out = original(**kwargs)
            self.other.claim_coordinator(project="p", session_id="ses_old", actor="test", ttl=7200)
            return out

        with patch.object(self.client, "create_session", side_effect=create_then_renew):
            self.dispatch()
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_no_lease_explicit_legacy_coordinator_remains_compatible(self):
        self.ledger.release_coordinator("p", actor="test")
        out = self.dispatch()
        self.assertEqual(out["task_id"], self.task["id"])
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_paused_task_does_not_even_claim_an_attempt(self):
        self.ledger.transition_task(self.task["id"], "paused", actor="operator")
        before = self.ledger.get_task(self.task["id"])
        with self.assertRaises((ActionError, LedgerError)):
            self.dispatch()
        self.assertEqual(self.ledger.get_task(self.task["id"]), before)
        self.assertEqual(self.ledger.list_attempts(), [])
        self.assertEqual(self.client.create_calls, [])

    def test_boolean_authority_epoch_does_not_equal_integer_epoch(self):
        out = self.dispatch()
        att = self.ledger.get_attempt(out["attempt_id"])
        meta = dict(att["metadata"])
        meta["dispatch_authority"]["epoch"] = True
        # 模拟持久数据损坏，不经正常metadata入口伪装成合法修改授权。
        self.sup.conn.execute("UPDATE attempts SET metadata=? WHERE id=?", (json.dumps(meta), att["id"]))
        with self.assertRaises(LedgerError) as rejected:
            self.ledger.check_attempt_dispatch(att["id"])
        self.assertEqual(rejected.exception.code, "DISPATCH_AUTHORITY_INVALID")

    def test_explicit_no_lease_rebind_is_audited_with_atomic_claim(self):
        self.ledger.release_coordinator("p", actor="test")
        out = self.dispatch(coordinator="ses_explicit")
        self.assertEqual(self.ledger.get_task(self.task["id"])["coordinator_session"], "ses_explicit")
        rows = self.sup.conn.execute(
            "SELECT detail FROM audit WHERE entity='task' AND entity_id=? AND action='task.patch'",
            (self.task["id"],)).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertIn('"to_session":"ses_explicit"', rows[0]["detail"])
        att = self.ledger.get_attempt(out["attempt_id"])
        self.assertEqual(att["metadata"]["dispatch_authority"]["session_id"], "ses_explicit")

    def test_generic_metadata_update_cannot_strip_frozen_subject(self):
        out = self.dispatch()
        att = self.ledger.get_attempt(out["attempt_id"])
        for bad in ({}, None, {**att["metadata"], "dispatch_authority":None}):
            with self.subTest(bad=bad), self.assertRaises(LedgerError) as rejected:
                self.ledger.update_attempt(att["id"], fields={"metadata":bad}, actor="test")
            self.assertEqual(rejected.exception.code, "DISPATCH_AUTHORITY_IMMUTABLE")
        self.assertEqual(self.ledger.get_attempt(att["id"]), att)


if __name__ == "__main__":
    unittest.main()

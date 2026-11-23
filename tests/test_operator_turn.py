"""指定显式提交默认用户：原ID/许可/屏障先落账，unknown零重投，结束复用完整窗口。"""
import tempfile
import threading
import unittest
from unittest.mock import patch

from oc_orch import composition
from oc_orch.adapters.writer_guard import install_writer_guard
from oc_orch.adapters.delivery_guard import install_delivery_guard
from oc_orch.adapters.resource_authorization import ResourceAuthorizationStore
from oc_orch.domain.errors import LedgerError
from tests.fakes import FakeClock


class Client:
    def __init__(self, clock):
        self.clock, self.posts, self.queue, self.messages = clock, [], [], []
        self.fail = False

    def get_session(self, sid):
        return {"id": sid, "model": {"providerID": "trial", "id": "flash"},
                "outcome": "succeeded", "time": {"busy": 1, "idle": self.clock.now_ms()}}

    def inbox(self, sid):
        return self.queue

    def active_sessions(self):
        return {}

    def prompt(self, sid, **kw):
        self.posts.append((sid, kw))
        if self.fail:
            raise OSError("response lost")

    def message_page(self, sid, **kw):
        return {"data": list(reversed(self.messages)), "cursor": {"next": None}}


class OperatorTurnTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.conn = composition.init(self.tmp.name)
        self.addCleanup(self.conn.close)
        install_writer_guard(self.conn)
        install_delivery_guard(self.conn)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.ledger = composition.Ledger(self.conn, self.tmp.name, clock=self.clock)
        self.lease = self.ledger.claim_coordinator(project="p", session_id="ses_coord", ttl=3600, actor="test")
        self.control = composition.make_coordinator_control(self.conn, self.clock)
        self.client = Client(self.clock)
        self.service = composition.make_operator_turn(self.ledger, client=lambda: self.client, clock=self.clock)
        ResourceAuthorizationStore(self.conn, self.clock).replace("coordinator", "ses_coord",
            resources=[{"model": "trial/flash", "cost": "no-extra-cost", "evidence": "user trial", "internal_calls_controlled": True}],
            expires_at=2_000_000, expected_revision=0, operation_id="auth", actor="test", reason="test")

    def body(self, **patch):
        return {"project": "p", "session_id": "ses_coord", "operation_id": "op-1", "text": "请补充说明",
                "reason": "test", "expected_revision": 0, "expected_epoch": self.lease["epoch"], "confirm": "p", **patch}

    def send(self, **patch):
        return self.service.send(self.body(**patch))

    def test_prepared_barrier_and_independent_permit_before_network(self):
        original = self.client.prompt
        def prompt(sid, **kw):
            barrier = self.control.get_session(sid)["barrier"]
            self.assertEqual(barrier["message_id"], kw["message_id"])
            self.assertTrue(barrier["trusted"])
            self.assertIsNone(self.ledger.input_for_receipt(sid, kw["message_id"]))
            self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM meta WHERE key LIKE 'delivery-action:v1:%'").fetchone()[0], 1)
            original(sid, **kw)
        self.client.prompt = prompt
        result = self.send()
        self.assertEqual(result["operation"]["state"], "admitted")
        self.assertEqual(result["operation"]["source"], "explicit-operator")
        self.assertFalse(self.control.permits("p", "ses_coord")["permits"])

    def test_response_unknown_preserves_identity_and_duplicate_zero_post(self):
        self.client.fail = True
        first = self.send()
        self.assertEqual(first["operation"]["state"], "unknown")
        second = self.send()
        self.assertEqual(first["operation"]["message_id"], second["operation"]["message_id"])
        self.assertEqual(len(self.client.posts), 1)

    def test_changed_body_same_operation_refused(self):
        self.send()
        with self.assertRaises(LedgerError):
            self.send(text="different")
        self.assertEqual(len(self.client.posts), 1)

    def test_user_fields_cannot_promote_automatic_or_submit_end_evidence(self):
        for key in ("trusted", "attestation", "source", "message_id", "idle_id", "origin_at"):
            with self.subTest(key=key), self.assertRaises(LedgerError):
                self.send(**{key: "forged"})
        self.assertEqual(self.client.posts, [])

    def test_exact_scope_confirmation_and_current_epoch(self):
        for patch in ({"confirm": "p "}, {"expected_epoch": 99}, {"expected_revision": True}, {"session_id": "foreign"}):
            with self.subTest(patch=patch), self.assertRaises(LedgerError):
                self.send(**patch)
        self.assertEqual(self.client.posts, [])

    def test_pause_preserved_but_explicit_chat_allowed_and_never_claims(self):
        self.control.pause("p", session_id="ses_coord", operation_id="pause", reason="operator", expected_revision=0,
                           expected_epoch=self.lease["epoch"])
        self.send()
        self.assertEqual(self.control.get_project("p")["status"], "paused")
        self.assertEqual(self.ledger.coordinator_lease("p")["epoch"], self.lease["epoch"])

    def test_provider_refusal_zero_post_and_no_barrier(self):
        self.ledger.set_meta("provider_state", '{"trial/flash":{"until":2000000}}')
        with self.assertRaises(LedgerError):
            self.send()
        self.assertEqual(self.client.posts, [])
        self.assertIsNone(self.control.get_session("ses_coord")["barrier"])

    def test_queue_unknown_or_foreign_refused(self):
        self.client.queue = [{"id": "automatic"}]
        with self.assertRaises(LedgerError):
            self.send()
        self.assertEqual(self.client.posts, [])

    def test_full_original_window_finishes_only_registered_turn(self):
        sent = self.send()["operation"]
        self.clock.advance(seconds=1)
        self.client.messages = [
            {"id": sent["message_id"], "type": "user", "time": {"created": sent["origin_at"]}},
            {"id": "answer", "type": "assistant", "time": {"created": sent["origin_at"] + 1, "completed": self.clock.now_ms() - 1}},
            {"id": "idle", "type": "idle", "outcome": "succeeded", "time": {"created": self.clock.now_ms()}}]
        result = self.service.observe(project="p", session_id="ses_coord", operation_id="op-1")
        self.assertTrue(result["finished"])
        self.assertIsNone(self.control.get_session("ses_coord")["barrier"])
        self.assertEqual(len(self.client.posts), 1)

    def test_empty_window_never_counts_as_finished(self):
        self.send()
        self.clock.advance(seconds=1)
        self.assertFalse(self.service.observe(project="p", session_id="ses_coord", operation_id="op-1")["finished"])
        self.assertIsNotNone(self.control.get_session("ses_coord")["barrier"])

    def test_old_operation_observation_never_finishes_newer_turn(self):
        self.send()
        self.clock.advance(seconds=1)
        newer = self.send(operation_id="op-new", expected_revision=1)["operation"]
        self.clock.advance(seconds=1)
        self.client.messages = [{"id": newer["message_id"], "type": "user", "time": {"created": newer["origin_at"]}},
                                {"id": "idle-new", "type": "idle", "outcome": "succeeded", "time": {"created": self.clock.now_ms()}}]
        result = self.service.observe(project="p", session_id="ses_coord", operation_id="op-1")
        self.assertFalse(result["finished"])
        self.assertEqual(self.control.get_session("ses_coord")["barrier"]["message_id"], newer["message_id"])

    def test_supervisor_shared_observer_records_original_receipt(self):
        sent = self.send()["operation"]
        self.clock.advance(seconds=1)
        self.client.messages = [{"id": sent["message_id"], "type": "user", "time": {"created": sent["origin_at"]}},
                                {"id": "idle-runtime", "type": "idle", "outcome": "succeeded", "time": {"created": self.clock.now_ms()}}]
        observer = composition.make_registered_turn_observer(self.ledger, client=lambda: self.client, clock=self.clock)
        self.assertTrue(observer.observe("ses_coord")["finished"])
        self.assertEqual(self.service.show(project="p", session_id="ses_coord", operation_id="op-1")["operation"]["state"], "finished")

    def test_newer_user_or_interruption_cannot_be_cleared_by_old_receipt(self):
        self.send()
        self.clock.advance(seconds=1)
        self.control.hold_session("ses_coord", event_id="new-stop", origin_at=self.clock.now_ms(), reason="interrupted")
        self.assertFalse(self.service.observe(project="p", session_id="ses_coord", operation_id="op-1")["finished"])
        self.assertIsNotNone(self.control.get_session("ses_coord")["hold"])

    def test_read_only_show_zero_client_or_write(self):
        self.send()
        self.service.client = lambda: (_ for _ in ()).throw(AssertionError("no client"))
        before = self.conn.total_changes
        self.service.show(project="p", session_id="ses_coord", operation_id="op-1")
        self.assertEqual(self.conn.total_changes, before)

    def test_restart_unknown_preserves_original_body_permit_and_zero_repost(self):
        self.client.fail = True
        original = self.send()["operation"]
        conn = composition.connect(self.tmp.name)
        self.addCleanup(conn.close)
        ledger = composition.Ledger(conn, self.tmp.name, clock=self.clock)
        service = composition.make_operator_turn(ledger, client=lambda: self.client, clock=self.clock)
        out = service.send(self.body())
        self.assertEqual(out["operation"], original)
        self.assertEqual(len(self.client.posts), 1)

    def test_two_simultaneous_same_intents_single_durable_permit_and_post(self):
        barrier, results, errors = threading.Barrier(2), [], []
        def run():
            conn = composition.connect(self.tmp.name)
            try:
                ledger = composition.Ledger(conn, self.tmp.name, clock=self.clock)
                service = composition.make_operator_turn(ledger, client=lambda: self.client, clock=self.clock)
                barrier.wait(timeout=5)
                results.append(service.send(self.body()))
            except Exception as exc: errors.append(type(exc).__name__)
            finally: conn.close()
        threads = [threading.Thread(target=run) for _ in range(2)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10); self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(len(self.client.posts), 1)
        self.assertEqual(len({r["operation"]["message_id"] for r in results}), 1)

    def test_completion_receipt_failure_rolls_back_barrier_and_then_can_observe_original(self):
        sent = self.send()["operation"]
        self.clock.advance(seconds=1)
        self.client.messages = [{"id": sent["message_id"], "type": "user", "time": {"created": sent["origin_at"]}},
                                {"id": "idle-runtime", "type": "idle", "outcome": "succeeded", "time": {"created": self.clock.now_ms()}}]
        from oc_orch.adapters.operator_turn import OperatorTurnStore
        with patch.object(OperatorTurnStore, "_mark_in_tx", side_effect=RuntimeError("receipt disk failure")), self.assertRaises(RuntimeError):
            self.service.observe(project="p", session_id="ses_coord", operation_id="op-1")
        self.assertIsNotNone(self.control.get_session("ses_coord")["barrier"])
        self.clock.advance(seconds=31)
        self.client.messages[-1]["time"]["created"] = self.clock.now_ms()
        result = self.service.observe(project="p", session_id="ses_coord", operation_id="op-1")
        self.assertTrue(result["finished"])
        self.assertEqual(len(self.client.posts), 1)

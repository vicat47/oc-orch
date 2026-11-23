"""父协调集成：真实控制台账与Supervisor/派单路径，不用假控制服务。"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from oc_orch.composition import application, init, make_coordinator_control
from oc_orch.config import DEFAULTS
from oc_orch.domain.errors import ActionError, LedgerError
from oc_orch.domain.dependency_wait import CONDITION_TASK
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock


class ContinuityRuntimeTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="continuity-runtime-", dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = Path(tmp.name)
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_coord", outcome=None)
        self.sup = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                              client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.lease = self.ledger.claim_coordinator(
            project="p", session_id="ses_coord", actor="test", ttl=3600)
        self.task = self.ledger.create_task(
            project="p", title="task", coordinator_session="ses_coord")
        self.control = make_coordinator_control(self.sup.conn, self.clock)

    def change_mode(self, action, *, op="op-control"):
        snap = self.control.get_project("p") or {}
        return getattr(self.control, action)(
            "p", session_id="ses_coord", operation_id=op, reason="explicit-user",
            expected_revision=snap.get("revision", 0),
            expected_epoch=self.lease["epoch"])

    def pending(self, key="result"):
        return self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord", project="p",
            dedupe_key=key, payload={"summary": key})

    def begin_turn(self, mid="msg_user"):
        self.clock.advance(seconds=1)
        at = self.clock.now_ms()
        self.client.append_message("ses_coord", {
            "id": mid, "type": "user", "text": "user input",
            "time": {"created": at}})
        out = self.control.begin_user_turn(
            "ses_coord", message_id=mid, origin_at=at,
            operation_id="op-" + mid, trusted=True)
        return out

    def finish_remote(self):
        self.clock.advance(seconds=1)
        at = self.clock.now_ms()
        self.client.add_idle("ses_coord", outcome="succeeded", at=at)
        self.client.set_session("ses_coord", outcome="succeeded", time_={"idle": at})
        return at

    def test_project_pause_blocks_wake_but_keeps_provider_gate(self):
        self.sup.target_gate.observe_error(
            "ses_coord", {"category": "quota", "message": "quota"},
            evidence_key="quota", observed_at=self.clock.now_ms())
        before = self.sup.target_gate.get("ses_coord")
        self.change_mode("pause")
        notice = self.pending()
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.sup.target_gate.get("ses_coord"), before)
        self.assertEqual(self.ledger.get_notification(notice["id"])["state"], "pending")

    def test_resume_cannot_bypass_provider_cooldown(self):
        self.change_mode("pause")
        self.sup.target_gate.observe_error(
            "ses_coord", {"category": "quota"}, evidence_key="quota",
            observed_at=self.clock.now_ms())
        before = self.sup.target_gate.get("ses_coord")
        self.change_mode("resume", op="op-resume")
        self.pending()
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.sup.target_gate.get("ses_coord"), before)

    def test_new_pause_blocks_wake_even_without_any_target_gate(self):
        self.change_mode("pause")
        notice = self.pending()
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_notification(notice["id"])["attempts"], 0)
        self.assertIsNone(self.sup.target_gate.get("ses_coord"))

    def test_pause_keeps_inflight_worker_and_write_rights(self):
        attempt = self.ledger.create_attempt(
            self.task["id"], directory=str(self.home / "worker"),
            session_id="ses_worker", status="running", dispatch_state="confirmed")
        self.client.set_session("ses_worker", active=True)
        self.change_mode("pause")
        self.sup.dispatch_notifications()
        after = self.ledger.get_attempt(attempt["id"])
        self.assertEqual(after["status"], "running")
        self.assertIsNone(after["site_released_at"])
        self.assertIsNone(after["stop_requested_at"])
        self.assertEqual(self.client.interrupt_calls, [])

    def test_turn_interruption_does_not_overwrite_provider_gate(self):
        self.sup.target_gate.observe_error(
            "ses_coord", {"category": "quota"}, evidence_key="quota",
            observed_at=self.clock.now_ms())
        before = self.sup.target_gate.get("ses_coord")
        self.clock.advance(seconds=1)
        self.sup.handle_coordinator_terminal(
            "ses_coord", "interrupted", "user", {
                "id": "evt-stop", "created": self.clock.now_ms(),
                "data": {"sessionID": "ses_coord"}})
        self.assertEqual(self.sup.target_gate.get("ses_coord"), before)
        self.assertFalse(self.control.permits("p", "ses_coord")["permits"])

    def test_unknown_external_input_cannot_clear_legacy_manual_gate(self):
        self.sup.target_gate.block_manual(
            "ses_coord", reason="coordinator-interrupted")
        self.clock.advance(seconds=1)
        self.sup.ingest_event({
            "id": "evt-foreign", "type": "session.inbox.enqueued",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "inboxID": "msg_unknown"}})
        self.assertIsNotNone(self.sup.target_gate.get("ses_coord"))

    def test_trusted_user_turn_blocks_wake_then_rest_completion_resumes(self):
        self.begin_turn()
        notice = self.pending()
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        self.finish_remote()
        self.sup.observe_coordinators({})
        self.assertTrue(self.control.permits("p", "ses_coord")["permits"])
        self.clock.advance(seconds=301)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertEqual(self.ledger.get_notification(notice["id"])["state"], "sent")

    def test_extra_user_message_does_not_finish_the_registered_turn(self):
        self.begin_turn()
        self.clock.advance(seconds=1)
        self.client.append_message("ses_coord", {
            "id": "msg_later", "type": "user", "text": "later",
            "time": {"created": self.clock.now_ms()}})
        self.finish_remote()
        self.sup.observe_coordinators({})
        self.assertFalse(self.control.permits("p", "ses_coord")["permits"])

    def test_incomplete_window_keeps_user_barrier(self):
        self.begin_turn()
        self.finish_remote()
        with patch.object(self.sup, "_window", return_value=([], {"complete": False})):
            self.sup.observe_coordinators({})
        self.assertFalse(self.control.permits("p", "ses_coord")["permits"])

    def test_user_completion_cannot_clear_explicit_project_pause(self):
        self.begin_turn()
        self.change_mode("pause")
        self.finish_remote()
        self.sup.observe_coordinators({})
        self.assertFalse(self.control.permits("p", "ses_coord")["permits"])

    def test_pause_during_target_observation_is_rechecked_before_post(self):
        self.pending()
        original = self.client.get_session
        paused = False

        def pause_before_return(sid):
            nonlocal paused
            result = original(sid)
            if sid == "ses_coord" and not paused:
                paused = True
                self.change_mode("pause")
            return result

        with patch.object(self.client, "get_session", side_effect=pause_before_return):
            self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])

    def test_paused_project_cannot_dispatch_a_new_worker(self):
        self.change_mode("pause")
        app = application(self.ledger, lambda: self.client, dict(DEFAULTS), clock=self.clock)
        with self.assertRaises((ActionError, LedgerError)):
            app.dispatch(self.task["id"], directory=str(self.home / "new-worker"),
                         coordinator="ses_coord", prompt="work")
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])

    def test_pause_one_project_does_not_stop_other_project_on_same_session(self):
        self.ledger.claim_coordinator(project="q", session_id="ses_coord", actor="test")
        self.change_mode("pause")
        p = self.pending("p")
        q = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord", project="q",
            dedupe_key="q", payload={"summary": "q"})
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertEqual(self.ledger.get_notification(p["id"])["state"], "pending")
        self.assertEqual(self.ledger.get_notification(q["id"])["state"], "sent")
        self.assertNotIn(p["id"], self.client.prompt_calls[0]["text"])

    def test_broken_project_control_fails_closed(self):
        self.ledger.set_meta("coordinator-control:project:p", "bad-json")
        self.pending()
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])

    def prepared_input(self):
        notice = self.pending()
        reserved = self.ledger.reserve_target_input(
            "ses_coord", owner="test", notification_ids=[notice["id"]],
            source="automatic_wake", now=self.clock.now_ms())["input"]
        return self.ledger.finalize_input(
            reserved["input_id"], final_notification_ids=[notice["id"]],
            text_factory=lambda rows: "frozen", owner="test",
            expected_revision=reserved["revision"], now=self.clock.now_ms())["input"]

    def test_wal_pause_wins_before_possible_marker(self):
        inp = self.prepared_input()
        conn2 = init(str(self.home))
        self.addCleanup(conn2.close)
        other = make_coordinator_control(conn2, self.clock)
        out = other.pause(
            "p", session_id="ses_coord", operation_id="op-race", reason="pause",
            expected_revision=0, expected_epoch=self.lease["epoch"])
        self.assertTrue(out["executed"])
        with self.assertRaises(LedgerError) as raised:
            self.ledger.mark_input_possible(
                inp["input_id"], owner="test", expected_revision=inp["revision"])
        self.assertEqual(raised.exception.code, "PROJECT_PAUSED")
        after = self.ledger.get_input(inp["input_id"])
        self.assertEqual(after, inp)

    def test_wal_possible_wins_before_pause_keeps_original_identity(self):
        inp = self.prepared_input()
        pos = self.ledger.mark_input_possible(
            inp["input_id"], owner="test", expected_revision=inp["revision"])
        self.assertTrue(pos["changed"])
        self.change_mode("pause")
        after = self.ledger.get_input(inp["input_id"])
        self.assertEqual(after, pos["input"])
        self.assertEqual(after["admission_state"], "possible")
        self.assertTrue(after["outstanding"])

    def test_new_registered_turn_during_window_rejects_old_finish(self):
        self.begin_turn()
        self.finish_remote()
        original = self.sup._window

        def window_then_new_turn(*args, **kwargs):
            out = original(*args, **kwargs)
            self.begin_turn("msg_new")
            return out

        with patch.object(self.sup, "_window", side_effect=window_then_new_turn):
            self.sup.observe_coordinators({})
        snap = self.control.get_session("ses_coord")
        self.assertEqual(snap["barrier_message_id"], "msg_new")

    def test_user_turn_observation_budget_survives_supervisor_restart(self):
        self.begin_turn()
        self.finish_remote()
        incomplete = ([], {"complete": False})
        with patch.object(self.sup, "_window", return_value=incomplete) as read:
            for _ in range(8):
                self.sup.observe_coordinators({})
                self.clock.advance(seconds=31)
            self.assertEqual(read.call_count, 8)
        restarted = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                               client=self.client, clock=self.clock)
        self.addCleanup(restarted.conn.close)
        with patch.object(restarted, "_window", return_value=incomplete) as read:
            for _ in range(4):
                restarted.observe_coordinators({})
                self.clock.advance(seconds=31)
            self.assertEqual(read.call_count, 0)
        alerts = [a for a in self.ledger.list_alerts()
                  if a["kind"] == "user_turn_unknown"]
        self.assertEqual(len(alerts), 1)
        self.assertFalse(self.control.permits("p", "ses_coord")["permits"])

    def test_project_pause_during_worker_create_never_posts_prompt(self):
        app = application(self.ledger, lambda: self.client, dict(DEFAULTS), clock=self.clock)
        original = self.client.create_session

        def create_then_pause(**kwargs):
            out = original(**kwargs)
            self.change_mode("pause")
            return out

        with patch.object(self.client, "create_session", side_effect=create_then_pause):
            with self.assertRaises((ActionError, LedgerError)):
                app.dispatch(self.task["id"], directory=str(self.home / "new-worker"),
                             coordinator="ses_coord", prompt="work")
        self.assertEqual(len(self.client.create_calls), 1)
        self.assertEqual(self.client.prompt_calls, [])
        att = self.ledger.get_attempt(self.ledger.get_task(self.task["id"])["current_attempt_id"])
        self.assertIsNone(att["site_released_at"])
        self.assertIsNone(att["stop_requested_at"])

    def test_business_health_separately_exposes_project_intent(self):
        self.change_mode("pause")
        out = self.sup.tick_continuation(allow_remote=False)
        item = next(t for t in out["business_health"]["tasks"]
                    if t["task_id"] == self.task["id"])
        self.assertEqual(item["state"], "explicitly_paused")
        self.assertEqual(item["control"]["project_intent"], "paused")
        self.assertEqual(self.ledger.get_task(self.task["id"])["status"], "draft")
        self.assertFalse(out["business_health"]["events"], "正常暂停不催人工")
        self.assertEqual(self.client.prompt_calls, [])

    def test_business_health_exposes_user_barrier_without_making_progress(self):
        self.begin_turn()
        out = self.sup.continuity_health(now=self.clock.now_ms())
        item = next(t for t in out["tasks"] if t["task_id"] == self.task["id"])
        self.assertEqual(item["state"], "waiting_user_turn")
        self.assertFalse(item["business_progress"])
        self.assertFalse(out["events"])

    def collect_worker_while_project_paused(self):
        site = self.home / "worker-site"
        site.mkdir()
        app = application(self.ledger, lambda: self.client, dict(DEFAULTS), clock=self.clock)
        sent = app.dispatch(self.task["id"], directory=str(site),
                            coordinator="ses_coord", prompt="work")
        sid, aid = sent["session_id"], sent["attempt_id"]
        self.clock.advance(seconds=1)
        started = self.clock.now_ms()
        self.client.deliver_inbox(sid, at=started)
        self.ledger.record_execution_started(aid, started_at=started, event_id="worker-start")
        self.clock.advance(seconds=1)
        self.client.append_message(sid, {
            "id": "msg_worker_result", "type": "assistant",
            "time": {"created": self.clock.now_ms()},
            "content": [{"type": "text", "text": "worker result preserved"}]})
        self.clock.advance(seconds=1)
        ended = self.clock.now_ms()
        self.client.add_idle(sid, outcome="succeeded", at=ended)
        self.client.set_session(sid, outcome="succeeded", time_={"idle": ended})
        self.change_mode("pause")
        self.sup.observe_attempt(self.ledger.get_attempt(aid), {}, active_ok=True)
        return aid

    def test_paused_project_still_collects_real_worker_result(self):
        aid = self.collect_worker_while_project_paused()
        attempt = self.ledger.get_attempt(aid)
        self.assertEqual(attempt["status"], "result_collected")
        self.assertTrue(Path(attempt["result_path"]).is_file())
        self.assertIsNone(attempt["verification"])
        self.assertNotEqual(self.ledger.get_task(self.task["id"])["status"], "accepted")
        notes = self.ledger.list_notifications(target_session="ses_coord")
        self.assertTrue(notes)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1, "仅原worker派单，不唤醒暂停协调者")
        self.assertTrue(all(n["state"] == "pending" for n in notes))

    def test_resume_merges_collected_result_and_pending_notice_without_accepting(self):
        self.collect_worker_while_project_paused()
        self.pending("extra-review")
        self.sup.dispatch_notifications()
        self.change_mode("resume", op="op-resume")
        self.clock.advance(seconds=301)
        self.sup.dispatch_notifications()
        coordinator_calls = [c for c in self.client.prompt_calls if c["session_id"] == "ses_coord"]
        self.assertEqual(len(coordinator_calls), 1)
        self.assertGreaterEqual(len(coordinator_calls[0]["metadata"]["oc_orch_notifications"]), 2)
        self.assertNotEqual(self.ledger.get_task(self.task["id"])["status"], "accepted")

    def test_dependency_tick_accumulates_one_review_during_project_pause(self):
        upstream = self.ledger.create_task(project="p", title="explicit prerequisite")
        service = self.sup._dependency_wait_service()
        wait = service.register_condition_wait(
            "coordinator", "p", self.task["id"], {
                "type": CONDITION_TASK, "task_ids": [upstream["id"]],
                "require": "cancelled", "mode": "all", "revision": 1},
            target_session="ses_coord", deadline=self.clock.now_ms() + 3600_000,
            next_check_at=self.clock.now_ms(), next_action="review explicit cancellation")
        self.change_mode("pause")
        self.ledger.transition_task(upstream["id"], "cancelled", actor="operator")
        first = self.sup.tick_continuation(allow_remote=False)
        self.assertEqual(first["dependencies"]["ready"], 1)
        for _ in range(3):
            self.clock.advance(seconds=31)
            self.sup.tick_continuation(allow_remote=False)
            self.sup.dispatch_notifications()
        notes = self.ledger.list_notifications(target_session="ses_coord")
        reviews = [n for n in notes if n["dedupe_key"].startswith("dep-review:")]
        self.assertEqual(len(reviews), 1)
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_task(self.task["id"])["status"], "draft")
        after = self.sup._responsibility_store().get_responsibility(wait["id"])
        self.assertEqual(after["state"], "awaiting_review")
        self.assertEqual(after["deadline"], wait["deadline"])
        self.change_mode("resume", op="op-resume")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_legacy_dependency_exits_to_actionable_human_without_remote_service(self):
        now = self.clock.now_ms()
        store = self.sup._responsibility_store()
        wait = store.open_responsibility(
            "operator", "p", self.task["id"], state="waiting_dependency",
            wait_kind="dependency", wait_condition="old unstructured text",
            next_action="review dependency", next_check_at=now, deadline=now + 3600_000,
            condition_key="legacy-condition", target_session="ses_coord")
        self.sup.tick_continuation(allow_remote=False)
        after = store.get_responsibility(wait["id"])
        self.assertEqual(after["state"], "waiting_human")
        self.assertEqual(after["deadline"], wait["deadline"])
        self.assertEqual(after["owner"], wait["owner"])
        self.assertIn("人工", after["next_action"])
        self.assertEqual(self.client.prompt_calls, [])


if __name__ == "__main__":
    unittest.main()

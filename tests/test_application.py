"""应用用例的真实台账契约：恢复身份、写权与外部 I/O 事务边界。"""

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.composition import application
from oc_orch.config import load_config
from oc_orch.db import init
from oc_orch.domain.errors import ActionError, LedgerError, TransportError
from oc_orch.domain.rules import ATTEMPT_ACTIVE
from oc_orch.ledger import Ledger
from oc_orch.results import scan_deliverable_baseline
from oc_orch.util import now_ms


class Clock:
    def __init__(self):
        self.time = now_ms()

    def now_ms(self):
        return self.time


class Client:
    def __init__(self, conn):
        self.conn = conn
        self.sessions = {}
        self.prompts = []
        self.fail_prompt = False
        self.fail_shells = False
        self.crash_create = False
        self.running_shells = []
        self.interrupted = []
        self.message_rows = {}
        self.active = {}

    def outside_transaction(self):
        if self.conn.in_transaction:
            raise AssertionError("网络调用不得持有 SQLite 事务")

    def create_session(self, **spec):
        self.outside_transaction()
        if self.crash_create:
            raise RuntimeError("simulated process crash before request")
        session = {**spec, "id": spec["session_id"], "time": {}, "outcome": None}
        self.sessions[spec["session_id"]] = session
        return session

    def get_session(self, session_id):
        self.outside_transaction()
        return self.sessions.get(session_id)

    def active_sessions(self):
        self.outside_transaction()
        return dict(self.active)

    def list_sessions(self, *, parent_id=None, limit=50, order="desc"):
        self.outside_transaction()
        return [s for s in self.sessions.values() if s.get("parentID") == parent_id][:limit]

    def inbox(self, session_id):
        self.outside_transaction()
        return []

    def prompt(self, session_id, **payload):
        self.outside_transaction()
        self.prompts.append((session_id, payload))
        if self.fail_prompt:
            raise TransportError("/prompt", "response lost after acceptance")
        return {"id": payload["message_id"]}

    def shells(self, directory):
        self.outside_transaction()
        if self.fail_shells:
            raise TransportError("/shell", "unreachable")
        return self.running_shells

    def interrupt(self, session_id):
        self.outside_transaction()
        self.interrupted.append(session_id)
        raise TransportError("/interrupt", "response lost")

    def messages(self, session_id, *, limit=100, order="desc", **kwargs):
        self.outside_transaction()
        rows = sorted(self.message_rows.get(session_id, []),
                      key=lambda m: (m["time"]["created"], m["id"]),
                      reverse=order == "desc")
        return rows[:limit]

    def message_page(self, session_id, *, limit=100, order="asc", cursor=None):
        return {"data": self.messages(session_id, limit=limit, order=order),
                "cursor": {"next": None, "previous": None}}

    def get_message(self, session_id, message_id):
        self.outside_transaction()
        return next((m for m in self.message_rows.get(session_id, [])
                     if m["id"] == message_id), None)


class ApplicationTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-app-")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = Clock()
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.client = Client(self.conn)
        self.app = application(self.ledger, lambda: self.client,
                               load_config(self.home), clock=self.clock)
        self.task = self.ledger.create_task(project="p", title="work", coordinator_session="ses_coord")

    def dispatch(self, **kw):
        return self.app.dispatch(self.task["id"], directory=self.home,
                                 coordinator="ses_coord", **kw)

    def finish(self, aid):
        # 绑定派单的尝试必须经窄终态事务（合法证据入口），不得通用迁移伪造
        att = self.ledger.get_attempt(aid)
        if not att["metadata"].get("execution"):
            baseline = scan_deliverable_baseline(self.ledger, aid, captured_at=self.clock.now_ms())
            self.ledger.seal_execution(aid, mode="new", session_before={}, baseline=baseline)
        started = self.clock.now_ms() + 1
        ended = started + 5
        sid = att["session_id"]
        self.client.sessions[sid] = {
            **self.client.sessions.get(sid, {}), "id": sid, "outcome": "succeeded",
            "time": {"busy": started, "idle": ended}}
        self.client.message_rows[sid] = [
            {"id": att["dispatch_msg_id"], "type": "user", "text": "execute task",
             "time": {"created": started}},
            {"id": f"msg_answer_{aid}", "type": "assistant", "finish": "stop",
             "time": {"created": started + 1, "completed": ended - 1},
             "content": [{"type": "text", "text": "done"}]},
            {"id": f"msg_idle_{aid}", "type": "idle", "outcome": "succeeded",
             "time": {"created": ended}},
        ]
        self.ledger.record_execution_started(aid, started_at=started, event_id=f"start-{aid}")
        out = self.ledger.record_terminal(
            aid, outcome="succeeded", source="sse", event_id=f"end-{aid}",
            candidate={"session_id": sid, "source": "sse", "outcome": "succeeded",
                       "ended_at": ended, "input_in_inbox": False, "input_owner": "unique"})
        self.assertEqual(out["result"], "accepted", out)

    def test_dispatch_generation_identity_and_transaction_boundary(self):
        first = self.dispatch()
        self.finish(first["attempt_id"])
        self.app.ensure_site_free(self.ledger.get_attempt(first["attempt_id"]))
        second = self.dispatch()
        att = self.ledger.get_attempt(second["attempt_id"])
        self.assertEqual(att["generation"], 2)
        spec = self.client.sessions[att["session_id"]]
        self.assertEqual(spec["metadata"]["oc_orch_generation"], 2)
        self.assertEqual(self.client.prompts[-1][1]["metadata"]["oc_orch_generation"], 2)
        self.assertIn("第 2 次尝试", att["metadata"]["dispatch"]["prompt"])
        self.assertEqual(att["dispatch_msg_id"], second["message_id"])
        self.assertEqual(att["metadata"]["write_scope"], [self.home])

    def test_dispatch_lost_response_keeps_recovery_identity(self):
        self.client.fail_prompt = True
        with self.assertRaises(ActionError) as raised:
            self.dispatch()
        err = raised.exception
        self.assertEqual(err.code, "dispatch_unknown")
        att = self.ledger.get_attempt(err.detail["attempt_id"])
        self.assertEqual(att["status"], "dispatch_unknown")
        self.assertEqual(att["session_id"], err.detail["session_id"])
        self.assertEqual(att["dispatch_msg_id"], err.detail["message_id"])
        self.assertEqual(len(self.client.prompts), 1)
        self.assertEqual(self.client.prompts[0][1]["message_id"], att["dispatch_msg_id"])
        self.assertTrue(att["metadata"]["dispatch"]["prompt"])

    def test_prepared_attempt_survives_crash_before_network(self):
        self.client.crash_create = True
        with self.assertRaisesRegex(RuntimeError, "process crash"):
            self.dispatch()
        att = self.ledger.list_attempts(task_id=self.task["id"])[0]
        self.assertEqual(att["dispatch_state"], "prepared")
        planned = att["metadata"]["dispatch"]
        self.assertEqual(planned["session_id_planned"], att["session_id"])
        self.assertEqual(planned["create"]["metadata"]["oc_orch_generation"], att["generation"])
        self.assertIn(att["id"], planned["prompt"])

    def test_metadata_preparation_failure_rolls_back_claim(self):
        def broken(generation):
            self.assertTrue(self.conn.in_transaction)
            raise RuntimeError("prepare failed")

        with self.assertRaisesRegex(RuntimeError, "prepare failed"):
            self.ledger.create_attempt(self.task["id"], directory=self.home,
                                        prepare_metadata=broken)
        self.assertEqual(self.ledger.list_attempts(task_id=self.task["id"]), [])
        self.assertIsNone(self.ledger.get_task(self.task["id"])["current_attempt_id"])

    def test_dry_run_does_not_construct_client_or_patch_task(self):
        before = self.ledger.get_task(self.task["id"])
        with patch.object(self.app, "_client", side_effect=AssertionError("no client")):
            result = self.app.dispatch(self.task["id"], directory=self.home,
                                       coordinator="ses_other", dry_run=True)
        self.assertTrue(result["dry_run"])
        self.assertEqual(self.ledger.get_task(self.task["id"]), before)
        self.assertEqual(self.ledger.list_attempts(), [])

    def test_rework_preserves_specs_and_same_session(self):
        first = self.dispatch(deliverables=["out.txt"], checks=["true"])
        self.finish(first["attempt_id"])
        second = self.app.rework(self.task["id"], feedback="fix output")
        self.assertEqual(second["session_id"], first["session_id"])
        self.assertEqual(second["generation"], 2)
        self.assertEqual({d["target"] for d in self.ledger.deliverables(second["attempt_id"])},
                         {"out.txt", "true"})
        self.assertIsNotNone(self.ledger.get_attempt(first["attempt_id"])["site_released_at"])
        self.assertEqual(self.ledger.get_task(self.task["id"])["current_attempt_id"], second["attempt_id"])

    def test_rework_current_guard_rejects_stale_caller(self):
        first = self.dispatch()
        self.finish(first["attempt_id"])
        second = self.app.rework(self.task["id"], feedback="second")
        with self.assertRaises(LedgerError) as raised:
            self.ledger.create_attempt(self.task["id"], directory=self.home + "/different",
                                        expected_current=first["attempt_id"])
        self.assertEqual(raised.exception.code, "NOT_CURRENT")
        self.assertEqual(self.ledger.get_task(self.task["id"])["current_attempt_id"], second["attempt_id"])

    def test_rework_lost_response_is_unknown_not_new_session(self):
        first = self.dispatch()
        self.finish(first["attempt_id"])
        self.client.fail_prompt = True
        with self.assertRaises(ActionError) as raised:
            self.app.rework(self.task["id"], feedback="retry")
        self.assertEqual(raised.exception.code, "dispatch_unknown")
        att = self.ledger.get_attempt(raised.exception.detail["attempt_id"])
        self.assertEqual(att["generation"], 2)
        self.assertEqual(att["session_id"], first["session_id"])
        self.assertEqual(len(self.client.sessions), 1)

    def test_unreachable_shell_query_cannot_release_or_rework(self):
        first = self.dispatch()
        self.finish(first["attempt_id"])
        self.client.fail_shells = True
        with self.assertRaises(LedgerError) as raised:
            self.app.rework(self.task["id"], feedback="fix")
        self.assertEqual(raised.exception.code, "SITE_UNCONFIRMED")
        self.assertIsNone(self.ledger.get_attempt(first["attempt_id"])["site_released_at"])
        self.assertEqual(len(self.ledger.list_attempts()), 1)

    def test_cancel_preserves_intent_after_interrupt_response_lost(self):
        first = self.dispatch()
        result = self.app.cancel(self.task["id"], interrupt=True, reason="manual")
        self.assertEqual(result["task"]["status"], "cancelled")
        att = self.ledger.get_attempt(first["attempt_id"])
        self.assertEqual(att["status"], "stop_requested")
        self.assertIsNone(att["site_released_at"])
        self.assertEqual(self.app.cancel(self.task["id"], interrupt=True), result)

    def second_application(self):
        conn = init(self.home)
        self.addCleanup(conn.close)
        ledger = Ledger(conn, self.home, clock=self.clock)
        client = Client(conn)
        return application(ledger, lambda: client, load_config(self.home), clock=self.clock), ledger

    def test_late_dispatch_confirmation_cannot_rewind_current_or_status(self):
        other, ledger = self.second_application()
        newer = {}
        prompt = self.client.prompt

        def interleave(sid, **payload):
            # 真正已飞出的POST晚到确认窗口：新的前置current守卫现在会在
            # create之后、prompt之前拒绝旧尝试，不能把那个窗口冒充在途确认。
            newer.update(other.dispatch(self.task["id"], directory=self.home + "-new",
                                        coordinator="ses_coord", allow_active=True))
            ledger.transition_attempt(newer["attempt_id"], "running")
            return prompt(sid, **payload)

        with patch.object(self.client, "prompt", side_effect=interleave):
            self.dispatch()
        task = self.ledger.get_task(self.task["id"])
        self.assertEqual(task["current_attempt_id"], newer["attempt_id"])
        self.assertEqual(task["status"], "running")

    def test_new_current_before_prompt_refuses_old_dispatch(self):
        other, ledger = self.second_application()
        newer = {}
        create = self.client.create_session

        def interleave(**spec):
            newer.update(other.dispatch(self.task["id"], directory=self.home + "-new",
                                        coordinator="ses_coord", allow_active=True))
            ledger.transition_attempt(newer["attempt_id"], "running")
            return create(**spec)

        with patch.object(self.client, "create_session", side_effect=interleave):
            with self.assertRaises(ActionError):
                self.dispatch()
        task = self.ledger.get_task(self.task["id"])
        self.assertEqual(task["current_attempt_id"], newer["attempt_id"])
        self.assertEqual(task["status"], "running")
        self.assertEqual(self.client.prompts, [], "旧尝试不能在新current提交后再发prompt")

    def test_late_rework_confirmation_cannot_rewind_current(self):
        first = self.dispatch()
        self.finish(first["attempt_id"])
        other, ledger = self.second_application()
        newer = {}
        prompt = self.client.prompt

        def interleave(sid, **payload):
            newer.update(other.dispatch(self.task["id"], directory=self.home + "-new",
                                        coordinator="ses_coord", allow_active=True))
            ledger.transition_attempt(newer["attempt_id"], "running")
            return prompt(sid, **payload)

        with patch.object(self.client, "prompt", side_effect=interleave):
            self.app.rework(self.task["id"], feedback="fix")
        self.assertEqual(self.ledger.get_task(self.task["id"])["current_attempt_id"], newer["attempt_id"])

    def test_cancel_interrupts_all_active_attempts_without_limit(self):
        states = sorted(ATTEMPT_ACTIVE)
        for index in range(25):
            self.ledger.create_attempt(self.task["id"], directory=f"{self.home}/site-{index}",
                                        session_id=f"ses_{index}", status=states[index % len(states)])
        self.app.cancel(self.task["id"], interrupt=True)
        attempts = self.ledger.list_attempts(task_id=self.task["id"])
        self.assertEqual(len(self.client.interrupted), 25)
        self.assertTrue(all(a["stop_requested_at"] is not None for a in attempts))
        self.assertTrue(all(a["site_released_at"] is None for a in attempts))

    def test_cancel_is_persisted_before_interrupt_io(self):
        first = self.dispatch()

        def crash(_sid):
            self.assertEqual(self.ledger.get_task(self.task["id"])["status"], "cancelled")
            self.assertIsNotNone(self.ledger.get_attempt(first["attempt_id"])["stop_requested_at"])
            raise RuntimeError("process terminated during interrupt")

        with patch.object(self.client, "interrupt", side_effect=crash):
            with self.assertRaisesRegex(RuntimeError, "process terminated"):
                self.app.cancel(self.task["id"], interrupt=True)

    def test_cancel_during_session_creation_does_not_send_prompt(self):
        create = self.client.create_session

        def interleave(**spec):
            result = create(**spec)
            self.app.cancel(self.task["id"], interrupt=True)
            return result

        with patch.object(self.client, "create_session", side_effect=interleave):
            with self.assertRaises(ActionError):
                self.dispatch()
        att = self.ledger.list_attempts(task_id=self.task["id"])[0]
        self.assertEqual(self.ledger.get_task(self.task["id"])["status"], "cancelled")
        self.assertEqual(att["status"], "stop_requested")
        self.assertEqual(self.client.prompts, [])

    def test_late_prompt_response_preserves_terminal_and_manual_control(self):
        for control in ("terminal_succeeded", "paused", "cancelled"):
            with self.subTest(control=control):
                task = self.ledger.create_task(project="p", title=control, coordinator_session="ses_coord")
                prompt = self.client.prompt

                def interleave(sid, **payload):
                    result = prompt(sid, **payload)
                    aid = payload["metadata"]["oc_orch_attempt"]
                    if control == "terminal_succeeded":
                        self.finish(aid)
                    elif control == "paused":
                        self.app.pause(task["id"])
                    else:
                        self.app.cancel(task["id"], interrupt=True)
                    return result

                with patch.object(self.client, "prompt", side_effect=interleave):
                    result = self.app.dispatch(task["id"], directory=f"{self.home}/{control}", coordinator="ses_coord")
                att = self.ledger.get_attempt(result["attempt_id"])
                expected = "result_pending" if control == "terminal_succeeded" else control
                self.assertEqual(self.ledger.get_task(task["id"])["status"], expected)
                if control == "cancelled":
                    self.assertEqual(att["status"], "stop_requested")
                elif control == "terminal_succeeded":
                    self.assertEqual(att["status"], control)
                self.assertEqual(att["dispatch_state"], "confirmed")

    def test_late_unknown_cannot_overwrite_confirmed_dispatch(self):
        result = self.dispatch()
        before = self.ledger.get_attempt(result["attempt_id"])
        after = self.ledger.record_dispatch(result["attempt_id"], "unknown")
        self.assertEqual(after, before)

    def test_collect_then_verify_and_repeated_intent(self):
        first = self.dispatch(deliverables=["out.txt"])
        Path(self.home, "out.txt").write_text("done")
        self.finish(first["attempt_id"])
        collected = self.app.collect(first["attempt_id"])
        self.assertEqual(collected["status"], "result_collected")
        accepted = self.app.verify(first["attempt_id"], passed=True, evidence=["reviewed"])
        self.assertEqual(accepted["task"]["status"], "accepted")
        self.assertEqual(accepted["verification"]["classification"], "normal_effective")
        again = self.app.verify(first["attempt_id"], passed=True)
        self.assertTrue(again["already_accepted"])
        self.assertFalse(again["changed"])

    def test_forced_acceptance_does_not_release_active_children(self):
        first = self.dispatch()
        self.finish(first["attempt_id"])
        att = self.ledger.get_attempt(first["attempt_id"])
        self.ledger.update_attempt(att["id"], fields={"metadata": {**att["metadata"], "active_children": ["ses_child"]}})
        self.client.sessions["ses_child"] = {"id": "ses_child", "parentID": att["session_id"]}
        self.client.active["ses_child"] = self.client.sessions["ses_child"]
        out = self.app.verify(att["id"], passed=True, force=True, reason="manual exception")
        self.assertEqual(out["verification"]["classification"], "forced")
        self.assertIsNone(out["attempt"]["site_released_at"])
        other = self.ledger.create_task(project="p", title="overlap")
        with self.assertRaises(LedgerError) as raised:
            self.ledger.create_attempt(other["id"], directory=self.home)
        self.assertEqual(raised.exception.code, "SITE_BUSY")

    def test_business_terminal_is_not_write_site_release(self):
        for status in ("accepted", "verification_failed", "cancelled", "lost"):
            with self.subTest(status=status):
                site = str(Path(self.home, status))
                task = self.ledger.create_task(project="p", title=status)
                att = self.ledger.create_attempt(
                    task["id"], directory=site, status="dispatched",
                    session_id=f"ses_business_{status}", dispatch_msg_id=f"msg_business_{status}")
                if status in ("accepted", "verification_failed"):
                    self.finish(att["id"])
                    self.client.fail_shells = True
                    self.app.verify(att["id"], passed=status == "accepted", force=True,
                                    reason="intent does not confirm external stop")
                else:
                    self.ledger.transition_attempt(att["id"], status)
                other = self.ledger.create_task(project="p", title="new owner")
                with self.assertRaises(LedgerError) as raised:
                    self.ledger.create_attempt(other["id"], directory=site)
                self.assertEqual(raised.exception.code, "SITE_BUSY")
                self.ledger.release_site(att["id"], confirmed=True, external_stopped=True, actor="test")
                new = self.ledger.create_attempt(other["id"], directory=site)
                self.assertEqual(new["task_id"], other["id"])


if __name__ == "__main__":
    unittest.main()

"""父协调者闭环验收：真实 collect 后补发的只读核收必须重新形成待办。"""
from pathlib import Path
import json
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.client import TransportError
from oc_orch.composition import application
from oc_orch.config import DEFAULTS
from oc_orch.domain.errors import ActionError, LedgerError
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock


class ContinuationEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="s4-real-collection-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_coord")
        self.cfg = {**DEFAULTS, "auto_collect": False}
        self.sup = Supervisor(self.home, cfg=self.cfg, client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.task = self.ledger.create_task(
            project="s4-e2e", title="carry cleanup", coordinator_session="ses_coord",
            write_scope=[self.home])
        self.app = application(self.ledger, lambda: self.client, self.cfg, clock=self.clock)
        self.dispatch = self.app.dispatch(
            self.task["id"], directory=self.home, coordinator="ses_coord",
            deliverables=["result.txt"], prompt="write result")
        self.aid = self.dispatch["attempt_id"]
        self.sid = self.dispatch["session_id"]
        start = self.clock.now_ms()
        self.client.deliver_inbox(self.sid, at=start)
        self.sup.ingest_event({"id": "evt_original_start",
                               "type": "session.execution.started",
                               "created": start, "data": {"sessionID": self.sid}})
        self.clock.advance(wall=30)
        end = self.clock.now_ms()
        self.client.append_message(self.sid, {
            "id": "msg_original_answer", "type": "assistant", "finish": "stop",
            "time": {"created": start + 1, "completed": end - 1},
            "content": [{"type": "text", "text": "done"}]})
        self.client.add_idle(self.sid, outcome="succeeded", at=end)
        self.client.set_session(self.sid, outcome="succeeded",
                                time_={"busy": start, "idle": end, "updated": end})
        Path(self.home, "result.txt").write_text("immutable result")
        self.sup.cycle(dispatch=False)
        self.assertEqual(self.ledger.get_attempt(self.aid)["status"], "terminal_succeeded")
        self.app.collect(self.aid)
        self.assertEqual(self.ledger.get_task(self.task["id"])["status"], "result_received")
        self.assertIsNotNone(self.ledger.get_attempt(self.aid)["site_released_at"])
        old = self.ledger.list_notifications(target_session="ses_coord")
        self.ledger.ack_notifications([n["id"] for n in old], actor="test-old-ack")
        from oc_orch.composition import make_managed_inputs
        self.managed = make_managed_inputs(
            self.sup.conn, self.home, client=lambda: self.client, clock=self.clock)

    def send_followup(self, *, deadline_ms=60000):
        return self.managed.send_tracked(
            self.sid, text="只读核对 carry 声明；缺材料则回报并等待协调者",
            message_id="msg_followup", owner="coordinator",
            task_id=self.task["id"], target_session="ses_coord",
            deadline=self.clock.now_ms() + deadline_ms, work_kind="observe",
            materials={"baseline": self.ledger.get_attempt(self.aid)["metadata"]["execution"]["baseline"]["path"]})

    def finish_followup(self, *, foreign=False, report=None):
        self.clock.advance(wall=100)
        start = self.clock.now_ms()
        self.client.deliver_inbox(self.sid, at=start)
        if foreign:
            self.client.append_message(self.sid, {
                "id": "msg_unrelated", "type": "user", "text": "other work",
                "time": {"created": start + 1}})
        self.client.append_message(self.sid, {
            "id": "msg_followup_answer", "type": "assistant", "finish": "stop",
            "time": {"created": start + 2, "completed": start + 20},
            "content": [{"type": "text",
                         "text": (json.dumps(report, ensure_ascii=False) if report is not None
                                  else "找不到当前 baseline，请协调者提供材料。")}]})
        self.clock.advance(wall=30)
        end = self.clock.now_ms()
        self.client.add_idle(self.sid, outcome="succeeded", at=end)
        self.client.set_session(self.sid, outcome="succeeded",
                                time_={"busy": start, "idle": end, "updated": end})

    def pending_continuations(self):
        return [n for n in self.ledger.list_notifications(
            target_session="ses_coord", limit=100)
            if n["kind"] == "continuation_ready"]

    def test_ack_then_worker_succeeded_with_unknown_business_returns_review(self):
        sealed = self.ledger.get_attempt(self.aid)
        self.send_followup()
        self.finish_followup()
        self.sup.cycle(dispatch=False)
        self.assertEqual(len(self.pending_continuations()), 1)
        self.assertEqual(self.pending_continuations()[0]["state"], "pending")
        self.assertEqual(self.ledger.get_task(self.task["id"])["status"], "result_received")
        current = self.ledger.get_attempt(self.aid)
        self.assertIsNone(current["verification"])
        self.assertEqual(current["terminal_at"], sealed["terminal_at"])
        self.assertEqual(current["metadata"]["execution"]["terminal"],
                         sealed["metadata"]["execution"]["terminal"])
        self.assertEqual(current["result_path"], sealed["result_path"])

    def test_session_old_succeeded_cannot_complete_new_input(self):
        self.send_followup()
        self.sup.cycle(dispatch=False)
        self.assertEqual(self.pending_continuations(), [])
        self.assertTrue(any(i["id"] == "msg_followup" for i in self.client.inbox(self.sid)))

    def test_reopen_rest_recovers_end_without_any_managed_sse(self):
        self.send_followup()
        self.finish_followup()
        other = Supervisor(self.home, cfg=self.cfg, client=self.client, clock=self.clock)
        self.addCleanup(other.conn.close)
        other.cycle(dispatch=False)
        rows = self.pending_continuations()
        self.assertEqual(len(rows), 1)
        other.cycle(dispatch=False)
        self.assertEqual([r["id"] for r in self.pending_continuations()], [rows[0]["id"]])

    def test_foreign_input_in_execution_window_is_not_claimed_as_ours(self):
        self.send_followup()
        self.finish_followup(foreign=True)
        self.sup.cycle(dispatch=False)
        self.assertEqual(self.pending_continuations(), [])

    def test_query_failure_preserves_wait_without_resending_worker(self):
        self.send_followup()
        calls = len(self.client.prompt_calls)
        self.finish_followup()
        self.client.fail("message_page", TransportError("/messages", "unknown"), times=5)
        self.sup.cycle(dispatch=False)
        self.assertEqual(self.pending_continuations(), [])
        self.assertEqual(len(self.client.prompt_calls), calls)

    def test_missing_responsibility_after_all_acks_is_visible_in_two_cycles(self):
        self.sup.cycle(dispatch=False)
        self.sup.cycle(dispatch=False)
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        rows = ResponsibilityStore(self.ledger).list_open_responsibilities(task_id=self.task["id"])
        self.assertTrue(rows, "Task未完成且通知全ACK仍应有明确责任")
        self.assertTrue(all(r.get("owner") and r.get("next_action") for r in rows))

    def test_unprocessed_review_does_not_republish_each_cycle_after_ack(self):
        self.send_followup()
        self.finish_followup()
        self.sup.cycle(dispatch=False)
        rows = self.pending_continuations()
        self.assertEqual(len(rows), 1)
        self.ledger.ack_notifications([rows[0]["id"]], actor="test")
        for _ in range(4):
            self.clock.advance(wall=1000)
            self.sup.cycle(dispatch=False)
        after = self.pending_continuations()
        self.assertEqual([r["id"] for r in after], [rows[0]["id"]])
        self.assertEqual(after[0]["state"], "acked")

    def test_sealed_attempt_default_send_requires_explicit_read_only_intent(self):
        calls = len(self.client.prompt_calls)
        with self.assertRaises((ActionError, LedgerError)):
            self.managed.send_tracked(
                self.sid, text="continue", task_id=self.task["id"],
                owner="coordinator", target_session="ses_coord",
                deadline=self.clock.now_ms() + 60000)
        self.assertEqual(len(self.client.prompt_calls), calls)

    def test_write_cannot_use_released_attempt_via_send(self):
        original = self.ledger.get_attempt(self.aid)
        calls = len(self.client.prompt_calls)
        with self.assertRaises((ActionError, LedgerError)):
            self.managed.send_tracked(
                self.sid, text="fix carry file", task_id=self.task["id"],
                owner="coordinator", target_session="ses_coord",
                work_kind="write", deadline=self.clock.now_ms() + 60000)
        self.assertEqual(len(self.client.prompt_calls), calls)
        current = self.ledger.get_attempt(self.aid)
        for key in ("status", "terminal_at", "site_released_at", "result_path"):
            self.assertEqual(current[key], original[key])

    def test_missing_material_reference_is_available_in_original_input_record(self):
        self.send_followup()
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        rows = ResponsibilityStore(self.ledger).list_open_responsibilities(task_id=self.task["id"])
        baseline = self.ledger.get_attempt(self.aid)["metadata"]["execution"]["baseline"]["path"]
        self.assertTrue(rows)
        self.assertIn(baseline, str(rows), "材料引用不能只存在于调用者短期内存")

    def test_execution_receipt_is_produced_independently_of_business_ack(self):
        sent = self.send_followup()
        self.finish_followup()
        self.sup.cycle(dispatch=False)
        receipt = self.managed.get(sent["input"]["input_id"])["input"]
        self.assertEqual(receipt["delivery"], "delivered")
        self.assertEqual(receipt["execution"], "succeeded")
        self.assertNotEqual(receipt["business_ack"], "acknowledged")
        self.assertIsNone(self.ledger.get_attempt(self.aid)["verification"])

    def test_healthy_execution_wait_does_not_exhaust_failure_budget(self):
        sent = self.send_followup(deadline_ms=3600000)
        self.clock.advance(wall=100)
        start = self.clock.now_ms()
        self.client.deliver_inbox(self.sid, at=start)
        self.client.set_session(self.sid, outcome=None, time_={"busy": start}, active=True)
        for _ in range(5):
            self.sup.cycle(dispatch=False)
            self.clock.advance(wall=60000)
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        resp = ResponsibilityStore(self.ledger).get_responsibility(sent["responsibility"]["id"])
        self.assertNotEqual(resp["state"], "waiting_human",
                            "健康执行等待不能按失败重试耗尽预算")
        self.assertLess(resp["budget_used"], resp["max_budget"])
        self.assertEqual(self.pending_continuations(), [])

    def test_legacy_execute_cannot_bypass_sealed_attempt_work_intent(self):
        self.managed.register_watch(self.sid, owner="coordinator",
                                    project=self.task["project"], max_pending_age_ms=60000)
        calls = len(self.client.prompt_calls)
        with self.assertRaises((ActionError, LedgerError)):
            self.managed.send(self.sid, intent="execute", text="fix files",
                              owner="coordinator", request_id="legacy-bypass",
                              project=self.task["project"])
        self.assertEqual(len(self.client.prompt_calls), calls)

    def business_report(self, sent, kind="needs_material"):
        inp = sent["input"]
        return {"schema": "oc-orch.continuation.v1",
                "input_id": inp["input_id"], "session_id": self.sid,
                "message_id": inp["message_id"], "task_id": self.task["id"],
                "attempt_id": self.aid, "generation": inp["generation"],
                "result_version": inp["result_version"], "kind": kind,
                "next_action": "协调者提供当前 baseline 引用",
                "missing_materials": ["current-baseline"],
                "evidence_refs": ["worker:scoped-input-report"]}

    def test_collected_followup_binds_actual_result_version(self):
        sent = self.send_followup()
        expected = self.ledger.get_attempt(self.aid)["metadata"]["result"]["version"]
        self.assertTrue(expected)
        self.assertEqual(sent["responsibility"]["result_version"], expected)
        self.assertEqual(sent["input"]["result_version"], expected)

    def test_structured_missing_material_returns_distinct_coordinator_action(self):
        sent = self.send_followup()
        report = self.business_report(sent)
        self.finish_followup(report=report)
        self.sup.cycle(dispatch=False)
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        resp = ResponsibilityStore(self.ledger).get_responsibility(sent["responsibility"]["id"])
        self.assertEqual(resp["evidence"]["continuation"]["kind"], "needs_material")
        self.assertEqual(resp["next_action"], report["next_action"])
        self.assertIn("current-baseline", str(resp["evidence"]))
        rows = self.pending_continuations()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload"]["classification"], "NEEDS_MATERIAL")
        self.assertIs(rows[0]["payload"]["auto_accept"], False)
        self.assertIsNone(self.ledger.get_attempt(self.aid)["verification"])

    def test_structured_completion_still_requires_verify_and_business_ack(self):
        sent = self.send_followup()
        self.finish_followup(report=self.business_report(sent, "completed"))
        self.sup.cycle(dispatch=False)
        rows = self.pending_continuations()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload"]["classification"], "COMPLETED_REQUIRES_REVIEW")
        self.assertEqual(self.ledger.get_task(self.task["id"])["status"], "result_received")
        self.assertNotEqual(self.managed.get(sent["input"]["input_id"])["input"]["business_ack"],
                            "acknowledged")

    def test_report_for_another_input_cannot_claim_our_business_result(self):
        sent = self.send_followup()
        report = {**self.business_report(sent), "message_id": "msg_somebody_else"}
        self.finish_followup(report=report)
        self.sup.cycle(dispatch=False)
        rows = self.pending_continuations()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload"]["classification"], "AWAITING_REVIEW")
        self.assertIsNone(self.ledger.get_attempt(self.aid)["verification"])

    def test_explicit_false_business_ack_is_not_acknowledged(self):
        sent = self.send_followup()
        self.finish_followup()
        self.sup.cycle(dispatch=False)
        iid = sent["input"]["input_id"]
        self.managed.record_managed_observation(
            iid, business_ack={"value": False}, via="independent:negative-ack")
        self.assertNotEqual(self.managed.get(iid)["input"]["business_ack"], "acknowledged")
        self.assertEqual(self.ledger.get_task(self.task["id"])["status"], "result_received")

    def test_pending_result_notification_prevents_duplicate_missing_diagnosis(self):
        self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord",
            dedupe_key="result-pending-for-review", payload={"summary": "pending result"},
            project=self.task["project"], task_id=self.task["id"], attempt_id=self.aid)
        self.sup.cycle(dispatch=False)
        self.sup.cycle(dispatch=False)
        missing = [n for n in self.ledger.list_notifications(target_session="ses_coord")
                   if n["kind"] == "responsibility_missing"]
        self.assertEqual(missing, [], "仍有待投递待办时不得另建缺失责任通知")

    def test_superseded_followup_becomes_visible_manual_wait(self):
        sent = self.send_followup()
        self.ledger.create_attempt(self.task["id"], directory=self.home,
                                   session_id="ses_new_worker", status="running")
        self.finish_followup()
        self.sup.cycle(dispatch=False)
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        resp = ResponsibilityStore(self.ledger).get_responsibility(sent["responsibility"]["id"])
        self.assertEqual(resp["state"], "waiting_human")
        self.assertIsNone(resp["closed_at"])
        self.assertTrue(resp["next_action"])
        self.assertEqual(self.pending_continuations(), [])

    def test_export_retains_input_and_responsibility_identity(self):
        sent = self.send_followup()
        exported = self.ledger.export()
        self.assertIn(sent["input"]["input_id"],
                      [r["input_id"] for r in exported["session_inputs"]])
        self.assertIn(sent["responsibility"]["id"],
                      [r["id"] for r in exported["responsibilities"]])

    def test_successful_flush_is_not_reported_as_expired_park(self):
        self.managed.register_watch("ses_external", owner="operator", project="outside",
                                    max_pending_age_ms=60000)
        sent = self.managed.send(
            "ses_external", intent="park", text="deferred note", owner="operator",
            project="outside", request_id="park-then-flush",
            expires_at=self.clock.now_ms() + 1000, flush_condition="owner approves")
        iid = sent["input"]["input_id"]
        self.managed.flush(iid, owner="operator", confirm=iid,
                           expected_revision=sent["input"]["revision"], reason="approved")
        self.clock.advance(wall=2000)
        observed = self.managed.observe_registered(allow_remote=False)
        self.assertFalse(any(a.get("kind") == "park-expired" for a in observed["alerts"]))
        receipt = self.managed.get(iid)["input"]
        self.assertTrue(receipt["resume"])
        self.assertNotIn("park 已到期", receipt["next_action"])

    def test_inactive_overdue_execute_recovers_original_input_at_most_once(self):
        sent = self.send_followup(deadline_ms=3600000)
        original_prompts = len(self.client.prompt_calls)
        self.client.scheduled_sessions.clear()
        self.client.clear_active()
        self.clock.advance(wall=600000)
        self.sup.cycle(dispatch=False)
        queued = self.client.inbox(self.sid)
        self.assertEqual([m["id"] for m in queued], [sent["input"]["message_id"]])
        self.assertEqual(queued[0]["delivery"], "steer")
        self.assertIn(self.sid, self.client.scheduled_sessions)
        self.assertEqual(len(self.client.prompt_calls), original_prompts)
        self.assertEqual(self.managed.get(sent["input"]["input_id"])["input"]["execution"],
                         "unknown", "恢复受理不能当作已经执行")
        # 模拟成功恢复后服务仍未执行；新进程不能再无界 steer。
        from unittest.mock import patch
        self.client.scheduled_sessions.clear()
        self.clock.advance(wall=600000)
        other = Supervisor(self.home, cfg=self.cfg, client=self.client, clock=self.clock)
        self.addCleanup(other.conn.close)
        with patch.object(self.client, "update_inbox_delivery",
                          wraps=self.client.update_inbox_delivery) as write:
            other.cycle(dispatch=False)
            write.assert_not_called()
        self.assertEqual(len(self.client.prompt_calls), original_prompts)

    def test_overdue_execute_with_foreign_queue_keeps_visible_duty(self):
        sent = self.send_followup(deadline_ms=3600000)
        self.client.scheduled_sessions.clear()
        self.client.clear_active()
        queued = self.client.inbox(self.sid)
        self.client.set_inbox(self.sid, [{"id": "foreign-old-park", "type": "user",
                                         "delivery": "queue"}] + queued)
        calls = len(self.client.prompt_calls)
        self.clock.advance(wall=600000)
        self.sup.cycle(dispatch=False)
        self.assertNotIn(self.sid, self.client.scheduled_sessions)
        self.assertEqual(len(self.client.prompt_calls), calls)
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        resp = ResponsibilityStore(self.ledger).get_responsibility(sent["responsibility"]["id"])
        self.assertIsNone(resp["closed_at"])
        self.assertTrue(resp["owner"] and resp["next_action"])
        self.assertIn("foreign", str(resp["evidence"]).lower())

    def test_rework_reclaims_new_attempt_and_seals_baseline_before_prompt(self):
        old = self.ledger.get_attempt(self.aid)
        previous_prompt = self.client.prompt
        observed = []
        def verify_claim_before_send(session_id, **kwargs):
            task = self.ledger.get_task(self.task["id"])
            att = self.ledger.get_attempt(task["current_attempt_id"])
            self.assertNotEqual(att["id"], old["id"])
            self.assertIsNone(att["site_released_at"])
            self.assertTrue(att["metadata"]["execution"]["baseline"]["path"])
            self.assertTrue(Path(att["metadata"]["execution"]["baseline"]["path"]).is_file())
            observed.append(att["id"])
            return previous_prompt(session_id, **kwargs)
        from unittest.mock import patch
        with patch.object(self.client, "prompt", side_effect=verify_claim_before_send):
            revised = self.app.rework(self.task["id"], feedback="修改 result.txt 并重新回报")
        self.assertEqual(observed, [revised["attempt_id"]])
        after = self.ledger.get_attempt(self.aid)
        for key in ("status", "terminal_at", "result_path", "site_released_at"):
            self.assertEqual(after[key], old[key])
        self.assertEqual(after["metadata"]["result"], old["metadata"]["result"])

    def test_verified_task_closes_business_duty_without_claiming_worker_stopped(self):
        sent = self.send_followup(deadline_ms=3600000)
        checked = self.app.verify(self.aid, passed=True)
        self.assertEqual(checked["verification"]["classification"], "normal_effective")
        self.sup.cycle(dispatch=False)
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        resp = ResponsibilityStore(self.ledger).get_responsibility(sent["responsibility"]["id"])
        self.assertIsNotNone(resp["closed_at"])
        self.assertEqual(resp["state"], "done")
        self.assertTrue(any(m["id"] == sent["input"]["message_id"]
                            for m in self.client.inbox(self.sid)))

    def test_published_continuation_rechecks_result_version_before_sending(self):
        self.send_followup()
        self.finish_followup()
        self.sup.cycle(dispatch=False)
        rows = self.pending_continuations()
        self.assertEqual(len(rows), 1)
        previous = self.ledger.get_attempt(self.aid)
        original_version = previous["metadata"]["result"]["version"]
        revised_meta = json.loads(json.dumps(previous["metadata"]))
        revised_meta["result"]["version"] = "rv-next-collected-result"
        # 构造发布/发送之间的已提交新版本；不改 current，以覆盖同 attempt。
        self.ledger.conn.execute(
            "UPDATE attempts SET metadata=? WHERE id=?",
            (json.dumps(revised_meta), self.aid))
        calls = len(self.client.prompt_calls)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), calls)
        self.assertNotEqual(self.ledger.get_notification(rows[0]["id"])["state"], "sent")
        self.assertEqual(rows[0]["payload"]["result_version"], original_version)

    def test_explicit_business_ack_revision_blocks_late_negative_confirmation(self):
        sent = self.send_followup()
        self.finish_followup()
        self.sup.cycle(dispatch=False)
        iid = sent["input"]["input_id"]
        before = self.managed.get(iid)["input"]
        confirmed = self.managed.acknowledge(
            iid, value=True, owner="coordinator", confirm=iid,
            expected_revision=before["revision"], reason="核收完成")
        self.assertEqual(confirmed["input"]["business_ack"], "acknowledged")
        with self.assertRaises((LedgerError, ActionError)):
            self.managed.acknowledge(
                iid, value=False, owner="coordinator", confirm=iid,
                expected_revision=before["revision"], reason="过期窗口回报")
        self.assertEqual(self.managed.get(iid)["input"]["business_ack"], "acknowledged")

    def test_crash_after_send_claim_preserves_original_identity_and_visible_deadline(self):
        from unittest.mock import patch
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        with patch.object(self.client, "prompt", side_effect=SystemExit("simulated crash")):
            with self.assertRaises(SystemExit):
                self.send_followup(deadline_ms=60000)
        registered = self.managed.list_inputs(session_id=self.sid)["inputs"]
        self.assertEqual(len(registered), 1)
        original = registered[0]
        self.assertEqual(original["message_id"], "msg_followup")
        duties = ResponsibilityStore(self.ledger).list_open_responsibilities(
            task_id=self.task["id"])
        self.assertEqual(len(duties), 1)
        self.assertEqual(duties[0]["input_id"], original["input_id"])
        calls = len(self.client.prompt_calls)
        self.clock.advance(wall=60001)
        other = Supervisor(self.home, cfg=self.cfg, client=self.client, clock=self.clock)
        self.addCleanup(other.conn.close)
        other.cycle(dispatch=False)
        after = ResponsibilityStore(other.ledger).get_responsibility(duties[0]["id"])
        self.assertEqual(after["state"], "waiting_human")
        self.assertEqual(after["input_id"], original["input_id"])
        self.assertEqual(after["message_id"], original["message_id"])
        self.assertEqual(after["deadline"], duties[0]["deadline"])
        self.assertTrue(after["owner"] and after["next_action"])
        self.assertIsNone(after["closed_at"])
        self.assertEqual(len(self.client.prompt_calls), calls)


if __name__ == "__main__":
    unittest.main()

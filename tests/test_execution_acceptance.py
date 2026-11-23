"""主协调者发现的执行归属反例：三路活动快照及定点输入身份。"""
from unittest.mock import patch

from tests.test_completion_integrity import IncidentBase
from oc_orch.domain import execution, results as result_rules


class ExecutionAcceptanceTests(IncidentBase):
    def running(self):
        task = self.make_task()
        self.client.set_session("ses_w", time_={}, location_dir=self.tmp)
        out = self.app.dispatch(task["id"], directory=self.tmp, session="ses_w",
                                coordinator="ses_c", prompt="执行")
        self.client.deliver_inbox("ses_w", at=1000000)
        self.start_event("ses_w", 1000000, "start-current")
        return self.ledger.get_attempt(out["attempt_id"])

    def test_sse_newer_busy_and_missing_session_do_not_consume_terminal(self):
        att = self.running()
        self.completed_session("ses_w", outcome="succeeded", busy=1001100,
                               idle=1001000, active=False)
        self.terminal_event("ses_w", "succeeded", 1001000, "end-stale")
        self.assertNoFalseTerminal(att["id"])
        self.assertEqual(self.ledger.get_attempt(att["id"])["metadata"]["execution"]
                         ["unconfirmed"]["reason"], "newer-busy-after-idle")
        with patch.object(self.client, "get_session", return_value=None):
            self.terminal_event("ses_w", "succeeded", 1002000, "end-without-session")
        self.assertNoFalseTerminal(att["id"])

    def test_point_input_wrong_id_type_or_session_is_not_execution_evidence(self):
        att = self.running()
        self.completed_session("ses_w", outcome="succeeded", busy=1000000,
                               idle=1001000, active=False)
        message = {"id": att["dispatch_msg_id"], "type": "user",
                   "sessionID": "ses_w", "time": {"created": 1000000}}
        for changes in ({"id": "unrelated"}, {"type": "assistant"},
                        {"sessionID": "other"}, {"time": "malformed"}):
            with self.subTest(changes=changes):
                with patch.object(self.client, "get_message", return_value={**message, **changes}):
                    self.sup.observe_attempt(self.ledger.get_attempt(att["id"]), {})
                self.assertNoFalseTerminal(att["id"])

    def test_missing_open_turn_does_not_bypass_old_baseline(self):
        task = self.make_task()
        att = self.ledger.create_attempt(task["id"], directory=self.tmp,
                                         session_id="ses_w", dispatch_state="confirmed")
        self.ledger.seal_execution(att["id"], mode="reuse",
                                   session_before={"idle": 1000000, "outcome": "succeeded"})
        for ended in (998500, 1000000):
            out = self.ledger.record_terminal(
                att["id"], outcome="succeeded", source="rest",
                candidate=execution.build_candidate(outcome="succeeded", source="rest",
                                                     session_id="ses_w", ended_at=ended))
            self.assertEqual(out["result"], "rejected")
            self.assertEqual(self.ledger.turns(att["id"]), [])
            self.assertNoFalseTerminal(att["id"])

    def test_transport_event_identity_does_not_change_result_fingerprint(self):
        att = self.running()
        kwargs = dict(attempt=att, message_ids=[att["dispatch_msg_id"]],
                      text_sha256=result_rules.sha256_text("完成"), deliverables=[])
        sse = result_rules.content_fingerprint(
            execution={"outcome": "succeeded", "end_at": 1001000,
                       "event_id": "sse-1", "source": "sse"}, **kwargs)
        rest = result_rules.content_fingerprint(
            execution={"outcome": "succeeded", "end_at": 1001000,
                       "event_id": "obs-1001000", "source": "rest"}, **kwargs)
        self.assertEqual(sse, rest)

    def test_event_routing_does_not_compare_generations_across_tasks(self):
        old = self.running()
        self.completed_session("ses_w", outcome="succeeded", busy=1000000,
                               idle=1001000, active=False)
        self.terminal_event("ses_w", "succeeded", 1001000, "old-end")
        self.assertIsNotNone(self.ledger.get_attempt(old["id"])["terminal_at"])
        self.clock._wall = 1100000
        task = self.make_task()
        out = self.app.dispatch(task["id"], directory=self.tmp, session="ses_w",
                                coordinator="ses_c", prompt="另一任务")
        current = self.ledger.get_attempt(out["attempt_id"])
        self.assertEqual(current["generation"], old["generation"])
        self.assertEqual(self.sup._attempt_for_event("ses_w", 1100000)["id"], current["id"])

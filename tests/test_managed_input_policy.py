"""S4 受管输入纯规则单测：意图校验、确认、闸门/队列分类与四态视图。

只测 domain/managed_inputs.py，零台账/零 Client。
"""

import unittest

from oc_orch.domain import managed_inputs as rules


class ValidateSendPayloadTests(unittest.TestCase):
    def test_execute_requires_only_base_fields(self):
        r = rules.validate_send_payload(
            "ses_a", intent="execute", text="go", owner="op", request_id="r1")
        self.assertTrue(r["valid"], r)

    def test_park_requires_expires_and_flush_condition(self):
        for extra in ({}, {"expires_at": 10}, {"flush_condition": "go"}):
            with self.subTest(extra=extra):
                r = rules.validate_send_payload(
                    "ses_a", intent="park", text="p", owner="op",
                    request_id="r1", **extra)
                self.assertFalse(r["valid"])
        ok = rules.validate_send_payload(
            "ses_a", intent="park", text="p", owner="op", request_id="r1",
            expires_at=10, flush_condition="confirm")
        self.assertTrue(ok["valid"], ok)

    def test_rejects_bad_intent_and_empty_identity(self):
        self.assertFalse(rules.validate_send_payload(
            "ses_a", intent="wat", text="x", owner="op", request_id="r1")["valid"])
        for field in ({"text": ""}, {"owner": " "}, {"request_id": ""},
                      {"session_id": ""}):
            base = {"session_id": "ses_a", "intent": "execute", "text": "x",
                    "owner": "op", "request_id": "r1"}
            base.update(field)
            with self.subTest(field=field):
                self.assertFalse(rules.validate_send_payload(**base)["valid"])

    def test_record_needs_no_park_fields(self):
        r = rules.validate_send_payload(
            "ses_any", intent="record", text="note", owner="op",
            request_id="r1")
        self.assertTrue(r["valid"], r)


class ValidateConfirmationTests(unittest.TestCase):
    def test_confirm_must_match_object(self):
        bad = rules.validate_confirmation(
            object_id="I-1", confirm="wrong", owner="op",
            expected_revision=1, reason="x")
        self.assertFalse(bad["valid"])
        ok = rules.validate_confirmation(
            object_id="I-1", confirm="I-1", owner="op",
            expected_revision=1, reason="x")
        self.assertTrue(ok["valid"], ok)

    def test_requires_reason_unless_optional(self):
        self.assertFalse(rules.validate_confirmation(
            object_id="s", confirm="s", owner="op", expected_revision=1,
            reason="")["valid"])
        self.assertTrue(rules.validate_confirmation(
            object_id="s", confirm="s", owner="op", expected_revision=1,
            reason="", require_reason=False)["valid"])

    def test_requires_positive_revision_and_owner(self):
        for rev in (0, -1, True, "1", None):
            with self.subTest(rev=rev):
                self.assertFalse(rules.validate_confirmation(
                    object_id="s", confirm="s", owner="op",
                    expected_revision=rev, reason="x")["valid"])
        self.assertFalse(rules.validate_confirmation(
            object_id="s", confirm="s", owner="", expected_revision=1,
            reason="x")["valid"])


class GatewayTests(unittest.TestCase):
    def test_none_and_probe_ready_allow(self):
        self.assertIsNone(rules.gateway_allows(None))
        self.assertIsNone(rules.gateway_allows({"state": "probe-ready"}))

    def test_any_block_state_fails_closed(self):
        for state in ("blocked", "manual-blocked", "exhausted",
                      "probe-outstanding", "probe-unknown", "weird"):
            with self.subTest(state=state):
                self.assertIsNotNone(rules.gateway_allows({"state": state}))
        self.assertIsNotNone(rules.gateway_allows({"gate": {}}))   # 缺 state
        self.assertIsNotNone(rules.gateway_allows("not-a-dict"))


class QueueClassificationTests(unittest.TestCase):
    def test_execute_allows_only_empty_queue(self):
        self.assertTrue(rules.execute_queue_allowed([])["allowed"])
        self.assertFalse(rules.execute_queue_allowed(
            [{"id": "msg_x"}])["allowed"])
        self.assertFalse(rules.execute_queue_allowed(
            [{"id": "msg_own"}], {"msg_own"})["allowed"])

    def test_flush_allows_unique_own_target(self):
        ok = rules.flush_queue_allowed([{"id": "msg_t"}], {"msg_t"}, "msg_t")
        self.assertTrue(ok["allowed"], ok)

    def test_flush_rejects_foreign_other_own_and_unknown(self):
        self.assertEqual(
            rules.flush_queue_allowed(
                [{"id": "msg_f"}, {"id": "msg_t"}], {"msg_t"}, "msg_t")["reason"],
            "foreign-queue")
        self.assertEqual(
            rules.flush_queue_allowed(
                [{"id": "msg_o"}], {"msg_o"}, "msg_t")["reason"],
            "other-own-queued")
        self.assertEqual(
            rules.flush_queue_allowed([{}], set(), "msg_t")["reason"],
            "unknown-shape-item")
        self.assertEqual(
            rules.flush_queue_allowed([], set(), "msg_t")["reason"],
            "target-absent")


class ReceiptViewTests(unittest.TestCase):
    def _row(self, *, intent="park", admission="admitted", block=None):
        return {"input_id": "MI-1", "session_id": "ses_a", "message_id": "m1",
                "intent": intent, "admission_state": admission,
                "owner": "op", "revision": 3, "created_at": 100,
                "metadata": {"managed_input": dict(block or {
                    "schema": 1, "request_id": "r1", "project": "p",
                    "resume": intent != "execute",
                    "expires_at": 50, "flush_condition": "c",
                    "evidence": {}})}}

    def test_record_views_are_not_applicable(self):
        view = rules.receipt_view(self._row(intent="record",
                                            admission="recorded"), now=1000)
        self.assertEqual(view["intent"], "record")
        self.assertEqual(view["delivery"], "not-applicable")
        self.assertEqual(view["execution"], "not-applicable")
        self.assertEqual(view["business_ack"], "not-applicable")

    def test_pending_derived_and_pending_age(self):
        view = rules.receipt_view(self._row(), now=400)
        self.assertEqual(view["delivery"], "pending")
        self.assertEqual(view["pending_age_ms"], 300)
        self.assertEqual(view["execution"], "unknown")
        self.assertEqual(view["business_ack"], "unacknowledged")
        self.assertNotIn("body", view)

    def test_delivered_evidence_maps_delivered(self):
        row = self._row(admission="delivered")
        row["metadata"]["managed_input"]["evidence"]["delivered"] = {"at": 1}
        view = rules.receipt_view(row, now=1000, include_body=True)
        self.assertEqual(view["delivery"], "delivered")
        self.assertEqual(view["pending_age_ms"], 0)
        self.assertIn("body", view)

    def test_cancelled_evidence_wins(self):
        row = self._row(admission="unknown")
        row["metadata"]["managed_input"]["evidence"]["cancelled"] = {"at": 1}
        self.assertEqual(rules.receipt_view(row, now=1)["delivery"], "cancelled")

    def test_old_session_outcome_not_used_for_execution(self):
        row = self._row()
        row["outcome"] = "succeeded"  # 旧 session 事实不得冒充本输入执行
        view = rules.receipt_view(row, now=400)
        self.assertEqual(view["execution"], "unknown")

    def test_expired_park_next_action_alert_only(self):
        row = self._row(admission="admitted")
        view = rules.receipt_view(row, now=999)
        self.assertIn("只告警", view["next_action"])

    def test_execution_reference_outcome_is_trusted_only_when_valid(self):
        row = self._row()
        row["metadata"]["managed_input"]["evidence"]["execution"] = {
            "detail": {"reference": {"outcome": "succeeded"}}}
        self.assertEqual(rules.receipt_view(row, now=1)["execution"], "succeeded")
        row["metadata"]["managed_input"]["evidence"]["execution"] = {
            "detail": {"reference": {"outcome": "started"}}}
        self.assertEqual(rules.receipt_view(row, now=1)["execution"], "unknown")

    def test_watch_view(self):
        view = rules.watch_view({"session_id": "s", "owner": "o", "project": "p",
                                 "max_pending_age_ms": 5, "enabled": True,
                                 "revision": 2})
        self.assertEqual(view["session_id"], "s")
        self.assertTrue(view["enabled"])
        self.assertEqual(view["revision"], 2)

    def test_watch_view_sanitizes_last_observation(self):
        row = {"session_id": "s", "owner": "o", "project": "p",
               "max_pending_age_ms": 1000, "enabled": True, "revision": 2,
               "last_observation": {
                   "session_id": "s", "owner": "o", "project": "p",
                   "trusted": True, "pending": 3, "oldest_age_ms": 4000,
                   "aged_out": True, "blocking_reason": "foreign-input:x",
                   "next_action": "转 owner 告警", "watch_revision": 2,
                   "body": "SECRET", "error": "raw traceback",
                   "observations": [{"secret": 1}]}}
        view = rules.watch_view(row)
        obs = view["last_observation"]
        self.assertNotIn("body", obs)
        self.assertNotIn("error", obs)
        self.assertNotIn("observations", obs)
        self.assertEqual(obs["pending"], 3)
        self.assertEqual(obs["oldest_age_ms"], 4000)
        self.assertTrue(obs["aged_out"])
        self.assertFalse(obs["stale"])
        self.assertTrue(obs["trusted"])

    def test_watch_view_marks_revoked_or_rev_changed_observation_stale(self):
        base = {"session_id": "s", "owner": "o", "project": "p",
                "max_pending_age_ms": 1, "revision": 3,
                "last_observation": {"trusted": True, "watch_revision": 2}}
        revoked = {**base, "enabled": False}
        obs = rules.watch_view(revoked)["last_observation"]
        self.assertTrue(obs["stale"])
        self.assertFalse(obs["trusted"])
        changed = {**base, "enabled": True}   # watch_revision 2 != revision 3
        obs = rules.watch_view(changed)["last_observation"]
        self.assertTrue(obs["stale"])
        self.assertFalse(obs["trusted"])

    def test_watch_view_last_observed_at_from_observation_at(self):
        row = {"session_id": "s", "enabled": True, "revision": 1,
               "observations": [{"_at": 111}, {"_at": 222}],
               "last_observation": {"trusted": True, "watch_revision": 1}}
        self.assertEqual(rules.watch_view(row)["last_observed_at"], 222)
        self.assertIsNone(rules.watch_view({"session_id": "s"})["last_observation"])

    def test_pending_condition_key_stable_and_age_free(self):
        a = rules.pending_condition_key(["m2", "m1"])
        b = rules.pending_condition_key(["m1", "m2"])
        self.assertEqual(a, b)
        self.assertNotEqual(a, rules.pending_condition_key(["m1"]))
        self.assertTrue(a.startswith("watch-pending:"))


if __name__ == "__main__":
    unittest.main()

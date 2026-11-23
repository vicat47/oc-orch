"""度量口径反例：不挑成功快单，force/派单后取消/缺失保分母，混合不冒领收益。"""
from copy import deepcopy
import unittest
from oc_orch.domain.delivery_metrics import report
from oc_orch.domain.errors import LedgerError


def cohort(count=4):
    return {"schema": "oc-orch.delivery-cohort.v1", "fixed_at": 0, "window": {"start": 10, "end": 1000},
        "conditions": {"model": "fixture/free", "fees": "user-zero"},
        "members": [{"task_id": f"T-{i}", "arm": "control", "scope": "standard"} for i in range(count)]}


def fact(number=0, **patch):
    return {"task_id": f"T-{number}", "source": "isolated ledger export", "dispatch_intent_at": 20,
            "verifications": [{"at": 100, "result": "pass", "classification": "normal_effective"}], **patch}


class DeliveryMetricsTests(unittest.TestCase):
    def test_failure_cancelled_unknown_missing_force_stay_denominator(self):
        out = report(cohort(5), [fact(), fact(1, verifications=[{"at": 90, "result": "pass", "classification": "forced"}]),
                               fact(2, outcome="cancelled", verifications=[]), fact(3, outcome="failed", verifications=[])])
        self.assertEqual(out["summary"]["delivery_rate"], {"numerator": 1, "denominator": 5, "value": .2})
        self.assertEqual(out["summary"]["unresolved_rate"]["numerator"], 4)
        self.assertEqual(out["summary"]["latency"]["median_ms"], 80)
        self.assertTrue(out["tasks"][-1]["missing_observation"])
        self.assertEqual(out["summary"]["unresolved_ages_ms"], [980, 980, 980, None])

    def test_zero_denominator_is_not_applicable(self):
        out = report(cohort(0), [])
        self.assertIsNone(out["summary"]["delivery_rate"]["value"])
        self.assertIsNone(out["switch_consistency"]["value"])
        self.assertIsNone(out["first_pass_rate"]["value"])

    def test_later_formal_failure_or_force_does_not_reuse_historical_pass(self):
        for classification, result in (("fail", "fail"), ("forced", "pass")):
            events = [{"at": 100, "result": "pass", "classification": "normal_effective"},
                      {"at": 200, "result": result, "classification": classification}]
            out = report(cohort(1), [fact(verifications=events)])
            self.assertEqual(out["summary"]["delivery_rate"]["numerator"], 0)

    def test_after_window_success_not_counted_first_fail_remains_first(self):
        out = report(cohort(2), [fact(verifications=[{"at": 2000, "result": "pass", "classification": "normal_effective"}]),
            fact(1, verifications=[{"at": 100, "result": "fail", "classification": "fail"},
                                   {"at": 200, "result": "pass", "classification": "normal_effective"}])])
        self.assertEqual(out["summary"]["delivery_rate"]["numerator"], 1)
        self.assertEqual(out["first_pass_rate"]["value"], 0)
        self.assertEqual(out["not_entered_verification"], 1)

    def test_mixed_policy_scope_separate_and_consistency_is_at_permission_time(self):
        c = cohort(2); c["members"][1]["scope"] = "prototype"
        out = report(c, [fact(permits=[{"at": 30, "revision": 1, "effective_revision": 1, "authorized": True},
            {"at": 60, "revision": 2, "effective_revision": 2, "authorized": True}], switches=[{"at": 50, "revision": 2}], refusals=3)])
        self.assertEqual(out["switch_consistency"]["value"], 1)
        self.assertEqual(out["switch_propagation"]["median_ms"], 10)
        self.assertEqual(out["refusals"], 3)
        self.assertIn("standard:control:mixed", out["groups"])
        self.assertIn("prototype:control:stable", out["groups"])
        self.assertFalse(out["speedup_proven"])

    def test_stale_or_unauthorized_permission_never_counts_as_consistent(self):
        out = report(cohort(1), [fact(permits=[{"at": 30, "revision": 1, "effective_revision": 2, "authorized": True},
            {"at": 40, "revision": 2, "effective_revision": 2, "authorized": False}], switches=[{"at": 50, "revision": 3}])])
        self.assertEqual(out["switch_consistency"]["value"], 0)
        self.assertEqual(out["unauthorized_or_stale_permits"], 2)
        self.assertEqual(out["switch_propagation"]["not_observed"], 1)

    def test_unknown_usage_not_zero_full_chain_or_invoice(self):
        out = report(cohort(1), [fact(usage=[{"component": "worker", "tokens": 12, "amount": 0,
            "currency": "CNY", "basis": "user-zero", "coverage": "input-only", "source": "user authorization"}])])
        self.assertEqual(out["usage"]["worker"]["known_tokens"], 12)
        self.assertIsNone(out["usage"]["worker"]["amount_per_effective_delivery"])
        self.assertEqual(out["usage"]["coordinator"]["unknown_tasks"], 1)

    def test_foreign_duplicate_outside_window_and_bad_types_reject_not_filter(self):
        for observations in ([fact(5)], [fact(), fact()], [fact(dispatch_intent_at=1)], [fact(dispatch_intent_at=True)],
                             [fact(dispatch_intent_at=None, verifications=[{"at": 1, "result": "pass", "classification": "normal_effective"}])],
                             [fact(verifications=[{"at": 1, "result": "pass", "classification": "normal_effective"}])]):
            with self.subTest(facts=observations), self.assertRaises(LedgerError): report(cohort(), observations)
        c = cohort(); c["members"].append(deepcopy(c["members"][0]))
        with self.assertRaises(LedgerError): report(c, [])

    def test_fixed_before_window_required_and_p90_nearest_rank(self):
        c = cohort(10)
        out = report(c, [fact(i, verifications=[{"at": 100 + i * 10, "result": "pass", "classification": "normal_effective"}]) for i in range(10)])
        self.assertEqual(out["summary"]["latency"]["p90_ms"], 160)
        c["fixed_at"] = 999
        with self.assertRaises(LedgerError): report(c, [])

    def test_interim_snapshot_preserves_original_window_cohort_and_denominator(self):
        c = cohort(2)
        out = report(c, [fact()], as_of=90)
        self.assertTrue(out["interim"])
        self.assertEqual(out["planned_window"], c["window"])
        self.assertEqual(out["summary"]["delivery_rate"]["denominator"], 2)
        self.assertEqual(out["summary"]["delivery_rate"]["numerator"], 0)

#!/usr/bin/env python3
"""provider-fault-failover 域层纯规则测试（F01–F08/F23/F24 的域部分 + 模型/预算）。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.domain.failover import (
    AUTO_FAILOVER_CATEGORIES,
    EPISODE_CONTINUATION_REQUESTED,
    EPISODE_OPEN,
    EPISODE_RESUMED,
    EPISODE_SWITCH_CONFIRMED,
    EPISODE_SWITCH_REQUESTED,
    EPISODE_SWITCH_UNKNOWN,
    HARD_CATEGORIES,
    RETRY_ONLY_CATEGORIES,
    can_transition,
    classify_fault_source,
    model_ref,
    model_ref_from_dict,
    models_equal,
    notify_update_allowed,
    route_key,
    switch_allowed,
    transients_exhausted,
)


def _err(status=None, message="", code=None, tag=None, provider=None, model=None):
    error = {}
    if status is not None:
        error["status"] = status
    if message:
        error["message"] = message
    if code:
        error["code"] = code
    if tag:
        error["tag"] = tag
    if provider or model:
        error["providerID"] = provider
        error["id"] = model
    return error


class TestClassifyFaultSource(unittest.TestCase):
    def test_402_insufficient_balance_is_quota_hard_auto(self):
        out = classify_fault_source({"error": _err(402, "Insufficient Balance")})
        self.assertEqual(out["category"], "quota")
        self.assertTrue(out["hard"])
        self.assertTrue(out["auto_failover"])
        self.assertFalse(out["transient"])

    def test_402_without_message_falls_back_to_quota(self):
        out = classify_fault_source({"error": _err(402)})
        self.assertEqual(out["category"], "quota")
        self.assertTrue(out["auto_failover"])

    def test_quota_code_structured(self):
        out = classify_fault_source(
            {"error": _err(403, "balance", code="insufficient_quota")})
        self.assertEqual(out["category"], "quota")
        self.assertTrue(out["auto_failover"])

    def test_429_rate_limit_is_transient_not_auto(self):
        out = classify_fault_source(
            {"error": _err(429, "rate limit", code="rate_limit_exceeded")})
        self.assertEqual(out["category"], "rate_limit")
        self.assertTrue(out["transient"])
        self.assertFalse(out["auto_failover"])
        self.assertNotIn(out["category"], AUTO_FAILOVER_CATEGORIES)

    def test_auth_is_hard_but_not_auto(self):
        # 认证/内容策略拒绝不是换模型能解决的；保持 hard 但不自动切换（F03）。
        out = classify_fault_source(
            {"error": _err(401, "invalid api key", code="invalid_api_key")})
        self.assertEqual(out["category"], "auth")
        self.assertTrue(out["hard"])
        self.assertFalse(out["auto_failover"])
        self.assertIn("auth", HARD_CATEGORIES)

    def test_unknown_403_content_policy_not_quota(self):
        # 403 内容策略拒绝：不应判为 quota 而触发切换。
        out = classify_fault_source(
            {"error": _err(403, "content policy violation")})
        self.assertEqual(out["category"], "auth")  # 复用现有 403→auth 语义
        self.assertFalse(out["auto_failover"])

    def test_model_unavailable_is_hard_auto(self):
        out = classify_fault_source(
            {"error": _err(503, "no available channel", code="no_available_channel")})
        self.assertEqual(out["category"], "model_unavailable")
        self.assertTrue(out["hard"])
        self.assertTrue(out["auto_failover"])

    def test_route_from_error_structure(self):
        out = classify_fault_source({
            "error": {"status": 402, "message": "Insufficient Balance",
                      "providerID": "deepseek", "id": "deepseek-flash"}})
        self.assertEqual(out["route"], "deepseek/deepseek-flash")
        self.assertEqual(out["route_provider"], "deepseek")
        self.assertEqual(out["route_model"], "deepseek-flash")
        self.assertFalse(out["unknown_route"])

    def test_route_from_verified_evidence_not_ledger_target(self):
        # g4 场景：台账误记为 yfzs，但调用方以真实证据提供 deepseek → 归 deepseek。
        out = classify_fault_source({
            "error": _err(402, "Insufficient Balance"),
            "route_provider": "deepseek", "route_model": "deepseek-flash"})
        self.assertEqual(out["route"], "deepseek/deepseek-flash")
        self.assertFalse(out["unknown_route"])

    def test_no_route_evidence_is_unknown_route(self):
        out = classify_fault_source({"error": _err(402, "Insufficient Balance")})
        self.assertTrue(out["unknown_route"])
        self.assertEqual(out["route"], "route_unknown")

    def test_dedupe_key_stable_and_distinguishable(self):
        a = classify_fault_source({"error": _err(402, "Insufficient Balance"),
                                   "event_id": "e1"})
        b = classify_fault_source({"error": _err(402, "Insufficient Balance"),
                                   "event_id": "e2"})
        self.assertEqual(a["dedupe_key"], b["dedupe_key"])
        self.assertNotEqual(a["origin_key"], b["origin_key"])
        c = classify_fault_source({"error": _err(429, "rate limit")})
        self.assertNotEqual(a["dedupe_key"], c["dedupe_key"])

    def test_retry_only_categories_separated(self):
        for cat in RETRY_ONLY_CATEGORIES:
            self.assertNotIn(cat, AUTO_FAILOVER_CATEGORIES)
        self.assertEqual(RETRY_ONLY_CATEGORIES, frozenset({"rate_limit", "server"}))


class TestModelIdentity(unittest.TestCase):
    def test_model_ref_requires_nonempty(self):
        self.assertEqual(model_ref("p", "m"), {"providerID": "p", "id": "m"})
        self.assertEqual(model_ref("p", "m", "v"),
                         {"providerID": "p", "id": "m", "variant": "v"})
        for provider, model in (("", "m"), ("p", ""), (None, "m"), ("p", None)):
            with self.assertRaises(ValueError):
                model_ref(provider, model)  # type: ignore[arg-type]

    def test_model_ref_from_dict_fail_closed(self):
        base = {"providerID": "p", "id": "m"}
        self.assertEqual(model_ref_from_dict(base), base)
        for bad in (None, {}, {"providerID": "p"}, {"id": "m"},
                    {"providerID": 1, "id": "m"}, {"providerID": "p", "id": ""},
                    {"providerID": "p", "id": "m", "variant": 3}):
            self.assertIsNone(model_ref_from_dict(bad), bad)

    def test_models_equal(self):
        a = {"providerID": "p", "id": "m"}
        self.assertTrue(models_equal(a, {"providerID": "p", "id": "m"}))
        self.assertFalse(models_equal(a, {"providerID": "q", "id": "m"}))
        self.assertFalse(models_equal(a, {"providerID": "p", "id": "n"}))
        self.assertFalse(models_equal(
            a, {"providerID": "p", "id": "m", "variant": "v"}))
        self.assertFalse(models_equal(None, a))
        self.assertFalse(models_equal(a, None))

    def test_route_key(self):
        self.assertEqual(route_key("p", "m"), "p/m")
        self.assertEqual(route_key("p", "m", "v"), "p/m#v")

    def test_base_route_strips_variant(self):
        from oc_orch.domain.failover import base_route

        self.assertEqual(base_route("p/m#v"), "p/m")
        self.assertEqual(base_route("p/m"), "p/m")


class TestBudgetAndTransients(unittest.TestCase):
    def test_transients_waiting_respects_trusted_retry_after(self):
        out = transients_exhausted(failure_count=1, first_at=1000, now=1100,
                                   window_ms=900_000, max_attempts=3,
                                   trusted_retry_after=60_000)
        self.assertEqual(out["decision"], "waiting")
        self.assertEqual(out["retry_at"], 61_000)

    def test_transients_exhausted_by_window(self):
        out = transients_exhausted(failure_count=1, first_at=1000, now=2000,
                                   window_ms=500, max_attempts=3)
        self.assertEqual(out["decision"], "exhausted")
        self.assertEqual(out["remaining"], 0)

    def test_transients_exhausted_by_attempts(self):
        out = transients_exhausted(failure_count=3, first_at=1000, now=1100,
                                   window_ms=900_000, max_attempts=3)
        self.assertEqual(out["decision"], "exhausted")
        self.assertEqual(0, out["remaining"])

    def test_transients_retryable_inside_budget(self):
        out = transients_exhausted(failure_count=1, first_at=1000, now=1500,
                                   window_ms=10_000, max_attempts=3)
        self.assertEqual(out["decision"], "retryable")
        self.assertEqual(out["remaining"], 2)

    def test_switch_allowed(self):
        self.assertTrue(switch_allowed(switches_used=0)["allowed"])
        self.assertFalse(switch_allowed(switches_used=1)["allowed"])
        self.assertFalse(switch_allowed(switches_used=0, authorized=False)["allowed"])
        self.assertFalse(switch_allowed(switches_used=0, target_available=False)["allowed"])
        self.assertFalse(switch_allowed(switches_used=0, same_route=True)["allowed"])

    def test_notify_update_budget(self):
        self.assertTrue(notify_update_allowed(updates_used=0))
        self.assertTrue(notify_update_allowed(updates_used=2))
        self.assertFalse(notify_update_allowed(updates_used=3))
        self.assertFalse(notify_update_allowed(updates_used=9))


class TestStateTransitions(unittest.TestCase):
    def test_valid_flow(self):
        self.assertTrue(can_transition(None, EPISODE_OPEN))
        self.assertTrue(can_transition(EPISODE_OPEN, EPISODE_SWITCH_REQUESTED))
        self.assertTrue(can_transition(EPISODE_SWITCH_REQUESTED,
                                       EPISODE_SWITCH_CONFIRMED))
        self.assertTrue(can_transition(EPISODE_SWITCH_REQUESTED,
                                       EPISODE_SWITCH_UNKNOWN))
        self.assertTrue(can_transition(EPISODE_SWITCH_CONFIRMED,
                                       EPISODE_CONTINUATION_REQUESTED))
        self.assertTrue(can_transition(EPISODE_CONTINUATION_REQUESTED,
                                       EPISODE_RESUMED))

    def test_invalid_flow_rejected(self):
        self.assertFalse(can_transition(None, EPISODE_RESUMED))
        self.assertFalse(can_transition(EPISODE_SWITCH_CONFIRMED,
                                        EPISODE_SWITCH_UNKNOWN))
        self.assertFalse(can_transition(EPISODE_RESUMED, EPISODE_SWITCH_REQUESTED))
        self.assertFalse(can_transition(None, "bogus"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
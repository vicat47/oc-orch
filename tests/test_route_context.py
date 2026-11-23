"""P13/P14路线投影是可解释的观测，不是许可、零开销或提速证明。"""
import unittest
from oc_orch.domain.route_context import FIELDS, route_context


class RouteContextTests(unittest.TestCase):
    def test_missing_or_bad_shape_is_unknown_and_no_authorization(self):
        for raw in (None, [], "cache hit", True, {"cache_hit": True}):
            view = route_context({"metadata": {"route_observations": raw}}, None, 100)
            self.assertTrue(all(v["state"] == "unknown" and v["value"] is None for v in view["observations"].values()))
            self.assertFalse(view["faster_route_proven"])
            self.assertEqual(view["authorization_effect"], "none")

    def test_measured_estimate_and_stale_include_provenance(self):
        raw = {field: {"value": False if field == "cache_hit" else 50, "source": "session REST measured",
                       "observed_at": 80, "expires_at": 110, "kind": "measured"} for field in FIELDS}
        raw["remaining_ms"]["kind"] = "estimate"
        task = {"metadata": {"route_observations": raw}}
        view = route_context(task, {"session_id": "same", "model": "fixture/free"}, 100)
        self.assertEqual(view["observations"]["remaining_ms"]["state"], "estimate")
        self.assertEqual(view["observations"]["cache_hit"]["value"], False)
        self.assertEqual(view["session_id"], "same")
        self.assertIn("保留", view["continuity_reason"])
        stale = route_context(task, None, 110)
        self.assertTrue(all(v["state"] == "stale" and v["value"] is None and v["source"] for v in stale["observations"].values()))

    def test_bool_number_nan_future_or_absent_source_not_zero(self):
        for delta in ({"value": True}, {"value": float("nan")}, {"value": -1}, {"observed_at": 101}, {"expires_at": 80}, {"source": ""}):
            item = {"value": 0, "source": "probe", "observed_at": 80, "expires_at": 110, "kind": "estimate", **delta}
            out = route_context({"metadata": {"route_observations": {"queue_ms": item}}}, None, 100)
            self.assertEqual(out["observations"]["queue_ms"]["state"], "unknown")

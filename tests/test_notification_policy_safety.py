"""独立复核 C1–C3：坏形状与未知副作用不得崩溃或冒充清理完成。"""
import unittest

from oc_orch.domain.notification_policy import notification_decision
from tests import test_notification_policy as fixtures


class NotificationPolicySafetyTests(unittest.TestCase):
    def test_malformed_payload_defers_without_crashing(self):
        for bad in (["x"], "invalid", 1, [], None):
            with self.subTest(payload=bad):
                n = fixtures._notification("stall", task_id="T-1", attempt_id="A-cur")
                n["payload"] = bad
                result = notification_decision(n, task=fixtures._task(),
                                               attempt=fixtures._attempt("stalled"))
                self.assertEqual(result["action"], "defer")
                self.assertEqual(result["reason"], "invalid-notification")

    def test_malformed_metadata_keeps_released_cleanup_visible(self):
        for bad in (["x"], "invalid", 1, None):
            with self.subTest(metadata=bad):
                attempt = fixtures._attempt(site_released_at=10)
                attempt["metadata"] = bad
                result = notification_decision(
                    fixtures._notification("side_effects_running", task_id="T-1", attempt_id="A-cur"),
                    task=fixtures._task(), attempt=attempt)
                self.assertEqual(result["action"], "cleanup_required")
                self.assertEqual(attempt["site_released_at"], 10)

    def test_malformed_side_checks_are_unknown_not_inactive(self):
        for bad in (["x"], "invalid", 1, None):
            with self.subTest(checks=bad):
                attempt = fixtures._attempt(site_released_at=10,
                                             metadata={"side_effect_checks": bad})
                result = notification_decision(
                    fixtures._notification("side_effects_running", task_id="T-1", attempt_id="A-cur"),
                    task=fixtures._task(), attempt=attempt)
                self.assertEqual(result["action"], "cleanup_required")

    def test_unknown_side_check_state_cannot_clear_cleanup(self):
        for bad in ({"state": "wat"}, {}, None, "inactive", {"state": []}):
            with self.subTest(check=bad):
                attempt = fixtures._attempt(site_released_at=10,
                    metadata={"side_effect_checks": {"shells": bad}})
                result = notification_decision(
                    fixtures._notification("side_effects_running", task_id="T-1", attempt_id="A-cur"),
                    task=fixtures._task(), attempt=attempt)
                self.assertEqual(result["action"], "cleanup_required")
        attempt = fixtures._attempt(site_released_at=10,
            metadata={"side_effect_checks": {"shells": {"state": "inactive"}}})
        result = notification_decision(
            fixtures._notification("side_effects_running", task_id="T-1", attempt_id="A-cur"),
            task=fixtures._task(), attempt=attempt)
        self.assertEqual(result["action"], "suppress")

    def test_non_string_kind_is_a_diagnostic_not_type_error(self):
        for bad in (["stall"], {"name": "stall"}, 1):
            with self.subTest(kind=bad):
                result = notification_decision({"kind": bad, "payload": {}})
                self.assertEqual(result["action"], "defer")

    def test_partially_attributed_cleanup_is_not_global_compatibility(self):
        for kind in ("side_effects_running", "execution_unconfirmed"):
            with self.subTest(kind=kind):
                result = notification_decision(
                    fixtures._notification(kind, task_id="T-1"), task=fixtures._task())
                self.assertEqual(result["action"], "defer")
                self.assertEqual(result["reason"], "attempt-unresolved")


if __name__ == "__main__":
    unittest.main()

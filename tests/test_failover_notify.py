#!/usr/bin/env python3
"""provider-fault-notifications 域语义测试（N01/N02/N07/N10/N11 域侧）。"""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.domain.failover_notify import (
    EVENT_PROVIDER_FAULT,
    EVENT_PROVIDER_FAILOVER,
    KIND_FIRST,
    KIND_RECOVERY,
    KIND_UPDATE,
    base_key,
    content,
    decide,
    event_key,
    event_type_for_state,
)
from oc_orch.domain.failover import (
    EPISODE_BLOCKED_MANUAL,
    EPISODE_OPEN,
    EPISODE_RESUMED,
)


class TestEventType(unittest.TestCase):
    def test_fault_vs_failover(self):
        self.assertEqual(event_type_for_state(EPISODE_BLOCKED_MANUAL),
                         EVENT_PROVIDER_FAULT)
        self.assertEqual(event_type_for_state(EPISODE_OPEN),
                         EVENT_PROVIDER_FAILOVER)
        self.assertEqual(event_type_for_state(EPISODE_RESUMED),
                         EVENT_PROVIDER_FAILOVER)
        self.assertEqual(event_type_for_state("paused"), "")
        self.assertEqual(event_type_for_state("bogus"), "")


class TestKeys(unittest.TestCase):
    def test_base_key_stable_and_scoped_by_recipient(self):
        a = base_key("EP-1", EVENT_PROVIDER_FAILOVER, "operator-primary")
        b = base_key("EP-1", EVENT_PROVIDER_FAILOVER, "operator-primary")
        c = base_key("EP-1", EVENT_PROVIDER_FAILOVER, "other")
        d = base_key("EP-2", EVENT_PROVIDER_FAILOVER, "operator-primary")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertNotEqual(a, d)

    def test_event_key_versioned(self):
        a = event_key(episode_id="EP-1", incident_type=EVENT_PROVIDER_FAILOVER,
                      recipient_ref="r", version=1, kind=KIND_FIRST)
        b = event_key(episode_id="EP-1", incident_type=EVENT_PROVIDER_FAILOVER,
                      recipient_ref="r", version=1, kind=KIND_FIRST)
        c = event_key(episode_id="EP-1", incident_type=EVENT_PROVIDER_FAILOVER,
                      recipient_ref="r", version=2, kind=KIND_UPDATE)
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        with self.assertRaises(ValueError):
            event_key(episode_id="EP-1", incident_type=EVENT_PROVIDER_FAILOVER,
                      recipient_ref="r", version=0, kind=KIND_FIRST)


class TestDecide(unittest.TestCase):
    def test_first_notify_even_if_resolved(self):
        # N07：快速恢复后无既有提醒，仍发首封摘要（不是孤立恢复抑制）。
        out = decide(version=1, rows=[], resolved=True)
        self.assertEqual(out["event"], KIND_FIRST)
        self.assertFalse(out["prior_possible"])

    def test_recovery_once_after_reminder(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 1}]
        out = decide(version=2, rows=rows, resolved=True)
        self.assertEqual(out["event"], KIND_RECOVERY)
        out2 = decide(version=3, rows=[{"kind": KIND_FIRST, "condition_version": 1},
                                       {"kind": KIND_RECOVERY, "condition_version": 2}],
                      resolved=True)
        self.assertIsNone(out2["event"])

    def test_duplicate_poll_no_new_event(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 3}]
        out = decide(version=3, rows=rows)
        self.assertIsNone(out["event"])
        self.assertEqual(out["reason"], "duplicate-observation")

    def test_update_budget_limited(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 1},
                {"kind": KIND_UPDATE, "condition_version": 2},
                {"kind": KIND_UPDATE, "condition_version": 3},
                {"kind": KIND_UPDATE, "condition_version": 4}]
        out = decide(version=5, rows=rows, max_updates=3)
        self.assertIsNone(out["event"])
        self.assertEqual(out["reason"], "update-budget-exhausted")

    def test_condition_update_allowed_within_budget(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 1}]
        out = decide(version=2, rows=rows, max_updates=3)
        self.assertEqual(out["event"], KIND_UPDATE)


class TestContent(unittest.TestCase):
    def test_content_minimal_and_hashed(self):
        c = content(incident_type=EVENT_PROVIDER_FAILOVER, state=EPISODE_OPEN,
                    project="reverse-le5le-editor-drag-drop", task_id="T-1",
                    session_id="ses_w1", route="deepseek/deepseek-flash",
                    category="quota", target_route="yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash")
        self.assertIn("deepseek/deepseek-flash", c["subject"] + c["body"])
        self.assertIn("T-1", c["body"])
        self.assertEqual(len(c["content_hash"]), 64)
        again = content(incident_type=EVENT_PROVIDER_FAILOVER, state=EPISODE_OPEN,
                        project="reverse-le5le-editor-drag-drop", task_id="T-1",
                        session_id="ses_w1", route="deepseek/deepseek-flash",
                        category="quota",
                        target_route="yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash")
        self.assertEqual(c["content_hash"], again["content_hash"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

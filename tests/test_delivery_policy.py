"""交付策略值与指令投影：纯规则，不访问网络或台账。"""

import copy
import unittest

from oc_orch.domain.delivery_policy import (
    PRESETS, identity, normalize_policy, render_instructions, revision,
)
from oc_orch.domain.errors import LedgerError


def policy(preset="budget-first", stage="standard"):
    return {"preset": preset, "stage": stage,
            "limits": {"max_parallel": 4, "max_candidates": 2, "max_challenge_rounds": 1}}


def version(value=None):
    return {"schema": 1, "scope": "task", "scope_id": "T-example", "project": "p",
            "revision": 1, "policy": value or policy(), "source": None,
            "actor": "operator", "reason": "explicit", "operation_id": "op-1",
            "created_at": 1000}


class DeliveryPolicyRuleTests(unittest.TestCase):
    def test_all_presets_and_stages_are_independent(self):
        for mode in PRESETS:
            for stage in ("standard", "prototype"):
                with self.subTest(mode=mode, stage=stage):
                    self.assertEqual(normalize_policy(policy(mode, stage)), policy(mode, stage))

    def test_invalid_policy_shapes_are_rejected(self):
        values = [None, [], "budget-first", {}, {**policy(), "paid": True},
                  {**policy(), "preset": []}, {**policy(), "stage": True},
                  {**policy(), "preset": "fast"}, {**policy(), "stage": "unlimited"},
                  {**policy(), "limits": []}, {**policy(), "limits": {}},
                  {**policy(), "limits": {**policy()["limits"], "max_tokens": 100}}]
        for value in values:
            with self.subTest(value=value), self.assertRaises(LedgerError) as caught:
                normalize_policy(value)
            self.assertEqual(caught.exception.code, "DELIVERY_POLICY_INVALID")

    def test_limit_types_and_bounds(self):
        for key, bad_values in {
            "max_parallel": [True, "4", None, 0, -1, 33, 1.0],
            "max_candidates": [False, "2", None, 0, 9],
            "max_challenge_rounds": [True, "0", None, -1, 9],
        }.items():
            for bad in bad_values:
                value = policy()
                value["limits"][key] = bad
                with self.subTest(key=key, bad=bad), self.assertRaises(LedgerError):
                    normalize_policy(value)
        good = policy()
        good["limits"]["max_challenge_rounds"] = 0
        self.assertEqual(normalize_policy(good), good)

    def test_normalize_returns_detached_nested_value(self):
        original = policy()
        result = normalize_policy(original)
        original["limits"]["max_parallel"] = 30
        self.assertEqual(result["limits"]["max_parallel"], 4)

    def test_identity_and_revision_do_not_coerce(self):
        for value in (True, 1, [], None, "", " p", "p ", "p\nq", "x" * 501):
            with self.subTest(value=value), self.assertRaises(LedgerError):
                identity(value, "project")
        for value in (True, 1.0, "1", -1, None):
            with self.subTest(value=value), self.assertRaises(LedgerError):
                revision(value)
        self.assertEqual(revision(0), 0)

    def test_prompts_preserve_quality_authorization_and_control(self):
        for mode in PRESETS:
            with self.subTest(mode=mode):
                record = version(policy(mode))
                before = copy.deepcopy(record)
                text = render_instructions(record)
                for phrase in ("版本 1", "预设不是授权", "collect/verify", "不重播未知输入",
                               "不改变父模型", "不是必须用满", "简单、稳定、可维护"):
                    self.assertIn(phrase, text)
                self.assertEqual(record, before)

    def test_prototype_does_not_fake_implementation_or_reduce_commitment(self):
        text = render_instructions(version(policy(stage="prototype")))
        for phrase in ("mock模拟", "未实现占位", "不得伪造保存成功", "稳定ID", "补齐证据", "不降低"):
            self.assertIn(phrase, text)
        self.assertNotIn("本轮先交付可体验", render_instructions(version()))

    def test_corrupt_version_never_produces_instructions(self):
        variants = [{"schema": True}, {"schema": 2}, {"revision": False},
                    {"scope": "unknown"}, {"created_at": -1}, {"actor": []},
                    {"source": {"scope": "project", "scope_id": "q", "revision": 1}}]
        for delta in variants:
            with self.subTest(delta=delta), self.assertRaises(LedgerError):
                render_instructions({**version(), **delta})

    def test_incomplete_projection_raises_classified_error(self):
        for value in (None, [], {}, {"policy": policy()}):
            with self.subTest(value=value), self.assertRaises(LedgerError):
                render_instructions(value)

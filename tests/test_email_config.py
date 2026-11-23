#!/usr/bin/env python3
"""本机邮件策略配置与 email CLI：默认关闭、严格类型、零隐式网络。

不访问真实邮箱：enabled 配置也只验证装配/策略；tick 的 fake/空 outbox 证明
没有外部调用。真实发送/回执另由授权测试执行。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from oc_orch.adapters.email_config import (
    CONFIG_NAME,
    EmailConfigError,
    load_email_runtime,
)
from oc_orch.domain.operator_email import POLICY_ENABLED


def valid_policy(**overrides):
    policy = {
        "schema": "oc-orch.email-policy.v1",
        "enabled": True,
        "sender": {
            "ref": "agently-primary",
            "identity": "sender@example.test",
            "verified": True,
            "version": "1.0.18",
        },
        "recipients": [{"ref": "operator-primary",
                        "address": "recipient@example.test"}],
        "projects": ["project-a"],
        "event_kinds": ["first", "update", "escalation", "recovery"],
        "unattended": True,
        "receipt_queries_authorized": True,
    }
    policy.update(overrides)
    return policy


class EmailConfigTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="email-config-",
                                          dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.path = Path(self.home) / CONFIG_NAME

    def write(self, value):
        self.path.write_text(json.dumps(value), encoding="utf-8")
        os.chmod(self.path, 0o600)

    def test_missing_and_disabled_are_closed(self):
        runtime = load_email_runtime(self.home)
        self.assertFalse(runtime.enabled)
        self.assertEqual(runtime.state, "disabled")
        self.write({"schema": "oc-orch.email-policy.v1", "enabled": False})
        runtime = load_email_runtime(self.home)
        self.assertFalse(runtime.enabled)
        self.assertEqual(runtime.recipient_refs, ())

    def test_valid_policy_is_enabled_and_builds_binding(self):
        self.write(valid_policy())
        runtime = load_email_runtime(self.home)
        self.assertTrue(runtime.enabled)
        self.assertEqual(runtime.state, POLICY_ENABLED)
        self.assertEqual(runtime.sender_binding.ref, "agently-primary")
        self.assertTrue(runtime.sender_binding.verified)
        self.assertEqual(runtime.recipient_refs, ("operator-primary",))
        self.assertEqual(runtime.policy.projects, frozenset({"project-a"}))

    def test_bad_fields_fail_closed(self):
        cases = [
            {"sender": None},
            {"sender": {**valid_policy()["sender"], "verified": False}},
            {"sender": {**valid_policy()["sender"], "identity": " x "}},
            {"recipients": []},
            {"recipients": [{"ref": "r", "address": "not-an-address"}]},
            {"recipients": [{"ref": "r", "address": "a@b"},
                            {"ref": "r", "address": "c@d"}]},
            {"projects": []},
            {"projects": [" p "]},
            {"event_kinds": ["unknown"]},
            {"unattended": False},
            {"receipt_queries_authorized": "yes"},
            {"max_attempts": True},
            {"retry_min_ms": 100000, "retry_max_ms": 10000},
            {"schema": "other"},
        ]
        for overrides in cases:
            self.write(valid_policy(**overrides))
            with self.assertRaises(EmailConfigError, msg=str(overrides)):
                load_email_runtime(self.home)

    def test_invalid_json_is_error_not_disabled(self):
        self.path.write_text("{bad", encoding="utf-8")
        with self.assertRaises(EmailConfigError):
            load_email_runtime(self.home)


class EmailCliTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="email-cli-",
                                          dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.path = Path(self.home) / CONFIG_NAME

    def run_cli(self, *args):
        env = dict(
            os.environ,
            PYTHONPATH=str(ROOT / "src"),
            OC_ORCH_HOME=self.home,
            OC_ORCH_CONFIG_JSON="{}",
            OC_ORCH_SERVICE_CONFIG=str(Path(self.home) / "no-service.json"),
        )
        return subprocess.run(
            [sys.executable, "-B", "-m", "oc_orch.cli", "--home", self.home,
             "--json", *args],
            cwd=ROOT, env=env, text=True, capture_output=True, timeout=30)

    def test_check_and_status_without_policy(self):
        check = self.run_cli("email", "check")
        self.assertEqual(check.returncode, 0, check.stderr)
        data = json.loads(check.stdout)
        self.assertEqual(data["state"], "disabled")
        self.assertFalse(data["enabled"])
        status = self.run_cli("email", "status")
        self.assertEqual(status.returncode, 0, status.stderr)
        data = json.loads(status.stdout)
        self.assertEqual(data["outbox"], [])

    def test_tick_disabled_makes_zero_external_calls(self):
        tick = self.run_cli("email", "tick")
        self.assertEqual(tick.returncode, 0, tick.stderr)
        data = json.loads(tick.stdout)
        self.assertTrue(data["ok"])
        self.assertEqual(data["external_calls"], {"send": 0, "search": 0})

    def test_check_reports_invalid_policy(self):
        self.path.write_text(json.dumps(valid_policy(unattended=False)),
                             encoding="utf-8")
        check = self.run_cli("email", "check")
        self.assertNotEqual(check.returncode, 0)
        self.assertIn("email-config", check.stdout + check.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
"""完成协议的真实 CLI 文本、读取集合 ACK 与配置边界（T32/T34/T36）。"""

from contextlib import contextmanager, redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from oc_orch import cli
from oc_orch.composition import application
from oc_orch.config import DEFAULTS, load_config
from oc_orch.db import init
from oc_orch.ledger import Ledger


class CompletionCliContract(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="completion-cli-", dir="/tmp/opencode")
        self.addCleanup(temp.cleanup)
        self.home = temp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)
        self.payload = {
            "classification": "DELIVERABLE_MISSING",
            "result_version": "rv1-cli-contract",
            "result_path": "/results/versions/rv1-cli-contract.json",
            "collection_state": "complete",
            "missing_deliverables": ["missing.txt"],
            "stale_deliverables": ["unchanged.txt"],
            "unverified_deliverables": ["python3 validate.py"],
            "ready_for_verification": False,
        }

    def enqueue(self, key):
        return self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_cli", dedupe_key=key,
            payload=dict(self.payload))

    def test_inbox_human_text_has_version_all_gaps_and_explicit_ack(self):
        notification = self.enqueue("human:v1")
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"),
                   OC_ORCH_HOME=self.home, OC_ORCH_CONFIG_JSON="{}",
                   OC_ORCH_SERVICE_CONFIG=str(Path(self.home) / "no-service.json"))
        result = subprocess.run(
            [sys.executable, "-B", "-m", "oc_orch.cli", "--home", self.home,
             "inbox", "--session", "ses_cli"],
            cwd=ROOT, env=env, text=True, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for value in (notification["id"], "DELIVERABLE_MISSING", "rv1-cli-contract",
                      self.payload["result_path"], "collection_state: complete",
                      "missing.txt", "unchanged.txt", "python3 validate.py",
                      "ready_for_verification: False", "notify ack"):
            with self.subTest(value=value):
                self.assertIn(value, result.stdout)
        self.assertEqual(self.ledger.get_notification(notification["id"])["state"],
                         "pending", "读取人类文本不能隐式 ACK")

    def test_inbox_ack_all_only_acks_read_ids_when_new_version_arrives(self):
        old = self.enqueue("ack:v1")
        second = Ledger(init(self.home), self.home)
        self.addCleanup(second.conn.close)
        original = self.ledger.list_notifications
        newer = {}

        def read_then_publish(*args, **kwargs):
            rows = original(*args, **kwargs)
            if kwargs.get("state") == "pending" and not newer:
                newer.update(second.enqueue_notification(
                    kind="result_ready", target_session="ses_cli", dedupe_key="ack:v2",
                    payload={**self.payload, "result_version": "rv1-newer"}))
            return rows

        def no_client():
            self.fail("显式 ACK 不应构造真实服务客户端")

        app = application(self.ledger, no_client, {})

        @contextmanager
        def open_app():
            yield app

        ctx = SimpleNamespace(ledger=self.ledger, app=open_app)
        args = SimpleNamespace(session="ses_cli", ack_all=True, json=False)
        output = StringIO()
        with patch.object(cli, "Ctx", return_value=ctx), \
                patch.object(self.ledger, "list_notifications", side_effect=read_then_publish), \
                redirect_stdout(output):
            self.assertEqual(cli.cmd_inbox(args), 0)
        self.assertEqual(self.ledger.get_notification(old["id"])["state"], "acked")
        self.assertEqual(self.ledger.get_notification(newer["id"])["state"], "pending")
        self.assertIn("acked 1", output.getvalue())

    def test_collect_human_text_reports_classification_and_readiness(self):
        result = {
            "attempt_id": "A-cli", "status": "result_collected", "text_chars": 20,
            "assistant_message_count": 1, "result_path": self.payload["result_path"],
            "result_version": "rv1-cli-contract", "collection_state": "complete",
            "classification": "CHECKS_PENDING", "ready_for_verification": False,
            "notification_ids": ["N-cli"], "missing": [], "stale": [],
            "unverified": ["python3 validate.py"],
        }
        calls = []

        def collect(attempt, **intent):
            calls.append((attempt, intent))
            return result

        @contextmanager
        def open_app():
            yield SimpleNamespace(collect=collect)

        args = SimpleNamespace(attempt="A-cli", partial=False, carry_manifest=None,
                               json=False)
        output = StringIO()
        with patch.object(cli, "Ctx", return_value=SimpleNamespace(app=open_app)), \
                redirect_stdout(output):
            self.assertEqual(cli.cmd_collect(args), 0)
        self.assertEqual(calls, [("A-cli", {"partial": False, "carry_manifest": None})])
        for value in ("CHECKS_PENDING", "ready_for_verification=False", "rv1-cli-contract",
                      "python3 validate.py", "N-cli", "显式 ack"):
            with self.subTest(value=value):
                self.assertIn(value, output.getvalue())

    def test_scan_and_retry_budgets_reject_nonpositive_and_noninteger_overrides(self):
        keys = ("collect_retry_interval", "collect_max_attempts", "result_scan_max_pages",
                "deliverable_scan_max_entries", "deliverable_scan_max_bytes")
        path = Path(self.home) / "config.json"
        with patch.dict(os.environ, {"OC_ORCH_CONFIG_JSON": "{}"}):
            for value in (0, -1, True, "5", 1.5, None):
                with self.subTest(value=value, source="file"):
                    path.write_text(json.dumps(dict.fromkeys(keys, value)))
                    cfg = load_config(self.home)
                    self.assertEqual({k: cfg[k] for k in keys},
                                     {k: DEFAULTS[k] for k in keys})
            path.write_text(json.dumps(dict.fromkeys(keys, 7)))
            with patch.dict(os.environ, {"OC_ORCH_CONFIG_JSON": json.dumps(
                    dict.fromkeys(keys, 0))}):
                self.assertEqual({k: load_config(self.home)[k] for k in keys},
                                 dict.fromkeys(keys, 7))
            with patch.dict(os.environ, {"OC_ORCH_CONFIG_JSON": json.dumps(
                    dict.fromkeys(keys, 9))}):
                self.assertEqual({k: load_config(self.home)[k] for k in keys},
                                 dict.fromkeys(keys, 9))


if __name__ == "__main__":
    unittest.main()

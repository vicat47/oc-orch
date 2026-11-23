"""故障切换 CLI：本地只读、策略 CAS 与外部动作逐字确认。"""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from oc_orch.entrypoints import cli  # noqa: E402
from oc_orch.adapters.sqlite_db import init  # noqa: E402
from oc_orch.adapters.sqlite_ledger import Ledger  # noqa: E402
from oc_orch.adapters.writer_guard import install_writer_guard  # noqa: E402
from oc_orch.adapters.failover_guard import install_failover_guard  # noqa: E402
from oc_orch.adapters.failover_ledger import FailoverLedger  # noqa: E402


class FailoverCliTest(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="failover-cli-")
        conn = init(self.home)
        install_writer_guard(conn, actor="test")
        install_failover_guard(conn, actor="test")
        self.ledger = Ledger(conn, self.home)
        self.task = self.ledger.create_task(project="/tmp/failover-cli-project",
                                            title="failover cli")
        self.failover = FailoverLedger(conn, clock=lambda: 100,
                                       audit=self.ledger.audit)
        self.conn = conn

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.home, ignore_errors=True)

    def _exec(self, argv, *, as_json=True):
        raw = (["--json"] if as_json else []) + ["--home", self.home] + argv
        args = cli.build_parser().parse_args(raw)
        out, err = StringIO(), StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = args.func(args)
        return code, out.getvalue(), err.getvalue()

    def test_read_only_status_does_not_construct_client_or_expose_error_body(self):
        episode = {
            "episode_id": "EP-cli", "project": self.task["project"],
            "task_id": self.task["id"], "session_id": "ses-cli",
            "route": "provider/model", "category": "quota", "status": "open",
            "switches_used": 0, "attempts_used": 1,
            "created_at": 10, "updated_at": 10,
            "metadata": {"error_message": "private response body"},
        }
        self.failover.upsert_record("episode", "EP-cli", episode,
                                    expected_revision=None)
        with patch.object(cli, "Client", side_effect=AssertionError("network client")):
            code, out, err = self._exec(["failover", "status", "--task",
                                         self.task["id"]])
        self.assertEqual(code, 0, err)
        value = json.loads(out)
        self.assertTrue(value["read_only"])
        self.assertEqual(value["network_calls"], 0)
        self.assertEqual(value["episodes"][0]["episode_id"], "EP-cli")
        self.assertNotIn("private response body", out)

    def test_policy_requires_exact_target_and_cas_and_replays_operation(self):
        policy = json.dumps({"enabled": True,
                             "source_routes": ["provider/model"],
                             "fallback_routes": ["other/model"],
                             "max_switches": 1, "max_attempts": 3,
                             "notify_updates": 2})
        code, out, _ = self._exec([
            "failover", "policy", "set-project", "--project", self.task["project"],
            "--policy-json", policy, "--expected-revision", "0",
            "--operation-id", "op-policy-1", "--reason", "enable test policy",
            "--confirm", "wrong-target"])
        self.assertEqual(code, 2)
        self.assertIsNone(self.failover.get_policy("project", self.task["project"]))

        args = ["failover", "policy", "set-project", "--project",
                self.task["project"], "--policy-json", policy,
                "--expected-revision", "0", "--operation-id", "op-policy-1",
                "--reason", "enable test policy", "--confirm",
                self.task["project"]]
        code, out, err = self._exec(args)
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["revision"], 1)
        code, out, err = self._exec(args)
        self.assertEqual(code, 0, err)
        self.assertTrue(json.loads(out)["replayed"])

        stale = list(args)
        stale[stale.index("--operation-id") + 1] = "op-policy-2"
        code, _, _ = self._exec(stale)
        self.assertEqual(code, 2)
        self.assertEqual(self.failover.get_policy("project", self.task["project"])["revision"], 1)

    def test_tick_cannot_reach_client_before_episode_confirmation(self):
        with patch.object(cli, "Client", side_effect=AssertionError("must not construct")):
            code, out, _ = self._exec([
                "failover", "tick", "--episode", "EP-confirm",
                "--confirm", "wrong"])
        self.assertEqual(code, 2)
        self.assertIn("逐字确认", out)


if __name__ == "__main__":
    unittest.main()

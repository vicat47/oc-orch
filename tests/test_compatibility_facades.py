"""旧导入路径和 monkeypatch 必须指向同一权威模块，避免双实现漂移。"""

import importlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


class CompatibilityTests(unittest.TestCase):
    def test_legacy_module_identity(self):
        modules = {
            "ledger": "adapters.sqlite_ledger",
            "db": "adapters.sqlite_db",
            "config": "adapters.config",
            "client": "adapters.opencode",
            "results": "adapters.results",
            "classify": "domain.classify",
            "wake_policy": "domain.wake_policy",
            "supervisor": "runtime.supervisor",
            "stream": "runtime.stream",
            "cli": "entrypoints.cli",
            "web": "entrypoints.web",
        }
        for legacy, canonical in modules.items():
            with self.subTest(legacy=legacy):
                self.assertIs(importlib.import_module("oc_orch." + legacy),
                              importlib.import_module("oc_orch." + canonical))

    def test_legacy_clock_patch_reaches_sqlite_authority(self):
        from oc_orch.db import init
        from oc_orch.ledger import Ledger

        with tempfile.TemporaryDirectory(prefix="oc-orch-compat-") as home:
            conn = init(home)
            try:
                with patch("oc_orch.ledger.now_ms", return_value=123456):
                    task = Ledger(conn, home).create_task(project="p", title="patched clock")
                self.assertEqual(task["created_at"], 123456)
            finally:
                conn.close()

    def test_error_identity_across_legacy_ports(self):
        from oc_orch import client, ledger
        from oc_orch.domain import errors

        self.assertIs(ledger.LedgerError, errors.LedgerError)
        for name in ("ApiError", "TransportError", "StreamError", "OrchError"):
            self.assertIs(getattr(client, name), getattr(errors, name))


if __name__ == "__main__":
    unittest.main()

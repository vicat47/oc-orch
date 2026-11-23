"""架构约束本身及当前源码的回归门。"""

import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("check_boundaries", ROOT / "scripts" / "check_boundaries.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class BoundaryTests(unittest.TestCase):
    def test_current_dependencies(self):
        self.assertEqual(checker.check(), [])

    def test_guard_rejects_reverse_dependencies_and_sql(self):
        cases = [
            ("oc_orch.domain.rules", "from ..client import Client"),
            ("oc_orch.domain.rules", "from oc_orch import client"),
            ("oc_orch.application.services", "from ..adapters.opencode import Client"),
            ("oc_orch.application.services", "import sqlite3"),
            ("oc_orch.adapters.opencode", "from ..runtime.supervisor import Supervisor"),
            ("oc_orch.runtime.supervisor", "ledger.conn.execute('SELECT 1')"),
            ("oc_orch.entrypoints.web", "conn.execute('UPDATE tasks SET status=?', ('accepted',))"),
        ]
        for module, source in cases:
            with self.subTest(module=module, source=source):
                self.assertTrue(checker.violations(source, module))

    def test_allowed_ports_and_composition(self):
        self.assertEqual(checker.violations("from ..domain.errors import LedgerError", "oc_orch.application.services"), [])
        self.assertEqual(checker.violations("from ..application.ports import Clock", "oc_orch.adapters.sqlite_ledger"), [])
        self.assertEqual(checker.violations("from ..composition import application", "oc_orch.entrypoints.web"), [])


if __name__ == "__main__":
    unittest.main()

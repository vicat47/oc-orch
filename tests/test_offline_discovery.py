"""新增回归必须同时进入分层清单；真实 E2E 仍需显式安排。"""
from collections import Counter
import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("offline_runner", ROOT / "scripts/check_offline.py")
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class OfflineDiscoveryTests(unittest.TestCase):
    def test_each_discovered_module_has_exactly_one_layer(self):
        actual = {p.name for p in (ROOT / "tests").glob("test_*.py")}
        listed = [name for values in RUNNER.LAYERS.values() for name in values]
        self.assertEqual({name for name, count in Counter(listed).items() if count > 1}, set())
        self.assertEqual(set(listed), actual,
                         "新增测试需登记unit/integration/contract，不得只依赖all自动发现")

    def test_default_discovery_does_not_run_real_e2e(self):
        command = RUNNER.runner_command("unittest", "all", [])
        self.assertIn("test_*.py", command)
        self.assertNotIn("e2e_real.py", command)
        self.assertFalse(Path("e2e_real.py").match("test_*.py"))
        self.assertTrue(all("e2e_real.py" not in values for values in RUNNER.LAYERS.values()))


if __name__ == "__main__":
    unittest.main()

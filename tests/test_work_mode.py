#!/usr/bin/env python3
"""提示词层工作模式：默认 fast、别名、prompt 注入与 Web/CLI 边界。

不安装 delivery 契约、不写 delivery-policy、不调用资源授权；验证只是提示词。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from oc_orch.adapters.sqlite_db import init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.domain.errors import LedgerError
from oc_orch.domain.prompts import build_prompt
from oc_orch.domain.work_mode import (
    normalize_work_mode,
    render_work_mode,
)
from oc_orch.web import WebServer


class WorkModeRuleTests(unittest.TestCase):
    def test_default_aliases_and_invalid(self):
        self.assertEqual(normalize_work_mode(None), "latency-first")
        self.assertEqual(normalize_work_mode(""), "latency-first")
        self.assertEqual(normalize_work_mode("fast"), "latency-first")
        self.assertEqual(normalize_work_mode("budget"), "budget-first")
        self.assertEqual(normalize_work_mode("elastic"), "elastic")
        for bad in ("slow", " latency-first", True, 1):
            with self.assertRaises(LedgerError):
                normalize_work_mode(bad)

    def test_prompt_contains_mode_once_and_keeps_task_contract(self):
        task = {"id": "T-1", "title": "标题", "objective": "目标",
                "acceptance": ["验收A", "验收B"]}
        default = build_prompt(task, "A-1", 1, "/tmp/x", ["/tmp/x"],
                               ["r.md"], "正文")
        self.assertIn("latency-first", default)
        self.assertIn("不是资源/模型强制", default)
        self.assertEqual(default.count("工作模式（提示词提示"), 1)
        self.assertIn("目标", default)
        self.assertIn("验收A", default)
        budget = build_prompt(task, "A-1", 1, "/tmp/x", ["/tmp/x"],
                              ["r.md"], "正文", work_mode="budget-first")
        self.assertIn("budget-first", budget)
        self.assertIn("费用未知不得当免费", budget)
        elastic = build_prompt(task, "A-1", 1, "/tmp/x", ["/tmp/x"],
                               ["r.md"], "正文", work_mode="elastic")
        self.assertIn("elastic", elastic)
        self.assertIn("加速资源", elastic)

    def test_render_is_advisory(self):
        text = render_work_mode("fast")
        self.assertIn("不改变模型/provider/工具权限", text)
        self.assertIn("费用或预算已被锁定", text)


class WorkModeCliTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="work-mode-", dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.ledger = Ledger(conn, self.home)
        self.task = self.ledger.create_task(
            project="p", title="任务", coordinator_session="ses_c")

    def run_cli(self, *args):
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"),
                   OC_ORCH_HOME=self.home,
                   OC_ORCH_CONFIG_JSON="{}",
                   OC_ORCH_SERVICE_CONFIG=str(Path(self.home) / "no-service.json"))
        return subprocess.run(
            [sys.executable, "-B", "-m", "oc_orch.cli", "--home", self.home,
             "--json", *args],
            cwd=ROOT, env=env, text=True, capture_output=True, timeout=30)

    def test_dispatch_dry_run_default_and_override(self):
        base = ["dispatch", "--task", self.task["id"], "--dry-run",
                "--directory", self.home, "--coordinator", "ses_c",
                "--prompt", "执行"]
        default = self.run_cli(*base)
        self.assertEqual(default.returncode, 0, default.stderr)
        data = json.loads(default.stdout)
        self.assertEqual(data["work_mode"], "latency-first")
        self.assertIn("latency-first", data["prompt"])
        budget = self.run_cli(*base, "--work-mode", "budget")
        self.assertEqual(budget.returncode, 0, budget.stderr)
        data = json.loads(budget.stdout)
        self.assertEqual(data["work_mode"], "budget-first")
        self.assertIn("budget-first", data["prompt"])

    def test_invalid_mode_refused(self):
        run = self.run_cli("dispatch", "--task", self.task["id"], "--dry-run",
                           "--directory", self.home, "--coordinator", "ses_c",
                           "--work-mode", "slow")
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("WORK_MODE_INVALID", run.stdout + run.stderr)


class WorkModeWebTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="work-mode-web-",
                                          dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.ledger = Ledger(conn, self.home)
        self.task = self.ledger.create_task(
            project="p", title="任务", coordinator_session="ses_c")
        self.server = WebServer(self.home, port=0)
        self.port = self.server.start()

    def tearDown(self):
        self.server.stop()

    def post(self, path, body):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(body).encode(), method="POST",
            headers={"Content-Type": "application/json",
                     "X-OC-Orch-Web": "1"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def test_rework_requires_task_confirm_before_service(self):
        status, data = self.post(
            f"/api/task/{self.task['id']}/rework",
            {"feedback": "修一下", "work_mode": "budget-first",
             "confirm": "wrong"})
        self.assertEqual(status, 400, data)
        self.assertEqual(data["code"], "CONFIRM_REQUIRED")

    def test_rework_no_attempt_is_reported_not_fake_success(self):
        status, data = self.post(
            f"/api/task/{self.task['id']}/rework",
            {"feedback": "修一下", "work_mode": "budget-first",
             "confirm": self.task["id"]})
        self.assertEqual(status, 400, data)
        self.assertIn("没有当前尝试", data["message"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
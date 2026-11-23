"""原型CLI/Web薄入口，真实loopback，坏记录/只读不假成功、GET零Client。"""
from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
import urllib.request
from unittest.mock import Mock, patch

from oc_orch import composition
from oc_orch.adapters.prototype import PREFIX
from oc_orch.entrypoints import cli
from oc_orch.entrypoints.web import WebServer
from tests.test_prototype import prototype_plan


class PrototypeWebTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.conn = composition.init(self.tmp.name)
        self.addCleanup(self.conn.close)
        self.ledger = composition.Ledger(self.conn, self.tmp.name)
        self.task = self.ledger.create_task(project="p", title="原型Task", objective="original", acceptance=["不降低"])
        self.client = Mock(side_effect=AssertionError("原型入口不执行/请求模型"))
        self.server = WebServer(self.tmp.name, port=0, client_factory=self.client)
        self.port = self.server.start()
        self.addCleanup(self.server.stop)
        self.path = "/api/prototype/" + self.task["id"]

    def request(self, path=None, body=None, headers=None):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}" + (path or self.path),
            data=json.dumps(body).encode() if body is not None else None,
            headers=headers if headers is not None else {"Content-Type": "application/json", "X-OC-Orch-Web": "1"})
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc: return exc.code, json.loads(exc.read())

    def body(self):
        return {"plan": prototype_plan(), "expected_revision": 0, "operation_id": "web-prototype",
                "reason": "scope", "confirm": self.task["id"]}

    def test_shared_get_post_history_and_task_detail(self):
        self.assertEqual(self.request()[1]["revision"], 0)
        status, out = self.request(body=self.body())
        self.assertEqual(status, 200, out)
        self.assertTrue(out["applied"])
        before = self.conn.total_changes
        status, history = self.request(self.path + "?operation_id=web-prototype")
        self.assertEqual(status, 200)
        self.assertEqual(history["applied_revision"], 1)
        self.assertEqual(self.conn.total_changes, before)
        detail = self.request("/api/task/" + self.task["id"])[1]
        self.assertEqual(detail["prototype"]["revision"], 1)
        self.assertEqual(detail["task"]["objective"], "original")
        self.client.assert_not_called()

    def test_unknown_fields_exact_confirm_stale_and_origin_refused(self):
        for patch_body in ({"actor": "trusted"}, {"confirm": " " + self.task["id"]}, {"expected_revision": True}, {"expected_revision": 9}):
            self.assertEqual(self.request(body={**self.body(), **patch_body})[0], 400)
        self.assertEqual(self.request(body=self.body(), headers={"Content-Type": "application/json"})[0], 403)
        self.assertEqual(self.request(body=self.body(), headers={"X-OC-Orch-Web": "1", "Origin": "https://evil.invalid"})[0], 403)
        self.server.read_only = True
        self.assertEqual(self.request(body=self.body())[0], 403)

    def test_corrupt_plan_shown_as_error_not_empty_scope(self):
        self.request(body=self.body())
        self.conn.execute("UPDATE meta SET value='broken' WHERE key=?", (PREFIX + self.task["id"],))
        status, out = self.request()
        self.assertEqual((status, out["code"]), (400, "PROTOTYPE_CORRUPT"))
        detail = self.request("/api/task/" + self.task["id"])[1]
        self.assertEqual(detail["prototype"], {"ok": False, "code": "PROTOTYPE_CORRUPT"})

    def test_cli_plan_file_same_shared_rules_and_readonly_original_operation(self):
        plan_file = Path(self.tmp.name) / "plan.json"
        plan_file.write_text(json.dumps(prototype_plan()))
        out = StringIO()
        with patch.object(cli, "Client", self.client), redirect_stdout(out):
            status = cli.main(["--home", self.tmp.name, "--json", "prototype", "update", "--task", self.task["id"],
                "--plan-file", str(plan_file), "--expected-revision", "0", "--operation-id", "cli-prototype",
                "--reason", "scope", "--confirm", self.task["id"]])
        self.assertEqual(status, 0, out.getvalue())
        self.assertEqual(json.loads(out.getvalue())["revision"], 1)
        self.assertEqual(self.request(self.path + "?operation_id=cli-prototype")[1]["applied_revision"], 1)
        self.client.assert_not_called()

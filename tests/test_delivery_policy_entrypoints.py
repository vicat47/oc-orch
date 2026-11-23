"""共享CLI/loopback HTTP：类型/确认/CAS/只读/来源隔离，绝不构造真实Client。"""

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
import tempfile
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

from oc_orch.adapters.delivery_guard import install_delivery_guard
from oc_orch.adapters.delivery_policy import DeliveryPolicyStore
from oc_orch.adapters.sqlite_db import init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.writer_guard import install_writer_guard
from oc_orch.entrypoints import cli
from oc_orch.entrypoints.web import WebServer
from tests.fakes import FakeClock
from tests.test_delivery_policy import policy


class DeliveryPolicyEntrypointTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="delivery-entry-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock()
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.task = self.ledger.create_task(project="p", title="policy task", coordinator_session="ses_coord")
        self.client_factory = unittest.mock.Mock(side_effect=AssertionError("策略入口不构造Client"))
        self.server = WebServer(self.home, port=0, client_factory=self.client_factory)
        self.port = self.server.start()
        self.addCleanup(self.server.stop)

    def request(self, path, body=None, *, headers=None):
        default = {"Content-Type": "application/json", "X-OC-Orch-Web": "1"}
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}"+path,
            data=json.dumps(body).encode() if body is not None else None,
            method="POST" if body is not None else "GET", headers=default if headers is None else headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def body(self, **delta):
        return {"task_id": self.task["id"], "policy": policy(), "expected_revision": 0,
                "operation_id": "web-switch-1", "reason": "explicit", "confirm": self.task["id"], **delta}

    def dump(self):
        return "\n".join(self.conn.iterdump())

    def call_cli(self, *argv):
        out, err = StringIO(), StringIO()
        with patch.object(cli, "Client", self.client_factory), redirect_stdout(out), redirect_stderr(err):
            status = cli.main(["--home", self.home, "--json", *argv])
        return status, json.loads(out.getvalue() or err.getvalue())

    def test_read_is_zero_write_unmanaged_and_has_real_identity_boundary(self):
        before = self.dump()
        status, body = self.request("/api/delivery-policy?task_id="+self.task["id"])
        self.assertEqual(status, 200, body)
        self.assertFalse(body["managed"])
        self.assertFalse(body["execution_enforced"])
        self.assertFalse(body["runtime"]["manifest_verified"])
        self.assertIsNone(body["runtime"]["release_id"])
        self.assertEqual(self.dump(), before)
        self.client_factory.assert_not_called()
        install_writer_guard(self.conn)
        install_delivery_guard(self.conn)
        status, enabled_but_legacy = self.request("/api/delivery-policy?task_id="+self.task["id"])
        self.assertEqual(status, 200)
        self.assertFalse(enabled_but_legacy["managed"])
        self.assertFalse(enabled_but_legacy["execution_enforced"], "安装全局写门不等于历史Task已纳管")

    def test_post_draft_does_not_install_guard_or_authorize_resources(self):
        status, out = self.request("/api/delivery-policy/task", self.body())
        self.assertEqual(status, 200, out)
        self.assertEqual(out["current"]["revision"], 1)
        self.assertFalse(out["execution_enforced"])
        self.assertFalse(out["current"]["authorization"]["managed"])
        self.assertIn("预算优先", out["current"]["instructions"])
        self.client_factory.assert_not_called()

    def test_exact_confirm_and_strict_types_no_partial_write(self):
        for delta in ({"confirm": " " + self.task["id"]}, {"expected_revision": True},
                      {"operation_id": ["op"]}, {"reason": {}}, {"task_id": []},
                      {"inherit": "true"}, {"actor": "trusted-user"}, {"paid": True},
                      {"inherit": True}, {"policy": {}}):
            before = self.dump()
            status, out = self.request("/api/delivery-policy/task", self.body(**delta))
            self.assertEqual(status, 400, (delta, out))
            self.assertEqual(self.dump(), before, delta)

    def test_get_scope_must_be_explicit_unique_not_coerced(self):
        for path in ("", "?task_id=", "?task_id=x&project=p", "?project=p&project=q", "?session_id=x"):
            status, out = self.request("/api/delivery-policy"+path)
            self.assertEqual(status, 400, (path, out))

    def test_replay_after_switch_has_original_applied_and_current_separate(self):
        self.assertEqual(self.request("/api/delivery-policy/task", self.body())[0], 200)
        self.assertEqual(self.request("/api/delivery-policy/task", self.body(policy=policy("elastic"),
            expected_revision=1, operation_id="web-switch-2"))[0], 200)
        before = self.dump()
        status, replay = self.request("/api/delivery-policy/task", self.body())
        self.assertEqual(status, 200, replay)
        self.assertFalse(replay["applied"])
        self.assertEqual(replay["applied_revision"], 1)
        self.assertEqual(replay["current"]["revision"], 2)
        self.assertEqual(self.dump(), before)

    def test_two_windows_stale_cas_or_same_op_changed_intent_are_rejected(self):
        self.request("/api/delivery-policy/task", self.body())
        before = self.dump()
        for delta in ({"operation_id": "other-window"}, {"policy": policy("elastic")}):
            status, body = self.request("/api/delivery-policy/task", self.body(**delta))
            self.assertEqual(status, 400, body)
        self.assertEqual(self.dump(), before)

    def test_read_only_csrf_and_external_origin_reject_mutation(self):
        before = self.dump()
        status, _ = self.request("/api/delivery-policy/task", self.body(), headers={"Content-Type": "application/json"})
        self.assertEqual(status, 403)
        status, _ = self.request("/api/delivery-policy/task", self.body(), headers={
            "Content-Type": "application/json", "X-OC-Orch-Web": "1", "Origin": "https://external.invalid"})
        self.assertEqual(status, 403)
        self.server.read_only = True
        status, _ = self.request("/api/delivery-policy/task", self.body())
        self.assertEqual(status, 403)
        self.assertEqual(self.dump(), before)

    def test_cli_and_web_share_policy_but_actor_cannot_impersonate_replay(self):
        status, out = self.call_cli("delivery-policy", "switch-task", "--task", self.task["id"],
            "--policy", json.dumps(policy()), "--expected-revision", "0", "--operation-id", "cli-switch",
            "--reason", "explicit", "--confirm", self.task["id"])
        self.assertEqual(status, 0, out)
        self.assertEqual(self.request("/api/delivery-policy?task_id="+self.task["id"])[1]["revision"], 1)
        status, out = self.request("/api/delivery-policy/task", self.body(operation_id="cli-switch"))
        self.assertEqual(status, 400, out)
        self.assertEqual(out["code"], "DELIVERY_POLICY_OPERATION_CONFLICT")

    def test_cli_read_and_explicit_resource_replace_never_construct_client(self):
        status, body = self.call_cli("delivery-policy", "show", "--task", self.task["id"])
        self.assertEqual(status, 0, body)
        status, body = self.call_cli("resource-authorization", "replace", "--scope", "project", "--scope-id", "p",
            "--resources", "[]", "--expires-at", "9999999999999", "--expected-revision", "0",
            "--operation-id", "cli-revoke", "--reason", "explicit", "--confirm", "p")
        self.assertEqual(status, 0, body)
        status, body = self.call_cli("resource-authorization", "show", "--scope", "project", "--scope-id", "p")
        self.assertEqual(status, 0, body)
        self.assertEqual(body["revision"], 1)
        self.assertEqual(body["resources"], [])
        self.client_factory.assert_not_called()

    def test_task_policy_never_targets_shared_coordinator_instructions(self):
        install_writer_guard(self.conn)
        install_delivery_guard(self.conn)
        self.request("/api/delivery-policy/task", self.body())
        self.ledger.create_attempt(self.task["id"], directory=self.home, session_id="ses_coord",
            model="fixture/free", dispatch_msg_id="msg_coord", metadata={"dispatch": {"prompt": "frozen"}})
        status, out = self.request("/api/delivery-policy?task_id="+self.task["id"])
        self.assertEqual(status, 200, out)
        self.assertTrue(out["execution_enforced"])
        self.assertIsNone(out["instruction_target"])
        self.client_factory.assert_not_called()

    def test_project_default_change_preserves_existing_task_acceptance_and_status(self):
        before = self.ledger.get_task(self.task["id"])
        status, out = self.request("/api/delivery-policy/project", {"project": "p", "policy": policy(stage="prototype"),
            "expected_revision": 0, "operation_id": "project-default", "reason": "explicit", "confirm": "p"})
        self.assertEqual(status, 200, out)
        self.assertEqual(out["current"]["scope_effect"], "new-tasks-only")
        self.assertEqual(self.ledger.get_task(self.task["id"]), before)
        self.assertFalse(self.request("/api/delivery-policy?task_id="+self.task["id"])[1]["managed"])

"""真实loopback显式入口：默认用户、伪来源拒绝、GET零Client、Origin/read_only。"""
from contextlib import redirect_stdout
from io import StringIO
import json
from unittest.mock import patch

from tests.test_continuity_web import ContinuityWebBase
from tests.test_operator_turn import Client
from oc_orch.adapters.clock import SystemClock
from oc_orch.adapters.writer_guard import install_writer_guard
from oc_orch.adapters.delivery_guard import install_delivery_guard
from oc_orch.adapters.resource_authorization import ResourceAuthorizationStore
from oc_orch.entrypoints import cli


class OperatorTurnWebTests(ContinuityWebBase):
    def setUp(self):
        super().setUp()
        install_writer_guard(self.conn)
        install_delivery_guard(self.conn)
        self.clock = SystemClock()
        self.target = Client(self.clock)
        self.server.client_factory = lambda: self.target
        ResourceAuthorizationStore(self.conn, self.clock).replace("coordinator", "ses_coord",
            resources=[{"model": "trial/flash", "cost": "no-extra-cost", "evidence": "explicit test", "internal_calls_controlled": True}],
            expires_at=self.clock.now_ms() + 60000, expected_revision=0, operation_id="auth", actor="test", reason="test")

    def body(self):
        return {"project": "p", "session_id": "ses_coord", "operation_id": "web-op", "text": "补充说明",
                "reason": "test", "confirm": "p", "expected_epoch": self.lease["epoch"], "expected_revision": 0}

    def test_default_user_submission_then_exact_readonly_receipt(self):
        status, result = self.post("/api/coordinator/message", self.body())
        self.assertEqual(status, 200, result)
        self.assertEqual(result["operation"]["source"], "explicit-operator")
        self.assertEqual(len(self.target.posts), 1)
        before = list(self.conn.iterdump())
        self.server.client_factory = lambda: (_ for _ in ()).throw(AssertionError("GET zero Client"))
        status, read = self.get("/api/coordinator/message?project=p&session_id=ses_coord&operation_id=web-op")
        self.assertEqual(status, 200, read)
        self.assertEqual(list(self.conn.iterdump()), before)

    def test_forged_source_end_evidence_and_cross_origin_refused(self):
        for field in ("trusted", "source", "message_id", "idle_id"):
            status, result = self.post("/api/coordinator/message", {**self.body(), field: "forged"})
            self.assertEqual(status, 400, result)
        status, result = self.post("/api/coordinator/message", self.body(), headers={"Origin": "https://evil.invalid"})
        self.assertEqual(status, 403, result)
        self.assertEqual(self.target.posts, [])

    def test_readonly_refuses_submit_and_observe(self):
        self.server.read_only = True
        for path, body in (("/api/coordinator/message", self.body()),
                           ("/api/coordinator/message/observe", {"project": "p", "session_id": "ses_coord", "operation_id": "web-op"})):
            status, result = self.post(path, body)
            self.assertEqual(status, 403, result)
        self.assertEqual(self.target.posts, [])

    def test_get_ambiguous_or_missing_scope_rejected(self):
        for query in ("project=p", "project=p&session_id=ses_coord&operation_id=", "project=p&project=q&session_id=ses_coord&operation_id=x"):
            self.assertEqual(self.get("/api/coordinator/message?" + query)[0], 400)

    def test_cli_uses_same_structured_submission(self):
        body = self.body()
        argv = ["--home", self.tmp, "--json", "conversation", "send"]
        for key in ("project", "operation_id", "text", "reason", "confirm", "expected_epoch", "expected_revision"):
            argv.extend(["--" + key.replace("_", "-"), str(body[key])])
        argv.extend(["--session", "ses_coord"])
        output = StringIO()
        with patch.object(cli, "Client", return_value=self.target), redirect_stdout(output):
            code = cli.main(argv)
        self.assertEqual(code, 0, output.getvalue())
        self.assertEqual(json.loads(output.getvalue())["operation"]["source"], "explicit-operator")

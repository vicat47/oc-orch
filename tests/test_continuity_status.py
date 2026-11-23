"""共享业务健康投影：本地只读、分页先过滤、无正文泄漏及进程/业务分离。"""

from contextlib import redirect_stdout
from io import StringIO
import json
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from oc_orch import composition
from oc_orch.config import DEFAULTS
from oc_orch.domain.errors import LedgerError
from oc_orch.entrypoints import cli
from oc_orch.ledger import Ledger
from tests.fakes import FakeClock


class ContinuityStatusTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="continuity-status-", dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.conn = composition.init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock()
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.control = composition.make_coordinator_control(self.conn, self.clock)
        self.lease = self.ledger.claim_coordinator(
            project="p", session_id="ses_coord", actor="test", ttl=3600)
        self.task = self.ledger.create_task(project="p", title="task", coordinator_session="ses_coord")
        self.service = composition.make_continuity_status(self.ledger, self.clock)

    def pause(self):
        self.control.pause("p", session_id="ses_coord", operation_id="pause",
                           reason="private user text", expected_revision=0,
                           expected_epoch=self.lease["epoch"], evidence={"text": "SECRET-TEXT"})

    def test_snapshot_is_readonly_and_does_not_advance_runtime_cursor(self):
        self.ledger.set_meta("sweep-cursor:business-health-tasks", "T-z-last")
        before = list(self.conn.iterdump())
        for _ in range(2):
            out = self.service.snapshot(project="p")
            self.assertEqual(len(out["tasks"]), 1)
        self.assertEqual(list(self.conn.iterdump()), before)
        self.assertEqual(self.ledger.get_meta("sweep-cursor:business-health-tasks"), "T-z-last")

    def test_process_health_never_means_business_progress_or_completion(self):
        out = self.service.snapshot(process={"healthy": True, "connected": True})
        item = out["tasks"][0]
        self.assertTrue(item["process_healthy"])
        self.assertFalse(item["business_progress"])
        self.assertEqual(item["state"], "no_progress")
        self.assertNotEqual(self.ledger.get_task(self.task["id"])["status"], "accepted")

    def test_pause_projects_and_barriers_are_independent_in_projection(self):
        self.pause()
        self.control.begin_user_turn("ses_coord", message_id="msg_user", origin_at=self.clock.now_ms(),
                                     operation_id="user-turn", trusted=True)
        gate = composition.make_target_gate(self.conn, self.clock)
        gate.observe_error("ses_coord", {"category": "quota", "message": "SECRET-PROVIDER"},
                           evidence_key="quota", observed_at=self.clock.now_ms())
        out = self.service.snapshot()
        control = out["tasks"][0]["control"]
        self.assertEqual(control["project_intent"], "paused")
        self.assertTrue(control["user_turn_active"])
        self.assertIsNotNone(control["provider_gate"])
        self.assertEqual(out["tasks"][0]["state"], "explicitly_paused")
        self.assertEqual(out["events"], [])
        self.assertNotIn("SECRET-", json.dumps(out))
        self.assertNotIn("private user text", json.dumps(out))

    def test_project_and_session_filter_before_limit(self):
        for i in range(4):
            self.ledger.create_task(project="other", title="other", task_id=f"T-000{i}",
                                    coordinator_session="ses_other")
        out = self.service.snapshot(project="p", session_id="ses_coord", limit=1)
        self.assertEqual([t["task_id"] for t in out["tasks"]], [self.task["id"]])
        self.assertTrue(all(e["project"] == "p" for e in out["events"]))

    def test_pagination_reports_partial_scope_instead_of_claiming_done(self):
        self.ledger.create_task(project="p", title="second", coordinator_session="ses_coord")
        first = self.service.snapshot(project="p", limit=1)
        self.assertFalse(first["coverage"]["complete"])
        second = self.service.snapshot(project="p", limit=1,
                                       after_id=first["coverage"]["next_after_id"])
        self.assertNotEqual(first["tasks"][0]["task_id"], second["tasks"][0]["task_id"])
        self.assertIsNone(self.ledger.get_meta("sweep-cursor:business-health-tasks"))

    def test_human_responsibility_shows_owner_deadline_budget_and_action(self):
        store = composition.make_responsibilities(self.ledger)
        row = store.open_responsibility(
            "operator", "p", self.task["id"], state="waiting_human",
            wait_kind="decision", wait_condition="explicit decision",
            next_action="confirm owner", next_check_at=self.clock.now_ms()+30_000,
            deadline=self.clock.now_ms()+60_000, max_budget=3, condition_key="decision",
            target_session="ses_coord", evidence={"body": "SECRET-BODY"})
        item = self.service.snapshot()["tasks"][0]
        self.assertEqual(item["state"], "needs_human")
        self.assertEqual(item["responsibilities"][0]["owner"], "operator")
        self.assertEqual(item["responsibilities"][0]["deadline"], row["deadline"])
        self.assertEqual(item["responsibilities"][0]["max_budget"], 3)
        self.assertNotIn("SECRET-BODY", json.dumps(item))

    def test_bad_control_is_unknown_not_running_and_never_leaks_raw_record(self):
        self.ledger.set_meta("coordinator-control:project:p", "SECRET-BAD-JSON")
        out = self.service.snapshot()
        self.assertEqual(out["tasks"][0]["state"], "unknown")
        self.assertFalse(out["tasks"][0]["evidence_coverage"])
        self.assertEqual(out["events"], [])
        self.assertNotIn("SECRET-BAD-JSON", json.dumps(out))

    def test_expired_coordinator_is_visible_without_making_new_work(self):
        self.clock.advance(seconds=3601)
        out = self.service.snapshot()
        self.assertIn("lease-expired", out["tasks"][0]["control"]["blocking"])
        self.assertEqual(out["tasks"][0]["state"], "needs_human")
        self.assertEqual(self.ledger.list_attempts(), [])

    def test_invalid_limit_or_empty_filter_does_not_silently_expand_scope(self):
        for kw in ({"limit":0}, {"limit":True}, {"project":""}, {"session_id":""}):
            with self.subTest(kw=kw), self.assertRaises((LedgerError, ValueError)):
                self.service.snapshot(**kw)

    def test_cli_health_uses_same_local_projection_without_client(self):
        args = cli.build_parser().parse_args(["--home",self.home,"--json","health","--project","p"])
        out = StringIO()
        with patch.object(cli,"Client", side_effect=AssertionError("no network")), redirect_stdout(out):
            self.assertEqual(args.func(args), 0)
        item = json.loads(out.getvalue())["tasks"][0]
        self.assertEqual(item["task_id"], self.task["id"])
        self.assertEqual(item["state"], "needs_human")  # 真实系统时钟下测试租约已过期

    def test_wal_projection_uses_one_read_snapshot_across_control_tables(self):
        conn2 = composition.init(self.home)
        self.addCleanup(conn2.close)
        other = composition.make_coordinator_control(conn2, self.clock)
        original = self.service._health.business_health

        def health_then_pause(**kwargs):
            out = original(**kwargs)
            applied = other.pause("p", session_id="ses_coord", operation_id="race",
                                  reason="pause", expected_revision=0,
                                  expected_epoch=self.lease["epoch"])
            self.assertTrue(applied["executed"])
            return out

        with patch.object(self.service._health, "business_health", side_effect=health_then_pause):
            old = self.service.snapshot()["tasks"][0]
        self.assertEqual(old["control"]["project_revision"], 0)
        self.assertEqual(self.service.snapshot()["tasks"][0]["state"], "explicitly_paused")

    def test_release_identity_never_labels_uncommitted_tree_as_live_release(self):
        from oc_orch.adapters.release_identity import runtime_identity

        root = Path(self.home)/"release"
        root.mkdir()
        (root/"source.py").write_text("source")
        files = {"source.py":hashlib.sha256(b"source").hexdigest()}
        identity = hashlib.sha256(json.dumps(files,sort_keys=True,separators=(",", ":")).encode()).hexdigest()
        (root/"RELEASE.json").write_text(json.dumps({"release_id":"isolated-test", "runtime_files":files,
                                                    "runtime_manifest_sha256":identity}))
        self.assertTrue(runtime_identity(self.conn, root=root)["manifest_verified"])
        (root/"source.py").write_text("changed source")
        unverified = runtime_identity(self.conn, root=root)
        self.assertFalse(unverified["manifest_verified"])
        self.assertIsNone(unverified["release_id"])


if __name__ == "__main__":
    unittest.main()

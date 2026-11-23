"""S1真实WAL策略草案：无Client、无发送、业务事实保全；不冒称资源许可已接线。"""

import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from oc_orch.adapters.delivery_policy import DeliveryPolicyStore, _key
from oc_orch.composition import init, make_delivery_policy
from oc_orch.domain.errors import LedgerError
from oc_orch.ledger import Ledger
from tests.fakes import FakeClock
from tests.test_delivery_policy import policy


class DeliveryPolicyLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="delivery-policy-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home, self.clock = Path(self.tmp.name), FakeClock()
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.task = self.ledger.create_task(project="p", title="work", coordinator_session="ses_owner")
        self.store = DeliveryPolicyStore(self.conn, self.clock)
        self.service = make_delivery_policy(self.ledger, self.clock)

    def project(self, rev=0, op="default-1", value=None, **extra):
        return self.store.set_project_default("p", policy=value or policy(), expected_revision=rev,
                                               operation_id=op, actor="operator", reason="explicit", **extra)

    def task_switch(self, rev=0, op="task-1", value=None, inherit=False):
        return self.store.switch_task(self.task["id"], policy=None if inherit else value or policy(),
            inherit=inherit, expected_revision=rev, operation_id=op, actor="operator", reason="explicit")

    def dump(self):
        return "\n".join(self.conn.iterdump())

    def failure(self, code, fn):
        before = self.dump()
        with self.assertRaises(LedgerError) as caught:
            fn()
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(self.dump(), before, "拒绝不得留下部分状态或审计")
        self.assertFalse(self.conn.in_transaction)

    def business(self):
        names = ("tasks", "attempts", "turns", "deliverables", "notifications", "session_inputs",
                 "responsibilities", "alerts", "events", "coordinator_leases")
        return {name: [tuple(row) for row in self.conn.execute(f"SELECT * FROM {name}")]
                for name in names}

    def test_unmanaged_reads_are_zero_write_and_do_not_inherit_defaults(self):
        before = self.dump()
        changes = self.conn.total_changes
        self.assertFalse(self.store.project_snapshot("p")["managed"])
        self.assertIsNone(self.service.task(self.task["id"])["instructions"])
        self.assertEqual(self.conn.total_changes, changes)
        self.assertEqual(self.dump(), before)
        self.project()
        self.assertFalse(self.service.task(self.task["id"])["managed"])

    def test_saved_version_and_projection_are_detached_and_not_enforced(self):
        value = policy(stage="prototype")
        out = self.task_switch(value=value)
        value["limits"]["max_parallel"] = 30
        self.assertTrue(out["applied"])
        self.assertFalse(out["execution_enforced"])
        view = self.service.task(self.task["id"])
        self.assertIn("快速原型", view["instructions"])
        self.assertEqual(view["version"]["policy"]["limits"]["max_parallel"], 4)
        self.assertFalse(view["coverage"]["action_permits"])
        view["version"]["policy"]["preset"] = "elastic"
        self.assertEqual(self.store.task_snapshot(self.task["id"])["version"]["policy"]["preset"], "budget-first")

    def test_project_change_never_cascades_existing_binding(self):
        self.project()
        first = self.task_switch(inherit=True)
        self.project(1, "default-2", policy("latency-first"))
        old = self.store.task_snapshot(self.task["id"])
        self.assertEqual(old["version"]["policy"]["preset"], "budget-first")
        self.assertEqual(old["version"]["source"]["revision"], 1)
        new = self.task_switch(1, "task-2", inherit=True)
        self.assertEqual(new["current"]["version"]["source"]["revision"], 2)
        self.assertEqual(first["current"]["revision"], 1)
        original = self.store._version("task", self.task["id"], "p", 1)
        self.assertEqual(original["policy"]["preset"], "budget-first")

    def test_missing_default_or_task_is_rejected_without_write(self):
        self.failure("DELIVERY_POLICY_DEFAULT_MISSING", lambda: self.task_switch(inherit=True))
        self.failure("DELIVERY_POLICY_TASK_MISSING", lambda: self.store.switch_task(
            "T-absent", policy=policy(), expected_revision=0, operation_id="missing",
            actor="operator", reason="explicit"))

    def test_replay_after_new_switch_returns_original_and_current_separately(self):
        self.task_switch()
        self.task_switch(1, "task-2", policy("latency-first"))
        before = self.dump()
        out = self.task_switch()
        self.assertFalse(out["applied"])
        self.assertEqual(out["applied_revision"], 1)
        self.assertEqual(out["current"]["revision"], 2)
        self.assertEqual(out["current"]["version"]["policy"]["preset"], "latency-first")
        self.assertEqual(self.dump(), before)

    def test_inherit_replay_does_not_resolve_new_default(self):
        self.project()
        self.task_switch(inherit=True)
        self.project(1, "default-2", policy("elastic"))
        before = self.dump()
        replay = self.task_switch(inherit=True)
        self.assertEqual(replay["current"]["version"]["source"]["revision"], 1)
        self.assertEqual(self.dump(), before)

    def test_same_operation_changed_intent_or_cas_is_conflict(self):
        self.project()
        self.failure("DELIVERY_POLICY_OPERATION_CONFLICT", lambda: self.project(1))
        self.failure("DELIVERY_POLICY_OPERATION_CONFLICT", lambda: self.project(value=policy("elastic")))
        self.failure("DELIVERY_POLICY_OPERATION_CONFLICT", lambda: self.task_switch(op="default-1"))

    def test_changed_actor_or_reason_cannot_impersonate_replay(self):
        self.project()
        for delta in ({"actor": "another"}, {"reason": "different"}):
            kw = dict(policy=policy(), expected_revision=0, operation_id="default-1",
                      actor="operator", reason="explicit")
            kw.update(delta)
            self.failure("DELIVERY_POLICY_OPERATION_CONFLICT", lambda: self.store.set_project_default("p", **kw))

    def test_wal_two_connections_stale_revision_cannot_overwrite(self):
        conn2 = init(self.home)
        self.addCleanup(conn2.close)
        other = DeliveryPolicyStore(conn2, self.clock)
        self.assertEqual(self.store.project_snapshot("p")["revision"], 0)
        self.assertEqual(other.project_snapshot("p")["revision"], 0)
        self.project()
        self.failure("DELIVERY_POLICY_REVISION_CONFLICT", lambda: other.set_project_default(
            "p", policy=policy("elastic"), expected_revision=0, operation_id="other",
            actor="operator", reason="explicit"))
        self.assertEqual(other.project_snapshot("p")["revision"], 1)

    def test_actual_concurrent_wal_switch_has_one_winner(self):
        barrier, outputs, errors = threading.Barrier(2), [], []

        def switch(number):
            conn = init(self.home)
            try:
                store = DeliveryPolicyStore(conn, self.clock)
                rev = store.project_snapshot("p")["revision"]
                barrier.wait(timeout=5)
                try:
                    outputs.append(store.set_project_default("p", policy=policy(), expected_revision=rev,
                        operation_id=f"thread-{number}", actor="operator", reason="race"))
                except LedgerError as exc:
                    errors.append(exc.code)
            finally:
                conn.close()

        threads = [threading.Thread(target=switch, args=(i,)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(outputs), 1)
        self.assertEqual(errors, ["DELIVERY_POLICY_REVISION_CONFLICT"])
        count = self.conn.execute("SELECT COUNT(*) FROM audit WHERE action='delivery-policy.draft-switch'").fetchone()[0]
        self.assertEqual(count, 1)

    def test_audit_failure_rolls_back_version_head_and_operation(self):
        self.conn.execute("CREATE TEMP TRIGGER fail_policy_audit BEFORE INSERT ON audit "
                          "WHEN NEW.action='delivery-policy.draft-switch' "
                          "BEGIN SELECT RAISE(ABORT,'injected'); END")
        before = self.dump()
        with self.assertRaises(sqlite3.IntegrityError):
            self.project()
        self.assertEqual(self.dump(), before)
        self.assertEqual(self.store.project_snapshot("p")["revision"], 0)
        self.assertFalse(self.conn.in_transaction)

    def test_backup_reopen_preserves_versions_and_idempotency(self):
        self.project()
        self.task_switch(inherit=True)
        restored = sqlite3.connect(self.home / "restored.db", isolation_level=None)
        self.addCleanup(restored.close)
        self.conn.backup(restored)
        store = DeliveryPolicyStore(restored, self.clock)
        self.assertEqual(store.task_snapshot(self.task["id"]), self.store.task_snapshot(self.task["id"]))
        before = restored.total_changes
        replay = store.switch_task(self.task["id"], expected_revision=0, operation_id="task-1",
                                  actor="operator", reason="explicit", inherit=True)
        self.assertFalse(replay["applied"])
        self.assertEqual(restored.total_changes, before)

    def test_switches_preserve_inflight_and_all_business_facts(self):
        att = self.ledger.create_attempt(self.task["id"], directory=str(self.home / "site"),
            session_id="ses_worker", dispatch_msg_id="msg_original",
            metadata={"dispatch": {"prompt": "frozen", "create": {}}})
        before = self.business()
        self.project()
        self.task_switch(inherit=True)
        self.task_switch(1, "task-2", policy("latency-first"))
        self.assertEqual(self.business(), before)
        self.assertEqual(self.ledger.get_attempt(att["id"])["metadata"]["dispatch"]["prompt"], "frozen")

    def test_namespace_identity_cannot_collide(self):
        for project in ('p:1', 'p_1', 'p%1', 'p"1', '中文'):
            self.store.set_project_default(project, policy=policy(), expected_revision=0,
                operation_id="op:" + project, actor="operator", reason="explicit")
            self.assertEqual(self.store.project_snapshot(project)["revision"], 1)
        self.assertFalse(self.store.project_snapshot("p")["managed"])

    def test_bad_write_arguments_do_not_change_ledger(self):
        for delta in ({"expected_revision": True}, {"operation_id": []}, {"actor": ""},
                      {"reason": None}, {"policy": {}}, {"inherit": "false"},
                      {"inherit": True, "policy": policy()}):
            kw = dict(policy=policy(), expected_revision=0, operation_id="bad",
                      actor="operator", reason="explicit")
            kw.update(delta)
            self.failure("DELIVERY_POLICY_INVALID", lambda: self.store.switch_task(self.task["id"], **kw))

    def test_corrupt_head_or_version_fails_closed(self):
        self.project()
        key = _key("head", "project", "p")
        raw = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()[0]
        for value in ("null", "{broken", "[]", json.dumps({"schema": 1, "revision": True})):
            self.conn.execute("UPDATE meta SET value=? WHERE key=?", (value, key))
            self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.store.project_snapshot("p"))
        self.conn.execute("UPDATE meta SET value=? WHERE key=?", (raw, key))
        self.conn.execute("UPDATE meta SET value='{}' WHERE key=?", (_key("version", "project", "p", 1),))
        self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.project(1, "next"))

    def test_missing_head_or_referenced_version_is_not_unmanaged(self):
        self.project()
        self.conn.execute("DELETE FROM meta WHERE key=?", (_key("head", "project", "p"),))
        self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.store.project_snapshot("p"))

    def test_missing_version_is_not_a_successful_replay(self):
        self.project()
        self.conn.execute("DELETE FROM meta WHERE key=?", (_key("version", "project", "p", 1),))
        self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.project())

    def test_corrupt_operation_index_cannot_be_overwritten(self):
        self.project()
        self.conn.execute("UPDATE meta SET value='{}' WHERE key=?", (_key("operation", "default-1"),))
        self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.project())

    def test_deleted_operation_index_does_not_allow_identity_reuse(self):
        self.project()
        self.conn.execute("DELETE FROM meta WHERE key=?", (_key("operation", "default-1"),))
        self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.project(1))
        self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.store.project_snapshot("p"))

    def test_valid_shape_version_tampering_is_detected_by_intent_digest(self):
        self.project()
        key = _key("version", "project", "p", 1)
        original = self.store._load(key)
        for field, value in (("reason", "changed"), ("actor", "changed"), ("policy", policy("elastic"))):
            record = {**original, field: value}
            self.conn.execute("UPDATE meta SET value=? WHERE key=?", (json.dumps(record), key))
            self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.store.project_snapshot("p"))

    def test_inherited_source_corruption_is_visible(self):
        self.project()
        self.task_switch(inherit=True)
        key = _key("version", "project", "p", 1)
        row = self.store._load(key)
        row["policy"] = policy("elastic")
        self.conn.execute("UPDATE meta SET value=? WHERE key=?", (json.dumps(row), key))
        self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.store.task_snapshot(self.task["id"]))

    def test_task_project_change_does_not_reassign_old_policy(self):
        self.task_switch()
        self.conn.execute("UPDATE tasks SET project='q' WHERE id=?", (self.task["id"],))
        self.failure("DELIVERY_POLICY_CORRUPT", lambda: self.service.task(self.task["id"]))

    def test_read_snapshot_uses_one_wal_view(self):
        self.project()
        before = self.conn.total_changes
        self.assertEqual(self.service.project("p")["revision"], 1)
        self.assertEqual(self.conn.total_changes, before)
        self.assertFalse(self.conn.in_transaction)

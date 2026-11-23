"""S2独立资源授权底座：严格条目、不可变版本/CAS/操作幂等、只读授权检查。

authorize只做只读检查，不产生单次动作许可；快照/授权join已有事务（父在同一
BEGIN IMMEDIATE内组装单次许可与模式切换）。生产v1与S1策略不受影响。
"""

import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from oc_orch.adapters.resource_authorization import PREFIX, ResourceAuthorizationStore, _key
from oc_orch.adapters.sqlite_db import init, transaction
from oc_orch.application.resource_authorization import ResourceAuthorizationService
from oc_orch.domain import resource_authorization as rules
from oc_orch.domain.errors import LedgerError
from oc_orch.ledger import Ledger
from tests.fakes import FakeClock

FREE = {"model": "provider/free", "cost": "no-extra-cost",
        "evidence": "docs/evidence-free", "internal_calls_controlled": True}
PAID = {"model": "provider/paid", "cost": "paid",
        "evidence": "docs/evidence-paid", "internal_calls_controlled": False}


class ResourceAuthorizationLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="resource-auth-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home, self.clock = Path(self.tmp.name), FakeClock()
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.task = self.ledger.create_task(project="p", title="work",
                                            coordinator_session="ses_owner")
        self.store = ResourceAuthorizationStore(self.conn, self.clock)
        self.service = ResourceAuthorizationService(self.store)

    def expiry(self, ahead=60_000):
        return self.clock.now_ms() + ahead

    def replace(self, scope="project", scope_id="p", *, resources=None, expires_at=None,
                rev=0, op="op-1", actor="operator", reason="explicit"):
        return self.store.replace(scope, scope_id,
                                  resources=[FREE] if resources is None else resources,
                                  expires_at=self.expiry() if expires_at is None else expires_at,
                                  expected_revision=rev, operation_id=op,
                                  actor=actor, reason=reason)

    def snapshot(self, scope="project", scope_id="p"):
        return self.store.snapshot(scope, scope_id)

    def authorize(self, scope="project", scope_id="p", *, model="provider/free",
                  preset="budget-first", now=None):
        return self.store.authorize(scope, scope_id, model=model, preset=preset, now=now)

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
        names = ("tasks", "attempts", "turns", "deliverables", "notifications",
                 "session_inputs", "responsibilities", "alerts", "events",
                 "coordinator_leases")
        return {name: [tuple(row) for row in self.conn.execute(f"SELECT * FROM {name}")]
                for name in names}

    def audit_count(self):
        return self.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action='resource-authorization.replace'"
        ).fetchone()[0]

    # ---- 只读快照/稳定/坏状态失败关闭 --------------------------------
    def test_expired_original_operation_replays_without_regranting(self):
        expiry = self.expiry(1000)
        self.replace(expires_at=expiry)
        self.clock.advance(seconds=2)
        before = self.dump()
        replay = self.replace(expires_at=expiry)
        self.assertFalse(replay["applied"])
        self.assertEqual(replay["applied_revision"], 1)
        self.assertTrue(replay["current"]["expired"])
        self.assertEqual(self.dump(), before)
        self.failure("RESOURCE_AUTHORIZATION_EXPIRED", lambda: self.authorize())

    def test_model_identity_requires_explicit_provider_and_model(self):
        for model in ("default", "/model", "provider/", "provider/ model"):
            with self.subTest(model=model):
                self.failure("RESOURCE_AUTHORIZATION_INVALID", lambda: self.replace(resources=[{**FREE, "model": model}]))
                self.failure("RESOURCE_AUTHORIZATION_INVALID", lambda: self.authorize(model=model))

    def test_unmanaged_reads_are_zero_write_and_snapshots_are_stable(self):
        before = self.dump()
        changes = self.conn.total_changes
        snap = self.store.snapshot("project", "p")
        self.assertFalse(snap["managed"])
        self.assertEqual(snap["revision"], 0)
        self.assertIsNone(snap["version"])
        self.assertEqual(snap["resources"], [])
        self.assertEqual(self.conn.total_changes, changes)
        self.assertEqual(self.dump(), before)
        self.assertEqual(self.store.snapshot("project", "p"), snap)

    def test_same_snapshot_does_not_change_over_repeated_reads(self):
        self.replace()
        first = self.store.snapshot("project", "p")
        for _ in range(5):
            self.assertEqual(self.store.snapshot("project", "p"), first)
        self.assertEqual(first["revision"], 1)

    def test_read_snapshot_uses_one_wal_view_without_write(self):
        self.replace()
        before = self.conn.total_changes
        self.assertEqual(self.store.snapshot("project", "p")["revision"], 1)
        self.assertEqual(self.store.snapshot("project", "p")["revision"], 1)
        self.assertEqual(self.conn.total_changes, before)
        self.assertFalse(self.conn.in_transaction)

    def test_reads_join_an_existing_transaction_without_new_writes(self):
        self.replace()
        self.replace(rev=1, op="join-2", resources=[FREE, PAID])
        with transaction(self.conn):
            self.assertTrue(self.conn.in_transaction)
            snap = self.store.snapshot("project", "p")
            decision = self.store.authorize("project", "p", model="provider/free",
                                           preset="budget-first")
            self.assertTrue(snap["managed"])
            self.assertTrue(decision["allowed"])
            before = self.conn.total_changes
            self.assertEqual(self.conn.total_changes, before)
            self.assertTrue(self.conn.in_transaction)
            # 授权拒绝不破坏外层事务（父在同一事务内继续组装/回退）
            with self.assertRaises(LedgerError) as caught:
                self.store.authorize("project", "p", model="provider/paid",
                                     preset="budget-first")
            self.assertEqual(caught.exception.code, "RESOURCE_AUTHORIZATION_PRESET_REJECTED")
            self.assertTrue(self.conn.in_transaction)
            self.conn.execute("INSERT INTO meta(key,value) VALUES('outer-write','1')")
        self.conn.execute("DELETE FROM meta WHERE key='outer-write'")

    # ---- 替换/幂等/CAS ------------------------------------------------
    def test_replace_and_authorize_allow_with_expected_contract(self):
        out = self.replace()
        self.assertTrue(out["applied"])
        self.assertFalse(out["single_use_permit"])
        self.assertEqual(out["applied_revision"], 1)
        snap = self.store.snapshot("project", "p")
        self.assertTrue(snap["managed"])
        self.assertEqual(snap["revision"], 1)
        self.assertFalse(snap["expired"])
        self.assertTrue(snap["valid"])
        decision = self.authorize()
        self.assertTrue(decision["allowed"])
        self.assertEqual(decision["scope"], "project")
        self.assertEqual(decision["scope_id"], "p")
        self.assertEqual(decision["authorization_revision"], 1)
        self.assertEqual(decision["expires_at"], self.expiry())
        self.assertEqual(decision["resource"],
                         {"model": "provider/free", "cost": "no-extra-cost",
                          "evidence": "docs/evidence-free", "internal_calls_controlled": True})
        self.assertFalse(decision["single_use_permit"])
        # 快照/授权结果都是游离数据，改动不影响持久版本
        snap["version"]["resources"][0]["model"] = "hacked"
        decision["resource"]["cost"] = "paid"
        self.assertEqual(self.store.snapshot("project", "p")
                         ["version"]["resources"][0]["model"], "provider/free")
        self.assertTrue(self.authorize()["resource"]["cost"] == "no-extra-cost")

    def test_authorize_is_read_only(self):
        self.replace()
        changes = self.conn.total_changes
        self.authorize()
        self.assertEqual(self.conn.total_changes, changes)
        self.assertEqual(self.audit_count(), 1)

    def test_same_operation_id_same_intent_replays_original_and_current_separately(self):
        self.replace()  # op-1 → rev1
        self.replace(rev=1, op="op-2", resources=[PAID])  # rev2
        before = self.dump()
        replay = self.replace()  # 旧op-1回放
        self.assertFalse(replay["applied"])
        self.assertEqual(replay["applied_revision"], 1)
        self.assertEqual(replay["current"]["revision"], 2)
        self.assertEqual(replay["current"]["version"]["resources"][0]["cost"], "paid")
        self.assertEqual(self.dump(), before)

    def test_same_operation_id_different_intent_is_conflict(self):
        self.replace()
        cases = ({"resources": [PAID]}, {"actor": "another"}, {"reason": "different"},
                 {"expires_at": self.expiry(999_999)}, {"expected_revision": 1},
                 {"scope": "coordinator", "scope_id": "ses_x"})
        for changed in cases:
            with self.subTest(changed=changed):
                kw = dict(scope="project", scope_id="p", resources=[FREE],
                          expires_at=self.expiry(), expected_revision=0, op="op-1",
                          actor="operator", reason="explicit")
                kw.update(changed)
                self.failure("RESOURCE_AUTHORIZATION_OPERATION_CONFLICT",
                             lambda: self.store.replace(kw["scope"], kw["scope_id"],
                                                        resources=kw["resources"],
                                                        expires_at=kw["expires_at"],
                                                        expected_revision=kw["expected_revision"],
                                                        operation_id="op-1",
                                                        actor=kw["actor"],
                                                        reason=kw["reason"]))

    def test_stale_revision_cas_conflict(self):
        self.replace()
        self.failure("RESOURCE_AUTHORIZATION_REVISION_CONFLICT",
                     lambda: self.replace(rev=0, op="op-2"))
        self.assertEqual(self.store.snapshot("project", "p")["revision"], 1)

    def test_actual_concurrent_wal_replace_has_one_winner(self):
        barrier, outputs, errors = threading.Barrier(2), [], []

        def worker(number):
            conn = init(self.home)
            try:
                store = ResourceAuthorizationStore(conn, self.clock)
                rev = store.snapshot("project", "p")["revision"]
                barrier.wait(timeout=5)
                try:
                    outputs.append(store.replace("project", "p", resources=[FREE],
                        expires_at=self.expiry(), expected_revision=rev,
                        operation_id=f"race-{number}", actor="operator", reason="race"))
                except LedgerError as exc:
                    errors.append(exc.code)
            finally:
                conn.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(outputs), 1)
        self.assertEqual(errors, ["RESOURCE_AUTHORIZATION_REVISION_CONFLICT"])
        self.assertEqual(self.audit_count(), 1)
        self.assertEqual(self.store.snapshot("project", "p")["revision"], 1)

    def test_audit_failure_rolls_back_version_head_and_operation(self):
        self.conn.execute("CREATE TEMP TRIGGER fail_resource_audit BEFORE INSERT ON audit "
                          "WHEN NEW.action='resource-authorization.replace' "
                          "BEGIN SELECT RAISE(ABORT,'injected'); END")
        before = self.dump()
        with self.assertRaises(sqlite3.IntegrityError):
            self.replace()
        self.assertEqual(self.dump(), before)
        self.assertEqual(self.store.snapshot("project", "p")["revision"], 0)
        self.assertFalse(self.conn.in_transaction)
        self.assertEqual(self.audit_count(), 0)

    # ---- 过期/撤销/无授权 ---------------------------------------------
    def test_expiry_rejects_later_use_and_preserves_history(self):
        self.replace(expires_at=self.expiry(1_000))
        self.assertTrue(self.authorize()["allowed"])
        self.clock.advance(wall=1_001)
        self.failure("RESOURCE_AUTHORIZATION_EXPIRED", lambda: self.authorize())
        snap = self.store.snapshot("project", "p")
        self.assertTrue(snap["managed"])
        self.assertTrue(snap["expired"])
        self.assertFalse(snap["valid"])
        self.assertEqual(snap["revision"], 1)  # 历史不删除；查询拒绝而不是删历史
        again = self.replace(rev=1, op="op-2", resources=[FREE], expires_at=self.expiry(2_000))
        self.assertTrue(again["applied"])
        self.assertEqual(again["applied_revision"], 2)
        self.assertTrue(self.authorize()["allowed"])

    def test_authorize_honors_explicit_now(self):
        self.replace(expires_at=self.expiry(1_000))
        now = self.clock.now_ms()
        self.assertTrue(self.authorize(now=now)["allowed"])
        with self.assertRaises(LedgerError) as caught:
            self.authorize(now=now + 1_001)
        self.assertEqual(caught.exception.code, "RESOURCE_AUTHORIZATION_EXPIRED")
        self.failure("RESOURCE_AUTHORIZATION_INVALID", lambda: self.authorize(now="later"))
        self.failure("RESOURCE_AUTHORIZATION_INVALID", lambda: self.authorize(now=-5))

    def test_replace_requires_strict_future_expiry(self):
        now = self.clock.now_ms()
        for bad in (now, now - 1):
            with self.subTest(expires_at=bad):
                self.failure("RESOURCE_AUTHORIZATION_INVALID",
                             lambda: self.store.replace("project", "p", resources=[FREE],
                                                        expires_at=bad, expected_revision=0,
                                                        operation_id="exp1",
                                                        actor="operator", reason="explicit"))
        for bad in (True, "123", None, 1.0, []):
            with self.subTest(expires_at=bad):
                self.failure("RESOURCE_AUTHORIZATION_INVALID",
                             lambda: self.store.replace("project", "p", resources=[FREE],
                                                        expires_at=bad, expected_revision=0,
                                                        operation_id="exp2",
                                                        actor="operator", reason="explicit"))

    def test_revoke_empty_resources_denies_all_models(self):
        out = self.replace(resources=[])
        self.assertTrue(out["applied"])
        snap = self.store.snapshot("project", "p")
        self.assertTrue(snap["managed"])
        self.assertEqual(snap["resources"], [])
        self.assertEqual(snap["revision"], 1)
        self.failure("RESOURCE_AUTHORIZATION_REVOKED", lambda: self.authorize())
        self.assertTrue(self.replace(rev=1, op="op-2", resources=[FREE])["applied"])

    def test_revoke_then_authorize_denies_previously_allowed_model(self):
        self.replace(resources=[FREE])
        self.assertTrue(self.authorize()["allowed"])
        self.replace(rev=1, op="revoke", resources=[])
        self.failure("RESOURCE_AUTHORIZATION_REVOKED", lambda: self.authorize())
        self.assertEqual(self.store.snapshot("project", "p")["revision"], 2)

    def test_no_authorization_is_missing(self):
        self.failure("RESOURCE_AUTHORIZATION_MISSING", lambda: self.authorize())
        self.failure("RESOURCE_AUTHORIZATION_MISSING",
                     lambda: self.authorize(model="provider/anything", preset="elastic"))

    # ---- paid/预算预设矩阵 ---------------------------------------------
    def test_paid_only_in_elastic_and_latency_first_with_independent_authorization(self):
        self.replace(resources=[PAID])
        self.failure("RESOURCE_AUTHORIZATION_PRESET_REJECTED",
                     lambda: self.authorize(model="provider/paid", preset="budget-first"))
        self.assertTrue(self.authorize(model="provider/paid", preset="elastic")["allowed"])
        self.assertTrue(self.authorize(model="provider/paid", preset="latency-first")["allowed"])
        # 非独立授权覆盖的付费模型不可在elastic下“免费”使用
        self.failure("RESOURCE_AUTHORIZATION_MISSING",
                     lambda: self.authorize(model="provider/other-paid", preset="elastic"))

    def test_budget_first_requires_controlled_no_extra_cost(self):
        uncontrolled = {"model": "provider/u", "cost": "no-extra-cost",
                        "evidence": "docs/u", "internal_calls_controlled": False}
        self.replace(resources=[uncontrolled])
        self.failure("RESOURCE_AUTHORIZATION_PRESET_REJECTED",
                     lambda: self.authorize(model="provider/u", preset="budget-first"))
        self.assertTrue(self.authorize(model="provider/u", preset="elastic")["allowed"])
        self.assertTrue(self.authorize(model="provider/u", preset="latency-first")["allowed"])

    def test_budget_first_allows_controlled_free_but_rejects_paid(self):
        self.replace(op="op-b", resources=[FREE, PAID])
        for preset in ("budget-first", "elastic", "latency-first"):
            self.assertTrue(self.authorize(model="provider/free", preset=preset)["allowed"])
        self.failure("RESOURCE_AUTHORIZATION_PRESET_REJECTED",
                     lambda: self.authorize(model="provider/paid", preset="budget-first"))
        self.assertTrue(self.authorize(model="provider/paid", preset="elastic")["allowed"])

    # ---- 严格形状/未知字段/bool ----------------------------------------
    def test_resource_entries_are_strict(self):
        base = dict(FREE)
        bad_resources = [
            [None], [True], ["provider/free"], [{}],
            [{**base, "extra": 1}],
            [{k: v for k, v in base.items() if k != "model"}],
            [{**base, "model": ""}],
            [{**base, "model": " x"}],
            [{**base, "cost": "ultra-cheap"}],
            [{**base, "cost": True}],
            [{**base, "evidence": ""}],
            [{**base, "evidence": "   "}],
            [{**base, "evidence": None}],
            [{**base, "internal_calls_controlled": 1}],
            [{**base, "internal_calls_controlled": "true"}],
            [{**base, "internal_calls_controlled": None}],
            [base, FREE],  # 模型重复
        ]
        for resources in bad_resources:
            with self.subTest(resources=resources):
                self.failure("RESOURCE_AUTHORIZATION_INVALID",
                             lambda: self.replace(resources=resources, op="shape-1"))
        for not_list in (None, {}, "x", FREE):
            with self.subTest(not_list=not_list):
                self.failure("RESOURCE_AUTHORIZATION_INVALID",
                             lambda: self.store.replace("project", "p", resources=not_list,
                                                        expires_at=self.expiry(),
                                                        expected_revision=0,
                                                        operation_id="shape-2",
                                                        actor="operator", reason="explicit"))

    def test_unknown_scope_preset_model_now_rejected(self):
        for scope in ("group", "", 1):
            self.failure("RESOURCE_AUTHORIZATION_INVALID", lambda: self.replace(scope=scope))
            self.failure("RESOURCE_AUTHORIZATION_INVALID", lambda: self.snapshot(scope=scope))
            self.failure("RESOURCE_AUTHORIZATION_INVALID", lambda: self.authorize(scope=scope))
        for preset in ("fast", "", None, 1):
            self.failure("RESOURCE_AUTHORIZATION_INVALID", lambda: self.authorize(preset=preset))
        for model in ("", " x", None, [], True):
            self.failure("RESOURCE_AUTHORIZATION_INVALID", lambda: self.authorize(model=model))

    def test_bad_scope_id_rejected(self):
        for scope_id in ("", " ", "p\nq", "x" * 501):
            self.failure("RESOURCE_AUTHORIZATION_INVALID",
                         lambda: self.replace(scope_id=scope_id))
            self.failure("RESOURCE_AUTHORIZATION_INVALID",
                         lambda: self.snapshot(scope_id=scope_id))
            self.failure("RESOURCE_AUTHORIZATION_INVALID",
                         lambda: self.authorize(scope_id=scope_id))

    def test_bad_replace_arguments_do_not_change_ledger(self):
        cases = ({"resources": {}}, {"resources": "x"}, {"expires_at": "1"},
                 {"expires_at": 0}, {"expected_revision": True},
                 {"expected_revision": "1"}, {"operation_id": ""},
                 {"operation_id": []}, {"actor": ""}, {"reason": None})
        for delta in cases:
            with self.subTest(delta=delta):
                kw = dict(resources=[FREE], expires_at=self.expiry(), expected_revision=0,
                          operation_id="bad", actor="operator", reason="explicit")
                kw.update(delta)
                self.failure("RESOURCE_AUTHORIZATION_INVALID",
                             lambda: self.store.replace("project", "p",
                                                        resources=kw["resources"],
                                                        expires_at=kw["expires_at"],
                                                        expected_revision=kw["expected_revision"],
                                                        operation_id=kw["operation_id"],
                                                        actor=kw["actor"], reason=kw["reason"]))

    # ---- 跨scope/归属 --------------------------------------------------
    def test_cross_scope_isolation_and_no_parent_model_guess(self):
        self.replace(scope="project", scope_id="p")  # 项目授权FREE
        # 任务无自身授权：store绝不代猜/回退项目授权（回退是父的显式决策）
        self.failure("RESOURCE_AUTHORIZATION_MISSING",
                     lambda: self.authorize(scope="task", scope_id=self.task["id"]))
        other = {"model": "provider/task", "cost": "no-extra-cost",
                 "evidence": "docs/task", "internal_calls_controlled": True}
        self.replace(scope="task", scope_id=self.task["id"], resources=[other], op="task-1")
        self.failure("RESOURCE_AUTHORIZATION_MISSING",
                     lambda: self.authorize(scope="task", scope_id=self.task["id"],
                                            model="provider/free"))
        self.assertTrue(self.authorize(scope="task", scope_id=self.task["id"],
                                       model="provider/task")["allowed"])
        # 项目快照不受任务/协调者影响
        self.assertEqual(self.store.snapshot("project", "p")["revision"], 1)
        # 同scope_id不同scope互不碰撞
        self.replace(scope="coordinator", scope_id=self.task["id"], op="coord-1")
        self.assertEqual(self.store.snapshot("coordinator", self.task["id"])["revision"], 1)
        self.assertEqual(self.store.snapshot("task", self.task["id"])["revision"], 1)

    def test_task_explicit_revocation_is_not_a_missing_fallback_signal(self):
        self.replace(scope="project", scope_id="p")  # 项目授权FREE
        self.replace(scope="task", scope_id=self.task["id"], op="task-revoke", resources=[])
        with self.assertRaises(LedgerError) as caught:
            self.authorize(scope="task", scope_id=self.task["id"])
        self.assertEqual(caught.exception.code, "RESOURCE_AUTHORIZATION_REVOKED")
        snap = self.store.snapshot(scope="task", scope_id=self.task["id"])
        self.assertTrue(snap["managed"])  # 父据此不回退到项目授权
        self.assertEqual(snap["resources"], [])

    def test_coordinator_scope_is_independent_session(self):
        self.replace(scope="coordinator", scope_id="ses_coord", op="coord-1")
        snap = self.store.snapshot("coordinator", "ses_coord")
        self.assertTrue(snap["managed"])
        self.assertIsNone(snap["project"])
        self.assertTrue(self.authorize(scope="coordinator", scope_id="ses_coord")["allowed"])
        # 项目scope_id同名不与coordinator混淆
        self.failure("RESOURCE_AUTHORIZATION_MISSING",
                     lambda: self.authorize(scope="coordinator", scope_id="p"))

    def test_task_project_change_is_corrupt_not_reassignment(self):
        self.replace(scope="task", scope_id=self.task["id"], op="task-1")
        self.conn.execute("UPDATE tasks SET project='q' WHERE id=?", (self.task["id"],))
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT",
                     lambda: self.snapshot(scope="task", scope_id=self.task["id"]))
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT",
                     lambda: self.authorize(scope="task", scope_id=self.task["id"]))

    def test_nonexistent_task_rejected(self):
        for kind in ("replace", "snapshot", "authorize"):
            with self.subTest(kind=kind):
                before = self.dump()
                with self.assertRaises(LedgerError) as caught:
                    if kind == "replace":
                        self.replace(scope="task", scope_id="T-absent")
                    elif kind == "snapshot":
                        self.snapshot(scope="task", scope_id="T-absent")
                    else:
                        self.authorize(scope="task", scope_id="T-absent")
                self.assertEqual(caught.exception.code, "RESOURCE_AUTHORIZATION_TASK_MISSING")
                self.assertEqual(self.dump(), before)

    # ---- 坏记录失败关闭 ------------------------------------------------
    def test_corrupt_head_or_version_fails_closed(self):
        self.replace()
        key = _key("head", "project", "p")
        raw = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()[0]
        for value in ("null", "{broken", "[]",
                      json.dumps({"schema": 1, "revision": True, "scope": "project",
                                  "scope_id": "p", "project": "p"})):
            self.conn.execute("UPDATE meta SET value=? WHERE key=?", (value, key))
            self.failure("RESOURCE_AUTHORIZATION_CORRUPT",
                         lambda: self.store.snapshot("project", "p"))
        self.conn.execute("UPDATE meta SET value=? WHERE key=?", (raw, key))
        self.conn.execute("UPDATE meta SET value='{}' WHERE key=?",
                          (_key("version", "project", "p", 1),))
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT", lambda: self.replace(rev=1, op="next"))
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT",
                     lambda: self.store.snapshot("project", "p"))

    def test_missing_head_or_referenced_version_is_not_unmanaged(self):
        self.replace()
        self.conn.execute("DELETE FROM meta WHERE key=?", (_key("head", "project", "p"),))
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT",
                     lambda: self.store.snapshot("project", "p"))

    def test_missing_version_is_not_a_successful_replay(self):
        self.replace()
        self.conn.execute("DELETE FROM meta WHERE key=?", (_key("version", "project", "p", 1),))
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT", lambda: self.replace())
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT",
                     lambda: self.store.snapshot("project", "p"))

    def test_corrupt_operation_index_cannot_be_overwritten(self):
        self.replace()
        self.conn.execute("UPDATE meta SET value='{}' WHERE key=?",
                          (_key("operation", "op-1"),))
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT", lambda: self.replace())

    def test_deleted_operation_index_does_not_allow_identity_reuse(self):
        self.replace()
        self.conn.execute("DELETE FROM meta WHERE key=?",
                          (_key("operation", "op-1"),))
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT", lambda: self.replace(rev=1))
        self.failure("RESOURCE_AUTHORIZATION_CORRUPT",
                     lambda: self.store.snapshot("project", "p"))

    def test_valid_shape_version_tampering_is_detected(self):
        self.replace()
        key = _key("version", "project", "p", 1)
        original = self.store._load(key)
        for field, value in (("reason", "changed"), ("actor", "changed"),
                             ("expires_at", self.expiry(999)), ("resources", [PAID]),
                             ("scope", "task")):
            record = {**original, field: value}
            self.conn.execute("UPDATE meta SET value=? WHERE key=?",
                              (json.dumps(record), key))
            self.failure("RESOURCE_AUTHORIZATION_CORRUPT",
                         lambda: self.store.snapshot("project", "p"))

    # ---- 重启/备份/命名空间/业务事实保全 --------------------------------
    def test_backup_reopen_preserves_versions_and_idempotent_replay(self):
        self.replace()
        self.replace(scope="task", scope_id=self.task["id"], op="task-1")
        restored = sqlite3.connect(self.home / "restored.db", isolation_level=None)
        self.addCleanup(restored.close)
        self.conn.backup(restored)
        store = ResourceAuthorizationStore(restored, self.clock)
        self.assertEqual(store.snapshot("project", "p"), self.store.snapshot("project", "p"))
        self.assertEqual(store.snapshot("task", self.task["id"]),
                         self.store.snapshot("task", self.task["id"]))
        before = restored.total_changes
        replay = store.replace("project", "p", resources=[FREE], expires_at=self.expiry(),
                               expected_revision=0, operation_id="op-1",
                               actor="operator", reason="explicit")
        self.assertFalse(replay["applied"])
        self.assertEqual(replay["applied_revision"], 1)
        self.assertEqual(restored.total_changes, before)

    def test_namespace_keys_are_json_arrays_and_do_not_collide(self):
        self.assertEqual(_key("version", "project", "p", 1),
                         PREFIX + "version:" + json.dumps(["project", "p", 1],
                                                          ensure_ascii=False,
                                                          separators=(",", ":")))
        self.assertIn(PREFIX, _key("head", "project", "p"))
        self.assertNotEqual(_key("head", "project", "p"), _key("head", "task", "p"))
        self.assertNotEqual(_key("head", "project", "p"),
                            _key("head", "coordinator", "p"))
        self.replace()
        count = self.conn.execute(
            "SELECT COUNT(*) FROM meta WHERE key LIKE 'resource-authorization:v1:%'"
        ).fetchone()[0]
        self.assertEqual(count, 3)  # version + head + operation

    def test_namespace_coexists_with_delivery_policy_draft(self):
        from oc_orch.adapters.delivery_policy import DeliveryPolicyStore
        from tests.test_delivery_policy import policy as dp_policy

        dp = DeliveryPolicyStore(self.conn, self.clock)
        dp.set_project_default("p", policy=dp_policy(), expected_revision=0,
                               operation_id="dp-1", actor="operator", reason="explicit")
        self.replace()
        self.assertTrue(dp.project_snapshot("p")["managed"])
        self.assertEqual(self.store.snapshot("project", "p")["revision"], 1)
        dp_keys = self.conn.execute(
            "SELECT key FROM meta WHERE key LIKE 'delivery-policy-draft:v1:%'").fetchall()
        self.assertTrue(dp_keys)
        self.assertEqual(self.store.snapshot("project", "p")
                         ["version"]["resources"][0]["cost"], "no-extra-cost")

    def test_replaces_preserve_inflight_and_all_business_facts(self):
        att = self.ledger.create_attempt(self.task["id"], directory=str(self.home / "site"),
            session_id="ses_worker", dispatch_msg_id="msg_original",
            metadata={"dispatch": {"prompt": "frozen", "create": {}}})
        before = self.business()
        self.replace(scope="project", scope_id="p")
        self.replace(scope="task", scope_id=self.task["id"], op="task-1")
        self.replace(scope="coordinator", scope_id="ses_coord", op="coord-1")
        self.assertEqual(self.business(), before)
        self.assertEqual(self.ledger.get_attempt(att["id"])["metadata"]["dispatch"]["prompt"],
                         "frozen")

    # ---- 应用层契约 -----------------------------------------------------
    def test_application_service_delegates_contract(self):
        self.service.replace("project", "p", resources=[FREE, PAID], expires_at=self.expiry(),
                             expected_revision=0, operation_id="svc-1",
                             actor="operator", reason="explicit")
        self.assertEqual(self.service.snapshot("project", "p")["revision"], 1)
        decision = self.service.authorize("project", "p", model="provider/free",
                                          preset="budget-first")
        self.assertTrue(decision["allowed"])
        self.assertEqual(decision["authorization_revision"], 1)
        with self.assertRaises(LedgerError) as caught:
            self.service.authorize("project", "p", model="provider/paid",
                                   preset="budget-first")
        self.assertEqual(caught.exception.code, "RESOURCE_AUTHORIZATION_PRESET_REJECTED")


if __name__ == "__main__":
    unittest.main()

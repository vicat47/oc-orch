#!/usr/bin/env python3
"""S4-A schema 3 迁移演练：2→3 短事务、1→2→3 链式升级、备份与拒绝边界。

在隔离临时台账上验证：

- v1 台账链式升级到当前版本：session_inputs **与** responsibilities 同事务出现，
  历史回执仍是 unknown/outstanding=0，不重放、不判定 delivered；
- v2 台账执行 2→3 短事务：只加 responsibilities 结构，既有行**逐字节不变**，
  不回填责任（不猜历史输入已执行）、不改通知；
- 迁移失败整体回滚：v2 失败仍为 v2 且无 responsibilities；v1 失败仍为 v1、
  无 session_inputs、无 responsibilities（历史 1→2 语义保留）；
- 一致备份：迁移前用 backup API 得到的 v2 快照不含 responsibilities，升级后
  备份仍可读且保持 v2；
- expect_version 过旧（1/2）或未来版本在 **任何 DDL 之前** 拒绝，表结构与
  版本号不变；空目录不允许以旧版本打开；
- 新库初始化原子（fresh_hook 失败零半建表），并发生成同一空库幂等。

不调用真实服务、不读生产台账；全部使用临时目录。
"""
from __future__ import annotations

import os
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO))

from oc_orch import SCHEMA_VERSION
from oc_orch.db import init
from oc_orch.adapters.sqlite_db import (
    _RESPONSIBILITIES_TABLE,
    _RESPONSIBILITIES_INDEXES,
    _RESPONSIBILITIES_CONDITION_INDEX,
    _SCHEMA_V2_DELTA,
    _SESSION_INPUTS_OUTSTANDING_INDEX,
)

FIXTURE = HERE / "fixtures" / "schema-v1.sql"
T0 = 1_800_000_000_000


def _build_v1(home: str) -> sqlite3.Connection:
    os.makedirs(home, exist_ok=True)
    conn = sqlite3.connect(os.path.join(home, "ledger.db"), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(FIXTURE.read_text(encoding="utf-8"))
    conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', '1')")
    conn.execute(
        "INSERT INTO tasks(id, project, title, status, coordinator_session, "
        "created_at, updated_at) "
        "VALUES('T-1','p','sample','running',NULL,1000,1000)")
    conn.execute(
        "INSERT INTO attempts(id, task_id, generation, directory, status, "
        "created_at, updated_at) VALUES('A-1','T-1',1,'/tmp/site','running',"
        "1000,1000)")
    conn.execute(
        "INSERT INTO notifications(id, project, kind, target_session, task_id, "
        "attempt_id, dedupe_key, payload, state, attempts, max_attempts, "
        "next_attempt_at, message_id, created_at, metadata) "
        "VALUES('N-1','p','stall','ses_c','T-1','A-1','dk1','{\"s\":\"x\"}',"
        "'pending',0,3,1000,'msg_1',1000,'{}')")
    conn.execute(
        "INSERT INTO notifications(id, project, kind, target_session, "
        "dedupe_key, payload, state, attempts, max_attempts, next_attempt_at, "
        "message_id, created_at, sent_at, metadata) "
        "VALUES('N-2','p','result_ready','ses_c','dk2','{}','sent',1,3,1000,"
        "'msg_2',1000,1050,'{}')")
    conn.execute(
        "INSERT INTO notifications(id, project, kind, target_session, "
        "dedupe_key, payload, state, attempts, max_attempts, next_attempt_at, "
        "created_at, metadata) "
        "VALUES('N-3','p','stall','ses_c','dk3','{}','pending',0,3,1000,"
        "1000,'{}')")
    conn.commit()
    return conn


def _build_v2(home: str) -> sqlite3.Connection:
    """用 v1 夹具 + 公开 v2 增量 DDL 构造真实 v2 台账（不依赖旧代码）。"""
    conn = _build_v1(home)
    conn.executescript(_SCHEMA_V2_DELTA)
    conn.execute(_SESSION_INPUTS_OUTSTANDING_INDEX)
    # 一条已存在的 v2 物理输入回执：2→3 必须原样保留。
    conn.execute(
        "INSERT INTO session_inputs(input_id, session_id, message_id, source, "
        "intent, outstanding, revision, owner, body, body_hash, "
        "notification_ids, admission_state, prepared_at, created_at, updated_at, "
        "metadata) VALUES('I-v2','ses_c','msg_v2','automatic_wake','execute',0,1,"
        "'sup','body-v2','hash-v2','[\"N-1\"]','delivered',1000,1000,1000,"
        "'{\"keep\":true}')")
    conn.execute(
        "UPDATE meta SET value='2' WHERE key='schema_version'")
    conn.commit()
    return conn


def _raw(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


def _tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    return [r["name"] for r in rows]


def _indexes(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? "
        "ORDER BY name", (table,)).fetchall()
    return [r["name"] for r in rows]


def _version(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    return row["value"] if row else None


def _dump(conn: sqlite3.Connection, table: str) -> list[tuple]:
    rows = conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
    return [tuple(r) for r in rows]


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="oc-orch-resp-migrate-",
                                               dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name

    def build(self, kind: str):
        conn = _build_v1(self.home) if kind == "v1" else _build_v2(self.home)
        self.addCleanup(conn.close)
        return conn


class TestSchemaDefinition(_Base):
    def test_responsibility_ddl_matches_contract(self):
        stmts = (_RESPONSIBILITIES_TABLE + "".join(_RESPONSIBILITIES_INDEXES)
                 + _RESPONSIBILITIES_CONDITION_INDEX)
        for name in ("responsibilities", "task_id", "condition_key", "generation",
                     "result_version", "owner", "target_session", "state",
                     "wait_kind", "wait_condition", "next_action", "next_check_at",
                     "deadline", "revision", "budget_used", "max_budget",
                     "evidence", "prior_id", "successor_id", "closed_at",
                     "close_reason"):
            self.assertIn(name, stmts)
        self.assertEqual(SCHEMA_VERSION, 3)

    def test_fresh_init_creates_v3_with_both_tables(self):
        conn = init(os.path.join(self.home, "fresh"))
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        tables = _tables(conn)
        self.assertIn("session_inputs", tables)
        self.assertIn("responsibilities", tables)
        self.assertIn("responsibilities_condition", _indexes(conn, "responsibilities"))
        self.assertIn("responsibilities_due", _indexes(conn, "responsibilities"))

    def test_unclosed_condition_unique_at_db_level(self):
        conn = init(os.path.join(self.home, "unique"))
        self.addCleanup(conn.close)
        base = ("INSERT INTO responsibilities(id, project, task_id, owner, state, "
                "wait_kind, wait_condition, next_action, next_check_at, deadline, "
                "condition_key, generation, result_version, created_at, updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        args = ("R-1", "p", "T-1", "o", "waiting_input", "w", "c", "n",
                1, 2, "ck", 1, None, 1, 1)
        conn.execute(base, args)
        # 同 task/condition/generation，result_version NULL 视为相同（ifnull 归一）
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(base, ("R-2", "p", "T-1", "o", "waiting_input", "w", "c",
                                "n", 1, 2, "ck", 1, None, 1, 1))
        # 已关闭行不占唯一约束
        conn.execute("UPDATE responsibilities SET closed_at=1, state='done' "
                     "WHERE id='R-1'")
        conn.execute(base, ("R-3", "p", "T-1", "o", "waiting_input", "w", "c",
                            "n", 1, 2, "ck", 1, None, 1, 1))
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM responsibilities").fetchone()[0], 2)


class TestV1Chain(_Base):
    def test_v1_upgrades_to_v3_and_keeps_receipt_semantics(self):
        v1 = self.build("v1")
        v1.close()
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        tables = _tables(conn)
        self.assertIn("session_inputs", tables)
        self.assertIn("responsibilities", tables)
        # 历史回执语义保持：unknown、不占未决槽、不重放
        receipts = {}
        for r in conn.execute("SELECT * FROM session_inputs").fetchall():
            members = json.loads(r["notification_ids"])
            for member in members:
                receipts[member] = dict(r)
        self.assertIn("N-1", receipts)
        self.assertIn("N-2", receipts)
        self.assertNotIn("N-3", receipts, "无 message_id 不登记")
        for r in receipts.values():
            self.assertEqual(r["admission_state"], "unknown")
            self.assertEqual(r["outstanding"], 0)
        n2 = conn.execute(
            "SELECT state, attempts FROM notifications WHERE id='N-2'").fetchone()
        self.assertEqual((n2["state"], n2["attempts"]), ("sent", 1))
        # 迁移不回填责任（不猜历史执行）
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM responsibilities").fetchone()[0], 0)
        actions = [r["detail"] for r in conn.execute(
            "SELECT detail FROM audit WHERE action='schema.migrate' "
            "ORDER BY id").fetchall()]
        self.assertEqual(len(actions), 2, "1→2 与 2→3 都应留审计")

    def test_v1_chain_failure_rolls_back_to_v1(self):
        v1 = self.build("v1")
        v1.close()
        with self.assertRaises(RuntimeError):
            init(self.home, migrate_hook=lambda c: (_ for _ in ()).throw(
                RuntimeError("injected-2-to-3-failure")))
        raw = _raw(os.path.join(self.home, "ledger.db"))
        try:
            self.assertEqual(_version(raw), "1", "链式升级失败必须整体回到 v1")
            tables = _tables(raw)
            self.assertNotIn("session_inputs", tables)
            self.assertNotIn("responsibilities", tables)
            self.assertEqual(raw.execute(
                "SELECT message_id FROM notifications WHERE id='N-1'"
            ).fetchone()["message_id"], "msg_1")
        finally:
            raw.close()
        conn = init(self.home)  # 回滚后可继续升级
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")


class TestV2ShortMigration(_Base):
    def test_v2_to_v3_preserves_rows_and_backfills_nothing(self):
        v2 = self.build("v2")
        before_inputs = _dump(v2, "session_inputs")
        before_notifs = _dump(v2, "notifications")
        before_tasks = _dump(v2, "tasks")
        v2.close()
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        self.assertIn("responsibilities", _tables(conn))
        self.assertEqual(_dump(conn, "session_inputs"), before_inputs,
                         "2→3 不得改写既有输入回执")
        self.assertEqual(_dump(conn, "notifications"), before_notifs)
        self.assertEqual(_dump(conn, "tasks"), before_tasks)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM responsibilities").fetchone()[0], 0,
            "迁移不回填责任：历史缺归属事实不得被伪造成已执行/已处理")

    def test_v2_migration_failure_rolls_back_and_is_retryable(self):
        v2 = self.build("v2")
        before = _tables(v2)
        v2.close()
        with self.assertRaises(RuntimeError):
            init(self.home, migrate_hook=lambda c: (_ for _ in ()).throw(
                RuntimeError("injected-2-to-3")))
        raw = _raw(os.path.join(self.home, "ledger.db"))
        try:
            self.assertEqual(_version(raw), "2")
            self.assertEqual(_tables(raw), before, "失败迁移不得残留半建表")
        finally:
            raw.close()
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        self.assertIn("responsibilities", _tables(conn))

    def test_backup_before_migration_stays_v2_and_readable(self):
        v2 = self.build("v2")
        bak_path = os.path.join(self.home, "ledger-v2-backup.db")
        bak = sqlite3.connect(bak_path)
        try:
            v2.backup(bak)
        finally:
            bak.close()
        v2.close()
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        check = _raw(bak_path)
        try:
            self.assertEqual(_version(check), "2")
            self.assertNotIn("responsibilities", _tables(check))
            self.assertEqual(check.execute("PRAGMA integrity_check").fetchone()[0],
                             "ok")
        finally:
            check.close()

    def test_v2_to_v3_audit_records_no_backfill(self):
        v2 = self.build("v2")
        v2.close()
        conn = init(self.home)
        self.addCleanup(conn.close)
        row = conn.execute(
            "SELECT from_state, to_state, detail FROM audit "
            "WHERE action='schema.migrate' ORDER BY id DESC LIMIT 1").fetchone()
        self.assertEqual((row["from_state"], row["to_state"]), ("2", "3"))
        detail = json.loads(row["detail"])
        self.assertIs(detail["backfilled"], False)
        self.assertEqual(detail["responsibilities"], 0)


class TestRejection(_Base):
    def test_future_schema_rejected_before_ddl(self):
        v2 = self.build("v2")
        v2.execute("UPDATE meta SET value='99' WHERE key='schema_version'")
        v2.commit()
        before = _tables(v2)
        v2.close()
        with self.assertRaises(RuntimeError) as ctx:
            init(self.home)
        self.assertIn("不兼容", str(ctx.exception))
        raw = _raw(os.path.join(self.home, "ledger.db"))
        try:
            self.assertEqual(_tables(raw), before)
            self.assertEqual(_version(raw), "99")
        finally:
            raw.close()

    def test_old_expect_versions_rejected_on_v3_without_side_effects(self):
        v2 = self.build("v2")
        v2.close()
        conn = init(self.home)
        before = _tables(conn)
        conn.close()
        for stale in (1, 2):
            with self.assertRaises(RuntimeError):
                init(self.home, expect_version=stale)
        raw = _raw(os.path.join(self.home, "ledger.db"))
        try:
            self.assertEqual(_version(raw), "3")
            self.assertEqual(_tables(raw), before)
        finally:
            raw.close()

    def test_expect_v2_on_v1_does_not_migrate(self):
        v1 = self.build("v1")
        v1.close()
        with self.assertRaises(RuntimeError):
            init(self.home, expect_version=2)
        raw = _raw(os.path.join(self.home, "ledger.db"))
        try:
            self.assertEqual(_version(raw), "1")
            self.assertNotIn("session_inputs", _tables(raw))
            self.assertNotIn("responsibilities", _tables(raw))
        finally:
            raw.close()

    def test_empty_home_cannot_open_as_legacy(self):
        with self.assertRaises(RuntimeError):
            init(os.path.join(self.home, "empty"), expect_version=2)


class TestFreshAtomicity(_Base):
    def test_fresh_failure_leaves_no_partial_tables_then_recovers(self):
        home = os.path.join(self.home, "atomic")

        def boom(conn):
            raise RuntimeError("injected-fresh-failure")

        with self.assertRaises(RuntimeError):
            init(home, fresh_hook=boom)
        raw = _raw(os.path.join(home, "ledger.db"))
        try:
            self.assertEqual(_tables(raw), [])
        finally:
            raw.close()
        conn = init(home)
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        self.assertIn("responsibilities", _tables(conn))

    def test_reopen_v3_is_idempotent(self):
        conn = init(os.path.join(self.home, "idem"))
        self.addCleanup(conn.close)
        conn.close()
        again = init(os.path.join(self.home, "idem"))
        self.addCleanup(again.close)
        self.assertEqual(_version(again), "3")
        self.assertEqual(
            _indexes(again, "responsibilities").count("responsibilities_condition"), 1)


class TestBackupAndExport(_Base):
    """真 SQLite 一致备份（可恢复）与 JSON export（不可恢复）边界。"""

    def _build_v3_with_facts(self):
        conn = init(os.path.join(self.home, "factful"))
        self.addCleanup(conn.close)
        home = os.path.join(self.home, "factful")
        from oc_orch.adapters.sqlite_ledger import Ledger
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        from tests.fakes import FakeClock
        clock = FakeClock(wall_ms=T0)
        ledger = Ledger(conn, home, clock=clock)
        ledger.create_task(project="p", title="t", task_id="T-1",
                           coordinator_session="ses_c")
        ledger.create_attempt("T-1", attempt_id="A-1", directory=home,
                              session_id="ses_w", generation=1, status="running")
        conn.execute(
            "INSERT INTO session_inputs(input_id, session_id, message_id, source, "
            "intent, outstanding, revision, owner, body, body_hash, "
            "notification_ids, admission_state, prepared_at, created_at, "
            "updated_at, metadata) VALUES('I-1','ses_w','msg_1','managed_input',"
            "'execute',0,1,'coord','body','hash','[]','delivered',?,?,?,'{}')",
            (T0, T0, T0))
        note = ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c", dedupe_key="k-1",
            payload={"summary": "x", "result_version": "rv1-1"},
            project="p", task_id="T-1", attempt_id="A-1")
        ledger.ack_notifications([note["id"]], actor="test")
        store = ResponsibilityStore(ledger)
        resp = store.open_responsibility(
            owner="coord", project="p", task_id="T-1", attempt_id="A-1",
            generation=1, input_id="I-1", session_id="ses_w",
            message_id="msg_1", target_session="ses_c",
            state="awaiting_review", wait_kind="review",
            wait_condition="business-report-unknown",
            next_action="核对回报", next_check_at=T0 + 1000,
            deadline=T0 + 60_000, condition_key="ck-1",
            evidence={"execution": {"ended_at": T0 - 1}}, max_budget=3)
        store.update_responsibility(resp["id"], expected_revision=1,
                                    fields={"budget_used": 2})
        pub = store.publish_responsibility(
            resp["id"], expected_revision=2, payload={"summary": "待核收"})
        ledger.ack_notifications([pub["notification"]["id"]], actor="test")
        return conn, store, resp["id"], pub["notification"]["id"]

    def test_sqlite_backup_is_consistent_readonly_and_restorable(self):
        import shutil
        from oc_orch.adapters.sqlite_db import backup_to, inspect_backup
        conn, store, rid, nid = self._build_v3_with_facts()
        bak = os.path.join(self.home, "backup.db")
        backup_to(conn, bak)
        info = inspect_backup(bak)
        self.assertTrue(info["ok"], info)
        self.assertEqual(info["integrity"], "ok")
        self.assertEqual(info["schema_version"], "3")
        self.assertEqual(info["tables"]["session_inputs"], 1)
        self.assertEqual(info["tables"]["responsibilities"], 1)
        self.assertEqual(info["tables"]["notifications"], 2)
        self.assertEqual(info["tables"]["tasks"], 1)
        # 只读核验：对备份写入必须被拒绝，且不产生旁写
        raw = sqlite3.connect("file:" + os.path.abspath(bak) + "?mode=ro", uri=True)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                raw.execute("INSERT INTO meta(key,value) VALUES('x','1')")
        finally:
            raw.close()
        # 隔离 restore：schema3 事实完整，可继续对账且不重投
        restored = os.path.join(self.home, "restored")
        os.makedirs(restored)
        shutil.copyfile(bak, os.path.join(restored, "ledger.db"))
        rconn = init(restored)
        self.addCleanup(rconn.close)
        self.assertEqual(_version(rconn), "3")
        from oc_orch.adapters.sqlite_ledger import Ledger
        from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
        from tests.fakes import FakeClock
        ledger2 = Ledger(rconn, restored, clock=FakeClock(wall_ms=T0))
        store2 = ResponsibilityStore(ledger2)
        row = store2.get_responsibility(rid)
        self.assertIsNotNone(row)
        self.assertEqual((row["state"], row["budget_used"], row["max_budget"]),
                         ("awaiting_review", 2, 3))
        self.assertEqual(ledger2.get_notification(nid)["state"], "acked")
        self.assertTrue(store2.list_due_responsibilities(now=T0 + 2000))
        # 同一事实再发布：命中已 ACK 的去重键，不新建通知（不重投）
        out = store2.publish_responsibility(
            rid, expected_revision=row["revision"], payload={"summary": "restore"})
        self.assertFalse(out["published"])
        self.assertEqual(out["notification"]["id"], nid)
        self.assertEqual(len(ledger2.list_notifications(limit=100)), 2)

    def test_json_export_is_complete_but_not_a_backup(self):
        conn, store, rid, nid = self._build_v3_with_facts()
        from oc_orch.adapters.sqlite_ledger import Ledger
        ledger = Ledger(conn, os.path.join(self.home, "factful"))
        data = ledger.export()
        self.assertEqual(data["schema"], "oc-orch.export.v1")
        self.assertEqual(data["schema_version"], 3)
        self.assertIs(data["recoverable_backup"], False)
        for key in ("tasks", "attempts", "deliverables", "turns",
                    "notifications", "alerts", "leases", "audit"):
            self.assertIn(key, data, "既有 9 键必须保留")
        for key in ("meta", "events", "session_inputs", "responsibilities"):
            self.assertIn(key, data)
            self.assertIn(key, data["additive_keys"])
        self.assertEqual(data["meta"]["schema_version"], "3")
        self.assertEqual(len(data["session_inputs"]), 1)
        self.assertEqual(len(data["responsibilities"]), 1)
        self.assertEqual(data["session_inputs"][0]["notification_ids"], [])
        self.assertIsInstance(data["responsibilities"][0]["evidence"], dict)
        self.assertTrue(all(n["state"] == "acked"
                            for n in data["notifications"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)

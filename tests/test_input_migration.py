#!/usr/bin/env python3
"""S2 schema 迁移演练（B 组）：1→2 与 1→2→3 链式升级、备份、回滚与拒绝。

复用固封的 schema-v1.sql 夹具在隔离临时台账构造 v1 数据库：
- 用 conn.backup() 在 schema 改动前做一致备份，备份可读且不含新表；
- init 先版本检查再建 schema：future schema 无副作用拒绝、连接关闭；
- 显式迁移：新增 session_inputs、旧 notifications.message_id 登记为
  unknown 历史回执（outstanding=0）、不批量重放、不认定 delivered；
  v1 台账在同一事务内链式升到当前版本（SCHEMA_VERSION，responsibilities）；
- 迁移失败整体回滚（v1 失败后版本仍 1、无新表、数据不变）；
- 旧二进制（expect_version 过旧）打开新 schema 一律拒绝且无写入。

本文件断言以“当前 SCHEMA_VERSION”为准；历史 1→2 语义断言（unknown 回执、
不重放、失败回滚到 1、备份保持 1）保持不变。v2→v3 细节见
test_responsibility_migration.py。

全部在 /tmp 下临时目录，不触碰生产台账。
"""
from __future__ import annotations

import io
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO))  # 便于直接运行 python3 tests/test_x.py

from oc_orch.db import init
from oc_orch.ledger import Ledger
from tests.fakes import FakeClock

FIXTURE = HERE / "fixtures" / "schema-v1.sql"


def _build_v1(home: str) -> sqlite3.Connection:
    """用固封 schema-v1.sql 夹具构造 schema1 台账（不回退、不依赖旧代码）。"""
    os.makedirs(home, exist_ok=True)
    conn = sqlite3.connect(os.path.join(home, "ledger.db"),
                           isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(FIXTURE.read_text(encoding="utf-8"))
    conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', '1')")
    conn.execute(
        "INSERT INTO tasks(id, project, title, status, created_at, updated_at) "
        "VALUES('T-1','p','sample','dispatched',1000,1000)")
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
        "message_id, created_at, acked_at, metadata) "
        "VALUES('N-3','p','stall','ses_c','dk3','{}','acked',1,3,1000,"
        "'msg_3',1000,1200,'{}')")
    conn.execute(
        "INSERT INTO notifications(id, project, kind, target_session, "
        "dedupe_key, payload, state, attempts, max_attempts, next_attempt_at, "
        "created_at, metadata) "
        "VALUES('N-4','p','stall','ses_c','dk4','{}','pending',0,3,1000,"
        "1000,'{}')")
    conn.commit()
    return conn


def _tables(conn: sqlite3.Connection) -> list[str]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r["name"] for r in rows]


def _version(conn: sqlite3.Connection) -> str | None:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT value FROM meta WHERE key='schema_version'").fetchone()
    return row["value"] if row else None


def _raw_conn(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path, isolation_level=None)
    c.row_factory = sqlite3.Row
    return c


class MigrationBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="oc-orch-migrate-")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name

    def build_v1(self):
        self.v1 = _build_v1(self.home)
        self.addCleanup(self.v1.close)
        return self.v1


class TestFreshAndBackup(MigrationBase):
    def test_fresh_init_is_v2_with_session_inputs(self):
        clock = FakeClock(wall_ms=1_000_000)
        conn = init(os.path.join(self.home, "fresh"))
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        self.assertIn("session_inputs", _tables(conn))
        ledger = Ledger(conn, os.path.join(self.home, "fresh"), clock=clock)
        self.assertEqual(ledger.list_pending_inputs(outstanding=True), [])

    def test_backup_v1_is_consistent_and_survives_migration(self):
        self.build_v1()
        # 未 checkpoint 写入：同连接写入 marker 后再用 backup API 快照
        self.v1.execute(
            "INSERT INTO tasks(id, project, title, status, created_at, "
            "updated_at) VALUES('T-marker','p','marker','draft',1500,1500)")
        self.v1.commit()
        bak_path = os.path.join(self.home, "ledger-v1-backup.db")
        bak = sqlite3.connect(bak_path)
        try:
            self.v1.backup(bak)
        finally:
            bak.close()
        # 备份必须包含 marker（一致备份包含 WAL 中未 checkpoint 内容）
        check = _raw_conn(bak_path)
        try:
            self.assertEqual(check.execute(
                "PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(_version(check), "1")
            self.assertNotIn("session_inputs", _tables(check))
            marker = check.execute(
                "SELECT 1 FROM tasks WHERE id='T-marker'").fetchone()
            self.assertIsNotNone(marker, "backup API 快照必须含未 checkpoint 写入")
            n1 = check.execute(
                "SELECT message_id FROM notifications WHERE id='N-1'").fetchone()
            self.assertEqual(n1["message_id"], "msg_1")
        finally:
            check.close()
        # 原库迁移后，备份仍是 v1 且可读
        self.v1.close()
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        reopened = _raw_conn(bak_path)
        try:
            self.assertEqual(_version(reopened), "1", "备份保持 schema1 夹具")
        finally:
            reopened.close()


class TestMigration(MigrationBase):
    def test_v1_to_v2_imports_unknown_receipts_without_replay(self):
        self.build_v1()
        self.v1.close()
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        self.assertIn("session_inputs", _tables(conn))
        ledger = Ledger(conn, self.home, clock=FakeClock(wall_ms=1_000_000))
        # 只有 N-1(待核实)/N-2(已受理) 且 message_id 非空 → 登记 unknown 回执
        receipts = ledger.list_pending_inputs()
        by_note = {tuple(sorted(i["notification_ids"]))[0]: i
                   for i in receipts}
        self.assertEqual(set(by_note) & {"N-1", "N-2"}, {"N-1", "N-2"})
        self.assertNotIn("N-3", by_note, "acked 通知不登记为待核实回执")
        self.assertNotIn("N-4", by_note, "无 message_id 不登记")
        for r in receipts:
            self.assertEqual(r["admission_state"], "unknown",
                             "旧回执登记为 unknown，不认定 delivered")
            self.assertFalse(r["outstanding"], "历史回执不占用未决槽位")
        # 不批量重放：通知行状态/计数不变
        n2 = conn.execute(
            "SELECT state, attempts FROM notifications WHERE id='N-2'"
        ).fetchone()
        self.assertEqual(n2["state"], "sent")
        self.assertEqual(n2["attempts"], 1)
        # 迁移后同目标可正常预留（历史回执不挡未决槽）
        res = ledger.reserve_target_input("ses_c", notification_ids=["N-1"], owner="o")
        self.assertTrue(res["created"])

    def test_migration_failure_rolls_back_fully(self):
        self.build_v1()
        self.v1.close()
        with self.assertRaises(RuntimeError):
            init(self.home, migrate_hook=lambda c: (_ for _ in ()).throw(
                RuntimeError("injected-migration-failure")))
        raw = _raw_conn(os.path.join(self.home, "ledger.db"))
        try:
            self.assertEqual(_version(raw), "1", "失败迁移不得写版本")
            self.assertNotIn("session_inputs", _tables(raw))
            n1 = raw.execute(
                "SELECT message_id FROM notifications WHERE id='N-1'").fetchone()
            self.assertEqual(n1["message_id"], "msg_1")
            t = raw.execute("SELECT 1 FROM tasks WHERE id='T-1'").fetchone()
            self.assertIsNotNone(t)
        finally:
            raw.close()
        # 回滚后仍可正常迁移（台账可继续使用）
        conn = init(self.home)
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        self.assertIn("session_inputs", _tables(conn))


class TestVersionRejection(MigrationBase):
    def test_future_schema_rejected_without_side_effects(self):
        self.build_v1()
        self.v1.execute("UPDATE meta SET value='99' WHERE key='schema_version'")
        self.v1.commit()
        before = _tables(self.v1)
        self.v1.close()
        with self.assertRaises(RuntimeError) as ctx:
            init(self.home)
        self.assertIn("不兼容", str(ctx.exception))
        raw = _raw_conn(os.path.join(self.home, "ledger.db"))
        try:
            self.assertEqual(_tables(raw), before, "future schema 不得改动表结构")
            self.assertEqual(_version(raw), "99")
        finally:
            raw.close()

    def test_old_binary_cannot_write_new_schema(self):
        self.build_v1()
        self.v1.close()
        init(self.home)  # 迁到 v2
        with self.assertRaises(RuntimeError) as ctx:
            init(self.home, expect_version=1)
        self.assertIn("不兼容", str(ctx.exception))
        conn = init(self.home)  # 新代码可继续打开
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")
        # 失败的旧二进制打开没有留下任何写入
        self.assertIsNone(conn.execute(
            "SELECT 1 FROM meta WHERE key='old-binary-wrote'").fetchone())

    def test_empty_home_cannot_open_as_legacy(self):
        with self.assertRaises(RuntimeError):
            init(os.path.join(self.home, "empty"), expect_version=1)


class TestRound2Migration(MigrationBase):
    """round2：legacy 回执聚合、坏目标跳过、新库并发/失败原子性。"""

    def _build_v1_shared(self):
        conn = self.build_v1()  # N-1 msg_1 / N-2 msg_2 / N-3 acked / N-4 无 msg
        conn.execute(
            "INSERT INTO notifications(id, project, kind, target_session, "
            "dedupe_key, payload, state, attempts, max_attempts, next_attempt_at, "
            "message_id, created_at, metadata) "
            "VALUES('N-5','p','stall','ses_c','dk5','{}','pending',0,3,1000,"
            "'msg_shared',1000,'{}')")
        conn.execute(
            "INSERT INTO notifications(id, project, kind, target_session, "
            "dedupe_key, payload, state, attempts, max_attempts, next_attempt_at, "
            "message_id, created_at, metadata) "
            "VALUES('N-6','p','result_ready','ses_c','dk6','{}','sent',1,3,1000,"
            "'msg_shared',1000,'{}')")
        # 无 target 的目标不能迁移成伪目标
        conn.execute(
            "INSERT INTO notifications(id, project, kind, target_session, "
            "dedupe_key, payload, state, attempts, max_attempts, next_attempt_at, "
            "message_id, created_at, metadata) "
            "VALUES('N-7','p','stall',NULL,'dk7','{}','pending',0,3,1000,"
            "'msg_no_target',1000,'{}')")
        conn.commit()
        return conn

    def test_shared_message_aggregates_all_members_with_true_count(self):
        conn = self._build_v1_shared()
        conn.close()
        c = init(self.home)
        self.addCleanup(c.close)
        ledger = Ledger(c, self.home, clock=FakeClock(wall_ms=1_000_000))
        rows = ledger.list_pending_inputs()
        # msg_1 / msg_2 / msg_shared 三个物理输入（N-5 与 N-6 共用 msg_shared 聚合）

        by_msg = {r["message_id"]: r for r in rows}
        self.assertEqual(sorted(by_msg), ["msg_1", "msg_2", "msg_shared"],
                         "真实导入计数 = 物理输入组数，不是通知行数")
        shared = by_msg["msg_shared"]
        self.assertEqual(sorted(shared["notification_ids"]), ["N-5", "N-6"],
                         "共用 (session,message) 的通知聚合为完整成员集合")
        for r in rows:
            self.assertEqual(r["admission_state"], "unknown")
            self.assertFalse(r["outstanding"])
        # 无 target 的旧回执不迁移成伪目标
        self.assertIsNone(ledger.input_for_receipt("ses_c", "msg_no_target"))

    def test_two_connections_fresh_init_same_empty_home_idempotent(self):
        import threading as _t
        home2 = os.path.join(self.home, "race-fresh")
        barrier = _t.Barrier(2)
        results = {}

        def work(name):
            try:
                barrier.wait()
                conn = init(home2)
                results[name] = "ok"
                conn.close()
            except Exception as exc:  # noqa: BLE001
                results[name] = exc

        threads = [_t.Thread(target=work, args=("a",)),
                   _t.Thread(target=work, args=("b",))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(results, {"a": "ok", "b": "ok"})
        c = sqlite3.connect(os.path.join(home2, "ledger.db"))
        c.row_factory = sqlite3.Row
        try:
            self.assertEqual(c.execute(
                "SELECT value FROM meta WHERE key='schema_version'").fetchone()["value"],
                "3")
            row = c.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_inputs'"
            ).fetchone()
            self.assertIsNotNone(row)
            integrity = c.execute("PRAGMA integrity_check").fetchone()[0]
            self.assertEqual(integrity, "ok")
        finally:
            c.close()

    def test_fresh_init_failure_is_atomic_with_no_partial_tables(self):
        fresh = os.path.join(self.home, "atomic-fail")

        def boom(conn):
            raise RuntimeError("injected-fresh-failure")

        with self.assertRaises(RuntimeError):
            init(fresh, fresh_hook=boom)
        raw = sqlite3.connect(os.path.join(fresh, "ledger.db"))
        try:
            self.assertEqual(_tables(raw), [], "失败的新库初始化必须整体回滚（无半建表）")
        finally:
            raw.close()
        # 失败后可再次干净初始化

        conn = init(fresh)
        self.addCleanup(conn.close)
        self.assertEqual(_version(conn), "3")




if __name__ == "__main__":
    unittest.main(verbosity=2)
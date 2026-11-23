"""发布门：schema3新旧连接拒绝、同事务安装/回滚、备份与保留新事实。"""

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from oc_orch.adapters.sqlite_db import backup_to, connect, init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters import writer_guard as guard


class WriterGuardTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="writer-guard-", dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)
        self.task = self.ledger.create_task(project="p", title="preserve")

    def raw(self):
        conn = sqlite3.connect(str(Path(self.home) / "ledger.db"), isolation_level=None)
        self.addCleanup(conn.close)
        return conn

    def snapshot(self):
        return list(self.conn.iterdump())

    def test_default_init_has_no_implicit_guard_install(self):
        self.assertFalse(guard.check_writer_guard(self.conn))
        self.raw().execute("INSERT INTO meta(key,value) VALUES('legacy','unchanged')")

    def test_install_preserves_business_rows_and_adds_one_audit(self):
        tables = {t: [tuple(r) for r in self.conn.execute(f'SELECT * FROM "{t}"')]
                  for t in guard.TABLES if t not in ("meta", "audit")}
        meta = dict(self.conn.execute("SELECT key,value FROM meta"))
        old_audit = [tuple(r) for r in self.conn.execute("SELECT * FROM audit ORDER BY id")]
        self.assertTrue(guard.install_writer_guard(self.conn)["installed"])
        for t, rows in tables.items():
            self.assertEqual([tuple(r) for r in self.conn.execute(f'SELECT * FROM "{t}"')], rows)
        self.assertEqual(dict(self.conn.execute("SELECT key,value FROM meta")),
                         {**meta, guard.GUARD_KEY: guard.GUARD_VERSION})
        after = [tuple(r) for r in self.conn.execute("SELECT * FROM audit ORDER BY id")]
        self.assertEqual(after[:-1], old_audit)
        self.assertEqual(after[-1][3], "writer-contract.install")

    def test_old_connection_open_before_install_cannot_write(self):
        old = self.raw()
        old.execute("SELECT * FROM meta").fetchall()
        guard.install_writer_guard(self.conn)
        before = self.snapshot()
        with self.assertRaisesRegex(sqlite3.OperationalError, "oc_orch_writer_contract"):
            old.execute("UPDATE tasks SET title='old' WHERE id=?", (self.task["id"],))
        self.assertEqual(self.snapshot(), before)

    def test_all_business_tables_reject_old_insert_update_delete(self):
        guard.install_writer_guard(self.conn)
        old = self.raw()
        for table in guard.TABLES:
            col = self.conn.execute(f'PRAGMA table_info("{table}")').fetchone()[1]
            statements = (
                f'INSERT INTO "{table}" SELECT * FROM "{table}" WHERE 0',
                f'UPDATE "{table}" SET "{col}"="{col}" WHERE 0',
                f'DELETE FROM "{table}" WHERE 0',
            )
            for sql in statements:
                with self.subTest(table=table, statement=sql):
                    with self.assertRaisesRegex(sqlite3.OperationalError, "oc_orch_writer_contract"):
                        old.execute(sql)

    def test_new_connection_reopens_and_preserves_new_facts(self):
        guard.install_writer_guard(self.conn)
        self.ledger.set_meta("coordinator-control:project:p", "preserve-new-control")
        conn = init(self.home)
        self.addCleanup(conn.close)
        ledger = Ledger(conn, self.home)
        ledger.set_meta("new-receipt", "msg-original")
        self.assertEqual(ledger.get_meta("coordinator-control:project:p"), "preserve-new-control")
        self.assertEqual(self.ledger.get_meta("new-receipt"), "msg-original")

    def test_install_is_idempotent_zero_additional_write(self):
        guard.install_writer_guard(self.conn)
        before = self.snapshot()
        self.assertFalse(guard.install_writer_guard(self.conn)["installed"])
        self.assertEqual(self.snapshot(), before)

    def test_wrong_contract_function_is_rejected_by_trigger(self):
        guard.install_writer_guard(self.conn)
        old = self.raw()
        old.create_function(guard.FUNCTION, 0, lambda: "unknown")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "OC_ORCH_WRITER_INCOMPATIBLE"):
            old.execute("INSERT INTO meta(key,value) VALUES('bad','bad')")

    def test_install_failure_rolls_back_all_schema_and_data(self):
        before = self.snapshot()
        real_expected = guard._expected_triggers

        def bad_sql():
            expected = real_expected()
            expected["broken"] = "CREATE TRIGGER bad NOT VALID"
            return expected

        with patch.object(guard, "_expected_triggers", side_effect=bad_sql):
            with self.assertRaises(sqlite3.OperationalError):
                guard.install_writer_guard(self.conn)
        self.assertEqual(self.snapshot(), before)
        self.raw().execute("INSERT INTO meta(key,value) VALUES('old-still-valid','yes')")

    def test_missing_trigger_rejected_on_new_connection(self):
        guard.install_writer_guard(self.conn)
        self.conn.execute(f'DROP TRIGGER "{guard.PREFIX}tasks_update"')
        with self.assertRaises(guard.WriterGuardStateError):
            connect(self.home)

    def test_bad_marker_and_missing_marker_fail_closed(self):
        guard.install_writer_guard(self.conn)
        for value in ("unknown", None):
            with self.subTest(value=value):
                if value is None:
                    self.conn.execute("DELETE FROM meta WHERE key=?", (guard.GUARD_KEY,))
                else:
                    self.conn.execute("UPDATE meta SET value=? WHERE key=?", (value, guard.GUARD_KEY))
                with self.assertRaises(guard.WriterGuardStateError):
                    connect(self.home)

    def test_backup_restore_keeps_guard_and_post_install_facts(self):
        guard.install_writer_guard(self.conn)
        self.ledger.set_meta("new-unknown-input", "same-message-id")
        restore = Path(self.home) / "restored"
        restore.mkdir()
        backup_to(self.conn, str(restore / "ledger.db"))
        restored = init(str(restore))
        self.addCleanup(restored.close)
        self.assertTrue(guard.check_writer_guard(restored))
        self.assertEqual(Ledger(restored, str(restore)).get_meta("new-unknown-input"), "same-message-id")

    def test_readonly_connection_can_inspect_guarded_backup(self):
        guard.install_writer_guard(self.conn)
        ro = sqlite3.connect(f'file:{self.home}/ledger.db?mode=ro', uri=True)
        self.addCleanup(ro.close)
        self.assertEqual(ro.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.assertEqual(ro.execute("SELECT title FROM tasks WHERE id=?", (self.task["id"],)).fetchone()[0], "preserve")

    def test_install_inside_existing_transaction_is_refused(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(RuntimeError):
                guard.install_writer_guard(self.conn)
        finally:
            self.conn.rollback()


if __name__ == "__main__":
    unittest.main()

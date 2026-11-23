"""独立策略契约：旧已打开v2连接拒写、精确覆盖、回滚和备份保全。"""

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from oc_orch.adapters.delivery_guard import (
    FUNCTION, KEY, VERSION, _expected, check_delivery_guard, install_delivery_guard,
)
from oc_orch.adapters.sqlite_db import connect, init
from oc_orch.adapters.writer_guard import (
    FUNCTION as BASE_FUNCTION, GUARD_VERSION, install_writer_guard,
)
from oc_orch.domain.errors import LedgerError


class DeliveryGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="delivery-guard-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)

    def dump(self):
        return "\n".join(self.conn.iterdump())

    def install(self):
        install_writer_guard(self.conn)
        return install_delivery_guard(self.conn)

    def test_default_uninstalled_zero_write(self):
        before = self.dump()
        self.assertFalse(check_delivery_guard(self.conn))
        self.assertEqual(self.dump(), before)

    def test_base_required_without_partial_install(self):
        before = self.dump()
        with self.assertRaises(LedgerError) as raised:
            install_delivery_guard(self.conn)
        self.assertEqual(raised.exception.code, "DELIVERY_GUARD_BASE_REQUIRED")
        self.assertEqual(self.dump(), before)

    def test_install_is_exact_and_repeat_zero_write(self):
        self.assertTrue(self.install()["installed"])
        self.assertTrue(check_delivery_guard(self.conn))
        before = self.dump()
        changes = self.conn.total_changes
        self.assertFalse(install_delivery_guard(self.conn)["installed"])
        self.assertEqual(self.conn.total_changes, changes)
        self.assertEqual(self.dump(), before)
        other = connect(self.home)
        self.addCleanup(other.close)
        other.execute("INSERT INTO meta(key,value) VALUES('new-writer','ok')")

    def test_old_already_open_v2_cannot_dml_after_activation(self):
        install_writer_guard(self.conn)
        old = sqlite3.connect(self.home / "ledger.db", isolation_level=None)
        self.addCleanup(old.close)
        old.create_function(BASE_FUNCTION, 0, lambda: GUARD_VERSION)
        old.execute("INSERT INTO meta(key,value) VALUES('old-writer','before')")
        install_delivery_guard(self.conn)
        before = self.dump()
        for sql in ("INSERT INTO meta(key,value) VALUES('old-more','x')",
                    "UPDATE meta SET value='after' WHERE key='old-writer'",
                    "DELETE FROM meta WHERE key='old-writer'",
                    "INSERT INTO audit(at,actor,action,entity,entity_id) "
                    "VALUES(1,'old','old-write','test','x')"):
            with self.subTest(sql=sql), self.assertRaises(sqlite3.OperationalError):
                old.execute(sql)
        self.assertEqual(old.execute("SELECT value FROM meta WHERE key='old-writer'").fetchone()[0], "before")
        self.assertEqual(self.dump(), before)

    def test_wrong_udf_version_rejected(self):
        self.install()
        self.conn.create_function(FUNCTION, 0, lambda: "delivery-policy-old")
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("INSERT INTO meta(key,value) VALUES('bad','x')")

    def test_injected_audit_failure_rolls_back_all_new_contract(self):
        install_writer_guard(self.conn)
        self.conn.execute("CREATE TEMP TRIGGER fail_delivery_install BEFORE INSERT ON audit "
                          "WHEN NEW.action='delivery-policy.install' "
                          "BEGIN SELECT RAISE(ABORT,'injected'); END")
        before = self.dump()
        with self.assertRaises(sqlite3.IntegrityError):
            install_delivery_guard(self.conn)
        self.assertEqual(self.dump(), before)
        self.assertFalse(check_delivery_guard(self.conn))

    def test_missing_or_changed_trigger_fails_closed(self):
        self.install()
        name = next(iter(_expected()))
        self.conn.execute(f'DROP TRIGGER "{name}"')
        before = self.dump()
        with self.assertRaises(LedgerError):
            check_delivery_guard(self.conn)
        with self.assertRaises(LedgerError):
            connect(self.home)
        with self.assertRaises(LedgerError):
            install_delivery_guard(self.conn)
        self.assertEqual(self.dump(), before)

    def test_marker_missing_or_future_is_not_uninstalled(self):
        self.install()
        for value in (None, "delivery-policy-v999"):
            if value is None:
                self.conn.execute("DELETE FROM meta WHERE key=?", (KEY,))
            else:
                self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?)", (KEY, value))
            with self.subTest(value=value), self.assertRaises(LedgerError):
                check_delivery_guard(self.conn)

    def test_backup_reopen_keeps_contract_and_old_facts(self):
        self.conn.execute("INSERT INTO meta(key,value) VALUES('preserved','fact')")
        self.install()
        backup = sqlite3.connect(self.home / "copy.db", isolation_level=None)
        self.addCleanup(backup.close)
        self.conn.backup(backup)
        self.assertTrue(check_delivery_guard(backup))
        self.assertEqual(backup.execute("SELECT value FROM meta WHERE key='preserved'").fetchone()[0], "fact")
        with self.assertRaises(sqlite3.OperationalError):
            backup.execute("UPDATE meta SET value='wrong' WHERE key='preserved'")

    def test_nested_transaction_not_allowed(self):
        install_writer_guard(self.conn)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(sqlite3.OperationalError):
                install_delivery_guard(self.conn)
        finally:
            self.conn.execute("ROLLBACK")

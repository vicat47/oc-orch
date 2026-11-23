"""Provider failover capability contract, mixed-version refusal and session lock."""

from __future__ import annotations

import sqlite3
import tempfile
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.adapters import failover_guard as guard
from oc_orch.adapters.failover_ledger import FailoverLedger
from oc_orch.adapters.sqlite_db import backup_to, connect, init, inspect_backup, meta_set
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.writer_guard import (
    configure_writer_connection, install_writer_guard,
)


class FailoverGuardTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="failover-guard-")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)
        self.task = self.ledger.create_task(project="p", title="guard-test")
        self.conn.execute(
            "INSERT INTO attempts(id,task_id,generation,session_id,directory,status,"
            "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            ("A-1", self.task["id"], 1, "ses-worker", self.home,
             "creating", 1, 1))

    def install(self):
        install_writer_guard(self.conn, actor="test")
        return guard.install_failover_guard(self.conn, actor="test")

    def test_default_is_uninstalled_and_read_only(self):
        before = dict(self.conn.execute("SELECT key,value FROM meta"))
        store = FailoverLedger(self.conn, clock=lambda: 100)
        self.assertFalse(guard.check_failover_guard(self.conn))
        self.assertIsNone(store.get_record("episode", "EP-missing"))
        self.assertIsNone(store.get_policy("project", "p"))
        self.assertEqual(store.list_records("episode")["records"], [])
        self.assertFalse(store.guard_valid()["valid"])
        self.assertEqual(dict(self.conn.execute("SELECT key,value FROM meta")), before)

    def test_install_is_explicit_idempotent_and_preserves_business_rows(self):
        before = self.conn.execute("SELECT * FROM attempts WHERE id='A-1'").fetchone()
        out = self.install()
        self.assertTrue(out["installed"])
        self.assertTrue(guard.check_failover_guard(self.conn))
        self.assertEqual(self.conn.execute(
            "SELECT * FROM attempts WHERE id='A-1'").fetchone(), before)
        snapshot = list(self.conn.iterdump())
        self.assertFalse(guard.install_failover_guard(
            self.conn, actor="test")['installed'])
        self.assertEqual(list(self.conn.iterdump()), snapshot)

    def test_old_continuity_connection_cannot_write_after_install(self):
        install_writer_guard(self.conn, actor="test")
        old = sqlite3.connect(str(Path(self.home) / "ledger.db"),
                              isolation_level=None)
        self.addCleanup(old.close)
        configure_writer_connection(old)

        guard.install_failover_guard(self.conn, actor="test")

        with self.assertRaisesRegex(sqlite3.OperationalError,
                                    "oc_orch_failover_operation"):
            old.execute("UPDATE attempts SET status=status WHERE id='A-1'")

    def test_session_lock_blocks_other_attempts_and_managed_inputs(self):
        self.install()
        meta_set(self.conn, guard.session_lock_key("ses-worker"), "OP-1")

        with self.assertRaisesRegex(sqlite3.IntegrityError,
                                    "OC_ORCH_FAILOVER_SESSION_OCCUPIED"):
            self.conn.execute("UPDATE attempts SET status='dispatched' WHERE id='A-1'")

        with guard.authorized_failover_operation(self.conn, "OP-1"):
            self.conn.execute("UPDATE attempts SET status='dispatched' WHERE id='A-1'")

        with self.assertRaisesRegex(sqlite3.IntegrityError,
                                    "OC_ORCH_FAILOVER_SESSION_OCCUPIED"):
            self.conn.execute(
                "INSERT INTO session_inputs(input_id,session_id,admission_state,"
                "prepared_at,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                ("IN-1", "ses-worker", "prepared", 1, 1, 1))

    def test_session_lock_keeps_manual_stop_and_release_available(self):
        self.install()
        meta_set(self.conn, guard.session_lock_key("ses-worker"), "OP-1")

        stopped = self.ledger.stop_attempt("A-1", actor="operator:test")
        self.assertEqual(stopped["status"], "stop_requested")
        released = self.ledger.release_site(
            "A-1", confirmed=True, external_stopped=True,
            actor="operator:test", note="verified")
        self.assertTrue(released["site_released_at"])
        self.assertTrue(released["stop_confirmed_at"])

    def test_failed_install_rolls_back_trigger_and_marker(self):
        install_writer_guard(self.conn, actor="test")
        expected = guard._expected
        with patch.object(guard, "_expected", return_value={
                "oc_orch_failover_bad": "CREATE TRIGGER oc_orch_failover_bad "
                "BEFORE INSERT ON missing_table BEGIN SELECT 1; END"}):
            with self.assertRaises(sqlite3.OperationalError):
                guard.install_failover_guard(self.conn, actor="test")
        self.assertFalse(guard.check_failover_guard(self.conn))
        self.assertIsNone(self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (guard.KEY,)).fetchone())
        self.assertEqual(expected().keys(), guard._expected().keys())

    def test_consistent_backup_retains_failover_lock(self):
        self.install()
        meta_set(self.conn, guard.session_lock_key("ses-worker"), "OP-1")
        path = str(Path(self.home) / "backup.db")
        backup_to(self.conn, path)
        result = inspect_backup(path)
        self.assertTrue(result["ok"])
        restored = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = restored.execute(
                "SELECT value FROM meta WHERE key=?",
                (guard.session_lock_key("ses-worker"),)).fetchone()
            self.assertEqual(row[0], "OP-1")
        finally:
            restored.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

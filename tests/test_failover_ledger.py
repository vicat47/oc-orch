#!/usr/bin/env python3
"""failover 台账适配器测试（F15/F23/F24/F25/F26 的存储面：CAS、fail-closed、
路线阻断隔离、策略版本、WAL 双连接单写）。"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
import unittest

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.adapters.sqlite_db import connect, init
from oc_orch.adapters.writer_guard import install_writer_guard
from oc_orch.adapters.failover_guard import install_failover_guard
from oc_orch.adapters.failover_ledger import (
    FailoverLedger,
    FailoverStateError,
    failover_episode_id,
)
from oc_orch.util import now_ms


def _episode(project="p", task_id="T-1", route="deepseek/deepseek-flash",
             status="open", **extra):
    rec = {
        "episode_id": failover_episode_id(project, task_id, route),
        "project": project, "task_id": task_id, "session_id": None,
        "route": route, "category": "quota", "status": status,
        "switches_used": 0, "attempts_used": 1, "schema": 1,
    }
    rec.update(extra)
    return rec, rec["episode_id"]


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="oc-orch-failover-")
        self.home = self._tmp
        conn = init(self.home)
        install_writer_guard(conn, actor="test")
        install_failover_guard(conn, actor="test")
        conn.close()
        self.audit_rows = []

        def audit(actor, action, entity, entity_id, detail):
            self.audit_rows.append((actor, action, entity, entity_id, detail))

        self.audit = audit

    def tearDown(self):
        shutil.rmtree(self._tmp, ignore_errors=True)

    def store(self, conn=None, clock=None):
        return FailoverLedger(
            conn or connect(self.home), clock=clock or now_ms, audit=self.audit)


class TestEpisodeCAS(Base):
    def test_create_and_cas_update(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            rec, rid = _episode()
            out = s.upsert_record("episode", rid, rec, expected_revision=None)
            self.assertTrue(out["created"])
            self.assertEqual(out["revision"], 1)
            got = s.get_record("episode", rid)
            self.assertEqual(got["status"], "open")

            rec2 = dict(rec, status="switch_requested")
            out = s.upsert_record("episode", rid, rec2, expected_revision=1)
            self.assertFalse(out["created"])
            self.assertEqual(out["revision"], 2)
            self.assertEqual(s.get_record("episode", rid)["status"],
                             "switch_requested")
        finally:
            conn.close()

    def test_stale_cas_rejected(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            rec, rid = _episode()
            s.upsert_record("episode", rid, rec, expected_revision=None)
            with self.assertRaises(FailoverStateError):
                s.upsert_record("episode", rid, dict(rec, status="x"),
                                expected_revision=99)
            # 原记录未被覆盖
            self.assertEqual(s.get_record("episode", rid)["status"], "open")
        finally:
            conn.close()

    def test_create_existing_without_expected_rejected(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            rec, rid = _episode()
            s.upsert_record("episode", rid, rec, expected_revision=None)
            with self.assertRaises(FailoverStateError):
                s.upsert_record("episode", rid, rec, expected_revision=None)
        finally:
            conn.close()

    def test_wal_two_connections_only_one_claim(self):
        # 真实 WAL 双连接（check_same_thread=False）：同时认领只有一成功（F15 存储面）。
        import sqlite3 as _sqlite3

        path = os.path.join(self.home, "ledger.db")

        def raw_conn():
            conn = _sqlite3.connect(path, timeout=15, isolation_level=None,
                                    check_same_thread=False)
            conn.row_factory = _sqlite3.Row
            conn.execute("PRAGMA busy_timeout=15000")
            from oc_orch.adapters.writer_guard import configure_writer_connection
            from oc_orch.adapters.delivery_guard import configure_delivery_connection
            from oc_orch.adapters.failover_guard import configure_failover_connection
            configure_writer_connection(conn)
            configure_delivery_connection(conn)
            configure_failover_connection(conn)
            return conn

        c1, c2 = raw_conn(), raw_conn()
        s1, s2 = self.store(c1), self.store(c2)
        rec, rid = _episode()
        barrier = threading.Barrier(2)
        results = {}

        def attempt(store):
            barrier.wait()
            try:
                out = store.upsert_record("episode", rid, rec,
                                          expected_revision=None)
                results[store] = ("ok", out["revision"])
            except FailoverStateError as e:
                results[store] = ("stale", str(e)[:60])

        t1 = threading.Thread(target=attempt, args=(s1,))
        t2 = threading.Thread(target=attempt, args=(s2,))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(1, sum(1 for v in results.values() if v[0] == "ok"))
        self.assertTrue(any(v[0] == "stale" for v in results.values()))
        c1.close(); c2.close()


class TestFailClosed(Base):
    def test_corrupt_record_detected(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            rec, rid = _episode()
            s.upsert_record("episode", rid, rec, expected_revision=None)
            # 直接注入坏记录（缺必需字段/坏 schema），读取必须 fail-closed。
            conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
                (f"failover:episode:{rid}", '{"schema": 1, "kind": "episode"}'))
            with self.assertRaises(FailoverStateError):
                s.get_record("episode", rid)
            listed = s.list_records("episode", limit=10)
            self.assertGreaterEqual(listed["invalid_records"], 1)
        finally:
            conn.close()

    def test_bad_limit_rejected(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            for bad in (True, 0, -1, "5", 1.5):
                with self.assertRaises(ValueError):
                    s.list_records("episode", limit=bad)
        finally:
            conn.close()


class TestPolicy(Base):
    def test_policy_create_update_cas(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            policy = {
                "enabled": True, "source_routes": ["deepseek/deepseek-flash"],
                "fallback_routes": ["yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash"],
                "max_switches": 1, "max_attempts": 3, "scope": "project",
            }
            out = s.set_policy("project", "p1", policy, expected_revision=None)
            self.assertTrue(out["created"])
            self.assertEqual(out["revision"], 1)
            got = s.get_policy("project", "p1")
            self.assertTrue(got["enabled"])
            with self.assertRaises(FailoverStateError):
                s.set_policy("project", "p1", policy, expected_revision=None)
            with self.assertRaises(FailoverStateError):
                s.set_policy("project", "p1",
                             dict(policy, enabled=False), expected_revision=7)
            out = s.set_policy("project", "p1",
                               dict(policy, enabled=False), expected_revision=1)
            self.assertEqual(out["revision"], 2)
            self.assertFalse(s.get_policy("project", "p1")["enabled"])
        finally:
            conn.close()

    def test_invalid_policy_rejected(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            with self.assertRaises(FailoverStateError):
                s.set_policy("project", "p1", {"enabled": "yes", "scope": "project"},
                             expected_revision=None)
        finally:
            conn.close()


class TestRouteBlock(Base):
    def test_block_and_clear(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            route = "deepseek/deepseek-flash"
            s.set_route_blocked(route, reason="quota", until_ms=None, now=1000)
            self.assertTrue(s.is_route_blocked(route, now=5000)["blocked"])
            # 备用成功不清原故障（F23 存储面）
            self.assertTrue(s.is_route_blocked("yfzs-newapi/x", now=5000) is None)
            out = s.clear_route_blocked(route, reason="manual-verified", now=6000)
            self.assertTrue(out["cleared"])
            self.assertFalse(
                s.is_route_blocked(route, now=7000)["blocked"])
        finally:
            conn.close()

    def test_until_expiry_not_unlock_by_time(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            route = "deepseek/deepseek-flash"
            s.set_route_blocked(route, reason="quota", until_ms=2000, now=1000)
            before = s.is_route_blocked(route, now=1000)
            self.assertTrue(before["blocked"])
            after = s.is_route_blocked(route, now=3000)
            # F24：时间流逝不等于解除，仍返回记录并标记 expired
            self.assertTrue(after["blocked"])
            self.assertTrue(after["expired"])
        finally:
            conn.close()

    def test_clear_unblocked_is_noop(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            out = s.clear_route_blocked("deepseek/x", reason="nothing")
            self.assertFalse(out["cleared"])
        finally:
            conn.close()


class TestMisc(Base):
    def test_cursor_and_guard(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            self.assertIsNone(s.get_cursor("episode-sweep"))
            s.set_cursor("episode-sweep", "EP-abc")
            self.assertEqual(s.get_cursor("episode-sweep"), "EP-abc")
            guard = s.guard_valid()
            self.assertTrue(guard["valid"])
            self.assertEqual(guard["schema"], 1)
        finally:
            conn.close()

    def test_audit_recorded(self):
        conn = connect(self.home)
        try:
            s = self.store(conn)
            rec, rid = _episode()
            s.upsert_record("episode", rid, rec, expected_revision=None)
            actions = [a for _, a, _, _, _ in self.audit_rows]
            self.assertIn("failover.episode.create", actions)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

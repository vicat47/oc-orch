#!/usr/bin/env python3
"""R4 写契约 v1→v2 升级窄 API（upgrade_writer_guard）的独立真 SQLite/WAL 验证。

仅使用临时库（/tmp/opencode）与只读冻结生产快照源码；不触生产 DB、无网络/
邮箱/配置/安装/Git；不运行全量。

完整 v1 fixture 语义（与已上线 continuity-v1 精确一致，脚本依据为冻结生产
快照，可用 OC_ORCH_V1_WRITER_GUARD 指定 writer_guard.py）：

- schema3（init 生成的 12 表）+ 既有业务事实（task/attempt/notification/
  responsibility/session_input/meta 等）+ meta 标记 `writer-contract:version`
  = `continuity-v1` + 36 个精确 v1 trigger（校验 `oc_orch_writer_contract()
  IS NOT 'continuity-v1'`）。

覆盖：默认 connect/init 遇完整 v1 明确拒写且零副作用；upgrade 保全业务行/
meta（除 marker）并只增 1 条升级 audit；失败注入整体回滚回完整 v1；未知/
半残/未来版本拒绝不修复；v1 旧 UDF 写者升级前可写、升级后被 v2 trigger 拒写；
重复升级零额外写入；并发/重开；备份恢复保留升级后新事实；v1 SQL 契约与冻结
生产快照逐字一致。
"""

import os
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from oc_orch.adapters.sqlite_db import backup_to, connect, init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters import writer_guard as guard
from oc_orch.composition import make_responsibilities
from tests.fakes import FakeClock

FROZEN_V1_WRITER_GUARD = Path(os.environ.get("OC_ORCH_V1_WRITER_GUARD", ""))


def build_v1_db(home: str) -> None:
    """构造完整 v1 写契约台账（schema3 + 业务事实 + v1 标记 + 36 个 v1 trigger）。"""
    clock = FakeClock(wall_ms=1_800_000_000_000)
    conn = init(home)
    try:
        ledger = Ledger(conn, home, clock=clock)
        task = ledger.create_task(project="p", title="preserve-me",
                                  coordinator_session="ses_coord",
                                  objective="keep this row identical")
        att = ledger.create_attempt(
            task["id"], directory=str(Path(home) / "site"), session_id="ses_worker",
            generation=1, status="running", dispatch_state="confirmed",
            dispatch_msg_id="msg_v1", deliverables=["README.md"])
        ledger.enqueue_notification(
            kind="result_ready", target_session="ses_coord", project="p",
            dedupe_key="v1-notification", payload={"summary": "v1"})
        ledger.set_meta("legacy-key", "legacy-value")
        store = make_responsibilities(ledger)
        now = clock.now_ms()
        store.open_responsibility(
            "coord", "p", task["id"], attempt_id=att["id"], generation=1,
            result_version=None, target_session="ses_coord",
            state="waiting_input", wait_kind="input_execution",
            wait_condition="v1-row", next_action="wait",
            next_check_at=now, deadline=now + 3_600_000,
            condition_key="v1-condition", max_budget=3)
        conn.execute(
            "INSERT INTO session_inputs(input_id, session_id, source, intent, "
            "outstanding, revision, body, body_hash, notification_ids, "
            "admission_state, prepared_at, created_at, updated_at) "
            "VALUES('si-v1','ses_worker','legacy-receipt','unknown',0,1,'','',"
            "'[]','unknown',?,?,?)", (now, now, now))
    finally:
        conn.close()
    raw = sqlite3.connect(str(Path(home) / "ledger.db"), isolation_level=None)
    try:
        raw.execute("INSERT INTO meta(key,value) VALUES(?,?)",
                    (guard.GUARD_KEY, guard.V1_GUARD_VERSION))
        for sql in guard._expected_v1_triggers().values():
            raw.execute(sql)
    finally:
        raw.close()


class V1UpgradeBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="writer-upgrade-",
                                               dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name

    def raw(self):
        conn = sqlite3.connect(str(Path(self.home) / "ledger.db"),
                               isolation_level=None)
        self.addCleanup(conn.close)
        return conn

    def v1w(self):
        """旧 v1 UDF 写者连接：用于对 v1 守卫库做 setup 变更（过 v1 trigger）。"""
        conn = self.raw()
        conn.create_function(guard.FUNCTION, 0, lambda: guard.V1_GUARD_VERSION,
                             deterministic=True)
        return conn

    def trigger_state(self):
        return dict(self.raw().execute(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' "
            "AND name GLOB 'oc_orch_writer_*'").fetchall())

    def marker(self):
        row = self.raw().execute(
            "SELECT value FROM meta WHERE key=?", (guard.GUARD_KEY,)).fetchone()
        return row[0] if row else None

    def upgrades(self):
        rows = self.raw().execute(
            "SELECT action FROM audit WHERE action LIKE 'writer-contract.%' "
            "ORDER BY id").fetchall()
        return [r[0] for r in rows]


class V1DefaultRefusalTests(V1UpgradeBase):
    def test_default_connect_and_init_refuse_v1_without_side_effects(self):
        build_v1_db(self.home)
        before_triggers = self.trigger_state()
        with self.assertRaises(guard.WriterGuardStateError):
            connect(self.home)
        with self.assertRaises(guard.WriterGuardStateError):
            init(self.home)
        with self.assertRaises(guard.WriterGuardStateError):
            guard.install_writer_guard(self.raw(), actor="probe")
        # 零副作用：仍是完整 v1，无升级/安装痕迹，旧事实可读
        self.assertEqual(self.marker(), guard.V1_GUARD_VERSION)
        self.assertEqual(self.trigger_state(), before_triggers)
        self.assertEqual(self.trigger_state(), guard._expected_v1_triggers())
        self.assertEqual(self.upgrades(), [])
        self.assertEqual(self.raw().execute(
            "SELECT title FROM tasks WHERE title='preserve-me'").fetchone()[0],
            "preserve-me")
        self.assertEqual(self.raw().execute(
            "SELECT value FROM meta WHERE key='legacy-key'").fetchone()[0],
            "legacy-value")

    def test_upgrade_requires_actor_and_rejects_nested_transaction(self):
        build_v1_db(self.home)
        with self.assertRaises(ValueError):
            guard.upgrade_writer_guard(self.raw(), actor="  ")
        conn = self.raw()
        conn.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(RuntimeError):
                guard.upgrade_writer_guard(conn, actor="nested")
        finally:
            conn.execute("ROLLBACK")
        self.assertEqual(self.upgrades(), [])
        self.assertEqual(self.marker(), guard.V1_GUARD_VERSION)


class V1UpgradeSuccessTests(V1UpgradeBase):
    def _facts(self):
        conn = self.raw()
        return {t: [tuple(r) for r in conn.execute(f'SELECT * FROM "{t}"')]
                for t in guard.TABLES if t not in ("meta", "audit")}

    def test_upgrade_preserves_facts_exact_v2_contract_single_audit(self):
        build_v1_db(self.home)
        before_facts = self._facts()
        before_meta = dict(self.raw().execute("SELECT key,value FROM meta"))
        before_audit = [tuple(r) for r in self.raw().execute(
            "SELECT * FROM audit ORDER BY id")]
        out = guard.upgrade_writer_guard(self.raw(), actor="release-upgrade")
        self.assertTrue(out["upgraded"])
        self.assertEqual(out["version"], guard.GUARD_VERSION)
        self.assertEqual(self._facts(), before_facts, "业务行必须逐字保全")
        self.assertEqual(self.trigger_state(), guard._expected_triggers())
        self.assertIn("continuity-v2", next(iter(self.trigger_state().values())))
        self.assertNotIn("continuity-v1", " ".join(self.trigger_state().values()),
                         "升级后不得残留 v1 trigger 内容")
        self.assertEqual(self.marker(), "continuity-v2")
        meta_after = dict(self.raw().execute("SELECT key,value FROM meta"))
        self.assertEqual(
            {k: v for k, v in meta_after.items() if k != guard.GUARD_KEY},
            {k: v for k, v in before_meta.items() if k != guard.GUARD_KEY})
        self.assertEqual(self.raw().execute(
            "SELECT value FROM meta WHERE key='schema_version'").fetchone()[0], "3")
        audit_after = [tuple(r) for r in self.raw().execute(
            "SELECT * FROM audit ORDER BY id")]
        self.assertEqual(audit_after[:-1], before_audit)
        self.assertEqual(audit_after[-1][3], "writer-contract.upgrade")
        self.assertEqual(self.upgrades(), ["writer-contract.upgrade"])
        # 升级后：无函数写入者被拒；正式 init（v2 绑定）可写并保留新事实
        with self.assertRaisesRegex(sqlite3.OperationalError,
                                    re.escape(guard.FUNCTION)):
            self.raw().execute("INSERT INTO meta(key,value) VALUES('x','y')")
        conn = init(self.home)
        self.addCleanup(conn.close)
        ledger = Ledger(conn, self.home)
        ledger.set_meta("post-upgrade-fact", "preserved-2")
        self.assertEqual(ledger.get_meta("post-upgrade-fact"), "preserved-2")
        self.assertEqual(ledger.get_meta("legacy-key"), "legacy-value")
        self.assertTrue(guard.check_writer_guard(conn))

    def test_old_v1_udf_writer_allowed_before_upgrade_blocked_after(self):
        build_v1_db(self.home)
        old = self.raw()
        old.create_function(guard.FUNCTION, 0, lambda: guard.V1_GUARD_VERSION,
                            deterministic=True)
        old.execute("INSERT INTO meta(key,value) VALUES('pre-upgrade-write','ok')")
        self.assertEqual(self.raw().execute(
            "SELECT value FROM meta WHERE key='pre-upgrade-write'").fetchone()[0],
            "ok")
        self.assertTrue(guard.upgrade_writer_guard(self.raw(),
                                                   actor="release-upgrade")["upgraded"])
        # 旧 v1 连接仍打开、函数仍返回 v1：v2 trigger 必须拒写
        with self.assertRaisesRegex(sqlite3.IntegrityError,
                                    "OC_ORCH_WRITER_INCOMPATIBLE"):
            old.execute("INSERT INTO meta(key,value) VALUES('post-upgrade','bad')")
        with self.assertRaisesRegex(sqlite3.IntegrityError,
                                    "OC_ORCH_WRITER_INCOMPATIBLE"):
            old.execute("UPDATE tasks SET title='bad' WHERE title='preserve-me'")
        self.assertEqual(self.raw().execute(
            "SELECT value FROM meta WHERE key='post-upgrade'").fetchone(), None)
        self.assertEqual(self.raw().execute(
            "SELECT title FROM tasks WHERE title='preserve-me'").fetchone()[0],
            "preserve-me")

    def test_repeat_upgrade_is_noop_with_zero_extra_writes(self):
        build_v1_db(self.home)
        first = guard.upgrade_writer_guard(self.raw(), actor="a")
        self.assertTrue(first["upgraded"])
        before = list(self.raw().iterdump())
        second = guard.upgrade_writer_guard(self.raw(), actor="b")
        self.assertFalse(second["upgraded"])
        self.assertEqual(list(self.raw().iterdump()), before)
        self.assertEqual(self.upgrades(), ["writer-contract.upgrade"])

    def test_reopen_and_second_connection_upgrade_single_audit(self):
        build_v1_db(self.home)
        self.assertTrue(guard.upgrade_writer_guard(self.raw(), actor="primary")["upgraded"])
        # 另一连接重开：已是 v2 → no-op，不再产生 audit/写入
        self.assertFalse(guard.upgrade_writer_guard(self.raw(), actor="second")["upgraded"])
        self.assertEqual(self.upgrades(), ["writer-contract.upgrade"])
        conn = init(self.home)
        self.addCleanup(conn.close)
        ledger = Ledger(conn, self.home)
        ledger.set_meta("reopen-new-fact", "si-2")
        self.assertEqual(ledger.get_meta("reopen-new-fact"), "si-2")


class V1UpgradeFailureTests(V1UpgradeBase):
    def test_injected_failure_rolls_back_to_complete_v1(self):
        build_v1_db(self.home)
        before = {
            "marker": self.marker(),
            "triggers": self.trigger_state(),
            "facts": [tuple(r) for r in self.raw().execute(
                "SELECT * FROM tasks").fetchall()],
            "upgrades": self.upgrades(),
        }

        def boom():
            raise RuntimeError("injected-trigger-failure")

        with patch.object(guard, "_expected_triggers", side_effect=boom):
            with self.assertRaises(RuntimeError):
                guard.upgrade_writer_guard(self.raw(), actor="fail")
        self.assertEqual(self.marker(), before["marker"])
        self.assertEqual(self.trigger_state(), before["triggers"],
                         "必须整体回滚回完整 v1")
        self.assertEqual([tuple(r) for r in self.raw().execute(
            "SELECT * FROM tasks").fetchall()], before["facts"])
        self.assertEqual(self.upgrades(), [])
        old = self.raw()
        old.create_function(guard.FUNCTION, 0, lambda: guard.V1_GUARD_VERSION,
                            deterministic=True)
        old.execute("INSERT INTO meta(key,value) VALUES('still-v1','yes')")
        self.assertEqual(self.raw().execute(
            "SELECT value FROM meta WHERE key='still-v1'").fetchone()[0], "yes")

    def test_unknown_marker_refused_without_touching_db(self):
        build_v1_db(self.home)
        self.v1w().execute(
            "UPDATE meta SET value='continuity-xyz' WHERE key=?",
            (guard.GUARD_KEY,))
        with self.assertRaises(guard.WriterGuardStateError):
            guard.upgrade_writer_guard(self.raw(), actor="x")
        self.assertEqual(self.marker(), "continuity-xyz")
        self.assertEqual(self.trigger_state(), guard._expected_v1_triggers())

    def test_future_marker_refused(self):
        build_v1_db(self.home)
        self.v1w().execute(
            "UPDATE meta SET value='continuity-v3' WHERE key=?",
            (guard.GUARD_KEY,))
        with self.assertRaises(guard.WriterGuardStateError):
            guard.upgrade_writer_guard(self.raw(), actor="x")
        self.assertEqual(self.marker(), "continuity-v3")
        self.assertEqual(len(self.trigger_state()), 36)

    def test_missing_v1_trigger_refused_without_repair(self):
        build_v1_db(self.home)
        broken = self.raw()
        broken.execute(f'DROP TRIGGER "{guard.PREFIX}tasks_insert"')
        with self.assertRaises(guard.WriterGuardStateError):
            guard.upgrade_writer_guard(broken, actor="x")
        self.assertEqual(len(self.trigger_state()), 35, "不允许修复破损")
        self.assertEqual(self.marker(), guard.V1_GUARD_VERSION)

    def test_wrong_v1_trigger_sql_refused(self):
        build_v1_db(self.home)
        conn = self.raw()
        conn.execute(f'DROP TRIGGER "{guard.PREFIX}tasks_update"')
        conn.execute(
            f'CREATE TRIGGER "{guard.PREFIX}tasks_update" BEFORE UPDATE ON "tasks" '
            "WHEN oc_orch_writer_contract() IS NOT 'continuity-v1' "
            "BEGIN SELECT RAISE(ABORT, 'TAMPERED-V1'); END")
        with self.assertRaises(guard.WriterGuardStateError):
            guard.upgrade_writer_guard(self.raw(), actor="x")
        state = self.trigger_state()
        self.assertIn("TAMPERED-V1", state[f"{guard.PREFIX}tasks_update"])

    def test_unguarded_or_broken_schema_refused(self):
        fresh = Path(self.tmp.name) / "fresh"
        fresh.mkdir()
        init(str(fresh))
        with self.assertRaises(guard.WriterGuardStateError):
            guard.upgrade_writer_guard(
                sqlite3.connect(str(fresh / "ledger.db"),
                                isolation_level=None), actor="x")
        build_v1_db(self.home)
        bad = self.v1w()
        bad.execute("DELETE FROM meta WHERE key='schema_version'")
        with self.assertRaises(guard.WriterGuardStateError):
            guard.upgrade_writer_guard(self.raw(), actor="x")
        self.assertEqual(self.marker(), guard.V1_GUARD_VERSION)


class V1UpgradeBackupRestoreTests(V1UpgradeBase):
    def test_backup_restore_after_upgrade_keeps_v2_and_new_facts(self):
        build_v1_db(self.home)
        self.assertTrue(guard.upgrade_writer_guard(self.raw(), actor="backup")["upgraded"])
        conn = init(self.home)
        self.addCleanup(conn.close)
        ledger = Ledger(conn, self.home)
        ledger.set_meta("new-fact-after-upgrade", "same-session-id")
        restore = Path(self.tmp.name) / "restore"
        restore.mkdir()
        backup_to(conn, str(restore / "ledger.db"))
        restored = init(str(restore))
        self.addCleanup(restored.close)
        self.assertTrue(guard.check_writer_guard(restored))
        self.assertEqual(
            Ledger(restored, str(restore)).get_meta("new-fact-after-upgrade"),
            "same-session-id")
        self.assertEqual(Ledger(restored, str(restore)).get_meta("legacy-key"),
                         "legacy-value")
        bad = sqlite3.connect(str(restore / "ledger.db"), isolation_level=None)
        self.addCleanup(bad.close)
        bad.create_function(guard.FUNCTION, 0, lambda: guard.V1_GUARD_VERSION,
                            deterministic=True)
        with self.assertRaisesRegex(sqlite3.IntegrityError,
                                    "OC_ORCH_WRITER_INCOMPATIBLE"):
            bad.execute("INSERT INTO meta(key,value) VALUES('old-writer','bad')")


@unittest.skipUnless(FROZEN_V1_WRITER_GUARD.is_file(),
                     "冻结生产快照 writer_guard.py 不在场（只读依据缺失）")
class FrozenV1ContractEquivalenceTests(V1UpgradeBase):
    def test_v1_expected_triggers_match_frozen_production_snapshot(self):
        from oc_orch import util as _util

        text = FROZEN_V1_WRITER_GUARD.read_text(encoding="utf-8")
        source = text.replace("from ..util import json_dumps, now_ms", "pass")
        ns = {"sqlite3": sqlite3, "json_dumps": _util.json_dumps,
              "now_ms": _util.now_ms}
        exec(compile(source, "frozen-v1-writer-guard.py", "exec"), ns)  # noqa: S102
        self.assertEqual(ns["GUARD_VERSION"], guard.V1_GUARD_VERSION)
        self.assertEqual(ns["PREFIX"], guard.PREFIX)
        self.assertEqual(tuple(ns["TABLES"]), guard.TABLES)
        self.assertEqual(tuple(ns["OPERATIONS"]), guard.OPERATIONS)
        frozen = {f"{ns['PREFIX']}{t}_{op.lower()}": ns["_trigger_sql"](t, op)
                  for t in ns["TABLES"] for op in ns["OPERATIONS"]}
        self.assertEqual(frozen, guard._expected_v1_triggers(),
                         "本开发版 v1 SQL 契约必须与冻结生产快照逐字一致")
        self.assertEqual(len(frozen), 36)


if __name__ == "__main__":
    unittest.main()
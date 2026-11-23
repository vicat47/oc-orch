#!/usr/bin/env python3
"""R4 组合迁移/故障验收矩阵（openspec improve-coordinator-continuity 任务 6.1/6.2）。

用**真实 API** 在临时 SQLite 台账上构造合法控制/责任/outbox 事实（依赖 satisfied
必须是真实 collect_attempt + verify_attempt 产物），覆盖：

- 合法组合维度：project running/paused；session hold/barrier/完成水位；
  provider cooldown/probe-unknown/legacy-manual；dependency waiting/
  satisfied-awaiting_review/waiting-human；mail pending-auth/pending/claimed/
  submit-unknown/accepted（显式覆盖表见 continuity_matrix_fixtures.COVERAGE_TABLE）；
- 合法组合备份恢复不丢：原 ACK、current/generation、结果版本、原输入 ID、
  期限/预算、operation 索引；重复打开/恢复不复活旧状态、不多造邮件/模型回合；
- 失败路径：安装/事务中断回滚、坏 JSON/schema/缺字段 fail-closed、旧 schema3
  连接与（可选）真实旧实现写拒绝、新事实保留后恢复不倒旧快照、晚到 idle/ACK/回执
  不盖新意图。

只跑定向测试；零网络、零真实服务；不写生产；故障矩阵只作用于临时库。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO))

from oc_orch.adapters.sqlite_db import (  # noqa: E402
    backup_to,
    init,
    inspect_backup,
    meta_get,
    meta_set,
)
from oc_orch.adapters.sqlite_ledger import Ledger  # noqa: E402
from oc_orch.adapters.coordinator_control import (  # noqa: E402
    CoordinatorControlLedger,
    CoordinatorControlStateError,
    OPERATION_KEY_PREFIX,
    PROJECT_KEY_PREFIX,
    SESSION_KEY_PREFIX,
)
from oc_orch.application.coordinator_control import (  # noqa: E402
    CoordinatorControlService,
)
from oc_orch.adapters.target_gate import (  # noqa: E402
    GATE_KEY_PREFIX,
    HALF_OPEN_TIMEOUT_MS,
    TargetGate,
    TargetGateStateError,
)
from oc_orch.adapters.email_outbox import (  # noqa: E402
    MAIL_KEY_PREFIX,
    MAIL_SCHEMA_KEY,
    EmailOutbox,
    MailOutboxStateError,
)
from oc_orch.adapters.responsibility_ledger import (  # noqa: E402
    STATE_AWAITING_REVIEW,
    STATE_WAITING_DEPENDENCY,
    STATE_WAITING_HUMAN,
    ResponsibilityStore,
)
from oc_orch.application.dependency_wait import DependencyWaitService  # noqa: E402
from oc_orch.application.operator_email import OperatorEmailService  # noqa: E402
from oc_orch.adapters.writer_guard import (  # noqa: E402
    TABLES as GUARD_TABLES,
    check_writer_guard,
    install_writer_guard,
)
from oc_orch.domain.operator_email import (  # noqa: E402
    STATUS_ACCEPTED,
    STATUS_CLAIMED,
    STATUS_PENDING,
    STATUS_PENDING_AUTH,
    STATUS_SUBMIT_UNKNOWN,
)
from tests.fakes import FakeClock  # noqa: E402
from tests.continuity_matrix_fixtures import (  # noqa: E402
    COVERAGE_TABLE,
    FAILURE_TABLE,
    GATE_COOLDOWN_SID,
    GATE_MANUAL_SID,
    GATE_PROBE_SID,
    PROJ_CTRL,
    PROJ_RUN,
    REQUIRED_FAILURES,
    REQUIRED_STATES,
    SID,
    SID_COMBINED,
    SID_FINISHED,
    T0,
    build_matrix_state,
    coverage_report,
    enabled_policy,
    incident_fact,
    meta_map,
    table_facts,
)


def _raw_conn(home):
    """无契约函数的旧 schema3 连接（模拟安装前已打开/旧代码的连接）。"""
    conn = sqlite3.connect(os.path.join(home, "ledger.db"), timeout=15,
                           isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


class MatrixBase(unittest.TestCase):
    """共享临时 home + 真实状态构造 + cleanup；guard=True 时先装发布契约门。"""

    guard = False

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="r4-matrix-",
                                               dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.st = build_matrix_state(self.tmp.name)
        self.addCleanup(self._close_extra)
        if self.guard:
            res = install_writer_guard(self.st.conn, actor="matrix")
            assert res["installed"] and res["triggers"] == 36, res

    def _close_extra(self):
        for conn in getattr(self, "_extra_conns", []):
            try:
                conn.close()
            except Exception:
                pass

    def track_conn(self, conn):
        self._extra_conns = getattr(self, "_extra_conns", []) + [conn]
        return conn

    # ---- 恢复辅助 ---------------------------------------------------
    def backup(self, name="backup.db"):
        dest = os.path.join(self.st.home, name)
        backup_to(self.st.conn, dest)
        return dest

    def restore_home(self, dest):
        """把一致备份放入新 home 的 ledger.db 并用同版入口打开。"""
        home2 = tempfile.mkdtemp(prefix="r4-restore-", dir="/tmp/opencode")
        self.addCleanup(shutil.rmtree, home2)
        shutil.copyfile(dest, os.path.join(home2, "ledger.db"))
        conn2 = self.track_conn(init(home2))
        return home2, conn2

    def restored_clock(self):
        """恢复侧“事实时刻”时钟：对齐最终构建时刻（probe-unknown/观察预算看 now）。"""
        return FakeClock(wall_ms=self.st.clock.now_ms())

    def lease_clock(self):
        """恢复侧“租约时刻”时钟：回到构建起点，保证 PROJ_CTRL 租约未过期。"""
        return FakeClock(wall_ms=T0)

    def service_on(self, conn, transport):
        return OperatorEmailService(EmailOutbox(conn, self.restored_clock()),
                                    transport, self.restored_clock(),
                                    enabled_policy())

    def control_on(self, conn):
        return CoordinatorControlService(
            CoordinatorControlLedger(conn, self.lease_clock()),
            self.lease_clock())


def _all_test_methods():
    """收集本模块所有 test_* 方法名（覆盖表自检用）。"""
    out = set()
    for name in dir(sys.modules[__name__]):
        obj = getattr(sys.modules[__name__], name)
        if (isinstance(obj, type) and issubclass(obj, unittest.TestCase)
                and obj is not unittest.TestCase and obj is not MatrixBase):
            for m in dir(obj):
                if m.startswith("test_"):
                    out.add(m)
    return out


class LegalStateTests(MatrixBase):
    """真实 API 合法组合构造 + 显式覆盖表（机器可读，可复现子测试）。"""

    def test_legal_state_construction_coverage(self):
        st = self.st
        # project
        self.assertEqual(st.control.get_project(PROJ_CTRL)["status"], "paused")
        self.assertEqual(st.control.get_project(PROJ_CTRL)["revision"], 3,
                         "pause→resume→pause 历史保留 revision")
        run = st.control.get_project(PROJ_RUN)
        self.assertEqual(run["status"], "running")
        self.assertEqual(run["revision"], 0)
        # session
        fin = st.control.get_session(SID_FINISHED)
        self.assertIsNone(fin["barrier"])
        self.assertEqual(fin["watermark"], st.completed_watermark)
        comb = st.control.get_session(SID_COMBINED)
        self.assertIsNotNone(comb["hold"])
        self.assertIsNotNone(comb["barrier"])
        # provider gate
        self.assertEqual(st.gate.get(GATE_COOLDOWN_SID)["state"], "blocked")
        probe = st.gate.get(GATE_PROBE_SID)
        self.assertEqual(probe["state"], "probe-unknown")
        self.assertEqual(probe["probe_observation"]["checks"],
                         st.probe_observation_checks)
        self.assertEqual(st.gate.get(GATE_MANUAL_SID)["state"], "manual-blocked")
        # dependency
        self.assertEqual(st.store.get_responsibility(st.dep_wait_rid)["state"],
                         STATE_WAITING_DEPENDENCY)
        self.assertEqual(st.store.get_responsibility(st.dep_sat_rid)["state"],
                         STATE_AWAITING_REVIEW)
        self.assertEqual(st.store.get_responsibility(st.dep_human_rid)["state"],
                         STATE_WAITING_HUMAN)
        # mail
        statuses = {name: st.outbox.get_item(item_id)["status"]
                    for name, item_id in st.mail_items.items()}
        self.assertEqual(statuses["pending-auth"], STATUS_PENDING_AUTH)
        self.assertEqual(statuses["pending"], STATUS_PENDING)
        self.assertEqual(statuses["claimed"], STATUS_CLAIMED)
        self.assertEqual(statuses["submit-unknown"], STATUS_SUBMIT_UNKNOWN)
        self.assertEqual(statuses["accepted"], STATUS_ACCEPTED)

    def test_coverage_table_is_complete_and_machine_readable(self):
        report = coverage_report()
        self.assertEqual(report["schema"], "oc-orch.r4-matrix-coverage.v1")
        self.assertTrue(report["complete"], report["missing_states"])
        covered = {}
        for dim, state, _api, _method in COVERAGE_TABLE:
            covered.setdefault(dim, set()).add(state)
        for dim, required in REQUIRED_STATES.items():
            self.assertEqual(required - covered.get(dim, set()), set(),
                             f"覆盖表缺 {dim} 状态：{required - covered.get(dim, set())}")
        referenced_methods = {row[3] for row in COVERAGE_TABLE} | {
            row[2] for row in FAILURE_TABLE}
        missing_methods = referenced_methods - _all_test_methods()
        self.assertEqual(missing_methods, set(),
                         f"覆盖表引用了不存在的测试方法：{missing_methods}")
        self.assertEqual(len(COVERAGE_TABLE), 16)
        self.assertEqual({row[0] for row in FAILURE_TABLE}, REQUIRED_FAILURES)
        # 非穷尽声明：complete 仅表内枚举完整，不冒充全笛卡尔/36 场景/功能完毕
        self.assertIs(report["exhaustive_cartesian"], False)
        self.assertIn("36 场景", report["complete_meaning"])
        self.assertIn("非 openspec 36 场景/功能全量验收", report["scope"])

    def test_each_dimensional_fact_came_from_real_api_not_hand_rows(self):
        """每个维度持久化事实带真实审计/真实调用痕迹（抽查，防手写证据过门）。"""
        st = self.st
        audit_rows = st.conn.execute(
            "SELECT detail FROM audit WHERE action LIKE 'coordinator-control.%'"
        ).fetchall()
        self.assertGreaterEqual(len(audit_rows), 3)
        for op in (st.op_p1, st.op_r1, st.op_p2):
            self.assertIsNotNone(meta_get(st.conn, OPERATION_KEY_PREFIX + op),
                                 f"operation 索引缺失：{op}")
        # 依赖 satisfied 的证据是真实 verify 产物（非 force）且版本绑定
        sat = st.store.get_responsibility(st.dep_sat_rid)
        dep_fact = sat["evidence"]["dependency"]["task_facts"]["T-up-s"]
        self.assertTrue(dep_fact["effective"])
        self.assertEqual(dep_fact["result_version"],
                         dep_fact["verify_result_version"])
        att = st.conn.execute(
            "SELECT * FROM attempts WHERE id='A-up-s'").fetchone()
        verification = json.loads(att["verification"])
        metadata = json.loads(att["metadata"])
        self.assertEqual(verification["result"], "pass")
        self.assertFalse(verification["forced"])
        self.assertEqual(verification["result_snapshot"]["result_version"],
                         metadata["result"]["version"])


class BackupRestoreTests(MatrixBase):
    """合法组合：一致备份恢复保持全部不变量；重复打开/恢复不复活旧状态。"""

    guard = True

    def test_consistent_backup_restore_preserves_all_invariants(self):
        st = self.st
        dest = self.backup()
        info = inspect_backup(dest)
        self.assertTrue(info["ok"])
        self.assertEqual(info["schema_version"], "3")
        self.assertTrue(check_writer_guard(self.st.conn))

        home2, conn2 = self.restore_home(dest)
        self.assertEqual(table_facts(conn2), table_facts(st.conn))
        self.assertEqual(meta_map(conn2), meta_map(st.conn))

        ledger2 = Ledger(conn2, home2, clock=self.lease_clock())
        control2 = self.control_on(conn2)
        gate2 = TargetGate(conn2, self.restored_clock())
        store2 = ResponsibilityStore(ledger2)
        outbox2 = EmailOutbox(conn2, self.restored_clock())

        # 原 ACK
        n = ledger2.get_notification(st.ack_nid)
        self.assertEqual(n["state"], "acked")
        self.assertEqual(n["acked_at"], st.acked_at)
        self.assertEqual(n["message_id"], st.ack_message_id)
        # current/generation
        task = ledger2.get_task(st.biz_task_id)
        self.assertEqual(task["current_attempt_id"], st.biz_attempt_id)
        att = ledger2.get_attempt(st.biz_attempt_id)
        self.assertEqual(att["generation"], 1)
        # 结果版本绑定（真实 verify 产物）
        up = ledger2.get_attempt(st.up_verified_aid)
        self.assertEqual(up["metadata"]["result"]["version"], st.result_version)
        self.assertEqual(up["verification"]["result_snapshot"]["result_version"],
                         st.result_version)
        # 原输入 ID
        self.assertEqual(up["dispatch_msg_id"], "msg_real_in")
        self.assertEqual(att["dispatch_msg_id"], st.original_input_id)
        # 期限/预算
        dep2 = store2.get_responsibility(st.dep_wait_rid)
        self.assertEqual(dep2["deadline"], st.dep_deadline)
        probe2 = gate2.get(GATE_PROBE_SID)
        self.assertEqual(probe2["state"], "probe-unknown")
        self.assertEqual(probe2["probe_observation"]["checks"],
                         st.probe_observation_checks)
        self.assertEqual(probe2["probe_observation"]["deadline_at"],
                         st.probe_observation_deadline)
        self.assertEqual(probe2["probe_observation"]["max_checks"], 8)
        # 控制状态与 operation 索引
        self.assertEqual(control2.get_project(PROJ_CTRL)["status"], "paused")
        self.assertEqual(control2.get_project(PROJ_CTRL)["revision"], 3)
        for op in (st.op_p1, st.op_r1, st.op_p2):
            self.assertIsNotNone(meta_get(conn2, OPERATION_KEY_PREFIX + op))
        self.assertEqual(control2.get_session(SID_FINISHED)["watermark"],
                         st.completed_watermark)
        # 邮件 outbox 状态（claimed/submit-unknown/accepted 不复活）
        self.assertEqual(len(outbox2.list_all()), st.mail_row_count)
        for name, item_id in st.mail_items.items():
            self.assertEqual(outbox2.get_item(item_id)["status"],
                             st.outbox.get_item(item_id)["status"])

    def test_repeated_open_and_restore_does_not_revive_state_or_dup_turns(self):
        st = self.st
        dest = self.backup("backup-1.db")
        home2, conn2 = self.restore_home(dest)
        dest2 = os.path.join(home2, "backup-2.db")
        backup_to(conn2, dest2)
        home3, conn3 = self.restore_home(dest2)
        # 两次“打开→恢复”后全表事实与最初逐字一致（无状态复活/无重复行）
        self.assertEqual(table_facts(conn3), table_facts(st.conn))
        self.assertEqual(meta_map(conn3), meta_map(st.conn))

        control3 = self.control_on(conn3)
        self.assertEqual(control3.get_project(PROJ_CTRL)["revision"], 3)
        replay = control3.pause(PROJ_CTRL, session_id=SID, operation_id=st.op_p2,
                                reason="user-requested-2", expected_revision=0)
        self.assertTrue(replay["executed"])
        self.assertFalse(replay["applied"])
        self.assertEqual(control3.get_project(PROJ_CTRL)["revision"], 3,
                         "历史操作重放不得推进 revision 或冒认新暂停")

        # 依赖复核事实不重复发布（不多造模型回合/通知）
        ledger3 = Ledger(conn3, home3, clock=self.restored_clock())
        store3 = ResponsibilityStore(ledger3)
        dep3 = DependencyWaitService(ledger3, self.restored_clock(), store=store3)
        out = dep3.reconcile(limit=10)
        self.assertEqual(out["ready"], 0, out["details"])
        notif_count = conn3.execute(
            "SELECT COUNT(*) FROM notifications").fetchone()[0]
        self.assertEqual(notif_count, st.notif_total,
                         "恢复后重复对账不得再造复核通知/结果通知")

        # 邮件：恢复 + 一次 dispatch 只发那一条 pending；不重发其余
        before_sends = len(st.transport.sent)
        svc3 = self.service_on(conn3, st.transport)
        r = svc3.dispatch_once(owner="matrix")
        self.assertEqual(r["result"], "accepted", r)
        self.assertEqual(len(st.transport.sent) - before_sends, 1,
                         "恢复后只发一条 pending，不重发其余")
        outbox_c3 = EmailOutbox(conn3, self.restored_clock())
        claimed_row = outbox_c3.get_item(st.mail_items["claimed"])
        self.assertEqual(claimed_row["status"], STATUS_CLAIMED,
                         "claimed 不因恢复被重置回待发")
        unknown_row = outbox_c3.get_item(st.mail_items["submit-unknown"])
        self.assertEqual(unknown_row["status"], STATUS_SUBMIT_UNKNOWN)
        r2 = svc3.dispatch_once(owner="matrix")
        self.assertEqual(r2["result"], "nothing-due", r2)

    def test_pure_copy_cycles_digest_stable(self):
        """纯副本矩阵：backup→restore→backup 多次，事实摘要逐字一致。"""
        st = self.st
        reference = table_facts(st.conn)
        current_conn = st.conn
        current_home = st.home
        for i in range(3):
            dest = os.path.join(current_home, f"copy-{i}.db")
            backup_to(current_conn, dest)
            home_n = tempfile.mkdtemp(prefix=f"r4-copy-{i}-", dir="/tmp/opencode")
            self.addCleanup(shutil.rmtree, home_n)
            shutil.copyfile(dest, os.path.join(home_n, "ledger.db"))
            conn_n = self.track_conn(init(home_n))
            self.assertEqual(table_facts(conn_n), reference,
                             f"第 {i} 轮副本摘要不一致")
            self.assertEqual(meta_map(conn_n), meta_map(st.conn))
            current_home, current_conn = home_n, conn_n


class InstallRollbackTests(MatrixBase):
    """安装/事务中断回滚：失败不落任何半成品事实。"""

    def test_guard_install_failure_rolls_back_whole_transaction(self):
        st = self.st
        before = table_facts(st.conn)
        before_meta = meta_map(st.conn)
        real_expected = __import__(
            "oc_orch.adapters.writer_guard", fromlist=["_expected_triggers"]
        )._expected_triggers

        def bad_sql():
            expected = real_expected()
            expected["broken"] = "CREATE TRIGGER bad NOT VALID"
            return expected

        with patch("oc_orch.adapters.writer_guard._expected_triggers",
                   side_effect=bad_sql):
            with self.assertRaises(sqlite3.OperationalError):
                install_writer_guard(st.conn, actor="matrix")
        self.assertEqual(table_facts(st.conn), before, "安装失败不得改任何业务行")
        self.assertEqual(meta_map(st.conn), before_meta,
                         "安装失败不得留 meta/trigger 半成品")
        self.assertFalse(check_writer_guard(st.conn))
        # 现场仍可按“未安装门”的旧 schema3 语义读写（失败回滚后无残留门）
        raw = self.track_conn(_raw_conn(st.home))
        raw.execute("INSERT INTO meta(key,value) VALUES('post-fail','ok')")
        raw.commit()
        self.assertIsNotNone(st.conn.execute(
            "SELECT 1 FROM meta WHERE key='post-fail'").fetchone())

    def test_guard_install_inside_existing_transaction_refused(self):
        st = self.st
        st.conn.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(RuntimeError):
                install_writer_guard(st.conn, actor="matrix")
        finally:
            st.conn.rollback()
        self.assertFalse(check_writer_guard(st.conn))

    def test_control_write_failure_rolls_back_no_partial_state(self):
        """事务内写入中途失败：项目记录/operation 索引/审计都不残留。"""
        st = self.st
        project = "/tmp/opencode/matrix-abort"
        st.ledger.claim_coordinator(project=project, session_id=SID, actor="matrix")
        before = meta_map(st.conn)
        orig_store_operation = st.control_ledger._store_operation

        def boom(rec):
            raise RuntimeError("injected-store-failure")

        st.control_ledger._store_operation = boom
        try:
            with self.assertRaises(RuntimeError):
                st.control.pause(project, session_id=SID, operation_id="op-abort",
                                 reason="r", expected_revision=0)
        finally:
            st.control_ledger._store_operation = orig_store_operation
        self.assertEqual(meta_map(st.conn), before,
                         "事务中断后不得残留暂停/operation 索引/审计，且旧事实不丢")
        # 现场事务外可正常重试（证明未半写、可继续合法 CAS）
        ok = st.control.pause(project, session_id=SID, operation_id="op-abort",
                              reason="r", expected_revision=0)
        self.assertTrue(ok["applied"], ok)
        self.assertEqual(st.control.get_project(project)["status"], "paused")

    def test_nested_transaction_is_refused_without_side_effect(self):
        """外层 BEGIN 未提交时嵌套控制事务被安全拒绝，无半成品。"""
        st = self.st
        project = "/tmp/opencode/matrix-nested"
        st.ledger.claim_coordinator(project=project, session_id=SID, actor="matrix")
        before = meta_map(st.conn)
        st.conn.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(sqlite3.OperationalError):
                st.control.pause(project, session_id=SID, operation_id="op-n",
                                 reason="r", expected_revision=0)
        finally:
            st.conn.rollback()
        self.assertEqual(meta_map(st.conn), before)


class FailClosedTests(MatrixBase):
    """坏 JSON/schema/缺字段一律 fail-closed，且不影响其他维度。"""

    guard = True

    def _corrupt_meta(self, key, value):
        meta_set(self.st.conn, key, value)

    def _renew_lease(self):
        """把 PROJ_CTRL 租约续到未来，让控制操作在最终时钟下可执行。"""
        self.st.conn.execute(
            "UPDATE coordinator_leases SET expires_at=? WHERE project=?",
            (self.st.clock.now_ms() + 300 * 1000, PROJ_CTRL))
        self.st.conn.commit()

    def test_bad_json_control_fail_closed(self):
        st = self.st
        self._corrupt_meta(PROJECT_KEY_PREFIX + PROJ_CTRL, "{not-json")
        with self.assertRaises(CoordinatorControlStateError):
            st.control.get_project(PROJ_CTRL)
        with self.assertRaises(CoordinatorControlStateError):
            st.control.resume(PROJ_CTRL, session_id=SID, operation_id="op-x",
                              reason="r", expected_revision=3)
        # 其他维度仍可读（坏一行不得污染整个台账）
        self.assertEqual(st.control.get_project(PROJ_RUN)["status"], "running")
        self.assertIsNotNone(st.gate.get(GATE_PROBE_SID))
        self.assertIsNotNone(st.outbox.get_item(st.mail_items["pending"]))
        self.assertEqual(st.control.get_session(SID_FINISHED)["watermark"],
                         st.completed_watermark)

    def test_bad_json_session_and_operation_fail_closed(self):
        st = self.st
        self._corrupt_meta(SESSION_KEY_PREFIX + SID_COMBINED, "{bad")
        with self.assertRaises(CoordinatorControlStateError):
            st.control.get_session(SID_COMBINED)
        with self.assertRaises(CoordinatorControlStateError):
            st.control.begin_user_turn(SID_COMBINED, message_id="m",
                                       origin_at=T0 + 5, operation_id="o",
                                       trusted=True)
        self._renew_lease()
        self._corrupt_meta(OPERATION_KEY_PREFIX + st.op_p2, "{bad")
        with self.assertRaises(CoordinatorControlStateError):
            st.control.pause(PROJ_CTRL, session_id=SID, operation_id=st.op_p2,
                             reason="user-requested-2", expected_revision=3)
        # 失败后现场其他维度仍可读
        self.assertEqual(st.control.get_project(PROJ_RUN)["status"], "running")

    def test_bad_json_gate_and_mail_fail_closed(self):
        st = self.st
        self._corrupt_meta(GATE_KEY_PREFIX + GATE_PROBE_SID, "{junk")
        with self.assertRaises(TargetGateStateError):
            st.gate.get(GATE_PROBE_SID)
        with self.assertRaises(TargetGateStateError):
            st.gate.record_success(GATE_PROBE_SID, message_id="x",
                                   observed_at=T0, evidence_key="k")
        item = st.mail_items["pending"]
        self._corrupt_meta(MAIL_KEY_PREFIX + item, "{junk")
        with self.assertRaises(MailOutboxStateError):
            st.outbox.get_item(item)
        with self.assertRaises(MailOutboxStateError):
            st.outbox.claim(item, owner="x", now=T0)
        # 其他邮件行不受影响
        self.assertEqual(st.outbox.get_item(st.mail_items["accepted"])["status"],
                         STATUS_ACCEPTED)

    def test_schema_mismatch_fail_closed_across_dimensions(self):
        st = self.st
        self._corrupt_meta(MAIL_SCHEMA_KEY, "99")
        with self.assertRaises(MailOutboxStateError):
            st.outbox.list_all()
        with self.assertRaises(MailOutboxStateError):
            st.outbox.claim(st.mail_items["pending"], owner="x", now=T0)
        self._corrupt_meta(MAIL_SCHEMA_KEY, "1")
        self.assertEqual(st.outbox.get_item(st.mail_items["pending"])["status"],
                         STATUS_PENDING)

        self._corrupt_meta("coordinator-control:schema", "99")
        with self.assertRaises(CoordinatorControlStateError):
            st.control.get_project(PROJ_CTRL)
        self._corrupt_meta("target-gate:schema", "99")
        with self.assertRaises(TargetGateStateError):
            st.gate.list_all()

    def test_missing_fields_fail_closed_for_outbox_invariants(self):
        """accepted 缺受理证据 / claimed 缺 owner → fail-closed；坏记录不被覆盖。"""
        st = self.st
        item = st.mail_items["pending"]
        raw = st.conn.execute(
            "SELECT value FROM meta WHERE key=?", (MAIL_KEY_PREFIX + item,)
        ).fetchone()[0]
        rec = json.loads(raw)
        rec["status"] = STATUS_ACCEPTED
        rec["submitted_at"] = None
        rec["external_ref"] = None  # 缺受理证据 → 结构非法
        bad_value = json.dumps(rec)
        self._corrupt_meta(MAIL_KEY_PREFIX + item, bad_value)
        with self.assertRaises(MailOutboxStateError):
            st.outbox.get_item(item)
        with self.assertRaises(MailOutboxStateError):
            st.outbox.update_observation(item, observed_at=T0, summary={}, now=T0)
        # 损坏记录不被静默覆盖
        self.assertEqual(st.conn.execute(
            "SELECT value FROM meta WHERE key=?", (MAIL_KEY_PREFIX + item,)
        ).fetchone()[0], bad_value)


class OldWriterRefusedTests(MatrixBase):
    """旧 schema3 连接（无契约函数）与（可选）真实旧实现试写拒绝；只临时库。"""

    guard = True

    def test_old_connection_write_refused_all_guarded_tables(self):
        st = self.st
        before = table_facts(st.conn)
        old = self.track_conn(_raw_conn(st.home))
        old.execute("SELECT * FROM meta").fetchall()  # 模拟安装前已打开的连接
        for table in GUARD_TABLES:
            col = st.conn.execute(f'PRAGMA table_info("{table}")').fetchone()[1]
            statements = (
                f'INSERT INTO "{table}" SELECT * FROM "{table}" WHERE 0',
                f'UPDATE "{table}" SET "{col}"="{col}" WHERE 0',
                f'DELETE FROM "{table}" WHERE 0',
            )
            for sql in statements:
                with self.subTest(table=table, statement=sql):
                    with self.assertRaisesRegex(
                            sqlite3.OperationalError, "oc_orch_writer_contract"):
                        old.execute(sql)
        self.assertEqual(table_facts(st.conn), before, "旧连接写失败后零数据变化")

    def test_old_implementation_refused_when_old_source_provided(self):
        """真实旧实现子进程试写拒绝；仅当 OC_ORCH_MATRIX_OLD_SOURCE 提供时运行。"""
        old_source = os.environ.get("OC_ORCH_MATRIX_OLD_SOURCE", "")
        if not old_source:
            self.skipTest("未提供 OC_ORCH_MATRIX_OLD_SOURCE；真实旧实现试写由 "
                          "scripts/check_continuity_release.py --old-source 证明")
        st = self.st
        dest = self.backup("old-probe.db")
        home2 = tempfile.mkdtemp(prefix="r4-oldimpl-", dir="/tmp/opencode")
        self.addCleanup(shutil.rmtree, home2)
        shutil.copyfile(dest, os.path.join(home2, "ledger.db"))
        code = ("from oc_orch.db import init; from oc_orch.ledger import Ledger; "
                "import sys; c=init(sys.argv[1]); "
                "Ledger(c, sys.argv[1]).set_meta('r4-old-writer-probe','must-not-exist')")
        env = dict(os.environ,
                   PYTHONPATH=str(Path(old_source).resolve() / "src"),
                   PYTHONDONTWRITEBYTECODE="1",
                   OC_ORCH_HOME=home2)
        old = subprocess.run([sys.executable, "-B", "-c", code, home2], cwd=home2,
                             env=env, capture_output=True, text=True, timeout=45)
        self.assertNotEqual(old.returncode, 0)
        self.assertIn("oc_orch_writer_contract", old.stderr)
        ro = sqlite3.connect(f"file:{os.path.join(home2, 'ledger.db')}?mode=ro",
                             uri=True, isolation_level=None)
        self.addCleanup(ro.close)
        self.assertIsNone(ro.execute(
            "SELECT 1 FROM meta WHERE key='r4-old-writer-probe'").fetchone())


class RetainedFactsTests(MatrixBase):
    """新事实保留后恢复不倒旧快照；恢复是当前账的再一次一致备份。"""

    guard = True

    def test_recovery_reuses_current_account_not_old_snapshot(self):
        st = self.st
        snapshot_before = table_facts(st.conn)
        dest = self.backup("snapshot-a.db")

        # 备份后写入新事实（真实 API）：新暂停、新邮件 accepted、新 ACK
        st.ledger.claim_coordinator(project="/tmp/opencode/matrix-newpause",
                                    session_id=SID, actor="matrix")
        p = st.control.pause("/tmp/opencode/matrix-newpause", session_id=SID,
                             operation_id="op-new-pause", reason="r",
                             expected_revision=0)
        self.assertTrue(p["applied"], p)
        now = st.clock.now_ms()
        acc2 = st.svc_on.enqueue_incident(
            incident_fact(incident_id="inc-new", observed_at=now))
        st.transport.send_result = {"queued": True}
        r = st.svc_on.dispatch_once(item_id=acc2["item_id"], owner="matrix")
        self.assertEqual(r["result"], "accepted", r)
        st.ledger.enqueue_notification(
            kind="continuation_ready", target_session=SID, dedupe_key="new-ack-k",
            payload={"classification": "MATRIX_NEW"}, project=PROJ_RUN,
            task_id=st.biz_task_id, next_attempt_at=T0)
        new_notif = st.ledger.list_notifications(
            target_session=SID, state=None, limit=100)[0]
        st.ledger.claim_notifications([new_notif["id"]], owner="matrix")
        st.ledger.mark_notification_sent(new_notif["id"], message_id="msg-new-ack",
                                         next_attempt_at=T0, owner="matrix")
        st.ledger.ack_notifications([new_notif["id"]], actor="coordinator")

        after_new = table_facts(st.conn)
        self.assertNotEqual(after_new, snapshot_before, "确已写入新事实")

        # 恢复流程 = 对当前账再做一致备份（不是倒回 snapshot-a）
        recovery_home, recovery_conn = self.restore_home(self.backup("recovery.db"))
        self.assertEqual(table_facts(recovery_conn), after_new,
                         "恢复保留新事实，不倒回旧快照")
        self.assertEqual(meta_map(recovery_conn), meta_map(st.conn))
        # 旧快照文件保持原样、未被新账改写（只读打开内容与当时一致）
        ro = sqlite3.connect(f"file:{dest}?mode=ro", uri=True, isolation_level=None)
        self.addCleanup(ro.close)
        self.assertEqual(table_facts(ro), snapshot_before,
                         "旧备份文件是只读快照，不得被新账改写")

    def test_recovery_backup_keeps_new_facts_and_guard(self):
        st = self.st
        self.backup("snapshot-b.db")
        st.ledger.set_meta("matrix:new-unknown-input", "same-original-message-id")
        recovery = os.path.join(st.home, "recovery-b")
        os.makedirs(recovery, exist_ok=True)
        backup_to(st.conn, os.path.join(recovery, "ledger.db"))
        restored = self.track_conn(init(recovery))
        self.assertEqual(Ledger(restored, recovery).get_meta(
            "matrix:new-unknown-input"), "same-original-message-id")
        self.assertTrue(check_writer_guard(restored))
        self.assertEqual(meta_get(st.conn, "matrix:new-unknown-input"),
                         "same-original-message-id")


class LateOverridesTests(MatrixBase):
    """晚到 idle/ACK/回执不得覆盖更新意图。"""

    guard = True

    def test_late_idle_and_late_hold_cannot_clear_barrier_or_rebuild_hold(self):
        st = self.st
        comb = st.control.get_session(SID_COMBINED)
        bar_rev = comb["revision"]
        # 旧 idle（<= 水位）清屏障 → 拒绝，屏障保持
        late = st.control.finish_user_turn(
            SID_COMBINED, message_id="msg-m1", idle_id="idle-late",
            idle_at=comb["watermark"],  # 等于水位 → stale-termination
            expected_revision=bar_rev)
        self.assertFalse(late["executed"])
        self.assertEqual(late["refused"], "stale-termination")
        self.assertIsNotNone(st.control.get_session(SID_COMBINED)["barrier"])
        # 旧 hold（<= 水位）重建 hold → 拒绝
        late_hold = st.control.hold_session(
            SID_COMBINED, event_id="ev-late", origin_at=comb["watermark"],
            reason="again")
        self.assertFalse(late_hold["executed"])
        self.assertEqual(late_hold["refused"], "stale-event")
        # 旧 probe 观察（非新 idle）→ 拒绝且预算不消耗
        probe_view = st.gate.get(GATE_PROBE_SID)
        obs = st.gate.claim_probe_observation(
            GATE_PROBE_SID, message_id=st.probe_message_id,
            expected_revision=probe_view["revision"],
            idle_at=probe_view["probe_observation"]["last_claim_at"])
        self.assertFalse(obs["allowed"])
        self.assertIn(obs["reason"], ("stale-observation", "observation-backoff"))
        after = st.gate.get(GATE_PROBE_SID)
        self.assertEqual(after["probe_observation"]["checks"],
                         st.probe_observation_checks, "旧观察不得再度消耗预算")

    def test_late_probe_success_not_overriding_newer_error(self):
        st = self.st
        # 新失败（非 probe-fail 归属）推进水位但保留原 probe（probe-unattributed）
        newer = st.gate.observe_error(
            GATE_PROBE_SID, {"category": "quota", "status": 429},
            evidence_key="mx-e2-late",
            observed_at=st.clock.now_ms(), origin_at=st.clock.now_ms())
        self.assertTrue(newer["changed"], newer)
        # 晚到成功（时间 <= 新水位）→ 拒绝，不清门、保持原身份
        started = st.gate.get(GATE_PROBE_SID)["probe_started_at"]
        stale = st.gate.record_success(
            GATE_PROBE_SID, message_id=st.probe_message_id,
            observed_at=started + 1, evidence_key="late-success")
        self.assertFalse(stale["cleared"])
        self.assertEqual(stale["reason"], "newer-error")
        view = st.gate.get(GATE_PROBE_SID)
        self.assertIsNotNone(view)
        self.assertEqual(view["probe_message_id"], st.probe_message_id)

    def test_late_ack_and_late_dispatch_confirm_no_op_or_no_regress(self):
        st = self.st
        before = st.ledger.get_notification(st.ack_nid)
        # 已 ACK 通知：重复 ACK 幂等为 0
        self.assertEqual(st.ledger.ack_notifications([st.ack_nid], actor="late"), 0)
        # 晚到 sent 不得重开责任、不得重复计 attempts、不得改写 ACK 时间
        st.ledger.mark_notification_sent(st.ack_nid, message_id="msg-late-ack",
                                         next_attempt_at=T0, owner="matrix")
        n = st.ledger.get_notification(st.ack_nid)
        self.assertEqual(n["state"], "acked")
        self.assertEqual(n["acked_at"], st.acked_at, "晚到 sent 不得改写 ACK 时间")
        self.assertEqual(n["attempts"], before["attempts"],
                         "同一物理消息晚到确认不得重复计 attempts")
        # 业务尝试：已 confirmed 派单不得被晚到 unknown 结果覆盖
        att = st.ledger.get_attempt(st.up_verified_aid)
        self.assertEqual(att["dispatch_state"], "confirmed")
        again = st.ledger.record_dispatch(st.up_verified_aid, "unknown",
                                          actor="late")
        self.assertEqual(again["dispatch_state"], "confirmed",
                         "晚到 unknown 派单事实不能覆盖已确认派单")
        self.assertEqual(st.ledger.get_attempt(st.up_verified_aid)["status"],
                         "accepted")

    def test_late_receipt_not_overriding_new_event_or_accepted_evidence(self):
        st = self.st
        unknown_item = st.mail_items["submit-unknown"]
        # 新事件（条件版本变化）先登记
        upd = st.svc_on.enqueue_incident(
            incident_fact(incident_id="inc-unknown", condition_version=2,
                          observed_at=st.clock.now_ms()))
        self.assertEqual(upd["result"], "enqueued", upd)
        upd_id = upd["item_id"]
        self.assertNotEqual(upd_id, unknown_item)

        # 第一次 claim 消耗回执预算并推进 receipt_revision；晚到写入（旧 revision）拒绝
        c1 = st.outbox.claim_receipt(unknown_item, owner="observer-a", now=T0)
        self.assertTrue(c1["allowed"], c1)
        stale = st.outbox.record_receipt(
            unknown_item, expected_receipt_revision=c1["receipt_revision"] - 1,
            receipt_match="unique", external_ref="msg_late", now=T0)
        self.assertFalse(stale["applied"])
        self.assertEqual(stale["reason"], "receipt-revision-changed")
        # 以当前 CAS 的 unique 回执只更新旧行 → accepted；新事件保持 pending
        c2 = st.outbox.claim_receipt(unknown_item, owner="observer-b", now=T0)
        self.assertTrue(c2["allowed"], c2)
        done = st.outbox.record_receipt(
            unknown_item, expected_receipt_revision=c2["receipt_revision"],
            receipt_match="unique", external_ref="msg_first", now=T0)
        self.assertTrue(done["applied"], done)
        self.assertEqual(st.outbox.get_item(unknown_item)["status"],
                         STATUS_ACCEPTED)
        self.assertEqual(st.outbox.get_item(unknown_item)["external_ref"],
                         "msg_first")
        self.assertEqual(st.outbox.get_item(upd_id)["status"], STATUS_PENDING,
                         "晚到回执只更新原记录，不覆盖新事件")



class ScriptBoundaryTests(unittest.TestCase):
    """scripts/check_continuity_release.py 参数边界：越界/危险路径必须在任何
    mkdir/复制之前拒绝（exit 2），且从不覆盖/删除用户内容。"""

    SCRIPT = REPO / "scripts" / "check_continuity_release.py"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="r4-script-boundary-",
                                               dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        home = os.path.join(self.tmp.name, "seed")
        os.makedirs(home, exist_ok=True)
        conn = init(home)
        self.addCleanup(conn.close)
        Ledger(conn, home).create_task(project="p", title="t", task_id="T-b")
        self.backup = os.path.join(self.tmp.name, "backup.db")
        backup_to(conn, self.backup)

    def run_script(self, work_name="work-cmd", *extra_args):
        work = os.path.join(self.tmp.name, work_name)
        cmd = [sys.executable, "-B", str(self.SCRIPT),
               "--backup", self.backup, "--work", work,
               "--old-source", "PARSE-TIME-ONLY-PLACEHOLDER"] + list(extra_args)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                              cwd=str(REPO))
        return proc, work

    def test_copy_matrix_out_of_bounds_rejected_before_work_created(self):
        cases = [("-1", "越界"), ("11", "越界"), ("1000000000", "越界"),
                 ("abc", "必须是 0..10")]
        for value, needle in cases:
            with self.subTest(value=value):
                proc, work = self.run_script(f"work-{value}",
                                             "--copy-matrix", value)
                self.assertEqual(proc.returncode, 2, proc.stderr)
                self.assertIn(needle, proc.stderr)
                self.assertFalse(os.path.exists(work),
                                 f"越界 {value} 不得创建 work 目录")

    def test_copy_matrix_bound_helpers(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("ccr_script_boundary",
                                                      self.SCRIPT)
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        self.assertEqual(mod.MAX_COPY_MATRIX, 10)
        self.assertEqual(mod._bounded_copy_matrix("0"), 0)
        self.assertEqual(mod._bounded_copy_matrix("10"), 10)
        # 模块结构完整（此前插入 helper 曾误吞 class 声明，防止结构回归）
        self.assertTrue(hasattr(mod, "NoNetworkMail"))
        self.assertTrue(callable(mod._validate_report_path))
        for bad in ("-1", "11", "abc", "1e9"):
            with self.subTest(bad=bad):
                with self.assertRaises(argparse.ArgumentTypeError):
                    mod._bounded_copy_matrix(bad)

    def test_report_existing_rejected_and_content_preserved(self):
        target = os.path.join(self.tmp.name, "out.json")
        with open(target, "w", encoding="utf-8") as fh:
            fh.write("user-content-keep-me")
        proc, work = self.run_script("work-report-exists", "--report", target,
                                     "--copy-matrix", "1")
        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertIn("已存在", proc.stderr)
        self.assertFalse(os.path.exists(work))
        with open(target, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "user-content-keep-me")

    def test_report_equal_backup_rejected_backup_unchanged(self):
        import hashlib as _hashlib
        with open(self.backup, "rb") as fh:
            before = _hashlib.sha256(fh.read()).hexdigest()
        proc, work = self.run_script("work-report-backup",
                                     "--report", self.backup)
        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertIn("不得与 --backup 同一路径", proc.stderr)
        self.assertFalse(os.path.exists(work))
        with open(self.backup, "rb") as fh:
            after = _hashlib.sha256(fh.read()).hexdigest()
        self.assertEqual(before, after, "备份文件不得被改写")

    def test_report_ledgerdb_under_work_rejected(self):
        proc, work = self.run_script(
            "work-report-ledger",
            "--report", os.path.join(self.tmp.name, "work-report-ledger",
                                     "guarded", "ledger.db"))
        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertIn("ledger.db", proc.stderr)
        self.assertFalse(os.path.exists(work))

    def test_report_parent_is_file_rejected(self):
        blocked = os.path.join(self.tmp.name, "blocked")
        with open(blocked, "w", encoding="utf-8") as fh:
            fh.write("I am a file, not a directory")
        proc, work = self.run_script(
            "work-report-parent-file",
            "--report", os.path.join(blocked, "report.json"))
        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertIn("不是目录", proc.stderr)
        self.assertFalse(os.path.exists(work))


if __name__ == "__main__":
    unittest.main(verbosity=2)
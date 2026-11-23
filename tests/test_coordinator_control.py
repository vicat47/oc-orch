"""对话控制底座：项目调谐意图（C01–C08）存储/规则/共享用例与确定性交错。

覆盖（openspec improve-coordinator-continuity / specs/coordinator-continuity）：

- C01 明确停止调谐：持久暂停、审计、带真实快照确认，Task/通知不被改写；
- C02 明确继续调谐：幂等解除、返回实际控制版本；
- C04 归属/来源不明确：租约 holder/epoch/operation-id/expected_revision 缺失
  或不符一律拒绝（fail-closed），绝不猜测；
- C05 暂停与 provider 冷却共存：resume 只改项目维度，provider 门保持；
- C06 迟到恢复与新暂停交错：旧 expected_revision 拒绝、历史 op 重放不修改
  当前状态（稳定 operation 索引仲裁）；
- C07 重启与历史 manual 状态：重启后 revision/暂停/op 索引保持；租约过期/
  recovery_blocked 拒绝晚到控制操作；target-gate manual 记录不被恢复删除；
- C08 停用/交接：非 holder / epoch / 过期租约的晚到操作一律拒绝。

仅标准库 + FakeClock + 真临时 SQLite（WAL 两连接）离线测试；不访问真实服务
/台账、不发消息、不跑 offline_discovery（新文件由父注册 runner）。
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from oc_orch.adapters.coordinator_control import (
    EPOCH_MISMATCH,
    INVALID_EPOCH,
    LEASE_EXPIRED,
    LEASE_HOLDER_MISMATCH,
    LEASE_MISSING,
    LEASE_RECOVERY_BLOCKED,
    OPERATION_KEY_PREFIX,
    PROJECT_KEY_PREFIX,
    SCHEMA_KEY,
    CoordinatorControlLedger,
)
from oc_orch.adapters.sqlite_db import backup_to, init, inspect_backup, meta_get
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.target_gate import TargetGate
from oc_orch.application.coordinator_control import (
    CoordinatorControlService,
    CoordinatorControlStateError,
)
from tests.fakes import FakeClock


def _open_wal(home):
    """测试专用 WAL 两连接：等价 sqlite_db.connect 的 PRAGMA 组合。"""
    conn = sqlite3.connect(os.path.join(home, "ledger.db"), timeout=15,
                           isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


class ControlBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="coord-control-",
                                               dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.control = CoordinatorControlLedger(self.conn, self.clock)
        self.service = CoordinatorControlService(self.control, self.clock)
        self.project = "/tmp/opencode/coord-control-project"
        self.sid = "ses_control_holder"
        self.ledger.claim_coordinator(project=self.project, session_id=self.sid,
                                      actor="test")

    # ---- 断言辅助 ----------------------------------------------------
    def pause_ok(self, *, operation="pause-op", reason="user-requested",
                 expected_revision=0, expected_epoch=None, evidence=None):
        return self.service.pause(
            self.project, session_id=self.sid, operation_id=operation,
            reason=reason, expected_revision=expected_revision,
            expected_epoch=expected_epoch, evidence=evidence)

    def audit_rows(self, action, entity, entity_id):
        return self.conn.execute(
            "SELECT from_state, to_state, detail FROM audit "
            "WHERE action=? AND entity=? AND entity_id=? ORDER BY id",
            (action, entity, entity_id)).fetchall()


class PauseResumeTests(ControlBase):
    def test_c01_pause_persists_snapshot_and_audits(self):
        before_tasks = self.conn.execute(
            "SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
        before_notif = self.conn.execute(
            "SELECT COUNT(*) AS n FROM notifications").fetchone()["n"]
        result = self.pause_ok(evidence={"where": "user-dialog"})
        self.assertTrue(result["executed"])
        self.assertIsNone(result["refused"])
        self.assertTrue(result["applied"])
        self.assertEqual(result["reason"], "paused")
        self.assertEqual(result["epoch"], 1, "结果顶层携带当前租约代次")
        snap = result["snapshot"]
        self.assertEqual(snap["status"], "paused")
        self.assertEqual(snap["revision"], 1)
        self.assertEqual(snap["paused"], True)
        self.assertEqual(snap["operation_id"], "pause-op")
        self.assertEqual(snap["reason"], "user-requested")
        self.assertEqual(snap["evidence"], {"where": "user-dialog"})
        self.assertEqual(snap["holder_session"], self.sid)
        self.assertEqual(snap["epoch"], 1)
        # 结果就是真实快照：get_project 一致
        self.assertEqual(self.service.get_project(self.project), snap)
        # 审计同事务落库
        rows = self.audit_rows("coordinator-control.pause", "project",
                               self.project)
        self.assertEqual(len(rows), 1)
        detail = json.loads(rows[0]["detail"])
        self.assertEqual(detail["operation_id"], "pause-op")
        self.assertEqual(rows[0]["from_state"], "running")
        self.assertEqual(rows[0]["to_state"], "paused")
        # 暂停不改 Task/通知（停止调谐不停止 worker、不改业务状态）
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) AS n FROM tasks").fetchone()["n"], before_tasks)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) AS n FROM notifications").fetchone()["n"],
            before_notif)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) AS n FROM audit WHERE entity='task'"
        ).fetchone()["n"], 0)
        # 持久记录与 operation 索引可读回
        self.assertEqual(meta_get(self.conn, SCHEMA_KEY), "1")
        raw = meta_get(self.conn, PROJECT_KEY_PREFIX + self.project)
        self.assertIsNotNone(raw)
        self.assertIn('"status": "paused"', raw)
        op_raw = meta_get(self.conn, OPERATION_KEY_PREFIX + "pause-op")
        self.assertIsNotNone(op_raw)
        op = json.loads(op_raw)
        self.assertEqual(op["action"], "pause")
        self.assertEqual(op["project"], self.project)
        self.assertEqual(op["revision_after"], 1)

    def test_c02_resume_idempotent_and_revision(self):
        first = self.pause_ok()
        self.assertTrue(first["executed"])
        resumed = self.service.resume(
            self.project, session_id=self.sid, operation_id="resume-op",
            reason="user-resumed", expected_revision=1)
        self.assertTrue(resumed["executed"])
        self.assertTrue(resumed["applied"])
        self.assertEqual(resumed["reason"], "resumed")
        self.assertEqual(resumed["snapshot"]["status"], "running")
        self.assertEqual(resumed["snapshot"]["revision"], 2)
        self.assertEqual(resumed["epoch"], 1)
        # 重复/重试同一控制操作 → 幂等解除
        retry = self.service.resume(
            self.project, session_id=self.sid, operation_id="resume-op",
            reason="user-resumed", expected_revision=2)
        self.assertTrue(retry["executed"])
        self.assertFalse(retry["applied"])
        self.assertEqual(retry["reason"], "already-applied")
        self.assertEqual(retry["revision"], 2)
        # 幂等重复不新增审计
        rows = self.audit_rows("coordinator-control.resume", "project",
                               self.project)
        self.assertEqual(len(rows), 1)
        # 同一 operation_id、参数变更不得冒认重复
        conflict = self.service.resume(
            self.project, session_id=self.sid, operation_id="resume-op",
            reason="different-reason", expected_revision=2)
        self.assertFalse(conflict["executed"])
        self.assertEqual(conflict["refused"], "operation-conflict")
        self.assertEqual(conflict["snapshot"]["revision"], 2)

    def test_resume_not_paused_refused(self):
        result = self.service.resume(
            self.project, session_id=self.sid, operation_id="op-r",
            reason="resume", expected_revision=0)
        self.assertFalse(result["executed"])
        self.assertEqual(result["refused"], "not-paused")
        self.assertEqual(result["revision"], 0)
        self.assertEqual(result["snapshot"]["status"], "running")
        self.assertEqual(result["epoch"], 1, "拒绝结果仍携带真实 epoch")

    def test_parameter_change_not_idempotent_for_pause(self):
        self.pause_ok(reason="reason-A")
        repeat = self.service.pause(
            self.project, session_id=self.sid, operation_id="pause-op",
            reason="reason-B", expected_revision=1)
        self.assertFalse(repeat["executed"])
        self.assertEqual(repeat["refused"], "operation-conflict")
        # 不同 operation_id 对已暂停项目 → already-paused
        other = self.service.pause(
            self.project, session_id=self.sid, operation_id="pause-op-2",
            reason="again", expected_revision=1)
        self.assertFalse(other["executed"])
        self.assertEqual(other["refused"], "already-paused")
        self.assertEqual(self.service.get_project(self.project)["revision"], 1)

    def test_operation_index_historical_replay_never_modifies(self):
        """父要求反例：pause(op1)->resume(op2)->pause(op3)，旧 op1 重放不能成为
        新暂停，也不能冒认为未出现；同 ID 改参 → 冲突，一律不改状态。"""
        self.pause_ok(operation="op1", reason="pause-1")          # rev1, paused
        self.service.resume(self.project, session_id=self.sid,
                            operation_id="op2", reason="resume-2",
                            expected_revision=1)                  # rev2
        third = self.service.pause(
            self.project, session_id=self.sid, operation_id="op3",
            reason="pause-3", expected_revision=2)                # rev3
        self.assertTrue(third["applied"])
        snap = self.service.get_project(self.project)
        self.assertEqual(snap["revision"], 3)
        self.assertEqual(snap["operation_id"], "op3")
        self.assertEqual(snap["status"], "paused")
        # 历史 op1 重放（即使 expected_revision 是旧值/新值）→ 幂等，状态不动
        replay = self.service.pause(
            self.project, session_id=self.sid, operation_id="op1",
            reason="pause-1", expected_revision=0)
        self.assertTrue(replay["executed"])
        self.assertFalse(replay["applied"])
        self.assertEqual(replay["reason"], "already-applied")
        snap2 = self.service.get_project(self.project)
        self.assertEqual(snap2["revision"], 3, "历史重放不推进 revision")
        self.assertEqual(snap2["operation_id"], "op3", "历史重放不冒认为新暂停")
        # 历史 op1 同 ID 改参 → operation-conflict，不改状态
        conflict = self.service.pause(
            self.project, session_id=self.sid, operation_id="op1",
            reason="changed-params", expected_revision=3)
        self.assertFalse(conflict["executed"])
        self.assertEqual(conflict["refused"], "operation-conflict")
        self.assertEqual(self.service.get_project(self.project)["revision"], 3)
        # 历史 resume op2 重放 → 幂等，不清当前暂停（op3）
        replay_resume = self.service.resume(
            self.project, session_id=self.sid, operation_id="op2",
            reason="resume-2", expected_revision=3)
        self.assertTrue(replay_resume["executed"])
        self.assertFalse(replay_resume["applied"])
        self.assertEqual(self.service.get_project(self.project)["status"],
                         "paused")

    def test_operation_index_survives_restart(self):
        self.pause_ok(operation="op-p", reason="pause")
        conn2 = init(self.home)
        self.addCleanup(conn2.close)
        clock2 = FakeClock(wall_ms=self.clock.now_ms() + 5000)
        svc2 = CoordinatorControlService(
            CoordinatorControlLedger(conn2, clock2), clock2)
        # 重启后同 op 重试（old expected_revision）→ 幂等（跨重启反例）
        retry = svc2.pause(self.project, session_id=self.sid,
                           operation_id="op-p", reason="pause",
                           expected_revision=0)
        self.assertTrue(retry["executed"])
        self.assertFalse(retry["applied"])
        self.assertEqual(retry["reason"], "already-applied")
        self.assertEqual(svc2.get_project(self.project)["revision"], 1)
        # 重启后同 op 改参 → conflict
        conflict = svc2.pause(self.project, session_id=self.sid,
                              operation_id="op-p", reason="other",
                              expected_revision=1)
        self.assertFalse(conflict["executed"])
        self.assertEqual(conflict["refused"], "operation-conflict")

    def test_c06_stale_recovery_rejected_new_pause_preserved(self):
        self.pause_ok(operation="op-pause", reason="user-pause")
        # 旧恢复读取的是暂停前的版本（expected_revision=0），暂停已提交 → 拒绝
        stale = self.service.resume(
            self.project, session_id=self.sid, operation_id="op-resume",
            reason="old-resume", expected_revision=0)
        self.assertFalse(stale["executed"])
        self.assertEqual(stale["refused"], "revision-changed")
        snap = self.service.get_project(self.project)
        self.assertEqual(snap["status"], "paused")
        self.assertEqual(snap["revision"], 1)
        # 新暂停后（已 paused）再以旧 expected_revision 暂停 → 精确拒绝，不清状态
        double = self.service.pause(
            self.project, session_id=self.sid, operation_id="op-pause-2",
            reason="later", expected_revision=0)
        self.assertFalse(double["executed"])
        self.assertEqual(double["refused"], "already-paused")
        snap2 = self.service.get_project(self.project)
        self.assertEqual(snap2["revision"], 1)
        self.assertEqual(snap2["operation_id"], "op-pause")

    def test_invalid_params_fail_closed(self):
        bad_sessions = [
            ("invalid-project", dict(project="", session_id=self.sid)),
            ("invalid-session", dict(project=self.project, session_id="")),
            ("invalid-operation-id", dict(project=self.project,
                                          session_id=self.sid,
                                          operation_id="  ")),
            ("invalid-reason", dict(project=self.project, session_id=self.sid,
                                    operation_id="op", reason="")),
        ]
        for code, kw in bad_sessions:
            kw.setdefault("operation_id", "op")
            kw.setdefault("reason", "r")
            kw.setdefault("expected_revision", 0)
            result = self.service.pause(
                kw["project"], session_id=kw["session_id"],
                operation_id=kw["operation_id"], reason=kw["reason"],
                expected_revision=kw["expected_revision"])
            self.assertFalse(result["executed"], code)
            self.assertEqual(result["refused"], code)
        neg = self.service.pause(
            self.project, session_id=self.sid, operation_id="op",
            reason="r", expected_revision=-1)
        self.assertEqual(neg["refused"], "invalid-revision")
        bool_rev = self.service.pause(
            self.project, session_id=self.sid, operation_id="op",
            reason="r", expected_revision=True)
        self.assertEqual(bool_rev["refused"], "invalid-revision")
        bad_ev = self.service.pause(
            self.project, session_id=self.sid, operation_id="op",
            reason="r", expected_revision=0, evidence="not-a-dict")
        self.assertEqual(bad_ev["refused"], "invalid-evidence")
        # 拒绝不写任何记录
        self.assertIsNone(meta_get(self.conn, PROJECT_KEY_PREFIX + self.project))

    def test_lease_missing_refused(self):
        other = "/tmp/opencode/coord-no-lease"
        result = self.service.pause(
            other, session_id=self.sid, operation_id="op", reason="r",
            expected_revision=0)
        self.assertFalse(result["executed"])
        self.assertEqual(result["refused"], LEASE_MISSING)
        snap = result["snapshot"]
        self.assertEqual(snap["revision"], 0)
        self.assertEqual(snap["status"], "running")
        self.assertIsNone(result["epoch"])

    def test_c08_non_holder_and_epoch_guard(self):
        other_sid = "ses_control_other"
        result = self.service.pause(
            self.project, session_id=other_sid, operation_id="op",
            reason="r", expected_revision=0)
        self.assertFalse(result["executed"])
        self.assertEqual(result["refused"], LEASE_HOLDER_MISMATCH)

        # 交接：new holder 接管（epoch 2），old holder 晚到操作被拒
        self.ledger.claim_coordinator(
            project=self.project, session_id=other_sid, actor="test",
            takeover=True, old_confirmed_stopped=True)
        old_attempt = self.service.pause(
            self.project, session_id=self.sid, operation_id="op-old",
            reason="late-pause", expected_revision=0, expected_epoch=1)
        self.assertFalse(old_attempt["executed"])
        self.assertEqual(old_attempt["refused"], LEASE_HOLDER_MISMATCH)
        wrong_epoch = self.service.pause(
            self.project, session_id=other_sid, operation_id="op-new",
            reason="r", expected_revision=0, expected_epoch=1)
        self.assertEqual(wrong_epoch["refused"], EPOCH_MISMATCH)
        invalid_epoch = self.service.pause(
            self.project, session_id=other_sid, operation_id="op-new",
            reason="r", expected_revision=0, expected_epoch=0)
        self.assertEqual(invalid_epoch["refused"], INVALID_EPOCH)
        # 新 holder 用当前 epoch 成功
        ok = self.service.pause(
            self.project, session_id=other_sid, operation_id="op-new",
            reason="r", expected_revision=0, expected_epoch=2)
        self.assertTrue(ok["executed"])
        self.assertEqual(ok["snapshot"]["epoch"], 2)
        self.assertEqual(ok["epoch"], 2)

    def test_lease_expired_refused(self):
        """租约过期：晚到控制操作拒绝，直到重新认领/续约（C07/C08）。"""
        self.pause_ok(operation="op-p", reason="pause")
        self.conn.execute(
            "UPDATE coordinator_leases SET expires_at=? WHERE project=?",
            (self.clock.now_ms() - 1000, self.project))
        self.conn.commit()
        result = self.service.pause(
            self.project, session_id=self.sid, operation_id="op-p2",
            reason="pause", expected_revision=1)
        self.assertFalse(result["executed"])
        self.assertEqual(result["refused"], LEASE_EXPIRED)
        resume = self.service.resume(
            self.project, session_id=self.sid, operation_id="op-r",
            reason="resume", expected_revision=1)
        self.assertFalse(resume["executed"])
        self.assertEqual(resume["refused"], LEASE_EXPIRED)
        # 只读照常；状态未被迟到的过期操作改动
        self.assertEqual(self.service.get_project(self.project)["status"],
                         "paused")

    def test_lease_recovery_blocked_refused(self):
        """恢复被阻断/协调者停用：控制操作拒绝，直到显式解阻（C07/C08）。"""
        self.conn.execute(
            "UPDATE coordinator_leases SET recovery_blocked=1, expires_at=? "
            "WHERE project=?",
            (self.clock.now_ms() + 300 * 1000, self.project))
        self.conn.commit()
        result = self.service.pause(
            self.project, session_id=self.sid, operation_id="op-p",
            reason="pause", expected_revision=0)
        self.assertFalse(result["executed"])
        self.assertEqual(result["refused"], LEASE_RECOVERY_BLOCKED)

    def test_c05_resume_keeps_provider_gate_and_session_dimensions(self):
        # provider 冷却门（target-gate 命名空间，独立维度）
        gate = TargetGate(self.conn, self.clock)
        gate.observe_error(
            self.sid, {"category": "quota", "status": 429, "message": "limit"},
            evidence_key="coord-control:e1", observed_at=self.clock.now_ms())
        # session 维度：hold（先）与继续后的 barrier（后）
        self.service.hold_session(
            self.sid, event_id="ev-stop", origin_at=self.clock.now_ms(),
            reason="user-interrupted")
        self.service.begin_user_turn(
            self.sid, message_id="msg-1", origin_at=self.clock.now_ms() + 1,
            operation_id="op-1", trusted=True)
        self.pause_ok()
        self.assertFalse(self.service.permits(self.project, self.sid)["permits"])
        resumed = self.service.resume(
            self.project, session_id=self.sid, operation_id="op-resume",
            reason="resume", expected_revision=1)
        self.assertTrue(resumed["executed"])
        # provider 门保持（未被恢复删除）
        self.assertIsNotNone(gate.get(self.sid))
        # session 维度保持（hold/barrier 未被恢复删除）
        sess = self.service.get_session(self.sid)
        self.assertIsNotNone(sess["hold"])
        self.assertIsNotNone(sess["barrier"])
        # 项目暂停维度已解除，但其他门继续拦
        permit = self.service.permits(self.project, self.sid)
        self.assertFalse(permit["permits"])
        self.assertIn("user-turn-active", permit["blocking"])
        self.assertIn("turn-held", permit["blocking"])
        self.assertNotIn("project-paused", permit["blocking"])

    def test_c07_restart_preserves_state_and_history_manual_not_cleared(self):
        self.pause_ok(operation="op-p", reason="pause")
        # target-gate 历史 manual 记录与项目控制无关：不受本组件影响
        gate = TargetGate(self.conn, self.clock)
        gate.block_manual(self.sid, reason="historical-manual")
        conn2 = init(self.home)
        self.addCleanup(conn2.close)
        clock2 = FakeClock(wall_ms=self.clock.now_ms() + 5000)
        svc2 = CoordinatorControlService(
            CoordinatorControlLedger(conn2, clock2), clock2)
        snap2 = svc2.get_project(self.project)
        self.assertEqual(snap2["status"], "paused")
        self.assertEqual(snap2["revision"], 1)
        self.assertEqual(snap2["operation_id"], "op-p")
        # 历史 manual 记录仍在 target-gate 命名空间（重启默认不自动解锁）
        self.assertEqual(gate.get(self.sid)["state"], "manual-blocked")
        # 重启后合法 holder 可以基于当前版本继续
        resumed = svc2.resume(
            self.project, session_id=self.sid, operation_id="op-r",
            reason="resume-after-restart", expected_revision=1)
        self.assertTrue(resumed["executed"])

    def test_get_project_default_revision_zero(self):
        snap = self.service.get_project("/tmp/opencode/coord-unknown")
        self.assertEqual(snap["revision"], 0)
        self.assertEqual(snap["status"], "running")
        self.assertEqual(snap["recorded"], False)
        self.assertIsNone(snap["session_id"])
        self.assertIsNone(snap["operation_id"])
        self.assertIsNone(snap["holder_session"])
        snap2 = self.service.get_project(self.project)
        self.assertEqual(snap2["revision"], 0)
        self.assertEqual(snap2["holder_session"], self.sid)
        self.assertEqual(snap2["epoch"], 1)

    def test_session_projects_are_holder_projects(self):
        """单 session 多项目：get_session.projects 返回持有租约的**项目**。"""
        p2 = "/tmp/opencode/coord-control-project-2"
        self.ledger.claim_coordinator(project=p2, session_id=self.sid,
                                      actor="test")
        snap = self.service.get_session(self.sid)
        self.assertEqual(snap["projects"], sorted([self.project, p2]))
        self.assertNotIn(self.sid, snap["projects"], "projects 不得混入 session")
        # 无关 session 的项目集合为空
        other = self.service.get_session("ses_unrelated")
        self.assertEqual(other["projects"], [])

    def test_permits_composes_project_pause(self):
        self.assertTrue(self.service.permits(self.project, self.sid)["permits"])
        self.pause_ok()
        view = self.service.permits(self.project, self.sid)
        self.assertFalse(view["permits"])
        self.assertIn("project-paused", view["blocking"])
        self.assertEqual(view["project"], self.project)
        self.assertEqual(view["session_id"], self.sid)
        self.assertEqual(view["project_snapshot"]["status"], "paused")


class FailClosedPersistenceTests(ControlBase):
    def _corrupt(self, key, value):
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value))
        self.conn.commit()

    def test_corrupted_project_record_raises_fail_closed(self):
        self.pause_ok()
        key = PROJECT_KEY_PREFIX + self.project
        self._corrupt(key, '{"schema":1,"project":"' + self.project +
                      '","broken":1}')
        with self.assertRaises(CoordinatorControlStateError):
            self.service.get_project(self.project)
        with self.assertRaises(CoordinatorControlStateError):
            self.service.resume(
                self.project, session_id=self.sid, operation_id="op",
                reason="r", expected_revision=1)
        with self.assertRaises(CoordinatorControlStateError):
            self.service.permits(self.project, self.sid)

    def test_corrupted_json_raises(self):
        key = PROJECT_KEY_PREFIX + self.project
        self._corrupt(key, "{not-json")
        with self.assertRaises(CoordinatorControlStateError):
            self.service.get_project(self.project)
        with self.assertRaises(CoordinatorControlStateError):
            self.service.pause(
                self.project, session_id=self.sid, operation_id="op",
                reason="r", expected_revision=0)

    def test_corrupted_operation_index_fail_closed(self):
        self.pause_ok()
        self._corrupt(OPERATION_KEY_PREFIX + "pause-op", "not-json")
        with self.assertRaises(CoordinatorControlStateError):
            self.service.pause(
                self.project, session_id=self.sid, operation_id="pause-op",
                reason="user-requested", expected_revision=1)

    def test_future_schema_rejected(self):
        self._corrupt(SCHEMA_KEY, "99")
        with self.assertRaises(CoordinatorControlStateError):
            self.service.get_project(self.project)


class BackupRestoreTests(ControlBase):
    def test_consistent_backup_and_restore_preserves_control(self):
        self.pause_ok(operation="op-p", reason="pause",
                      evidence={"note": "backup-check"})
        self.service.hold_session(
            self.sid, event_id="ev-1", origin_at=self.clock.now_ms(),
            reason="interrupted")
        self.service.begin_user_turn(
            self.sid, message_id="msg-1",
            origin_at=self.clock.now_ms() + 1, operation_id="op-turn",
            trusted=True)
        dest = os.path.join(self.home, "control-backup.db")
        backup_to(self.conn, dest)
        info = inspect_backup(dest)
        self.assertTrue(info["ok"])
        self.assertEqual(info["schema_version"], "3")
        self.assertGreater(info["tables"].get("audit", 0), 0)

        # 恢复：把一致副本放回新 home 的 ledger.db，用同版入口读取
        home2 = tempfile.mkdtemp(prefix="coord-restore-", dir="/tmp/opencode")
        self.addCleanup(shutil.rmtree, home2)
        shutil.copyfile(dest, os.path.join(home2, "ledger.db"))
        conn2 = init(home2)
        self.addCleanup(conn2.close)
        clock2 = FakeClock(wall_ms=self.clock.now_ms() + 1000)
        svc2 = CoordinatorControlService(
            CoordinatorControlLedger(conn2, clock2), clock2)
        proj = svc2.get_project(self.project)
        self.assertEqual(proj["status"], "paused")
        self.assertEqual(proj["revision"], 1)
        session = svc2.get_session(self.sid)
        self.assertIsNotNone(session["hold"])
        self.assertIsNotNone(session["barrier"])
        # 审计与 operation 索引随备份保持
        rows = conn2.execute(
            "SELECT COUNT(*) AS n FROM audit WHERE action="
            "'coordinator-control.pause'").fetchone()["n"]
        self.assertEqual(rows, 1)
        self.assertIsNotNone(meta_get(conn2, OPERATION_KEY_PREFIX + "op-p"))
        # 恢复副本可正常继续 CAS；历史 op 重放（同参数同证据）幂等
        replay = svc2.pause(self.project, session_id=self.sid,
                            operation_id="op-p", reason="pause",
                            expected_revision=0,
                            evidence={"note": "backup-check"})
        self.assertTrue(replay["executed"])
        self.assertFalse(replay["applied"])
        # 历史 op 重放但参数/证据变更 → 不得冒认重复
        changed = svc2.pause(self.project, session_id=self.sid,
                             operation_id="op-p", reason="pause",
                             expected_revision=0)
        self.assertFalse(changed["executed"])
        self.assertEqual(changed["refused"], "operation-conflict")
        ok = svc2.resume(
            self.project, session_id=self.sid, operation_id="op-r",
            reason="restored-resume", expected_revision=1)
        self.assertTrue(ok["executed"])


class DeterministicInterleaveTests(ControlBase):
    """真 WAL 两连接 + 线程屏障的确定性交错（C06/C08 反例）。"""

    def test_two_connections_single_pause_wins_retry_idempotent(self):
        conn_a = _open_wal(self.home)
        conn_b = _open_wal(self.home)
        self.addCleanup(conn_a.close)
        self.addCleanup(conn_b.close)
        ledger_a = CoordinatorControlLedger(conn_a, self.clock)
        ledger_b = CoordinatorControlLedger(conn_b, self.clock)
        gate = threading.Barrier(2)
        results = {}

        def worker(who, led, store):
            gate.wait()
            store[who] = led.pause(
                self.project, session_id=self.sid, operation_id="op-X",
                reason="user-pause", expected_revision=0)

        t1 = threading.Thread(target=worker, args=("a", ledger_a, results))
        t2 = threading.Thread(target=worker, args=("b", ledger_b, results))
        t1.start()
        t2.start()
        t1.join(timeout=15)
        t2.join(timeout=15)
        # 同一 op-X/同参数并发：恰好一个 winner，另一个幂等（不重复改写）
        winners = [r for r in results.values()
                   if r["executed"] and r["applied"]]
        idems = [r for r in results.values()
                 if r["executed"] and not r["applied"]
                 and r["reason"] == "already-applied"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(idems), 1)
        snap = self.service.get_project(self.project)
        self.assertEqual(snap["status"], "paused")
        self.assertEqual(snap["revision"], 1)

        # 另一连接用“当前 revision + 同一 operation_id/参数”重试 → 幂等成功
        retry = ledger_b.pause(
            self.project, session_id=self.sid, operation_id="op-X",
            reason="user-pause", expected_revision=1)
        self.assertTrue(retry["executed"])
        self.assertFalse(retry["applied"])
        self.assertEqual(retry["reason"], "already-applied")
        self.assertEqual(retry["revision"], 1)
        self.assertEqual(self.service.get_project(self.project)["revision"], 1)

    def test_old_resume_cannot_clear_new_pause(self):
        planned_revision = self.service.get_project(self.project)["revision"]
        self.assertEqual(planned_revision, 0)
        # 新暂停先提交
        self.pause_ok(operation="op-p", reason="pause")
        # 旧恢复（计划时 revision=0）提交 → 被 CAS 拒绝，新暂停保持
        old_resume = self.service.resume(
            self.project, session_id=self.sid, operation_id="op-r-old",
            reason="old-resume", expected_revision=planned_revision)
        self.assertFalse(old_resume["executed"])
        self.assertEqual(old_resume["refused"], "revision-changed")
        snap = self.service.get_project(self.project)
        self.assertEqual(snap["status"], "paused")
        self.assertEqual(snap["revision"], 1)
        self.assertEqual(snap["operation_id"], "op-p")

    def test_epoch_changes_between_read_and_commit(self):
        # 计划时 epoch=1，提交前交接 → epoch=2，expected_epoch=1 被拒
        self.service.pause(
            self.project, session_id=self.sid, operation_id="op-p",
            reason="pause", expected_revision=0, expected_epoch=1)
        self.ledger.claim_coordinator(
            project=self.project, session_id="ses_control_other",
            actor="test", takeover=True, old_confirmed_stopped=True)
        stale = self.service.pause(
            self.project, session_id="ses_control_other", operation_id="op-p2",
            reason="pause", expected_revision=1, expected_epoch=1)
        self.assertFalse(stale["executed"])
        self.assertEqual(stale["refused"], EPOCH_MISMATCH)
        # 当前 epoch + 当前修订：新 holder 可正常继续（恢复不替代验收）
        ok = self.service.resume(
            self.project, session_id="ses_control_other", operation_id="op-p3",
            reason="resume", expected_revision=1, expected_epoch=2)
        self.assertTrue(ok["executed"])
        self.assertEqual(ok["snapshot"]["status"], "running")
        self.assertEqual(ok["snapshot"]["epoch"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
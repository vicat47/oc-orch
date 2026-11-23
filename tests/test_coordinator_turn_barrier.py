"""对话控制底座：session 回合 hold 与用户回合屏障（C09–C12）与确定性交错。

覆盖（openspec improve-coordinator-continuity / specs/coordinator-continuity）：

- C09 普通续聊后再恢复自动协调：可信输入建屏障 → 期间 permits 阻塞 → 精确
  结束证据才解除；输入入队/投递本身不触发恢复；
- C10 停止当前回答后等待续聊：hold 不自清（时间流逝/auto-wake 不解除），
  新用户回合结束后才解除对应 hold；
- C11 连续输入和再次中断：屏障跟随最新有效输入，旧 idle/旧输入结束/重复回执
  拒绝（message-mismatch / stale-termination / revision-changed；
  interrupted-turn 的回合自身结束不解除）；
- C12 事件漏收重启与证据不足：缺归属证据 fail-closed；损坏记录抛错；
  重启后屏障/hold 保持；无证据完成不可用。

信任边界：``begin_user_turn`` 的 trusted 必须严格 ``is True``（fail-closed）；
未登记 REST 消息、metadata、自动 wake 与任意 truthy 值（1/"yes"）绝不自动视为
真人（untrusted 拒绝，不写记录）。session 水位（watermark）单调推进：finish
后迟到旧中断不能重建 hold，旧 idle 不能清更新屏障。
"""

from __future__ import annotations

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
    SESSION_KEY_PREFIX,
    CoordinatorControlLedger,
)
from oc_orch.adapters.sqlite_db import backup_to, init, inspect_backup, meta_get
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


class BarrierBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="coord-barrier-",
                                               dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.sid = "ses_turn_holder"
        self.control = CoordinatorControlLedger(self.conn, self.clock)
        self.service = CoordinatorControlService(self.control, self.clock)

    def begin(self, message_id, origin_at=None, *, operation_id="op-turn",
              trusted=True):
        if origin_at is None:
            origin_at = self.clock.now_ms()
        return self.service.begin_user_turn(
            self.sid, message_id=message_id, origin_at=origin_at,
            operation_id=operation_id, trusted=trusted)

    def finish(self, message_id, idle_id, idle_at, expected_revision):
        return self.service.finish_user_turn(
            self.sid, message_id=message_id, idle_id=idle_id,
            idle_at=idle_at, expected_revision=expected_revision)

    def audit_rows(self, action):
        return self.conn.execute(
            "SELECT action, from_state, to_state, detail FROM audit "
            "WHERE entity='session' AND entity_id=? AND action=? ORDER BY id",
            (self.sid, action)).fetchall()


class TurnBarrierTests(BarrierBase):
    def test_c09_trusted_begin_blocks_then_exact_finish_restores(self):
        self.assertTrue(self.service.permits("/tmp/none-paused", self.sid)[
            "permits"])
        at = self.clock.now_ms()
        begun = self.begin("msg-1", at)
        self.assertTrue(begun["executed"])
        self.assertTrue(begun["applied"])
        self.assertEqual(begun["reason"], "barrier-set")
        snap = begun["snapshot"]
        self.assertEqual(snap["barrier"]["message_id"], "msg-1")
        self.assertEqual(snap["barrier"]["origin_at"], at)
        self.assertTrue(snap["barrier"]["trusted"])
        self.assertEqual(snap["watermark"], at)
        self.assertEqual(self.service.get_session(self.sid), snap)
        # 回合进行中：自动动作被拦（输入入队/投递本身不触发恢复）
        view = self.service.permits("/tmp/none-paused", self.sid)
        self.assertFalse(view["permits"])
        self.assertIn("user-turn-active", view["blocking"])
        # 精确结束证据（message + idle + revision）才可解除屏障
        finished = self.finish("msg-1", "idle-1", self.clock.now_ms() + 100,
                               expected_revision=1)
        self.assertTrue(finished["executed"])
        self.assertTrue(finished["applied"])
        self.assertEqual(finished["reason"], "finished")
        self.assertIsNone(finished["snapshot"]["barrier"])
        self.assertGreaterEqual(finished["snapshot"]["watermark"],
                                self.clock.now_ms() + 100)
        self.assertTrue(self.service.permits("/tmp/none-paused", self.sid)[
            "permits"])

    def test_untrusted_begin_never_treated_as_human(self):
        # 未登记 REST / metadata / 自动 wake 一律不可信（fail-closed）
        for bogus in (False, 1, "yes", "", None):
            with self.subTest(bogus=bogus):
                rejected = self.begin("msg-auto-wake", trusted=bogus)
                self.assertFalse(rejected["executed"])
                self.assertEqual(rejected["refused"], "untrusted")
                self.assertEqual(rejected["revision"], 0)
        self.assertIsNone(meta_get(self.conn, SESSION_KEY_PREFIX + self.sid))
        self.assertTrue(self.service.permits("/tmp/none-paused", self.sid)[
            "permits"])

    def test_c10_hold_persists_until_new_turn_finishes(self):
        t_stop = self.clock.now_ms()
        held = self.service.hold_session(
            self.sid, event_id="ev-stop", origin_at=t_stop,
            reason="user-interrupted")
        self.assertTrue(held["executed"])
        self.assertEqual(held["reason"], "held")
        self.assertEqual(held["snapshot"]["watermark"], t_stop)
        view = self.service.permits("/tmp/none-paused", self.sid)
        self.assertFalse(view["permits"])
        self.assertIn("turn-held", view["blocking"])
        # 时间流逝 / 自动 wake（无任何可写动作可自清）不解除 hold
        self.clock.advance(seconds=3600)
        snap = self.service.get_session(self.sid)
        self.assertIsNotNone(snap["hold"])
        self.assertFalse(self.service.permits(
            "/tmp/none-paused", self.sid)["permits"])
        # 用户续聊（新输入，origin 在 hold 之后）：hold 保持，屏障建立
        t_msg = self.clock.now_ms()
        self.begin("msg-after-hold", t_msg)
        snap2 = self.service.get_session(self.sid)
        self.assertIsNotNone(snap2["hold"])
        self.assertIsNotNone(snap2["barrier"])
        # 新用户回合结束 → 解除对应 hold + 屏障（C10）
        finished = self.finish("msg-after-hold", "idle-x",
                               self.clock.now_ms() + 100,
                               expected_revision=2)
        self.assertTrue(finished["executed"])
        self.assertEqual(finished["reason"], "finished-with-hold-release")
        snap3 = self.service.get_session(self.sid)
        self.assertIsNone(snap3["hold"])
        self.assertIsNone(snap3["barrier"])
        self.assertTrue(self.service.permits("/tmp/none-paused", self.sid)[
            "permits"])

    def test_finish_then_late_interrupt_cannot_rebuild_hold(self):
        """水位反例：finish 成功后，迟到的旧中断（<= 水位）不能重建 hold。"""
        t0 = self.clock.now_ms()
        self.service.hold_session(self.sid, event_id="ev-stop",
                                  origin_at=t0, reason="user-interrupted")
        self.begin("msg-1", t0 + 1)
        rev = self.service.get_session(self.sid)["revision"]
        done = self.finish("msg-1", "idle-1", t0 + 2, rev)
        self.assertTrue(done["executed"])
        self.assertEqual(done["reason"], "finished-with-hold-release")
        wm = self.service.get_session(self.sid)["watermark"]
        self.assertEqual(wm, t0 + 2)
        # 迟到的旧中断（origin 早于/等于 finish 水位）→ stale，不能重建 hold
        old = self.service.hold_session(self.sid, event_id="ev-old",
                                        origin_at=t0, reason="late")
        self.assertFalse(old["executed"])
        self.assertEqual(old["refused"], "stale-event")
        older = self.service.hold_session(self.sid, event_id="ev-older",
                                          origin_at=t0 + 1, reason="later")
        self.assertFalse(older["executed"])
        self.assertEqual(older["refused"], "stale-event")
        self.assertIsNone(self.service.get_session(self.sid)["hold"])
        # 真正的新中断（origin > 水位）→ 可建立 hold
        newer = self.service.hold_session(self.sid, event_id="ev-still-old",
                                          origin_at=t0 + 1 + 1,
                                          reason="late")
        self.assertFalse(newer["executed"], "t0+2 不严新于水位 t0+2 → stale")
        fresh = self.service.hold_session(self.sid, event_id="ev-new",
                                          origin_at=t0 + 3, reason="new")
        self.assertTrue(fresh["executed"])
        self.assertTrue(fresh["applied"])
        self.assertEqual(fresh["snapshot"]["hold"]["event_id"], "ev-new")

    def test_c11_new_input_supersedes_old_and_rejects_old_termination(self):
        t0 = self.clock.now_ms()
        self.begin("msg-1", t0)
        rev1 = self.service.get_session(self.sid)["revision"]
        self.assertEqual(rev1, 1)
        # 更新输入：屏障跟随最新（C11）
        t1 = t0 + 10
        newer = self.begin("msg-2", t1)
        self.assertTrue(newer["applied"])
        rev2 = self.service.get_session(self.sid)["revision"]
        self.assertEqual(rev2, 2)
        # 旧输入结束：message 不归属当前屏障 → 拒绝
        stale_msg = self.finish("msg-1", "idle-1", t1 + 5,
                                expected_revision=rev2)
        self.assertFalse(stale_msg["executed"])
        self.assertEqual(stale_msg["refused"], "message-mismatch")
        # 旧 idle：idle_at 不新于当前 barrier/水位 → stale-termination
        stale_idle = self.finish("msg-2", "idle-old", t0 + 1,
                                 expected_revision=rev2)
        self.assertFalse(stale_idle["executed"])
        self.assertEqual(stale_idle["refused"], "stale-termination")
        # 旧 expected_revision → revision-changed
        stale_rev = self.finish("msg-2", "idle-2", t1 + 100,
                                expected_revision=rev1)
        self.assertFalse(stale_rev["executed"])
        self.assertEqual(stale_rev["refused"], "revision-changed")
        # 屏障未被动摇：仍指向 msg-2
        snap = self.service.get_session(self.sid)
        self.assertEqual(snap["barrier"]["message_id"], "msg-2")
        self.assertEqual(snap["revision"], 2)
        # 重复回执：同一结束证据第二次（revision 已推进）→ revision-changed
        done = self.finish("msg-2", "idle-2", t1 + 100,
                           expected_revision=rev2)
        self.assertTrue(done["executed"])
        dup = self.finish("msg-2", "idle-2", t1 + 100,
                          expected_revision=rev2)
        self.assertFalse(dup["executed"])
        self.assertEqual(dup["refused"], "revision-changed")

    def test_c11_interrupted_turn_own_finish_does_not_release(self):
        t0 = self.clock.now_ms()
        self.begin("msg-1", t0)
        # 处理期间再次中断：hold 更新于 barrier 之后
        t_hold = t0 + 5
        held = self.service.hold_session(
            self.sid, event_id="ev-stop-2", origin_at=t_hold,
            reason="user-interrupted")
        self.assertTrue(held["applied"])
        rev2 = self.service.get_session(self.sid)["revision"]
        # 被中断回合自身的结束证据（即使晚于中断）也不能解除 hold/屏障
        rejected = self.finish("msg-1", "idle-interrupted",
                               t_hold + 100, expected_revision=rev2)
        self.assertFalse(rejected["executed"])
        self.assertEqual(rejected["refused"], "interrupted-turn")
        snap = self.service.get_session(self.sid)
        self.assertIsNotNone(snap["hold"])
        self.assertIsNotNone(snap["barrier"])
        self.assertEqual(snap["revision"], rev2, "拒绝不改动状态")
        # 用户再次输入 → 屏障跟随；其结束才解除 hold + 屏障
        t3 = t_hold + 10
        self.begin("msg-3", t3)
        rev3 = self.service.get_session(self.sid)["revision"]
        done = self.finish("msg-3", "idle-3", t3 + 100,
                           expected_revision=rev3)
        self.assertTrue(done["executed"])
        self.assertEqual(done["reason"], "finished-with-hold-release")
        snap2 = self.service.get_session(self.sid)
        self.assertIsNone(snap2["hold"])
        self.assertIsNone(snap2["barrier"])

    def test_stale_hold_event_rejected(self):
        t0 = self.clock.now_ms()
        self.begin("msg-1", t0)
        self.service.hold_session(
            self.sid, event_id="ev-1", origin_at=t0 + 5,
            reason="user-interrupted")
        # 迟到的旧中断事件（早于当前 hold/barrier 水位）→ 拒绝
        stale1 = self.service.hold_session(
            self.sid, event_id="ev-old", origin_at=t0 + 1,
            reason="late-interrupt")
        self.assertFalse(stale1["executed"])
        self.assertEqual(stale1["refused"], "stale-event")
        # 同刻不同 event → 归属不清拒绝
        stale2 = self.service.hold_session(
            self.sid, event_id="ev-twin", origin_at=t0 + 5,
            reason="twin")
        self.assertFalse(stale2["executed"])
        self.assertEqual(stale2["refused"], "stale-event")
        # 同刻同 event → 幂等
        again = self.service.hold_session(
            self.sid, event_id="ev-1", origin_at=t0 + 5,
            reason="user-interrupted")
        self.assertTrue(again["executed"])
        self.assertFalse(again["applied"])
        self.assertEqual(again["reason"], "already-applied")
        # 更新事件（更晚 origin）→ 替换
        newer = self.service.hold_session(
            self.sid, event_id="ev-2", origin_at=t0 + 8,
            reason="user-interrupted")
        self.assertTrue(newer["applied"])
        self.assertEqual(newer["snapshot"]["hold"]["event_id"], "ev-2")

    def test_begin_turn_idempotent_for_same_message_and_origin(self):
        at = self.clock.now_ms()
        first = self.begin("msg-1", at)
        self.assertTrue(first["applied"])
        again = self.begin("msg-1", at)
        self.assertTrue(again["executed"])
        self.assertFalse(again["applied"])
        self.assertEqual(again["reason"], "already-applied")
        self.assertEqual(again["revision"], 1)
        # 同刻不同消息 → 归属不清拒绝
        twin = self.begin("msg-1b", at)
        self.assertFalse(twin["executed"])
        self.assertEqual(twin["refused"], "stale-event")
        # 迟到输入（早于当前 barrier/水位）→ stale-event
        late = self.begin("msg-old", at - 5)
        self.assertFalse(late["executed"])
        self.assertEqual(late["refused"], "stale-event")

    def test_independent_dimensions_not_mutually_cleared(self):
        # hold 不影响 barrier；begin 不清 hold
        t0 = self.clock.now_ms()
        self.service.hold_session(self.sid, event_id="ev-h",
                                  origin_at=t0, reason="interrupted")
        self.begin("msg-1", t0 + 1)
        snap = self.service.get_session(self.sid)
        self.assertIsNotNone(snap["hold"])
        self.assertIsNotNone(snap["barrier"])
        # 再次 hold 更新只改 hold，barrier 保留
        self.service.hold_session(self.sid, event_id="ev-h2",
                                  origin_at=t0 + 2, reason="interrupted")
        snap2 = self.service.get_session(self.sid)
        self.assertIsNotNone(snap2["barrier"])
        self.assertEqual(snap2["hold"]["event_id"], "ev-h2")

    def test_c12_missing_evidence_fail_closed(self):
        at = self.clock.now_ms()
        self.begin("msg-1", at)
        rev = self.service.get_session(self.sid)["revision"]
        # 空 inbox / session.updated 不是完成依据：缺 idle 证据 → 拒绝
        missing_idle = self.finish("msg-1", "", at + 10, rev)
        self.assertFalse(missing_idle["executed"])
        self.assertEqual(missing_idle["refused"], "invalid-idle-id")
        missing_time = self.finish("msg-1", "idle-1", -1, rev)
        self.assertFalse(missing_time["executed"])
        self.assertEqual(missing_time["refused"], "invalid-idle-at")
        self.assertIsNotNone(self.service.get_session(self.sid)["barrier"])
        # 缺 message（原输入）→ message-mismatch（空输入不能归属）
        missing_msg = self.finish("", "idle-1", at + 10, rev)
        self.assertFalse(missing_msg["executed"])
        self.assertEqual(missing_msg["refused"], "message-mismatch")
        # 精确结束证据生效（revision 推进）
        done = self.finish("msg-1", "idle-1", at + 10, rev)
        self.assertTrue(done["executed"])
        # 无活动回合：finish → no-active-turn（不能凭空关闭）
        rev_now = self.service.get_session(self.sid)["revision"]
        none = self.finish("msg-1", "idle-1", at + 20, rev_now)
        self.assertFalse(none["executed"])
        self.assertEqual(none["refused"], "no-active-turn")

    def test_schema_and_audit_records(self):
        at = self.clock.now_ms()
        # hold 先于新输入（hold.origin < barrier.origin）：结束后解除对应 hold
        self.service.hold_session(self.sid, event_id="ev-1",
                                  origin_at=at, reason="interrupted")
        # 同事件幂等：不改状态、不新增审计
        again = self.service.hold_session(self.sid, event_id="ev-1",
                                          origin_at=at,
                                          reason="interrupted")
        self.assertTrue(again["executed"])
        self.assertFalse(again["applied"])
        self.begin("msg-1", at + 1)
        rev = self.service.get_session(self.sid)["revision"]
        self.finish("msg-1", "idle-1", at + 10, rev)
        for action in ("begin-user-turn", "hold-session", "finish-user-turn"):
            rows = self.audit_rows(f"coordinator-control.{action}")
            self.assertEqual(len(rows), 1, action)
        rows = self.audit_rows("coordinator-control.hold-session")
        self.assertEqual(len(rows), 1, "幂等 hold 不重复审计")
        # 水位推进并持久化（finish 后迟到旧中断不能重建 hold 的依据）
        self.assertEqual(self.service.get_session(self.sid)["watermark"],
                         at + 10)


class BarrierFailClosedTests(BarrierBase):
    def _corrupt(self, key, value):
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value))
        self.conn.commit()

    def test_corrupted_session_record_fail_closed(self):
        self.begin("msg-1", self.clock.now_ms())
        key = SESSION_KEY_PREFIX + self.sid
        self._corrupt(key, '{"schema":1,"session_id":"' + self.sid +
                      '","broken":true}')
        for method in ("get_session", "hold_session", "begin_user_turn",
                       "finish_user_turn"):
            with self.assertRaises(CoordinatorControlStateError):
                if method == "get_session":
                    self.service.get_session(self.sid)
                elif method == "hold_session":
                    self.service.hold_session(
                        self.sid, event_id="ev", origin_at=1, reason="r")
                elif method == "begin_user_turn":
                    self.service.begin_user_turn(
                        self.sid, message_id="m", origin_at=1,
                        operation_id="op", trusted=True)
                else:
                    self.service.finish_user_turn(
                        self.sid, message_id="m", idle_id="i", idle_at=2,
                        expected_revision=0)

    def test_c12_restart_preserves_barrier_and_hold(self):
        at = self.clock.now_ms()
        # C10 顺序：先 hold，后新输入，二者共存并可在结束后解除
        self.service.hold_session(self.sid, event_id="ev-1",
                                  origin_at=at, reason="interrupted")
        self.begin("msg-1", at + 1)
        conn2 = init(self.home)
        self.addCleanup(conn2.close)
        clock2 = FakeClock(wall_ms=self.clock.now_ms() + 1000)
        svc2 = CoordinatorControlService(
            CoordinatorControlLedger(conn2, clock2), clock2)
        snap = svc2.get_session(self.sid)
        self.assertEqual(snap["barrier"]["message_id"], "msg-1")
        self.assertIsNotNone(snap["hold"])
        # 重启后无自动放行
        self.assertFalse(svc2.permits("/tmp/none-paused", self.sid)["permits"])
        # 重启后可以正常结束（精确证据）
        rev = svc2.get_session(self.sid)["revision"]
        done = svc2.finish_user_turn(
            self.sid, message_id="msg-1", idle_id="idle-1",
            idle_at=at + 10, expected_revision=rev)
        self.assertTrue(done["executed"])
        self.assertEqual(done["reason"], "finished-with-hold-release")
        self.assertTrue(svc2.permits("/tmp/none-paused", self.sid)["permits"])


class BarrierBackupRestoreTests(BarrierBase):
    def test_consistent_backup_restore_keeps_barrier_state(self):
        at = self.clock.now_ms()
        self.service.hold_session(self.sid, event_id="ev-1",
                                  origin_at=at, reason="interrupted")
        self.begin("msg-1", at + 1)
        dest = os.path.join(self.home, "barrier-backup.db")
        backup_to(self.conn, dest)
        info = inspect_backup(dest)
        self.assertTrue(info["ok"])
        home2 = tempfile.mkdtemp(prefix="barrier-restore-", dir="/tmp/opencode")
        self.addCleanup(shutil.rmtree, home2)
        shutil.copyfile(dest, os.path.join(home2, "ledger.db"))
        conn2 = init(home2)
        self.addCleanup(conn2.close)
        clock2 = FakeClock(wall_ms=self.clock.now_ms() + 1000)
        svc2 = CoordinatorControlService(
            CoordinatorControlLedger(conn2, clock2), clock2)
        snap = svc2.get_session(self.sid)
        self.assertIsNotNone(snap["barrier"])
        self.assertIsNotNone(snap["hold"])
        rev = snap["revision"]
        done = svc2.finish_user_turn(
            self.sid, message_id="msg-1", idle_id="idle-1",
            idle_at=at + 5, expected_revision=rev)
        self.assertTrue(done["executed"])
        self.assertEqual(done["reason"], "finished-with-hold-release")


class BarrierInterleaveTests(BarrierBase):
    """真 WAL 两连接 + 线程屏障的确定性交错（C11/C12 反例）。"""

    def test_two_connections_same_origin_one_message_wins(self):
        conn_a = _open_wal(self.home)
        conn_b = _open_wal(self.home)
        self.addCleanup(conn_a.close)
        self.addCleanup(conn_b.close)
        ledger_a = CoordinatorControlLedger(conn_a, self.clock)
        ledger_b = CoordinatorControlLedger(conn_b, self.clock)
        gate = threading.Barrier(2)
        at = self.clock.now_ms()
        results = {}

        def worker(who, led, store, msg):
            gate.wait()
            store[who] = led.begin_user_turn(
                self.sid, message_id=msg, origin_at=at,
                operation_id=f"op-{who}", trusted=True)

        t1 = threading.Thread(target=worker,
                              args=("a", ledger_a, results, "msg-a"))
        t2 = threading.Thread(target=worker,
                              args=("b", ledger_b, results, "msg-b"))
        t1.start()
        t2.start()
        t1.join(timeout=15)
        t2.join(timeout=15)
        # 同刻两个不同输入 → 恰好一个建立屏障，另一个归属不清拒绝（C11）
        winners = [r for r in results.values()
                   if r["executed"] and r["applied"]]
        stale = [r for r in results.values()
                 if not r["executed"] and r["refused"] == "stale-event"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(stale), 1)
        snap = self.service.get_session(self.sid)
        self.assertEqual(snap["revision"], 1)
        self.assertIsNotNone(snap["barrier"])

    def test_cas_finish_after_cross_connection_new_input(self):
        at = self.clock.now_ms()
        self.begin("msg-1", at)
        rev1 = self.service.get_session(self.sid)["revision"]
        conn_b = _open_wal(self.home)
        self.addCleanup(conn_b.close)
        ledger_b = CoordinatorControlLedger(conn_b, self.clock)
        # 另一连接观察到 rev1 后，又来新输入 msg-2（rev→2）
        ledger_b.begin_user_turn(self.sid, message_id="msg-2",
                                 origin_at=at + 10,
                                 operation_id="op-2", trusted=True)
        # 旧连接的 finish（expected=rev1）→ CAS 拒绝；新屏障保持
        stale = self.finish("msg-1", "idle-1", at + 20, rev1)
        self.assertFalse(stale["executed"])
        self.assertEqual(stale["refused"], "revision-changed")
        snap = self.service.get_session(self.sid)
        self.assertEqual(snap["barrier"]["message_id"], "msg-2")
        self.assertEqual(snap["revision"], 2)

    def test_duplicate_finish_only_one_applies(self):
        at = self.clock.now_ms()
        self.begin("msg-1", at)
        rev = self.service.get_session(self.sid)["revision"]
        conn_a = _open_wal(self.home)
        conn_b = _open_wal(self.home)
        self.addCleanup(conn_a.close)
        self.addCleanup(conn_b.close)
        ledger_a = CoordinatorControlLedger(conn_a, self.clock)
        ledger_b = CoordinatorControlLedger(conn_b, self.clock)
        gate = threading.Barrier(2)
        results = {}

        def finisher(who, led, store):
            gate.wait()
            store[who] = led.finish_user_turn(
                self.sid, message_id="msg-1", idle_id="idle-1",
                idle_at=at + 10, expected_revision=rev)

        t1 = threading.Thread(target=finisher,
                              args=("a", ledger_a, results))
        t2 = threading.Thread(target=finisher,
                              args=("b", ledger_b, results))
        t1.start()
        t2.start()
        t1.join(timeout=15)
        t2.join(timeout=15)
        # 重复回执：恰好一个生效，另一个 CAS 拒绝（不重复清除/不清新屏障）
        applied = [r for r in results.values()
                   if r["executed"] and r["applied"]]
        cas = [r for r in results.values()
               if not r["executed"] and r["refused"] == "revision-changed"]
        self.assertEqual(len(applied), 1)
        self.assertEqual(len(cas), 1)
        snap = self.service.get_session(self.sid)
        self.assertIsNone(snap["barrier"])
        self.assertEqual(snap["revision"], 2)
        # 已清除后再 finish（当前 revision）→ no-active-turn
        again = ledger_b.finish_user_turn(
            self.sid, message_id="msg-1", idle_id="idle-1",
            idle_at=at + 10, expected_revision=2)
        self.assertFalse(again["executed"])
        self.assertEqual(again["refused"], "no-active-turn")


class ReleaseHoldTests(BarrierBase):
    """显式运维解除 hold / 登记已完成用户回合（C10 补充入口）。"""

    def hold(self, *, reason="coordinator-interrupted", at=None):
        at = at if at is not None else self.clock.now_ms()
        result = self.service.hold_session(
            self.sid, event_id=f"ev-stop-{at}", origin_at=at, reason=reason)
        self.assertTrue(result["executed"])
        return at

    def test_release_hold_cas_then_idempotent(self):
        self.hold()
        rev = self.service.get_session(self.sid)["revision"]
        stale = self.service.release_hold(
            self.sid, operation_id="op-release-1", reason="operator-resume",
            expected_revision=rev - 1)
        self.assertFalse(stale["executed"])
        self.assertEqual(stale["refused"], "revision-changed")
        self.assertIsNotNone(self.service.get_session(self.sid)["hold"])

        ok = self.service.release_hold(
            self.sid, operation_id="op-release-1", reason="operator-resume",
            expected_revision=rev)
        self.assertTrue(ok["executed"])
        self.assertTrue(ok["applied"])
        self.assertEqual(ok["reason"], "hold-released")
        snap = self.service.get_session(self.sid)
        self.assertIsNone(snap["hold"])
        self.assertEqual(snap["revision"], rev + 1)
        rows = self.audit_rows("coordinator-control.release-hold")
        self.assertEqual(len(rows), 1)
        self.assertIn("operator-resume", rows[0]["detail"])

        again = self.service.release_hold(
            self.sid, operation_id="op-release-2", reason="operator-resume",
            expected_revision=snap["revision"])
        self.assertTrue(again["executed"])
        self.assertFalse(again["applied"])
        self.assertEqual(again["reason"], "no-active-hold")
        self.assertEqual(len(self.audit_rows(
            "coordinator-control.release-hold")), 1)

    def test_release_hold_refuses_barrier_and_other_reasons(self):
        self.hold()
        self.begin("msg-after-hold", self.clock.now_ms() + 10)
        snap = self.service.get_session(self.sid)
        active = self.service.release_hold(
            self.sid, operation_id="op-release-barrier",
            reason="operator-resume", expected_revision=snap["revision"])
        self.assertFalse(active["executed"])
        self.assertEqual(active["refused"], "turn-active")

        # 新建一个不同原因的 hold 必须继续被拒绝（不扩大运维放行范围）
        sid2 = "ses_other_hold"
        t2 = self.clock.now_ms() + 100
        self.service.hold_session(
            sid2, event_id="ev-other", origin_at=t2,
            reason="user-interrupted")
        snap2 = self.service.get_session(sid2)
        refused = self.service.release_hold(
            sid2, operation_id="op-release-other",
            reason="operator-resume", expected_revision=snap2["revision"])
        self.assertFalse(refused["executed"])
        self.assertEqual(refused["refused"], "hold-not-releasable")

    def test_register_completed_turn_is_atomic_and_releases_hold(self):
        held_at = self.hold()
        rev = self.service.get_session(self.sid)["revision"]
        origin = held_at + 10
        idle_at = origin + 20
        result = self.service.register_completed_turn(
            self.sid, message_id="msg-user-done", origin_at=origin,
            operation_id="op-register", idle_id="idle-done",
            idle_at=idle_at, expected_revision=rev,
            reason="operator-registered", evidence={"coverage": "complete"})
        self.assertTrue(result["executed"])
        self.assertTrue(result["applied"])
        self.assertEqual(result["reason"], "finished-with-hold-release")
        snap = self.service.get_session(self.sid)
        self.assertIsNone(snap["hold"])
        self.assertIsNone(snap["barrier"])
        self.assertEqual(snap["revision"], rev + 2)
        self.assertEqual(len(self.audit_rows(
            "coordinator-control.register-completed-turn")), 1)

    def test_register_completed_turn_failure_leaves_no_barrier(self):
        held_at = self.hold()
        rev = self.service.get_session(self.sid)["revision"]
        origin = held_at + 10
        result = self.service.register_completed_turn(
            self.sid, message_id="msg-user-done", origin_at=origin,
            operation_id="op-register-bad", idle_id="idle-bad",
            idle_at=origin, expected_revision=rev,
            reason="operator-registered")
        self.assertFalse(result["executed"])
        self.assertEqual(result["refused"],
                         "stale-termination")
        snap = self.service.get_session(self.sid)
        self.assertIsNotNone(snap["hold"])
        self.assertIsNone(snap["barrier"])
        self.assertEqual(snap["revision"], rev)
        self.assertEqual(len(self.audit_rows(
            "coordinator-control.register-completed-turn")), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
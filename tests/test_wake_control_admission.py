"""原子发送认领：provider 半开探测 + 输入 possible 标记同一事务。

覆盖（父接线后识别的风险）：Supervisor 旧 ``_send_wake_input`` 在两笔短事务间
（``claim_probe`` 后、``mark_input_possible`` 前）若暂停/输入 CAS 先提交会烧
provider 预算或留下孤立 probe（实际未 POST）。本组测试用真 WAL 双连接确定性
交错验证 ``WakeAdmission.claim``：

- 暂停先赢 / 回合 hold 先赢：控制先提交 → claim 内复核拒绝并**整体回滚**，
  不产生 probe（finalize 先于控制建立，避免 finalize 自身被控制挡掉）；
- 发送先赢 / 两个发送者唯一赢家：恰好一个 winner，败者不持久化 probe；
- CAS 失败不烧 probe：陈旧 expected_revision / owner/终态 → 回滚探针花费；
- provider 拒绝无 possible：冷却只拒绝，输入保持 prepared；
- unknown 原 ID 不得换：以新 message_id 换发被拒；
- 审计回滚/唯一：败者 input.possible 审计不落库，胜者恰一条；
- 嵌套事务拒绝：claim 必须自管一笔短事务。

仅标准库 + FakeClock + 真临时 SQLite（WAL 两连接）离线测试；不 POST、不访问
真实服务/台账、不跑 offline_discovery。
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from oc_orch.adapters.coordinator_control import CoordinatorControlLedger
from oc_orch.adapters.sqlite_db import init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.target_gate import TargetGate
from oc_orch.adapters.wake_admission import WakeAdmission
from oc_orch.application.coordinator_control import CoordinatorControlService
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


def _body_of(members):
    return "wake: " + ", ".join(sorted(m["id"] for m in members))


class WakeAdmissionBase(unittest.TestCase):
    PROJECT = "p"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="wake-admission-",
                                               dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.gate = TargetGate(self.conn, self.clock)
        self.wake = WakeAdmission(self.ledger, self.gate)
        self.session = "ses_c"
        self.owner = "sup-a"

    # ---- 准备 --------------------------------------------------------
    def enqueue(self, key: str):
        return self.ledger.enqueue_notification(
            kind="stall", target_session=self.session, dedupe_key=key,
            payload={"summary": key}, project=self.PROJECT)

    def finalized_input(self, *keys: str):
        """reserve + finalize：返回可认领输入的关键字段（admission=prepared）。"""
        ids = [self.enqueue(k)["id"] for k in keys]
        res = self.ledger.reserve_target_input(
            self.session, notification_ids=ids, owner=self.owner)
        inp = self.ledger.get_input(res["input"]["input_id"])
        fin = self.ledger.finalize_input(
            inp["input_id"], final_notification_ids=ids,
            text_factory=_body_of, owner=self.owner,
            expected_revision=inp["revision"])
        fresh = fin["input"]
        return {"input_id": fresh["input_id"], "session_id": fresh["session_id"],
                "message_id": fresh["message_id"], "owner": self.owner,
                "revision": fresh["revision"], "notification_ids": ids}

    def activate_gate(self):
        """让 claim_probe 真正花费：冷却已过期的 provider 闸门。"""
        self.gate.observe_error(
            self.session, {"category": "rate_limit", "status": 429,
                           "message": "burst", "retry_after_ms": 1},
            evidence_key="wake-admission:e1", observed_at=self.clock.now_ms())
        self.clock.advance(seconds=2)

    def claim(self, fi, *, expected_revision=None, message_id=None,
              session_id=None, owner=None):
        return self.wake.claim(
            session_id or fi["session_id"], input_id=fi["input_id"],
            message_id=message_id or fi["message_id"],
            owner=owner or fi["owner"],
            expected_revision=(expected_revision
                               if expected_revision is not None
                               else fi["revision"]),
            now=self.clock.now_ms())

    def control(self):
        return CoordinatorControlService(
            CoordinatorControlLedger(self.conn, self.clock), self.clock)

    # ---- 断言辅助 ----------------------------------------------------
    def audit_possible(self, input_id):
        return self.conn.execute(
            "SELECT COUNT(*) AS n FROM audit WHERE action='input.possible' "
            "AND entity='session_input' AND entity_id=?",
            (input_id,)).fetchone()["n"]

    def gate_snap(self):
        return self.gate.get(self.session) or {}


class ClaimHappyPathTests(WakeAdmissionBase):
    def test_bad_revision_and_nonautomatic_intent_never_claim_probe(self):
        fi = self.finalized_input("strict")
        self.activate_gate()
        before = self.gate_snap()
        for revision in (True, False, 0, -1, "1", None):
            with self.subTest(revision=revision):
                out = self.wake.claim(
                    self.session, input_id=fi["input_id"], message_id=fi["message_id"],
                    owner=self.owner, expected_revision=revision, now=self.clock.now_ms())
                self.assertFalse(out["allowed"])
                self.assertEqual(self.gate_snap(), before)
        self.conn.execute("UPDATE session_inputs SET intent='park' WHERE input_id=?",
                          (fi["input_id"],))
        out = self.claim(fi)
        self.assertFalse(out["allowed"])
        self.assertEqual(out["reason"], "invalid-automatic-intent")
        self.assertEqual(self.gate_snap(), before)

    def test_internal_mutations_require_an_open_transaction(self):
        fi = self.finalized_input("transaction")
        with self.assertRaises(RuntimeError):
            self.ledger._mark_input_possible_in_tx(
                fi["input_id"], owner=self.owner, expected_revision=fi["revision"],
                now=self.clock.now_ms())
        with self.assertRaises(RuntimeError):
            self.gate._claim_probe_in_tx(
                self.session, owner=self.owner, message_id=fi["message_id"],
                now=self.clock.now_ms())

    def test_happy_path_claims_probe_and_possible_atomically(self):
        fi = self.finalized_input("one")
        self.activate_gate()
        result = self.claim(fi)
        self.assertTrue(result["allowed"], result)
        self.assertEqual(result["reason"], "admitted")
        self.assertEqual(result["probe"]["allowed"], True)
        self.assertEqual(result["probe"]["reason"], "probe")
        self.assertEqual(result["probe"]["spent"], True)
        self.assertEqual(result["probe"]["state"]["probe_message_id"],
                         fi["message_id"])
        self.assertEqual(result["input"]["admission_state"], "possible")
        self.assertEqual(result["input"]["claim_owner"], self.owner)
        # 事务已提交：probe 与 possible 都在磁盘可见
        snap = self.gate_snap()
        self.assertEqual(snap["state"], "probe-outstanding")
        self.assertEqual(snap["probe_message_id"], fi["message_id"])
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "possible")
        self.assertEqual(self.audit_possible(fi["input_id"]), 1)

    def test_second_claim_same_input_refused_without_double_count(self):
        fi = self.finalized_input("one")
        self.activate_gate()
        first = self.claim(fi)
        self.assertTrue(first["allowed"])
        self.clock.advance(seconds=1)
        second = self.claim(fi)
        self.assertFalse(second["allowed"])
        self.assertEqual(second["reason"], "gate:probe-outstanding")
        # 重复认领不新增 possible 审计；发送权只授予一次
        self.assertEqual(self.audit_possible(fi["input_id"]), 1)
        self.assertEqual(self.gate_snap()["probe_message_id"],
                         fi["message_id"])

    def test_provider_cooldown_refused_keeps_input_prepared(self):
        """provider 拒绝（冷却）→ allowed=False，无 possible，输入保持 prepared。"""
        self.gate.observe_error(
            self.session, {"category": "quota", "status": 429,
                           "message": "limit"},
            evidence_key="wake-admission:e2", observed_at=self.clock.now_ms())
        fi = self.finalized_input("one")
        result = self.claim(fi)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "gate:cooldown")
        self.assertIsNotNone(result["probe"])
        self.assertEqual(result["probe"]["allowed"], False)
        self.assertEqual(result["probe"]["reason"], "cooldown")
        self.assertEqual(result["input"]["admission_state"], "prepared")
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "prepared")
        self.assertEqual(self.audit_possible(fi["input_id"]), 0)
        self.assertEqual(self.gate_snap()["state"], "blocked")


class IdentityGuardTests(WakeAdmissionBase):
    def test_unknown_original_message_id_cannot_be_replaced(self):
        fi = self.finalized_input("one")
        result = self.claim(fi, message_id="msg_ocorch_tampered_deadbeef")
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "input-message-mismatch")
        self.assertIsNone(result["probe"])
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "prepared")

    def test_session_mismatch_refused(self):
        fi = self.finalized_input("one")
        result = self.claim(fi, session_id="ses_other")
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "session-mismatch")
        self.assertIsNone(result["probe"])

    def test_invalid_owner_refused(self):
        fi = self.finalized_input("one")
        result = self.claim(fi, owner="  ")
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "invalid-owner")
        self.assertIsNone(result["probe"])

    def test_input_not_found_refused(self):
        fi = self.finalized_input("one")
        result = self.wake.claim(
            fi["session_id"], input_id="N-unknown-input",
            message_id=fi["message_id"], owner=fi["owner"],
            expected_revision=1, now=self.clock.now_ms())
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "input-not-found")
        self.assertIsNone(result["input"])


class RollbackSpendTests(WakeAdmissionBase):
    """possible/CAS/控制失败必须整体回滚 probe 花费（不烧预算、不留孤立 probe）。"""

    def test_stale_revision_cas_failure_does_not_burn_probe(self):
        fi = self.finalized_input("one")
        self.activate_gate()
        self.assertEqual(self.gate_snap()["probe_count"], 0)
        result = self.claim(fi, expected_revision=fi["revision"] - 100)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "control:INPUT_REVISION_CONFLICT")
        self.assertIsNone(result["probe"])
        snap = self.gate_snap()
        self.assertEqual(snap["probe_count"], 0, "CAS 失败不得烧 probe")
        self.assertIsNone(snap["probe_message_id"])
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "prepared")
        self.assertEqual(self.audit_possible(fi["input_id"]), 0)

    def test_owner_mismatch_rolls_back_probe_spend(self):
        fi = self.finalized_input("one")
        self.activate_gate()
        result = self.claim(fi, owner="sup-other")
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "control:INPUT_OWNER_MISMATCH")
        self.assertIsNone(result["probe"])
        snap = self.gate_snap()
        self.assertEqual(snap["probe_count"], 0)
        self.assertIsNone(snap["probe_message_id"])

    def test_project_pause_first_blocks_claim_and_rolls_back(self):
        """暂停先赢（先提交）：claim 内控制复核拒绝 → 整体回滚 probe。"""
        fi = self.finalized_input("one")
        self.ledger.claim_coordinator(project=self.PROJECT,
                                      session_id=self.session, actor="test")
        self.control().pause(self.PROJECT, session_id=self.session,
                             operation_id="op-pause", reason="user-pause",
                             expected_revision=0, expected_epoch=1)
        self.activate_gate()
        result = self.claim(fi)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "control:PROJECT_PAUSED")
        self.assertIsNone(result["probe"])
        snap = self.gate_snap()
        self.assertEqual(snap["probe_count"], 0, "暂停拒绝不得烧 probe")
        self.assertIsNone(snap["probe_message_id"])
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "prepared")
        self.assertEqual(self.audit_possible(fi["input_id"]), 0)
        # 恢复后可正常认领（说明拒绝只是回滚，不破坏后续路径）
        self.control().resume(self.PROJECT, session_id=self.session,
                              operation_id="op-resume", reason="resume",
                              expected_revision=1, expected_epoch=1)
        retry = self.claim(fi)
        self.assertTrue(retry["allowed"], retry)

    def test_session_hold_blocks_claim_and_rolls_back(self):
        fi = self.finalized_input("one")
        self.control().hold_session(self.session, event_id="ev-stop",
                                    origin_at=self.clock.now_ms(),
                                    reason="user-interrupted")
        self.activate_gate()
        result = self.claim(fi)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "control:COORDINATOR_HELD")
        self.assertIsNone(result["probe"])
        snap = self.gate_snap()
        self.assertEqual(snap["probe_count"], 0)
        self.assertIsNone(snap["probe_message_id"])
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "prepared")
        self.assertEqual(self.audit_possible(fi["input_id"]), 0)

    def test_terminal_input_blocks_claim(self):
        fi = self.finalized_input("one")
        self.ledger.release_input_slot(
            fi["input_id"], reason="free-up", now=self.clock.now_ms())
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "released")
        self.activate_gate()
        conn_b = _open_wal(self.home)
        self.addCleanup(conn_b.close)
        ledger_b = Ledger(conn_b, self.home, clock=self.clock)
        gate_b = TargetGate(ledger_b.conn, self.clock)
        wake_b = WakeAdmission(ledger_b, gate_b)
        result = wake_b.claim(
            self.session, input_id=fi["input_id"],
            message_id=fi["message_id"], owner=fi["owner"],
            expected_revision=fi["revision"])
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "control:INPUT_TERMINAL")
        self.assertIsNone(result["probe"])
        self.assertEqual(gate_b.get(self.session)["probe_count"], 0,
                         "终态输入不烧 probe")

    def test_nested_call_inside_existing_transaction_refused(self):
        fi = self.finalized_input("one")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(RuntimeError):
                self.wake.claim(fi["session_id"], input_id=fi["input_id"],
                                message_id=fi["message_id"], owner=fi["owner"],
                                expected_revision=fi["revision"])
        finally:
            self.conn.execute("ROLLBACK")


class DeterministicInterleaveTests(WakeAdmissionBase):
    """真 WAL 两连接 + 线程屏障：暂停先赢 / 发送唯一赢家 / CAS 失败不烧 probe。"""

    def test_pause_wins_before_claim_in_concurrent_wal(self):
        self.ledger.claim_coordinator(project=self.PROJECT,
                                      session_id=self.session, actor="test")
        fi = self.finalized_input("one")
        self.activate_gate()
        start = threading.Barrier(2)
        pause_done = threading.Barrier(2)
        results: dict = {}

        def worker_pause():
            conn_a = _open_wal(self.home)
            try:
                ledger_a = Ledger(conn_a, self.home, clock=self.clock)
                control_a = CoordinatorControlService(
                    CoordinatorControlLedger(conn_a, self.clock), self.clock)
                start.wait()
                control_a.pause(self.PROJECT, session_id=self.session,
                                operation_id="op-p", reason="user-pause",
                                expected_revision=0, expected_epoch=1)
            finally:
                conn_a.close()
            pause_done.wait()

        def worker_claim():
            conn_b = _open_wal(self.home)
            try:
                ledger_b = Ledger(conn_b, self.home, clock=self.clock)
                gate_b = TargetGate(conn_b, self.clock)
                wake_b = WakeAdmission(ledger_b, gate_b)
                start.wait()
                pause_done.wait()  # 暂停已提交，claim 再进入
                results["claim"] = wake_b.claim(
                    self.session, input_id=fi["input_id"],
                    message_id=fi["message_id"], owner=fi["owner"],
                    expected_revision=fi["revision"])
            finally:
                conn_b.close()

        t1 = threading.Thread(target=worker_pause)
        t2 = threading.Thread(target=worker_claim)
        t1.start()
        t2.start()
        t1.join(timeout=20)
        t2.join(timeout=20)
        claim = results["claim"]
        self.assertFalse(claim["allowed"])
        self.assertEqual(claim["reason"], "control:PROJECT_PAUSED")
        self.assertIsNone(claim["probe"])
        snap = self.gate_snap()
        self.assertEqual(snap["probe_count"], 0, "并发暂停先赢不得烧 probe")
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "prepared")
        self.assertEqual(self.audit_possible(fi["input_id"]), 0)

    def test_two_senders_only_one_winner_with_wal(self):
        fi = self.finalized_input("one")
        self.activate_gate()
        start = threading.Barrier(2)
        results: dict = {}

        def worker(name):
            conn = _open_wal(self.home)
            try:
                ledger = Ledger(conn, self.home, clock=self.clock)
                gate = TargetGate(conn, self.clock)
                wake = WakeAdmission(ledger, gate)
                start.wait()
                results[name] = wake.claim(
                    self.session, input_id=fi["input_id"],
                    message_id=fi["message_id"], owner=fi["owner"],
                    expected_revision=fi["revision"])
            finally:
                conn.close()

        t1 = threading.Thread(target=worker, args=("a",))
        t2 = threading.Thread(target=worker, args=("b",))
        t1.start()
        t2.start()
        t1.join(timeout=20)
        t2.join(timeout=20)
        winners = [r for r in results.values() if r["allowed"]]
        losers = [r for r in results.values() if not r["allowed"]]
        self.assertEqual(len(winners), 1, results)
        self.assertEqual(len(losers), 1, results)
        # 败者不持久化 probe：probe 只来自胜者那一次提交
        snap = self.gate_snap()
        self.assertEqual(snap["probe_count"], 1)
        self.assertEqual(snap["probe_message_id"], fi["message_id"])
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "possible")
        self.assertEqual(self.audit_possible(fi["input_id"]), 1)
        self.assertEqual(winners[0]["reason"], "admitted")
        self.assertTrue(losers[0]["reason"].startswith("gate:"),
                        losers[0]["reason"])

    def test_two_senders_stale_cas_both_fail_no_probe_burned(self):
        """两连接同用陈旧 revision：双双 CAS 失败并回滚，probe 一枚都不烧。"""
        fi = self.finalized_input("one")
        self.activate_gate()
        stale_rev = fi["revision"] - 100
        start = threading.Barrier(2)
        results: dict = {}

        def worker(name):
            conn = _open_wal(self.home)
            try:
                ledger = Ledger(conn, self.home, clock=self.clock)
                gate = TargetGate(conn, self.clock)
                wake = WakeAdmission(ledger, gate)
                start.wait()
                results[name] = wake.claim(
                    self.session, input_id=fi["input_id"],
                    message_id=fi["message_id"], owner=fi["owner"],
                    expected_revision=stale_rev)
            finally:
                conn.close()

        t1 = threading.Thread(target=worker, args=("a",))
        t2 = threading.Thread(target=worker, args=("b",))
        t1.start()
        t2.start()
        t1.join(timeout=20)
        t2.join(timeout=20)
        for r in results.values():
            self.assertFalse(r["allowed"])
            self.assertEqual(r["reason"], "control:INPUT_REVISION_CONFLICT")
            self.assertIsNone(r["probe"])
        self.assertEqual(self.gate_snap()["probe_count"], 0,
                         "两个败者都不应烧 probe")
        self.assertEqual(self.ledger.get_input(fi["input_id"])[
            "admission_state"], "prepared")
        self.assertEqual(self.audit_possible(fi["input_id"]), 0)

    def test_cross_connection_claim_after_winner_stops_at_probe_gate(self):
        """发送先赢后，另一连接再 claim → 停在 probe-outstanding，不再烧预算。"""
        fi = self.finalized_input("one")
        self.activate_gate()
        first = self.claim(fi)
        self.assertTrue(first["allowed"])
        conn_b = _open_wal(self.home)
        self.addCleanup(conn_b.close)
        ledger_b = Ledger(conn_b, self.home, clock=self.clock)
        gate_b = TargetGate(conn_b, self.clock)
        wake_b = WakeAdmission(ledger_b, gate_b)
        again = wake_b.claim(self.session, input_id=fi["input_id"],
                             message_id=fi["message_id"], owner=fi["owner"],
                             expected_revision=fi["revision"],
                             now=self.clock.now_ms())
        self.assertFalse(again["allowed"])
        self.assertEqual(again["reason"], "gate:probe-outstanding")
        self.assertIsNotNone(again["probe"])
        snap = self.gate_snap()
        self.assertEqual(snap["probe_count"], 1, "败者不得再烧 probe")
        self.assertEqual(self.audit_possible(fi["input_id"]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)

#!/usr/bin/env python3
"""S4 B 组存储：ManagedInputLedger（受管输入回执 + 显式白名单观察）独立回归。

只使用真临时 SQLite（WAL，含两连接 CAS）+ FakeClock；不连接真实服务、
不构造 Client、不伪造 Task/Attempt。覆盖：

- prepare：execute/park/record 独立受管行（outstanding=0）与 request_id 幂等；
  park 必填 owner/flush_condition/expires_at 且 expiry_policy 仅 alert；
  record 留档终态无发送；绝不与 P0 自动 wake 合并；
- claim：owner + revision CAS 一次赢家，reason 持久，operation 原 ID/revision
  保存，终态/late 守卫，同连接与两连接并发单赢家；
- record_managed_observation：受理/投递/取消四态单调（重复不计 send、不重置
  时间），delivery 与 cancel 冲突不伪造 ACK，execution/business_ack 仅留档；
- 白名单：默认空、owner 冲突拒绝、unregister CAS、撤销后晚到观察不重新启用、
  坏记录 fail-closed；
- expire_managed_parks：只告警不 cancel/flush/烧 budget；持久 expiry_alert
  标记 + condition_key——ACK 后同一未变条件不再每轮造新告警；避免嵌套事务。
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))
sys.path.insert(0, os.path.dirname(HERE))  # 便于直接运行 python3 tests/test_x.py

from oc_orch.db import db_path, init
from oc_orch.ledger import Ledger, LedgerError
from oc_orch.adapters.managed_input_ledger import (
    ADMISSION_RECORDED,
    MANAGED_META_SCHEMA,
    MAX_CLOCK_MS,
    OP_CANCEL,
    OP_FLUSH,
    OP_SEND,
    SOURCE_MANAGED_INPUT,
    WATCH_KEY_PREFIX,
    WATCH_SCHEMA,
    ManagedInputLedger,
)
from tests.fakes import FakeClock


def _open_wal(home):
    """测试专用：跨线程独占连接（等价 sqlite_db.connect 的 PRAGMA 组合）。"""
    conn = sqlite3.connect(db_path(home), timeout=15, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


class ManagedBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="oc-orch-mil-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.mil = ManagedInputLedger(self.conn, self.ledger)

    @property
    def t0(self):
        return self.clock.now_ms()

    def prepare(self, session="ses_a", *, intent="execute", text="hi",
                owner="owner-a", request_id="rid-1", **kw) -> dict:
        return self.mil.prepare_managed_input(
            session, intent=intent, text=text, owner=owner,
            request_id=request_id, now=self.clock.now_ms(), **kw)

    def park(self, session="ses_a", *, expires_at=None, flush_condition="fc-1",
             **kw) -> dict:
        return self.prepare(session, intent="park",
                            expires_at=expires_at if expires_at is not None
                            else self.clock.now_ms() + 10_000,
                            flush_condition=flush_condition, **kw)

    def claim(self, inp: dict, operation=OP_SEND, owner=None,
              expected_revision=None, reason="test", **kw) -> dict:
        data = self.mil.get_managed_input(inp["input_id"]) or inp
        return self.mil.claim_managed_operation(
            data["input_id"], operation=operation,
            owner=owner or data["owner"],
            expected_revision=data["revision"] if expected_revision is None
            else expected_revision,
            reason=reason, now=self.clock.now_ms(), **kw)

    def observe(self, inp: dict, *, operation=OP_SEND, state="admission",
                op_rev=None, message_id=None, **kw) -> dict:
        data = self.mil.get_managed_input(inp["input_id"]) or inp
        claim = (data["metadata"]["managed_input"]["operations"] or {}).get(operation) or {}
        evidence = {
            "message_id": message_id if message_id is not None else data["message_id"],
            "operation": operation,
            "operation_revision": claim.get("operation_revision")
            if op_rev is None else op_rev,
            state: True,
            "via": "test",
        }
        evidence.update(kw)
        return self.mil.record_managed_observation(
            data["input_id"], evidence=evidence, now=self.clock.now_ms())

    def fresh(self, inp: dict) -> dict:
        return self.mil.get_managed_input(inp["input_id"])


class TestPrepare(ManagedBase):
    def test_execute_creates_isolated_managed_row(self):
        res = self.prepare("ses_a", intent="execute", text="go",
                           owner="owner-a", request_id="rid-x", project="p1")
        self.assertTrue(res["created"])
        inp = res["input"]
        self.assertEqual(inp["source"], SOURCE_MANAGED_INPUT)
        self.assertEqual(inp["intent"], "execute")
        self.assertEqual(inp["admission_state"], "prepared")
        self.assertFalse(inp["outstanding"])
        self.assertEqual(inp["revision"], 1)
        self.assertEqual(inp["owner"], "owner-a")
        self.assertEqual(inp["body"], "go")
        self.assertTrue(inp["body_hash"])
        self.assertTrue(inp["message_id"].startswith("msg_ocorch"))
        self.assertEqual(inp["notification_ids"], [])
        block = inp["metadata"]["managed_input"]
        self.assertEqual(block["schema"], MANAGED_META_SCHEMA)
        self.assertEqual(block["request_id"], "rid-x")
        self.assertEqual(block["project"], "p1")
        self.assertIs(block["resume"], True)
        # 不占用自动 wake 未决槽
        self.assertEqual(self.ledger.list_pending_inputs(
            target_session="ses_a", outstanding=True), [])

    def test_park_required_fields(self):
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", intent="park", expires_at=None,
                         flush_condition="fc")
        self.assertEqual(cm.exception.code, "INVALID_INPUT")
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", intent="park", expires_at=1, flush_condition="")
        self.assertEqual(cm.exception.code, "INVALID_INPUT")
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", intent="park", expires_at=1,
                         flush_condition=None)
        self.assertEqual(cm.exception.code, "INVALID_INPUT")

    def test_park_meta_resume_false_expiry_policy_alert(self):
        at = self.clock.now_ms()
        res = self.prepare("ses_a", intent="park", expires_at=at + 5000,
                           flush_condition="idle and empty",
                           owner="owner-a", request_id="rid-p")
        block = res["input"]["metadata"]["managed_input"]
        self.assertIs(block["resume"], False)
        self.assertEqual(block["expires_at"], at + 5000)
        self.assertEqual(block["flush_condition"], "idle and empty")
        self.assertEqual(block["expiry_policy"], "alert")
        self.assertEqual(res["input"]["admission_state"], "prepared")

    def test_record_intent_terminal_no_send_phase(self):
        res = self.prepare("ses_a", intent="record", text="留档",
                           owner="owner-a", request_id="rid-r")
        inp = res["input"]
        self.assertEqual(inp["admission_state"], ADMISSION_RECORDED)
        block = inp["metadata"]["managed_input"]
        self.assertIsNone(block["resume"])
        self.assertEqual(block["recorded_at"], self.clock.now_ms())
        with self.assertRaises(LedgerError) as cm:
            self.claim(inp, operation=OP_SEND)
        self.assertEqual(cm.exception.code, "INPUT_TERMINAL")

    def test_bad_args_rejected(self):
        with self.assertRaises(LedgerError) as cm:
            self.prepare("", text="x", owner="o", request_id="r")
        self.assertEqual(cm.exception.code, "INVALID_INPUT")
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", text="", owner="o", request_id="r")
        self.assertEqual(cm.exception.code, "INVALID_INPUT")
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", text="x", owner=" ", request_id="r")
        self.assertEqual(cm.exception.code, "INVALID_OWNER")
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", text="x", owner="o", request_id=" ")
        self.assertEqual(cm.exception.code, "INVALID_INPUT")
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", text="x", owner="o", request_id="r",
                         intent="bogus")
        self.assertEqual(cm.exception.code, "INVALID_INTENT")
        with self.assertRaises(LedgerError) as cm:
            self.mil.prepare_managed_input(
                "ses_a", intent="execute", text="x", owner="o",
                request_id="r", now=10**30)
        self.assertEqual(cm.exception.code, "INVALID_TIMESTAMP")


class TestRequestIdIdempotency(ManagedBase):
    def test_same_request_id_same_content_idempotent(self):
        first = self.prepare("ses_a", text="same", owner="o1", request_id="K")
        again = self.prepare("ses_a", text="same", owner="o1", request_id="K")
        self.assertFalse(again["created"])
        self.assertEqual(again["input"]["input_id"], first["input"]["input_id"])
        self.assertEqual(len(self.mil.list_managed_inputs(session_id="ses_a")), 1)

    def test_same_request_id_different_body_conflict(self):
        self.prepare("ses_a", text="A", owner="o1", request_id="K")
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", text="B", owner="o1", request_id="K")
        self.assertEqual(cm.exception.code, "REQUEST_ID_CONFLICT")

    def test_same_request_id_different_intent_conflict(self):
        self.prepare("ses_a", intent="execute", text="A", owner="o1",
                     request_id="K")
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", intent="park", text="A", owner="o1",
                         request_id="K", expires_at=1, flush_condition="fc")
        self.assertEqual(cm.exception.code, "REQUEST_ID_CONFLICT")

    def test_same_request_id_different_owner_conflict(self):
        self.prepare("ses_a", text="A", owner="o1", request_id="K")
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", text="A", owner="o2", request_id="K")
        self.assertEqual(cm.exception.code, "REQUEST_ID_CONFLICT")

    def test_same_request_id_different_session_allowed(self):
        a = self.prepare("ses_a", text="A", owner="o1", request_id="K")
        b = self.prepare("ses_b", text="A", owner="o1", request_id="K")
        self.assertTrue(a["created"] and b["created"])
        self.assertNotEqual(a["input"]["input_id"], b["input"]["input_id"])

    def test_tracking_identity_ignores_volatile_deadline_but_not_task(self):
        tracking = {"tracked": True, "task_id": "T-1", "attempt_id": "A-1",
                    "generation": 1, "work_kind": "observe",
                    "target_session": "ses_c", "deadline": 100,
                    "next_check_at": 200}
        first = self.prepare("ses_a", text="same", owner="o1", request_id="K",
                             message_id="msg_idem_1", tracking=tracking)
        later = dict(tracking, deadline=999, next_check_at=1000)
        again = self.prepare("ses_a", text="same", owner="o1", request_id="K",
                             message_id="msg_idem_1", tracking=later)
        self.assertFalse(again["created"])
        self.assertEqual(again["input"]["input_id"],
                         first["input"]["input_id"])
        with self.assertRaises(LedgerError) as cm:
            self.prepare("ses_a", text="same", owner="o1", request_id="K",
                         message_id="msg_idem_1",
                         tracking=dict(tracking, task_id="T-2"))
        self.assertEqual(cm.exception.code, "REQUEST_ID_CONFLICT")

    def test_managed_row_never_merges_into_automatic_wake(self):
        # P0 自动 wake 预留
        n = self.ledger.enqueue_notification(
            kind="stall", target_session="ses_c", dedupe_key="w1",
            payload={"summary": "w"}, project="p")
        wake = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[n["id"]], owner="sup")
        self.assertTrue(wake["created"])
        # 同一目标再登记受管输入：各自独立，互不合并
        res = self.prepare("ses_c", intent="execute", text="manual",
                           owner="o1", request_id="m1")
        self.assertTrue(res["created"])
        self.assertEqual(res["input"]["admission_state"], "prepared")
        # 自动 wake 槽位/状态不变
        self.assertEqual(self.ledger.list_pending_inputs(
            target_session="ses_c", outstanding=True)[0]["input_id"],
            wake["input"]["input_id"])
        listed = self.mil.list_managed_inputs(session_id="ses_c")
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["input_id"], res["input"]["input_id"])
        # 自动 wake 的 P0 方法不受影响（reserve 返回既有行）
        n2 = self.ledger.enqueue_notification(
            kind="stall", target_session="ses_c", dedupe_key="w2",
            payload={"summary": "w2"}, project="p")
        held = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[n2["id"]], owner="other")
        self.assertFalse(held["created"])
        self.assertEqual(held["input"]["input_id"], wake["input"]["input_id"])

    def test_list_excludes_body_get_includes(self):
        self.prepare("ses_a", text="secret-text", owner="o1", request_id="K")
        inp = self.mil.get_managed_input(
            self.mil.list_managed_inputs(session_id="ses_a")[0]["input_id"])
        self.assertIsNotNone(inp)
        listed = self.mil.list_managed_inputs(session_id="ses_a")[0]
        self.assertNotIn("body", listed)
        self.assertEqual(inp["body"], "secret-text")
        self.assertIsNone(self.mil.get_managed_input("MI-missing"))
        with self.assertRaises(LedgerError) as cm:
            self.mil.list_managed_inputs(limit=0)
        self.assertEqual(cm.exception.code, "INVALID_INPUT")

    def test_pending_page_ascending_bounded_and_excludes_record_delivered(self):
        first = self.prepare("ses_a", text="p1", owner="o1",
                             request_id="page-1", message_id="msg_page_1")
        second = self.prepare("ses_a", text="p2", owner="o1",
                              request_id="page-2", message_id="msg_page_2")
        self.prepare("ses_a", intent="record", text="r", owner="o1",
                     request_id="page-record")
        ids = sorted([first["input"]["input_id"], second["input"]["input_id"]])
        page = self.mil.list_pending_managed_inputs_page(limit=1)
        self.assertEqual(len(page), 1)
        self.assertEqual(page[0]["input_id"], ids[0])
        self.assertIsNone(page[0]["metadata"].get("legacy_receipt"))
        nxt = self.mil.list_pending_managed_inputs_page(
            after_id=ids[0], limit=1)
        self.assertEqual([r["input_id"] for r in nxt], [ids[1]])
        # delivered 行不再进入待对账页。
        target = next(r for r in (first, second)
                      if r["input"]["input_id"] == ids[0])["input"]
        self.mil.claim_managed_operation(
            target["input_id"], operation=OP_SEND, owner="o1",
            expected_revision=target["revision"], reason="page",
            now=self.clock.now_ms())
        current = self.mil.get_managed_input(target["input_id"])
        self.mil.record_managed_observation(
            target["input_id"], evidence={
                "message_id": current["message_id"], "operation": OP_SEND,
                "operation_revision": current["metadata"]["managed_input"][
                    "operations"][OP_SEND]["operation_revision"],
                "delivery": True}, now=self.clock.now_ms())
        again = self.mil.list_pending_managed_inputs_page(limit=10)
        self.assertNotIn(ids[0], [r["input_id"] for r in again])


class TestClaimOperation(ManagedBase):
    def test_claim_single_winner_advances_revision(self):
        res = self.prepare("ses_a", text="go", owner="o1", request_id="K")
        out = self.claim(res["input"], operation=OP_SEND, reason="now-send")
        self.assertTrue(out["changed"])
        self.assertEqual(out["revision"], 2)
        self.assertEqual(out["input"]["admission_state"], "possible")
        self.assertEqual(out["input"]["send_count"], 1)
        op = out["input"]["metadata"]["managed_input"]["operations"][OP_SEND]
        self.assertEqual(op["state"], "claimed")
        self.assertEqual(op["operation_revision"], 2)
        self.assertEqual(op["message_id"], res["input"]["message_id"])
        self.assertEqual(op["attempts"], 1)
        self.assertEqual(op["owner"], "o1")
        self.assertEqual(op["reason"], "now-send")

    def test_owner_and_revision_cas(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        with self.assertRaises(LedgerError) as cm:
            self.claim(res["input"], owner="o2")
        self.assertEqual(cm.exception.code, "INPUT_OWNER_MISMATCH")
        with self.assertRaises(LedgerError) as cm:
            self.claim(res["input"], expected_revision=99)
        self.assertEqual(cm.exception.code, "INPUT_REVISION_CONFLICT")
        self.claim(res["input"])  # rev 1 → 2
        with self.assertRaises(LedgerError) as cm:
            self.claim(res["input"], expected_revision=1)  # 旧快照
        self.assertEqual(cm.exception.code, "INPUT_REVISION_CONFLICT")

    def test_second_claim_while_inflight_returns_unresolved(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        first = self.claim(res["input"], operation=OP_SEND)
        out = self.claim(res["input"], operation=OP_SEND)
        self.assertFalse(out["changed"])
        self.assertEqual(out["reason"], "operation-unresolved")
        self.assertEqual(out["revision"], first["revision"])
        # 未新增 claim：attempts/revision 不变
        data = self.fresh(res["input"])
        self.assertEqual(data["revision"], first["revision"])
        self.assertEqual(
            data["metadata"]["managed_input"]["operations"][OP_SEND]["attempts"], 1)

    def test_flush_cancel_state_gates(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        # prepared 不能 flush（尚未入队）
        with self.assertRaises(LedgerError) as cm:
            self.claim(res["input"], operation=OP_FLUSH)
        self.assertEqual(cm.exception.code, "FLUSH_NOT_QUEUED")
        # cancel from prepared 允许（未发送取消）
        self.assertTrue(self.claim(res["input"], operation=OP_CANCEL)["changed"])
        # 重新准备：admitted 时 send 拒、flush/cancel 允许
        res2 = self.prepare("ses_b", owner="o1", request_id="K2")
        self.claim(res2["input"], operation=OP_SEND)
        self.fresh(res2["input"])
        self.observe(res2["input"], state="admission")
        with self.assertRaises(LedgerError) as cm:
            self.claim(res2["input"], operation=OP_SEND)
        self.assertEqual(cm.exception.code, "INPUT_ALREADY_ADMITTED")
        self.assertTrue(self.claim(res2["input"], operation=OP_FLUSH)["changed"])
        # delivered 后任何 claim 终态拒绝
        res3 = self.prepare("ses_c", owner="o1", request_id="K3")
        self.claim(res3["input"], operation=OP_SEND)
        self.observe(res3["input"], state="admission")
        self.observe(res3["input"], state="delivery")
        with self.assertRaises(LedgerError) as cm:
            self.claim(res3["input"], operation=OP_CANCEL)
        self.assertEqual(cm.exception.code, "INPUT_TERMINAL")

    def test_claim_on_automatic_wake_input_rejected(self):
        n = self.ledger.enqueue_notification(
            kind="stall", target_session="ses_w", dedupe_key="w1",
            payload={"summary": "w"}, project="p")
        wake = self.ledger.reserve_target_input(
            "ses_w", notification_ids=[n["id"]], owner="sup")
        with self.assertRaises(LedgerError) as cm:
            self.mil.claim_managed_operation(
                wake["input"]["input_id"], operation=OP_SEND, owner="sup",
                expected_revision=wake["input"]["revision"], reason="x",
                now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "INPUT_NOT_MANAGED")

    def test_bad_operation_and_reason(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        with self.assertRaises(LedgerError) as cm:
            self.claim(res["input"], operation="explode")
        self.assertEqual(cm.exception.code, "INVALID_OPERATION")
        with self.assertRaises(LedgerError) as cm:
            self.claim(res["input"], reason="  ")
        self.assertEqual(cm.exception.code, "INVALID_INPUT")
        with self.assertRaises(LedgerError) as cm:
            self.mil.claim_managed_operation(
                res["input"]["input_id"], operation=OP_SEND, owner="o1",
                expected_revision=True, reason="x", now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "INPUT_REVISION_CONFLICT")

    def test_concurrent_claim_single_winner_two_connections(self):
        self.prepare("ses_a", owner="o1", request_id="K")
        conn_a, conn_b = _open_wal(self.home), _open_wal(self.home)
        self.addCleanup(conn_a.close)
        self.addCleanup(conn_b.close)
        ledger_a = Ledger(conn_a, self.home, clock=self.clock)
        ledger_b = Ledger(conn_b, self.home, clock=self.clock)
        mil_a = ManagedInputLedger(conn_a, ledger_a)
        mil_b = ManagedInputLedger(conn_b, ledger_b)
        input_id = self.mil.list_managed_inputs(session_id="ses_a")[0]["input_id"]
        barrier = Barrier(2, timeout=20)

        def claim(gate):
            barrier.wait()
            try:
                out = gate.claim_managed_operation(
                    input_id, operation=OP_SEND, owner="o1",
                    expected_revision=1, reason="race",
                    now=self.clock.now_ms())
                return {"changed": out["changed"]}
            except LedgerError as exc:
                return {"changed": False, "code": exc.code}

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(claim, mil_a)
            f2 = pool.submit(claim, mil_b)
            r1 = f1.result(timeout=25)
            r2 = f2.result(timeout=25)
        results = [r1, r2]
        wins = [r for r in results if r.get("changed")]
        self.assertEqual(len(wins), 1, (r1, r2))
        losers = [r for r in results if not r.get("changed")]
        self.assertEqual(len(losers), 1)
        # 输家是 revision CAS 拒绝（一次赢家），并非第二个发送者
        self.assertEqual(losers[0].get("code"), "INPUT_REVISION_CONFLICT")
        final = self.mil.get_managed_input(input_id)
        self.assertEqual(final["revision"], 2)
        self.assertEqual(final["send_count"], 1)


class TestRecordObservation(ManagedBase):
    def test_admission_then_delivery_monotonic(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        self.claim(res["input"], operation=OP_SEND)
        out = self.observe(res["input"], state="admission")
        self.assertTrue(out["changed"])
        self.assertEqual(out["admission_state"], "admitted")
        data = self.fresh(res["input"])
        self.assertEqual(data["send_count"], 1)  # 观察不增 send
        dup = self.observe(res["input"], state="admission")
        self.assertFalse(dup["changed"])
        self.assertEqual(dup["reason"], "duplicate-admitted")
        self.assertEqual(self.fresh(res["input"])["send_count"], 1)
        delivered = self.observe(res["input"], state="delivery")
        self.assertTrue(delivered["changed"])
        self.assertEqual(delivered["admission_state"], "delivered")
        self.assertIsNotNone(self.fresh(res["input"])["delivered_at"])
        self.assertFalse(self.observe(res["input"], state="delivery")["changed"])

    def test_unknown_is_not_replayed_and_clears_only_when_resolved(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        self.claim(res["input"], operation=OP_SEND)
        unk = self.observe(res["input"], state="unknown")
        self.assertTrue(unk["changed"])
        data = self.fresh(res["input"])
        self.assertEqual(data["admission_state"], "unknown")
        self.assertEqual(data["message_id"], res["input"]["message_id"])
        op_rev = data["metadata"]["managed_input"]["operations"][OP_SEND][
            "operation_revision"]
        send_count = data["send_count"]
        # 未知未收敛：同操作不得重 POST（不再新 claim/不换 ID/不加计数）
        replay = self.claim(res["input"], operation=OP_SEND, reason="retry")
        self.assertFalse(replay["changed"])
        self.assertEqual(replay["reason"], "operation-unresolved")
        self.assertEqual(replay["revision"], op_rev)
        # 跨 operation 也不得盲 PATCH/DELETE
        cross = self.claim(res["input"], operation=OP_FLUSH, reason="steer")
        self.assertFalse(cross["changed"])
        self.assertEqual(cross["reason"], "operation-unresolved")
        after = self.fresh(res["input"])
        self.assertEqual(after["revision"], op_rev)
        self.assertEqual(after["send_count"], send_count)
        self.assertEqual(
            after["metadata"]["managed_input"]["operations"][OP_SEND]["attempts"], 1)
        # 只读对账收敛（admission）后未知操作 done，才允许后续（不同）操作
        resolved = self.observe(res["input"], state="admission")
        self.assertTrue(resolved["changed"])
        self.assertEqual(resolved["admission_state"], "admitted")
        flush = self.claim(res["input"], operation=OP_FLUSH, reason="steer")
        self.assertTrue(flush["changed"])
        # 跨操作旧 operation_revision 不得冒充新操作结果
        with self.assertRaises(LedgerError) as cm:
            self.observe(res["input"], state="cancelled", operation=OP_SEND,
                         op_rev=flush["revision"], trusted=True)
        self.assertEqual(cm.exception.code, "STALE_OBSERVATION")

    def test_delivery_after_cancel_conflict_no_ack(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        self.claim(res["input"], operation=OP_SEND)
        self.observe(res["input"], state="cancelled", trusted=True)
        self.assertEqual(self.fresh(res["input"])["admission_state"], "cancelled")
        late = self.observe(res["input"], state="delivery")
        self.assertFalse(late["changed"])
        self.assertTrue(late["conflict"])
        self.assertEqual(late["reason"], "conflict-delivered-after-cancel")
        data = self.fresh(res["input"])
        self.assertEqual(data["admission_state"], "cancelled")  # 不翻回
        self.assertIsNotNone(
            data["metadata"]["managed_input"]["evidence"]["delivered"])  # 事实留档
        self.assertIsNone(data["delivered_at"])

    def test_cancel_after_delivered_conflict(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        self.claim(res["input"], operation=OP_SEND)
        self.observe(res["input"], state="admission")
        self.observe(res["input"], state="delivery")
        late = self.observe(res["input"], state="cancelled", trusted=True)
        self.assertFalse(late["changed"])
        self.assertTrue(late["conflict"])
        self.assertEqual(late["reason"], "conflict-cancel-after-delivered")
        self.assertEqual(self.fresh(res["input"])["admission_state"], "delivered")
        # delivered 事实仍保留（DELETE 204/no-op 不等于取消成功）
        self.assertIsNotNone(self.fresh(res["input"])["delivered_at"])
        self.assertEqual(self.fresh(res["input"])["admission_state"], "delivered")

    def test_execution_business_ack_never_mutate(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        self.claim(res["input"], operation=OP_SEND)
        out = self.observe(
            res["input"], state="delivery",
            admission=True,  # 状态指示需至少一种；这里 delivery 为主
            execution={"interval": {"proven": False}},
            business_ack={"ack": "only-reference", "value": True})
        self.assertTrue(out["changed"])
        data = self.fresh(res["input"])
        ev = data["metadata"]["managed_input"]["evidence"]
        self.assertEqual(data["admission_state"], "delivered")
        self.assertIsNotNone(ev["execution"])
        self.assertIsNotNone(ev["business_ack"])
        # 未伪造 executed_at / ACK：managed 输入无 notifications/Task。
        self.assertIsNone(data["executed_at"])
        self.assertEqual(self.ledger.list_alerts(state="new"), [])
        # 已 delivery 后允许仅 execution/business_ack 的归属证据：只留档、不改状态。
        only = self.mil.record_managed_observation(
            data["input_id"],
            evidence={"message_id": data["message_id"], "operation": OP_SEND,
                      "operation_revision":
                          data["metadata"]["managed_input"][
                          "operations"][OP_SEND]["operation_revision"],
                      "execution": {"x": 1}},
            now=self.clock.now_ms())
        self.assertFalse(only["changed"])
        self.assertTrue(only["recorded"])
        self.assertEqual(self.fresh(res["input"])["admission_state"], "delivered")

    def test_execution_only_rejected_before_delivery(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        self.claim(res["input"], operation=OP_SEND)
        with self.assertRaises(LedgerError) as cm:
            self.mil.record_managed_observation(
                res["input"]["input_id"],
                evidence={"message_id": res["input"]["message_id"],
                          "operation": OP_SEND, "operation_revision": 2,
                          "execution": {"x": 1}},
                now=self.clock.now_ms())
        self.assertEqual(cm.exception.code,
                         "EXECUTION_EVIDENCE_REQUIRES_DELIVERY")

    def test_message_id_mismatch_and_bad_shape(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        self.claim(res["input"], operation=OP_SEND)
        with self.assertRaises(LedgerError) as cm:
            self.observe(res["input"], state="admission", message_id="other")
        self.assertEqual(cm.exception.code, "INPUT_EVIDENCE_MISMATCH")
        with self.assertRaises(LedgerError) as cm:
            self.mil.record_managed_observation(
                res["input"]["input_id"], evidence="bad", now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "INVALID_EVIDENCE")
        with self.assertRaises(LedgerError) as cm:
            self.mil.record_managed_observation(
                res["input"]["input_id"],
                evidence={"message_id": res["input"]["message_id"],
                          "operation": OP_SEND, "operation_revision": 1},
                now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "INVALID_EVIDENCE")

    def test_duplicate_unknown_does_not_reset_time(self):
        res = self.prepare("ses_a", owner="o1", request_id="K")
        self.claim(res["input"], operation=OP_SEND)
        self.clock.advance(wall=1000)
        self.observe(res["input"], state="unknown")
        possible_at = self.fresh(res["input"])["possible_at"]
        self.clock.advance(wall=5000)
        dup = self.observe(res["input"], state="unknown")
        self.assertFalse(dup["changed"])
        self.assertEqual(dup["reason"], "duplicate-unknown")
        self.assertEqual(self.fresh(res["input"])["possible_at"], possible_at)


class TestWatches(ManagedBase):
    def test_default_empty_and_register(self):
        self.assertEqual(self.mil.list_managed_watches(), [])
        out = self.mil.register_managed_watch(
            "ses_w", owner="obs-a", project="wg", max_pending_age_ms=3600_000,
            now=self.clock.now_ms())
        self.assertTrue(out["created"])
        self.assertTrue(out["watch"]["enabled"])
        self.assertEqual(out["watch"]["revision"], 1)
        got = self.mil.get_managed_watch("ses_w")
        self.assertEqual(got["owner"], "obs-a")
        self.assertEqual(got["project"], "wg")
        self.assertEqual(got["max_pending_age_ms"], 3600_000)
        self.assertIsNone(self.mil.get_managed_watch("ses_missing"))

    def test_owner_conflict_and_same_owner_update(self):
        self.mil.register_managed_watch(
            "ses_w", owner="obs-a", project="p", max_pending_age_ms=100,
            now=self.clock.now_ms())
        with self.assertRaises(LedgerError) as cm:
            self.mil.register_managed_watch(
                "ses_w", owner="obs-b", project="p", max_pending_age_ms=100,
                now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "WATCH_OWNER_CONFLICT")
        same = self.mil.register_managed_watch(
            "ses_w", owner="obs-a", project="p", max_pending_age_ms=100,
            now=self.clock.now_ms())
        self.assertFalse(same["changed"])  # 幂等
        upd = self.mil.register_managed_watch(
            "ses_w", owner="obs-a", project="p2", max_pending_age_ms=200,
            now=self.clock.now_ms())
        self.assertTrue(upd["changed"])
        self.assertEqual(upd["watch"]["revision"], 2)
        self.assertEqual(upd["watch"]["project"], "p2")

    def test_re_register_same_params_after_unregister_reenables(self):
        args = dict(owner="obs-a", project="p", max_pending_age_ms=100)
        self.mil.register_managed_watch("ses_w", now=self.clock.now_ms(), **args)
        old = self.mil.unregister_managed_watch(
            "ses_w", owner="obs-a", expected_revision=1,
            now=self.clock.now_ms())["watch"]
        self.assertFalse(old["enabled"])
        again = self.mil.register_managed_watch(
            "ses_w", now=self.clock.now_ms(), **args)
        self.assertTrue(again["changed"])
        current = self.mil.get_managed_watch("ses_w")
        self.assertTrue(current["enabled"])
        self.assertGreater(current["revision"], old["revision"])
        self.assertIsNone(current["unregistered_at"])
        # 旧（disabled 代）revision 的晚到观察不得复活
        with self.assertRaises(LedgerError) as cm:
            self.mil.record_managed_watch_observation(
                "ses_w", expected_revision=old["revision"],
                observation={"pending": 1}, now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "WATCH_REVISION_CONFLICT")
        self.assertTrue(self.mil.get_managed_watch("ses_w")["enabled"])

    def test_unregister_cas_owner(self):
        self.mil.register_managed_watch(
            "ses_w", owner="obs-a", project="p", max_pending_age_ms=100,
            now=self.clock.now_ms())
        with self.assertRaises(LedgerError) as cm:
            self.mil.unregister_managed_watch(
                "ses_w", owner="obs-b", expected_revision=1,
                now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "WATCH_OWNER_CONFLICT")
        with self.assertRaises(LedgerError) as cm:
            self.mil.unregister_managed_watch(
                "ses_w", owner="obs-a", expected_revision=9,
                now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "WATCH_REVISION_CONFLICT")
        out = self.mil.unregister_managed_watch(
            "ses_w", owner="obs-a", expected_revision=1,
            now=self.clock.now_ms())
        self.assertTrue(out["changed"])
        self.assertFalse(out["watch"]["enabled"])
        self.assertEqual(out["watch"]["revision"], 2)
        # 已禁用后再撤销（同一最新 revision）幂等推进，不报错
        again = self.mil.unregister_managed_watch(
            "ses_w", owner="obs-a", expected_revision=2,
            now=self.clock.now_ms())
        self.assertTrue(again["changed"])
        self.assertEqual(again["watch"]["revision"], 3)

    def test_late_observation_after_unregister_does_not_reenable(self):
        self.mil.register_managed_watch(
            "ses_w", owner="obs-a", project="p", max_pending_age_ms=100,
            now=self.clock.now_ms())
        self.mil.unregister_managed_watch(
            "ses_w", owner="obs-a", expected_revision=1,
            now=self.clock.now_ms())
        # 撤销后 revision=2；用新 revision 的晚到观察
        out = self.mil.record_managed_watch_observation(
            "ses_w", expected_revision=2,
            observation={"pending": 3}, now=self.clock.now_ms())
        self.assertFalse(out["recorded"])
        self.assertEqual(out["reason"], "watch-disabled")
        watch = self.mil.get_managed_watch("ses_w")
        self.assertFalse(watch["enabled"])
        self.assertIsNone(watch["last_observation"])
        # 撤销后即使旧 revision 的晚到观察也只返回 watch-disabled，不重新启用
        late = self.mil.record_managed_watch_observation(
            "ses_w", expected_revision=1, observation={"pending": 3},
            now=self.clock.now_ms())
        self.assertFalse(late["recorded"])
        self.assertEqual(late["reason"], "watch-disabled")
        self.assertFalse(self.mil.get_managed_watch("ses_w")["enabled"])

    def test_observation_recorded_bounded(self):
        self.mil.register_managed_watch(
            "ses_w", owner="obs-a", project="p", max_pending_age_ms=100,
            now=self.clock.now_ms())
        # enabled watch 的过期 revision → CAS 拒绝
        with self.assertRaises(LedgerError) as cm:
            self.mil.record_managed_watch_observation(
                "ses_w", expected_revision=9, observation={"pending": 0},
                now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "WATCH_REVISION_CONFLICT")
        for i in range(20):
            out = self.mil.record_managed_watch_observation(
                "ses_w", expected_revision=1,
                observation={"pending": i}, now=self.clock.now_ms() + i)
            self.assertTrue(out["recorded"])
        watch = self.mil.get_managed_watch("ses_w")
        self.assertEqual(len(watch["observations"]), 16)
        self.assertEqual(watch["last_observation"]["pending"], 19)
        self.assertEqual(watch["revision"], 1)  # 观察不递增 revision

    # ---- S4/F 补修：有界游标列表 + aged_out 持久 owner 告警 ----
    def test_list_paging_cursor_and_enabled_only_before_limit(self):
        for sid in ("ses_a", "ses_b", "ses_c"):
            self.mil.register_managed_watch(
                sid, owner="obs-a", project="p", max_pending_age_ms=100,
                now=self.clock.now_ms())
        self.mil.unregister_managed_watch(
            "ses_a", owner="obs-a", expected_revision=1,
            now=self.clock.now_ms())
        # 默认列表不变：含 disabled
        all_rows = self.mil.list_managed_watches()
        self.assertEqual([w["session_id"] for w in all_rows],
                         ["ses_a", "ses_b", "ses_c"])
        # enabled_only 在 LIMIT 前排除 disabled：ses_a 不占窗
        enabled = self.mil.list_managed_watches(limit=2, enabled_only=True)
        self.assertEqual([w["session_id"] for w in enabled],
                         ["ses_b", "ses_c"])
        # after_session 游标：严格 > 之后，升序
        page = self.mil.list_managed_watches(limit=10, after_session="ses_b")
        self.assertEqual([w["session_id"] for w in page], ["ses_c"])
        page2 = self.mil.list_managed_watches(limit=10, after_session="ses_a")
        self.assertEqual([w["session_id"] for w in page2], ["ses_b", "ses_c"])

    def _insert_watch_row(self, sid: str) -> None:
        rec = {
            "schema": WATCH_SCHEMA, "session_id": sid, "owner": "obs-a",
            "project": "p", "max_pending_age_ms": 100, "enabled": True,
            "revision": 1, "registered_at": 1, "updated_at": 1,
            "unregistered_at": None, "last_observation": None,
            "observations": [],
        }
        self.conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES(?,?)",
            (f"{WATCH_KEY_PREFIX}{sid}", json.dumps(rec, ensure_ascii=False)))

    def test_list_bounded_over_500_with_cursor(self):
        for i in range(505):
            self._insert_watch_row(f"ses_{i:04d}")
        self.assertEqual(len(self.mil.list_managed_watches()), 100,
                         "默认列表保持有界（默认 limit=100）")
        first = self.mil.list_managed_watches(limit=500)
        self.assertEqual(len(first), 500, "显式上限也必须有界（<=500）")
        tail = self.mil.list_managed_watches(
            limit=500, after_session=first[-1]["session_id"])
        self.assertEqual(len(tail), 5)
        self.assertEqual({w["session_id"] for w in first + tail},
                         {f"ses_{i:04d}" for i in range(505)})

    def test_list_enabled_only_bad_json_fail_closed(self):
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?,?)",
            (f"{WATCH_KEY_PREFIX}ses_bad", "{broken"))
        with self.assertRaises(LedgerError) as cm:
            self.mil.list_managed_watches(enabled_only=True)
        self.assertEqual(cm.exception.code, "WATCH_STATE_CORRUPT")
        with self.assertRaises(LedgerError) as cm:
            self.mil.list_managed_watches()
        self.assertEqual(cm.exception.code, "WATCH_STATE_CORRUPT")

    def _age_obs(self, condition="msg-a,msg-b", *, age=5000, aged_out=True,
                 trusted=True):
        return {"aged_out": aged_out, "trusted": trusted,
                "pending_condition_key": condition,
                "oldest_age_ms": age, "oldest_message_id": "msg-a",
                "pending": 2}

    def test_aged_out_trusted_creates_persistent_owner_alert(self):
        self.mil.register_managed_watch(
            "ses_al", owner="obs-owner", project="p", max_pending_age_ms=1000,
            now=self.clock.now_ms())
        out = self.mil.record_managed_watch_observation(
            "ses_al", expected_revision=1,
            observation=self._age_obs(age=5000), now=self.clock.now_ms())
        self.assertTrue(out["recorded"])
        alerts = self.ledger.list_alerts(state="new", limit=50)
        self.assertEqual(len(alerts), 1)
        detail = alerts[0]["detail"]
        self.assertIn("ses_al", detail)
        self.assertIn("obs-owner", detail)
        self.assertIn("5000", detail)          # 最老年龄
        self.assertIn("flush-or-review", detail)
        self.assertNotIn("park payload", detail)
        # 同稳定条件、年龄变化：不每轮新造告警
        for age in (6000, 9000):
            self.mil.record_managed_watch_observation(
                "ses_al", expected_revision=1,
                observation=self._age_obs(age=age), now=self.clock.now_ms())
        self.assertEqual(len(self.ledger.list_alerts(state="new", limit=50)), 1)
        # ACK 后同一未变条件不再重复
        self.ledger.ack_alert(alerts[0]["id"], actor="operator")
        self.mil.record_managed_watch_observation(
            "ses_al", expected_revision=1,
            observation=self._age_obs(age=12000), now=self.clock.now_ms())
        self.assertEqual(len(self.ledger.list_alerts(state="new", limit=50)), 0)
        # 条件变化 → 可新告警
        self.mil.record_managed_watch_observation(
            "ses_al", expected_revision=1,
            observation=self._age_obs(condition="msg-c"), now=self.clock.now_ms())
        self.assertEqual(len(self.ledger.list_alerts(state="new", limit=50)), 1)

    def test_aged_out_untrusted_or_not_aged_no_alert(self):
        self.mil.register_managed_watch(
            "ses_al", owner="o", project="p", max_pending_age_ms=1,
            now=self.clock.now_ms())
        self.mil.record_managed_watch_observation(
            "ses_al", expected_revision=1,
            observation=self._age_obs(aged_out=True, trusted=False),
            now=self.clock.now_ms())
        self.mil.record_managed_watch_observation(
            "ses_al", expected_revision=1,
            observation=self._age_obs(aged_out=False, trusted=True),
            now=self.clock.now_ms())
        self.assertEqual(self.ledger.list_alerts(state="new", limit=50), [])

    def test_aged_out_alert_blocked_when_disabled_or_stale(self):
        self.mil.register_managed_watch(
            "ses_al", owner="o", project="p", max_pending_age_ms=1,
            now=self.clock.now_ms())
        self.mil.unregister_managed_watch(
            "ses_al", owner="o", expected_revision=1, now=self.clock.now_ms())
        out = self.mil.record_managed_watch_observation(
            "ses_al", expected_revision=2,
            observation=self._age_obs(), now=self.clock.now_ms())
        self.assertFalse(out["recorded"])
        self.assertEqual(out["reason"], "watch-disabled")
        self.assertEqual(self.ledger.list_alerts(state="new", limit=50), [])
        # 旧 revision 的观察：CAS 拒绝，不造告警
        self.mil.register_managed_watch(
            "ses_al", owner="o", project="p", max_pending_age_ms=1,
            now=self.clock.now_ms())
        with self.assertRaises(LedgerError) as cm:
            self.mil.record_managed_watch_observation(
                "ses_al", expected_revision=2,
                observation=self._age_obs(), now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "WATCH_REVISION_CONFLICT")
        self.assertEqual(self.ledger.list_alerts(state="new", limit=50), [])

    def test_aged_out_requires_bounded_condition_key(self):
        self.mil.register_managed_watch(
            "ses_al", owner="o", project="p", max_pending_age_ms=1,
            now=self.clock.now_ms())
        for bad in (None, 123, "", "x" * 1025):
            with self.subTest(bad=repr(bad)[:20]):
                with self.assertRaises(LedgerError) as cm:
                    self.mil.record_managed_watch_observation(
                        "ses_al", expected_revision=1,
                        observation=self._age_obs(condition=bad),
                        now=self.clock.now_ms())
                self.assertEqual(cm.exception.code, "INVALID_OBSERVATION")
        self.assertEqual(self.ledger.list_alerts(state="new", limit=50), [])

    def test_watch_corrupt_and_future_schema_fail_closed(self):
        key = f"{WATCH_KEY_PREFIX}ses_w"
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?,?)", (key, "{broken"),
        )
        with self.assertRaises(LedgerError) as cm:
            self.mil.get_managed_watch("ses_w")
        self.assertEqual(cm.exception.code, "WATCH_STATE_CORRUPT")
        with self.assertRaises(LedgerError):
            self.mil.unregister_managed_watch(
                "ses_w", owner="x", expected_revision=1,
                now=self.clock.now_ms())
        self.conn.execute(
            "UPDATE meta SET value=? WHERE key=?",
            (json.dumps({"schema": 99, "session_id": "ses_w"}),
             key),
        )
        with self.assertRaises(LedgerError) as cm:
            self.mil.list_managed_watches()
        self.assertEqual(cm.exception.code, "WATCH_STATE_CORRUPT")
        with self.assertRaises(LedgerError):
            self.mil.register_managed_watch(
                "ses_w", owner="o", project="p", max_pending_age_ms=1,
                now=self.clock.now_ms())


class TestExpireParks(ManagedBase):
    def _expired_park(self, session="ses_p", owner="o1", request_id="K"):
        return self.prepare(session, intent="park",
                            text="park payload", owner=owner,
                            request_id=request_id,
                            expires_at=self.clock.now_ms() - 1000,
                            flush_condition="idle")

    def test_expired_park_alerts_and_persists_condition(self):
        res = self._expired_park()
        out = self.mil.expire_managed_parks(now=self.clock.now_ms())
        self.assertEqual(out["alerts"], 1)
        self.assertIn(res["input"]["input_id"], out["expired"])
        data = self.fresh(res["input"])
        alarm = data["metadata"]["managed_input"]["expiry_alert"]
        self.assertEqual(alarm["kind"], "park-expired")
        self.assertEqual(alarm["owner"], "o1")
        # 只有一条 new 告警
        new_alerts = self.ledger.list_alerts(state="new")
        self.assertEqual(len(new_alerts), 1)
        self.assertEqual(new_alerts[0]["kind"], "managed-park-expired")
        # 未改动输入：不 cancel、不烧 budget
        self.assertEqual(data["admission_state"], "prepared")
        self.assertEqual(data["send_count"], 0)
        self.assertEqual(data["revision"], 1)

    def test_ack_then_same_condition_no_new_alert_each_round(self):
        res = self._expired_park()
        self.mil.expire_managed_parks(now=self.clock.now_ms())
        alert = self.ledger.list_alerts(state="new")[0]
        # ACK 后 alerts_active_dedupe(new) 失效
        acked = self.ledger.ack_alert(alert["id"], actor="human")
        self.assertEqual(acked["state"], "acked")
        for _ in range(3):
            out = self.mil.expire_managed_parks(now=self.clock.now_ms())
            self.assertEqual(out["alerts"], 0, "同一到期条件不得 ACK 后每轮再造告警")
        self.assertEqual(len(self.ledger.list_alerts(state="acked")), 1)
        # park 原责任仍 pending/expired-alert，alert ACK 不算 business ack / 改执行
        data = self.fresh(res["input"])
        self.assertEqual(data["admission_state"], "prepared")
        self.assertIsNone(data["delivered_at"])
        self.assertIsNone(data["executed_at"])

    def test_not_expired_ignored(self):
        self.prepare("ses_p", intent="park", text="p", owner="o1",
                     request_id="K", expires_at=self.clock.now_ms() + 10_000,
                     flush_condition="fc")
        out = self.mil.expire_managed_parks(now=self.clock.now_ms())
        self.assertEqual(out["alerts"], 0)
        self.assertEqual(out["expired"], [])
        data = self.mil.list_managed_inputs(session_id="ses_p")[0]
        self.assertIsNone(
            (self.mil.get_managed_input(data["input_id"])["metadata"]
             ["managed_input"]["expiry_alert"]))

    def test_delivered_park_ignored(self):
        res = self._expired_park()
        self.claim(res["input"], operation=OP_SEND)
        self.observe(res["input"], state="admission")
        self.observe(res["input"], state="delivery")
        out = self.mil.expire_managed_parks(now=self.clock.now_ms())
        self.assertEqual(out["alerts"], 0)
        self.assertEqual(out["expired"], [])
        self.assertEqual(self.fresh(res["input"])["admission_state"], "delivered")

    def test_limit_bounded_and_no_nested_transaction(self):
        for i in range(4):
            self._expired_park(session=f"ses_p{i}", owner=f"o{i}",
                               request_id=f"K{i}")
        out = self.mil.expire_managed_parks(now=self.clock.now_ms(), limit=2)
        self.assertEqual(out["checked"], 2)
        self.assertEqual(len(out["expired"]), 2)
        self.assertEqual(out["alerts"], 2)
        # 再次调用不嵌套事务、不崩；未处理的仍按限续处理
        out2 = self.mil.expire_managed_parks(now=self.clock.now_ms(), limit=4)
        self.assertEqual(out2["alerts"], 2)

    # ---- 反例（E 复核 2 阻断 + 中） --------------------------------
    def _alerted_ids(self) -> set[str]:
        ids: set[str] = set()
        for alert in self.ledger.list_alerts(state="new", limit=500):
            detail = alert.get("detail") or ""
            for row in self.mil.list_managed_inputs(limit=500):
                if row["input_id"] in detail:
                    ids.add(row["input_id"])
        return ids

    def test_same_owner_each_expired_input_gets_own_alert(self):
        first = self._expired_park(session="ses_x1", owner="same",
                                   request_id="X1")
        second = self._expired_park(session="ses_x2", owner="same",
                                    request_id="X2")
        out = self.mil.expire_managed_parks(now=self.clock.now_ms())
        self.assertEqual(out["alerts"], 2, "同owner不得聚合吞掉第二个输入的责任")
        alerted = self._alerted_ids()
        self.assertIn(first["input"]["input_id"], alerted)
        self.assertIn(second["input"]["input_id"], alerted)
        before = len(self.ledger.list_alerts(state="new", limit=500))
        self.mil.expire_managed_parks(now=self.clock.now_ms())
        self.assertEqual(len(self.ledger.list_alerts(state="new", limit=500)),
                         before)

    def test_marked_prefix_does_not_starve_later_limit1(self):
        first = self._expired_park(session="ses_y1", owner="owner-y1",
                                   request_id="Y1")
        second = self._expired_park(session="ses_y2", owner="owner-y2",
                                    request_id="Y2")
        self.mil.expire_managed_parks(now=self.clock.now_ms(), limit=1)
        self.mil.expire_managed_parks(now=self.clock.now_ms(), limit=1)
        alerted = self._alerted_ids()
        self.assertIn(first["input"]["input_id"], alerted)
        self.assertIn(second["input"]["input_id"], alerted,
                      "已标前缀不得永久占据 limit=1 候选窗")

    def test_not_expired_prefix_does_not_starve_expired_limit1(self):
        not_due = self.prepare(
            "ses_z1", intent="park", text="x", owner="owner-z1",
            request_id="Z1", expires_at=self.clock.now_ms() + 1_000_000,
            flush_condition="fc")
        due = self._expired_park(session="ses_z2", owner="owner-z2",
                                 request_id="Z2")
        self.mil.expire_managed_parks(now=self.clock.now_ms(), limit=1)
        alerted = self._alerted_ids()
        self.assertIn(due["input"]["input_id"], alerted,
                      "未到期前缀不得挤掉真正到期责任")
        self.assertNotIn(not_due["input"]["input_id"], alerted)

    def test_park_missing_fields_fail_closed(self):
        res = self._expired_park()
        row = self.conn.execute(
            "SELECT metadata FROM session_inputs WHERE input_id=?",
            (res["input"]["input_id"],)).fetchone()
        meta = json.loads(row["metadata"])
        meta["managed_input"]["flush_condition"] = None
        self.conn.execute(
            "UPDATE session_inputs SET metadata=? WHERE input_id=?",
            (json.dumps(meta, ensure_ascii=False), res["input"]["input_id"]))
        with self.assertRaises(LedgerError) as cm:
            self.mil.expire_managed_parks(now=self.clock.now_ms())
        self.assertEqual(cm.exception.code, "MANAGED_META_CORRUPT")


if __name__ == "__main__":
    unittest.main(verbosity=2)
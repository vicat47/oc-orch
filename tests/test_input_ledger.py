#!/usr/bin/env python3
"""S2 输入回执表回归（B 组 round3）：发送权仲裁、终态守护、nudge 预算与安全退役。

覆盖 design D2/D3 与协调者 R1–R8 / 活性边界的台账层正确行为：
- reserve/finalize/mark_possible 以 owner + expected_revision CAS 仲裁发送权；
  finalize 后不可变 finalized 快照（同 owner 并发也不能暗改正文/成员）；
- 终态守护：unknown/defer/retry/confirm/possible 不得重开已 cancelled/released/
  被 nudge 后继替换的输入；delivered 确认拒绝已取消输入；
- 批次受理证据原子补证全部成员且幂等；同物理 message 的晚到 mark_sent 绝不
  重复计 attempts；ACK 竞态保持终态；
- nudge：仅确证 delivered（delivery 证据门）；重复 nudge 幂等返回既有后继；
  跨代绝对预算（nudge_count < max_nudges），不能靠新 ID 无限重置；
- 发送前重校验冻结成员（ACK/人工控制/任务关闭）→ 拒发；
- release_input_slot 支持 delivered/cancelled + 全成员终结的安全退役（保住
  delivery 事实/原 ID），目标不再永久被占；未知/未决不因仅 ACK 退役。

只使用真临时 SQLite（WAL）+ 可控时钟，不连接真实服务、不调用模型。
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))
sys.path.insert(0, os.path.dirname(HERE))  # 便于直接运行 python3 tests/test_x.py

from oc_orch.db import init, db_path
from oc_orch.ledger import Ledger, LedgerError
from tests.fakes import FakeClock


def _body_of(members: list[dict]) -> str:
    """纯 body 派生（text_factory 用例）：只允许由传入成员派生正文。"""
    return "wake members: " + ", ".join(m["id"] for m in sorted(
        members, key=lambda m: m["id"]))


class InputLedgerBase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-input-")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.clock = FakeClock(wall_ms=1_000_000)
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)

    def enqueue(self, key: str, *, kind: str = "stall") -> dict:
        return self.ledger.enqueue_notification(
            kind=kind, target_session="ses_c", dedupe_key=key,
            payload={"summary": key}, project="p")

    def reserve(self, *keys: str, target: str = "ses_c", owner: str = "owner-a",
                **kw) -> dict:
        ids = [self.enqueue(k)["id"] for k in keys]
        return self.ledger.reserve_target_input(
            target, notification_ids=ids, owner=owner, **kw)

    def _fresh(self, res: dict) -> dict:
        inp = res["input"]
        return self.ledger.get_input(inp["input_id"]) or inp

    def finalize(self, res: dict, final_ids, owner: str | None = None,
                 expected_revision=None, **kw) -> dict:
        inp = self._fresh(res)
        rev = expected_revision if expected_revision is not None else inp["revision"]
        return self.ledger.finalize_input(
            inp["input_id"], final_notification_ids=final_ids, text_factory=_body_of,
            owner=owner or (inp.get("claim_owner") or "owner-a"),
            expected_revision=rev, **kw)

    def possible(self, res: dict, owner: str | None = None,
                 expected_revision=None, **kw) -> dict:
        inp = self._fresh(res)
        rev = expected_revision if expected_revision is not None else inp["revision"]
        return self.ledger.mark_input_possible(
            inp["input_id"],
            owner=owner or (inp.get("claim_owner") or "owner-a"),
            expected_revision=rev, **kw)

    def delivered(self, res: dict) -> dict:
        out = self.possible(res)
        self.ledger.confirm_input_admitted(res["input"]["input_id"])
        return self.ledger.confirm_input_delivered(res["input"]["input_id"])


class TestTargetSlotUniqueness(InputLedgerBase):
    def test_reserve_creates_single_outstanding_with_persisted_candidate(self):
        n = self.enqueue("one")
        res = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[n["id"]], intent="execute", owner="o1")
        self.assertTrue(res["created"])
        inp = res["input"]
        self.assertTrue(inp["outstanding"])
        self.assertEqual(inp["admission_state"], "prepared")
        self.assertEqual(inp["revision"], 1)
        self.assertEqual(inp["claim_owner"], "o1")
        self.assertTrue(inp["message_id"].startswith("msg_ocorch"),
                        "候选 message_id 必须在任何 POST 之前持久化")
        self.assertEqual(inp["notification_ids"], [n["id"]])
        res2 = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[n["id"]], owner="o1")
        self.assertFalse(res2["created"])
        self.assertTrue(res2["held"])
        self.assertEqual(res2["input"]["input_id"], inp["input_id"])
        self.assertEqual(len(self.ledger.list_pending_inputs(
            target_session="ses_c", outstanding=True)), 1)

    def test_two_connections_reserve_same_target_only_one_created(self):
        def open_ledger():
            c = sqlite3.connect(db_path(self.home), isolation_level=None, timeout=15)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA busy_timeout=15000")
            return c

        barrier = threading.Barrier(2)
        results: dict[str, object] = {}

        def work(name: str) -> None:
            conn = open_ledger()
            try:
                ledger = Ledger(conn, self.home, clock=self.clock)
                n = ledger.enqueue_notification(
                    kind="stall", target_session="ses_race", dedupe_key=name,
                    payload={"s": name}, project="p")
                barrier.wait()
                results[name] = ledger.reserve_target_input(
                    "ses_race", notification_ids=[n["id"]], owner=name)
            except Exception as exc:  # noqa: BLE001 - 记录任意失败供断言
                results[name] = exc
            finally:
                conn.close()

        threads = [threading.Thread(target=work, args=("a",)),
                   threading.Thread(target=work, args=("b",))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        created = [r for r in results.values()
                   if isinstance(r, dict) and r["created"]]
        self.assertEqual(len(created), 1, f"结果 {results}")
        self.assertEqual(len(self.ledger.list_pending_inputs(
            target_session="ses_race", outstanding=True)), 1)

    def test_partial_unique_index_rejects_second_outstanding_row(self):
        self.enqueue("raw")
        self.ledger.reserve_target_input("ses_raw", notification_ids=["x"],
                                         owner="o")
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.conn.execute(
                "INSERT INTO session_inputs(input_id, session_id, message_id, "
                "source, intent, outstanding, revision, notification_ids, "
                "admission_state, prepared_at, created_at, updated_at) "
                "VALUES('z','ses_raw','m9','automatic_wake','execute',1,1,'[]',"
                "'prepared',1,1,1)")
            self.ledger.conn.commit()


class TestSendArbitration(InputLedgerBase):
    def test_other_owner_has_no_send_authority_on_prepared_slot(self):
        res_a = self.reserve("shared", owner="owner-a")
        # 第二发送者（不同 owner）：held 但无发送权，不合并不暗改
        res_b = self.ledger.reserve_target_input(
            "ses_c", notification_ids=["other"], owner="owner-b")
        self.assertTrue(res_b["held"])
        self.assertFalse(res_b.get("authority", True), "无发送权")
        self.assertFalse(res_b["merged"])
        self.assertEqual(self.ledger.get_input(res_a["input"]["input_id"])[
            "notification_ids"], [self.enqueue("shared")["id"] if False else
            res_a["input"]["notification_ids"][0]])
        with self.assertRaises(LedgerError) as e1:
            self.finalize(res_b, [res_a["input"]["notification_ids"][0]],
                          owner="owner-b")
        self.assertEqual(e1.exception.code, "INPUT_OWNER_MISMATCH")
        with self.assertRaises(LedgerError) as e2:
            self.possible(res_b, owner="owner-b")
        self.assertEqual(e2.exception.code, "INPUT_OWNER_MISMATCH")

    def test_finalize_uses_revision_cas_against_concurrent_change(self):
        res = self.reserve("cas-finalize")
        inp = res["input"]
        ok = self.finalize(res, inp["notification_ids"])
        self.assertTrue(ok["changed"])
        self.assertEqual(ok["input"]["revision"], 2)
        # 旧 revision 的并发 finalize → conflict
        with self.assertRaises(LedgerError) as e1:
            self.finalize({"input": inp}, inp["notification_ids"],
                          expected_revision=1)
        self.assertEqual(e1.exception.code, "INPUT_REVISION_CONFLICT")

    def test_mark_possible_uses_revision_cas(self):
        res = self.reserve("cas-possible")
        inp = res["input"]
        self.finalize(res, inp["notification_ids"])
        # finalize 后 revision=2；用过期 revision=1 标记 possible → conflict
        with self.assertRaises(LedgerError) as e1:
            self.possible({"input": inp}, expected_revision=1)
        self.assertEqual(e1.exception.code, "INPUT_REVISION_CONFLICT")
        out = self.possible(res)  # 新 revision 2 → 成功（rev→3）
        self.assertTrue(out["changed"])
        # 已 possible 的重复标记是幂等 no-op（不重复计数），非 rev conflict
        again = self.possible(res)
        self.assertFalse(again["changed"])
        self.assertEqual(again["input"]["send_count"], 1)

    def test_mark_possible_persists_and_counts_once(self):
        res = self.reserve("mk-possible")
        inp = res["input"]
        self.finalize(res, inp["notification_ids"])
        out = self.possible(res)
        self.assertTrue(out["changed"])
        self.assertEqual(out["input"]["admission_state"], "possible")
        self.assertEqual(out["input"]["send_count"], 1)
        self.assertEqual(out["input"]["message_id"], inp["message_id"])
        again = self.possible(res)
        self.assertFalse(again["changed"], "重复标记不重复计数")
        self.assertEqual(again["input"]["send_count"], 1)

    def test_mark_possible_blocks_release_after_send_window(self):
        res = self.reserve("guard")
        self.possible(res)
        release = self.ledger.release_input_slot(res["input"]["input_id"],
                                                 reason="x")
        self.assertFalse(release["released"])
        self.assertEqual(release["input"]["admission_state"], "possible")


class TestBatchFinalize(InputLedgerBase):
    def test_finalize_body_only_from_final_claim_set(self):
        stale = self.enqueue("stale-1")
        valid = self.enqueue("valid-1")
        res = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[stale["id"], valid["id"]], owner="o")
        inp = res["input"]
        self.ledger.mark_notification_dead(stale["id"], error="superseded",
                                           owner="t")
        seen: list[list[dict]] = []

        def tf(members: list[dict]) -> str:
            seen.append(list(members))
            return _body_of(members)

        out = self.ledger.finalize_input(
            inp["input_id"], final_notification_ids=[valid["id"]],
            text_factory=tf, owner="o", expected_revision=1)
        self.assertTrue(out["changed"])
        self.assertEqual(out["input"]["notification_ids"], [valid["id"]])
        self.assertIn(valid["id"], out["text"])
        self.assertNotIn(stale["id"], out["text"], "被剔除成员不能泄漏进正文")
        self.assertEqual({m["id"] for m in seen[0]}, {valid["id"]},
                         "text_factory 只能见最终有效集合")

    def test_finalize_is_immutable_after_send(self):
        res = self.reserve("frozen-a")
        inp = res["input"]
        self.finalize(res, inp["notification_ids"])
        self.possible(res)
        out = self.finalize(res, inp["notification_ids"],
                            expected_revision=inp["revision"])
        self.assertFalse(out["changed"], "已发送输入正文不可暗改")
        self.assertEqual(out["reason"], "already-final")


class TestBatchConfirm(InputLedgerBase):
    def test_one_admission_event_confirms_all_batch_members_once(self):
        rows = [self.enqueue(f"batch-{i}") for i in range(3)]
        res = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[r["id"] for r in rows], owner="o")
        inp = res["input"]
        self.possible(res)
        out = self.ledger.confirm_input_admitted(
            inp["input_id"], evidence={"via": "inbox.enqueued"},
            now=self.clock.now_ms() + 10)
        self.assertEqual(out["confirmed"], 3)
        for r in rows:
            final = self.ledger.get_notification(r["id"])
            self.assertEqual(final["state"], "sent")
            self.assertEqual(final["attempts"], 1, "每条只计一次物理发送")
            self.assertEqual(final["message_id"], inp["message_id"])
        again = self.ledger.confirm_input_admitted(
            inp["input_id"], evidence={"via": "duplicate"},
            now=self.clock.now_ms() + 20)
        self.assertEqual(again["confirmed"], 0)
        for r in rows:
            self.assertEqual(self.ledger.get_notification(r["id"])["attempts"], 1)

    def test_late_mark_sent_same_message_never_duplicate_count(self):
        rows = [self.enqueue(f"idem-{i}") for i in range(2)]
        res = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[r["id"] for r in rows], owner="o")
        inp = res["input"]
        self.possible(res)
        self.ledger.confirm_input_admitted(inp["input_id"])
        nid = rows[0]["id"]
        before = self.ledger.get_notification(nid)
        # 同物理 message 的晚到确认（旧 mark_sent 路径）绝不重复计 attempts（R5）
        self.ledger.mark_notification_sent(
            nid, message_id=inp["message_id"], next_attempt_at=1000, owner="o")
        after = self.ledger.get_notification(nid)
        self.assertEqual(after["attempts"], before["attempts"])
        self.assertEqual(after["state"], "sent")

    def test_ack_race_keeps_acked_member_and_confirms_others(self):
        rows = [self.enqueue(f"race-{i}") for i in range(3)]
        res = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[r["id"] for r in rows], owner="o")
        inp = res["input"]
        self.possible(res)
        self.ledger.ack_notifications([rows[0]["id"]], actor="concurrent-web")
        out = self.ledger.confirm_input_admitted(
            inp["input_id"], now=self.clock.now_ms() + 10)
        self.assertEqual(out["confirmed"], 2)
        acked = self.ledger.get_notification(rows[0]["id"])
        self.assertEqual(acked["state"], "acked", "已 ACK 项保持终态")
        self.assertEqual(acked["attempts"], 0)
        for r in rows[1:]:
            self.assertEqual(self.ledger.get_notification(r["id"])["state"], "sent")

    def test_batch_confirm_keeps_dead_member_dead(self):
        rows = [self.enqueue(f"dead-batch-{i}") for i in range(2)]
        self.ledger.mark_notification_dead(rows[1]["id"], error="exhausted",
                                           owner="t")
        res = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[r["id"] for r in rows], owner="o")
        inp = res["input"]
        out = self.ledger.confirm_input_admitted(inp["input_id"],
                                                 now=self.clock.now_ms() + 10)
        self.assertEqual(out["confirmed"], 1)
        self.assertEqual(self.ledger.get_notification(rows[1]["id"])["state"],
                         "dead", "已 dead 成员不被受理证据复活")


class TestTerminalGuards(InputLedgerBase):
    def test_late_unknown_cannot_resurrect_cancelled(self):
        res = self.reserve("late-unknown")
        inp = res["input"]
        self.possible(res)
        self.ledger.confirm_input_admitted(inp["input_id"])
        self.ledger.record_input_cancelled(
            session_id="ses_c", message_id=inp["message_id"], reason="deleted")
        self.assertEqual(self.ledger.get_input(inp["input_id"])["admission_state"],
                         "cancelled")
        with self.assertRaises(LedgerError) as e1:
            self.ledger.record_input_unknown(inp["input_id"], reason="late")
        self.assertEqual(e1.exception.code, "INPUT_TERMINAL")
        with self.assertRaises(LedgerError) as e2:
            self.ledger.retry_input(inp["input_id"], next_reconcile_at=1,
                                    reason="late")
        self.assertEqual(e2.exception.code, "INPUT_TERMINAL")
        with self.assertRaises(LedgerError) as e3:
            self.possible({"input": inp})
        self.assertEqual(e3.exception.code, "INPUT_TERMINAL")
        self.assertEqual(self.ledger.get_input(inp["input_id"])["admission_state"],
                         "cancelled", "晚到证据不得把已取消输入翻回 unknown/复活")

    def test_confirm_delivered_refuses_cancelled_input(self):
        res = self.reserve("cancel-delivered")
        inp = res["input"]
        self.possible(res)
        self.ledger.confirm_input_admitted(inp["input_id"])
        self.ledger.record_input_cancelled(
            session_id="ses_c", message_id=inp["message_id"], reason="cancel")
        with self.assertRaises(LedgerError) as e:
            self.ledger.confirm_input_delivered(inp["input_id"],
                                                evidence={"via": "user-message"})
        self.assertEqual(e.exception.code, "INPUT_TERMINAL")
        self.assertEqual(self.ledger.get_input(inp["input_id"])["admission_state"],
                         "cancelled")

    def test_reviewed_input_rejects_further_writes(self):
        res = self.reserve("replaced-guard")
        inp = res["input"]

        def to_delivered(r):
            self.possible(r)
            c = self.ledger.confirm_input_admitted(r["input"]["input_id"])
            return self.ledger.confirm_input_delivered(r["input"]["input_id"])

        to_delivered(res)
        out = self.ledger.nudge_input(inp["input_id"], text_factory=_body_of,
                                      owner="o")
        self.assertTrue(out["created"])
        with self.assertRaises(LedgerError) as e:
            self.ledger.record_input_unknown(inp["input_id"], reason="late")
        self.assertEqual(e.exception.code, "INPUT_SUPERSEDED")


class TestCancelAndReceipt(InputLedgerBase):
    def test_cancel_frees_slot_without_acking_business(self):
        n = self.enqueue("cancel-me")
        res = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[n["id"]], owner="o")
        inp = res["input"]
        self.possible(res)
        out = self.ledger.record_input_cancelled(
            session_id="ses_c", message_id=inp["message_id"], reason="deleted")
        self.assertTrue(out["matched"])
        self.assertFalse(out["business_acked"], "取消不能隐式 ACK 业务")
        self.assertTrue(out.get("notifications_untouched"))
        self.assertEqual(out["input"]["admission_state"], "cancelled")
        self.assertFalse(out["input"]["outstanding"])
        biz = self.ledger.get_notification(n["id"])
        self.assertEqual(biz["state"], "pending", "业务通知状态保持不变")
        self.assertIsNone(biz["acked_at"])
        res2 = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[n["id"]], owner="o")
        self.assertTrue(res2["created"])

    def test_cancel_with_wrong_session_does_not_match(self):
        res = self.reserve("cancel-wrong")
        inp = res["input"]
        out = self.ledger.record_input_cancelled(
            session_id="ses_foreign", message_id=inp["message_id"])
        self.assertFalse(out["matched"])
        self.assertTrue(self.ledger.get_input(inp["input_id"])["outstanding"])

    def test_receipt_is_session_scoped(self):
        res = self.reserve("receipt-session")
        inp = res["input"]
        self.assertEqual(
            self.ledger.input_for_receipt("ses_c", inp["message_id"])["input_id"],
            inp["input_id"])
        self.assertIsNone(
            self.ledger.input_for_receipt("ses_foreign", inp["message_id"]))


class TestUnknownIdentity(InputLedgerBase):
    def test_unknown_keeps_original_id_and_slot(self):
        res = self.reserve("unknown-identity")
        inp = res["input"]
        self.possible(res)
        out = self.ledger.record_input_unknown(
            inp["input_id"], reason="transport-lost",
            next_reconcile_at=self.clock.now_ms() + 60_000,
            reconcile_deadline=self.clock.now_ms() + 3600_000)
        u = out["input"]
        self.assertEqual(u["admission_state"], "unknown")
        self.assertEqual(u["message_id"], inp["message_id"])
        self.assertTrue(u["outstanding"])
        res2 = self.ledger.reserve_target_input(
            "ses_c", notification_ids=["other"], owner="o")
        self.assertFalse(res2["created"])
        self.assertEqual(res2["input"]["input_id"], inp["input_id"])

    def test_retry_after_unknown_reuses_same_identity_and_counts(self):
        res = self.reserve("retry-same-id")
        inp = res["input"]
        self.possible(res)
        rev_after_first = self.ledger.get_input(inp["input_id"])["revision"]
        self.ledger.record_input_unknown(inp["input_id"], reason="lost")
        again = self.possible({"input": self.ledger.get_input(inp["input_id"])},
                              expected_revision=rev_after_first)
        self.assertTrue(again["changed"])
        self.assertEqual(again["input"]["message_id"], inp["message_id"])
        self.assertEqual(again["input"]["send_count"], 2)
        self.assertEqual(again["input"]["retry_count"], 1)


class TestMembersRevalidation(InputLedgerBase):
    def test_finalize_refuses_terminal_members(self):
        res = self.reserve("term-final", "term-final-2")
        inp = res["input"]
        first = inp["notification_ids"][0]
        self.ledger.ack_notifications([first], actor="web")
        with self.assertRaises(LedgerError) as e:
            self.finalize(res, inp["notification_ids"])
        self.assertEqual(e.exception.code, "MEMBERS_NOT_SENDABLE")
        self.assertEqual(self.ledger.get_input(inp["input_id"])["admission_state"],
                         "prepared", "拒发时不暗改输入状态")

    def test_mark_possible_refuses_when_members_resolved_after_finalize(self):
        res = self.reserve("resolved-after")
        inp = res["input"]
        self.finalize(res, inp["notification_ids"])
        # 冻结后业务先被 ACK：标记 possible 前必须复核，变化时拒发（R6）
        self.ledger.ack_notifications([inp["notification_ids"][0]], actor="web")
        out = self.possible({"input": self.ledger.get_input(inp["input_id"]),
                             "revision": 2})
        self.assertFalse(out["changed"], "成员已终结不得再发送")
        self.assertIn("members-changed", out.get("reason", ""))
        self.assertEqual(self.ledger.get_input(inp["input_id"])["admission_state"],
                         "prepared")


class TestNudge(InputLedgerBase):
    def test_nudge_after_delivered_atomically_replaces_slot_keeping_history(self):
        res = self.reserve("nudge-target")
        inp = res["input"]
        self.delivered(res)
        out = self.ledger.nudge_input(inp["input_id"], text_factory=_body_of,
                                      owner="o", now=self.clock.now_ms() + 100)
        self.assertTrue(out["created"])
        new = out["input"]
        self.assertEqual(new["prior_input_id"], inp["input_id"])
        self.assertTrue(new["outstanding"])
        self.assertEqual(new["intent"], "nudge")
        self.assertEqual(new["nudge_count"], 1)
        self.assertEqual(new["notification_ids"], inp["notification_ids"])
        old = self.ledger.get_input(inp["input_id"])
        self.assertFalse(old["outstanding"])
        self.assertEqual(old["replaced_by"], new["input_id"])

    def test_nudge_requires_delivery_evidence_not_http_admission(self):
        res = self.reserve("nudge-delivery-evidence")
        inp = res["input"]
        self.possible(res)
        self.ledger.confirm_input_admitted(inp["input_id"])
        self.assertEqual(self.ledger.get_input(inp["input_id"])["admission_state"],
                         "admitted")
        with self.assertRaises(LedgerError) as e:
            self.ledger.nudge_input(inp["input_id"], text_factory=_body_of,
                                          owner="o")
        self.assertIn("NUDGE", e.exception.code)
        self.ledger.confirm_input_delivered(inp["input_id"],
                                            evidence={"via": "user-message"})
        out = self.ledger.nudge_input(inp["input_id"], text_factory=_body_of,
                                      owner="o")
        self.assertTrue(out["created"])

    def test_nudge_forbids_unknown_or_pending_identity(self):
        res = self.reserve("nudge-forbidden")
        inp = res["input"]
        with self.assertRaises(LedgerError):
            self.ledger.nudge_input(inp["input_id"], text_factory=_body_of,
                                    owner="o")
        self.possible(res)
        self.ledger.confirm_input_admitted(inp["input_id"])
        self.ledger.record_input_unknown(inp["input_id"], reason="lost")
        with self.assertRaises(LedgerError):
            self.ledger.nudge_input(inp["input_id"], text_factory=_body_of,
                                    owner="o")

    def test_double_nudge_returns_existing_successor_idempotently(self):
        res = self.reserve("double-nudge")
        inp = res["input"]
        self.delivered(res)
        first = self.ledger.nudge_input(inp["input_id"], text_factory=_body_of,
                                        owner="o")
        self.assertTrue(first["created"])
        second = self.ledger.nudge_input(inp["input_id"], text_factory=_body_of,
                                         owner="o")
        self.assertFalse(second["created"])
        self.assertEqual(second["reason"], "successor-exists")
        self.assertEqual(second["input"]["input_id"], first["input"]["input_id"])
        self.assertEqual(len(self.ledger.list_pending_inputs(
            target_session="ses_c", outstanding=True)), 1,
            "重复 nudge 不得新建第二物理输入")

    def test_nudge_budget_is_bounded_across_generations(self):
        res = self.reserve("nudge-budget")
        cur = res
        for _ in range(3):
            inp = cur["input"]
            self.delivered(cur)
            out = self.ledger.nudge_input(inp["input_id"],
                                          text_factory=_body_of, owner="o")
            self.assertTrue(out["created"], "前 3 代 nudge 应成功")
            cur = {"input": out["input"]}
        # 第 4 次 nudge：跨代绝对预算耗尽（R7：不能靠新 ID 无限重置）
        with self.assertRaises(LedgerError) as e:
            self.delivered(cur)
            self.ledger.nudge_input(cur["input"]["input_id"],
                                    text_factory=_body_of, owner="o")
        self.assertEqual(e.exception.code, "NUDGE_BUDGET_EXHAUSTED")


class TestReleaseAndLiveness(InputLedgerBase):
    def test_defer_and_release_prepared_slot(self):
        res = self.reserve("defer-release")
        inp = res["input"]
        deferred = self.ledger.defer_input(
            inp["input_id"], next_reconcile_at=self.clock.now_ms() + 60_000,
            reason="cooldown")
        self.assertTrue(deferred["input"]["outstanding"])
        released = self.ledger.release_input_slot(
            inp["input_id"], reason="proven-unadmitted")
        self.assertTrue(released["released"])
        self.assertFalse(released["input"]["outstanding"])
        self.assertEqual(released["input"]["admission_state"], "released")

    def test_delivered_acked_wake_retires_without_permanently_holding_target(self):
        n = self.enqueue("ack-all")
        res = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[n["id"]], owner="o")
        inp = res["input"]
        self.delivered(res)
        # 全部业务成员已 ACK：既不能 nudge（无未 ACK）也不应永久占目标
        self.ledger.ack_notifications([n["id"]], actor="coordinator")
        out = self.ledger.release_input_slot(
            inp["input_id"], reason="all-acked")
        self.assertTrue(out["released"], "确证 delivered + 全成员终结可安全退役")
        self.assertEqual(out["input"]["admission_state"], "released")
        self.assertFalse(out["input"]["outstanding"])
        # 退役保留 delivery 事实与原 message ID
        self.assertEqual(out["input"]["message_id"], inp["message_id"])
        # 目标不再被占：第二结果可重新预留（活性边界）
        res2 = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[n["id"]], owner="o")
        self.assertTrue(res2["created"])

    def test_unknown_not_retired_by_ack_alone(self):
        n = self.enqueue("ack-alone")
        res = self.ledger.reserve_target_input(
            "ses_c", notification_ids=[n["id"]], owner="o")
        inp = res["input"]
        self.possible(res)
        self.ledger.record_input_unknown(inp["input_id"], reason="lost")
        # 未知输入绝不能只凭 ACK/空 inbox 退役
        release = self.ledger.release_input_slot(inp["input_id"], reason="ack")
        self.assertFalse(release["released"])
        self.assertTrue(self.ledger.get_input(inp["input_id"])["outstanding"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
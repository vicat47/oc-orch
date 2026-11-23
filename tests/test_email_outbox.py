#!/usr/bin/env python3
"""EmailOutbox 持久性：meta 命名空间 + 严格 schema + 短事务。

用真临时 SQLite（WAL、双连接、FakeClock）覆盖：事件身份去重、claim 唯一发送
权与持久 attempt cap（M09/M11）、崩溃窗口（possible 持久）、result CAS、
回执查询 claim（网络前原子预算，双观察者/崩溃有限）、唯一匹配 CAS、恢复抑制、
陈旧认领（短事务重读，不覆盖晚结果）、损坏/不变量记录 fail-closed、预算跨重启。
不触 runtime/真实网络。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.adapters.sqlite_db import init, meta_get, meta_set  # noqa: E402
from oc_orch.adapters.email_outbox import (  # noqa: E402
    MAIL_KEY_PREFIX,
    MAIL_SCHEMA,
    MAIL_SCHEMA_KEY,
    EmailOutbox,
    MailOutboxStateError,
)
from oc_orch.domain.operator_email import (  # noqa: E402
    KIND_FIRST,
    KIND_UPDATE,
    STATUS_ACCEPTED,
    STATUS_AUTH_BLOCKED,
    STATUS_CLAIMED,
    STATUS_EXPIRED_SUPPRESSED,
    STATUS_PENDING,
    STATUS_PENDING_AUTH,
    STATUS_PERMANENT_FAILED,
    STATUS_RETRY_WAIT,
    STATUS_SUBMIT_UNKNOWN,
)
from tests.fakes import FakeClock  # noqa: E402


def _open_wal(home):
    conn = sqlite3.connect(os.path.join(home, "ledger.db"), timeout=15,
                           isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def _content(subject, body):
    return hashlib.sha256(f"{subject}\x00{body}".encode("utf-8")).hexdigest()


def _payload(**over):
    subject = "[oc-orch:proj-a] 需要人工: manual-step#inc-1 [ev1]"
    body = "事件 ID: ev1\n项目: proj-a"
    base = dict(
        item_id="ev1", base_key="base1", event_key="ev1",
        project="proj-a", incident_id="inc-1", incident_type="manual-step",
        kind=KIND_FIRST, condition_version=1, escalation_level=0,
        recipient_ref="op-primary", sender_ref="oc-ops",
        recipient_fingerprint="fp-recipient-a", sender_fingerprint="fp-sender-a",
        subject=subject, body=body, content_hash=_content(subject, body),
        status=STATUS_PENDING, first_seen_at=1_000_000, observed_at=1_000_000,
        summary={"blocked_reason": "x"}, max_attempts=3, receipt_limit=3,
        deadline_at=None, now=1_000_000,
    )
    base.update(over)
    if "subject" in over or "body" in over or "content_hash" not in over:
        base["content_hash"] = _content(base["subject"], base["body"])
    return base


class OutboxBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="outbox-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.outbox = EmailOutbox(self.conn, self.clock)

    def upsert(self, **over):
        return self.outbox.upsert_item(**_payload(**over))

    def raw(self, item_id: str) -> str | None:
        return meta_get(self.conn, f"{MAIL_KEY_PREFIX}{item_id}")

    def now(self) -> int:
        return self.clock.now_ms()


class TestUpsertDedupe(OutboxBase):
    def test_create_then_duplicate_observations(self):
        first = self.upsert()
        self.assertTrue(first["created"])
        self.assertEqual(meta_get(self.conn, MAIL_SCHEMA_KEY), str(MAIL_SCHEMA))
        self.clock.advance(wall=5_000)
        second = self.upsert(observed_at=1_005_000,
                             summary={"blocked_reason": "y"})
        self.assertFalse(second["created"])
        self.assertTrue(second["changed"])
        self.assertEqual(second["reason"], "duplicate-observation")
        rows = self.outbox.list_all()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["last_observed_at"], 1_005_000)
        self.assertEqual(row["summary"]["blocked_reason"], "y")
        self.assertEqual(row["revision"], 1)  # 观察不推进发送 revision

    def test_item_event_mismatch_rejected(self):
        out = self.upsert(event_key="other")
        self.assertFalse(out["created"])
        self.assertEqual(out["reason"], "item-event-mismatch")

    def test_pending_auth_promoted_refreshes_binding_fingerprints(self):
        self.upsert(status=STATUS_PENDING_AUTH,
                    recipient_fingerprint="fp-unbound",
                    sender_fingerprint="fp-unbound")
        promoted = self.upsert(status=STATUS_PENDING,
                               recipient_fingerprint="fp-recipient-a",
                               sender_fingerprint="fp-sender-a")
        self.assertFalse(promoted["created"])
        row = self.outbox.get_item("ev1")
        self.assertEqual(row["status"], STATUS_PENDING)
        self.assertEqual(row["recipient_fingerprint"], "fp-recipient-a")
        self.assertEqual(row["sender_fingerprint"], "fp-sender-a")
        self.assertEqual(row["revision"], 2)

    def test_different_kind_same_base_are_separate_items(self):
        self.upsert()
        upd = self.upsert(item_id="ev2", event_key="ev2", kind=KIND_UPDATE,
                          condition_version=2)
        self.assertTrue(upd["created"])
        self.assertEqual(len(self.outbox.items_for_base("base1")), 2)


class TestClaimAndResult(OutboxBase):
    def test_claim_is_single_winner_and_persists_possible(self):
        self.upsert()
        c = self.outbox.claim("ev1", owner="sidecar", now=self.now())
        self.assertTrue(c["allowed"])
        row = self.outbox.get_item("ev1")
        self.assertEqual(row["status"], STATUS_CLAIMED)
        self.assertEqual(row["claim_owner"], "sidecar")
        self.assertIsNotNone(row["possible_submitted_at"])  # 网络前已持久
        self.assertEqual(row["revision"], 2)
        again = self.outbox.claim("ev1", owner="other", now=self.now())
        self.assertFalse(again["allowed"])
        self.assertEqual(again["reason"], "not-claimable")

    def test_claim_attempt_cap_blocks_and_transitions_permanent(self):
        self.upsert(max_attempts=1)
        c = self.outbox.claim("ev1", owner="s", now=self.now())
        self.assertTrue(c["allowed"])
        self.outbox.mark_result("ev1", expected_revision=c["revision"],
                                change={"status": STATUS_RETRY_WAIT,
                                        "attempts": 1,
                                        "next_attempt_at": self.now()},
                                now=self.now())
        # 已耗尽持久 cap（attempts=1 >= max=1）→ 不再认领，转永久失败
        again = self.outbox.claim("ev1", owner="s2", now=self.now())
        self.assertFalse(again["allowed"])
        self.assertEqual(again["reason"], "attempt-budget-exhausted")
        self.assertEqual(self.outbox.get_item("ev1")["status"],
                         STATUS_PERMANENT_FAILED)

    def test_mark_result_cas_guards_concurrent_writer(self):
        self.upsert()
        c = self.outbox.claim("ev1", owner="s", now=self.now())
        revision = c["revision"]
        ok = self.outbox.mark_result(
            "ev1", expected_revision=revision,
            change={"status": STATUS_ACCEPTED, "attempts": 1,
                    "submitted_at": self.now()}, now=self.now())
        self.assertTrue(ok["applied"])
        self.assertEqual(self.outbox.get_item("ev1")["status"], STATUS_ACCEPTED)
        stale = self.outbox.mark_result(
            "ev1", expected_revision=revision,
            change={"status": STATUS_RETRY_WAIT}, now=self.now())
        self.assertFalse(stale["applied"])
        self.assertEqual(stale["reason"], "revision-changed")
        self.assertEqual(self.outbox.get_item("ev1")["status"], STATUS_ACCEPTED)

    def test_result_never_overwrites_frozen_content(self):
        self.upsert()
        before = self.outbox.get_item("ev1")
        c = self.outbox.claim("ev1", owner="s", now=self.now())
        self.outbox.mark_result("ev1", expected_revision=c["revision"],
                                change={"status": STATUS_ACCEPTED, "attempts": 1,
                                        "subject": "HACK", "body": "HACK",
                                        "submitted_at": self.now()},
                                now=self.now())
        after = self.outbox.get_item("ev1")
        self.assertEqual(after["subject"], before["subject"])
        self.assertEqual(after["body"], before["body"])

    def test_claim_not_due_denied(self):
        self.upsert()
        c = self.outbox.claim("ev1", owner="s", now=999_999)
        self.assertFalse(c["allowed"])
        self.assertEqual(c["reason"], "not-due")


class TestConcurrentClaim(OutboxBase):
    def test_two_connections_single_winner_atomic(self):
        self.upsert()
        conn_a, conn_b = _open_wal(self.home), _open_wal(self.home)
        self.addCleanup(conn_a.close)
        self.addCleanup(conn_b.close)
        ga = EmailOutbox(conn_a, self.clock)
        gb = EmailOutbox(conn_b, self.clock)
        barrier = Barrier(2, timeout=20)

        def claim(gate, owner):
            barrier.wait()
            return gate.claim("ev1", owner=owner, now=self.now())

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(claim, ga, "s-a")
            f2 = pool.submit(claim, gb, "s-b")
            r1, r2 = f1.result(timeout=25), f2.result(timeout=25)

        winners = [r for r in (r1, r2) if r["allowed"]]
        self.assertEqual(len(winners), 1, (r1, r2))
        final = self.outbox.get_item("ev1")
        self.assertEqual(final["status"], STATUS_CLAIMED)
        self.assertIsNotNone(final["possible_submitted_at"])


class TestCrashWindow(OutboxBase):
    def test_crash_after_claim_is_not_resent_after_restart(self):
        self.upsert()
        now = self.now()
        claimed = self.outbox.claim("ev1", owner="sidecar", now=now)
        self.assertTrue(claimed["allowed"])
        # 模拟在写回执前崩溃：不调用 mark_result，直接“重启”（新连接）。
        self.conn.close()
        self.conn = init(self.home)
        self.outbox = EmailOutbox(self.conn, self.clock)
        row = self.outbox.get_item("ev1")
        self.assertEqual(row["status"], STATUS_CLAIMED)
        self.assertIsNotNone(row["possible_submitted_at"])
        # 重启后不因崩溃被无条件再次认领/重发：list_due 不含 claimed。
        self.assertEqual(self.outbox.list_due(now=now + 60_000), [])
        # 陈旧认领转 submit-unknown（保留原身份等回执），仍不重发。
        self.clock.advance(wall=400_000)
        stale = self.outbox.stale_claimed(now=self.now(), stale_after_ms=300_000)
        self.assertEqual([s["item_id"] for s in stale], ["ev1"])
        self.assertEqual(self.outbox.get_item("ev1")["status"],
                         STATUS_SUBMIT_UNKNOWN)
        denied = self.outbox.claim("ev1", owner="other", now=self.now())
        self.assertFalse(denied["allowed"])
        self.assertEqual(denied["reason"], "not-claimable")

    def test_stale_claimed_does_not_clobber_late_result(self):
        # 网络晚结果先落库（accepted）→ stale_claimed 事务内重读后跳过。
        self.upsert()
        now = self.now()
        c = self.outbox.claim("ev1", owner="s", now=now)
        self.outbox.mark_result("ev1", expected_revision=c["revision"],
                                change={"status": STATUS_ACCEPTED, "attempts": 1,
                                        "submitted_at": now}, now=now)
        self.clock.advance(wall=400_000)
        stale = self.outbox.stale_claimed(now=self.now(), stale_after_ms=300_000)
        self.assertEqual(stale, [])  # 晚结果不被覆盖
        self.assertEqual(self.outbox.get_item("ev1")["status"], STATUS_ACCEPTED)
        self.assertEqual(self.outbox.get_item("ev1")["external_ref"], None)


class TestReceiptClaim(OutboxBase):
    def _unknown(self):
        self.upsert()
        c = self.outbox.claim("ev1", owner="s", now=self.now())
        self.outbox.mark_result("ev1", expected_revision=c["revision"],
                                change={"status": STATUS_SUBMIT_UNKNOWN,
                                        "attempts": 1}, now=self.now())
        return "ev1"

    def test_claim_receipt_consumes_budget_before_network(self):
        item_id = self._unknown()
        out = self.outbox.claim_receipt(item_id, owner="observer", now=self.now())
        self.assertTrue(out["allowed"])
        self.assertEqual(out["reason"], "receipt-claimed")
        row = self.outbox.get_item(item_id)
        self.assertEqual(row["receipt_checks"], 1)
        self.assertEqual(row["receipt_claim_owner"], "observer")
        self.assertEqual(row["receipt_revision"], 2)
        # 崩溃在查询后：预算已消耗（重启不还原，不能无限查询）
        self.conn.close()
        self.conn = init(self.home)
        self.outbox = EmailOutbox(self.conn, self.clock)
        self.assertEqual(self.outbox.get_item(item_id)["receipt_checks"], 1)

    def test_receipt_budget_exhaustion_atomic(self):
        item_id = self._unknown()
        self.outbox.claim_receipt(item_id, owner="a", now=self.now())
        self.outbox.claim_receipt(item_id, owner="b", now=self.now())
        self.outbox.claim_receipt(item_id, owner="c", now=self.now())  # 第 3 次
        third = self.outbox.claim_receipt(item_id, owner="d", now=self.now())
        self.assertFalse(third["allowed"])
        self.assertEqual(third["reason"], "receipt-check-exhausted")
        self.assertEqual(self.outbox.get_item(item_id)["receipt_checks"], 3)

    def test_receipt_budget_not_reset_by_config_change(self):
        # 预算冻结在行内：换政策（receipt_limit 更高）不能给旧行加预算。
        item_id = self._unknown()
        for _ in range(3):
            self.outbox.claim_receipt(item_id, owner="o", now=self.now())
        # 强行用更高 limit upsert 旧行 → 不改变既有行的 receipt_limit。
        self.upsert(observed_at=self.now(), receipt_limit=99)
        row = self.outbox.get_item(item_id)
        self.assertEqual(row["receipt_limit"], 3)
        denied = self.outbox.claim_receipt(item_id, owner="x", now=self.now())
        self.assertEqual(denied["reason"], "receipt-check-exhausted")

    def test_record_receipt_cas_rejects_stale(self):
        item_id = self._unknown()
        r1 = self.outbox.claim_receipt(item_id, owner="a", now=self.now())
        ok = self.outbox.record_receipt(
            item_id, expected_receipt_revision=r1["receipt_revision"],
            receipt_match="unique", external_ref="msg_1", now=self.now())
        self.assertTrue(ok["applied"])
        r2 = self.outbox.claim_receipt(item_id, owner="b", now=self.now())
        ok2 = self.outbox.record_receipt(
            item_id, expected_receipt_revision=r1["receipt_revision"],
            receipt_match="unique", external_ref="msg_stale", now=self.now())
        self.assertFalse(ok2["applied"])  # 晚到回执不覆盖
        self.assertEqual(ok2["reason"], "receipt-revision-changed")
        self.assertEqual(self.outbox.get_item(item_id)["external_ref"], "msg_1")

    def test_unique_does_not_overwrite_accepted_evidence(self):
        item_id = self._unknown()
        r1 = self.outbox.claim_receipt(item_id, owner="a", now=self.now())
        self.outbox.record_receipt(
            item_id, expected_receipt_revision=r1["receipt_revision"],
            receipt_match="unique", external_ref="msg_first", now=self.now())
        row = self.outbox.get_item(item_id)
        self.assertEqual(row["status"], STATUS_ACCEPTED)
        self.assertEqual(row["external_ref"], "msg_first")
        # accepted 行不再参与回执查询/晚到覆盖：claim 拒绝、证据不改写
        r2 = self.outbox.claim_receipt(item_id, owner="b", now=self.now())
        self.assertFalse(r2["allowed"])
        self.assertEqual(r2["reason"], "not-applicable")
        self.assertEqual(self.outbox.get_item(item_id)["external_ref"],
                         "msg_first")
        self.assertEqual(self.outbox.get_item(item_id)["status"], STATUS_ACCEPTED)

    def test_none_match_keeps_unknown(self):
        item_id = self._unknown()
        r1 = self.outbox.claim_receipt(item_id, owner="a", now=self.now())
        out = self.outbox.record_receipt(
            item_id, expected_receipt_revision=r1["receipt_revision"],
            receipt_match="none", external_ref=None, now=self.now())
        self.assertTrue(out["applied"])
        row = self.outbox.get_item(item_id)
        self.assertEqual(row["status"], STATUS_SUBMIT_UNKNOWN)
        self.assertEqual(row["receipt_match"], "none")
        self.assertEqual(row["receipt_checks"], 1)


class TestClaimReceiptConcurrent(OutboxBase):
    def test_two_observers_budget_atomic_serialized(self):
        # limit=1：双连接并发 claim_receipt 至多一个获得预算（查询预算原子）。
        item_id = self._unknown(receipt_limit=1)
        conn_a, conn_b = _open_wal(self.home), _open_wal(self.home)
        self.addCleanup(conn_a.close)
        self.addCleanup(conn_b.close)
        ga = EmailOutbox(conn_a, self.clock)
        gb = EmailOutbox(conn_b, self.clock)
        barrier = Barrier(2, timeout=20)

        def claim(out, owner):
            barrier.wait()
            return out.claim_receipt(item_id, owner=owner, now=self.now())

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(claim, ga, "o-a")
            f2 = pool.submit(claim, gb, "o-b")
            r1, r2 = f1.result(timeout=25), f2.result(timeout=25)

        winners = [r for r in (r1, r2) if r["allowed"]]
        self.assertEqual(len(winners), 1, (r1, r2))
        self.assertEqual(self.outbox.get_item(item_id)["receipt_checks"], 1)

    def _unknown(self, receipt_limit=3):
        self.upsert(receipt_limit=receipt_limit)
        c = self.outbox.claim("ev1", owner="s", now=self.now())
        self.outbox.mark_result("ev1", expected_revision=c["revision"],
                                change={"status": STATUS_SUBMIT_UNKNOWN,
                                        "attempts": 1}, now=self.now())
        return "ev1"


class TestRecoveryAndExpiry(OutboxBase):
    def test_expire_unsubmitted_skips_possibly_submitted(self):
        self.upsert()  # ev1 pending (possible None)
        self.upsert(item_id="ev2", event_key="ev2", kind=KIND_UPDATE,
                    condition_version=2, status=STATUS_PENDING)
        self.outbox.claim("ev2", owner="s", now=self.now())  # possible 置位
        count = self.outbox.expire_unsubmitted(base_key="base1",
                                               now=self.now())
        self.assertEqual(count, 1)  # 只有未可能提交的 ev1 被抑制
        self.assertEqual(self.outbox.get_item("ev1")["status"],
                         STATUS_EXPIRED_SUPPRESSED)
        self.assertEqual(self.outbox.get_item("ev2")["status"], STATUS_CLAIMED)

    def test_expire_includes_pending_auth_rows(self):
        self.upsert(status=STATUS_PENDING_AUTH)
        count = self.outbox.expire_unsubmitted(base_key="base1",
                                               now=self.now())
        self.assertEqual(count, 1)
        self.assertEqual(self.outbox.get_item("ev1")["status"],
                         STATUS_EXPIRED_SUPPRESSED)


class TestAuthBlock(OutboxBase):
    def test_auth_block_claimable_rows_only(self):
        self.upsert()
        out = self.outbox.auth_block("ev1", reason="revoked", now=self.now())
        self.assertTrue(out["applied"])
        self.assertEqual(self.outbox.get_item("ev1")["status"], STATUS_AUTH_BLOCKED)
        self.upsert()  # 重复事件登记不覆盖 auth-blocked（无新身份）
        self.assertEqual(self.outbox.get_item("ev1")["status"], STATUS_AUTH_BLOCKED)
        self.upsert(item_id="ev2", event_key="ev2", kind=KIND_UPDATE,
                    condition_version=2)
        c = self.outbox.claim("ev2", owner="s", now=self.now())
        self.outbox.mark_result("ev2", expected_revision=c["revision"],
                                change={"status": STATUS_ACCEPTED, "attempts": 1,
                                        "submitted_at": self.now()},
                                now=self.now())
        blocked = self.outbox.auth_block("ev2", reason="revoked",
                                         now=self.now())
        self.assertFalse(blocked["applied"])
        self.assertEqual(self.outbox.get_item("ev2")["status"], STATUS_ACCEPTED)


class TestFailClosed(OutboxBase):
    def _corrupt(self, item_id: str, value: str):
        meta_set(self.conn, f"{MAIL_KEY_PREFIX}{item_id}", value)

    def _record(self, **over):
        p = _payload()
        rec = {
            "schema": 1,
            "item_id": p["item_id"], "base_key": p["base_key"],
            "event_key": p["event_key"],
            "project": p["project"], "incident_id": p["incident_id"],
            "incident_type": p["incident_type"], "kind": p["kind"],
            "condition_version": p["condition_version"],
            "escalation_level": p["escalation_level"],
            "recipient_ref": p["recipient_ref"], "sender_ref": p["sender_ref"],
            "recipient_fingerprint": p["recipient_fingerprint"],
            "sender_fingerprint": p["sender_fingerprint"],
            "subject": p["subject"], "body": p["body"],
            "content_hash": p["content_hash"],
            "status": p["status"], "revision": 1,
            "claim_owner": None, "claim_at": None,
            "possible_submitted_at": None, "submitted_at": None,
            "external_ref": None, "receipt_match": None,
            "receipt_checks": 0, "receipt_revision": 1,
            "receipt_limit": p["receipt_limit"],
            "receipt_claim_owner": None, "receipt_claim_at": None,
            "attempts": 0, "max_attempts": p["max_attempts"],
            "next_attempt_at": p["now"], "deadline_at": None,
            "retry_after_ms": None, "failure_class": None, "last_error": None,
            "first_seen_at": p["first_seen_at"],
            "last_observed_at": p["observed_at"],
            "summary": p["summary"], "metadata": {},
            "created_at": p["now"], "updated_at": p["now"],
        }
        if "content_hash" not in over and ("subject" in over or "body" in over):
            rec["content_hash"] = _content(rec["subject"], rec["body"])
        rec.update(over)
        return rec

    def test_corrupt_json_fails_closed(self):
        self._corrupt("ev1", "{not-json")
        for call in (
            lambda: self.outbox.get_item("ev1"),
            lambda: self.outbox.claim("ev1", owner="s", now=self.now()),
            lambda: self.outbox.list_due(now=self.now()),
            lambda: self.outbox.mark_result(
                "ev1", expected_revision=1, change={}, now=self.now()),
            lambda: self.outbox.claim_receipt("ev1", owner="s", now=self.now()),
        ):
            with self.subTest(call=call):
                with self.assertRaises(MailOutboxStateError):
                    call()

    def test_corrupt_record_never_overwritten(self):
        self.upsert()
        value = "{\"foo\":1}"
        self._corrupt("ev1", value)
        with self.assertRaises(MailOutboxStateError):
            self.outbox.update_observation(
                "ev1", observed_at=1_000_000, summary={"a": "b"},
                now=self.now())
        self.assertEqual(self.raw("ev1"), value)

    def test_max_attempts_validation_isolated_branch(self):
        # 回归：max_attempts 必须独立校验（曾误缩进在 retry_after 分支内漏检）。
        for bad in ("3", True, 0, -1, 1.5):
            with self.subTest(bad=bad):
                self._corrupt("ev1", json.dumps(self._record(max_attempts=bad)))
                with self.assertRaises(MailOutboxStateError):
                    self.outbox.get_item("ev1")
        # 正常值可读（retry_after 为 None 也不影响 max_attempts 校验）
        self._corrupt("ev2", json.dumps(self._record(
            item_id="ev2", event_key="ev2", max_attempts=3,
            retry_after_ms=None)))
        self.assertEqual(self.outbox.get_item("ev2")["max_attempts"], 3)

    def test_state_invariant_claimed_requires_owner_and_possible(self):
        rec = self._record(status=STATUS_CLAIMED, claim_owner=None,
                                claim_at=None, possible_submitted_at=None)
        self._corrupt("ev1", json.dumps(rec))
        with self.assertRaises(MailOutboxStateError):
            self.outbox.get_item("ev1")

    def test_state_invariant_accepted_requires_evidence(self):
        rec = self._record(status=STATUS_ACCEPTED, submitted_at=None,
                                external_ref=None)
        self._corrupt("ev1", json.dumps(rec))
        with self.assertRaises(MailOutboxStateError):
            self.outbox.get_item("ev1")

    def test_content_hash_must_match(self):
        rec = self._record(content_hash="deadbeef")
        self._corrupt("ev1", json.dumps(rec))
        with self.assertRaises(MailOutboxStateError):
            self.outbox.get_item("ev1")

    def test_schema_mismatch_blocks_all_without_side_effect(self):
        self.upsert()
        meta_set(self.conn, MAIL_SCHEMA_KEY, "99")
        with self.assertRaises(MailOutboxStateError):
            self.outbox.list_all()
        with self.assertRaises(MailOutboxStateError):
            self.outbox.claim("ev1", owner="s", now=self.now())
        meta_set(self.conn, MAIL_SCHEMA_KEY, str(MAIL_SCHEMA))
        self.assertEqual(self.outbox.get_item("ev1")["status"], STATUS_PENDING)

    def test_absurd_clock_never_persists_invalid_state(self):
        self.upsert()
        with self.assertRaises(MailOutboxStateError):
            self.outbox.claim("ev1", owner="s", now=10 ** 30)
        self.assertEqual(self.outbox.get_item("ev1")["status"], STATUS_PENDING)


class TestPersistenceAcrossRestart(OutboxBase):
    def test_budget_attempts_and_status_survive_restart(self):
        self.upsert()
        c = self.outbox.claim("ev1", owner="s", now=self.now())
        self.outbox.mark_result("ev1", expected_revision=c["revision"],
                                change={"status": STATUS_RETRY_WAIT,
                                        "attempts": 1,
                                        "possible_submitted_at": None,
                                        "next_attempt_at": 1_000_100},
                                now=self.now())
        self.conn.close()
        self.conn = init(self.home)
        self.outbox = EmailOutbox(self.conn, self.clock)
        row = self.outbox.get_item("ev1")
        self.assertEqual(row["attempts"], 1)  # 预算跨重启不重置
        self.assertEqual(row["status"], STATUS_RETRY_WAIT)
        self.assertEqual(row["next_attempt_at"], 1_000_100)
        self.assertTrue(row["max_attempts"] >= 1)
        self.clock.advance(wall=200)
        due = self.outbox.list_due(now=self.now())
        self.assertEqual([d["item_id"] for d in due], ["ev1"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
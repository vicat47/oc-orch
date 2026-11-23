#!/usr/bin/env python3
"""S3.2/S3.3 目标级额度闸门：单目标持久冷却 + 有界半开探测（最小 API）。

用真临时 SQLite（WAL + 两连接）与 FakeClock 覆盖：并发 probe、重启、
新通知/无关成功、重复 error、worker 隔离/人工 stop、旧 succeeded 不能 clear。
只调用本组 domain/adapters，不触 runtime/真实网络。
"""

from __future__ import annotations

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

from oc_orch.adapters.sqlite_db import init, meta_get, meta_set
from oc_orch.adapters.target_gate import (
    GATE_KEY_PREFIX,
    GATE_SCHEMA,
    GATE_SCHEMA_KEY,
    HALF_OPEN_TIMEOUT_MS,
    MAX_CLOCK_MS,
    PROBE_FAILURE_KEY_PREFIX,
    TargetGate,
    TargetGateStateError,
)
from oc_orch.domain.cooldown import (
    COOLDOWN_MS_15,
    COOLDOWN_MS_30,
    COOLDOWN_MS_60,
    HARD_BLOCK_MS,
)
from tests.fakes import FakeClock

CLASS_QUOTA = {
    "category": "quota", "transient": False, "status": 429,
    "message": "Subscription Sharing usage limit exceeded",
    "action": "block-and-alert", "limit_kind": "subscription",
    "code": "provider.chatgpt-limit",
}


def quota(**kw):
    c = dict(CLASS_QUOTA)
    c.update(kw)
    return c


def auth():
    return {"category": "auth", "transient": False, "status": 403,
            "message": "permission denied", "action": "block-and-alert"}


def _open_wal(home):
    """测试专用：跨线程独占连接，等价 sqlite_db.connect 的 PRAGMA 组合。"""
    conn = sqlite3.connect(os.path.join(home, "ledger.db"), timeout=15,
                           isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


class GateBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gate-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.gate = TargetGate(self.conn, self.clock)
        self.addCleanup(self.conn.close)

    @property
    def t0(self):
        return self.clock.now_ms()


class TestObserveError(GateBase):
    def test_first_error_creates_blocked_gate(self):
        now = self.t0
        out = self.gate.observe_error(
            "ses_a", quota(), evidence_key="ev-1", observed_at=now)
        self.assertTrue(out["changed"])
        self.assertEqual(out["reason"], "new-episode")
        gate = out["gate"]
        self.assertEqual(gate["state"], "blocked")
        self.assertEqual(gate["blocked_until"], now + COOLDOWN_MS_15)
        self.assertEqual(gate["category"], "quota")
        self.assertEqual(gate["failure_count"], 1)
        self.assertEqual(gate["probe_count"], 0)
        self.assertEqual(gate["revision"], 1)
        # schema 版本记录已写入
        self.assertEqual(meta_get(self.conn, GATE_SCHEMA_KEY), str(GATE_SCHEMA))

    def test_get_idle_returns_none(self):
        self.assertIsNone(self.gate.get("ses_missing"))
        self.assertEqual(self.gate.get("ses_missing"), None)

    def test_duplicate_evidence_within_window_no_extension(self):
        now = self.t0
        first = self.gate.observe_error("ses_a", quota(), evidence_key="ev-1",
                                        observed_at=now)
        again = self.gate.observe_error("ses_a", quota(), evidence_key="ev-1",
                                        observed_at=now + 60_000)
        self.assertFalse(again["changed"])
        self.assertEqual(again["reason"], "duplicate-evidence")
        self.assertEqual(again["gate"]["blocked_until"],
                         first["gate"]["blocked_until"])
        self.assertEqual(again["gate"]["failure_count"], 1)
        self.assertEqual(again["gate"]["probe_count"], 0)

    def test_different_evidence_within_window_no_extension_no_reset(self):
        now = self.t0
        first = self.gate.observe_error("ses_a", quota(), evidence_key="ev-1",
                                        observed_at=now)
        dup = self.gate.observe_error("ses_a", quota(), evidence_key="ev-2",
                                      observed_at=now + 60_000)
        self.assertTrue(dup["changed"])
        self.assertEqual(dup["reason"], "in-window-no-extension")
        self.assertEqual(dup["gate"]["blocked_until"],
                         first["gate"]["blocked_until"])
        self.assertEqual(dup["gate"]["failure_count"], 1)
        self.assertEqual(dup["gate"]["probe_count"], 0)

    def test_failure_ladder_15_30_60_then_exhausted(self):
        now = self.t0
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=now)
        self.assertEqual(self.gate.get("ses_a")["blocked_until"],
                         now + COOLDOWN_MS_15)
        # 第 1 次半开探测
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        claim1 = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.assertTrue(claim1["allowed"] and claim1["spent"])
        self.assertEqual(self.gate.get("ses_a")["probe_count"], 1)
        # 探测失败 → 30 分钟窗口
        now2 = self.clock.now_ms()
        f2 = self.gate.observe_error(
            "ses_a", quota(), evidence_key=f"{PROBE_FAILURE_KEY_PREFIX}m1",
            observed_at=now2)
        self.assertEqual(f2["reason"], "failure-round")
        self.assertEqual(f2["gate"]["blocked_until"], now2 + COOLDOWN_MS_30)
        self.assertEqual(f2["gate"]["failure_count"], 2)
        self.assertEqual(f2["gate"]["probe_count"], 1)
        # 第 2 次半开探测失败 → 60 分钟窗口
        self.clock.advance(wall=COOLDOWN_MS_30 + 1)
        self.assertTrue(self.gate.claim_probe("ses_a", owner="sup",
                                              message_id="m2")["spent"])
        now3 = self.clock.now_ms()
        f3 = self.gate.observe_error(
            "ses_a", quota(), evidence_key=f"{PROBE_FAILURE_KEY_PREFIX}m2",
            observed_at=now3)
        self.assertEqual(f3["gate"]["blocked_until"], now3 + COOLDOWN_MS_60)
        self.assertEqual(f3["gate"]["failure_count"], 3)
        # 第 3 次半开探测失败 → 预算耗尽
        self.clock.advance(wall=COOLDOWN_MS_60 + 1)
        self.assertTrue(self.gate.claim_probe("ses_a", owner="sup",
                                              message_id="m3")["spent"])
        self.assertEqual(self.gate.get("ses_a")["probe_count"], 3)
        now4 = self.clock.now_ms()
        ex = self.gate.observe_error(
            "ses_a", quota(), evidence_key=f"{PROBE_FAILURE_KEY_PREFIX}m3",
            observed_at=now4)
        self.assertEqual(ex["reason"], "exhausted")
        self.assertTrue(ex["gate"]["exhausted"])
        self.assertIsNone(ex["gate"]["blocked_until"])
        # 耗尽后不再放行探测，重复错误不重置
        denied = self.gate.claim_probe("ses_a", owner="sup", message_id="m4")
        self.assertFalse(denied["allowed"])
        self.assertEqual(denied["reason"], "exhausted")
        again = self.gate.observe_error("ses_a", quota(), evidence_key="e5",
                                        observed_at=self.clock.now_ms())
        self.assertTrue(again["gate"]["exhausted"])

    def test_trusted_retry_after_wins_over_staircase(self):
        now = self.t0
        out = self.gate.observe_error(
            "ses_a", quota(retry_after_ms=120_000), evidence_key="ev-1",
            observed_at=now)
        self.assertEqual(out["gate"]["blocked_until"], now + 120_000)

    def test_nonfinite_retry_after_falls_back_without_crash(self):
        for i, bad in enumerate((float("nan"), float("inf"), float("-inf"))):
            with self.subTest(bad=bad):
                session = f"ses-nf-{i}"
                out = self.gate.observe_error(
                    session, quota(retry_after_ms=bad), evidence_key=f"ev-{i}",
                    observed_at=self.t0)
                self.assertEqual(out["gate"]["blocked_until"],
                                 self.t0 + COOLDOWN_MS_15)

    def test_auth_category_uses_hard_block(self):
        now = self.t0
        out = self.gate.observe_error("ses_a", auth(), evidence_key="auth-1",
                                      observed_at=now)
        self.assertEqual(out["gate"]["blocked_until"], now + HARD_BLOCK_MS)


class TestClaimProbe(GateBase):
    def _expired_gate(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        return self.t0

    def test_denied_in_cooldown(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        out = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.assertFalse(out["allowed"])
        self.assertEqual(out["reason"], "cooldown")
        self.assertFalse(out["spent"])

    def test_allowed_and_spends_once_at_window(self):
        self._expired_gate()
        out = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.assertTrue(out["allowed"])
        self.assertTrue(out["spent"])
        self.assertEqual(out["reason"], "probe")
        self.assertEqual(out["state"]["probe_count"], 1)
        self.assertEqual(out["state"]["probe_message_id"], "m1")
        # 未决半开期间第二次 claim 拒绝（保持原身份，不换 owner/message）
        again = self.gate.claim_probe("ses_a", owner="other", message_id="m2")
        self.assertFalse(again["allowed"])
        self.assertEqual(again["reason"], "probe-outstanding")
        self.assertEqual(self.gate.get("ses_a")["probe_message_id"], "m1")

    def test_idle_allowed_without_spending(self):
        out = self.gate.claim_probe("ses_b", owner="sup", message_id="m-x")
        self.assertTrue(out["allowed"])
        self.assertFalse(out["spent"])
        self.assertEqual(out["reason"], "idle")
        self.assertIsNone(out["state"])

    def test_get_is_readonly_no_budget_consumed(self):
        self._expired_gate()
        before = self.gate.get("ses_a")
        for _ in range(5):
            snap = self.gate.get("ses_a")
        self.assertEqual(snap["revision"], before["revision"])
        self.assertEqual(snap["probe_count"], before["probe_count"])

    def test_timeout_unknown_keeps_identity_and_turns_visible_blocked(self):
        self._expired_gate()
        claim = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.assertTrue(claim["allowed"])
        self.clock.advance(wall=HALF_OPEN_TIMEOUT_MS + 1)
        retry = self.gate.claim_probe("ses_a", owner="other", message_id="m2")
        self.assertFalse(retry["allowed"])
        self.assertEqual(retry["reason"], "probe-unknown")
        # 保留原 message 身份，不因失效 owner 无限换身份
        gate = self.gate.get("ses_a")
        self.assertEqual(gate["state"], "probe-unknown")
        self.assertEqual(gate["probe_message_id"], "m1")
        # 后续可归因成功按原身份清除
        cleared = self.gate.record_success(
            "ses_a", message_id="m1",
            observed_at=self.clock.now_ms(), evidence_key="s-1")
        self.assertTrue(cleared["cleared"])
        self.assertEqual(cleared["reason"], "probe-success")


class TestConcurrency(GateBase):
    def test_concurrent_claim_single_winner(self):
        """两连接并发 claim_probe：原子单半开，只有一个 allowed+spent。"""
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)

        conn_a, conn_b = _open_wal(self.home), _open_wal(self.home)
        self.addCleanup(conn_a.close)
        self.addCleanup(conn_b.close)
        ga = TargetGate(conn_a, self.clock)
        gb = TargetGate(conn_b, self.clock)
        barrier = Barrier(2, timeout=20)

        def claim(gate, owner, mid):
            barrier.wait()
            return gate.claim_probe("ses_a", owner=owner, message_id=mid)

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(claim, ga, "sup-a", "ma")
            f2 = pool.submit(claim, gb, "sup-b", "mb")
            r1, r2 = f1.result(timeout=25), f2.result(timeout=25)

        winners = [r for r in (r1, r2) if r["allowed"] and r["spent"]]
        self.assertEqual(len(winners), 1, (r1, r2))
        loser = r1 if winners[0] is r2 else r2
        self.assertFalse(loser["allowed"])
        self.assertEqual(loser["reason"], "probe-outstanding")
        # 最终只占用一个探测预算（原连接视角）
        final = self.gate.get("ses_a")
        self.assertEqual(final["probe_count"], 1)


class TestRecordSuccess(GateBase):
    def _probe(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        claim = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        assert claim["allowed"]
        return self.clock.now_ms()

    def test_success_clears_matching_probe(self):
        claimed_at = self._probe()
        out = self.gate.record_success("ses_a", message_id="m1",
                                       observed_at=claimed_at + 1,
                                       evidence_key="s1")
        self.assertTrue(out["cleared"])
        self.assertEqual(out["reason"], "probe-success")
        self.assertIsNone(out["gate"])
        self.assertIsNone(self.gate.get("ses_a"))
        # 清除后可重新开启新 episode（新预算）
        now = self.t0
        fresh = self.gate.observe_error("ses_a", quota(), evidence_key="e2",
                                        observed_at=now)
        self.assertEqual(fresh["reason"], "new-episode")
        self.assertEqual(fresh["gate"]["failure_count"], 1)
        self.assertEqual(fresh["gate"]["probe_count"], 0)

    def test_old_success_or_started_cannot_clear(self):
        claimed_at = self._probe()
        # 老消息（非本半开输入）不能清
        mismatch = self.gate.record_success("ses_a", message_id="old-msg",
                                            observed_at=claimed_at + 1,
                                            evidence_key="old-s")
        self.assertFalse(mismatch["cleared"])
        self.assertEqual(mismatch["reason"], "message-mismatch")
        # 匹配消息但时间早于半开出发（旧 started/成功）不能清
        stale = self.gate.record_success("ses_a", message_id="m1",
                                         observed_at=claimed_at - 1,
                                         evidence_key="stale-s")
        self.assertFalse(stale["cleared"])
        self.assertEqual(stale["reason"], "stale-success")
        self.assertIsNotNone(self.gate.get("ses_a"))

    def test_success_without_gate_is_noop(self):
        out = self.gate.record_success("ses_x", message_id="m0",
                                       observed_at=self.t0,
                                       evidence_key="s0")
        self.assertFalse(out["cleared"])
        self.assertEqual(out["reason"], "no-active-gate")


class TestManualAndIsolation(GateBase):
    def test_block_manual_priority_no_auto_unlock(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        manual = self.gate.block_manual("ses_a", reason="人工暂停")
        self.assertTrue(manual["changed"])
        self.assertEqual(manual["reason"], "manual-block")
        self.assertEqual(manual["gate"]["source"], "manual")
        self.assertIsNone(manual["gate"]["blocked_until"])
        # 冷却到期不会解除人工意图；claim/成功/provider 都不覆盖
        self.clock.advance(wall=COOLDOWN_MS_60 + 1)
        denied = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.assertFalse(denied["allowed"])
        self.assertEqual(denied["reason"], "manual")
        succ = self.gate.record_success("ses_a", message_id="m1",
                                        observed_at=self.clock.now_ms(),
                                        evidence_key="s1")
        self.assertFalse(succ["cleared"])
        self.assertEqual(succ["reason"], "manual-holds")
        err = self.gate.observe_error("ses_a", quota(), evidence_key="e2",
                                      observed_at=self.clock.now_ms())
        self.assertFalse(err["changed"])
        self.assertEqual(err["reason"], "manual-holds")
        self.assertEqual(self.gate.get("ses_a")["state"], "manual-blocked")

    def test_block_manual_preserves_identity_and_persists(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.gate.block_manual("ses_a", reason="stop")
        gate = self.gate.get("ses_a")
        self.assertEqual(gate["probe_message_id"], "m1")
        # 重启后仍 manual，身份保留
        self.conn.close()
        self.conn = init(self.home)
        self.gate = TargetGate(self.conn, self.clock)
        reopen = self.gate.get("ses_a")
        self.assertEqual(reopen["state"], "manual-blocked")
        self.assertEqual(reopen["probe_message_id"], "m1")

    def test_clear_manual_is_explicit_and_removes_gate(self):
        self.gate.block_manual("ses_a", reason="人工停止")
        out = self.gate.clear_manual("ses_a", reason="operator-ack")
        self.assertTrue(out["cleared"])
        self.assertEqual(out["reason"], "manual-cleared")
        self.assertIsNone(self.gate.get("ses_a"))
        self.assertIsNone(meta_get(self.conn, "target-gate:ses_a"))
        # 清除后可正常进入发送流程（无活跃闸门，不消耗探测预算）
        allowed = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.assertTrue(allowed["allowed"])
        self.assertFalse(allowed["spent"])

    def test_clear_manual_refuses_provider_gate(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        out = self.gate.clear_manual("ses_a", reason="operator-ack")
        self.assertFalse(out["cleared"])
        self.assertEqual(out["reason"], "not-manual")
        self.assertEqual(out["gate"]["state"], "blocked")
        self.assertIsNotNone(self.gate.get("ses_a"))

    def test_clear_manual_without_gate_is_noop(self):
        out = self.gate.clear_manual("ses_missing")
        self.assertFalse(out["cleared"])
        self.assertEqual(out["reason"], "no-active-gate")
        self.assertIsNone(out["gate"])

    def test_list_all_reports_manual_provider_and_corrupt_records(self):
        self.gate.block_manual("ses_a", reason="人工停止")
        self.gate.observe_error("ses_b", quota(), evidence_key="e1",
                                observed_at=self.t0)
        meta_set(self.conn, "target-gate:ses_bad", "{not valid json")
        rows = {g["session_id"]: g for g in self.gate.list_all()}
        self.assertEqual(rows["ses_a"]["state"], "manual-blocked")
        self.assertEqual(rows["ses_b"]["state"], "blocked")
        self.assertEqual(rows["ses_bad"]["state"], "state-error")

    def test_worker_session_isolation(self):
        self.gate.observe_error("ses_worker", quota(), evidence_key="e1",
                                observed_at=self.t0)
        # 另一目标（其他 worker/coordinator）不受影响
        self.assertIsNone(self.gate.get("ses_other"))
        idle = self.gate.claim_probe("ses_other", owner="sup", message_id="mx")
        self.assertTrue(idle["allowed"])
        self.assertFalse(idle["spent"])
        # 阻塞 ses_worker 不阻塞别处
        self.assertEqual(self.gate.get("ses_worker")["state"], "blocked")
        self.assertIsNone(self.gate.get("ses_completely_unrelated"))


class TestPersistence(GateBase):
    def test_restart_persists_window_and_probe_budget(self):
        now = self.t0
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=now)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.conn.close()

        self.conn = init(self.home)
        self.gate = TargetGate(self.conn, self.clock)
        reopen = self.gate.get("ses_a")
        self.assertEqual(reopen["probe_count"], 1)
        self.assertEqual(reopen["probe_message_id"], "m1")
        # 重启不重置探测预算：继续推进窗口
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        self.gate.observe_error(
            "ses_a", quota(), evidence_key=f"{PROBE_FAILURE_KEY_PREFIX}m1",
            observed_at=self.clock.now_ms())
        step = self.gate.get("ses_a")
        self.assertEqual(step["failure_count"], 2)
        self.assertEqual(step["probe_count"], 1)
        self.assertEqual(step["blocked_until"],
                         self.clock.now_ms() + COOLDOWN_MS_30)

    def test_exhausted_persists_across_restart(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        for msg, wait in (("m1", COOLDOWN_MS_15),
                         ("m2", COOLDOWN_MS_30),
                         ("m3", COOLDOWN_MS_60)):
            self.clock.advance(wall=wait + 1)
            self.gate.claim_probe("ses_a", owner="sup", message_id=msg)
            self.gate.observe_error(
                "ses_a", quota(), evidence_key=f"{PROBE_FAILURE_KEY_PREFIX}{msg}",
                observed_at=self.clock.now_ms())
        # 完成 3 次探测后耗尽
        self.clock.advance(wall=COOLDOWN_MS_60 + 1)
        ex = self.gate.observe_error("ses_a", quota(), evidence_key="e5",
                                     observed_at=self.clock.now_ms())
        self.assertEqual(ex["reason"], "exhausted")
        self.conn.close()

        self.conn = init(self.home)
        self.gate = TargetGate(self.conn, self.clock)
        reopen = self.gate.get("ses_a")
        self.assertTrue(reopen["exhausted"])
        self.assertFalse(self.gate.claim_probe(
            "ses_a", owner="sup", message_id="mX")["allowed"])

    def test_schema_version_written_and_enforced(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.assertEqual(meta_get(self.conn, GATE_SCHEMA_KEY), str(GATE_SCHEMA))
        meta_set(self.conn, GATE_SCHEMA_KEY, "99")
        with self.assertRaises(RuntimeError):
            self.gate.observe_error("ses_b", quota(), evidence_key="e2",
                                    observed_at=self.clock.now_ms())
        # 被拒绝的写事务回滚：不产生半成品闸门
        self.assertIsNone(meta_get(self.conn, "target-gate:ses_b"))
        meta_set(self.conn, GATE_SCHEMA_KEY, str(GATE_SCHEMA))



class TestCorruptedStateFailClosed(GateBase):
    def _corrupt(self, value):
        meta_set(self.conn, f"{GATE_KEY_PREFIX}ses_a", value)
        return f"{GATE_KEY_PREFIX}ses_a"

    def test_corrupt_json_never_allows_send(self):
        self._corrupt("{not-json")
        for call in (
            lambda: self.gate.get("ses_a"),
            lambda: self.gate.claim_probe("ses_a", owner="sup", message_id="m1"),
            lambda: self.gate.record_success("ses_a", message_id="m1",
                                             observed_at=self.t0,
                                             evidence_key="s1"),
        ):
            with self.subTest(call=call.__doc__):
                with self.assertRaises(TargetGateStateError):
                    call()

    def test_non_dict_record_never_allows_send(self):
        self._corrupt("[1, 2, 3]")
        with self.assertRaises(TargetGateStateError):
            self.gate.get("ses_a")
        with self.assertRaises(TargetGateStateError):
            self.gate.claim_probe("ses_a", owner="sup", message_id="m1")

    def test_corrupt_record_not_overwritten_or_allowed(self):
        key = self._corrupt("{broken")
        with self.assertRaises(TargetGateStateError):
            self.gate.observe_error("ses_a", quota(), evidence_key="e-new",
                                    observed_at=self.t0)
        # 损坏记录被保留（绝不静默覆盖），状态未知即不自动发送
        self.assertEqual(meta_get(self.conn, key), "{broken")


class TestSchemaMismatchFailClosed(GateBase):
    def test_schema_mismatch_blocks_reads_and_writes_without_side_effect(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        key = f"{GATE_KEY_PREFIX}ses_a"
        before = meta_get(self.conn, key)
        meta_set(self.conn, GATE_SCHEMA_KEY, "99")
        with self.assertRaises(TargetGateStateError):
            self.gate.get("ses_a")  # 读操作也校验 schema
        with self.assertRaises(TargetGateStateError):
            self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        with self.assertRaises(TargetGateStateError):
            self.gate.record_success("ses_a", message_id="m1",
                                     observed_at=self.t0, evidence_key="s1")
        # 坏版本必须无副作用拒绝：记录原样保留
        self.assertEqual(meta_get(self.conn, key), before)
        meta_set(self.conn, GATE_SCHEMA_KEY, str(GATE_SCHEMA))


class TestStaleAndAttributionEvidence(GateBase):
    def test_old_evidence_cannot_clean_current_probe(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        claim = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.assertTrue(claim["allowed"])
        claimed_at = self.clock.now_ms()
        # 早于 probe_started_at 的迟到旧证据（新 key）不得清 probe / 推进 failure
        old = self.gate.observe_error("ses_a", quota(), evidence_key="e-old",
                                      observed_at=claimed_at - 1000)
        self.assertFalse(old["changed"])
        self.assertEqual(old["reason"], "stale-evidence")
        gate = self.gate.get("ses_a")
        self.assertEqual(gate["probe_message_id"], "m1")
        self.assertEqual(gate["probe_count"], 1)
        self.assertEqual(gate["failure_count"], 1)

    def test_late_old_error_with_new_key_no_advance_no_extend(self):
        now = self.t0
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=now)
        blocked = self.gate.get("ses_a")["blocked_until"]
        stale = self.gate.observe_error("ses_a", quota(), evidence_key="e-old-2",
                                        observed_at=now - 1000)
        self.assertFalse(stale["changed"])
        self.assertEqual(stale["reason"], "stale-evidence")
        self.assertEqual(self.gate.get("ses_a")["blocked_until"], blocked)
        self.assertEqual(self.gate.get("ses_a")["failure_count"], 1)

    def test_unattributed_new_error_preserves_probe_as_diagnostic(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        self.assertTrue(self.gate.claim_probe("ses_a", owner="sup",
                                               message_id="m1")["allowed"])
        at = self.clock.now_ms()
        out = self.gate.observe_error("ses_a", quota(), evidence_key="e-mystery",
                                      observed_at=at)
        self.assertEqual(out["reason"], "probe-unattributed")
        gate = self.gate.get("ses_a")
        self.assertEqual(gate["probe_message_id"], "m1")  # 不换新 ID
        self.assertEqual(gate["failure_count"], 1)        # 不推进 failure
        self.assertEqual(gate["probe_count"], 1)          # 不重置预算
        self.assertIn(gate["state"], ("probe-outstanding", "probe-unknown"))
        # 缺失归属时不能借新 claim 换身份
        denied = self.gate.claim_probe("ses_a", owner="other", message_id="m2")
        self.assertFalse(denied["allowed"])

    def test_attributed_probe_failure_advances_next_window(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        self.assertTrue(self.gate.claim_probe("ses_a", owner="sup",
                                               message_id="m1")["allowed"])
        at = self.clock.now_ms()
        out = self.gate.observe_error(
            "ses_a", quota(), evidence_key=f"{PROBE_FAILURE_KEY_PREFIX}m1",
            observed_at=at)
        self.assertEqual(out["reason"], "failure-round")
        self.assertEqual(out["gate"]["blocked_until"], at + COOLDOWN_MS_30)
        self.assertEqual(out["gate"]["failure_count"], 2)
        self.assertIsNone(out["gate"]["probe_message_id"])

    def test_alternating_old_evidence_does_not_chain_failures(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        self.assertTrue(self.gate.claim_probe("ses_a", owner="sup",
                                               message_id="m1")["allowed"])
        at = self.clock.now_ms()
        self.gate.observe_error("ses_a", quota(),
                                evidence_key=f"{PROBE_FAILURE_KEY_PREFIX}m1",
                                observed_at=at)  # failure=2
        # 旧 A/B 证据来回（时间单调但 key 已见）不得当新 failure / 无限推迟
        for key in (f"{PROBE_FAILURE_KEY_PREFIX}m1", "e1",
                    f"{PROBE_FAILURE_KEY_PREFIX}m1"):
            dup = self.gate.observe_error("ses_a", quota(), evidence_key=key,
                                          observed_at=self.clock.now_ms())
            self.assertFalse(dup["changed"])
            self.assertEqual(dup["reason"], "duplicate-evidence")
        self.assertEqual(self.gate.get("ses_a")["failure_count"], 2)


class TestRecordSuccessGuards(GateBase):
    def _probe(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        claim = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        assert claim["allowed"]
        return self.clock.now_ms()

    def test_success_without_real_probe_never_clears(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        key = f"{GATE_KEY_PREFIX}ses_a"
        snap = meta_get(self.conn, key)
        for msg in (None, "", "  ", "other-msg"):
            with self.subTest(msg=msg):
                out = self.gate.record_success("ses_a", message_id=msg,
                                               observed_at=self.t0 + 1,
                                               evidence_key="s1")
                self.assertFalse(out["cleared"])
                self.assertIsNotNone(self.gate.get("ses_a"))
        # 未被误清
        self.assertEqual(meta_get(self.conn, key), snap)

    def test_invalid_observed_at_does_not_crash_or_clear(self):
        claimed_at = self._probe()
        for bad in (None, -5, "yesterday", float("nan")):
            with self.subTest(bad=bad):
                out = self.gate.record_success("ses_a", message_id="m1",
                                               observed_at=bad,
                                               evidence_key="s-bad")
                self.assertFalse(out["cleared"])
                self.assertEqual(out["reason"], "invalid-observed-at")
        self.assertIsNotNone(self.gate.get("ses_a"))


class TestInvalidPersistedState(GateBase):
    """G1：合法 JSON 但结构非法的持久记录，5 方法一律 fail-closed。"""

    def _write(self, rec_or_str):
        value = rec_or_str if isinstance(rec_or_str, str) else json.dumps(rec_or_str)
        meta_set(self.conn, f"{GATE_KEY_PREFIX}ses_a", value)
        return value

    def _assert_all_methods_fail_closed(self, value):
        for call in (
            lambda: self.gate.get("ses_a"),
            lambda: self.gate.claim_probe("ses_a", owner="o", message_id="m1"),
            lambda: self.gate.record_success("ses_a", message_id="m1",
                                             observed_at=self.t0, evidence_key="s"),
            lambda: self.gate.observe_error("ses_a", quota(), evidence_key="e-x",
                                            observed_at=self.t0),
            lambda: self.gate.block_manual("ses_a", reason="x"),
        ):
            with self.subTest(call=call):
                with self.assertRaises(TargetGateStateError):
                    call()
        # 回滚不改记录：原始字节原样保留
        self.assertEqual(meta_get(self.conn, f"{GATE_KEY_PREFIX}ses_a"), value)

    def _round2_like(self, **overrides):
        rec = {
            "schema": 1, "session_id": "ses_a", "blocked_until": self.t0 + COOLDOWN_MS_15,
            "category": "quota", "source": "provider", "revision": 1,
            "failure_count": 1, "probe_count": 0,
            "probe_message_id": None, "probe_owner": None, "probe_started_at": None,
            "probe_unknown_since": None, "last_error_key": "e1",
            "last_error_at": self.t0, "reason": None, "message": "quota",
            "exhausted": False, "recent_errors": {"e1": self.t0},
        }
        rec.update(overrides)
        return rec

    def test_missing_required_fields_all_methods_fail_closed(self):
        # {'foo': 1}：合法 JSON dict，但缺全部必需控制字段
        value = self._write("{\"foo\": 1}")
        self._assert_all_methods_fail_closed(value)

    def test_blocked_until_text_and_bool_rejected(self):
        for bad in ("later", True):
            with self.subTest(bad=bad):
                value = self._write(self._round2_like(blocked_until=bad))
                self._assert_all_methods_fail_closed(value)

    def test_wrong_typed_numbers_rejected(self):
        cases = (
            {"failure_count": "2"},
            {"probe_count": True},
            {"revision": 1.5},
            {"category": 42},
            {"exhausted": "yes"},
            {"recent_errors": [1, 2]},
            {"last_error_at": 10 ** 30},
            {"blocked_until": 10 ** 30},
            {"probe_started_at": 1.9},
        )
        for override in cases:
            with self.subTest(override=override):
                value = self._write(self._round2_like(**override))
                self._assert_all_methods_fail_closed(value)

    def test_record_of_wrong_session_rejected(self):
        value = self._write(self._round2_like(session_id="ses_other"))
        self._assert_all_methods_fail_closed(value)

    def test_probe_field_inconsistency_rejected(self):
        for override in (
            {"probe_message_id": "m1", "probe_started_at": None, "probe_owner": "o"},
            {"probe_message_id": None, "probe_started_at": 5, "probe_owner": "o"},
            {"probe_message_id": "m1", "probe_started_at": 5, "probe_owner": None},
        ):
            with self.subTest(override=override):
                value = self._write(self._round2_like(**override))
                self._assert_all_methods_fail_closed(value)

    def test_legal_round2_records_accepted(self):
        base = [
            lambda sid: self._round2_like(session_id=sid),
            lambda sid: self._round2_like(
                session_id=sid, source="manual", category="manual",
                blocked_until=None, failure_count=0, reason="stop", message=""),
            lambda sid: self._round2_like(
                session_id=sid, exhausted=True, blocked_until=None,
                failure_count=4, probe_count=3, reason="exhausted"),
            lambda sid: self._round2_like(
                session_id=sid, probe_message_id="m1", probe_owner="sup",
                probe_started_at=self.t0 + 1, probe_count=1,
                probe_unknown_since=None, blocked_until=None),
        ]
        for i, build in enumerate(base):
            key = f"{GATE_KEY_PREFIX}ses_g1_{i}"
            rec = build(f"ses_g1_{i}")
            meta_set(self.conn, key, json.dumps(rec))
            snap = self.gate.get(f"ses_g1_{i}")
            self.assertIsNotNone(snap)
            self.assertEqual(snap["session_id"], f"ses_g1_{i}")
            self.assertIn(snap["state"], ("blocked", "manual-blocked", "exhausted",
                                          "probe-outstanding"))
            # 读不消耗预算
            self.assertEqual(snap["probe_count"], rec["probe_count"])


class TestTimestampBoundary(GateBase):
    """G6：荒谬 observed_at / origin_at 不崩、不造天文 deadline。"""

    def test_absurd_observed_at_no_deadline_no_crash(self):
        for bad in (10 ** 30, -1, "later", float("nan")):
            with self.subTest(bad=bad):
                out = self.gate.observe_error("ses_t", quota(), evidence_key=f"e-{bad}",
                                              observed_at=bad)
                self.assertFalse(out["changed"])
                self.assertEqual(out["reason"], "invalid-timestamp")
                self.assertIsNone(out["gate"])

    def test_absurd_origin_at_no_deadline_and_existing_gate_kept(self):
        self.gate.observe_error("ses_t", quota(), evidence_key="e1",
                                observed_at=self.t0)
        snap = self.gate.get("ses_t")
        out = self.gate.observe_error("ses_t", quota(), evidence_key="e-bad",
                                      observed_at=self.t0 + 1,
                                      origin_at=10 ** 30)
        self.assertFalse(out["changed"])
        self.assertEqual(out["reason"], "invalid-timestamp")
        self.assertEqual(self.gate.get("ses_t")["blocked_until"],
                         snap["blocked_until"])

    def test_record_success_absurd_time_no_crash_no_clear(self):
        self.gate.observe_error("ses_t", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        claim = self.gate.claim_probe("ses_t", owner="sup", message_id="m1")
        self.assertTrue(claim["allowed"])
        out = self.gate.record_success("ses_t", message_id="m1",
                                       observed_at=10 ** 30, evidence_key="s1")
        self.assertFalse(out["cleared"])
        self.assertEqual(out["reason"], "invalid-observed-at")
        self.assertIsNotNone(self.gate.get("ses_t"))

    def test_fakeclock_legal_values_still_work(self):
        now = self.t0  # FakeClock 1_000_000，远在有效边界内
        out = self.gate.observe_error("ses_ok", quota(), evidence_key="e1",
                                      observed_at=now)
        self.assertEqual(out["gate"]["blocked_until"], now + COOLDOWN_MS_15)


class TestOriginWatermark(GateBase):
    """G2：origin 水位线——被淘汰的旧事实用新观察时间冒充新失败必须被拒。"""

    def test_evicted_old_fact_with_fresh_observation_stays_stale(self):
        t0 = self.t0
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=t0, origin_at=t0)
        blocked = self.gate.get("ses_a")["blocked_until"]
        # 窗内填满 16 个不同来源（每个 origin 递增，覆盖最近证据集合并淘汰 e1）
        for i in range(16):
            out = self.gate.observe_error(
                "ses_a", quota(), evidence_key=f"d{i}",
                observed_at=t0 + 10 + i, origin_at=t0 + 10 + i)
            self.assertEqual(out["reason"], "in-window-no-extension")
        self.assertNotIn("e1", self.gate.get("ses_a").get("recent_errors", {}))
        # 窗口已过：e1 用新观察时间 + 旧稳定 origin 再来，不能推进 failure
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        out = self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                      observed_at=self.clock.now_ms(),
                                      origin_at=t0)
        self.assertFalse(out["changed"])
        self.assertEqual(out["reason"], "stale-evidence")
        gate = self.gate.get("ses_a")
        self.assertEqual(gate["failure_count"], 1)
        self.assertEqual(gate["blocked_until"], blocked)
        # 真正新的 origin 仍可进入下一轮（不破坏有限失败推进）
        out2 = self.gate.observe_error(
            "ses_a", quota(), evidence_key="e-fresh",
            observed_at=self.clock.now_ms(), origin_at=self.clock.now_ms())
        self.assertEqual(out2["reason"], "failure-round")
        self.assertEqual(out2["gate"]["failure_count"], 2)

class TestRoundupGuards(GateBase):
    """round4 收尾：deadline 封顶可读 + 坏 evidence_key/owner/message_id 不自毒。"""

    def test_deadline_capped_and_record_stays_readable(self):
        near_max = MAX_CLOCK_MS - 1000
        out = self.gate.observe_error(
            "ses_a", quota(retry_after_ms=4_000_000_000_000),
            evidence_key="e1", observed_at=near_max)
        self.assertEqual(out["reason"], "new-episode")
        self.assertLessEqual(out["gate"]["blocked_until"], MAX_CLOCK_MS)
        # 写入后立即可读，不进入自校验拒绝后的故障态
        snap = self.gate.get("ses_a")
        self.assertIsNotNone(snap)
        self.assertEqual(snap["blocked_until"], MAX_CLOCK_MS)

    def test_origin_at_near_boundary_plus_delay_stays_valid(self):
        near = MAX_CLOCK_MS - 1000
        out = self.gate.observe_error(
            "ses_a", quota(retry_after_ms=4_000_000_000_000),
            evidence_key="e1", observed_at=1000, origin_at=near)
        self.assertLessEqual(out["gate"]["blocked_until"], MAX_CLOCK_MS)
        self.assertEqual(self.gate.get("ses_a")["blocked_until"], MAX_CLOCK_MS)

    def test_observe_error_bad_evidence_key_no_poison(self):
        from oc_orch.adapters.target_gate import GATE_KEY_PREFIX as _KP
        for bad in (123, None, "", "  "):
            with self.subTest(bad=bad):
                out = self.gate.observe_error("ses_a", quota(), evidence_key=bad,
                                              observed_at=self.t0)
                self.assertFalse(out["changed"])
                self.assertEqual(out["reason"], "invalid-evidence-key")
        # 未产生自毒记录；正常键仍可用
        self.assertIsNone(meta_get(self.conn, _KP + "ses_a"))
        self.gate.observe_error("ses_a", quota(), evidence_key="e-ok",
                                observed_at=self.t0)
        self.assertIsNotNone(self.gate.get("ses_a"))

    def test_record_success_bad_evidence_key_does_not_clear(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        claim = self.gate.claim_probe("ses_a", owner="sup", message_id="m1")
        self.assertTrue(claim["allowed"])
        key = f"{GATE_KEY_PREFIX}ses_a"
        before = meta_get(self.conn, key)
        out = self.gate.record_success(
            "ses_a", message_id="m1",
            observed_at=self.clock.now_ms(), evidence_key=123)
        self.assertFalse(out["cleared"])
        self.assertEqual(out["reason"], "invalid-evidence-key")
        self.assertEqual(meta_get(self.conn, key), before)

    def test_claim_probe_bad_ids_no_self_poison(self):
        self.gate.observe_error("ses_a", quota(), evidence_key="e1",
                                observed_at=self.t0)
        self.clock.advance(wall=COOLDOWN_MS_15 + 1)
        for owner, mid in ((123, "m1"), ("sup", 456), (None, "m1"), ("sup", "")):
            with self.subTest(owner=owner, mid=mid):
                out = self.gate.claim_probe("ses_a", owner=owner, message_id=mid)
                self.assertFalse(out["allowed"])
                self.assertEqual(out["reason"], "invalid-probe-ids")
        # 闸门未被污染：仍可读、仍可正常 claim（坏请求不消耗预算）
        self.assertIsNotNone(self.gate.get("ses_a"))
        ok = self.gate.claim_probe("ses_a", owner="sup", message_id="m-ok")
        self.assertTrue(ok["allowed"])
        self.assertEqual(self.gate.get("ses_a")["probe_count"], 1)

if __name__ == "__main__":
    unittest.main(verbosity=2)
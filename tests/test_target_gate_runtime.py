#!/usr/bin/env python3
"""C 组 S2/S3 运行时整合定向测试：目标闸门冷却/半开 + session_inputs 单槽。

不连接真实服务；使用临时 SQLite + FakeClient/FakeClock（受控时间，无 sleep）。
覆盖（improve-wake-delivery-reliability S2/S3 + wake-delivery-control spec）：
- S3：冷却期间新通知与 coordinator_recovery 都不触发 prompt；到期单半开原子
  claim；探测预算耗尽 → exhausted 去重告警；未归属错误不换 ID（probe-unknown）；
  当前 probe 明确失败用 canonical probe-fail:原ID；HTTP 成功不清闸门，只有
  可归属本输入执行成功才 record_success；人工中断 block_manual 不自动复活；
  worker 错误不扩大到 coordinator；闸门状态损坏 fail-closed（视为阻塞诊断）。
- S2：单目标未决槽 DB 唯一；批次受理补证整批幂等；取消不 sent/ACK；传输未知
  保持原 message_id 并跨重启对账；delivered+全部 ACK 的旧槽经安全退役释放，
  健康目标第二结果两轮内投递。
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from oc_orch.client import TransportError
from oc_orch.config import DEFAULTS
from oc_orch.supervisor import Supervisor

from tests.fakes import FakeClient, FakeClock


class TargetGateRuntimeBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="wake-se-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_coord", outcome=None)
        self.sup = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                              client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.gate = self.sup.target_gate

    def enqueue(self, key, *, kind="result_ready", **kw):
        return self.ledger.enqueue_notification(
            kind=kind, target_session="ses_coord", dedupe_key=key,
            payload={"summary": key, "classification": "RESULT_READY"},
            project="p", **kw)

    def register_coordinator(self):
        return self.ledger.create_task(project="p", title="c",
                                       coordinator_session="ses_coord")

    def quota_cls(self, retry_after_ms=None):
        cls = {"category": "quota", "transient": False, "status": 429,
               "message": "Subscription Sharing usage limit exceeded",
               "action": "block-and-alert"}
        if retry_after_ms is not None:
            cls["retry_after_ms"] = retry_after_ms
        return cls

    def open_gate(self, retry_after_ms=None):
        now = self.clock.now_ms()
        return self.gate.observe_error(
            "ses_coord", self.quota_cls(retry_after_ms),
            evidence_key="test-episode-1", observed_at=now, origin_at=now)


class TestS3CooldownGate(TargetGateRuntimeBase):
    def test_cooldown_blocks_new_and_recovery_notifications_then_single_probe(self):
        self.open_gate()
        n = self.enqueue("new-under-cooling")
        recovery = self.enqueue("coord-recovery", kind="coordinator_recovery")
        summary = self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [], "冷却期间不得追加任何自动输入")
        self.assertEqual(summary["suppressed"], 2)
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")
        self.assertEqual(self.ledger.get_notification(recovery["id"])["state"], "pending")
        # 冷却到期：单半开探测 → 只发一次 prompt（同目标单槽）
        self.clock.advance(seconds=15 * 60 + 1)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        snap = self.gate.get("ses_coord")
        self.assertEqual(snap["state"], "probe-outstanding")
        self.assertEqual(snap["probe_count"], 1)
        # 探测期间再次 dispatch → 不产生第二 probe（probe-outstanding 挡）
        self.clock.advance(seconds=5)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertEqual(self.gate.get("ses_coord")["probe_count"], 1)

    def test_probe_budget_exhausts_to_visible_alert(self):
        self.open_gate()
        # 每次推进 61 分钟，覆盖 quota 阶梯最大冷却（60 分钟），确保探针能发出并耗尽。
        # 发送后投递清空 inbox + 业务 ACK，释放单槽，否则 queue 语义下 inbox-pending
        # 会遮蔽闸门路径、单槽也挡住下一轮 probe。
        for i in range(6):
            self.clock.advance(seconds=61 * 60 + 1)
            snap = self.gate.get("ses_coord")
            if snap and snap["state"] == "exhausted":
                break
            n = self.enqueue(f"n{i}")
            before = len(self.client.prompt_calls)
            self.sup.dispatch_notifications()
            newly = len(self.client.prompt_calls) - before
            self.client.deliver_inbox("ses_coord", at=self.clock.now_ms())
            snap = self.gate.get("ses_coord")
            if not newly or not snap:
                continue
            self.ledger.ack_notifications([n["id"]], actor="coordinator")
            probe = snap["probe_message_id"]
            now = self.clock.now_ms()
            # canonical probe-fail：证明当前半开探测失败，进入下一冷却/耗尽
            self.gate.observe_error(
                "ses_coord", self.quota_cls(),
                evidence_key=f"probe-fail:{probe}",
                observed_at=now, origin_at=now)
        self.assertEqual(self.gate.get("ses_coord")["state"], "exhausted",
                         "探测预算应耗尽")
        self.enqueue("after-exhausted")
        before = len(self.client.prompt_calls)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls) - before, 0,
                         "耗尽后不再自动探测")
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=200)
                  if a["kind"] == "target_gate_exhausted"]
        self.assertEqual(len(alerts), 1, "耗尽必须形成去重可见告警")

    def test_unattributed_error_keeps_original_probe_id_and_turns_visible(self):
        self.open_gate()
        self.clock.advance(seconds=15 * 60 + 1)
        self.enqueue("probe-round")
        self.sup.dispatch_notifications()
        snap = self.gate.get("ses_coord")
        self.assertEqual(snap["state"], "probe-outstanding")
        original = snap["probe_message_id"]
        # 未归属错误（SSE retry 稳定键，不带 probe-fail）：只转可见阻塞，保留原 ID
        self.clock.advance(seconds=1)
        self.sup.ingest_event({
            "id": "evt_unattr", "type": "session.retry.scheduled",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "error": self.quota_cls()},
        })
        snap = self.gate.get("ses_coord")
        self.assertEqual(snap["probe_message_id"], original,
                         "无归属错误不得换身份/清当前 probe")
        # 超过半开超时 → probe-unknown + 可见告警，且不换 ID。
        # 同时越过 ack_timeout（900s）使旧 sent 不再触发 unacked-recent 抑制；
        # 投递清空 inbox，避免 queue 语义的 inbox-pending 遮蔽闸门 fast-path。
        self.clock.advance(seconds=16 * 60 + 1)
        self.client.deliver_inbox("ses_coord", at=self.clock.now_ms())
        snap = self.gate.get("ses_coord")
        self.assertEqual(snap["state"], "probe-unknown")
        self.assertEqual(snap["probe_message_id"], original)
        self.enqueue("blocked-by-unknown")
        before = len(self.client.prompt_calls)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls) - before, 0,
                         "probe-unknown 不触发新自动输入")
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=200)
                  if a["kind"] == "target_gate_probe_unknown"]
        self.assertEqual(len(alerts), 1)

    def test_probe_fail_canonical_only_with_attributable_current_idle(self):
        self.register_coordinator()
        self.open_gate()
        self.clock.advance(seconds=15 * 60 + 1)
        self.enqueue("probe-fail-round")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        msg = self.client.prompt_calls[0]["message_id"]
        snap = self.gate.get("ses_coord")
        self.assertEqual(snap["state"], "probe-outstanding")
        # 布置可归属失败：probe 消息已投递 + 当前 idle outcom==failed 且带 error
        self.clock.advance(seconds=1)
        t1 = self.clock.now_ms()
        self.client.deliver_inbox("ses_coord", at=t1)
        self.clock.advance(seconds=1)
        t2 = self.clock.now_ms()
        self.client.add_idle("ses_coord", outcome="failed", at=t2,
                             error={"code": "provider.chatgpt-limit",
                                    "message": "Subscription Sharing usage limit exceeded"})
        self.client.set_session("ses_coord", outcome="failed", time_={"idle": t2})
        self.sup.observe_coordinators({})
        snap = self.gate.get("ses_coord")
        self.assertIsNotNone(snap)
        self.assertNotEqual(snap["state"], "probe-outstanding",
                            "可归属当前 probe 失败必须结束探测进入下一冷却")
        self.assertIsNone(snap.get("probe_message_id"))
        self.assertEqual(snap.get("last_error_key"), f"probe-fail:{msg}")
        self.assertGreater(snap["probe_count"], 0)

    def test_http_success_does_not_clear_and_attributable_success_clears(self):
        self.register_coordinator()
        self.open_gate()
        self.clock.advance(seconds=15 * 60 + 1)
        self.enqueue("success-round")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        msg = self.client.prompt_calls[0]["message_id"]
        snap = self.gate.get("ses_coord")
        self.assertEqual(snap["state"], "probe-outstanding")
        # HTTP 受理不是成功：record_success 用错误消息/旧时间都不清；运行时也不清
        now = self.clock.now_ms()
        self.assertEqual(self.gate.record_success(
            "ses_coord", message_id="different", observed_at=now,
            evidence_key="bad")["cleared"], False)
        self.assertIsNotNone(self.gate.get("ses_coord"), "HTTP 受理不清闸门")
        # 可归属本输入执行成功：probe 消息投递 + 当前 idle succeeded
        self.clock.advance(seconds=1)
        t1 = self.clock.now_ms()
        self.client.deliver_inbox("ses_coord", at=t1)
        self.clock.advance(seconds=1)
        t2 = self.clock.now_ms()
        self.client.add_idle("ses_coord", outcome="succeeded", at=t2)
        self.client.set_session("ses_coord", outcome="succeeded", time_={"idle": t2})
        self.sup.observe_coordinators({})
        self.assertIsNone(self.gate.get("ses_coord"),
                          "本输入可归属成功应清除闸门")

    def test_manual_block_overrides_cooldown_and_recovery_does_not_resurrect(self):
        self.gate.block_manual("ses_coord", reason="interrupted")
        self.enqueue("under-manual")
        recovery = self.enqueue("recovery-under-manual", kind="coordinator_recovery")
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_notification(recovery["id"])["state"], "pending")
        # 冷却到期不能解除人工意图
        self.clock.advance(seconds=60 * 60 + 1)
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.gate.get("ses_coord")["state"], "manual-blocked")

    def test_worker_retry_does_not_gate_coordinator(self):
        task = self.register_coordinator()
        worker = self.ledger.create_attempt(
            task["id"], directory=str(self.home / "w"), session_id="ses_w")
        self.sup.ingest_event({
            "id": "evt_worker_retry", "type": "session.retry.scheduled",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_w", "error": self.quota_cls()},
        })
        # worker session 有 attempt → handle_retry（不做目标闸门）
        self.assertIsNone(self.gate.get("ses_w"), "worker 错误不建 coordinator 闸门")
        self.assertIsNone(self.gate.get("ses_coord"))
        self.enqueue("still-fine")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1, "worker 错误不阻塞 coordinator 目标")

    def test_gate_state_error_fails_closed(self):
        self.ledger.set_meta("target-gate:ses_coord", "{not valid json")
        n = self.enqueue("fail-closed")
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [], "闸门状态损坏=未知，不得放行")
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=200)
                  if a["kind"] == "target_gate_state_error"]
        self.assertEqual(len(alerts), 1)


class TestS2InputSlot(TargetGateRuntimeBase):
    def test_single_outstanding_slot_blocks_second_identity_until_resolved(self):
        self.enqueue("one")
        self.enqueue("two")
        self.sup.cfg["notification_batch"] = 1
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1, "单槽：只发第一条")
        first = self.client.prompt_calls[0]["message_id"]
        rows = self.ledger.list_pending_inputs(outstanding=True)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["message_id"], first)
        self.assertEqual(rows[0]["admission_state"], "possible")

    def test_batch_admission_confirms_all_members_once(self):
        rows = [self.enqueue(f"b{i}") for i in range(3)]
        self.client.fail("prompt", TransportError("/prompt", "lost"))
        self.sup.dispatch_notifications()
        msg = self.client.prompt_calls[0]["message_id"]
        self.client.set_inbox("ses_coord", [{"id": msg, "delivery": "queue"}])
        self.sup.ingest_event({
            "id": "evt_admit", "type": "session.inbox.enqueued",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "inboxID": msg},
        })
        for r in rows:
            row = self.ledger.get_notification(r["id"])
            self.assertEqual(row["state"], "sent")
            self.assertEqual(row["attempts"], 1)
        # 重复受理证据幂等：不重算次数
        self.sup.ingest_event({
            "id": "evt_admit_dup", "type": "session.inbox.enqueued",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "inboxID": msg},
        })
        for r in rows:
            self.assertEqual(self.ledger.get_notification(r["id"])["attempts"], 1)

    def test_cancelled_input_is_not_sent_or_business_ack(self):
        n = self.enqueue("cancel-me")
        self.client.fail("prompt", TransportError("/prompt", "lost"))
        self.sup.dispatch_notifications()
        msg = self.client.prompt_calls[0]["message_id"]
        self.sup.ingest_event({
            "id": "evt_cancel", "type": "session.inbox.cancelled",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "inboxID": msg},
        })
        row = self.ledger.get_notification(n["id"])
        self.assertNotIn(row["state"], ("sent", "acked"))
        self.assertIsNone(row["acked_at"])
        self.assertFalse(self.ledger.list_pending_inputs(outstanding=True),
                         "取消释放槽位")

    def test_transport_unknown_keeps_original_id_and_restart_reconciles(self):
        n = self.enqueue("keep-id")
        self.client.fail("prompt", TransportError("/prompt", "lost"), times=2)
        self.sup.dispatch_notifications()
        first = self.client.prompt_calls[0]["message_id"]
        # 跨重启：新 Supervisor 同 home；下一轮以原 ID 有界重试
        self.clock.advance(seconds=31)
        other = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                           client=self.client, clock=self.clock)
        try:
            other.dispatch_notifications()
        finally:
            other.conn.close()
        ids = [c["message_id"] for c in self.client.prompt_calls]
        self.assertEqual(set(ids), {first}, "未知响应不得制造新 message 身份")
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")

    def test_delivered_and_acked_retires_slot_freeing_second_result(self):
        first = self.enqueue("first-result")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        msg = self.client.prompt_calls[0]["message_id"]
        self.clock.advance(seconds=1)
        self.client.deliver_inbox("ses_coord", at=self.clock.now_ms())
        self.sup.ingest_event({
            "id": "evt_delivered", "type": "session.inbox.delivered",
            "created": self.clock.now_ms(),
            "data": {"sessionID": "ses_coord", "inboxID": msg},
        })
        self.ledger.ack_notifications([first["id"]], actor="coordinator")
        second = self.enqueue("second-result")
        self.sup.dispatch_notifications()
        self.sup.dispatch_notifications()
        self.assertEqual(self.ledger.get_notification(second["id"])["state"], "sent",
                         "已投递且业务 ACK 的旧槽必须安全退役，第二结果两轮内投递")
        self.assertEqual(len(self.client.prompt_calls), 2)
        retired = [i for i in self.ledger.list_pending_inputs(outstanding=False)
                   if i["admission_state"] == "released"]
        self.assertTrue(any(i["notification_ids"] == [first["id"]] for i in retired),
                        "退役保留历史回执")


if __name__ == "__main__":
    unittest.main()
#!/usr/bin/env python3
"""C 组运行时/领域回归：S1 claim 组装、P03 unknown 观察、P04 foreign 队头。

不连接真实服务；使用临时 SQLite + FakeClient/FakeClock（受控时间，无 sleep）。

覆盖（对应 improve-wake-delivery-reliability 的 S1 与 wake-delivery-control spec）：
- should_suppress_wake 兼容可选 known 标志：观察 unknown → 抑制，不追加输入；
  既有调用不传 known 时行为不变。
- dispatch 投递前 active/inbox 查询失败（P03）：不把“查询失败”当空队列/空闲，
  不触发新模型请求；持久记录 first/last 原因与 next_check；超过有界期限后
  去重告警；同一目标不重复告警；健康目标不受其他目标 unknown 影响（不饥饿）。
- foreign/parked 队头（P04）：不能只 defer 到永远；到达有界期限后形成去重
  可见责任（kind 含 foreign_input、含目标 session、owner/next_action），
  绝不自动 steer/delete 陌生输入；自有排队输入只对账不告警 foreign。
- S1/claim 组装：正文与 metadata 来自最终 claim，被剔除的旧项同步消失、
  不烧发送预算、不当 ACK；compose_wake 保留结果版本/缺项/显式 ACK 语义。
- 纯状态抑制不吞旧执行的清理责任、不触碰写权。
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from oc_orch.client import TransportError
from oc_orch.config import DEFAULTS
from oc_orch.supervisor import Supervisor
from oc_orch.wake_policy import should_suppress_wake

from tests.fakes import FakeClient, FakeClock


class WakeRuntimeBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="wake-runtime-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_coord", outcome=None)
        for sid in ("ses_good", "ses_bad"):
            self.client.set_session(sid, outcome=None)
        self.sup = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                              client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger

    def enqueue(self, key, *, kind="stall", task=None, attempt=None,
                target="ses_coord", priority=5, payload=None):
        return self.ledger.enqueue_notification(
            kind=kind, target_session=target, dedupe_key=key,
            payload=payload or {"summary": key}, project="p",
            task_id=task["id"] if task else None,
            attempt_id=attempt["id"] if attempt else None,
            priority=priority,
        )

    def alert_kinds(self):
        return [a["kind"] for a in self.ledger.list_alerts(state="new", limit=200)]


class TestShouldSuppressWakeKnownFlag(unittest.TestCase):
    """known 是兼容可选标志：不传时既有语义不变，传 False 表示观察 unknown。"""

    def test_known_false_suppresses_nothing_known(self):
        ok, why = should_suppress_wake(
            outstanding=[], inbox=[], is_active=False,
            now_ms=1000, ack_timeout_ms=5000, known=False,
        )
        self.assertTrue(ok)
        self.assertEqual(why, "observation-unknown")

    def test_known_true_with_empty_observation_allows_nudge(self):
        ok, why = should_suppress_wake(
            outstanding=[{"sent_at": 0}], inbox=[], is_active=False,
            now_ms=10_000, ack_timeout_ms=5_000, known=True,
        )
        self.assertFalse(ok)
        self.assertEqual(why, "")

    def test_existing_calls_without_known_unchanged(self):
        # 既有调用不传 known：inbox-pending 依旧优先、空队列+空闲仍允许 nudge
        ok, why = should_suppress_wake(
            outstanding=[], inbox=[{"id": "x"}], is_active=False,
            now_ms=0, ack_timeout_ms=0,
        )
        self.assertTrue(ok)
        self.assertEqual(why, "inbox-pending")
        ok, why = should_suppress_wake(
            outstanding=[], inbox=[], is_active=False,
            now_ms=0, ack_timeout_ms=0,
        )
        self.assertFalse(ok)


class TestUnknownObservationDispatch(WakeRuntimeBase):
    def test_query_failure_defers_and_never_appends_input(self):
        n = self.enqueue("unknown-observation")
        self.client.set_inbox("ses_coord", [{"id": "msg_already_queued"}])
        self.client.fail("active_sessions", TransportError("/active", "probe"))
        self.client.fail("inbox", TransportError("/inbox", "probe"))
        summary = self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [], "unknown 观察不得追加模型输入")
        self.assertEqual(summary["suppressed"], 1)
        row = self.ledger.get_notification(n["id"])
        self.assertEqual(row["state"], "pending", "unknown 只 defer，不 claim")
        # 持久诊断：first/last 原因与 next_check
        rec = {}
        raw = self.ledger.get_meta("wake-diagnostic:ses_coord")
        self.assertIsInstance(raw, str)
        rec = __import__("json").loads(raw)
        self.assertEqual(rec["first_reason"], "observation-unknown")
        self.assertEqual(rec["last_reason"], "observation-unknown")
        self.assertEqual(rec["first_at"], rec["last_at"])
        self.assertEqual(rec["count"], 1)
        self.assertEqual(rec["next_check"],
                         self.clock.now_ms() + int(DEFAULTS["ack_timeout"]) * 1000)

    def test_persistent_unknown_raises_one_dedupe_alert_after_deadline(self):
        self.enqueue("persistent-unknown")
        self.client.fail("inbox", TransportError("/inbox", "probe"), times=3)
        # 第一次：只排 next_check，不立刻告警
        self.sup.dispatch_notifications()
        self.assertEqual(self.alert_kinds(), [], "首次 unknown 不立刻告警")
        self.assertEqual(self.client.prompt_calls, [])
        # 跨过有界期限：去重告警一次
        self.clock.advance(seconds=int(DEFAULTS["ack_timeout"]) + 1)
        self.sup.dispatch_notifications()
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=200)
                  if a["kind"] == "wake_observation_unknown"]
        self.assertEqual(len(alerts), 1)
        self.assertIn("ses_coord", alerts[0]["title"])
        self.assertEqual(self.client.prompt_calls, [])
        # 再跨一个期限：同一 target/kind 未 ack 前不重复告警（去重）
        self.clock.advance(seconds=int(DEFAULTS["ack_timeout"]) + 1)
        self.sup.dispatch_notifications()
        still = [a for a in self.ledger.list_alerts(state="new", limit=200)
                 if a["kind"] == "wake_observation_unknown"]
        self.assertEqual(len(still), 1, "未 ack 的阻塞告警必须去重")

    def test_healthy_target_not_starved_by_another_unknown(self):
        bad = self.enqueue("bad-unknown", target="ses_bad", priority=10)
        good = self.enqueue("good-clear", target="ses_good", priority=1,
                            kind="result_ready")
        self.client.fail("inbox", TransportError("/inbox", "probe"), times=1)
        self.sup.dispatch_notifications()
        # bad 目标查询失败被抑制；good 目标照常投递 → 正常目标不饥饿
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertEqual(self.client.prompt_calls[0]["session_id"], "ses_good")
        self.assertEqual(self.ledger.get_notification(bad["id"])["state"], "pending")
        self.assertEqual(self.ledger.get_notification(good["id"])["state"], "sent")


class TestForeignQueueHead(WakeRuntimeBase):
    def test_foreign_head_blocks_sending_and_forms_deduped_responsibility(self):
        n = self.enqueue("new-result", kind="result_ready")
        self.client.set_inbox("ses_coord", [{"id": "msg_parked", "delivery": "queue"}])
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [], "不绕过陌生队头发新输入")
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")
        self.assertEqual(self.alert_kinds(), [], "首次阻塞只排期，不立刻告警")
        # 跨过有界期限 → 可见诊断责任（owner/next_action），且去重
        self.clock.advance(seconds=int(DEFAULTS["ack_timeout"]) + 1)
        self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [], "陌生队头仍在时不能自动投递")
        blocked = [a for a in self.ledger.list_alerts(state="new", limit=200)
                   if "inbox" in a["kind"] or "foreign_input" in a["kind"]]
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["kind"], "foreign_input_blocked")
        detail = blocked[0]["title"] + "|" + blocked[0]["detail"]
        self.assertIn("ses_coord", detail)
        self.assertIn("人工核对", detail, "阻塞责任必须给出 owner/下一动作")
        self.assertIn("不自动", detail)
        # 没有调用任何 steer/delete 陌生输入
        self.assertEqual(len(self.client.inbox("ses_coord")), 1)
        # 再跨期限：未 ack 前不重复
        self.clock.advance(seconds=int(DEFAULTS["ack_timeout"]) + 1)
        self.sup.dispatch_notifications()
        still = [a for a in self.ledger.list_alerts(state="new", limit=200)
                 if a["kind"] == "foreign_input_blocked"]
        self.assertEqual(len(still), 1, "同一 foreign 阻塞未 ack 前只告警一次")

    def test_own_queued_input_is_wait_not_foreign_alert(self):
        # 本系统已投递的通知消息仍在 inbox：这是自有输入等待消费，不是 foreign
        n1 = self.enqueue("first-sent", kind="result_ready")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        msg = self.client.prompt_calls[0]["message_id"]
        self.client.set_inbox("ses_coord", [{"id": msg, "delivery": "queue"}])
        n2 = self.enqueue("second-wait", kind="result_ready")
        self.clock.advance(seconds=10)
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1, "自有输入排队时不追加")
        self.assertEqual(self.ledger.get_notification(n2["id"])["state"], "pending")
        self.assertFalse([k for k in self.alert_kinds()
                          if "foreign_input" in k], "自有输入不是 foreign 责任")
        raw = self.ledger.get_meta("wake-diagnostic:ses_coord")
        self.assertIn("inbox-pending-self", raw)


class TestIdleOwnWakeRecovery(WakeRuntimeBase):
    def _stale_sent_wake(self):
        """模拟中断后的重投：wake 已入 inbox，通知被中断降为 pending。"""
        n = self.enqueue("stale-wake", kind="result_ready")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        message_id = self.client.prompt_calls[0]["message_id"]
        self.ledger.defer_notifications_for_session(
            "ses_coord", next_attempt_at=self.clock.now_ms(),
            reason="coordinator-interrupted")
        self.client.set_inbox("ses_coord", [{
            "id": message_id, "delivery": "queue",
            "time": {"created": self.clock.now_ms()},
        }])
        self.clock.advance(seconds=int(DEFAULTS["wake_inbox_recover_grace"]) + 1)
        return n, message_id

    def test_pending_own_wake_is_not_foreign(self):
        n, message_id = self._stale_sent_wake()
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")
        # 宽限内只等待，不告警、不追加 prompt
        self.client.set_inbox("ses_coord", [{
            "id": message_id, "delivery": "queue",
            "time": {"created": self.clock.now_ms()},
        }])
        self.sup.dispatch_notifications()
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")
        self.assertFalse([k for k in self.alert_kinds()
                          if "foreign_input" in k],
                         "pending 的自有通知消息仍必须识别为自有")
        raw = self.ledger.get_meta("wake-diagnostic:ses_coord")
        self.assertIn("inbox-pending-self", raw)

    def test_idle_stale_own_wake_steers_original_id_without_new_prompt(self):
        n, message_id = self._stale_sent_wake()
        attempts_before = self.ledger.get_notification(n["id"])["attempts"]
        prompt_calls_before = len(self.client.prompt_calls)
        with patch.object(self.client, "update_inbox_delivery",
                          wraps=self.client.update_inbox_delivery) as steer:
            summary = self.sup.dispatch_notifications()
        self.assertEqual(summary["suppressed"], 1)
        self.assertEqual(steer.call_count, 1, "空闲自有排队唤醒只 steer 一次")
        self.assertEqual(steer.call_args.args, ("ses_coord", message_id))
        self.assertEqual(steer.call_args.kwargs["delivery"], "steer")
        self.assertEqual(len(self.client.prompt_calls), prompt_calls_before,
                         "原 ID steer 不得另造新 prompt")
        row = self.ledger.get_notification(n["id"])
        self.assertEqual(row["state"], "sent", "恢复后通知回到 sent 等待 ACK")
        self.assertEqual(row["attempts"], attempts_before, "恢复不重复计 attempts")
        inbox = self.client.inbox("ses_coord")
        self.assertEqual(inbox[0]["delivery"], "steer")
        audits = self.sup.conn.execute(
            "SELECT detail FROM audit WHERE action='wake.recovered' "
            "AND entity_id='ses_coord' ORDER BY id DESC LIMIT 1").fetchone()
        self.assertIsNotNone(audits)
        self.assertIn(message_id, audits["detail"])

    def test_active_target_is_not_steered(self):
        n, message_id = self._stale_sent_wake()
        self.client.set_active(["ses_coord"])
        with patch.object(self.client, "update_inbox_delivery",
                          wraps=self.client.update_inbox_delivery) as steer:
            self.sup.dispatch_notifications()
        self.assertEqual(steer.call_count, 0, "活跃目标不得自动 steer/改写队列")
        self.assertEqual(self.client.inbox("ses_coord")[0]["delivery"], "queue")
        self.assertEqual(self.ledger.get_notification(n["id"])["state"], "pending")

    def test_stuck_steer_alerts_once_after_ack_timeout(self):
        n, message_id = self._stale_sent_wake()
        with patch.object(self.client, "update_inbox_delivery",
                          wraps=self.client.update_inbox_delivery) as steer:
            self.sup.dispatch_notifications()
        self.assertEqual(steer.call_count, 1)
        self.clock.advance(seconds=int(DEFAULTS["ack_timeout"]) + 1)
        self.enqueue("new-due-after-stuck", kind="result_ready")
        with patch.object(self.client, "update_inbox_delivery",
                          wraps=self.client.update_inbox_delivery) as steer2:
            self.sup.dispatch_notifications()
        self.assertEqual(steer2.call_count, 0, "同一条消息不得重复 steer")
        stuck = [a for a in self.ledger.list_alerts(state="new", limit=200)
                 if a["kind"] == "wake_recover_stuck"]
        self.assertEqual(len(stuck), 1)
        self.assertIn(message_id, stuck[0]["detail"])


class TestClaimBasedCompose(WakeRuntimeBase):
    def helper_task_and_old_attempt(self):
        task = self.ledger.create_task(project="p", title="gen")
        old = self.ledger.create_attempt(
            task["id"], directory=str(self.home / "old"), session_id="ses_old")
        self.ledger.release_site(old["id"], confirmed=True,
                                 external_stopped=True, actor="test")
        return task, old

    def test_mixed_claim_removes_old_member_from_body_and_metadata(self):
        task, old = self.helper_task_and_old_attempt()
        stale = self.enqueue("old-result", kind="result_ready", task=task, attempt=old)
        other = self.enqueue("current-action", kind="result_ready")
        claim = self.ledger.claim_notifications

        def interleave(ids, owner):
            self.ledger.create_attempt(
                task["id"], directory=str(self.home / "new"), session_id="ses_new")
            return claim(ids, owner)

        with patch.object(self.ledger, "claim_notifications", side_effect=interleave):
            self.sup.dispatch_notifications()
        stale_row = self.ledger.get_notification(stale["id"])
        self.assertEqual(stale_row["state"], "dead", "旧代次项在 claim 内被剔除")
        self.assertEqual(stale_row["attempts"], 0, "抑制不计发送预算")
        self.assertIsNone(stale_row["acked_at"], "抑制不是 ACK")
        self.assertEqual(len(self.client.prompt_calls), 1)
        call = self.client.prompt_calls[0]
        self.assertNotIn(stale["id"], call["text"], "被剔除旧项不能泄漏进正文")
        self.assertNotIn(stale["id"], call["metadata"]["oc_orch_notifications"])
        self.assertIn(other["id"], call["text"])
        self.assertIn(other["id"], call["metadata"]["oc_orch_notifications"])
        self.assertEqual(set(call["metadata"]["oc_orch_notifications"]),
                         {n["id"] for n in self.ledger.list_notifications(
                             target_session="ses_coord", state="sent", limit=10)})

    def test_compose_wake_keeps_version_missing_and_explicit_ack_semantics(self):
        items = [{
            "id": "N-1", "kind": "result_ready", "task_id": "T-1",
            "attempt_id": "A-1",
            "payload": {
                "classification": "COLLECTION_REQUIRED",
                "result_version": "v3",
                "summary": "收集完成，待人工验收",
                "missing_deliverables": ["out.txt"],
                "stale_deliverables": [],
                "unverified_deliverables": ["check.sh"],
                "ready_for_verification": False,
                "next_action": "先 collect 再 verify",
            },
        }]
        text = self.sup.compose_wake("ses_coord", items)
        self.assertIn("result_version=v3", text)
        self.assertIn("missing=out.txt", text)
        self.assertIn("unverified(未执行检查)=check.sh", text)
        self.assertIn("显式 ack", text)
        self.assertIn("不是验收结论", text)


class TestSuppressionDoesNotDropCleanupOrWrite(WakeRuntimeBase):
    def test_state_suppression_defers_not_drop_cleanup_and_keeps_write(self):
        task = self.ledger.create_task(project="p", title="gen")
        old = self.ledger.create_attempt(
            task["id"], directory=str(self.home / "old"), session_id="ses_old")
        # 当前已推进到新一代尝试；旧尝试仍持有写权（site 未释放）
        self.ledger.create_attempt(
            task["id"], directory=str(self.home / "new"), session_id="ses_new")
        old_after = self.ledger.get_attempt(old["id"])
        self.assertIsNone(old_after["site_released_at"], "写权应保持")
        # 旧执行仍有清理责任（后台 shell 活动）→ 清理责任通知
        self.client.add_shell(directory=str(self.home / "old"), shell_id="sh_old",
                              status="running",
                              metadata={"sessionID": "ses_old"})
        cleanup = self.enqueue("cleanup-needed", kind="side_effects_running",
                               task=task, attempt=old, priority=7)
        # 目标有陌生队头 → 抑制（即使无队头，旧代次清理也走 ledger 策略）
        self.client.set_inbox("ses_coord", [{"id": "msg_foreign", "delivery": "queue"}])
        self.sup.dispatch_notifications()
        row = self.ledger.get_notification(cleanup["id"])
        # 旧代次清理责任按策略表达：dead（不烧预算、不当 ACK）+ 去重清理告警；
        # 写权与责任不因纯状态过滤被释放/丢失（见下方告警与 site 断言）。
        self.assertEqual(row["state"], "dead")
        self.assertEqual(row["attempts"], 0, "抑制不计发送预算")
        self.assertIsNone(row["acked_at"], "抑制不是 ACK")
        self.assertIn("cleanup", row["last_error"])
        # 去重可见清理责任（owner/下一动作由告警携带）
        cleanup_alerts = [a for a in self.ledger.list_alerts(state="new", limit=200)
                          if a["kind"] == "notification_cleanup_required"
                          and a.get("attempt_id") == old["id"]]
        self.assertTrue(cleanup_alerts, "旧执行清理责任必须形成去重可见告警")
        self.assertIn("write-site-held", cleanup_alerts[0]["detail"]
                      or cleanup_alerts[0]["dedupe_key"])
        # 清理与写权不受抑制影响：旧尝试仍持有写现场；后续对账不复活清理通知
        self.assertIsNone(self.ledger.get_attempt(old["id"])["site_released_at"])
        self.clock.advance(seconds=min(300, int(DEFAULTS["ack_timeout"])) + 1)
        self.sup.dispatch_notifications()
        again = self.ledger.get_notification(cleanup["id"])
        self.assertEqual(again["state"], "dead", "清理责任不因对账复活/重发")
        self.assertIsNone(self.ledger.get_attempt(old["id"])["site_released_at"])


class TestStalePreparedReclaim(WakeRuntimeBase):
    """S2 活性：快速重启/连续调谐的有界回收；possible/unknown 严格保原 ID。

    覆盖规范 review 观察4 反例：旧 Sup 在 mark_possible 前崩溃（prepared、从未
    POST），快速重启错过启动 recover_inflight 后，正常周期/dispatch 必须对成熟
    孤立槽有界回收；已 possible/unknown 输入不得被释放/换身份。
    """

    def _crash_before_possible(self, key="fast-crash"):
        notice = self.enqueue(key)
        # 组合发送门现直接调用同事务窄方法；保持原崩溃位置和全部活性断言。
        with patch.object(self.ledger, "_mark_input_possible_in_tx",
                          side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.sup.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [], "崩溃点在可能发送标记之前")
        return notice

    def test_fast_restart_before_claim_expiry_recovers_via_dispatch(self):
        notice = self._crash_before_possible()
        receipt = self.ledger.list_pending_inputs(
            target_session="ses_coord", outstanding=True)[0]
        self.assertEqual(receipt["admission_state"], "prepared")
        # 快速重启：120s claim 宽限未过，首次 recover_inflight 为 0。
        self.clock.advance(seconds=1)
        recovered = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                               client=self.client, clock=self.clock)
        self.addCleanup(recovered.conn.close)
        self.assertEqual(recovered.ledger.recover_inflight(), 0)
        recovered.dispatch_notifications()
        self.assertEqual(self.client.prompt_calls, [], "claim 宽限内不抢发")
        # 不依赖第二次 startup、不依赖新通知：正常 dispatch 在 prepared 对账
        # 期限过后必须回收并恢复原责任（总计只 POST 一次）。
        deadline = receipt.get("reconcile_deadline") or (
            receipt["prepared_at"] + int(DEFAULTS["ack_timeout"]) * 1000)
        self.clock.advance(wall=max(1, deadline - self.clock.now_ms()) + 1000)
        recovered.dispatch_notifications()
        recovered.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1,
                         "快速重启错过首次 claim 回收后不能让 prepared/inflight 永久滞留")
        self.assertEqual(self.ledger.get_notification(notice["id"])["state"], "sent")

    def test_continuous_tuning_recovers_without_new_notice(self):
        notice = self._crash_before_possible("tuning-crash")
        receipt = self.ledger.list_pending_inputs(outstanding=True)[0]
        deadline = receipt.get("reconcile_deadline") or (
            receipt["prepared_at"] + int(DEFAULTS["ack_timeout"]) * 1000)
        recovered = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                               client=self.client, clock=self.clock)
        self.addCleanup(recovered.conn.close)
        recovered.cycle(dispatch=False)  # 期限前：无新通知也不恢复、不抢发
        self.assertEqual(self.client.prompt_calls, [])
        self.clock.advance(wall=max(1, deadline - self.clock.now_ms()) + 1000)
        recovered.cycle(dispatch=False)  # 有界回收：成熟孤立 prepared 槽 + inflight 归还
        recovered.cycle(dispatch=True)   # 随后 dispatch 完成投递
        self.assertEqual(self.ledger.get_notification(notice["id"])["state"], "sent")
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_possible_input_not_released_or_reidentified_by_reclaim(self):
        self.enqueue("possible-keep")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        first_id = self.client.prompt_calls[0]["message_id"]
        self.clock.advance(seconds=int(DEFAULTS["ack_timeout"]) * 3)
        self.sup.reconcile_outstanding_inputs()
        self.sup.dispatch_notifications()
        rows = self.ledger.list_pending_inputs(
            target_session="ses_coord", outstanding=True)
        self.assertEqual(len(rows), 1, "possible 输入不得被周期回收释放")
        self.assertIn(rows[0]["admission_state"], ("possible", "admitted"),
                      "已发出输入保留未决身份（对账可推进到 admitted）")
        self.assertEqual(rows[0]["message_id"], first_id, "已发出输入保持原 ID")
        self.assertEqual(len(self.client.prompt_calls), 1, "不因回收换身份重复 POST")

    def test_unknown_query_failure_keeps_identity_without_blind_repost(self):
        notice = self.enqueue("unknown-keep")
        self.client.fail("prompt", TransportError("/prompt", "lost"))
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        original = self.client.prompt_calls[0]["message_id"]
        self.clock.advance(seconds=31)
        self.client.fail("get_message", TransportError("/message", "down"), times=10)
        self.sup.reconcile_outstanding_inputs()
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1,
                         "查询失败≠明确未受理，不得盲重投原 ID")
        rows = self.ledger.list_pending_inputs(
            target_session="ses_coord", outstanding=True)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["admission_state"], "unknown", "unknown 槽不得被释放")
        self.assertEqual(rows[0]["message_id"], original)
        self.assertEqual(self.ledger.get_notification(notice["id"])["message_id"], original)


if __name__ == "__main__":
    unittest.main()

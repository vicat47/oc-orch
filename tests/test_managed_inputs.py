"""S4 受管输入用例契约（真实 B 存储 + 临时 SQLite + FakeClient + FakeClock）。

覆盖本组红→绿重点：record 零 Client、park 必填零 I/O、resume 位、request_id 幂等、
到期只告警、白名单 zero 扫描、foreign 挡 flush、旧 succeeded 不假 execution、
DELETE 已 delivered 不假 cancel、allow_remote=False 本地到期、人工 resume 零 prompt。
严格离线：只用临时 home/FakeClient，不读生产、不连真实 API。
"""

import tempfile
import unittest
from unittest.mock import patch

from oc_orch.adapters.managed_input_ledger import ManagedInputLedger
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.target_gate import TargetGate
from oc_orch.application.managed_inputs import ManagedInputs
from oc_orch.db import init
from oc_orch.domain.errors import ActionError, LedgerError
from tests.fakes import FakeClient, FakeClock, TransportError


def make_managed_inputs(conn, home, *, client, clock):
    """测试本地装配：不依赖 composition（其他 worker 独占接线）。"""
    base = Ledger(conn, home, clock=clock)
    return ManagedInputs(ManagedInputLedger(conn, base), client=client,
                         clock=clock, target_gate=TargetGate(conn, clock))


class ManagedInputsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(
            prefix="managed-c-", dir="/tmp/opencode")
        self.addCleanup(self.temp.cleanup)
        self.conn = init(self.temp.name)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock()
        self.client = FakeClient()
        self.client.set_session("ses_ok", outcome="succeeded")
        self.svc = self.make(self.client)
        self.register()

    def make(self, client):
        return make_managed_inputs(self.conn, self.temp.name,
                                   client=lambda: client, clock=self.clock)

    def forbidden_client(self):
        def factory():
            self.fail("该路径不得构造 Client")
        return factory

    def register(self, sid="ses_ok", *, owner="op", threshold=1000):
        return self.svc.register_watch(sid, owner=owner, project="proj",
                                       max_pending_age_ms=threshold)

    def deliver(self, sid="ses_ok"):
        """FakeClient 投递；真实 V2 user 对象可以没有 sessionID。"""
        self.client.deliver_inbox(sid, at=self.clock.now_ms())

    def send(self, *, intent="park", request_id="r1", **extra):
        fields = ({"expires_at": self.clock.now_ms() + 1000,
                   "flush_condition": "human-confirm"} if intent == "park" else {})
        fields.update(extra)
        return self.svc.send("ses_ok", intent=intent, text="payload",
                             owner="op", request_id=request_id,
                             project="proj", **fields)["input"]

    def alerts(self):
        return self.conn.execute(
            "SELECT detail FROM alerts WHERE state='new'").fetchall()

    # ---- record 零 Client ---------------------------------------------------
    def test_record_never_constructs_client_or_prompts(self):
        svc = make_managed_inputs(self.conn, self.temp.name,
                                  client=self.forbidden_client(), clock=self.clock)
        result = svc.send("ses_offline", intent="record", text="note",
                          owner="op", request_id="rec-1")
        view = result["input"]
        self.assertEqual(view["intent"], "record")
        self.assertEqual(view["delivery"], "not-applicable")
        self.assertNotIn(view["execution"], ("succeeded", "executed"))
        self.assertEqual(self.client.prompt_calls, [])
        again = svc.send("ses_offline", intent="record", text="note",
                         owner="op", request_id="rec-1")
        self.assertFalse(again["created"])
        self.assertEqual(again["input"]["input_id"], view["input_id"])

    # ---- park 必填在任何 I/O 前拒绝 ----------------------------------------
    def test_park_missing_responsibility_rejected_before_io(self):
        with patch.object(self.client, "prompt") as prompt, \
                patch.object(self.client, "inbox") as inbox:
            with self.assertRaises((ActionError, LedgerError)):
                self.svc.send("ses_ok", intent="park", text="x", owner="op",
                              request_id="bad-park")
        prompt.assert_not_called()
        inbox.assert_not_called()

    # ---- 显式 resume 位 -----------------------------------------------------
    def test_execute_and_park_preserve_explicit_resume(self):
        executed = self.send(intent="execute", request_id="ex-1")
        self.assertIs(self.client.prompt_calls[-1]["resume"], True)
        self.assertEqual(executed["message_id"],
                         self.client.prompt_calls[-1]["message_id"])
        parked = self.send(intent="park", request_id="pk-1")
        self.assertIs(self.client.prompt_calls[-1]["resume"], False)
        self.assertEqual(parked["message_id"],
                         self.client.prompt_calls[-1]["message_id"])

    # ---- request_id 幂等：不新 POST / 不改身份 -----------------------------
    def test_same_request_does_not_repeat_prompt_or_identity(self):
        first = self.send()
        second = self.send()
        self.assertEqual(first["input_id"], second["input_id"])
        self.assertEqual(first["message_id"], second["message_id"])
        self.assertEqual(len(self.client.prompt_calls), 1)

    # ---- 白名单 -------------------------------------------------------------
    def test_unregistered_target_rejected_without_persisting(self):
        with self.assertRaises(ActionError):
            self.svc.send("ses_unknown", intent="execute", text="x",
                          owner="op", request_id="u-1")
        self.assertEqual(self.svc.list_inputs()["inputs"], [])
        self.assertEqual(self.client.prompt_calls, [])

    def test_wrong_owner_rejected(self):
        with self.assertRaises(ActionError):
            self.svc.send("ses_ok", intent="park", text="x", owner="intruder",
                          request_id="o-1",
                          expires_at=self.clock.now_ms() + 1000,
                          flush_condition="c")
        self.assertEqual(self.client.prompt_calls, [])

    # ---- execute 队列/闸门 fail-closed -------------------------------------
    def test_execute_blocked_by_foreign_queue_head(self):
        self.client.set_inbox("ses_ok", [{"id": "msg_foreign"}])
        with self.assertRaises(ActionError):
            self.send(intent="execute", request_id="ex-q")
        self.assertEqual(self.client.prompt_calls, [])
        inputs = self.svc.list_inputs()["inputs"]
        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0]["delivery"], "pending")

    def test_execute_blocked_by_gate(self):
        TargetGate(self.conn, self.clock).block_manual(
            "ses_ok", reason="operator stop")
        with self.assertRaises(ActionError):
            self.send(intent="execute", request_id="ex-g")
        self.assertEqual(self.client.prompt_calls, [])

    # ---- 响应未知保原身份、不重投 ------------------------------------------
    def test_send_unknown_keeps_identity_and_does_not_retry(self):
        self.client.fail("prompt", TransportError("/prompt", "lost"))
        with self.assertRaises(ActionError):
            self.send(intent="execute", request_id="ex-unknown")
        inputs = self.svc.list_inputs()["inputs"]
        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0]["admission"], "unknown")
        self.assertEqual(inputs[0]["delivery"], "unknown")
        self.assertEqual(len(self.client.prompt_calls), 1)  # 未自动重投

    # ---- reconcile：旧 succeeded 不假 execution ----------------------------
    def test_reconcile_delivers_without_faking_execution(self):
        row = self.send()
        self.clock.advance(seconds=1)
        self.deliver("ses_ok")
        view = self.svc.reconcile(row["input_id"])["input"]
        self.assertEqual(view["delivery"], "delivered")
        self.assertEqual(view["execution"], "unknown")
        self.assertEqual(view["business_ack"], "unacknowledged")

    def test_reconcile_query_failure_is_not_delivery(self):
        row = self.send()
        self.client.fail("inbox", TransportError("/inbox", "down"))
        self.client.fail("get_message", TransportError("/m", "down"))
        view = self.svc.reconcile(row["input_id"])["input"]
        self.assertNotEqual(view["delivery"], "delivered")

    # ---- flush --------------------------------------------------------------
    def test_flush_requires_confirmation_and_rejects_foreign(self):
        row = self.send()
        self.client.set_inbox("ses_ok", [{"id": "msg_foreign"},
                                         {"id": row["message_id"]}])
        with patch.object(self.client, "update_inbox_delivery") as steer:
            with self.assertRaises((ActionError, LedgerError)):
                self.svc.flush(row["input_id"], owner="op", confirm="wrong",
                               expected_revision=row["revision"], reason="manual")
            current = self.svc.get(row["input_id"])["input"]
            with self.assertRaises((ActionError, LedgerError)):
                self.svc.flush(row["input_id"], owner="op",
                               confirm=row["input_id"],
                               expected_revision=current["revision"],
                               reason="manual")
        steer.assert_not_called()
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_flush_unique_own_target_steers_original_id(self):
        row = self.send()
        result = self.svc.flush(row["input_id"], owner="op",
                                confirm=row["input_id"],
                                expected_revision=row["revision"], reason="do it")
        self.assertTrue(result["changed"])
        steered = self.client.inboxes["ses_ok"][0]
        self.assertEqual(steered["id"], row["message_id"])
        self.assertEqual(steered["delivery"], "steer")
        self.assertEqual(len(self.client.prompt_calls), 1)  # 未 POST 新输入

    def test_flush_target_absent_is_no_op_not_fake_execution(self):
        row = self.send()
        self.client.set_inbox("ses_ok", [])
        with patch.object(self.client, "update_inbox_delivery") as steer:
            result = self.svc.flush(row["input_id"], owner="op",
                                    confirm=row["input_id"],
                                    expected_revision=row["revision"],
                                    reason="do it")
        self.assertFalse(result["changed"])
        self.assertEqual(result["reason"], "target-absent")
        steer.assert_not_called()

    # ---- cancel -------------------------------------------------------------
    def test_delete_noop_with_delivered_message_is_not_cancelled(self):
        row = self.send()
        self.clock.advance(seconds=1)
        self.deliver("ses_ok")
        current = self.svc.get(row["input_id"])["input"]
        view = self.svc.cancel(row["input_id"], owner="op",
                               confirm=row["input_id"],
                               expected_revision=current["revision"],
                               reason="check")["input"]
        self.assertEqual(view["delivery"], "delivered")
        self.assertNotEqual(view["business_ack"], "acknowledged")

    def test_cancel_queued_input_without_atomic_proof_stays_unknown(self):
        # 旧期望 cancelled 不成立（见 C-safety-pre-fix-red.log / C-api §5）：
        # 当前 Client cancel 返回 None，无法原子证明取消；非历史命中一律 unknown。
        row = self.send()
        self.assertIn(row["message_id"],
                      [i["id"] for i in self.client.inbox("ses_ok")])
        view = self.svc.cancel(row["input_id"], owner="op",
                               confirm=row["input_id"],
                               expected_revision=row["revision"],
                               reason="stop")["input"]
        self.assertEqual(view["delivery"], "unknown")

    def test_cancel_absent_without_evidence_stays_unknown(self):
        row = self.send()
        self.client.set_inbox("ses_ok", [])
        view = self.svc.cancel(row["input_id"], owner="op",
                               confirm=row["input_id"],
                               expected_revision=row["revision"],
                               reason="stop")["input"]
        self.assertEqual(view["delivery"], "unknown")

    def test_cancel_delete_error_does_not_repeat(self):
        row = self.send()
        calls = []
        original = self.client.cancel_inbox

        def boom(session_id, message_id):
            calls.append(message_id)
            raise TransportError("/inbox", "lost")

        with patch.object(self.client, "cancel_inbox", side_effect=boom):
            result = self.svc.cancel(row["input_id"], owner="op",
                                     confirm=row["input_id"],
                                     expected_revision=row["revision"],
                                     reason="stop")
        self.assertEqual(len(calls), 1)
        self.assertIn(result["input"]["delivery"],
                      ("unknown", "pending", "cancelled"))
        self.assertTrue(original)

    # ---- resume_target：人工、零 prompt ------------------------------------
    def test_resume_target_is_explicit_and_never_prompts(self):
        gate = TargetGate(self.conn, self.clock)
        gate.block_manual("ses_ok", reason="operator stop")
        state = gate.get("ses_ok")
        with self.assertRaises(ActionError):
            self.svc.resume_target("ses_ok", owner="op", confirm="wrong",
                                   expected_revision=state["revision"],
                                   reason="resume")
        result = self.svc.resume_target("ses_ok", owner="op", confirm="ses_ok",
                                        expected_revision=state["revision"],
                                        reason="resume")
        self.assertTrue(result["changed"])
        self.assertEqual(self.client.prompt_calls, [])

    # ---- observe_registered -------------------------------------------------
    def test_local_expiry_alerts_without_remote(self):
        self.send()
        self.clock.advance(seconds=2)
        local = make_managed_inputs(self.conn, self.temp.name,
                                    client=self.forbidden_client(),
                                    clock=self.clock)
        out = local.observe_registered(allow_remote=False)
        self.assertGreater(len(self.alerts()), 0)
        self.assertEqual(out["observed"], [])
        self.assertTrue(out["alerts"])

    def test_empty_whitelist_observation_never_constructs_client(self):
        current = self.svc.list_watches()["watches"][0]
        self.svc.unregister_watch("ses_ok", owner="op",
                                  expected_revision=current["revision"],
                                  confirm="ses_ok")
        svc = make_managed_inputs(self.conn, self.temp.name,
                                  client=self.forbidden_client(),
                                  clock=self.clock)
        out = svc.observe_registered()
        self.assertEqual(out["observed"], [])

    def test_whitelist_observation_only_queries_registered(self):
        self.client.set_session("ses_other")
        queried = []
        original = self.client.inbox

        def check(sid):
            queried.append(sid)
            return original(sid)

        with patch.object(self.client, "inbox", side_effect=check), \
                patch.object(self.client, "list_sessions",
                             side_effect=AssertionError("禁止全局扫描")), \
                patch.object(self.client, "active_sessions",
                             side_effect=AssertionError("禁止全局 active")):
            self.svc.observe_registered()
        self.assertEqual(queried, ["ses_ok"])

    def test_revoked_watch_is_not_observed_or_reenabled(self):
        first = self.register()["watch"]
        self.svc.unregister_watch("ses_ok", owner="op",
                                  expected_revision=first["revision"],
                                  confirm="ses_ok")
        queried = []
        original = self.client.inbox

        def check(sid):
            queried.append(sid)
            return original(sid)

        with patch.object(self.client, "inbox", side_effect=check):
            self.svc.observe_registered()
        self.assertEqual(queried, [])
        watches = self.svc.list_watches()["watches"]
        self.assertEqual(len(watches), 1)
        self.assertFalse(watches[0]["enabled"])

    # ---- 列表不泄露正文 / get 显示正文 ------------------------------------
    def test_list_hides_body_but_get_shows_it(self):
        row = self.send()
        listed = self.svc.list_inputs()["inputs"][0]
        self.assertNotIn("body", listed)
        self.assertIn("body", self.svc.get(row["input_id"])["input"])

    # ---- 未知操作不重放 / 轮转 / 到期持久告警 ------------------------------
    def test_unknown_cancel_not_replayed_by_fresh_revision(self):
        row = self.send()
        calls = []

        def boom(*args, **kwargs):
            calls.append(args)
            raise TransportError("/inbox", "lost")

        with patch.object(self.client, "cancel_inbox", side_effect=boom):
            self.svc.cancel(row["input_id"], owner="op",
                            confirm=row["input_id"],
                            expected_revision=row["revision"], reason="x")
            current = self.svc.get(row["input_id"])["input"]
            self.svc.cancel(row["input_id"], owner="op",
                            confirm=row["input_id"],
                            expected_revision=current["revision"], reason="x")
        self.assertEqual(len(calls), 1, "未知 DELETE 不得因新 revision 重放")

    def test_watch_rounds_rotate_all_enabled(self):
        for sid in ("sa", "sb", "sc", "sd", "se"):
            self.client.set_session(sid)
            self.svc.register_watch(sid, owner="op", project="p",
                                    max_pending_age_ms=1000)
        seen = []
        original = self.client.inbox

        def observe(sid):
            seen.append(sid)
            return original(sid)

        with patch.object(self.client, "inbox", side_effect=observe):
            for _ in range(3):
                before = len(seen)
                self.svc.observe_registered(limit=2)
                self.assertLessEqual(len(seen) - before, 2)
        self.assertTrue({"sa", "sb", "sc", "sd", "se"}.issubset(set(seen)),
                        "有界观察应轮转覆盖全部 enabled，而非只查前缀")

    def test_watch_truncation_reports_total_lower_bound_and_coverage(self):
        self.svc.register_watch("ses_many", owner="op", project="p",
                                max_pending_age_ms=1000)
        now = self.clock.now_ms()
        self.client.set_inbox("ses_many", [
            {"id": f"msg_many_{i:02d}", "time": {"created": now - 10000}}
            for i in range(55)])
        out = self.svc.observe_registered()
        obs = next(o for o in out["observed"]
                   if o["session_id"] == "ses_many")
        self.assertIsNone(obs["pending"])
        self.assertEqual(obs["pending_total"], 55)
        self.assertEqual(obs["pending_lower_bound"], 50)
        self.assertTrue(obs["truncated"])
        self.assertEqual(obs["coverage"], "partial")
        self.assertIn("覆盖不足", obs["next_action"])
        watch = next(w for w in self.svc.list_watches()["watches"]
                     if w["session_id"] == "ses_many")
        last = watch["last_observation"]
        self.assertEqual(last["pending_total"], 55)
        self.assertEqual(last["pending_lower_bound"], 50)
        self.assertTrue(last["truncated"])
        self.assertEqual(last["coverage"], "partial")
        alerts = self.conn.execute(
            "SELECT detail FROM alerts WHERE state='new'").fetchall()
        detail = "\n".join(a["detail"] for a in alerts)
        self.assertIn('"pending_total":55', detail)
        self.assertIn('"pending_lower_bound":50', detail)
        self.assertIn('"truncated":true', detail)

    def test_overdue_pending_creates_owner_alert(self):
        self.send()
        self.client.inboxes["ses_ok"][0]["time"]["created"] = \
            self.clock.now_ms() - 10000
        self.svc.observe_registered()
        details = [a["detail"] for a in self.alerts()]
        self.assertTrue(any("ses_ok" in d and "op" in d for d in details),
                        "超期 pending 必须形成持久 owner 责任，而非仅瞬时 observed")

    def test_unresolved_flush_validates_owner_and_revision(self):
        row = self.send()
        with patch.object(self.client, "cancel_inbox",
                          side_effect=TransportError("/inbox", "lost")):
            self.svc.cancel(row["input_id"], owner="op",
                            confirm=row["input_id"],
                            expected_revision=row["revision"], reason="x")
        current = self.svc.get(row["input_id"])["input"]
        # 未收敛也不能绕过 B 的 owner 先验。
        with self.assertRaises(LedgerError):
            self.svc.flush(row["input_id"], owner="intruder",
                           confirm=row["input_id"],
                           expected_revision=current["revision"], reason="x")
        # 旧 revision 也不得得到成功回执。
        with self.assertRaises(LedgerError):
            self.svc.flush(row["input_id"], owner="op",
                           confirm=row["input_id"],
                           expected_revision=max(1, current["revision"] - 1),
                           reason="x")

    def test_unknown_cancel_then_flush_does_not_write(self):
        row = self.send()
        with patch.object(self.client, "cancel_inbox",
                          side_effect=TransportError("/inbox", "lost")):
            self.svc.cancel(row["input_id"], owner="op",
                            confirm=row["input_id"],
                            expected_revision=row["revision"], reason="x")
        current = self.svc.get(row["input_id"])["input"]
        with patch.object(self.client, "update_inbox_delivery") as steer:
            result = self.svc.flush(row["input_id"], owner="op",
                                    confirm=row["input_id"],
                                    expected_revision=current["revision"],
                                    reason="x")
        self.assertFalse(result["changed"])
        steer.assert_not_called()

    def test_age_alert_carries_oldest_message_id_and_owner(self):
        row = self.send()
        self.client.inboxes["ses_ok"][0]["time"]["created"] = \
            self.clock.now_ms() - 10000
        self.svc.observe_registered()
        details = [a["detail"] for a in self.alerts()]
        self.assertTrue(any(row["message_id"] in d and "op" in d for d in details),
                        "age 告警须保留可信最老原 messageID 与 owner")

    def test_persisted_last_observation_is_sanitized_without_client(self):
        self.send()
        self.client.inboxes["ses_ok"][0]["time"]["created"] = \
            self.clock.now_ms() - 10000
        self.svc.observe_registered()
        local = make_managed_inputs(self.conn, self.temp.name,
                                    client=self.forbidden_client(),
                                    clock=self.clock)
        watch = next(w for w in local.list_watches()["watches"]
                     if w["session_id"] == "ses_ok")
        obs = watch["last_observation"]
        self.assertIsNotNone(obs)
        self.assertNotIn("body", obs)
        self.assertNotIn("oldest_message_id", obs)  # 渲染白名单不含身份细节
        self.assertTrue(obs["aged_out"])
        self.assertTrue(obs["trusted"])
        self.assertIsNotNone(watch["last_observed_at"])

    def test_external_pending_age_and_owner_alert_without_managed_input(self):
        # 显式登记的真正外部分支（无 managed_input 行）：仍保留时间事实并出 owner 告警。
        self.client.set_inbox("ses_ok", [{
            "id": "msg_external", "type": "user", "delivery": "queue",
            "time": {"created": self.clock.now_ms() - 10000}}])
        out = self.svc.observe_registered()
        obs = next(o for o in out["observed"] if o["session_id"] == "ses_ok")
        self.assertEqual(obs["oldest_age_ms"], 10000)
        self.assertTrue(obs["aged_out"])
        self.assertIn("msg_external", obs["blocking_reason"])
        details = [a["detail"] for a in self.alerts()]
        self.assertTrue(any("msg_external" in d and "op" in d for d in details))
        # 不创建托管输入/Task、不 flush/cancel，队列不变。
        self.assertEqual(self.svc.list_inputs()["inputs"], [])
        self.assertEqual([i["id"] for i in self.client.inbox("ses_ok")],
                         ["msg_external"])
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 0)

    def test_missing_or_bad_created_time_does_not_fabricate_age(self):
        self.client.set_inbox("ses_ok", [
            {"id": "msg_a", "delivery": "queue", "time": "bad"},
            {"id": "msg_b", "delivery": "queue"},  # 无 time
            {"id": "msg_c", "delivery": "queue", "time": {"created": 0}},
        ])
        out = self.svc.observe_registered()
        obs = next(o for o in out["observed"] if o["session_id"] == "ses_ok")
        self.assertIsNone(obs["oldest_age_ms"])
        self.assertFalse(obs["aged_out"])

    # ---- watch revision CAS -------------------------------------------------
    def test_watch_revision_cas_and_owner_conflict(self):
        first = self.register()["watch"]
        self.assertEqual(first["revision"], 1)
        with self.assertRaises((ActionError, LedgerError)):
            self.register(owner="other")
        bumped = self.register(threshold=5000)["watch"]
        self.assertEqual(bumped["revision"], 2)
        with self.assertRaises(LedgerError):
            self.svc.unregister_watch("ses_ok", owner="op",
                                      expected_revision=1, confirm="ses_ok")
        removed = self.svc.unregister_watch("ses_ok", owner="op",
                                            expected_revision=2,
                                            confirm="ses_ok")["watch"]
        self.assertFalse(removed["enabled"])


if __name__ == "__main__":
    unittest.main()

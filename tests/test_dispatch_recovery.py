"""恢复派单的晚到事实与人工控制（fake + 真 SQLite）。"""

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.config import DEFAULTS
from oc_orch.domain.errors import TransportError
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock, complete_execution


class DispatchRecoveryTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-recovery-")
        self.addCleanup(tmp.cleanup)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.client = FakeClient()
        self.sup = Supervisor(tmp.name, cfg={**DEFAULTS, "desktop_alerts": False},
                              client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.task = self.ledger.create_task(project="p", title="recover", coordinator_session="ses_coord")
        self.att = self.ledger.create_attempt(
            self.task["id"], directory=tmp.name, session_id="ses_worker", dispatch_msg_id="msg_original",
            metadata={"dispatch": {"session_id_planned": "ses_worker", "prompt": "work",
                                    "create": {"directory": tmp.name}}},
        )

    def assert_control_blocks_recovery(self, control):
        if control == "stop_requested":
            self.ledger.stop_attempt(self.att["id"], actor="test")
        else:
            self.ledger.transition_task(self.task["id"], control)
        self.assertIsNone(self.sup.reconcile_dispatch(self.att))
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])

    def test_stale_snapshot_cannot_restart_cancelled_task(self):
        self.assert_control_blocks_recovery("cancelled")

    def test_stale_snapshot_cannot_restart_paused_task(self):
        self.assert_control_blocks_recovery("paused")

    def test_stale_snapshot_cannot_restart_stopped_attempt(self):
        self.assert_control_blocks_recovery("stop_requested")

    def test_old_attempt_is_not_automatically_redispatched(self):
        current = self.ledger.create_attempt(self.task["id"], directory=self.att["directory"] + "-new")
        self.assertIsNone(self.sup.reconcile_dispatch(self.att))
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_task(self.task["id"])["current_attempt_id"], current["id"])

    def test_cancel_during_lookup_prevents_prompt(self):
        self.client.set_session("ses_worker")
        original = self.client.get_session

        def lookup(sid):
            session = original(sid)
            self.ledger.transition_task(self.task["id"], "cancelled")
            self.ledger.stop_attempt(self.att["id"], actor="test")
            return session

        with patch.object(self.client, "get_session", side_effect=lookup):
            self.sup.reconcile_dispatch(self.att)
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.ledger.get_attempt(self.att["id"])["status"], "stop_requested")

    def test_confirmed_inbox_does_not_overwrite_late_terminal(self):
        self.client.set_session("ses_worker")

        def inbox(_sid):
            complete_execution(self.ledger, self.att["id"])
            return [{"id": "msg_original"}]

        with patch.object(self.client, "inbox", side_effect=inbox):
            self.sup.reconcile_dispatch(self.att)
        att = self.ledger.get_attempt(self.att["id"])
        self.assertEqual(att["status"], "terminal_succeeded")
        self.assertEqual(att["dispatch_state"], "confirmed")

    def test_lost_response_after_cancel_keeps_stop_intent(self):
        self.client.set_session("ses_worker")

        def prompt(*args, **kwargs):
            self.ledger.transition_task(self.task["id"], "cancelled")
            self.ledger.stop_attempt(self.att["id"], actor="test")
            raise TransportError("/prompt", "response lost")

        with patch.object(self.client, "prompt", side_effect=prompt):
            self.sup.reconcile_dispatch(self.att)
        att = self.ledger.get_attempt(self.att["id"])
        self.assertEqual(att["status"], "stop_requested")
        self.assertIsNotNone(att["stop_requested_at"])
        self.assertIsNone(att["site_released_at"])

    def test_persisted_retry_deadline_and_exhaustion_allow_only_reconciliation(self):
        self.client.set_session("ses_worker")

        def set_retry_count(count, next_at=None):
            # 重读当前 attempt 再合并 dispatch_retry 子字段：保留并发封存的
            # execution（B 的 execution.terminal 保护要求不得用旧 metadata 覆盖）。
            fresh = self.ledger.get_attempt(self.att["id"])
            meta = {**fresh["metadata"], "dispatch_retry": {
                "count": count,
                "next_at": (next_at if next_at is not None
                            else self.clock.now_ms() + 30_000)}}
            self.ledger.update_attempt(self.att["id"], fields={"metadata": meta},
                                       actor="test", reason="retry-meta")

        set_retry_count(1)
        self.sup.reconcile_dispatch(self.ledger.get_attempt(self.att["id"]))
        self.assertEqual(self.client.prompt_calls, [])
        self.clock.advance(seconds=31)
        set_retry_count(self.sup.cfg["dispatch_max_retries"])
        self.sup.reconcile_dispatch(self.ledger.get_attempt(self.att["id"]))
        self.assertEqual(self.client.prompt_calls, [])
        # 重试耗尽后仍查询同一消息事实，服务恢复时可确认，绝不换 ID。
        self.client.set_inbox("ses_worker", [{"id": "msg_original"}])
        out = self.sup.reconcile_dispatch(self.ledger.get_attempt(self.att["id"]))
        self.assertEqual(out["dispatch_state"], "confirmed")
        self.assertEqual(out["dispatch_msg_id"], "msg_original")


    def test_pre_seal_crash_recovery_seals_baseline_before_prompt(self):
        """F9/T14：基线封存前崩溃恢复——必须复用真实前置基线准备后才发 prompt。"""
        root = Path(self.att["directory"])
        (root / "old.txt").write_text("旧存量", encoding="utf-8")
        self.ledger.add_deliverables(self.att["id"], ["old.txt"])
        self.client.set_session("ses_worker", outcome="succeeded",
                                time_={"idle": 900_000, "busy": 800_000},
                                location_dir=self.att["directory"])
        out = self.sup.reconcile_dispatch(self.ledger.get_attempt(self.att["id"]))
        self.assertEqual(out["dispatch_state"], "confirmed")
        self.assertIsNotNone(out["dispatch_state"])
        # 只发一次 prompt，且使用原 message ID（绝不换 ID 重投）
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertEqual(self.client.prompt_calls[0]["message_id"], "msg_original")
        execution = out["metadata"]["execution"]
        self.assertTrue(execution["baseline"],
                        "恢复不得用空基线 seal_execution 后直接发送")
        self.assertEqual(execution["session_before"]["idle"], 900_000,
                         "派单前旧回合快照必须冻结")
        baseline_path = (Path(self.sup.home) / "results"
                         / self.att["id"] / "baseline.json")
        self.assertTrue(baseline_path.exists(),
                        "基线必须固化为 results/<attempt>/baseline.json")
        # 再次恢复：已封存幂等，不重建基线、不重复 prompt
        out2 = self.sup.reconcile_dispatch(out)
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertEqual(out2["metadata"]["execution"]["baseline"]
                         ["baseline_sha256"],
                         execution["baseline"]["baseline_sha256"])
        self.assertEqual(out2["dispatch_msg_id"], "msg_original")

    def test_post_send_unknown_recovery_keeps_placeholder_baseline(self):
        """T14：请求可能已发出（unknown）时不得重新采集/重建派单前基线。"""
        self.ledger.update_attempt(self.att["id"],
                                   fields={"dispatch_state": "unknown"},
                                   actor="test", reason="simulate-sent")
        self.ledger.seal_execution(
            self.att["id"], mode="reuse",
            session_before={"post_send_recovery": True},
            baseline={}, allow_send=True, actor="supervisor",
            reason="post-send-placeholder")
        self.client.set_session("ses_worker")
        self.client.set_inbox("ses_worker", [{"id": "msg_original"}])
        out = self.sup.reconcile_dispatch(self.ledger.get_attempt(self.att["id"]))
        self.assertEqual(out["dispatch_state"], "confirmed")
        execution = out["metadata"]["execution"]
        self.assertEqual(execution["baseline"], {},
                         "已经可能发送后：保持占位，不重建派单前基线")
        self.assertTrue(execution["session_before"].get("post_send_recovery"))
        self.assertEqual(self.client.prompt_calls, [],
                         "已受理（inbox 命中）不重复发送")


if __name__ == "__main__":
    unittest.main()

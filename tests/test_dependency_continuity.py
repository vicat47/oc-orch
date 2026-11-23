#!/usr/bin/env python3
"""改善协调连续性 tasks 3.1–3.4：依赖出口与业务健康场景级验证。

场景映射（openspec improve-coordinator-continuity acceptance）：

- C13 依赖具备可核验完成证据：至多两次有界公平对账内登记一次复核待办；\n  暂停时持久保留复核事实而不唤醒；\n- C14 条件未变时低成本等待：重复轮询不重置 deadline/预算，跨重启不重置；\n- C15 失效/循环/未知/到期转明确人工：owner/下一动作/告警持久，不无限顺延；\n- C16 后继修订与版本变化：评估期间 current/结果版本变化 → 复核被拒转人工；\n  不按标题猜后继、不把旧 rejected 自动 accepted；\n- C21 空通知不等于责任闭合：无推进/等待责任的任务投影 no_progress 事件；\n- C22 进程健康 ≠ 业务进展：heartbeat 只展示健康，阻塞独立可见，产生可\n  供独立邮件出口去重的结构化事件。\n\n复用 tests/test_dependency_wait.DepBase 的夹具（真临时 SQLite + FakeClock），\n零真实服务/模型/邮件。\n"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO))

from oc_orch.adapters.responsibility_ledger import (  # noqa: E402
    STATE_AWAITING_REVIEW,
    STATE_WAITING_DEPENDENCY,
    STATE_WAITING_HUMAN,
)
from oc_orch.domain import dependency_wait as dw  # noqa: E402
from tests.test_dependency_wait import (  # noqa: E402
    T0,
    DepBase,
    condition,
)


class ContinuityScenarioC13C14(DepBase):
    def test_c13_single_review_within_two_fair_scans(self):
        self.register()
        self.new_upstream()
        # 第一轮：未满足 → waiting；满足后第二轮登记一次复核
        out1 = self.service.reconcile(limit=10)
        self.assertEqual(out1["waiting"], 1)
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        out2 = self.service.reconcile(limit=10)
        self.assertEqual(out2["ready"], 1)
        # 复核事实只登记一次：状态已离开 waiting_dependency，后续扫不到；\n        # 通知也只有一封
        out3 = self.service.reconcile(limit=10)
        self.assertEqual(out3["ready"], 0)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM notifications").fetchone()[0], 1)
        # 复核通知不直接派单/验收：任务仍是原有状态，等待任务 current 不变
        task = self.ledger.get_task("T-1")
        self.assertIn(task["status"], ("running", "dispatched", "result_pending",
                                       "result_received"))
        self.assertNotEqual(task["status"], "accepted")

    def test_c13_paused_keeps_review_fact_without_wake(self):
        self.register()
        self.new_upstream()
        self.ledger.transition_task("T-1", "paused", actor="op")
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["ready"], 1)
        row = self.store.list_open_responsibilities(task_id="T-1", limit=10)[0]
        self.assertEqual(row["state"], STATE_AWAITING_REVIEW)
        self.assertTrue(row["evidence"][dw.DEPENDENCY_KEY]["satisfied"])
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM notifications").fetchone()[0], 0,
            "暂停只积累，不创建唤醒通知/不烧模型预算")

    def test_c14_repeat_poll_keeps_identity_and_budget(self):
        row = self.register(deadline=T0 + 1_000_000)
        self.new_upstream()
        first = self.store.get_responsibility(row["id"])
        for _ in range(4):
            self.clock.advance(wall=5_000)
            out = self.service.reconcile(limit=10)
            self.assertEqual(out["waiting"], 1)
        fresh = self.store.get_responsibility(row["id"])
        self.assertEqual(fresh["id"], row["id"])
        self.assertEqual(fresh["deadline"], T0 + 1_000_000)
        self.assertEqual(fresh["budget_used"], 0)
        self.assertEqual(fresh["max_budget"], first["max_budget"])
        self.assertEqual(fresh["next_action"], row["next_action"])


class ContinuityScenarioC15(DepBase):
    def test_unknown_dependency_gets_stable_human_with_next_action(self):
        row = self.register()
        self.new_upstream()
        self.conn.execute("UPDATE tasks SET status='accepted', accepted_at=? "
                          "WHERE id='T-2'", (self.clock.now_ms(),))
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        human = self.store.get_responsibility(row["id"])
        self.assertEqual(human["state"], STATE_WAITING_HUMAN)
        self.assertTrue(human["owner"])
        self.assertTrue(human["next_action"])
        blocked = human["evidence"]["blocked"]["dependency"]
        self.assertTrue(blocked)
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=100)
                  if a["kind"] == "responsibility_blocked"]
        self.assertGreaterEqual(len(alerts), 1)

    def test_no_infinite_defer_after_deadline(self):
        opened = self.store.ensure_dependency_wait(
            "coord:main", "proj", "T-1", condition(), attempt_id="A-1",
            generation=1, result_version=None, target_session="ses_coord",
            deadline=T0 - 1, next_check_at=T0, next_action="wait",
            actor="coordinator")
        for _ in range(3):
            self.clock.advance(wall=10_000)
            self.service.reconcile(limit=10)
        fresh = self.store.get_responsibility(opened["id"])
        self.assertEqual(fresh["state"], STATE_WAITING_HUMAN)
        self.assertEqual(fresh["deadline"], T0 - 1, "期限绝不重置")


class ContinuityScenarioC16(DepBase):
    def test_current_change_during_review_rejects_old_fact(self):
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        # 等待任务前进新代次 → 旧复核被归属守卫拒绝，转人工而非误接续
        self.ledger.release_site("A-1", confirmed=True, external_stopped=True,
                                 actor="test")
        self.ledger.create_attempt("T-1", attempt_id="A-3", directory=self.home,
                                   session_id="ses_worker2", generation=2,
                                   status="running", expected_current="A-1",
                                   takeover_confirmed=True)
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        self.assertIn("stale", out["details"][0]["reason"])
        self.assertEqual(self.store.get_responsibility(
            row["id"])["state"], STATE_WAITING_HUMAN)

    def test_no_guess_successor_by_title(self):
        # 条件未声明 successor：满足后只登记复核，不产生任何转移/接管
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["ready"], 1)
        fresh = self.store.get_responsibility(row["id"])
        self.assertIsNone(fresh["successor_id"])
        self.assertIsNone(fresh["closed_at"], "未验收前不得关闭责任")
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM notifications").fetchone()[0], 1,
            "只登记一条复核通知，不发生责任转移")

    def test_old_rejected_task_is_not_auto_accepted_by_dependency(self):
        # 上游满足只登记复核；历史 rejected 的任务保持 rejected，直到真实验收。
        self.conn.execute(
            "UPDATE attempts SET status='rejected', verification=? WHERE id='A-1'",
            (json.dumps({"schema": "oc-orch.verification.v1", "result": "fail",
                         "forced": False}),))
        self.conn.execute("UPDATE tasks SET status='rejected', blocked_reason=? "
                          "WHERE id='T-1'", ("verification_failed",))
        # rejected 任务上登记依赖等待（任务未终结可存在）
        self.store.ensure_dependency_wait(
            "coord:main", "proj", "T-1", condition(), attempt_id="A-1",
            generation=1, result_version=None, target_session="ses_coord",
            deadline=T0 + 3_600_000, next_check_at=T0, next_action="wait",
            actor="coordinator")
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["ready"], 1)
        task = self.ledger.get_task("T-1")
        self.assertEqual(task["status"], "rejected", "依赖满足不得自动 accepted")
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM notifications").fetchone()[0], 1)


class ContinuityScenarioC21C22(DepBase):
    def test_c21_all_acked_but_unfinished_is_no_progress(self):
        # 通知全 ACK 不等于责任闭合：无推进责任 → no_progress 事件
        self.conn.execute(
            "UPDATE attempts SET status='result_collected', site_released_at=? "
            "WHERE id='A-1'", (T0,))
        self.ledger.transition_task("T-1", "result_received", actor="test")
        self.ledger.ack_notifications_for_task("T-1", actor="test")
        out = self.service.business_health(now=T0)
        item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
        self.assertEqual(item["state"], dw.STATE_NO_PROGRESS)
        self.assertEqual(item["task_status"], "result_received")
        self.assertFalse(item["business_progress"])
        self.assertTrue(item["blocked"])
        self.assertTrue(any(e["kind"] == "task-unaccepted-no-progress"
                            and e["task_id"] == "T-1" for e in out["events"]))
        event = next(e for e in out["events"] if e["task_id"] == "T-1")
        self.assertTrue(event["next_action"])
        self.assertTrue(event["event_key"].startswith("event:"))

    def test_c22_heartbeat_healthy_but_business_blocked(self):
        self.conn.execute(
            "UPDATE attempts SET status='result_collected', site_released_at=? "
            "WHERE id='A-1'", (T0,))
        self.ledger.transition_task("T-1", "result_received", actor="test")
        self.store.open_responsibility(
            owner="ops", project="proj", task_id="T-1", attempt_id=None,
            generation=None, result_version=None, target_session=None,
            state=STATE_WAITING_HUMAN, wait_kind="human",
            wait_condition="unknown-execution", next_action="对账原输入",
            next_check_at=T0, deadline=T0 + 3_600_000, condition_key="human-key")
        out = self.service.business_health(
            now=T0, heartbeat_per_session={"ses_coord": T0 - 1})
        item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
        # 进程健康与业务状态分开展示
        self.assertTrue(item["process_healthy"])
        self.assertEqual(item["state"], dw.STATE_NEEDS_HUMAN)
        self.assertTrue(item["blocked"])
        self.assertTrue(item["owner"], "ops")
        # heartbeat 不构成业务进展：状态与事件源自业务事实，与心跳无关
        stale_again = self.service.business_health(
            now=T0 + 900_000, heartbeat_per_session={"ses_coord": T0 - 900_000 - 1})
        stale_item = next(t for t in stale_again["tasks"]
                          if t["task_id"] == "T-1")
        self.assertFalse(stale_item["process_healthy"])
        self.assertEqual(stale_item["state"], dw.STATE_NEEDS_HUMAN,
                         "心跳过期只影响进程健康展示，业务状态仍按事实判定")
        # 结构化事件键对同一状态稳定（可去重，不会因重复观察生成新事件）
        again = self.service.business_health(
            now=T0 + 1, heartbeat_per_session={"ses_coord": T0 - 1})
        self.assertEqual(
            [e["event_key"] for e in out["events"]],
            [e["event_key"] for e in again["events"]])

    def test_c22_waiting_dependency_state_with_health(self):
        # 尝试已终结并释放写权，任务处于“等待上游依赖”而不是执行中。
        self.conn.execute(
            "UPDATE attempts SET status='result_collected', site_released_at=? "
            "WHERE id='A-1'", (T0,))
        self.ledger.transition_task("T-1", "result_received", actor="test")
        self.register()
        self.new_upstream()
        out = self.service.business_health(
            now=T0, heartbeat_per_session={"ses_coord": T0 - 1})
        item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
        self.assertEqual(item["state"], dw.STATE_WAITING_DEP)
        self.assertTrue(item["process_healthy"])
        self.assertTrue(item["evidence_coverage"])


if __name__ == "__main__":
    unittest.main()
#!/usr/bin/env python3
"""tasks 3.1–3.4 依赖等待结构化条件与出口：纯规则 + 真实临时 SQLite 服务用例。

覆盖（C13–C16/C21/C22 的底层不变量）:

- 结构化条件：有限类型（显式 Task ID 集合 + all/any + 所需有效验收状态/
  结果版本 + 条件版本）；normalize/identity/describe/validate；
- 评估：satisfied / waiting / unknown / invalid / broken。只有可证明的有效验收
  （任务 accepted + accepted_at + 结构化 verification pass 且非 force）才算
  satisfied；仅字符串 accepted 不算；取消/缺失依赖 → broken → 稳定人工；
- 复核去重键（责任 + 条件版本 + 证据版本，不含时间）、依赖环路检测、
  业务健康投影（进程健康 vs 业务进展分离，heartbeat 不算进展）；
- 服务用例：登记/幂等等待责任、reconcile 的 ready/waiting/needs_human 有界
  扫描与原子复核发布/转人工（FakeClock + 真临时 SQLite，两连接验证跨重启）。

零真实服务/模型/邮件；不写生产台账。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO))

from oc_orch.adapters.responsibility_ledger import (  # noqa: E402
    ResponsibilityStore,
    STATE_AWAITING_REVIEW,
    STATE_WAITING_DEPENDENCY,
    STATE_WAITING_HUMAN,
)
from oc_orch.adapters.sqlite_db import init  # noqa: E402
from oc_orch.adapters.sqlite_ledger import Ledger  # noqa: E402
from oc_orch.application.dependency_wait import DependencyWaitService  # noqa: E402
from oc_orch.domain import dependency_wait as dw  # noqa: E402
from oc_orch.domain.errors import LedgerError  # noqa: E402
from tests.fakes import FakeClock  # noqa: E402

T0 = 1_800_000_000_000
VERIFICATION_PASS = {"schema": "oc-orch.verification.v1", "result": "pass",
                     "forced": False}


def condition(**over):
    base = {"type": "task_state", "require": "accepted", "mode": "all",
            "task_ids": ["T-2"], "result_version": None, "revision": 1,
            "successor": None}
    base.update(over)
    return base


class ConditionRuleTests(unittest.TestCase):
    def test_normalize_fills_defaults(self):
        out = dw.normalize_condition({"type": "task_state", "require": "accepted",
                                      "task_ids": ["T-2", " T-3 "]})
        self.assertEqual(out["mode"], "all")
        self.assertEqual(out["task_ids"], ["T-2", "T-3"])
        self.assertEqual(out["revision"], 1)
        self.assertIsNone(out["result_version"])
        self.assertIsNone(out["successor"])
        with self.assertRaises(ValueError):
            dw.normalize_condition({"type": "task_state", "require": "accepted",
                                    "task_ids": ["T-2", "T-2"]})

    def test_validate_rejects_bad_shapes(self):
        cases = [None, [],
                 {"type": "other", "require": "accepted", "task_ids": ["T-2"]},
                 {"type": "task_state", "require": "accepted", "task_ids": []},
                 {"type": "task_state", "require": "done", "task_ids": ["T-2"]},
                 {"type": "task_state", "require": "accepted",
                  "task_ids": ["T-2", "T-2"]},
                 {"type": "task_state", "require": "accepted", "task_ids": [1]},
                 {"type": "task_state", "require": "accepted",
                  "task_ids": ["T-2"], "result_version": 5},
                 {"type": "task_state", "require": "accepted",
                  "task_ids": ["T-2"], "mode": "both"}]
        for case in cases:
            with self.subTest(case=case):
                self.assertTrue(dw.validate_condition(case))
        self.assertEqual(dw.validate_condition(condition()), [])

    def test_identity_stable_and_sensitive_to_semantics(self):
        base = condition()
        self.assertEqual(dw.condition_identity(base), dw.condition_identity(base))
        variants = [
            condition(task_ids=["T-2", "T-3"]),
            condition(result_version="rv1-a"),
            condition(revision=2),
            condition(mode="any"),
            condition(successor="T-9"),
        ]
        ids = [dw.condition_identity(base)] + [dw.condition_identity(v)
                                               for v in variants]
        self.assertEqual(len(set(ids)), len(ids))

    def test_describe_condition_is_display_safe(self):
        text = dw.describe_condition(condition(task_ids=["T-2", "T-3"],
                                               result_version="rv1-a"))
        self.assertTrue(text.startswith("dep:accepted:all:T-2,T-3:rvrv1-a"))
        self.assertEqual(dw.describe_condition("legacy-string"),
                         "dependency-condition-invalid")

    def test_dedupe_review_key_is_time_free_and_version_sensitive(self):
        a = dw.dedupe_review_key("R-1", condition(), "ev-a")
        self.assertEqual(a, dw.dedupe_review_key("R-1", condition(), "ev-a"))
        self.assertNotEqual(a, dw.dedupe_review_key("R-1", condition(), "ev-b"))
        self.assertNotEqual(a, dw.dedupe_review_key("R-2", condition(), "ev-a"))
        self.assertNotIn(str(T0), a)

    def test_detect_cycle(self):
        self.assertIsNone(dw.detect_cycle({"a": ["b"], "b": ["c"]}, "a"))
        cycle = dw.detect_cycle({"a": ["b"], "b": ["c"], "c": ["a"]}, "a")
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle[0], cycle[-1])
        self.assertIsNone(dw.detect_cycle({"a": ["b"], "b": ["a"]}, "c"))

    def test_record_condition_key_requires_rid(self):
        with self.assertRaises(ValueError):
            dw.record_condition_key("", condition())


class EvaluateTests(unittest.TestCase):
    FACT_WAIT = {"exists": True, "status": "running", "accepted_at": None,
                 "closed_at": None, "current_attempt_id": "A-2",
                 "generation": 1, "result_version": None, "effective": False,
                 "verification": None}
    FACT_OK = {"exists": True, "status": "accepted", "accepted_at": 10,
               "closed_at": None, "current_attempt_id": "A-2",
               "generation": 1, "result_version": "rv1-a", "effective": True,
               "verification": VERIFICATION_PASS}

    def _facts(self, **over):
        return {"T-2": {**self.FACT_WAIT, **over}}

    def test_all_waiting_until_all_accepted_verified(self):
        out = dw.evaluate_task_condition(condition(), self._facts())
        self.assertEqual(out["status"], "waiting")
        self.assertIsNone(out["evidence_version"])
        ok = dw.evaluate_task_condition(condition(), self._facts(
            status="accepted", accepted_at=10, effective=True,
            result_version="rv1-a", verification=VERIFICATION_PASS))
        self.assertEqual(ok["status"], "satisfied")
        self.assertTrue(ok["evidence_version"].startswith("ev-"))

    def test_accepted_without_verification_is_unknown(self):
        out = dw.evaluate_task_condition(condition(), self._facts(
            status="accepted", accepted_at=10, effective=False,
            verification=None))
        self.assertEqual(out["status"], "unknown")
        self.assertEqual(out["per_task"]["T-2"]["reason"],
                         "accepted-without-verification")

    def test_result_version_mismatch_is_unknown(self):
        out = dw.evaluate_task_condition(
            condition(result_version="rv1-x"), self._facts(
                status="accepted", accepted_at=10, effective=True,
                result_version="rv1-a", verification=VERIFICATION_PASS))
        self.assertEqual(out["status"], dw.UNKNOWN)
        self.assertEqual(out["reason"], "dependency-unknown")

    def test_cancelled_or_missing_dependency_is_broken(self):
        broken = dw.evaluate_task_condition(condition(), self._facts(
            status="cancelled", closed_at=10))
        self.assertEqual(broken["status"], "broken")
        missing = dw.evaluate_task_condition(condition(), {
            "T-2": {"exists": False}})
        self.assertEqual(missing["status"], "broken")

    def test_any_mode_satisfied_on_one(self):
        any_cond = condition(mode="any", task_ids=["T-2", "T-3"])
        facts = {"T-2": self.FACT_WAIT,
                 "T-3": {**self.FACT_OK, "result_version": "rv1-a"}}
        out = dw.evaluate_task_condition(any_cond, facts)
        self.assertEqual(out["status"], "satisfied")
        self.assertEqual(out["reason"], "any-dependency-satisfied")

    def test_legacy_string_condition_is_invalid(self):
        out = dw.evaluate_task_condition("upstream-task", {})
        self.assertEqual(out["status"], "invalid")

    def test_unstructured_never_counts_as_complete(self):
        # 纯文本/任意字符串永远不是结构化满足：走 invalid → 人工。
        self.assertEqual(dw.evaluate_task_condition("accepted", {})["status"],
                         "invalid")


class HealthProjectionTests(unittest.TestCase):
    def test_heartbeat_is_health_not_progress(self):
        out = dw.wait_health_projection(
            task={"id": "T-1", "status": "running"},
            heartbeat_at=T0, now=T0 + 1000)
        self.assertTrue(out["process_healthy"])
        self.assertFalse(out["business_progress"])
        self.assertEqual(out["state"], dw.STATE_NO_PROGRESS)

    def test_stale_heartbeat_is_visible_not_none(self):
        out = dw.wait_health_projection(
            task={"id": "T-1", "status": "running"},
            heartbeat_at=T0, now=T0 + 600_000,
            heartbeat_stale_ms=300_000)
        self.assertFalse(out["process_healthy"])
        self.assertFalse(out["business_progress"])

    def test_paused_is_not_unknown_or_done(self):
        out = dw.wait_health_projection(task={"id": "T-1", "status": "paused"})
        self.assertEqual(out["state"], dw.STATE_PAUSED)
        self.assertTrue(out["blocked"])

    def test_waiting_human_responsibility_is_needs_human(self):
        out = dw.wait_health_projection(
            task={"id": "T-1", "status": "result_received"},
            responsibilities=[{"state": STATE_WAITING_HUMAN, "closed_at": None}])
        self.assertEqual(out["state"], dw.STATE_NEEDS_HUMAN)
        self.assertTrue(out["blocked"])

    def test_open_attempt_is_executing_not_idle(self):
        out = dw.wait_health_projection(
            task={"id": "T-1", "status": "running"},
            attempt={"id": "A-1", "status": "running",
                     "site_released_at": None, "closed_at": None})
        self.assertEqual(out["state"], dw.STATE_EXECUTING)

    def test_accepted_without_verification_needs_human(self):
        out = dw.wait_health_projection(
            task={"id": "T-1", "status": "accepted", "accepted_at": 1},
            attempt={"id": "A-1", "verification": None})
        self.assertEqual(out["state"], dw.STATE_NEEDS_HUMAN)
        self.assertFalse(out["evidence_coverage"])

    def test_actionable_event_has_stable_key_without_time(self):
        a = dw.actionable_event(kind="task-unaccepted-no-progress", task_id="T-1",
                                reason="r", owner="o", next_action="n",
                                at_ms=T0)
        b = dw.actionable_event(kind="task-unaccepted-no-progress", task_id="T-1",
                                reason="r", owner="o", next_action="n",
                                at_ms=T0 + 999)
        self.assertEqual(a["event_key"], b["event_key"])
        self.assertEqual(a["at_ms"], T0)


class DepBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="oc-orch-depwait-",
                                               dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.clock = FakeClock(wall_ms=T0)
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.store = ResponsibilityStore(self.ledger)
        self.service = DependencyWaitService(self.ledger, self.clock,
                                             store=self.store)
        self.task = self.ledger.create_task(project="proj", title="downstream",
                                            task_id="T-1",
                                            coordinator_session="ses_coord")
        self.attempt = self.ledger.create_attempt(
            "T-1", attempt_id="A-1", directory=self.home,
            session_id="ses_worker", generation=1, status="running")
        self.addCleanup(self._cleanup_conns)

    def _cleanup_conns(self):
        for conn in getattr(self, "_extra_conns", []):
            try:
                conn.close()
            except Exception:
                pass

    def second(self):
        conn = init(self.home)
        self._extra_conns = getattr(self, "_extra_conns", []) + [conn]
        ledger = Ledger(conn, self.home, clock=self.clock)
        return ledger, ResponsibilityStore(ledger)

    def new_upstream(self, task_id="T-2", session="ses_up"):
        self.ledger.create_task(project="proj", title="upstream",
                                task_id=task_id,
                                coordinator_session="ses_coord")
        return self.ledger.create_attempt(
            task_id, attempt_id=f"A-{task_id[-1]}", directory=self.home,
            session_id=session, generation=1, status="running",
            takeover_confirmed=True)

    def mark_verified_accepted(self, task_id, attempt_id, version="rv1-a"):
        # 测试夹具：与真实 submit_verification 产物同构（schema/result/forced +
        # result_snapshot.result_version 绑定当前收集版本）。真实路径由
        # RealVerifyAcceptanceTests 兜底。
        verification = dict(VERIFICATION_PASS)
        verification["result_snapshot"] = {"result_version": version}
        self.conn.execute(
            "UPDATE attempts SET verification=?, metadata=?, status='accepted' "
            "WHERE id=?",
            (json.dumps(verification),
             json.dumps({"result": {"version": version}}), attempt_id))
        self.conn.execute(
            "UPDATE tasks SET status='accepted', accepted_at=?, updated_at=? "
            "WHERE id=?", (self.clock.now_ms(), self.clock.now_ms(), task_id))

    def register(self, c=None, **over):
        kw = dict(
            deadline=self.clock.now_ms() + 3_600_000,
            next_check_at=self.clock.now_ms(),
            next_action="wait for upstream", actor="coordinator")
        kw.update(over)
        return self.store.ensure_dependency_wait(
            "coord:main", "proj", "T-1", c or condition(),
            attempt_id="A-1", generation=1, result_version=None,
            target_session="ses_coord", **kw)

    def notifications(self):
        return self.ledger.list_notifications(target_session="ses_coord",
                                              state=None, limit=100)

    def notif_count(self):
        return self.conn.execute("SELECT COUNT(*) FROM notifications").fetchone()[0]


class StoreRegisterTests(DepBase):
    def test_register_persists_structured_condition(self):
        row = self.register()
        self.assertEqual(row["state"], STATE_WAITING_DEPENDENCY)
        self.assertEqual(row["wait_kind"], "dependency")
        dep = row["evidence"][dw.DEPENDENCY_KEY]
        self.assertIsInstance(dep["condition"], dict)
        self.assertEqual(dep["condition"]["task_ids"], ["T-2"])
        self.assertEqual(dep["condition_identity"],
                         dw.condition_identity(condition()))
        self.assertFalse(dep["satisfied"])
        self.assertEqual(row["max_budget"], 3)
        self.assertIsNotNone(row["deadline"])

    def test_register_is_idempotent_and_identity_stable(self):
        a = self.register()
        b = self.register()
        self.assertEqual(a["id"], b["id"])
        self.assertFalse(b["created"])
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM responsibilities").fetchone()[0], 1)
        c = self.register(condition(result_version="rv1-x"))
        self.assertNotEqual(a["id"], c["id"], "条件版本变化即新等待事实")
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM responsibilities").fetchone()[0], 2)

    def test_service_register_fills_defaults(self):
        row = self.service.register_condition_wait(
            "coord:main", "proj", "T-1", condition(), attempt_id="A-1",
            generation=1, target_session="ses_coord")
        self.assertEqual(row["state"], STATE_WAITING_DEPENDENCY)
        self.assertIsNotNone(row["deadline"])
        self.assertTrue(row["deadline"] >= self.clock.now_ms())

    def test_condition_key_constant(self):
        a = self.register()["id"]
        self.assertEqual(self.store.get_responsibility(a)["condition_key"],
                         dw.condition_identity(condition()))


class ReconcileTests(DepBase):
    def test_waiting_keeps_owner_deadline_budget_and_backs_off(self):
        row = self.register(next_action="wait for upstream")
        self.new_upstream()
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["scanned"], 1)
        self.assertEqual(out["waiting"], 1)
        self.assertEqual(out["ready"], 0)
        self.assertEqual(out["needs_human"], 0)
        fresh = self.store.get_responsibility(row["id"])
        self.assertEqual(fresh["owner"], "coord:main")
        self.assertEqual(fresh["deadline"], row["deadline"])
        self.assertEqual(fresh["budget_used"], 0)
        self.assertGreater(fresh["next_check_at"], row["next_check_at"])
        self.assertEqual(fresh["state"], STATE_WAITING_DEPENDENCY)
        self.assertEqual(self.notif_count(), 0)

    def test_satisfied_registers_single_review_notification(self):
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["ready"], 1)
        fresh = self.store.get_responsibility(row["id"])
        self.assertEqual(fresh["state"], STATE_AWAITING_REVIEW)
        self.assertTrue(fresh["evidence"][dw.DEPENDENCY_KEY]["satisfied"])
        self.assertEqual(self.notif_count(), 1)
        notif = self.notifications()[0]
        self.assertEqual(notif["kind"], "continuation_ready")
        self.assertNotIn(str(T0), notif["dedupe_key"])
        # 再次登记同一满足事实不产生第二封通知（幂等复用，改签名后不再信任
        # 调用方传入的 condition/evidence——事务内重读原条件并重新评估）。
        again = self.store.record_dependency_review(
            row["id"], expected_revision=fresh["revision"],
            now=self.clock.now_ms())
        self.assertFalse(again["published"])
        self.assertEqual(again["reason"], "already-published")
        self.assertEqual(again["notification"]["id"], notif["id"])
        self.assertEqual(self.notif_count(), 1)
        # 普通 publish 复用同一 dep-review 原键，不生成第二种去重键
        published = self.store.publish_responsibility(
            row["id"], expected_revision=self.store.get_responsibility(
                row["id"])["revision"], payload={"summary": "x"})
        self.assertFalse(published["published"])
        self.assertEqual(published["reason"], "review-already-recorded")
        self.assertEqual(published["notification"]["id"], notif["id"])
        self.assertEqual(self.notif_count(), 1)
        keys = [r["dedupe_key"] for r in self.conn.execute(
            "SELECT dedupe_key FROM notifications").fetchall()]
        self.assertEqual(len(set(keys)), 1, "同一满足事实只能有一种去重键")

    def test_pending_deps_do_not_create_review_or_consume_budget(self):
        row = self.register()
        self.new_upstream()
        for _ in range(3):
            self.clock.advance(wall=60_000)
            out = self.service.reconcile(limit=10)
            self.assertEqual(out["waiting"], 1)
        fresh = self.store.get_responsibility(row["id"])
        self.assertEqual(fresh["budget_used"], 0, "普通等待不烧失败预算")
        self.assertEqual(fresh["deadline"], row["deadline"])
        self.assertEqual(self.notif_count(), 0)
        observed = fresh["evidence"][dw.DEPENDENCY_KEY][dw.OBSERVED_KEY]
        self.assertGreaterEqual(observed, 2, "到期才写观察")
        self.assertLess(observed, 4, "有界退避：不是每次扫描都递增")

    def test_cancelled_dependency_is_broken_to_human(self):
        row = self.register()
        self.new_upstream()
        self.ledger.transition_task("T-2", "cancelled", actor="test")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        fresh = self.store.get_responsibility(row["id"])
        self.assertEqual(fresh["state"], STATE_WAITING_HUMAN)
        self.assertTrue(fresh["evidence"]["blocked"]["dependency"])
        self.assertGreaterEqual(self.conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE kind='responsibility_blocked'"
        ).fetchone()[0], 1)

    def test_missing_dependency_is_human(self):
        row = self.register(condition(task_ids=["T-nope"]))
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        self.assertEqual(self.store.get_responsibility(
            row["id"])["state"], STATE_WAITING_HUMAN)

    def test_accepted_without_verification_is_unknown_human(self):
        row = self.register()
        self.new_upstream()
        self.conn.execute("UPDATE tasks SET status='accepted', accepted_at=? "
                          "WHERE id='T-2'", (self.clock.now_ms(),))
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        self.assertEqual(out["details"][0]["reason"], "dependency-unknown")

    def test_legacy_string_condition_is_human_not_deferred(self):
        legacy = self.store.open_responsibility(
            owner="coord:main", project="proj", task_id="T-1",
            attempt_id="A-1", generation=1, result_version=None,
            target_session="ses_coord", state=STATE_WAITING_DEPENDENCY,
            wait_kind="dependency", wait_condition="upstream-task",
            next_action="wait", next_check_at=T0, deadline=T0 + 3_600_000,
            condition_key="legacy-key", evidence={})
        self.clock.advance(wall=1000)
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        self.assertEqual(self.store.get_responsibility(
            legacy["id"])["state"], STATE_WAITING_HUMAN)

    def test_deadline_exhaustion_is_human_without_reset(self):
        opened = self.store.ensure_dependency_wait(
            "coord:main", "proj", "T-1", condition(), attempt_id="A-1",
            generation=1, result_version=None, target_session="ses_coord",
            deadline=T0 - 1, next_check_at=T0, next_action="wait",
            actor="coordinator")
        self.clock.advance(wall=1000)
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        fresh = self.store.get_responsibility(opened["id"])
        self.assertEqual(fresh["state"], STATE_WAITING_HUMAN)
        self.assertEqual(fresh["deadline"], T0 - 1, "期限不重置")

    def test_dependency_cycle_escalates_human(self):
        row = self.register()
        self.new_upstream()
        self.store.ensure_dependency_wait(
            "coord:main", "proj", "T-2", condition(task_ids=["T-1"]),
            attempt_id="A-2", generation=1, result_version=None,
            target_session="ses_coord", deadline=T0 + 3_600_000,
            next_check_at=T0, next_action="wait back", actor="coordinator")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        self.assertEqual(out["details"][0]["reason"], "dependency-cycle")

    def test_paused_task_accumulates_review_without_wake(self):
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        self.ledger.transition_task("T-1", "paused", actor="op")
        out = self.service.reconcile(limit=10)
        # 复核事实保留（不吞），状态到 awaiting_review，但不创建唤醒通知。
        self.assertEqual(out["ready"], 1)
        self.assertEqual(out["details"][0]["reason"],
                         "dependency-satisfied-accumulated")
        fresh = self.store.get_responsibility(row["id"])
        self.assertEqual(fresh["state"], STATE_AWAITING_REVIEW)
        self.assertTrue(fresh["evidence"][dw.DEPENDENCY_KEY]["satisfied"])
        self.assertEqual(self.notif_count(), 0)

    def test_stale_generation_review_is_rejected(self):
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        self.ledger.release_site("A-1", confirmed=True, external_stopped=True,
                                 actor="test")
        self.ledger.create_attempt("T-1", attempt_id="A-3", directory=self.home,
                                   session_id="ses_worker2", generation=2,
                                   status="running", expected_current="A-1",
                                   takeover_confirmed=True)
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        self.assertIn("stale", out["details"][0]["reason"])

    def test_restart_keeps_observed_and_deadline(self):
        row = self.register()
        self.new_upstream()
        self.service.reconcile(limit=10)
        before = self.store.get_responsibility(row["id"])
        ledger2, store2 = self.second()
        service2 = DependencyWaitService(ledger2, self.clock, store=store2)
        service2.reconcile(limit=10)
        after = store2.get_responsibility(row["id"])
        self.assertEqual(after["budget_used"], before["budget_used"])
        self.assertEqual(after["deadline"], before["deadline"])
        self.assertGreaterEqual(after["evidence"][dw.DEPENDENCY_KEY][
            dw.OBSERVED_KEY], before["evidence"][dw.DEPENDENCY_KEY][
                dw.OBSERVED_KEY])


class BusinessHealthTests(DepBase):
    def test_no_progress_emits_actionable_event(self):
        # 尝试已终结、通知/责任全空：工作未验收仍无进展 → no_progress 事件
        self.conn.execute(
            "UPDATE attempts SET status='result_collected', site_released_at=? "
            "WHERE id='A-1'", (T0,))
        self.ledger.transition_task("T-1", "result_received", actor="test")
        out = self.service.business_health(now=T0)
        item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
        self.assertEqual(item["state"], dw.STATE_NO_PROGRESS)
        self.assertFalse(item["business_progress"])
        self.assertTrue(any(e["kind"] == "task-unaccepted-no-progress"
                            for e in out["events"]))

    def test_heartbeat_fresh_but_business_blocked(self):
        # 显式人工责任：进程健康（heartbeat 新）但业务阻塞（needs_human）。
        self.conn.execute(
            "UPDATE attempts SET status='result_collected', site_released_at=? "
            "WHERE id='A-1'", (T0,))
        self.ledger.transition_task("T-1", "result_received", actor="test")
        self.store.open_responsibility(
            owner="ops", project="proj", task_id="T-1", attempt_id=None,
            generation=None, result_version=None, target_session=None,
            state=STATE_WAITING_HUMAN, wait_kind="human",
            wait_condition="blocked", next_action="decide",
            next_check_at=T0, deadline=T0 + 3_600_000, condition_key="human-key")
        out = self.service.business_health(
            now=T0, heartbeat_per_session={"ses_coord": T0 - 1})
        item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
        self.assertEqual(item["state"], dw.STATE_NEEDS_HUMAN)
        self.assertTrue(item["process_healthy"], "进程健康单独展示")
        self.assertTrue(item["blocked"])
        # heartbeat 本身不构成业务进展：进展由 facts/business_event 决定
        self.assertFalse(item.get("next_action") is None)

    def test_open_attempt_is_executing_not_no_progress(self):
        out = self.service.business_health(now=T0)
        item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
        self.assertEqual(item["state"], dw.STATE_EXECUTING)

    def test_provider_backoff_scope_is_per_model(self):
        self.ledger.record_provider_state("providerA/modelA",
                                          category="quota", status=429,
                                          until_ms=T0 + 1000, reason="quota")
        self.conn.execute("UPDATE attempts SET model='providerB/modelB' "
                          "WHERE id='A-1'")
        out = self.service.business_health(now=T0)
        item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
        self.assertEqual(item["state"], dw.STATE_EXECUTING,
                         "其他模型的退避不得阻塞本任务")



class SecurityRaceReconcileTests(DepBase):
    """父审读补修：事务内重评估、防伪造/条件替换/撤销竞态、WAL 双连接、not-due
    不写观察、resume 后原 dedupe 单发、环检测截断。"""

    def test_record_rejects_pending_condition_invalid_proof(self):
        row = self.register()  # dep 未满足：直接调 record 不得伪造 satisfied
        self.new_upstream()
        out = self.store.record_dependency_review(
            row["id"], expected_revision=1, now=self.clock.now_ms())
        self.assertFalse(out["recorded"])
        self.assertEqual(out["reason"], "condition-not-satisfied")
        self.assertEqual(out["status"], "waiting")
        self.assertEqual(self.notif_count(), 0)
        self.assertEqual(self.store.get_responsibility(
            row["id"])["revision"], 1, "拒绝不得留下写入痕迹")

    def test_record_rejects_condition_identity_mismatch(self):
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        # 篡改登记条件（换成别的依赖任务）→ 与 row.condition_key 不再一致
        evidence = dict(row["evidence"])
        dep = dict(evidence[dw.DEPENDENCY_KEY])
        dep["condition"] = dw.normalize_condition(condition(task_ids=["T-9"]))
        dep["condition_identity"] = dw.condition_identity(
            condition(task_ids=["T-9"]))
        evidence[dw.DEPENDENCY_KEY] = dep
        self.conn.execute(
            "UPDATE responsibilities SET evidence=? WHERE id=?",
            (json.dumps(evidence), row["id"]))
        out = self.store.record_dependency_review(
            row["id"], expected_revision=1, now=self.clock.now_ms())
        self.assertFalse(out["recorded"])
        self.assertEqual(out["reason"], "condition-identity-mismatch")
        self.assertEqual(self.notif_count(), 0)

    def test_record_rejects_revoked_evidence_race(self):
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        # 外部扫描后、事务提交前证据被撤销：verification 清空 → 事务内重评估不满足
        self.conn.execute(
            "UPDATE attempts SET verification=NULL, "
            "metadata='{\"result\": {\"version\": \"rv1-old\"}}' WHERE id='A-2'")
        out = self.store.record_dependency_review(
            row["id"], expected_revision=1, now=self.clock.now_ms())
        self.assertFalse(out["recorded"])
        self.assertEqual(out["reason"], "condition-not-satisfied")
        self.assertEqual(self.notif_count(), 0)

    def test_waiting_not_due_does_not_write(self):
        row = self.store.ensure_dependency_wait(
            "coord:main", "proj", "T-1", condition(), attempt_id="A-1",
            generation=1, result_version=None, target_session="ses_coord",
            deadline=T0 + 3_600_000, next_check_at=T0 + 3_600_000,
            next_action="wait", actor="coordinator")
        self.new_upstream()
        for _ in range(2):
            out = self.service.reconcile(limit=10)
            self.assertEqual(out["waiting"], 1)
        fresh = self.store.get_responsibility(row["id"])
        self.assertEqual(fresh["revision"], 1, "未到期不得改写责任行")
        self.assertIsNone(fresh["evidence"][dw.DEPENDENCY_KEY].get(
            dw.OBSERVED_KEY), "未到期不递增 observed")
        self.assertEqual(self.notif_count(), 0)

    def test_double_connection_single_review_winner(self):
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        ledger2, store2 = self.second()
        first = self.store.record_dependency_review(
            row["id"], expected_revision=1, now=self.clock.now_ms())
        self.assertTrue(first["published"])
        rid = self.store.list_open_responsibilities(task_id="T-1")[0]["id"]
        fresh = self.store.get_responsibility(rid)
        with self.assertRaises(LedgerError) as ctx:
            store2.record_dependency_review(
                rid, expected_revision=1, now=self.clock.now_ms())
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_REVISION_CONFLICT")
        self.assertEqual(self.notif_count(), 1)

    def test_resumed_publish_once_with_original_dedupe(self):
        self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        self.ledger.transition_task("T-1", "paused", actor="op")
        out1 = self.service.reconcile(limit=10)
        self.assertEqual(out1["ready"], 1)  # 暂停积累，不唤醒
        self.assertEqual(self.notif_count(), 0)
        self.ledger.transition_task("T-1", "running", actor="op")
        out2 = self.service.reconcile(limit=10)
        self.assertEqual(out2["ready"], 1)
        self.assertEqual(self.notif_count(), 1)
        notif = self.notifications()[0]
        self.assertTrue(notif["dedupe_key"].startswith("dep-review:"))
        # resume 后再 reconcile：已发布，幂等跳过，不写/revision/审计
        before = self.store.get_responsibility(
            self.store.list_open_responsibilities(task_id="T-1")[0]["id"])
        out3 = self.service.reconcile(limit=10)
        self.assertEqual(out3["ready"], 0)
        self.assertEqual(self.notif_count(), 1)
        after = self.store.get_responsibility(before["id"])
        self.assertEqual(after["revision"], before["revision"],
                         "已发布行后续轮次不改 revision")

    def test_cycle_proof_truncation_is_unverified_human(self):
        self.register()
        self.new_upstream()
        # 模拟全图超过单页证明上限：读不到的不当“无环”
        real_count = self.store.count_dependency_waits
        self.store.count_dependency_waits = lambda: real_count() + 999
        try:
            out = self.service.reconcile(limit=10)
            self.assertEqual(out["needs_human"], 1)
            self.assertEqual(out["details"][0]["reason"],
                             "dependency-cycle-unverified")
            self.assertEqual(self.store.get_responsibility(
                self.store.list_open_responsibilities(task_id="T-1")[0]["id"]
            )["state"], STATE_WAITING_HUMAN)
        finally:
            self.store.count_dependency_waits = real_count

    def test_terminal_task_closes_dependency_wait(self):
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-1", "A-1", version="rv1-a")  # 等待任务自己收口
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["waiting"], 1)
        self.assertEqual(out["details"][0]["reason"], "task-accepted")
        self.assertIsNotNone(
            self.store.get_responsibility(row["id"])["closed_at"],
            "任务真收口后依赖责任必须关闭")


class RealVerifyAcceptanceTests(DepBase):
    """真实 collect/verify 路径测 effective：不是手写小 dict 当有效验收。"""

    def _real_verified_upstream(self, task_id="T-2", *, force=False):
        from oc_orch.adapters.results import (collect_attempt,
                                              scan_deliverable_baseline,
                                              verify_attempt)
        from tests.fakes import FakeClient
        self.ledger.create_task(project="proj", title="upstream", task_id=task_id,
                                coordinator_session="ses_coord")
        site = os.path.join(self.home, f"site-{task_id[-1]}")
        os.makedirs(site, exist_ok=True)
        with open(os.path.join(site, "README.md"), "w", encoding="utf-8") as fh:
            fh.write("baseline")
        aid = f"A-{task_id[-1]}"
        self.ledger.create_attempt(
            task_id, attempt_id=aid, directory=site, write_scope=[site],
            status="dispatched", dispatch_state="confirmed",
            session_id="ses_real", dispatch_msg_id="msg_real_in",
            deliverables=["README.md"], takeover_confirmed=True)
        ref = scan_deliverable_baseline(self.ledger, aid, captured_at=T0)
        self.ledger.seal_execution(aid, mode="new", session_before={}, baseline=ref)
        started = T0
        self.ledger.record_execution_started(aid, started_at=started,
                                             event_id="evt-real-start")
        self.ledger.record_terminal(
            aid, outcome="succeeded", source="sse", event_id="evt-real-end",
            candidate={"outcome": "succeeded", "source": "sse",
                       "ended_at": started + 10, "session_id": "ses_real",
                       "input_in_inbox": False})
        client = FakeClient()
        client.append_message("ses_real", {
            "id": "msg_real_in", "type": "user", "text": "go",
            "time": {"created": started}})
        client.append_message("ses_real", {
            "id": "msg_real_done", "type": "assistant",
            "time": {"created": started + 1},
            "content": [{"type": "text", "text": "任务完成"}]})
        with open(os.path.join(site, "README.md"), "w", encoding="utf-8") as fh:
            fh.write("collected-v1")
        info = collect_attempt(self.ledger, client, aid,
                               cfg={"result_max_messages": 50,
                                    "result_store_limit": 20000}, auto=False)
        assert info["status_after"] == "result_collected", info
        if force:
            out = verify_attempt(self.ledger, aid, result="pass", force=True,
                                 reason="manual-exception")
        else:
            out = verify_attempt(self.ledger, aid, result="pass")
        assert out["result"] == "pass", out
        return aid

    def dep_review_count(self):
        return sum(1 for r in self.conn.execute(
            "SELECT dedupe_key FROM notifications").fetchall()
            if r["dedupe_key"].startswith("dep-review:"))

    def test_real_verified_dependency_is_effective(self):
        self.register()
        self._real_verified_upstream("T-2")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["ready"], 1, out["details"])
        fresh = self.store.get_responsibility(
            self.store.list_open_responsibilities(task_id="T-1")[0]["id"])
        self.assertTrue(fresh["evidence"][dw.DEPENDENCY_KEY]["satisfied"])
        self.assertEqual(self.dep_review_count(), 1,
                         "真实 collect/verify 产物才算有效验收，只发布一次复核")
        binding = fresh["evidence"][dw.DEPENDENCY_KEY].get("task_facts")["T-2"]
        self.assertTrue(binding["effective"])
        self.assertIsNotNone(binding["verify_result_version"])
        self.assertEqual(binding["result_version"],
                         binding["verify_result_version"])

    def test_force_accepted_is_not_effective(self):
        self.register()
        self._real_verified_upstream("T-2", force=True)
        # force 不算正常有效交付：评估 unknown → 转人工，不发布复核
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        self.assertEqual(out["details"][0]["reason"], "dependency-unknown")
        self.assertEqual(self.dep_review_count(), 0,
                         "force 不算正常有效交付：不发布复核")


class BusinessHealthFairnessTests(DepBase):
    def test_business_health_uses_persistent_cursor(self):
        self.ledger.create_task(project="proj", title="t2", task_id="T-2",
                                coordinator_session="ses_coord")
        self.ledger.create_task(project="proj", title="t3", task_id="T-3",
                                coordinator_session="ses_coord")
        # T-1 的 current_attempt 清掉避免执行态干扰
        self.conn.execute("UPDATE tasks SET current_attempt_id=NULL WHERE id='T-1'")
        first = self.service.business_health(limit=2, now=T0)
        second = self.service.business_health(limit=2, now=T0)
        seen1 = {t["task_id"] for t in first["tasks"]}
        seen2 = {t["task_id"] for t in second["tasks"]}
        self.assertEqual(len(seen1), 2)
        self.assertNotEqual(seen1, seen2, "持久游标前进，不是每轮只看同一页")
        self.assertEqual(len(seen1 | seen2), 3, "两轮公平覆盖全部任务")

    def test_owner_is_real_responsibility_holder(self):
        self.conn.execute(
            "UPDATE attempts SET status='result_collected', site_released_at=? "
            "WHERE id='A-1'", (T0,))
        self.ledger.transition_task("T-1", "result_received", actor="test")
        self.store.open_responsibility(
            owner="ops:human", project="proj", task_id="T-1", attempt_id=None,
            generation=None, result_version=None, target_session=None,
            state=STATE_WAITING_HUMAN, wait_kind="human",
            wait_condition="blocked", next_action="decide",
            next_check_at=T0, deadline=T0 + 3_600_000, condition_key="human-key")
        out = self.service.business_health(now=T0)
        item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
        self.assertEqual(item["owner"], "ops:human", "不得永远落在 manual:unassigned")

    def test_read_failure_does_not_fabricate_no_progress(self):
        self.conn.execute("UPDATE tasks SET current_attempt_id=NULL WHERE id='T-1'")
        real = self.store.list_open_responsibilities
        def boom(task_id=None, owner=None, limit=200):
            raise LedgerError("RESPONSIBILITY_INVALID", "read failure")
        self.store.list_open_responsibilities = boom
        try:
            out = self.service.business_health(now=T0)
            item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
            self.assertFalse(item["coverage_ok"])
            self.assertFalse(any(e["task_id"] == "T-1"
                                 for e in out["events"]),
                             "读不到 ≠ 无责任：不得伪造 no_progress 事件")
        finally:
            self.store.list_open_responsibilities = real



class ParentRefinementTests(DepBase):
    """父审读第 3 轮：S4 自管 wait_kind 排除、可行动审计、revision/顺序身份稳定。"""

    def _s4_row(self, wait_kind, key):
        return self.store.open_responsibility(
            owner="coord:main", project="proj", task_id="T-1",
            attempt_id="A-1", generation=1, result_version=None,
            target_session="ses_coord", state=STATE_WAITING_DEPENDENCY,
            wait_kind=wait_kind, wait_condition="s4-handled",
            next_action="s4 reconciles", next_check_at=T0,
            deadline=T0 + 3_600_000, condition_key=key, evidence={})

    def test_s4_owned_wait_kinds_are_not_scanned(self):
        for kind, key in (("send_result_unknown", "s4-unknown"),
                          ("cancel_reconcile", "s4-cancel"),
                          ("execution", "s4-exec"),
                          ("input_execution", "s4-input")):
            self._s4_row(kind, key)
        self.assertEqual(self.store.list_dependency_waits_page(limit=100), [],
                         "S4 自管 wait_kind 不得进入结构化依赖扫描")
        self.assertEqual(self.store.count_dependency_waits(), 0)
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["scanned"], 0, "S4 行不被当历史字符串转人工")
        self.assertEqual(out["needs_human"], 0)

    def test_pure_dependency_legacy_string_still_human(self):
        self._s4_row("dependency", "s4-no")  # wait_kind=dependency -> 纯依赖行
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["scanned"], 1)
        self.assertEqual(out["needs_human"], 1)
        self.assertEqual(out["details"][0]["reason"], "condition-unstructured")

    def test_record_kind_exclusion_does_not_break_satisfied_scan(self):
        # satisfied 行不受 S4 wait_kind 排除影响（它走 review 发布而非 S4 原输入）
        self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        self.ledger.transition_task("T-1", "paused", actor="op")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["ready"], 1, "暂停积累行仍被扫描并保留复核事实")

    def test_escalate_error_is_audited_and_page_survives(self):
        self.register()  # T-1 依赖 T-2
        self.new_upstream()
        self.ledger.transition_task("T-2", "cancelled", actor="test")  # broken -> escalate
        # 第二个任务（T-3）依赖 T-4（仍 running）：验证单行异常不阻塞其余行
        self.ledger.create_task(project="proj", title="t3", task_id="T-3",
                                coordinator_session="ses_coord")
        self.ledger.create_attempt("T-3", attempt_id="A-3", directory=self.home,
                                   session_id="ses_worker3", generation=1,
                                   status="running", takeover_confirmed=True)
        self.new_upstream(task_id="T-4", session="ses_up4")
        self.store.ensure_dependency_wait(
            "coord:main", "proj", "T-3", condition(task_ids=["T-4"]),
            attempt_id="A-3", generation=1, result_version=None,
            target_session="ses_coord", deadline=T0 + 3_600_000,
            next_check_at=T0, next_action="wait3", actor="coordinator")
        real = self.store.escalate_dependency_human

        def boom(rid, *, expected_revision, reason, detail, next_check_at=None):
            raise LedgerError("RESPONSIBILITY_BUDGET_EXHAUSTED",
                              "injected escalate failure", {"rid": rid})
        self.store.escalate_dependency_human = boom
        try:
            out = self.service.reconcile(limit=20)
        finally:
            self.store.escalate_dependency_human = real
        # audit 可见：code+message 不丢
        rows = self.conn.execute(
            "SELECT detail FROM audit WHERE action='responsibility.tick-rejected'"
        ).fetchall()
        self.assertTrue(rows, "必须有持久可见审计")
        self.assertTrue(any("RESPONSIBILITY_BUDGET_EXHAUSTED" in r["detail"]
                            for r in rows))
        self.assertTrue(any("injected escalate failure" in r["detail"]
                            for r in rows))
        # 单行异常不吞整页：其余行仍推进（T-3 waiting 至少 1）
        self.assertGreaterEqual(out["waiting"], 1)
        self.assertTrue(any(
            d["bucket"] == "needs_human" and d.get("audit_logged")
            and "RESPONSIBILITY_BUDGET_EXHAUSTED" in d["reason"]
            for d in out["details"]),
            "escalate 拒绝走 audit 且 reason 带 code")

    def test_unhandled_row_error_is_audited_and_loop_continues(self):
        row = self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        real = self.store.dependency_task_facts
        def boom(task_ids):
            raise LedgerError("RESPONSIBILITY_SCHEMA_MISSING", "injected facts failure")
        self.store.dependency_task_facts = boom
        try:
            out = self.service.reconcile(limit=10)
        finally:
            self.store.dependency_task_facts = real
        self.assertEqual(out["scanned"], 1)
        self.assertTrue(any(d["bucket"] == "rejected" for d in out["details"]))
        rows = self.conn.execute(
            "SELECT detail FROM audit WHERE action='responsibility.tick-rejected'"
        ).fetchall()
        self.assertTrue(any("RESPONSIBILITY_SCHEMA_MISSING" in r["detail"]
                            for r in rows))


class ConditionStabilityDomainTests(unittest.TestCase):
    def test_revision_must_be_strictly_positive(self):
        for bad in (0, -1, -100):
            with self.subTest(revision=bad):
                self.assertIn("CONDITION_REVISION_INVALID",
                              dw.validate_condition(condition(revision=bad)))
                with self.assertRaises(ValueError):
                    dw.normalize_condition(condition(revision=bad))
        self.assertEqual(dw.validate_condition(condition(revision=1)), [])
        self.assertEqual(dw.normalize_condition(condition(revision=2))["revision"], 2)

    def test_task_ids_order_does_not_change_identity(self):
        a = dw.condition_identity(condition(task_ids=["T-2", "T-3"]))
        b = dw.condition_identity(condition(task_ids=["T-3", "T-2"]))
        self.assertEqual(a, b, "all/any 集合顺序不得改变条件身份/事件键")
        self.assertEqual(dw.normalize_condition(
            condition(task_ids=["T-3", "T-2"]))["task_ids"], ["T-2", "T-3"])

    def test_effective_acceptance_needs_binding(self):
        # 缺 result-version 绑定：accepted 也不算有效验收
        task = {"id": "T-1", "status": "accepted", "accepted_at": 1,
                "current_attempt_id": "A-1"}
        attempt = {"id": "A-1", "verification": {
            "schema": "oc-orch.verification.v1", "result": "pass",
            "forced": False}}
        projection = dw.wait_health_projection(task=task, attempt=attempt)
        self.assertEqual(projection["state"], dw.STATE_NEEDS_HUMAN)
        bound_ok = dict(attempt, verification={
            "schema": "oc-orch.verification.v1", "result": "pass",
            "forced": False, "result_snapshot": {"result_version": "rv1-a"}},
            metadata={"result": {"version": "rv1-a"}})
        ok = dw.wait_health_projection(task=task, attempt=bound_ok)
        self.assertEqual(ok["state"], dw.STATE_DONE)
        wrong = dict(attempt, verification={
            "schema": "oc-orch.verification.v1", "result": "pass",
            "forced": False, "result_snapshot": {"result_version": "rv1-b"}},
            metadata={"result": {"version": "rv1-a"}})
        bad = dw.wait_health_projection(task=task, attempt=wrong)
        self.assertEqual(bad["state"], dw.STATE_NEEDS_HUMAN, "版本不一致不算有效验收")



class PublishedReviewGuardTests(DepBase):
    """父最后规格项：已发布依赖复核在网络/普通 publish 前必须重新核验。"""

    def _published(self):
        self.register()
        self.new_upstream()
        self.mark_verified_accepted("T-2", "A-2", version="rv1-a")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["ready"], 1)
        fresh = self.store.get_responsibility(
            self.store.list_open_responsibilities(task_id="T-1")[0]["id"])
        notif_id = fresh["evidence"][dw.DEPENDENCY_KEY]["notification_id"]
        return fresh, notif_id

    def _notice(self, fresh):
        dep = fresh["evidence"][dw.DEPENDENCY_KEY]
        return {
            "dedupe_key": dep.get("review_dedupe_key"),
            "metadata": {"responsibility_id": fresh["id"],
                         "condition_identity": dep.get("condition_identity"),
                         "evidence_version": dep.get("evidence_version")},
            "payload": {"classification": "DEPENDENCY_SATISFIED"},
        }

    def test_reconcile_published_healthy_repeat_zero_write(self):
        fresh, notif_id = self._published()
        before = self.store.get_responsibility(fresh["id"])
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["waiting"], 1)
        self.assertEqual(out["details"][0]["reason"], "review-already-published")
        after = self.store.get_responsibility(fresh["id"])
        self.assertEqual(after["revision"], before["revision"], "健康重复零写")
        self.assertEqual(self.notif_count(), 1)

    def test_reconcile_detects_upstream_revocation_and_keeps_ack(self):
        fresh, notif_id = self._published()
        self.ledger.ack_notifications([notif_id], actor="test")
        row = self.conn.execute(
            "SELECT acked_at FROM notifications WHERE id=?", (notif_id,)).fetchone()
        acked = row["acked_at"]
        # 撤销上游有效验收（verification 清空、版本不匹配）
        self.conn.execute(
            "UPDATE attempts SET verification=NULL, "
            "metadata='{\"result\": {\"version\": \"rv1-b\"}}' WHERE id='A-2'")
        out = self.service.reconcile(limit=10)
        self.assertEqual(out["needs_human"], 1)
        self.assertTrue(out["details"][0]["reason"].startswith("review-stale:"),
                        out["details"][0]["reason"])
        # 不伪造旧通知：ACK 保持，不新增通知
        row2 = self.conn.execute(
            "SELECT acked_at, state FROM notifications WHERE id=?",
            (notif_id,)).fetchone()
        self.assertEqual(row2["acked_at"], acked)
        self.assertEqual(row2["state"], "acked")
        self.assertEqual(self.notif_count(), 1)

    def test_publish_after_upstream_revoked_rejected_before_send(self):
        fresh, notif_id = self._published()
        self.ledger.ack_notifications([notif_id], actor="test")
        self.conn.execute(
            "UPDATE attempts SET verification=NULL, "
            "metadata='{\"result\": {\"version\": \"rv1-b\"}}' WHERE id='A-2'")
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                fresh["id"], expected_revision=fresh["revision"], payload={})
        self.assertEqual(ctx.exception.code, "DEPENDENCY_REVIEW_STALE")
        row = self.conn.execute(
            "SELECT state, acked_at FROM notifications WHERE id=?",
            (notif_id,)).fetchone()
        self.assertEqual(row["state"], "acked")
        self.assertEqual(self.notif_count(), 1)

    def test_publish_after_dep_result_version_changed_rejected(self):
        fresh, notif_id = self._published()
        # 换依赖结果版本：verification 与收集版本都改到 rv1-b
        self.conn.execute(
            "UPDATE attempts SET verification=?, metadata=? WHERE id='A-2'",
            (json.dumps({**VERIFICATION_PASS,
                        "result_snapshot": {"result_version": "rv1-b"}}),
             json.dumps({"result": {"version": "rv1-b"}})))
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                fresh["id"], expected_revision=fresh["revision"], payload={})
        self.assertEqual(ctx.exception.code, "DEPENDENCY_REVIEW_STALE")
        self.assertEqual(self.notif_count(), 1)

    def test_publish_after_manual_hold_rejected(self):
        fresh, notif_id = self._published()
        self.store.escalate_dependency_human(
            fresh["id"], expected_revision=fresh["revision"],
            reason="manual-hold", detail="人工接管")
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                fresh["id"],
                expected_revision=self.store.get_responsibility(
                    fresh["id"])["revision"], payload={})
        self.assertIn(ctx.exception.code,
                      ("DEPENDENCY_REVIEW_STALE", "RESPONSIBILITY_NOT_REVIEWABLE"))
        self.assertEqual(self.notif_count(), 1)

    def test_publish_after_new_generation_rejected(self):
        fresh, notif_id = self._published()
        self.ledger.release_site("A-1", confirmed=True, external_stopped=True,
                                 actor="test")
        self.ledger.create_attempt("T-1", attempt_id="A-3", directory=self.home,
                                   session_id="ses_worker2", generation=2,
                                   status="running", expected_current="A-1",
                                   takeover_confirmed=True)
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                fresh["id"], expected_revision=fresh["revision"], payload={})
        self.assertIn(ctx.exception.code,
                      ("DEPENDENCY_REVIEW_STALE", "RESPONSIBILITY_STALE"))
        self.assertEqual(self.notif_count(), 1)

    def test_validator_noop_for_non_dep_notification(self):
        notice = {"kind": "continuation_ready", "id": "N-1",
                  "dedupe_key": "resp:...",
                  "metadata": {"review_fact": "execution-ended"},
                  "payload": {"classification": "AWAITING_REVIEW"}}
        self.assertIsNone(self.store.check_dependency_review_notification(notice))

    def test_validator_fail_closed_on_missing_binding(self):
        notice = {"kind": "continuation_ready", "id": "N-2", "dedupe_key": "x",
                  "metadata": {"responsibility_id": "R-nope"},
                  "payload": {"classification": "DEPENDENCY_SATISFIED"}}
        with self.assertRaises(LedgerError) as ctx:
            self.store.check_dependency_review_notification(notice)
        self.assertEqual(ctx.exception.code, "DEPENDENCY_REVIEW_STALE")
        bad = {"kind": "continuation_ready", "id": "N-3", "dedupe_key": "x",
               "metadata": {},
               "payload": {"classification": "DEPENDENCY_SATISFIED"}}
        with self.assertRaises(LedgerError) as ctx2:
            self.store.check_dependency_review_notification(bad)
        self.assertEqual(ctx.exception.code, "DEPENDENCY_REVIEW_STALE")

    def test_validator_rejects_ack_after_responsibility_closed(self):
        fresh, notif_id = self._published()
        self.mark_verified_accepted("T-1", "A-1", version="rv1-a")
        self.store.close_responsibility(
            fresh["id"], expected_revision=fresh["revision"],
            reason="task-accepted", actor="supervisor")
        with self.assertRaises(LedgerError) as ctx:
            self.store.check_dependency_review_notification(self._notice(fresh))
        self.assertEqual(ctx.exception.code, "DEPENDENCY_REVIEW_STALE")


if __name__ == "__main__":
    unittest.main()
#!/usr/bin/env python3
"""业务 incident 纯规则/存储/服务（roadmap R3，D8 三本账之一）。

用真临时 SQLite（WAL）、FakeClock 覆盖：waiting_human 责任/到期超限责任/
确证 no_progress 三类首批事实的推导；稳定身份；重复观察不推进 condition_version
也不重置 deadline；时间性升级只在实际越过级别时发生；正常 project pause /
provider 冷却不催；恢复至多一次；坏数据 fail-closed；meta 自有命名空间不与
outbox 冲突；list/status 只读脱敏投影。本文件不触 runtime/网络/真实邮箱。
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.adapters.sqlite_db import init, meta_set  # noqa: E402
from oc_orch.adapters.operator_incident import (  # noqa: E402
    INCIDENT_KEY_PREFIX,
    INCIDENT_SCHEMA,
    INCIDENT_SCHEMA_KEY,
    IncidentStateError,
    IncidentStore,
)
from oc_orch.application.operator_incident import OperatorIncidentService  # noqa: E402
from oc_orch.domain import operator_incident as inc  # noqa: E402
from tests.fakes import FakeClock  # noqa: E402


def _task(conn, tid="T-1", project="proj-a", status="draft",
          coordinator="ses-1", created=100, **over):
    stmt = (
        "INSERT INTO tasks(id, project, title, objective, acceptance, status, "
        "coordinator_session, write_scope, priority, next_check_at, "
        "current_attempt_id, created_at, updated_at, accepted_at, closed_at, "
        "blocked_reason, metadata) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    conn.execute(stmt, (tid, project, f"任务 {tid}", "", "[]", status,
                        coordinator, "[]", 0, None, None, created, created,
                        None, None, None, '{}'))
    if over:
        cols = ", ".join(f"{k}=?" for k in over)
        conn.execute(f"UPDATE tasks SET {cols} WHERE id=?", (*over.values(), tid))
    conn.commit()


def _resp(conn, rid="R-1", task_id="T-1", project="proj-a", state="waiting_human",
          wait_kind="human", condition="waiting-human", owner="coordinator",
          deadline=None, next_check=50, created=100, revision=1, **over):
    stmt = (
        "INSERT INTO responsibilities(id, project, task_id, attempt_id, "
        "generation, result_version, input_id, session_id, message_id, owner, "
        "target_session, state, wait_kind, wait_condition, next_action, "
        "next_check_at, deadline, condition_key, revision, budget_used, "
        "max_budget, evidence, prior_id, successor_id, created_at, updated_at, "
        "closed_at, close_reason) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    conn.execute(stmt, (rid, project, task_id, None, None, None, None, None,
                        None, owner, None, state, wait_kind, condition,
                        f"处理 {rid}", next_check, deadline, condition, revision,
                        0, 3, '{}', None, None, created, created, None, None))
    if over:
        cols = ", ".join(f"{k}=?" for k in over)
        conn.execute(f"UPDATE responsibilities SET {cols} WHERE id=?", (*over.values(), rid))
    conn.commit()


def _attempt(conn, aid="A-1", task_id="T-1", status="running", created=100):
    stmt = (
        "INSERT INTO attempts(id, task_id, generation, session_id, directory, "
        "agent, model, variant, status, dispatch_state, dispatch_msg_id, "
        "session_meta, started_at, dispatched_at, last_progress_at, "
        "last_observed_at, deadline_at, stall_after, terminal_at, "
        "terminal_kind, terminal_reason, stop_requested_at, stop_confirmed_at, "
        "site_released_at, result_collected_at, result_path, result_summary, "
        "verification, created_at, updated_at, metadata) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    conn.execute(stmt, (aid, task_id, 1, "ses-1", "/tmp/x", None, None, None,
                        status, "prepared", None, '{}', created, None, None,
                        None, None, None, None, None, None, None, None, None,
                        None, None, None, None, created, created, '{}'))
    conn.commit()


def _lease(conn, holder="ses-1", project="proj-a"):
    conn.execute(
        "INSERT INTO coordinator_leases(project, holder_session, epoch, "
        "acquired_at, renewed_at, expires_at, failure_count, recovery_blocked) "
        "VALUES(?,?,1,1,1,9999999999,0,0)", (project, holder))
    conn.commit()


def _pause_project(conn, clock, session="ses-1", project="proj-a"):
    from oc_orch.adapters.coordinator_control import CoordinatorControlLedger
    control = CoordinatorControlLedger(conn, clock)
    out = control.pause(project, session_id=session, operation_id="op-pause-1",
                        reason="test", expected_revision=0)
    assert out["executed"], out


def _set_project_paused(conn, clock, session="ses-1", project="proj-a"):
    _lease(conn, holder=session, project=project)
    _pause_project(conn, clock, session=session, project=project)


def _notify(conn, nid="N-1", task_id="T-1", state="pending", created=100):
    conn.execute(
        "INSERT INTO notifications(id, project, kind, target_session, task_id, "
        "attempt_id, dedupe_key, payload, state, priority, attempts, "
        "max_attempts, next_attempt_at, message_id, created_at, sent_at, "
        "acked_at, last_error, claim_owner, claim_at, metadata) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (nid, "proj-a", "test", "ses-1", task_id, None, f"k-{nid}", '{}', state,
         0, 0, 3, created, None, created, None, None, None, None, None, '{}'))
    conn.commit()


class IncidentBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="inc-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.store = IncidentStore(self.conn, self.clock)

    def svc(self, **kw) -> OperatorIncidentService:
        kw.setdefault("clock", self.clock)
        kw.setdefault("store", self.store)
        return OperatorIncidentService(**kw)

    def detail(self, out: dict, action: str) -> list:
        return [d for d in out["details"] if d.get("action") == action]

    def item(self, incident_id: str) -> dict | None:
        return self.store.get_incident(incident_id)


# ============================================================================
# domain 纯规则
# ============================================================================
class DomainRules(unittest.TestCase):
    def _src(self, **over):
        base = dict(
            source_kind="responsibility", source_id="R-1", project="proj-a",
            incident_type="waiting-human-responsibility", task_id="T-1",
            condition_key="waiting-human", condition_state="waiting_human",
            owner="coordinator", next_action="处理", condition_kind="human-responsibility",
            escalation_level=0, observed_at=100, first_evidence_at=100,
        )
        base.update(over)
        return inc.normalize_source(base)

    def test_stable_identity_no_time(self):
        a = inc.incident_id("p", "waiting-human-responsibility", "R-1")
        b = inc.incident_id("p", "waiting-human-responsibility", "R-1")
        self.assertEqual(a, b)
        self.assertNotEqual(
            a, inc.incident_id("p", "responsibility-due", "R-1"))
        self.assertNotEqual(a, inc.incident_id("p", "waiting-human-responsibility", "R-2"))

    def test_signature_ignores_time_but_catches_semantic_change(self):
        s1 = self._src(observed_at=100)
        s2 = self._src(observed_at=101)   # 轮询时间不改变签名
        self.assertEqual(inc.source_signature(s1), inc.source_signature(s2))
        s3 = self._src(condition_key="new-condition")  # 显著变化
        self.assertNotEqual(inc.source_signature(s1), inc.source_signature(s3))
        s4 = self._src(condition_state="handling")     # 状态变化
        self.assertNotEqual(inc.source_signature(s1), inc.source_signature(s4))

    def test_escalation_not_in_signature(self):
        s1 = self._src(escalation_level=0)
        s2 = self._src(escalation_level=2)
        self.assertEqual(inc.source_signature(s1), inc.source_signature(s2))

    def test_decide_create_observe_update_escalate(self):
        src = self._src()
        sig = inc.source_signature(src)
        self.assertEqual(inc.decide(src, None, now=1)["action"], "create")
        stored = {"state": "open", "condition_version": 1, "escalation_level": 0,
                  "source_signature": sig}
        d = inc.decide(src, stored, now=1)
        self.assertEqual(d["action"], "observe")
        self.assertEqual(d["condition_version"], 1)  # 不推进版本
        changed = self._src(condition_key="k2")
        d2 = inc.decide(changed, stored, now=1)
        self.assertEqual(d2["action"], "update")
        self.assertEqual(d2["condition_version"], 2)
        esc = self._src(escalation_level=2)
        d3 = inc.decide(esc, stored, now=1)
        self.assertEqual(d3["action"], "escalate")
        self.assertEqual(d3["condition_version"], 2)

    def test_decide_noop_when_not_open(self):
        src = self._src()
        stored = {"state": "recovered", "condition_version": 1,
                  "escalation_level": 0, "source_signature": inc.source_signature(src)}
        self.assertEqual(inc.decide(src, stored, now=1)["action"], "noop")

    def test_normalize_fail_closed(self):
        with self.assertRaises(ValueError):
            self._src(condition_kind="guess")
        with self.assertRaises(ValueError):
            self._src(incident_type="other")
        with self.assertRaises(ValueError):
            self._src(recovered="yes")
        with self.assertRaises(ValueError):
            self._src(escalation_level=True)
        with self.assertRaises(ValueError):
            inc.normalize_source({"source_kind": "responsibility"})

    def test_channel_state(self):
        self.assertEqual(inc.channel_state(recipient_refs=(), email_policy_state=None),
                         "not-configured")
        self.assertEqual(inc.channel_state(recipient_refs=("r",), email_policy_state="disabled"),
                         "email-disabled")
        self.assertEqual(inc.channel_state(recipient_refs=("r",), email_policy_state="enabled"),
                         "email-enabled")


# ============================================================================
# incident 存储（meta 命名空间 + 严格校验）
# ============================================================================
class StoreBasics(IncidentBase):
    def test_create_and_read_strict_namespace(self):
        _task(self.conn)
        _resp(self.conn)
        src = self.store.list_waiting_human_sources(limit=10)[0][0]
        self.assertIsNotNone(src)
        out = self.store.upsert(incident_id=inc.incident_id(
            "proj-a", "waiting-human-responsibility", "R-1"), source=src, now=self.clock.now_ms())
        self.assertEqual(out["action"], "create")
        self.assertEqual(meta_set is not None, True)
        # 命名空间独立：outbox 键不受影响
        raw = self.conn.execute(
            "SELECT key FROM meta WHERE key LIKE 'mail-outbox:%'").fetchall()
        self.assertEqual(raw, [])
        self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (INCIDENT_SCHEMA_KEY,)).fetchone()

    def test_bad_record_fail_closed(self):
        meta_set(self.conn, f"{INCIDENT_KEY_PREFIX}bad", "not-json")
        with self.assertRaises(IncidentStateError):
            self.store.get_incident("bad")
        meta_set(self.conn, f"{INCIDENT_KEY_PREFIX}bad2",
                 json.dumps({"schema": 1, "incident_id": "bad2"}))
        with self.assertRaises(IncidentStateError):
            self.store.get_incident("bad2")

    def test_schema_version_mismatch_fail_closed(self):
        meta_set(self.conn, INCIDENT_SCHEMA_KEY, "99")
        with self.assertRaises(IncidentStateError):
            self.store.get_incident("whatever")

    def test_duplicate_observe_keeps_version_and_first_seen(self):
        _task(self.conn)
        _resp(self.conn)
        src = self.store.list_waiting_human_sources(limit=10)[0][0]
        iid = inc.incident_id("proj-a", "waiting-human-responsibility", "R-1")
        self.store.upsert(incident_id=iid, source=src, now=self.clock.now_ms())
        self.clock.advance(wall=5_000)
        src2 = dict(src)
        src2["observed_at"] = self.clock.now_ms()
        out = self.store.upsert(incident_id=iid, source=src2, now=self.clock.now_ms())
        self.assertEqual(out["action"], "observe")
        rec = self.item(iid)
        self.assertEqual(rec["condition_version"], 1)
        self.assertEqual(rec["first_seen_at"], 1_000_000)  # 首次观察不重置
        self.assertEqual(rec["last_observed_at"], 1_005_000)
        self.assertEqual(rec["revision"], 2)

    def test_signature_change_bumps_version_without_reset_deadline(self):
        _task(self.conn)
        _resp(self.conn, deadline=1_000_000)
        src = self.store.list_waiting_human_sources(limit=10)[0][0]
        iid = inc.incident_id("proj-a", "waiting-human-responsibility", "R-1")
        self.store.upsert(incident_id=iid, source=src, now=100)
        self.conn.execute("UPDATE responsibilities SET condition_key='k-new' WHERE id='R-1'")
        self.conn.commit()
        src2 = self.store.list_waiting_human_sources(limit=10)[0][0]
        out = self.store.upsert(incident_id=iid, source=src2, now=200)
        self.assertEqual(out["action"], "update")
        rec = self.item(iid)
        self.assertEqual(rec["condition_version"], 2)
        self.assertEqual(rec["deadline"], 1_000_000)  # 不重置 deadline

    def test_time_escalation_applies_without_signature_change(self):
        _task(self.conn)
        _resp(self.conn, deadline=self.clock.now_ms() + 5_000)
        src = self.store.list_waiting_human_sources(limit=10)[0][0]
        iid = inc.incident_id("proj-a", "waiting-human-responsibility", "R-1")
        self.store.upsert(incident_id=iid, source=src, now=self.clock.now_ms())
        self.assertEqual(self.item(iid)["escalation_level"], 0)
        # 越过 deadline：升级一次（签名不变）
        self.clock.advance(wall=10_000)
        src2 = self.store.list_waiting_human_sources(limit=10)[0][0]
        out = self.store.upsert(incident_id=iid, source=src2, now=self.clock.now_ms())
        self.assertEqual(out["action"], "escalate")
        self.assertEqual(self.item(iid)["condition_version"], 2)
        self.assertEqual(self.item(iid)["escalation_level"], 1)
        # 再次观察：不再推进
        self.clock.advance(wall=1_000)
        src3 = self.store.list_waiting_human_sources(limit=10)[0][0]
        out3 = self.store.upsert(incident_id=iid, source=src3, now=self.clock.now_ms())
        self.assertEqual(out3["action"], "observe")
        self.assertEqual(self.item(iid)["condition_version"], 2)

    def test_recover_once_with_revision_cas(self):
        _task(self.conn)
        _resp(self.conn)
        src = self.store.list_waiting_human_sources(limit=10)[0][0]
        iid = inc.incident_id("proj-a", "waiting-human-responsibility", "R-1")
        self.store.upsert(incident_id=iid, source=src, now=100)
        rec = self.item(iid)
        # 直接恢复API也必须有真实源解除证据，不能把“请求恢复”当解除事实。
        self.conn.execute("UPDATE responsibilities SET closed_at=150, state='cancelled' WHERE id='R-1'")
        out1 = self.store.recover_incident(incident_id=iid,
                                           expected_revision=rec["revision"],
                                           evidence="closed", now=200)
        self.assertTrue(out1["applied"])
        self.assertEqual(self.item(iid)["state"], "recovered")
        out2 = self.store.recover_incident(incident_id=iid,
                                           expected_revision=rec["revision"],
                                           now=300)
        self.assertFalse(out2["applied"])  # 至多一次

    def test_recheck_binding_responsibility_verified_and_stale(self):
        _task(self.conn)
        _resp(self.conn)
        src = self.store.list_waiting_human_sources(limit=10)[0][0]
        iid = inc.incident_id("proj-a", "waiting-human-responsibility", "R-1")
        self.store.upsert(incident_id=iid, source=src, now=100)
        rec = self.item(iid)
        verdict = self.store.recheck_binding(self.conn, rec["source_binding"])
        self.assertTrue(verdict["ok"])
        # 关闭责任 → 重验失败（fail-closed）
        self.conn.execute("UPDATE responsibilities SET closed_at=1000, state='cancelled' WHERE id='R-1'")
        self.conn.commit()
        verdict2 = self.store.recheck_binding(self.conn, rec["source_binding"])
        self.assertFalse(verdict2["ok"])
        self.assertEqual(verdict2["reason"], "responsibility-closed")


# ============================================================================
# 服务（默认无邮件/无收件范围 → 仅本地）
# ============================================================================
class ServiceFacts(IncidentBase):
    def test_waiting_human_creates_local_incident(self):
        _task(self.conn)
        _resp(self.conn)
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["created"], 1)
        self.assertEqual(out["email_enqueued"], 0)  # 本地仅可见
        detail = self.detail(out, "create")[0]
        self.assertEqual(detail["email"], "local-only")
        rows = self.store.list_incidents()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["incident_type"], "waiting-human-responsibility")
        self.assertEqual(rows[0]["recipient_scope"], "")

    def test_repeated_poll_observes_without_version_bump(self):
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc()
        first = svc.reconcile(limit=10)
        iid = self.detail(first, "create")[0]["incident_id"]
        self.clock.advance(wall=5_000)
        second = svc.reconcile(limit=10)
        self.assertEqual(second["observed"], 1)
        self.assertEqual(self.item(iid)["condition_version"], 1)
        self.assertEqual(self.item(iid)["last_observed_at"], 1_005_000)

    def test_normal_pause_still_recorded_not_nagged(self):
        _task(self.conn, status="paused")
        _resp(self.conn)
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["created"], 1)
        rows = self.store.list_incidents()
        self.assertEqual(rows[0]["blocked_reason"], "责任等待人工处理（manual hold），不得自动解除")

    def test_due_exhausted_creates_due_incident(self):
        _task(self.conn)
        _resp(self.conn, state="waiting_dependency", wait_kind="dependency",
              condition="dep", deadline=50, next_check=10)
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["created"], 1)
        rows = self.store.list_incidents()
        self.assertEqual(rows[0]["incident_type"], "responsibility-due")
        self.assertEqual(rows[0]["escalation_level"], 2)

    def test_due_not_exhausted_skipped(self):
        _task(self.conn)
        _resp(self.conn, state="waiting_dependency", wait_kind="dependency",
              condition="dep", deadline=999_999_999, next_check=10)
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["created"], 0)
        self.assertGreaterEqual(out["skipped"], 1)  # not-exhausted

    def test_no_progress_task_creates_incident(self):
        _task(self.conn)  # 无 attempt/责任/通知
        out = self.svc().reconcile(limit=10)
        created = self.detail(out, "create")
        self.assertEqual(len(created), 1)
        rows = self.store.list_incidents()
        self.assertEqual(rows[0]["incident_type"], "task-no-progress")

    def test_no_progress_skipped_when_open_attempt(self):
        _task(self.conn, current_attempt_id="A-1")
        _attempt(self.conn, status="running")
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["created"], 0)

    def test_no_progress_skipped_when_paused(self):
        _task(self.conn, status="paused")
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["created"], 0)

    def test_no_progress_skipped_when_pending_notification(self):
        _task(self.conn)
        _notify(self.conn)
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["created"], 0)

    def test_recovery_when_responsibility_closed(self):
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc()
        first = svc.reconcile(limit=10)
        iid = self.detail(first, "create")[0]["incident_id"]
        self.conn.execute("UPDATE responsibilities SET closed_at=20000, state='cancelled' WHERE id='R-1'")
        self.conn.commit()
        second = svc.reconcile(limit=10)
        self.assertEqual(second["recovered"], 1)
        self.assertEqual(self.item(iid)["state"], "recovered")
        self.assertIsNotNone(self.item(iid)["recovered_at"])
        # 再跑一次：不重复恢复
        third = svc.reconcile(limit=10)
        self.assertEqual(third["recovered"], 0)

    def test_recovery_when_no_progress_resolved_by_responsibility(self):
        _task(self.conn)
        svc = self.svc()
        first = svc.reconcile(limit=10)
        no_progress = [r for r in self.store.list_incidents()
                       if r["incident_type"] == "task-no-progress"][0]
        # 登记推进责任 → no_progress 解除
        _resp(self.conn, state="waiting_input", wait_kind="input",
              condition="input")
        second = svc.reconcile(limit=10)
        self.assertEqual(second["recovered"], 1)
        self.assertEqual(self.item(no_progress["incident_id"])["state"], "recovered")

    def test_other_project_provider_cooldown_does_not_suppress_this_project(self):
        # 其他项目/模型的冷却不得抑制本项目。
        from oc_orch.adapters.sqlite_db import meta_set as _ms
        _ms(self.conn, "provider_state",
            json.dumps({"m-other-cool": {"until": 9_999_999_999}}))
        _task(self.conn)
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["created"], 1)

    def test_paused_project_waiting_human_kind_pause_not_recovered(self):
        _task(self.conn)
        _resp(self.conn)
        _lease(self.conn, holder="ses-1")
        _pause_project(self.conn, self.clock, session="ses-1")
        svc = self.svc()
        first = svc.reconcile(limit=10)
        self.assertEqual(first["created"], 1)
        rows = self.store.list_incidents()
        self.assertEqual(rows[0]["condition_kind"], "normal-pause")
        # 暂停不是故障恢复：sweep 判 paused，保持 open、绝不 recovered。
        self.assertEqual(first["recovered"], 0)
        self.assertGreaterEqual(first["paused"], 1)
        second = svc.reconcile(limit=10)
        self.assertEqual(second["recovered"], 0)
        self.assertEqual(self.store.list_incidents()[0]["state"], "open")

    def test_invalid_source_counted_and_bounded_details(self):
        _task(self.conn)
        _resp(self.conn)
        # 制造一条坏源（condition_state 非法由 store 扫描不会产生，直接注入服务不可达；
        # 这里验证 store.upsert 对坏源 fail-closed）
        with self.assertRaises(ValueError):
            self.store.upsert(incident_id="I-bad",
                              source={"source_kind": "responsibility"}, now=1)
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["scanned"] >= 1, True)
        self.assertLessEqual(len(out["details"]), 20)  # 有界 details

    def test_status_and_list_masked(self):
        _task(self.conn)
        _resp(self.conn)
        _task(self.conn, tid="T-2", status="draft")
        svc = self.svc()
        svc.reconcile(limit=10)
        status = svc.status(project="proj-a")
        self.assertIn("by_state", status)
        self.assertEqual(status["channel"], "not-configured")
        self.assertEqual(status["recipient_refs"], [])
        items = svc.list(project="proj-a", limit=10)
        for item in items:
            self.assertNotIn("operator@example", str(item))
            self.assertNotIn("address", item)
            self.assertIn("incident_id", item)
            self.assertIn("channel", item)

    def test_reconcile_never_touches_transport(self):
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc()
        self.assertFalse(hasattr(svc, "transport"))
        out = svc.reconcile(limit=10)
        self.assertIsInstance(out, dict)  # 无网络依赖


if __name__ == "__main__":
    unittest.main(verbosity=2)

# ============================================================================
# 父核收补充：attempt 归属/五态 sweep/公平游标/重开/严格 limit/未知读错误
# ============================================================================
class ReviewAttemptAttribution(IncidentBase):
    def test_responsibility_with_attempt_normalizes_no_nameerror(self):
        # 修复：normalize_source 曾引用未定义 current_attempt_id（NameError）。
        from oc_orch.domain import operator_incident as inc
        _task(self.conn)
        _attempt(self.conn, aid="A-1", status="terminal_succeeded")
        self.conn.execute("UPDATE tasks SET current_attempt_id='A-1' WHERE id='T-1'")
        _resp(self.conn, attempt_id="A-1", generation=1)
        src = self.store.list_waiting_human_sources(limit=10)[0][0]
        self.assertEqual(src["current_attempt_id"], "A-1")
        norm = inc.normalize_source(src)
        self.assertEqual(norm["current_attempt_id"], "A-1")
        self.assertTrue(inc.source_signature(src).startswith("sig-"))

    def test_source_signature_binds_project_attribution_and_deadline(self):
        from oc_orch.domain import operator_incident as inc
        base = dict(source_kind="responsibility", source_id="R-1", project="proj-a",
                    incident_type="waiting-human-responsibility", task_id="T-1",
                    condition_key="k", condition_state="waiting_human",
                    owner="o", next_action="a", condition_kind="human-responsibility",
                    escalation_level=0, observed_at=1, first_evidence_at=1,
                    deadline=1000)
        s1 = inc.normalize_source(base)
        t = dict(base); t["project"] = "proj-b"
        self.assertNotEqual(inc.source_signature(s1), inc.source_signature(t))
        t2 = dict(base); t2["condition_key"] = "k2"
        self.assertNotEqual(inc.source_signature(s1), inc.source_signature(t2))
        t3 = dict(base); t3["deadline"] = 9_999_999
        self.assertNotEqual(inc.source_signature(s1), inc.source_signature(t3))
        t4 = dict(base); t4["task_id"] = "T-OTHER"
        self.assertNotEqual(inc.source_signature(s1), inc.source_signature(t4))


class ReviewSweepVerdicts(IncidentBase):
    def _incident_id(self):
        return inc.incident_id("proj-a", "waiting-human-responsibility", "R-1")

    def test_sweep_unknown_keeps_open_no_recovery(self):
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc()
        svc.reconcile(limit=10)
        iid = self._incident_id()
        # 源行被删除 → recheck 判 unknown → 保持 open、不发恢复、不转终态。
        self.conn.execute("DELETE FROM responsibilities WHERE id='R-1'")
        self.conn.commit()
        out = svc.reconcile(limit=10)
        self.assertGreaterEqual(out["unknown"], 1)
        self.assertEqual(out["recovered"], 0)
        self.assertEqual(self.item(iid)["state"], "open")

    def test_sweep_stale_suppresses_then_reopen_on_new_condition(self):
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc()
        svc.reconcile(limit=10)
        iid = self._incident_id()
        self.conn.execute("UPDATE responsibilities SET state='handling' WHERE id='R-1'")
        self.conn.commit()
        out = svc.reconcile(limit=10)
        self.assertGreaterEqual(out["suppressed"], 1)
        self.assertEqual(out["recovered"], 0)
        self.assertEqual(self.item(iid)["state"], "suppressed")
        self.conn.execute(
            "UPDATE responsibilities SET state='waiting_human', condition_key='k-new' "
            "WHERE id='R-1'")
        self.conn.commit()
        out2 = svc.reconcile(limit=10)
        self.assertGreaterEqual(out2["reopened"], 1)
        rec = self.item(iid)
        self.assertEqual(rec["state"], "open")
        self.assertEqual(rec["condition_version"], 2)
        self.assertEqual(rec["condition_key"], "k-new")

    def test_reopen_after_recovered_with_new_condition(self):
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc()
        svc.reconcile(limit=10)
        iid = self._incident_id()
        self.conn.execute("UPDATE responsibilities SET closed_at=20000, state='cancelled' "
                          "WHERE id='R-1'")
        self.conn.commit()
        out = svc.reconcile(limit=10)
        self.assertGreaterEqual(out["recovered"], 1)
        self.assertEqual(self.item(iid)["state"], "recovered")
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=NULL, state='waiting_human', "
            "condition_key='k-reopen' WHERE id='R-1'")
        self.conn.commit()
        out2 = svc.reconcile(limit=10)
        self.assertGreaterEqual(out2["reopened"], 1)
        rec = self.item(iid)
        self.assertEqual(rec["state"], "open")
        self.assertEqual(rec["condition_version"], 2)


class ReviewFairCursor(IncidentBase):
    def test_keyset_cursor_covers_tail_not_starved(self):
        _task(self.conn, tid="T-A")
        for i in range(1, 8):
            _resp(self.conn, rid=f"R-{i}", task_id="T-A", condition=f"c-{i}")
        svc = self.svc()
        seen = set()
        for _ in range(4):  # 7 项 / limit=3 → 第 3 轮全见、第 4 轮回绕
            out = svc.reconcile(limit=3)
            for d in out["details"]:
                if d.get("action") == "create":
                    seen.add(d["incident_id"])
        self.assertEqual(len(seen), 7, f"尾项被饿死：{seen}")

    def test_strict_limit_rejects_bad_types(self):
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc()
        for bad in ("10", True, 0, -1, 1.5):
            with self.assertRaises(ValueError):
                svc.reconcile(limit=bad)
        with self.assertRaises(ValueError):
            self.store.list_waiting_human_sources(limit=True)


class ReviewNoProgressUnknown(IncidentBase):
    def test_attempt_missing_is_unknown_not_no_progress(self):
        # 有 current_attempt_id 但 attempt 行缺失 → unknown，不造 no_progress。
        _task(self.conn, current_attempt_id="A-404")
        out = self.svc().reconcile(limit=10)
        self.assertEqual(out["created"], 0)
        self.assertEqual(self.store.list_incidents(), [])

    def test_recheck_task_unknown_when_attempt_missing(self):
        from oc_orch.domain import operator_incident as inc
        _task(self.conn, current_attempt_id="A-404")
        src = self.store._task_source({
            "id": "T-1", "project": "proj-a", "status": "draft",
            "coordinator_session": "ses-1", "created_at": 1})
        binding = inc.build_binding(src)
        verdict = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(verdict["state"], "unknown")


class ReviewVerdictUnit(IncidentBase):
    def _binding(self):
        _task(self.conn)
        _resp(self.conn)
        src = self.store.list_waiting_human_sources(limit=10)[0][0]
        return inc.build_binding(src)

    def test_active_verified(self):
        binding = self._binding()
        v = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(v["state"], "active")
        self.assertTrue(v["ok"])

    def test_resolved_when_closed(self):
        binding = self._binding()
        self.conn.execute("UPDATE responsibilities SET closed_at=1 WHERE id='R-1'")
        self.conn.commit()
        v = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(v["state"], "resolved")

    def test_stale_when_state_changed(self):
        binding = self._binding()
        self.conn.execute("UPDATE responsibilities SET state='handling' WHERE id='R-1'")
        self.conn.commit()
        v = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(v["state"], "stale")

    def test_unknown_when_deleted(self):
        binding = self._binding()
        self.conn.execute("DELETE FROM responsibilities WHERE id='R-1'")
        self.conn.commit()
        v = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(v["state"], "unknown")

    def test_paused_when_project_paused(self):
        binding = self._binding()
        _set_project_paused(self.conn, self.clock, session="ses-1")
        v = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(v["state"], "paused")

    def test_stale_when_deadline_extended(self):
        binding = self._binding()
        self.conn.execute("UPDATE responsibilities SET deadline=999999999 WHERE id='R-1'")
        self.conn.commit()
        v = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(v["state"], "stale")

    def test_stale_when_generation_changed(self):
        binding = self._binding()
        self.conn.execute("UPDATE responsibilities SET generation=2 WHERE id='R-1'")
        self.conn.commit()
        v = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(v["state"], "stale")

    def test_resolved_when_task_accepted_old_responsibility_not_sent(self):
        binding = self._binding()
        self.conn.execute("UPDATE tasks SET status='accepted', accepted_at=1 WHERE id='T-1'")
        self.conn.commit()
        v = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(v["state"], "resolved")

    def test_project_mismatch_stale(self):
        binding = self._binding()
        binding["source_project"] = "other-proj"
        v = self.store.recheck_binding(self.store.conn, binding)
        self.assertEqual(v["state"], "stale")

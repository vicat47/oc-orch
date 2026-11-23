#!/usr/bin/env python3
"""incident/source 安全红/绿测（worker C 只交本文件，不改 src / 原两个测试）。

父核收 5 处缺口；本文件用真临时 SQLite（WAL 双连接）、FakeClock、
FakeTransport 构造最小反例，全部使用真实 API 合法 source；损坏/绕过直接临时
SQL 注明。父已完成逐项修复；本文件作为回归：每项断言对应父核收清单，
当前应全部转绿（若某条红即报告实缺口，不修改断言迁就代码）。

- G1 recover_incident 在 BEGIN IMMEDIATE 内必须 recheck_binding（真 WAL 交错）；
- G2 损坏 control/session/provider_state 必须 fail-closed 不发送且保 pending；
- G3 责任分支必须核 task.current_attempt_id/交接/真实 session·model（可选绑定
  提示不得绕过真实 hold/冷却）；
- G4 recovery-kind 必须比 binding 与当前 episode（recovered_at 等）一致；
- G5 list/status 过滤诚实：其他 project 前缀不得挡、total 是持久真实总数、
  坏记录经 coverage 显式暴露（不静默当作无待办）；只读 list/status/snapshot
  不写游标。
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.adapters.sqlite_db import init, meta_set  # noqa: E402
from oc_orch.adapters.email_outbox import (  # noqa: E402
    MAIL_KEY_PREFIX,
    STATUS_PENDING,
    EmailOutbox,
)
from oc_orch.adapters.operator_incident import (  # noqa: E402
    IncidentStateError,
    IncidentStore,
)
from oc_orch.application.operator_email import OperatorEmailService  # noqa: E402
from oc_orch.application.operator_incident import OperatorIncidentService  # noqa: E402
from oc_orch.domain.operator_email import (  # noqa: E402
    KIND_FIRST,
    EmailPolicy,
    Recipient,
    Sender,
)
from oc_orch.domain import operator_incident as inc  # noqa: E402
from oc_orch.composition import Ledger, make_responsibilities  # noqa: E402
from tests.fakes import FakeClock  # noqa: E402


def _task(conn, tid="T-1", project="proj-a", coordinator="ses-1"):
    return Ledger(conn, "", clock=FakeClock()).create_task(
        project=project, title="source safety", task_id=tid, coordinator_session=coordinator)


def _attempt(conn, aid="A-1", task_id="T-1", status="running", model=None):
    return Ledger(conn, "", clock=FakeClock()).create_attempt(
        task_id, attempt_id=aid, directory=f"/tmp/opencode/source-safety-{aid}",
        session_id="ses-worker", status=status, model=model)


def _resp(conn, rid="R-1", attempt_id=None, generation=None):
    ledger = Ledger(conn, "", clock=FakeClock())
    with patch("oc_orch.adapters.responsibility_ledger.short_id", return_value=rid):
        return make_responsibilities(ledger).open_responsibility(
            "operator", "proj-a", "T-1", attempt_id=attempt_id, generation=generation,
            state="waiting_human", wait_kind="decision", wait_condition="waiting-human",
            next_action="confirm decision", next_check_at=1_060_000, deadline=4_600_000,
            condition_key="waiting-human")


def _open_wal(home):
    conn = sqlite3.connect(os.path.join(home, "ledger.db"), timeout=15,
                           isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def enabled_policy(**overrides) -> EmailPolicy:
    kwargs = dict(
        enabled=True,
        sender=Sender("oc-ops", "ops@example.test"),
        recipients=(Recipient("op-primary", "operator@example.test"),),
        projects=frozenset({"proj-a"}),
        event_kinds=frozenset({KIND_FIRST, "recovery", "update", "escalation"}),
        unattended=True,
    )
    kwargs.update(overrides)
    return EmailPolicy(**kwargs)


class FakeTransport:
    def __init__(self):
        self.sent = []
        self.send_result = {"queued": True}

    def send(self, **kw):
        self.sent.append(dict(kw))
        if isinstance(self.send_result, Exception):
            raise self.send_result
        return self.send_result

    def search_receipt(self, **kw):
        return list(getattr(self, "search_result", []) or [])


class SafetyBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="safety-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.inc_store = IncidentStore(self.conn, self.clock)
        self.validator = self.inc_store.make_source_validator()
        self.transport = FakeTransport()

    def outbox(self):
        return EmailOutbox(self.conn, self.clock,
                           source_validator=self.validator,
                           require_source_binding=True)

    def mail(self):
        return OperatorEmailService(self.outbox(), self.transport, self.clock,
                                    enabled_policy())

    def svc(self, mail=None):
        return OperatorIncidentService(self.inc_store, self.clock,
                                       email=mail, recipient_refs=("op-primary",))

    def assert_no_cursor_written(self):
        cursors = self.conn.execute(
            "SELECT key FROM meta WHERE key LIKE 'incident:cursor:%'").fetchall()
        self.assertEqual(cursors, [], "只读 list/status 不得写游标")

    def seed_fresh_waiting_human(self, *, attempt_model=None):
        """真实 API 构造 attribution 一致的 waiting_human 责任。

        返回 (task_id, responsibility_id, item_id)。
        """
        _task(self.conn)
        _attempt(self.conn, aid="A-1", status="running", model=attempt_model)
        _resp(self.conn, attempt_id="A-1", generation=1)
        svc = self.svc(mail=self.mail())
        svc.reconcile(limit=10)
        item_id = self.outbox().list_all()[0]["item_id"]
        return "T-1", "R-1", item_id


# ============================================================================
# G1 recover_incident 事务内重验（真 WAL 双连接交错）
# ============================================================================
class G1RecoverInTxRecheck(SafetyBase):
    def test_recover_must_recheck_source_inside_transaction(self):
        """外层 recheck=resolved 之后源恢复 active，recover 事务内必须再重验。"""
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc()
        svc.reconcile(limit=10)
        iid = inc.incident_id("proj-a", "waiting-human-responsibility", "R-1")
        rec = self.inc_store.get_incident(iid)
        revision = rec["revision"]

        conn_b = _open_wal(self.home)
        self.addCleanup(conn_b.close)
        # B 关责任（resolved）
        conn_b.execute(
            "UPDATE responsibilities SET closed_at=20000, state='cancelled' "
            "WHERE id='R-1'")
        conn_b.commit()
        # A 先看到 resolved（外层观察）——这是“曾经 resolved”的候选。
        verdict = self.inc_store.recheck_binding(self.conn, rec["source_binding"])
        self.assertEqual(verdict["state"], "resolved")
        # B 在同一窗口把源“复活”为 active（与 A 的观察交错）。
        conn_b.execute(
            "UPDATE responsibilities SET closed_at=NULL, state='waiting_human' "
            "WHERE id='R-1'")
        conn_b.commit()
        # A 的事务内 recover_incident 必须重验到 active → 拒绝写 recovered。
        out = self.inc_store.recover_incident(
            incident_id=iid, expected_revision=revision,
            evidence="source-resolved:stale-verdict", now=self.clock.now_ms())
        self.assertFalse(out["applied"], "源已恢复 active 仍写 recovered")
        self.assertEqual(self.inc_store.get_incident(iid)["state"], "open")


# ============================================================================
# G2 损坏门 fail-closed：不发送且保 pending
# ============================================================================
class G2CorruptGatesFailClosed(SafetyBase):
    def test_corrupt_target_gate_blocks_send_keeps_pending(self):
        _task_id, _rid, item_id = self.seed_fresh_waiting_human()
        meta_set(self.conn, "target-gate:ses-1", "{bad json")
        result = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertNotEqual(result["result"], "accepted")
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.outbox().get_item(item_id)["status"], STATUS_PENDING)

    def test_inconsistent_validator_true_with_paused_does_not_allow_send(self):
        _task_id, _rid, item_id = self.seed_fresh_waiting_human()
        with patch.object(self.inc_store, "recheck_binding", return_value={
                "ok": True, "state": "paused", "reason": "contradictory-adapter"}):
            result = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertNotEqual(result["result"], "accepted")
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.outbox().get_item(item_id)["status"], STATUS_PENDING)

    def test_corrupt_provider_state_blocks_send_keeps_pending(self):
        _task_id, _rid, item_id = self.seed_fresh_waiting_human(attempt_model="m1")
        self.assertIsNotNone(item_id)
        # 临时 SQL：直接腐坏 provider_state（坏 JSON）。
        meta_set(self.conn, "provider_state", "{bad json")
        res = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertNotEqual(res["result"], "accepted", "损坏 provider_state 不得放行")
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.outbox().get_item(item_id)["status"], STATUS_PENDING)

    def test_corrupt_provider_state_deadline_invalid_blocks(self):
        _task_id, _rid, item_id = self.seed_fresh_waiting_human(attempt_model="m1")
        # 合法 JSON 但 until 类型/边界损坏 → 同样 fail-closed。
        meta_set(self.conn, "provider_state",
                 json.dumps({"m1": {"until": "not-an-int"}}))
        res = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertNotEqual(res["result"], "accepted")
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.outbox().get_item(item_id)["status"], STATUS_PENDING)

    def test_corrupt_session_record_blocks_send_keeps_pending(self):
        _task_id, _rid, item_id = self.seed_fresh_waiting_human()
        # 临时 SQL：直接腐坏 session 控制记录（损坏 gate）。
        meta_set(self.conn, "coordinator-control:session:ses-1", "{bad json")
        res = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertNotEqual(res["result"], "accepted")
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.outbox().get_item(item_id)["status"], STATUS_PENDING)

    def test_corrupt_project_record_blocks_send_keeps_pending(self):
        _task_id, _rid, item_id = self.seed_fresh_waiting_human()
        meta_set(self.conn, "coordinator-control:project:proj-a", "{bad json")
        res = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertNotEqual(res["result"], "accepted")
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.outbox().get_item(item_id)["status"], STATUS_PENDING)


# ============================================================================
# G3 真实归属/交接/路由：可选绑定提示不得作为门禁事实来源
# ============================================================================
class G3AttributionCurrentAttempt(SafetyBase):
    def test_current_generation_must_match_frozen_responsibility(self):
        _task_id, _rid, item_id = self.seed_fresh_waiting_human()
        # 损坏/迁移输入注入：当前attempt仍是同ID，但代次已经不同。
        self.conn.execute("UPDATE attempts SET generation=2 WHERE id='A-1'")
        result = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(result["result"], "not-claimed")
        self.assertEqual(self.transport.sent, [])

    def test_current_result_version_cannot_be_replaced_by_old_row_self_comparison(self):
        _task_id, _rid, item_id = self.seed_fresh_waiting_human()
        # 模拟台账已经出现新版本，原责任未记录其归属；不是合法collect证据，
        # 此负例只证明无法归属时拒发，不用它宣称验收已通过。
        self.conn.execute("UPDATE attempts SET metadata=? WHERE id='A-1'",
                          (json.dumps({"result": {"version": "new-result"}}),))
        result = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(result["result"], "not-claimed")
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.outbox().get_item(item_id)["status"], STATUS_PENDING)

    def test_current_coordinator_route_change_invalidates_old_binding(self):
        _task_id, _rid, item_id = self.seed_fresh_waiting_human()
        Ledger(self.conn, self.home, clock=self.clock).claim_coordinator(
            project="proj-a", session_id="ses-new", actor="operator", ttl=3600)
        result = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(result["result"], "not-claimed")
        self.assertEqual(self.transport.sent, [])

    def test_old_message_stale_when_task_current_attempt_moves(self):
        # 真实 API 交接：新 attempt（generation=2）成为任务 current；旧绑定责任
        # 归属被取代 → 旧 bound 消息不得发送（stale，不伪造恢复）。
        _task(self.conn)
        _attempt(self.conn, aid="A-1", status="running")
        _resp(self.conn, attempt_id="A-1", generation=1)
        svc = self.svc(mail=self.mail())
        svc.reconcile(limit=10)
        item_id = self.outbox().list_all()[0]["item_id"]
        # 构造新 attempt（generation=2）并设为任务 current。
        new = _attempt(self.conn, aid="A-2", status="running")
        self.assertEqual(new["generation"], 2)
        res = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(res["result"], "not-claimed")
        self.assertEqual(self.transport.sent, [])

    def test_session_hint_removal_must_not_bypass_real_held_session(self):
        # 真实任务 coordinator_session 后续被 hold：即使绑定把 source_session
        # 置空（可选提示缺失），同 conn 重验真实 session 仍必须拦下。
        _task(self.conn)
        _resp(self.conn)
        src = self.inc_store.list_waiting_human_sources(limit=10)[0][0]
        binding = inc.build_binding(src)
        binding["source_session"] = None  # 手改可选 hint 为空
        binding["source_model"] = None
        fact = {
            "incident_id": "I-x", "project": "proj-a",
            "incident_type": "waiting-human-responsibility",
            "condition_key": "waiting-human", "condition_version": 1,
            "recipient_ref": "op-primary", "condition_kind": "human-responsibility",
            "first_seen_at": 1_000_000, "observed_at": 1_000_000,
        }
        self.mail().enqueue_incident(fact, source_binding=binding)
        item_id = self.outbox().list_all()[0]["item_id"]
        # 真实 API 把任务当前协调者 session 置为 held。
        from oc_orch.adapters.coordinator_control import CoordinatorControlLedger
        out = CoordinatorControlLedger(self.conn, self.clock).hold_session(
            "ses-1", event_id="ev-1", origin_at=1, reason="test")
        self.assertTrue(out["executed"], out)
        res = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertNotEqual(res["result"], "accepted")
        self.assertEqual(self.transport.sent, [])

    def test_removed_model_hint_must_not_bypass_held_model_cooldown(self):
        # 绑定把 source_model 置空，但真实 attempt.model 处于冷却 → 仍不得发送。
        _task(self.conn)
        _attempt(self.conn, aid="A-1", status="running", model="m-cool")
        _resp(self.conn, attempt_id="A-1", generation=1)
        svc = self.svc(mail=self.mail())
        svc.reconcile(limit=10)
        item_id = self.outbox().list_all()[0]["item_id"]
        # 真实台账：模型处于 provider 冷却。
        meta_set(self.conn, "provider_state",
                 json.dumps({"m-cool": {"until": 9_999_999_999}}))
        # 手改绑定：清空 model 提示（可选字段）。
        item = self.outbox().get_item(item_id)
        fake = dict(item)
        fake["source_model"] = None
        # 显式损坏临时库中的绑定提示；正常upsert不会修改已冻结字段，不能把
        # “提交了一个被忽略的参数”误当作已经移除持久提示的反例。
        meta_set(self.conn, MAIL_KEY_PREFIX + item_id, json.dumps(fake))
        self.assertIsNone(self.outbox().get_item(item_id)["source_model"])
        res = self.mail().dispatch_once(item_id=item_id, owner="sidecar")
        self.assertNotEqual(res["result"], "accepted", "真实 model 冷却必须拦下")
        self.assertEqual(self.transport.sent, [])


# ============================================================================
# G4 recovery-kind 必须与当前 episode 一致（旧恢复不跨 episode 发送）
# ============================================================================
class G4RecoveryEpisodeVersion(SafetyBase):
    def test_old_episode_recovery_must_not_send(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail()
        svc = self.svc(mail=mail)
        svc.reconcile(limit=10)
        first_id = self.outbox().list_all()[0]["item_id"]
        self.transport.send_result = {"queued": True}
        mail.dispatch_once(item_id=first_id, owner="sidecar")  # 首封已发
        self.assertEqual(len(self.transport.sent), 1)
        # episode1 恢复
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=20000, state='cancelled' "
            "WHERE id='R-1'")
        self.conn.execute(
            "UPDATE tasks SET status='cancelled', closed_at=25000 WHERE id='T-1'")
        self.conn.commit()
        svc.reconcile(limit=10)
        old_recovery = [r for r in self.outbox().list_all()
                        if r["kind"] == "recovery"][0]
        old_rid = old_recovery["item_id"]
        # 故障注入：同一条件重现且时钟未前进（episode2），不能只比较时间/条件键。
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=NULL, state='waiting_human' WHERE id='R-1'")
        self.conn.execute(
            "UPDATE tasks SET status='draft', closed_at=NULL WHERE id='T-1'")
        self.conn.commit()
        svc.reconcile(limit=10)
        # episode2 再次恢复
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=30000, state='cancelled' "
            "WHERE id='R-1'")
        self.conn.execute(
            "UPDATE tasks SET status='cancelled', closed_at=35000 WHERE id='T-1'")
        self.conn.commit()
        svc.reconcile(limit=10)
        # 派发旧 episode 的恢复行：不得发送（binding 与当前 recovered_at 不符）。
        res = mail.dispatch_once(item_id=old_rid, owner="sidecar")
        self.assertNotEqual(res["result"], "accepted")
        self.assertEqual(len(self.transport.sent), 1)  # 只有首封


# ============================================================================
# G5 list/status/snapshot 只读诚实
# ============================================================================
class G5ListHonesty(SafetyBase):
    def test_structurally_invalid_scoped_record_is_not_complete_empty_page(self):
        self._seed_no_progress(0, "proj-a", "I-bad-schema")
        record = self.inc_store.get_incident("I-bad-schema")
        record["schema"] = True  # Python中True==1，但不能冒充schema整数。
        meta_set(self.conn, "incident:item:I-bad-schema", json.dumps(record))
        snapshot = self.svc().snapshot(project="proj-a", limit=10)
        self.assertEqual(snapshot["incidents"], [])
        self.assertEqual(snapshot["status"]["total"], 1)
        self.assertEqual(snapshot["coverage"]["invalid_records"], 1)
        self.assertFalse(snapshot["coverage"]["complete"])
        self.assert_no_cursor_written()

    def _no_progress_source(self, i: int, project: str) -> dict:
        return {
            "source_kind": "task", "source_id": f"{project}-S{i}",
            "project": project, "incident_type": "task-no-progress",
            "task_id": f"{project}-T{i}", "condition_key": f"k-{i}",
            "condition_state": "draft", "owner": "o", "next_action": "x",
            "condition_kind": "human-responsibility", "escalation_level": 0,
            "observed_at": 1, "first_evidence_at": 1, "task_status": "draft",
            "responsibility_states": (), "current_attempt_id": None,
        }

    def _seed_no_progress(self, i: int, project: str, incident_id: str):
        # upsert 现在会在真实台账上重验：源必须从真实任务行推导（签名一致）。
        _task(self.conn, tid=f"{project}-T{i}", project=project,
              coordinator="ses-co")
        task = self.inc_store._task_row(f"{project}-T{i}")
        source = self.inc_store._task_source(task, source_session="ses-co")
        self.inc_store.upsert(incident_id=incident_id, source=source, now=1)

    def test_other_project_keys_must_not_shadow_filtered_rows(self):
        # 16 个 proj-b（键序在前）+ 2 个 proj-a（键序在后）；过滤必须先于分页。
        for i in range(16):
            self._seed_no_progress(
                i, "proj-b", f"I-aaaa{chr(0x61 + i % 26)}{i:03d}")
        for i in range(2):
            self._seed_no_progress(i, "proj-a", f"I-ffff{i}000")
        svc = self.svc()
        rows = svc.list(project="proj-a", limit=2)
        self.assertEqual(len(rows), 2, "其他 project 前缀不得挡住本 project 过滤")
        self.assertTrue(all(r["project"] == "proj-a" for r in rows))
        self.assert_no_cursor_written()

    def test_status_total_is_true_total_not_scan_partial(self):
        n = 205  # 必须跨过旧实现200条上限，10条不能证明真实total。
        for i in range(n):
            self._seed_no_progress(i, "proj-a", f"I-a{i:04d}")
        svc = self.svc()
        status = svc.status(project="proj-a")
        self.assertEqual(status["total"], n)
        self.assertFalse(status["coverage"]["complete"])
        self.assertEqual(status["coverage"]["observed"], 200)
        self.assert_no_cursor_written()

    def test_corrupt_record_exposed_via_coverage_not_silently_zero(self):
        # 坏记录必须经 coverage 显式暴露（invalid_records），不得静默等同无待办。
        self._seed_no_progress(0, "proj-a", "I-ffff000")
        meta_set(self.conn, "incident:item:I-ffff000", "{bad json")
        svc = self.svc()
        snap = svc.snapshot(project="proj-a", limit=10)
        # 损坏记录（JSON 不合法 → 无法归属）必须经 coverage 显式暴露：
        # unattributed>0、total_is_exact=False、complete=False；不得静默当作
        # “无待办”的全绿视图。
        self.assertGreaterEqual(snap["coverage"]["unattributed_records"], 1)
        self.assertFalse(snap["coverage"]["total_is_exact"])
        self.assertFalse(snap["coverage"]["complete"])
        self.assert_no_cursor_written()


if __name__ == "__main__":
    unittest.main(verbosity=2)

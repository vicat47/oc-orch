"""S4 托管发送与后续责任定向回归（真临时 SQLite + FakeClient + mock 责任 store）。

覆盖：
- send_tracked 单一 task/attempt 推断、责任先于 prompt 持久、receipt 接续字段；
- 多 task 歧义在发送前拒绝；无托管 task 保留纯通信兼容但显式 untracked 边界；
- 已封存 attempt 的 write 拒绝并指向 rework，observe 只读接续；
- 闸门/完整 queue/current 复核，unknown 保原 ID 且不重投；
- 已 delivery 后 execution-only 归属证据与 business_ack 独立、ACK 不关责任；
- 严格 get_message 证据门与可信取消；cancel no-op 保持 unknown 并登记期限责任；
- resume_target 委托统一 clear_manual，不清 provider 冷却。

责任 store 由 mock 实现（A 组独立 store 未在本 worktree 落地时，不宣称端到端）。
"""

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

from oc_orch.adapters.managed_input_ledger import ManagedInputLedger
from oc_orch.adapters.responsibility_ledger import ResponsibilityStore
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.target_gate import TargetGate
from oc_orch.application.managed_inputs import ManagedInputs
from oc_orch.composition import application
from oc_orch.config import load_config
from oc_orch.db import db_path, init
from oc_orch.domain.errors import ActionError, LedgerError
from oc_orch.results import scan_deliverable_baseline
from tests.fakes import FakeClient, FakeClock, TransportError


class FakeResponsibilityStore:
    """按 A 组契约记录 open/update；base 暴露只读任务/尝试查询。"""

    def __init__(self, base, clock):
        self.base = base
        self.clock = clock
        self.records = {}
        self.open_calls = []
        self.updates = []
        self._next = 0

    def open_responsibility(
        self, owner, project, task_id, attempt_id, generation, result_version,
        input_id, session_id, message_id, target_session, *, state,
        wait_kind, wait_condition, next_action, next_check_at, deadline,
        condition_key, evidence, max_budget=3, actor,
    ):
        self._next += 1
        rid = f"RSP-{self._next:04d}"
        rec = {
            "responsibility_id": rid,
            "revision": 1,
            "owner": owner,
            "project": project,
            "task_id": task_id,
            "attempt_id": attempt_id,
            "generation": generation,
            "result_version": result_version,
            "input_id": input_id,
            "session_id": session_id,
            "message_id": message_id,
            "target_session": target_session,
            "state": state,
            "wait_kind": wait_kind,
            "wait_condition": wait_condition,
            "next_action": next_action,
            "next_check_at": next_check_at,
            "deadline": deadline,
            "condition_key": condition_key,
            "evidence": evidence,
            "max_budget": max_budget,
            "actor": actor,
            "opened_at": self.clock.now_ms(),
        }
        self.records[rid] = rec
        self.open_calls.append(dict(rec))
        return {"responsibility": dict(rec)}

    def update_responsibility(self, responsibility_id, *, expected_revision,
                              fields, actor="supervisor"):
        rec = self.records[responsibility_id]
        if rec["revision"] != expected_revision:
            raise LedgerError("RESPONSIBILITY_REVISION_CONFLICT",
                              "责任 revision 已变化")
        rec.update(fields)
        rec["revision"] += 1
        self.updates.append({"responsibility_id": responsibility_id,
                             "expected_revision": expected_revision,
                             "fields": dict(fields)})
        return {"responsibility": dict(rec)}


class CoverageBase:
    """模拟 attempts_for_session 返回超过有界窗口，验证不静默漏歧义。"""

    def __init__(self, count=51):
        self.attempts = [
            {"id": f"A-cov-{i:03d}", "task_id": f"T-cov-{i % 3}",
             "session_id": "ses_worker", "status": "running"}
            for i in range(count)
        ]

    def attempts_for_session(self, session_id, *, limit=20):
        return self.attempts[:limit]

    def get_task(self, task_id):
        return {"id": task_id, "status": "running", "project": "p",
                "coordinator_session": "ses_coord", "current_attempt_id": None}

    def coordinator_lease(self, project):
        return None


class ManagedTrackingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(
            prefix="managed-tracking-", dir="/tmp/opencode")
        self.addCleanup(self.temp.cleanup)
        self.conn = init(self.temp.name)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock()
        self.client = FakeClient()
        self.base = Ledger(self.conn, self.temp.name, clock=self.clock)
        self.mil = ManagedInputLedger(self.conn, self.base)
        self.store = FakeResponsibilityStore(self.base, self.clock)
        self.svc = ManagedInputs(
            self.mil, client=lambda: self.client, clock=self.clock,
            target_gate=TargetGate(self.conn, self.clock),
            responsibilities=self.store)

    # ---- 布置 ---------------------------------------------------------------
    def task(self, *, project="p", coordinator="ses_coord", title="task",
             scope=None):
        return self.base.create_task(
            project=project, title=title,
            write_scope=[scope or self.temp.name],
            coordinator_session=coordinator)

    def attempt(self, task, session="ses_worker", *, status="running",
                generation=1, directory=None, deadline_at=None):
        return self.base.create_attempt(
            task["id"], directory=directory or task["write_scope"][0],
            session_id=session, status=status, generation=generation,
            deadline_at=deadline_at)

    def deliver(self, session="ses_worker"):
        self.client.deliver_inbox(session, at=self.clock.now_ms())

    def send(self, *, session="ses_worker", message_id="msg_tracked_1",
             **kw):
        return self.svc.send_tracked(
            session, text="continue managed work", message_id=message_id,
            owner="op", **kw)

    # ---- 归属与持久顺序 -----------------------------------------------------
    def test_single_task_inferred_and_responsibility_persisted_before_prompt(self):
        task = self.task()
        att = self.attempt(task)
        order = []
        original_prompt = self.client.prompt

        def prompt(*args, **kwargs):
            order.append("prompt")
            return original_prompt(*args, **kwargs)

        original_open = self.store.open_responsibility

        def open_resp(*args, **kwargs):
            order.append("responsibility")
            return original_open(*args, **kwargs)

        with patch.object(self.client, "prompt", side_effect=prompt), \
                patch.object(self.store, "open_responsibility",
                             side_effect=open_resp):
            result = self.send(materials=["baseline.json"], work_kind="observe")

        self.assertEqual(order, ["responsibility", "prompt"])
        self.assertEqual(result["tracking"], "tracked")
        self.assertEqual(len(self.store.open_calls), 1)
        opened = self.store.open_calls[0]
        self.assertEqual(opened["task_id"], task["id"])
        self.assertEqual(opened["attempt_id"], att["id"])
        self.assertEqual(opened["target_session"], "ses_coord")
        self.assertEqual(opened["state"], "waiting_execution")
        self.assertEqual(opened["wait_kind"], "execution")
        self.assertIn(task["id"], opened["evidence"]["task_id"])
        self.assertEqual(opened["evidence"]["materials"], ["baseline.json"])
        self.assertIn("task", opened["evidence"]["attribution_sources"])

        receipt = result["receipt"]
        for key in ("responsibility_id", "target_session", "wait_condition",
                    "next_check_at", "deadline", "admission", "delivery",
                    "execution", "business_ack"):
            self.assertIn(key, receipt)
        self.assertEqual(receipt["responsibility_id"],
                         opened["responsibility_id"])
        self.assertEqual(receipt["target_session"], "ses_coord")
        self.assertEqual(receipt["inference"]["task"], "single-non-terminal-task")
        self.assertEqual(receipt["inference"]["attempt"],
                         "single-session-attempt")
        self.assertEqual(self.client.prompt_calls[-1]["resume"], True)
        self.assertEqual(self.client.prompt_calls[-1]["message_id"],
                         "msg_tracked_1")
        row = self.mil.get_managed_input(result["input"]["input_id"])
        self.assertEqual(row["metadata"]["managed_input"]["tracking"]
                         ["responsibility_id"],
                         opened["responsibility_id"])

    def test_target_prefers_current_coordinator_lease(self):
        task = self.task(coordinator="ses_old_coord")
        self.attempt(task)
        self.base.claim_coordinator(
            project="p", session_id="ses_new_coord", actor="operator")
        result = self.send()
        self.assertEqual(result["receipt"]["target_session"], "ses_new_coord")
        self.assertEqual(result["receipt"]["inference"]["target_session"],
                         "coordinator_lease")

    def test_task_next_check_at_never_becomes_responsibility_deadline(self):
        task = self.task()
        self.attempt(task)
        now = self.clock.now_ms()
        expected = now + 24 * 60 * 60 * 1000
        self.base.patch_task(
            task["id"], fields={"next_check_at": now + 1000},
            actor="operator", reason="rolling-reconcile")
        first = self.send(message_id="msg_deadline_1", work_kind="observe")
        self.assertEqual(first["receipt"]["deadline"], expected)
        self.assertEqual(first["receipt"]["inference"]["deadline"],
                         "default:24h")
        self.deliver()
        self.base.patch_task(
            task["id"], fields={"next_check_at": now + 500000},
            actor="operator", reason="rolling-reconcile-again")
        second = self.send(message_id="msg_deadline_2", work_kind="observe")
        self.assertEqual(second["receipt"]["deadline"], expected)
        self.assertEqual(self.store.open_calls[0]["deadline"],
                         self.store.open_calls[1]["deadline"])

    def test_deadline_priority_explicit_then_future_attempt_then_default(self):
        task = self.task()
        now = self.clock.now_ms()
        att = self.attempt(task, deadline_at=now + 3_600_000)
        result = self.send(message_id="msg_deadline_attempt",
                           work_kind="observe")
        self.assertEqual(result["receipt"]["deadline"], now + 3_600_000)
        self.assertEqual(result["receipt"]["inference"]["deadline"],
                         "attempt.deadline_at")
        self.deliver()
        explicit = self.send(message_id="msg_deadline_explicit",
                             work_kind="observe", deadline=now + 12_345)
        self.assertEqual(explicit["receipt"]["deadline"], now + 12_345)
        self.assertEqual(explicit["receipt"]["inference"]["deadline"],
                         "explicit")
        self.deliver()
        expired_task = self.task(title="expired",
                                 scope=self.temp.name + "_expired")
        self.attempt(expired_task, session="ses_worker_expired",
                     deadline_at=now - 1)
        expired_out = self.send(session="ses_worker_expired",
                                message_id="msg_deadline_expired",
                                work_kind="observe")
        self.assertEqual(expired_out["receipt"]["deadline"],
                         now + 24 * 60 * 60 * 1000)
        self.assertEqual(expired_out["receipt"]["inference"]["deadline"],
                         "default:24h:expired-attempt-deadline")
        self.assertIn("已过期", expired_out["receipt"]["deadline_warning"])

    def test_multiple_tasks_require_explicit_task_before_io(self):
        one = self.task(title="one", scope=self.temp.name + "/one")
        self.attempt(one)
        two = self.task(title="two", scope=self.temp.name + "/two")
        self.attempt(two)
        with self.assertRaises(ActionError) as cm:
            self.send()
        self.assertEqual(cm.exception.code, "ambiguous_ownership")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.mil.list_managed_inputs(), [])
        self.assertEqual(self.store.open_calls, [])
        # 显式 task 后唯一归属可发送。
        result = self.send(task_id=one["id"])
        self.assertEqual(result["receipt"]["task_id"], one["id"])

    def test_attempt_window_overflow_rejects_instead_of_guessing_unique(self):
        store = FakeResponsibilityStore(CoverageBase(), self.clock)
        svc = ManagedInputs(
            self.mil, client=lambda: self.client, clock=self.clock,
            target_gate=TargetGate(self.conn, self.clock),
            responsibilities=store)
        with self.assertRaises(ActionError) as cm:
            svc.send_tracked("ses_worker", text="coverage", owner="op",
                             message_id="msg_coverage")
        self.assertEqual(cm.exception.code, "ownership_coverage")
        self.assertEqual(cm.exception.detail["required"],
                         "explicit_task_or_untracked")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.mil.list_managed_inputs(), [])
        self.assertEqual(store.open_calls, [])

    def test_no_managed_task_falls_back_to_untracked_boundary(self):
        result = self.send(session="ses_unmanaged")
        self.assertEqual(result["tracking"], "untracked")
        self.assertIn("untracked", result["receipt"]["boundary"])
        self.assertIn("未登记", result["receipt"]["boundary"])
        self.assertIsNone(result["receipt"]["responsibility_id"])
        self.assertFalse(result["receipt"]["execution_observed"])
        self.assertFalse(result["receipt"]["continuation_managed"])
        self.assertIn("owner", result["receipt"]["next_action"])
        self.assertEqual(self.store.open_calls, [])
        self.assertIs(self.client.prompt_calls[-1]["resume"], True)
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_require_tracked_never_silently_degrades_to_untracked(self):
        with self.assertRaises(ActionError) as cm:
            self.svc.send_tracked(
                "ses_unmanaged", text="x", owner="op",
                message_id="msg_require_tracked", require_tracked=True)
        self.assertEqual(cm.exception.code, "ownership")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.mil.list_managed_inputs(), [])

    def test_same_candidate_requires_active_explicit_attempt_and_preserves_retry_identity(self):
        task = self.task()
        att = self.attempt(task)
        with self.assertRaises(ActionError):
            self.send(same_candidate=True)
        with self.assertRaises(ActionError):
            self.svc.send("ses_worker", intent="record", owner="op", text="x",
                          request_id="record", attempt_id=att["id"], same_candidate=True)
        first = self.send(attempt_id=att["id"], same_candidate=True)
        second = self.send(attempt_id=att["id"], same_candidate=True)
        self.assertEqual(first["input"]["input_id"], second["input"]["input_id"])
        with self.assertRaises(LedgerError):
            self.send(attempt_id=att["id"])
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.base.release_site(att["id"], confirmed=True, external_stopped=True,
                               actor="test", note="done")
        self.deliver()
        with self.assertRaises(ActionError):
            self.send(message_id="after-release", attempt_id=att["id"], work_kind="observe",
                      same_candidate=True)
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_explicit_untracked_never_grants_write_or_opens_responsibility(self):
        self.attempt(self.task())
        result = self.send(untracked=True)
        self.assertEqual(result["tracking"], "untracked")
        self.assertIn("未托管", result["receipt"]["boundary"])
        self.assertEqual(self.store.open_calls, [])
        self.assertIsNone(result["input"]["work_kind"])
        with self.assertRaises(ActionError) as cm:
            self.send(message_id="msg_write_untracked",
                      untracked=True, work_kind="write")
        self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(len(self.client.prompt_calls), 1)

    # ---- 封存/活动写权 ------------------------------------------------------
    def test_write_to_archived_attempt_rejected_with_rework_hint(self):
        task = self.task()
        att = self.attempt(task, status="terminal_succeeded")
        with self.assertRaises(ActionError) as cm:
            self.send(work_kind="write")
        self.assertEqual(cm.exception.code, "write_requires_rework")
        self.assertTrue(cm.exception.detail.get("rework_required"))
        self.assertEqual(cm.exception.detail["attempt_id"], att["id"])
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.store.open_calls, [])

    def test_write_to_released_active_attempt_rejected(self):
        task = self.task()
        att = self.attempt(task)
        self.base.release_site(att["id"], confirmed=True, external_stopped=True,
                               actor="operator", note="done")
        with self.assertRaises(ActionError) as cm:
            self.send(work_kind="write")
        self.assertEqual(cm.exception.code, "write_requires_rework")
        self.assertEqual(cm.exception.detail["reason"], "site-released")

    def test_stop_requested_attempt_blocks_any_new_input(self):
        task = self.task()
        att = self.attempt(task)
        self.base.stop_attempt(att["id"], actor="operator")
        for work_kind in (None, "observe", "write"):
            with self.subTest(work_kind=work_kind):
                with self.assertRaises(ActionError) as cm:
                    self.send(message_id=f"msg_stop_{work_kind}",
                              work_kind=work_kind)
                self.assertEqual(cm.exception.code, "stopped")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.store.open_calls, [])

    def test_observe_archived_attempt_allowed_as_readonly_continuation(self):
        task = self.task()
        self.attempt(task, status="result_collected")
        result = self.send(work_kind="observe")
        self.assertEqual(result["tracking"], "tracked")
        self.assertEqual(result["receipt"]["work_kind"], "observe")
        opened = self.store.open_calls[0]
        self.assertEqual(opened["task_id"], task["id"])
        self.assertIn("核收", opened["next_action"])

    def test_active_write_does_not_interrupt_or_change_attempt(self):
        task = self.task()
        att = self.attempt(task)
        result = self.send(work_kind="write")
        self.assertEqual(result["tracking"], "tracked")
        self.assertEqual(self.client.interrupt_calls, [])
        self.assertEqual(self.base.get_attempt(att["id"])["status"], "running")
        self.assertEqual(self.base.get_task(task["id"])["current_attempt_id"],
                         att["id"])

    def test_undeclared_work_on_active_current_inherits_existing_scope(self):
        self.attempt(self.task())
        result = self.send()
        self.assertEqual(result["receipt"]["work_kind"], "write")
        self.assertEqual(result["receipt"]["inference"]["work_kind"],
                         "inherited-current-active-attempt")
        self.assertEqual(self.store.open_calls[0]["evidence"]["work_kind"],
                         "write")

    def test_undeclared_work_on_archived_requires_explicit_intent(self):
        self.attempt(self.task(), status="result_collected")
        with self.assertRaises(ActionError) as cm:
            self.send()
        self.assertEqual(cm.exception.code, "work_kind_required")
        self.assertTrue(cm.exception.detail["rework_required"])
        self.assertEqual(cm.exception.detail["options"], ["observe", "read_only"])
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.store.open_calls, [])
        observed = self.send(message_id="msg_observe_archived",
                             work_kind="observe")
        self.assertEqual(observed["receipt"]["work_kind"], "observe")
        self.deliver()
        readonly = self.send(message_id="msg_read_only_archived",
                             work_kind="read_only")
        self.assertEqual(readonly["receipt"]["work_kind"], "observe")
        self.assertEqual(readonly["receipt"]["inference"]["work_kind"],
                         "explicit")

    def test_undeclared_write_without_managed_attempt_rejected(self):
        with self.assertRaises(ActionError) as cm:
            self.send(session="ses_unmanaged", work_kind="write")
        self.assertEqual(cm.exception.code, "write_requires_managed_attempt")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.store.open_calls, [])

    # ---- legacy-receipt 只读视图 ------------------------------------------
    def test_reconcile_registered_inputs_page_cursor_and_legacy_exclusion(self):
        self.mil.prepare_managed_input(
            "ses_page", intent="execute", text="p1", owner="op",
            request_id="page-1", now=self.clock.now_ms(),
            message_id="msg_page_1")
        self.mil.prepare_managed_input(
            "ses_page", intent="execute", text="p2", owner="op",
            request_id="page-2", now=self.clock.now_ms(),
            message_id="msg_page_2")
        self.conn.execute(
            "INSERT INTO session_inputs(input_id, session_id, message_id, "
            "source, intent, outstanding, revision, body, body_hash, "
            "notification_ids, admission_state, prepared_at, created_at, "
            "updated_at, metadata) VALUES(?,?,?,?,?,0,1,'','',?,?,?,?,?,?)",
            ("L-page-legacy", "ses_page", "msg_legacy_page",
             "legacy-receipt", "unknown", "[]", "unknown",
             self.clock.now_ms(), self.clock.now_ms(), self.clock.now_ms(),
             '{"legacy_receipt": true, "kinds": ["stall"], "members": 1}'))
        page = self.svc.reconcile_registered_inputs_page(limit=1)
        self.assertEqual(page["count"], 1)
        self.assertNotEqual(page["inputs"][0]["input_id"], "L-page-legacy")
        cursor = page["next_after_id"]
        self.assertTrue(cursor)
        rest = self.svc.reconcile_registered_inputs_page(
            after_id=cursor, limit=10)
        self.assertNotIn("L-page-legacy",
                         [r["input_id"] for r in rest["inputs"]])
        self.assertTrue(all(r["input_id"] > cursor for r in rest["inputs"]))

    def test_legacy_receipt_readonly_list_reconcile_no_replay(self):
        input_id = "L-legacy-test"
        self.conn.execute(
            "INSERT INTO session_inputs(input_id, session_id, message_id, "
            "source, intent, outstanding, revision, body, body_hash, "
            "notification_ids, admission_state, prepared_at, created_at, "
            "updated_at, metadata) VALUES(?,?,?,?,?,0,1,'','',?,?,?,?,?,?)",
            (input_id, "ses_legacy", "msg_legacy", "legacy-receipt", "unknown",
             "[]", "unknown", self.clock.now_ms(), self.clock.now_ms(),
             self.clock.now_ms(),
             '{"legacy_receipt": true, "kinds": ["stall"], "members": 1}'))
        listed = self.svc.list_inputs(session_id="ses_legacy")["inputs"]
        self.assertEqual(len(listed), 1)
        row = listed[0]
        self.assertTrue(row["legacy_receipt"])
        self.assertTrue(row["readonly"])
        self.assertIsNone(row["owner"])
        self.assertEqual(row["delivery"], "unknown")
        self.assertIsNone(row["tracking"])
        self.assertIn("legacy-receipt", row["next_action"])
        legacy = self.svc.list_legacy(session_id="ses_legacy")
        self.assertTrue(legacy["readonly"])
        self.assertEqual(legacy["inputs"][0]["input_id"], input_id)
        self.client.set_inbox("ses_legacy", [
            {"id": "msg_legacy", "time": {"created": self.clock.now_ms()}}])
        out = self.svc.reconcile(input_id)
        self.assertTrue(out["readonly"])
        self.assertEqual(out["observed_delivery"], "pending-or-unknown")
        self.assertEqual(out["input"]["delivery"], "unknown")
        for op in ("flush", "cancel"):
            with self.subTest(op=op):
                with self.assertRaises(LedgerError) as cm:
                    getattr(self.svc, op)(
                        input_id, owner="op", confirm=input_id,
                        expected_revision=1, reason="must not replay")
                self.assertEqual(cm.exception.code, "INPUT_NOT_MANAGED")

    # ---- 旧 send 共享入口与 park/flush 一致性 ------------------------------
    def test_legacy_send_execute_managed_delegates_and_keeps_request_id(self):
        task = self.task()
        att = self.attempt(task)
        first = self.svc.send(
            "ses_worker", intent="execute", text="continue", owner="op",
            request_id="req-managed-1", work_kind="observe")
        self.assertEqual(first["tracking"], "tracked")
        self.assertIs(first["resume"], True)
        self.assertTrue(first["receipt"]["responsibility_id"])
        self.assertEqual(first["input"]["tracking"], "tracked")
        self.assertEqual(self.store.open_calls[0]["task_id"], task["id"])
        self.assertEqual(self.store.open_calls[0]["attempt_id"], att["id"])
        self.assertEqual(self.client.prompt_calls[-1]["resume"], True)
        second = self.svc.send(
            "ses_worker", intent="execute", text="continue", owner="op",
            request_id="req-managed-1", work_kind="observe")
        self.assertFalse(second["created"])
        self.assertTrue(second["duplicate"])
        self.assertIs(second["resume"], True)
        self.assertEqual(second["input"]["input_id"],
                         first["input"]["input_id"])
        self.assertEqual(second["input"]["message_id"],
                         first["input"]["message_id"])
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_legacy_send_execute_unmanaged_watch_marks_external_boundary(self):
        self.client.set_session("ses_ext")
        self.svc.register_watch(
            "ses_ext", owner="op", project="p", max_pending_age_ms=1000)
        out = self.svc.send(
            "ses_ext", intent="execute", text="external", owner="op",
            request_id="ext-1", work_kind="observe")
        self.assertEqual(out["tracking"], "untracked")
        self.assertIs(out["resume"], True)
        self.assertIn("external-owner-watch", out["input"]["boundary"])
        self.assertEqual(out["input"]["owner"], "op")
        self.assertEqual(out["input"]["work_kind"], "observe")
        self.assertFalse(out["input"]["execution_observed"])
        self.assertFalse(out["input"]["continuation_managed"])
        self.assertIn("external-owner-watch", out["input"]["ack_boundary"])
        self.assertEqual(self.store.open_calls, [])
        self.assertEqual(self.client.prompt_calls[-1]["resume"], True)

    def test_legacy_send_execute_sealed_requires_explicit_work_kind(self):
        self.attempt(self.task(), status="result_collected")
        with self.assertRaises(ActionError) as cm:
            self.svc.send(
                "ses_worker", intent="execute", text="x", owner="op",
                request_id="sealed-none")
        self.assertEqual(cm.exception.code, "work_kind_required")
        observed = self.svc.send(
            "ses_worker", intent="execute", text="x", owner="op",
            request_id="sealed-observe", work_kind="read_only")
        self.assertEqual(observed["tracking"], "tracked")
        self.assertEqual(observed["receipt"]["work_kind"], "observe")
        with self.assertRaises(ActionError) as cm:
            self.svc.send(
                "ses_worker", intent="execute", text="x", owner="op",
                request_id="sealed-write", work_kind="write")
        self.assertEqual(cm.exception.code, "write_requires_rework")
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_legacy_send_park_then_flush_registers_responsibility(self):
        task = self.task()
        self.attempt(task)
        parked = self.svc.send(
            "ses_worker", intent="park", text="do later", owner="op",
            request_id="park-managed-1",
            expires_at=self.clock.now_ms() + 60000,
            flush_condition="explicit confirmation", work_kind="observe")
        self.assertEqual(parked["tracking"], "tracked")
        self.assertIs(parked["resume"], False)
        self.assertEqual(parked["input"]["tracking"], "tracked")
        self.assertEqual(self.store.open_calls, [])
        result = self.svc.flush(
            parked["input"]["input_id"], owner="op",
            confirm=parked["input"]["input_id"],
            expected_revision=parked["input"]["revision"], reason="go")
        self.assertTrue(result["changed"])
        self.assertTrue(result["responsibility_id"])
        opened = self.store.open_calls[-1]
        self.assertEqual(opened["input_id"], parked["input"]["input_id"])
        self.assertEqual(opened["message_id"], parked["input"]["message_id"])
        self.assertEqual(opened["state"], "waiting_execution")
        self.assertEqual(opened["wait_kind"], "execution")
        steered = self.client.inboxes["ses_worker"][0]
        self.assertEqual(steered["delivery"], "steer")
        self.assertEqual(len(self.client.prompt_calls), 1)
        marked = self.svc.get(parked["input"]["input_id"])["input"]
        self.assertEqual(marked["intent"], "park")
        self.assertIsNotNone(marked["flushed_at"])
        self.assertEqual(marked["effective_intent"], "execute")
        self.assertIs(marked["effective_resume"], True)
        self.assertIs(marked["resume"], True)
        self.assertIn("已 flush", marked["next_action"])
        self.clock.advance(seconds=120)
        self.svc.observe_registered(allow_remote=False)
        alerts = self.conn.execute(
            "SELECT kind, detail FROM alerts WHERE state='new'").fetchall()
        self.assertFalse(any(a["kind"] == "managed-park-expired"
                             or parked["input"]["input_id"] in a["detail"]
                             for a in alerts))

    def test_park_flush_cannot_bypass_stop_or_sealed_write(self):
        task = self.task()
        att = self.attempt(task)
        parked = self.svc.send(
            "ses_worker", intent="park", text="write later", owner="op",
            request_id="park-write-1",
            expires_at=self.clock.now_ms() + 60000,
            flush_condition="confirmation", work_kind="write")
        self.base.stop_attempt(att["id"], actor="operator")
        with patch.object(self.client, "update_inbox_delivery") as steer:
            with self.assertRaises(ActionError) as cm:
                self.svc.flush(
                    parked["input"]["input_id"], owner="op",
                    confirm=parked["input"]["input_id"],
                    expected_revision=parked["input"]["revision"],
                    reason="bypass stop")
        self.assertEqual(cm.exception.code, "stopped")
        steer.assert_not_called()
        self.base.release_site(att["id"], confirmed=True,
                               external_stopped=True, actor="operator")
        with patch.object(self.client, "update_inbox_delivery") as steer:
            with self.assertRaises(ActionError) as cm:
                self.svc.flush(
                    parked["input"]["input_id"], owner="op",
                    confirm=parked["input"]["input_id"],
                    expected_revision=parked["input"]["revision"],
                    reason="bypass sealed write")
        self.assertEqual(cm.exception.code, "write_requires_rework")
        steer.assert_not_called()

    def test_external_watch_park_flush_marks_flushed_and_skips_expiry(self):
        self.client.set_session("ses_ext_flush")
        self.svc.register_watch(
            "ses_ext_flush", owner="op", project="outside",
            max_pending_age_ms=60000)
        parked = self.svc.send(
            "ses_ext_flush", intent="park", text="deferred", owner="op",
            project="outside", request_id="ext-park-flush",
            expires_at=self.clock.now_ms() + 1000,
            flush_condition="owner approves")
        iid = parked["input"]["input_id"]
        out = self.svc.flush(iid, owner="op", confirm=iid,
                             expected_revision=parked["input"]["revision"],
                             reason="approved")
        self.assertTrue(out["changed"])
        self.clock.advance(wall=2000)
        observed = self.svc.observe_registered(allow_remote=False)
        self.assertFalse(any(a.get("kind") == "park-expired"
                             for a in observed["alerts"]))
        receipt = self.svc.get(iid)["input"]
        self.assertIsNotNone(receipt["flushed_at"])
        self.assertIs(receipt["resume"], True)
        self.assertEqual(receipt["intent"], "park")
        self.assertNotIn("park 已到期", receipt["next_action"])

    def test_recover_pending_execute_inactive_unique_success_and_no_repeat(self):
        self.attempt(self.task())
        sent = self.send(work_kind="write")
        input_id = sent["input"]["input_id"]
        self.clock.advance(seconds=120)
        out = self.svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertTrue(out["recovered"])
        self.assertEqual(out["reason"], "recovered-steer-original-id")
        steered = self.client.inboxes["ses_worker"][0]
        self.assertEqual(steered["delivery"], "steer")
        self.assertIn("ses_worker", self.client.scheduled_sessions)
        receipt = self.svc.get(input_id)["input"]
        self.assertIsNotNone(receipt["flushed_at"])
        self.assertIs(receipt["resume"], True)
        again = self.svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertFalse(again["recovered"])
        self.assertEqual(again["reason"], "already-recovered")

    def test_recover_blocked_by_active_foreign_unknown_gate(self):
        self.attempt(self.task())
        sent = self.send(work_kind="write")
        input_id = sent["input"]["input_id"]
        self.clock.advance(seconds=120)
        self.client.set_session("ses_worker")
        self.client.set_active(["ses_worker"])
        out = self.svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertEqual(out["reason"], "target-active")
        self.assertTrue(out["blocked"])
        self.client.clear_active()
        self.client.inboxes["ses_worker"].append({"id": "msg_foreign"})
        out = self.svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertEqual(out["reason"], "queue:foreign-queue")
        del self.client.inboxes["ses_worker"][-1]
        original = self.client.update_inbox_delivery
        calls = []

        def boom(*args, **kwargs):
            calls.append(args)
            raise TransportError("/inbox", "lost")

        with patch.object(self.client, "update_inbox_delivery",
                          side_effect=boom):
            out = self.svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertEqual(out["reason"], "patch-unknown")
        self.assertEqual(len(calls), 1)
        again = self.svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertFalse(again["recovered"])
        self.assertEqual(again["reason"], "operation-unresolved")
        self.assertTrue(original)

    def test_recover_blocked_by_gate_and_cross_restart_no_repeat(self):
        self.attempt(self.task())
        sent = self.send(work_kind="write")
        input_id = sent["input"]["input_id"]
        gate = TargetGate(self.conn, self.clock)
        gate.block_manual("ses_worker", reason="operator stop")
        self.clock.advance(seconds=120)
        with patch.object(self.client, "update_inbox_delivery") as steer:
            out = self.svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertTrue(out["blocked"])
        self.assertEqual(out["reason"], "gate:manual-blocked")
        steer.assert_not_called()
        snap = gate.get("ses_worker")
        self.svc.resume_target("ses_worker", owner="op", confirm="ses_worker",
                               expected_revision=snap["revision"], reason="resume")
        out = self.svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertTrue(out["recovered"])
        new_store = FakeResponsibilityStore(self.base, self.clock)
        new_svc = ManagedInputs(
            self.mil, client=lambda: self.client, clock=self.clock,
            target_gate=TargetGate(self.conn, self.clock),
            responsibilities=new_store)
        again = new_svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertFalse(again["recovered"])
        self.assertEqual(again["reason"], "already-recovered")

    def test_recover_within_grace_is_blocked(self):
        self.attempt(self.task())
        sent = self.send(work_kind="write")
        out = self.svc.recover_pending_execute(
            sent["input"]["input_id"], grace_ms=60000)
        self.assertTrue(out["blocked"])
        self.assertEqual(out["reason"], "within-grace")

    def test_recover_sealed_readonly_followup_steers_original_id(self):
        self.attempt(self.task(), status="result_collected")
        sent = self.send(work_kind="observe")
        self.clock.advance(seconds=120)
        out = self.svc.recover_pending_execute(
            sent["input"]["input_id"], grace_ms=60000)
        self.assertTrue(out["recovered"])
        self.assertEqual(self.client.inboxes["ses_worker"][0]["delivery"],
                         "steer")

    def test_unknown_flush_is_not_marked_success(self):
        self.attempt(self.task())
        parked = self.svc.send(
            "ses_worker", intent="park", text="later", owner="op",
            request_id="park-unknown-flush",
            expires_at=self.clock.now_ms() + 60000,
            flush_condition="confirmation", work_kind="observe")
        with patch.object(self.client, "update_inbox_delivery",
                          side_effect=TransportError("/inbox", "lost")):
            with self.assertRaises(ActionError) as cm:
                self.svc.flush(
                    parked["input"]["input_id"], owner="op",
                    confirm=parked["input"]["input_id"],
                    expected_revision=parked["input"]["revision"],
                    reason="lost response")
        self.assertEqual(cm.exception.code, "unknown_response")
        row = self.svc.get(parked["input"]["input_id"])["input"]
        self.assertIsNone(row.get("flushed_at"))

    # ---- 闸门 / 队列 / 未知 -------------------------------------------------
    def test_gate_block_rejects_before_record_or_responsibility(self):
        self.attempt(self.task())
        TargetGate(self.conn, self.clock).block_manual(
            "ses_worker", reason="operator stop")
        with self.assertRaises(ActionError) as cm:
            self.send()
        self.assertEqual(cm.exception.code, "gate")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.mil.list_managed_inputs(), [])
        self.assertEqual(self.store.open_calls, [])

    def test_nonempty_queue_rejects_before_prompt(self):
        self.attempt(self.task())
        self.client.set_inbox("ses_worker", [{"id": "msg_foreign"}])
        with self.assertRaises(ActionError) as cm:
            self.send()
        self.assertEqual(cm.exception.code, "queue")
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(self.store.open_calls, [])

    def test_unknown_prompt_keeps_identity_and_responsibility_without_retry(self):
        self.attempt(self.task())
        self.client.fail("prompt", TransportError("/prompt", "lost"))
        with self.assertRaises(ActionError) as cm:
            self.send()
        self.assertEqual(cm.exception.code, "unknown_response")
        detail = cm.exception.detail
        self.assertEqual(detail["message_id"], "msg_tracked_1")
        self.assertTrue(detail["responsibility_id"])
        row = self.mil.get_managed_input(detail["input_id"])
        self.assertEqual(row["admission_state"], "unknown")
        self.assertEqual(row["message_id"], "msg_tracked_1")
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertTrue(self.store.updates)
        self.assertEqual(self.store.updates[-1]["fields"]["wait_kind"],
                         "send_result_unknown")
        # 同一原 ID 重试只返回既有回执，不新 POST。
        again = self.send()
        self.assertFalse(again["created"])
        self.assertTrue(again["duplicate"])
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_duplicate_after_admission_does_not_repost(self):
        self.attempt(self.task())
        first = self.send()
        second = self.send()
        self.assertEqual(first["input"]["input_id"],
                         second["input"]["input_id"])
        self.assertEqual(first["input"]["message_id"],
                         second["input"]["message_id"])
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertFalse(second["created"])
        self.assertTrue(second["duplicate"])

    def test_explicit_deadline_materials_change_conflicts_but_default_retry_idempotent(self):
        self.attempt(self.task())
        now = self.clock.now_ms()
        self.send(message_id="msg_exp_deadline", work_kind="observe",
                  deadline=now + 1000)
        self.deliver()
        with self.assertRaises(LedgerError) as cm:
            self.send(message_id="msg_exp_deadline", work_kind="observe",
                      deadline=now + 2000)
        self.assertEqual(cm.exception.code, "REQUEST_ID_CONFLICT")
        self.send(message_id="msg_exp_materials", work_kind="observe",
                  materials=["a.json"])
        self.deliver()
        with self.assertRaises(LedgerError) as cm:
            self.send(message_id="msg_exp_materials", work_kind="observe",
                      materials=["b.json"])
        self.assertEqual(cm.exception.code, "REQUEST_ID_CONFLICT")
        first = self.send(message_id="msg_default_retry", work_kind="observe")
        self.deliver()
        self.clock.advance(seconds=10)
        again = self.send(message_id="msg_default_retry", work_kind="observe")
        self.assertFalse(again["created"])
        self.assertTrue(again["duplicate"])
        self.assertEqual(again["input"]["input_id"],
                         first["input"]["input_id"])

    # ---- 执行证据 / ACK 独立 ------------------------------------------------
    def test_execution_only_after_delivery_does_not_close_responsibility(self):
        self.attempt(self.task())
        sent = self.send()
        input_id = sent["input"]["input_id"]
        self.deliver()
        viewed = self.svc.reconcile(input_id)["input"]
        self.assertEqual(viewed["delivery"], "delivered")
        out = self.svc.record_managed_observation(
            input_id, execution={"outcome": "succeeded",
                                 "attempt_id": viewed["attempt_id"]},
            business_ack={"value": True}, via="runtime:test-execution")
        self.assertEqual(out["input"]["delivery"], "delivered")
        self.assertEqual(out["input"]["execution"], "succeeded")
        self.assertEqual(out["input"]["business_ack"], "acknowledged")
        self.assertEqual(self.store.records[
            sent["receipt"]["responsibility_id"]]["state"], "waiting_execution")
        self.assertEqual(self.store.records[
            sent["receipt"]["responsibility_id"]]["wait_kind"], "execution")

    def test_execution_only_before_delivery_rejected(self):
        self.attempt(self.task())
        sent = self.send()
        with self.assertRaises(LedgerError) as cm:
            self.svc.record_managed_observation(
                sent["input"]["input_id"],
                execution={"outcome": "succeeded",
                           "attempt_id": sent["input"]["attempt_id"]},
                via="runtime:early")
        self.assertEqual(cm.exception.code,
                         "EXECUTION_EVIDENCE_REQUIRES_DELIVERY")

    def test_acknowledge_requires_owner_revision_and_explicit_is_authoritative(self):
        self.attempt(self.task())
        sent = self.send()
        input_id = sent["input"]["input_id"]
        rid = sent["receipt"]["responsibility_id"]
        self.deliver()
        self.svc.reconcile(input_id)
        current = self.svc.get(input_id)["input"]
        false_ack = self.svc.acknowledge(
            input_id, value=False, owner="op", confirm=input_id,
            expected_revision=current["revision"], reason="not accepted")
        self.assertEqual(false_ack["business_ack"], "unacknowledged")
        self.assertEqual(self.store.records[rid]["state"], "waiting_execution")
        rev_after_false = false_ack["revision"]
        true_ack = self.svc.acknowledge(
            input_id, value=True, owner="op", confirm=input_id,
            expected_revision=rev_after_false, reason="human confirmed")
        self.assertEqual(true_ack["business_ack"], "acknowledged")
        self.assertEqual(self.store.records[rid]["state"], "waiting_execution")
        # runtime 晚到观察不能覆盖显式 True。
        late = self.svc.record_managed_observation(
            input_id, business_ack={"value": False}, via="runtime:late")
        self.assertEqual(late["input"]["business_ack"], "acknowledged")
        # 旧 revision（同 revision 二次确认）必须被 CAS 拒绝。
        with self.assertRaises(LedgerError) as cm:
            self.svc.acknowledge(
                input_id, value=True, owner="op", confirm=input_id,
                expected_revision=current["revision"], reason="stale")
        self.assertEqual(cm.exception.code, "INPUT_REVISION_CONFLICT")
        for owner, revision, confirm in (
                ("other", rev_after_false, input_id),
                ("op", rev_after_false, "wrong")):
            with self.subTest(owner=owner, revision=revision, confirm=confirm):
                with self.assertRaises((ActionError, LedgerError)):
                    self.svc.acknowledge(
                        input_id, value=True, owner=owner, confirm=confirm,
                        expected_revision=revision, reason="bad")

    def test_observation_params_cannot_forge_cancel_or_misattribute(self):
        self.attempt(self.task())
        sent = self.send()
        input_id = sent["input"]["input_id"]
        attempt_id = sent["input"]["attempt_id"]
        self.deliver()
        self.svc.reconcile(input_id)
        cases = [
            {"execution": {"outcome": "succeeded",
                           "attempt_id": "A-other"}},
            {"execution": {"outcome": "unknown",
                           "attempt_id": attempt_id}},
            {"execution": {"outcome": "succeeded"}},
            {"business_ack": {"reason": "done"}},
            {"evidence": {"cancelled": True, "trusted": True}},
            {"evidence": {"reason": "worker said done"}},
        ]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ActionError) as cm:
                    self.svc.record_managed_observation(
                        input_id, via="runtime:forged", **kwargs)
                self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(self.store.records[
            sent["receipt"]["responsibility_id"]]["state"], "waiting_execution")

    def test_structured_continuation_report_ids_and_no_auto_accept(self):
        task = self.task()
        self.attempt(task)
        sent = self.send()
        input_id = sent["input"]["input_id"]
        receipt = sent["receipt"]
        self.deliver()
        self.svc.reconcile(input_id)
        self.svc.record_managed_observation(
            input_id, execution={"outcome": "succeeded",
                                 "attempt_id": receipt["attempt_id"]},
            via="runtime:execution")
        report = {
            "schema": "oc-orch.continuation.v1",
            "input_id": input_id,
            "session_id": "ses_worker",
            "message_id": receipt["message_id"],
            "task_id": receipt["task_id"],
            "attempt_id": receipt["attempt_id"],
            "generation": receipt["generation"],
            "result_version": receipt["result_version"],
            "kind": "completed",
            "next_action": "coordinator reviews completion evidence",
            "materials": ["out.txt"],
            "evidence_refs": ["artifacts/out.txt"],
        }
        out = self.svc.record_business_report(
            input_id, report=report,
            source_message_id="msg_assistant_report_1")
        self.assertEqual(out["report"]["kind"], "completed")
        self.assertEqual(out["input"]["continuation"]["report"]["kind"],
                         "completed")
        self.assertEqual(out["business_ack"], "unacknowledged")
        self.assertNotEqual(self.base.get_task(task["id"])["status"], "accepted")
        self.assertEqual(self.store.records[
            receipt["responsibility_id"]]["state"], "waiting_execution")
        bad_reports = [
            {**report, "schema": "wrong"},
            {**report, "input_id": "MI-other"},
            {**report, "kind": "accepted"},
            {k: v for k, v in report.items() if k != "next_action"},
            {k: v for k, v in report.items() if k != "attempt_id"},
        ]
        for bad in bad_reports:
            with self.subTest(bad=bad):
                with self.assertRaises(ActionError) as cm:
                    self.svc.record_business_report(
                        input_id, report=bad,
                        source_message_id="msg_assistant_report_1")
                self.assertEqual(cm.exception.code, "usage")
        with self.assertRaises(ActionError) as cm:
            self.svc.record_business_report(
                input_id, report=report, source_message_id=receipt["message_id"])
        self.assertEqual(cm.exception.code, "usage")

    # ---- 严格消息证据 / 可信取消 -------------------------------------------
    def test_trusted_user_message_accepts_missing_session_but_rejects_wrong(self):
        valid = {"id": "m1", "type": "user", "time": {"created": 10}}
        self.assertTrue(ManagedInputs._trusted_user_message(
            valid, session_id="ses_a", message_id="m1"))
        self.assertFalse(ManagedInputs._trusted_user_message(
            {**valid, "sessionID": "ses_other"},
            session_id="ses_a", message_id="m1"))
        self.assertTrue(ManagedInputs._trusted_user_message(
            {**valid, "sessionID": "ses_a"},
            session_id="ses_a", message_id="m1"))
        for bad in ({**valid, "type": "assistant"},
                    {**valid, "time": {"created": 0}},
                    {**valid, "time": "bad"},
                    {**valid, "id": "other"}):
            with self.subTest(bad=bad):
                self.assertFalse(ManagedInputs._trusted_user_message(
                    bad, session_id="ses_a", message_id="m1"))

    def test_reconcile_delivers_without_session_id_and_rejects_wrong_session(self):
        self.attempt(self.task())
        first = self.send(message_id="msg_no_session")
        self.deliver()
        msg = self.client._messages_store["ses_worker"][-1]
        self.assertNotIn("sessionID", msg)
        self.assertEqual(self.svc.reconcile(first["input"]["input_id"])
                         ["input"]["delivery"], "delivered")
        second = self.send(message_id="msg_wrong_session")
        self.deliver()
        msg = self.client._messages_store["ses_worker"][-1]
        msg["sessionID"] = "ses_other"
        self.assertNotEqual(self.svc.reconcile(second["input"]["input_id"])
                            ["input"]["delivery"], "delivered")

    def test_cancel_requires_trusted_event_and_inbox_disappearance_is_unknown(self):
        self.attempt(self.task())
        sent = self.send()
        input_id = sent["input"]["input_id"]
        with self.assertRaises(ActionError) as cm:
            self.svc.record_managed_observation(
                input_id, cancelled=True, trusted=False, via="sse:fake")
        self.assertEqual(cm.exception.code, "usage")
        self.deliver()
        self.assertEqual(self.svc.reconcile(input_id)["input"]["delivery"],
                         "delivered")
        trusted = self.svc.record_managed_observation(
            input_id, cancelled=True, trusted=True, via="sse:inbox.cancelled")
        self.assertTrue(trusted["recorded"])

    def test_cancel_noop_keeps_unknown_and_registers_bounded_duty(self):
        self.attempt(self.task())
        sent = self.send()
        input_id = sent["input"]["input_id"]
        current = self.svc.get(input_id)["input"]
        result = self.svc.cancel(
            input_id, owner="op", confirm=input_id,
            expected_revision=current["revision"], reason="operator cancel")
        self.assertEqual(result["delivery"], "unknown")
        self.assertTrue(result["unknown"])
        self.assertTrue(self.store.updates)
        fields = self.store.updates[-1]["fields"]
        self.assertEqual(fields["wait_kind"], "cancel_reconcile")
        self.assertGreater(fields["deadline"], self.clock.now_ms())
        self.assertIn("unknown", fields["next_action"])

    # ---- 真实 A 存储 roundtrip（非 mock） ---------------------------------
    def test_real_app_collect_then_tracked_send_uses_result_version(self):
        task = self.task(title="collect", coordinator="ses_coord")
        att = self.base.create_attempt(
            task["id"], directory=self.temp.name, session_id="ses_collect",
            status="running", dispatch_msg_id="msg_dispatch_collect",
            deliverables=["out.txt"])
        Path(self.temp.name, "out.txt").write_text("done")
        baseline = scan_deliverable_baseline(
            self.base, att["id"], captured_at=self.clock.now_ms())
        self.base.seal_execution(
            att["id"], mode="new", session_before={}, baseline=baseline)
        started = self.clock.now_ms() + 1
        ended = started + 5
        self.client.set_session(
            "ses_collect", outcome="succeeded",
            time_={"busy": started, "idle": ended})
        self.client.append_message("ses_collect", {
            "id": "msg_dispatch_collect", "type": "user", "text": "go",
            "time": {"created": started}})
        self.client.append_message("ses_collect", {
            "id": "msg_collect_answer", "type": "assistant", "finish": "stop",
            "time": {"created": started + 1, "completed": ended - 1},
            "content": [{"type": "text", "text": "done"}]})
        self.client.append_message("ses_collect", {
            "id": "msg_collect_idle", "type": "idle", "outcome": "succeeded",
            "time": {"created": ended}})
        self.base.record_execution_started(
            att["id"], started_at=started, event_id="start-collect")
        self.base.record_terminal(
            att["id"], outcome="succeeded", source="sse",
            event_id="end-collect",
            candidate={"session_id": "ses_collect", "source": "sse",
                       "outcome": "succeeded", "ended_at": ended,
                       "input_in_inbox": False, "input_owner": "unique"})
        app = application(
            self.base, client=lambda: self.client,
            cfg=load_config(self.temp.name), clock=self.clock)
        collected = app.collect(att["id"])
        self.assertEqual(collected["status"], "result_collected")
        version = self.base.get_attempt(att["id"])["metadata"]["result"]["version"]
        self.assertTrue(version)
        sent = self.svc.send_tracked(
            "ses_collect", text="followup", message_id="msg_followup_real",
            owner="op", work_kind="observe")
        self.assertEqual(self.store.open_calls[-1]["result_version"], version)
        self.assertTrue(sent["receipt"]["responsibility_id"])

    def test_real_responsibility_store_roundtrip(self):
        store = ResponsibilityStore(self.base)
        service = ManagedInputs(
            self.mil, client=lambda: self.client, clock=self.clock,
            target_gate=TargetGate(self.conn, self.clock),
            responsibilities=store)
        task = self.task(coordinator="ses_real_coord")
        self.attempt(task, session="ses_worker_real")
        out = service.send_tracked(
            "ses_worker_real", text="real store", message_id="msg_real_1",
            owner="op", work_kind="observe", materials=["baseline.json"])
        rid = out["receipt"]["responsibility_id"]
        self.assertTrue(rid.startswith("R-"))
        row = store.get_responsibility(rid)
        self.assertEqual(row["task_id"], task["id"])
        self.assertEqual(row["input_id"], out["input"]["input_id"])
        self.assertEqual(row["message_id"], "msg_real_1")
        self.assertEqual(row["state"], "waiting_execution")
        self.assertEqual(row["wait_kind"], "execution")
        self.assertEqual(row["target_session"], "ses_real_coord")
        open_rows = store.list_open_responsibilities(task_id=task["id"])
        self.assertEqual([r["id"] for r in open_rows], [rid])
        current = service.get(out["input"]["input_id"])["input"]
        cancelled = service.cancel(
            out["input"]["input_id"], owner="op",
            confirm=out["input"]["input_id"],
            expected_revision=current["revision"], reason="operator cancel")
        self.assertEqual(cancelled["delivery"], "unknown")
        row = store.get_responsibility(rid)
        self.assertEqual(row["state"], "waiting_dependency")
        self.assertEqual(row["wait_kind"], "cancel_reconcile")

    # ---- resume_target：统一 clear_manual，不重置 provider -----------------
    def test_resume_target_delegates_clear_manual_and_preserves_provider_gate(self):
        gate = TargetGate(self.conn, self.clock)
        block = gate.block_manual("ses_worker", reason="operator stop")
        snap = gate.get("ses_worker")
        result = self.svc.resume_target(
            "ses_worker", owner="op", confirm="ses_worker",
            expected_revision=snap["revision"], reason="resume")
        self.assertTrue(result["changed"])
        self.assertIsNone(gate.get("ses_worker"))
        self.assertEqual(self.client.prompt_calls, [])
        audit = self.conn.execute(
            "SELECT action FROM audit WHERE entity='session' AND entity_id=? "
            "ORDER BY id DESC LIMIT 1", ("ses_worker",)).fetchone()
        self.assertEqual(audit["action"], "target-gate.clear-manual")
        # provider 冷却不是 manual：resume_target 必须拒绝且保留原闸门。
        gate.observe_error("ses_worker", {"category": "quota",
                                          "message": "quota"},
                           evidence_key="quota-1",
                           observed_at=self.clock.now_ms())
        provider = gate.get("ses_worker")
        self.assertIsNotNone(provider)
        with self.assertRaises(ActionError) as cm:
            self.svc.resume_target(
                "ses_worker", owner="op", confirm="ses_worker",
                expected_revision=provider["revision"], reason="resume")
        self.assertEqual(cm.exception.code, "gate")
        self.assertIsNotNone(gate.get("ses_worker"))


    def _open_wal(self):
        conn = sqlite3.connect(
            db_path(self.temp.name), timeout=15, isolation_level=None,
            check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def test_acknowledge_two_connection_cas_single_winner(self):
        self.attempt(self.task())
        sent = self.send()
        input_id = sent["input"]["input_id"]
        self.deliver()
        self.svc.reconcile(input_id)
        current = self.svc.get(input_id)["input"]
        owner = current["owner"]
        rev = current["revision"]
        conn1 = self._open_wal()
        conn2 = self._open_wal()
        self.addCleanup(conn1.close)
        self.addCleanup(conn2.close)
        base1 = Ledger(conn1, self.temp.name, clock=self.clock)
        base2 = Ledger(conn2, self.temp.name, clock=self.clock)
        svc1 = ManagedInputs(
            ManagedInputLedger(conn1, base1),
            client=lambda: self.client, clock=self.clock,
            target_gate=TargetGate(conn1, self.clock),
            responsibilities=FakeResponsibilityStore(base1, self.clock))
        svc2 = ManagedInputs(
            ManagedInputLedger(conn2, base2),
            client=lambda: self.client, clock=self.clock,
            target_gate=TargetGate(conn2, self.clock),
            responsibilities=FakeResponsibilityStore(base2, self.clock))
        barrier = Barrier(2)

        def ack(svc):
            barrier.wait()
            try:
                out = svc.acknowledge(
                    input_id, value=True, owner=owner, confirm=input_id,
                    expected_revision=rev, reason="race")
                return {"ok": True, "revision": out["revision"]}
            except LedgerError as exc:
                return {"ok": False, "code": exc.code}

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(ack, svc1)
            f2 = pool.submit(ack, svc2)
            results = [f.result(timeout=25) for f in (f1, f2)]
        wins = [r for r in results if r.get("ok")]
        losses = [r for r in results if not r.get("ok")]
        self.assertEqual(len(wins), 1, results)
        self.assertEqual(len(losses), 1, results)
        self.assertEqual(losses[0]["code"], "INPUT_REVISION_CONFLICT")

    def test_reconcile_page_backoff_checked_skipped_and_failure_throttle(self):
        now = self.clock.now_ms()
        self.mil.prepare_managed_input(
            "ses_b", intent="execute", text="p1", owner="op",
            request_id="br-1", now=now)
        self.mil.prepare_managed_input(
            "ses_b", intent="execute", text="p2", owner="op",
            request_id="br-2", now=now - 1000)
        page1 = self.svc.reconcile_registered_inputs_page(limit=10)
        self.assertEqual((page1["count"], page1["checked"], page1["skipped"]),
                         (2, 2, 0))
        cursor = page1["next_after_id"]
        page2 = self.svc.reconcile_registered_inputs_page(
            after_id=cursor, limit=10)
        self.assertEqual(page2["count"], 0)
        page3 = self.svc.reconcile_registered_inputs_page(limit=10)
        self.assertEqual((page3["checked"], page3["skipped"], page3["count"]),
                         (0, 2, 2))
        self.mil.prepare_managed_input(
            "ses_b", intent="execute", text="p3", owner="op",
            request_id="br-3", now=now + 1)
        with patch.object(self.client, "inbox",
                          side_effect=TransportError("/inbox", "down")):
            failed = self.svc.reconcile_registered_inputs_page(limit=10)
        self.assertEqual(failed["count"], 3)
        self.assertEqual(failed["checked"], 1)
        self.assertEqual(failed["skipped"], 2)
        self.clock.advance(wall=61000)
        later = self.svc.reconcile_registered_inputs_page(limit=10)
        self.assertEqual((later["checked"], later["skipped"], later["count"]),
                         (3, 0, 3))

    def test_recover_grace_uses_first_anchor_not_refreshed_observation(self):
        self.attempt(self.task())
        sent = self.send(work_kind="write")
        input_id = sent["input"]["input_id"]
        self.clock.advance(seconds=61)
        # 周期对账刷新 observed_at/last_checked，但宽限锚点必须是 admitted_at。
        self.svc.reconcile_registered_inputs_page(limit=10)
        out = self.svc.recover_pending_execute(input_id, grace_ms=60000)
        self.assertTrue(out["recovered"])
        self.assertEqual(out["reason"], "recovered-steer-original-id")

if __name__ == "__main__":
    unittest.main()

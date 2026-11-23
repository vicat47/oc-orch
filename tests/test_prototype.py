"""P17–P20原型权威记录：不假成功、不减验收，CAS/恢复与正式修复证据。"""
from copy import deepcopy
from pathlib import Path
import json
import tempfile
import threading
import unittest
from unittest.mock import patch

from oc_orch import composition
from oc_orch.adapters.prototype import PREFIX
from oc_orch.adapters.sqlite_db import backup_to
from oc_orch.domain import prototype
from oc_orch.domain.errors import LedgerError
from tests.fakes import FakeClock
from tests.test_unit import _FakeClient, _RESULT_CFG


def prototype_plan():
    return {"question": "验证菜单与补充指令代表路径，不承诺邮件后端", "entry": "#/task/T-example",
        "startup": "bin/oc-orch web --home <isolated-home>", "checks": ["web DOM与API契约"],
        "paths": [{"id": "path-main", "features": ["scope", "mail", "export"]}],
        "features": [{"id": "scope", "title": "真实范围读取", "state": "real", "source": "本地SQLite API", "gap_ids": []},
                     {"id": "mail", "title": "模拟通知", "state": "mock", "source": "固定模拟数据，无发信副作用", "gap_ids": ["gap-mail"]},
                     {"id": "export", "title": "导出菜单", "state": "placeholder", "source": "未实现，不下载/伪成功", "gap_ids": ["gap-export"]}],
        "gaps": [gap("gap-mail"), gap("gap-export", kind="defect")]}


def gap(key, kind="tradeoff"):
    return {"id": key, "kind": kind, "state": "open", "risk": "ordinary", "expected": "后端真实集成",
        "current": "仅展示", "reason": "本轮验证交互", "impact": "不能正式发送/导出", "owner": "operator",
        "next_action": "实现并注册联调检查", "review_at": "2026-11-17", "refs": ["docs/prd/runtime-delivery-policy.md"],
        "followup_task": None, "evidence": None}


class PrototypeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = composition.init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.ledger = composition.Ledger(self.conn, self.home, clock=self.clock)
        self.task = self.ledger.create_task(project="p", title="原型范围任务", objective="原型承诺", acceptance=["原型检查"])
        self.store = composition.make_prototype(self.ledger, self.clock)

    def body(self, **patches):
        return {"plan": prototype_plan(), "expected_revision": 0, "operation_id": "prototype-1",
                "reason": "explicit scope", "confirm": self.task["id"], **patches}

    def apply(self, body=None):
        return self.store.apply(self.task["id"], body or self.body(), actor="test")

    def test_p17_plan_visible_never_mutates_task_commitment_or_acceptance(self):
        before = self.ledger.get_task(self.task["id"])
        out = self.apply()
        self.assertEqual(out["unresolved"], ["gap-mail", "gap-export"])
        self.assertEqual(out["traceability"]["features_requiring_gaps"], 2)
        self.assertEqual(self.ledger.get_task(self.task["id"]), before)
        self.assertEqual(out["commitment"]["acceptance"], before["acceptance"])
        self.assertEqual(out["semantics"], "prototype-scope-not-formal-business-acceptance")

    def test_read_only_unregistered_is_zero_write_not_empty_verified(self):
        before = self.conn.total_changes
        out = self.store.show(self.task["id"])
        self.assertIsNone(out["current"])
        self.assertEqual(out["revision"], 0)
        self.assertEqual(self.conn.total_changes, before)

    def test_strict_cas_confirmation_fields_and_actor(self):
        for delta in ({"expected_revision": True}, {"confirm": " " + self.task["id"]}, {"trusted": True}, {"plan": {}}):
            before = self.conn.total_changes
            with self.subTest(delta=delta), self.assertRaises(LedgerError):
                self.apply(self.body(**delta))
            self.assertEqual(self.conn.total_changes, before)

    def test_p18_mock_placeholder_blocked_requires_valid_authoritative_gap(self):
        for change in ("missing-gap", "missing-owner", "missing-next", "unknown-state", "duplicate-feature"):
            plan = prototype_plan()
            if change == "missing-gap": plan["features"][1]["gap_ids"] = ["not-here"]
            elif change == "missing-owner": plan["gaps"][0]["owner"] = ""
            elif change == "missing-next": plan["gaps"][0]["next_action"] = ""
            elif change == "unknown-state": plan["features"][1]["state"] = "done"
            else: plan["features"].append(deepcopy(plan["features"][0]))
            with self.subTest(change=change), self.assertRaises(LedgerError):
                self.apply(self.body(plan=plan))

    def test_p19_no_drop_or_rename_defect_to_tradeoff(self):
        self.apply()
        for change in ("delete", "reclassify", "expected", "real-without-proof", "path"):
            plan = prototype_plan()
            if change == "delete": plan["gaps"].pop(); plan["features"].pop(); plan["paths"][0]["features"].pop()
            elif change == "reclassify": plan["gaps"][1]["kind"] = "tradeoff"
            elif change == "expected": plan["gaps"][0]["expected"] = "只要mock"
            elif change == "path": plan["paths"][0]["id"] = "replacement"
            else: plan["features"][1]["state"] = "real"
            with self.subTest(change=change), self.assertRaises(LedgerError):
                self.apply(self.body(plan=plan, operation_id="next", expected_revision=1))

    def test_p19_deferred_and_risk_accepted_keep_obligations_and_history(self):
        self.apply()
        plan = prototype_plan()
        plan["gaps"][0]["state"] = "deferred"
        plan["gaps"][1]["state"] = "risk-accepted"
        out = self.apply(self.body(plan=plan, operation_id="next", expected_revision=1))
        self.assertEqual(len(out["unresolved"]), 2)
        before = self.conn.total_changes
        replay = self.apply()
        self.assertFalse(replay["applied"])
        self.assertEqual((replay["applied_revision"], replay["revision"]), (1, 2))
        self.assertEqual(self.conn.total_changes, before)

    def test_p20_high_risk_is_not_exempt_by_stage_or_defer(self):
        for risk in ("security", "integrity", "permission"):
            for state in ("deferred", "risk-accepted"):
                plan = prototype_plan(); plan["gaps"][0].update(risk=risk, state=state)
                with self.subTest(risk=risk, state=state), self.assertRaises(LedgerError):
                    self.apply(self.body(plan=plan))

    def test_no_fixed_from_chat_ack_force_or_arbitrary_proof(self):
        for proof in (None, {"attempt_id": "not-an-attempt", "verified_at": 1}, {"attempt_id": "x", "verified_at": True}):
            plan = prototype_plan(); plan["gaps"][0].update(state="fixed", evidence=proof)
            with self.subTest(proof=proof), self.assertRaises(LedgerError):
                self.apply(self.body(plan=plan))

    def accepted(self, task_id=None, force=False):
        task_id = task_id or self.task["id"]
        att = self.ledger.create_attempt(task_id, directory=self.home, session_id="ses_proto",
            dispatch_msg_id="msg_proto", status="dispatched", dispatch_state="confirmed")
        from oc_orch.adapters.results import scan_deliverable_baseline, collect_attempt, verify_attempt
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
            baseline=scan_deliverable_baseline(self.ledger, att["id"], captured_at=self.clock.now_ms()))
        started = self.clock.now_ms()
        self.ledger.record_execution_started(att["id"], started_at=started, event_id="start")
        self.ledger.record_terminal(att["id"], outcome="succeeded", source="sse", event_id="end",
            candidate={"session_id": "ses_proto", "source": "sse", "outcome": "succeeded", "ended_at": started + 5,
                       "input_in_inbox": False, "input_owner": "unique"})
        target = _FakeClient([{"id": "msg_proto", "type": "user", "time": {"created": started}},
            {"id": "answer", "type": "assistant", "finish": "stop", "content": [{"type": "text", "text": "fixture scoped result"}],
             "time": {"created": started + 1, "completed": started + 4}},
            {"id": "idle", "type": "idle", "outcome": "succeeded", "time": {"created": started + 5}}])
        self.clock.advance(seconds=1)
        collect_attempt(self.ledger, target, att["id"], _RESULT_CFG, clock=self.clock.now_ms)
        verdict = verify_attempt(self.ledger, att["id"], result="pass", evidence=["fixture API tests"], force=force,
                                 reason="explicit force test" if force else None)
        self.assertTrue(verdict["ok"], verdict)
        return {"attempt_id": att["id"], "verified_at": verdict["verification"]["verified_at"]}

    def test_real_connection_requires_normal_effective_registered_verification(self):
        self.apply()
        proof = self.accepted()
        plan = prototype_plan(); plan["gaps"][0].update(state="fixed", evidence=proof)
        plan["features"][1]["state"] = "real"
        out = self.apply(self.body(plan=plan, operation_id="fixed", expected_revision=1))
        self.assertEqual(out["unresolved"], ["gap-export"])
        self.assertEqual(self.store.show(self.task["id"], "prototype-1")["applied_revision"], 1)

    def test_force_not_valid_gap_repair_and_cross_project_refused(self):
        proof = self.accepted(force=True)
        plan = prototype_plan(); plan["gaps"][0].update(state="fixed", evidence=proof)
        with self.assertRaises(LedgerError): self.apply(self.body(plan=plan))
        foreign = self.ledger.create_task(project="foreign", title="other")
        plan = prototype_plan(); plan["gaps"][0]["followup_task"] = foreign["id"]
        with self.assertRaises(LedgerError): self.apply(self.body(plan=plan))

    def test_audit_failure_atomic_rollback(self):
        before = "\n".join(self.conn.iterdump())
        with patch.object(self.ledger, "_audit", side_effect=RuntimeError("audit failed")), self.assertRaises(RuntimeError):
            self.apply()
        self.assertEqual("\n".join(self.conn.iterdump()), before)

    def test_corruption_fail_closed(self):
        self.apply()
        self.conn.execute("UPDATE meta SET value=? WHERE key=?", ('{"schema":1}', PREFIX + self.task["id"]))
        with self.assertRaises(LedgerError) as exc: self.store.show(self.task["id"])
        self.assertEqual(exc.exception.code, "PROTOTYPE_CORRUPT")

    def test_same_op_changed_body_refused_no_overwrite(self):
        self.apply()
        with self.assertRaises(LedgerError): self.apply(self.body(reason="changed"))
        self.assertEqual(self.store.show(self.task["id"])["revision"], 1)

    def test_backup_restart_preserves_original_intent_history_and_task(self):
        self.apply()
        backup = Path(self.home) / "backup.db"
        backup_to(self.conn, str(backup))
        import sqlite3
        restored = sqlite3.connect(str(backup), isolation_level=None)
        restored.row_factory = sqlite3.Row
        self.addCleanup(restored.close)
        service = composition.make_prototype(composition.Ledger(restored, self.home, clock=self.clock), self.clock)
        self.assertEqual(service.show(self.task["id"]), self.store.show(self.task["id"]))
        self.assertFalse(service.apply(self.task["id"], self.body(), actor="test")["applied"])

    def test_two_wal_writers_same_revision_exactly_one_wins(self):
        barrier, outcomes = threading.Barrier(2), []
        def run(name):
            conn = composition.connect(self.home)
            try:
                service = composition.make_prototype(composition.Ledger(conn, self.home, clock=self.clock), self.clock)
                barrier.wait(timeout=5)
                service.apply(self.task["id"], self.body(operation_id=name), actor="test")
                outcomes.append("applied")
            except LedgerError as exc: outcomes.append(exc.code)
            finally: conn.close()
        threads = [threading.Thread(target=run, args=(name,)) for name in ("window-1", "window-2")]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10); self.assertFalse(thread.is_alive())
        self.assertCountEqual(outcomes, ["applied", "PROTOTYPE_REVISION_CONFLICT"])

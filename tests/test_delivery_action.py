"""S2单次许可：真WAL、切换/撤权、未知原ID、全发送入口与业务控制反例。"""

import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from oc_orch.adapters.delivery_action import DeliveryActions, PREFIX, _key
from oc_orch.adapters.delivery_guard import install_delivery_guard
from oc_orch.adapters.delivery_policy import DeliveryPolicyStore
from oc_orch.adapters.resource_authorization import ResourceAuthorizationStore
from oc_orch.adapters.sqlite_db import connect, init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.target_gate import TargetGate
from oc_orch.adapters.wake_admission import WakeAdmission
from oc_orch.adapters.writer_guard import install_writer_guard
from oc_orch.composition import application, make_managed_inputs
from oc_orch.adapters.config import load_config
from oc_orch.domain.delivery_action import check_handoff, observed_model
from oc_orch.domain.errors import ActionError, LedgerError, TransportError
from oc_orch.runtime.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock
from tests.test_delivery_policy import policy


FREE, PAID = "fixture/free", "fixture/paid"


class DeliveryActionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="delivery-actions-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home, self.clock = Path(self.tmp.name), FakeClock()
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.client = FakeClient()
        self.cfg = load_config(self.home)
        self.policies = DeliveryPolicyStore(self.conn, self.clock)
        self.auth = ResourceAuthorizationStore(self.conn, self.clock)
        self.actions = DeliveryActions(self.ledger)
        install_writer_guard(self.conn)
        install_delivery_guard(self.conn)
        self.task = self.ledger.create_task(project="p", title="work", coordinator_session="ses_coord")
        self.policies.switch_task(self.task["id"], policy=policy(), expected_revision=0,
            operation_id="task-policy-1", actor="operator", reason="explicit")
        self.serial = 0

    def grant(self, scope="project", scope_id="p", *, models=(FREE, PAID), rev=0, op=None):
        resources = [{"model": model, "cost": "paid" if model == PAID else "no-extra-cost",
                      "evidence": "offline-fixture-only; not a real channel price",
                      "internal_calls_controlled": True} for model in models]
        return self.auth.replace(scope, scope_id, resources=resources,
            expires_at=self.clock.now_ms() + 3_600_000, expected_revision=rev,
            operation_id=op or f"grant-{scope}-{scope_id}-{rev}", actor="operator", reason="test-scope")

    def switch(self, mode, rev=1):
        return self.policies.switch_task(self.task["id"], policy=policy(mode), expected_revision=rev,
            operation_id=f"task-policy-{rev+1}", actor="operator", reason="explicit-switch")

    def attempt(self, model=FREE, *, collaboration=None):
        self.serial += 1
        sid, mid = f"ses_worker{self.serial}", f"msg_worker{self.serial}"
        meta = {"coordinator": "ses_coord", "dispatch": {"prompt": "frozen work", "create": {}}}
        if collaboration is not None:
            meta["delivery_collaboration"] = collaboration
        att = self.ledger.create_attempt(self.task["id"], session_id=sid, dispatch_msg_id=mid,
            directory=str(self.home / f"work{self.serial}"), model=model, metadata=meta)
        self.client.set_session(sid, model=dict(zip(("providerID", "id"), model.split("/", 1))))
        return att

    def acquire(self, att):
        return self.actions.attempt(att["id"], observation={"session_id": att["session_id"], "model": att["model"]})

    def dump(self):
        return "\n".join(self.conn.iterdump())

    def failure(self, code, fn):
        before = self.dump()
        with self.assertRaises(LedgerError) as caught:
            fn()
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(self.dump(), before)
        self.assertFalse(self.conn.in_transaction)

    def test_current_authorization_and_policy_are_frozen_once(self):
        self.grant()
        att = self.attempt()
        value = self.acquire(att)
        self.assertEqual(value["policy_revision"], 1)
        self.assertEqual(value["authorization"]["authorization_revision"], 1)
        self.assertEqual(value["expires_at"] - value["issued_at"], 30_000)
        self.assertFalse(value["internal_calls_intercepted"])
        self.switch("latency-first")
        self.failure("DELIVERY_ACTION_ALREADY_POSSIBLE", lambda: self.acquire(att))
        self.assertEqual(self.actions.task_inflight(self.task["id"])["items"][0]["policy_revision"], 1)

    def test_paid_denied_in_budget_but_mode_does_not_grant_resources(self):
        self.grant(models=(FREE,))
        self.switch("latency-first")
        att = self.attempt(PAID)
        before = self.dump()
        with self.assertRaises(LedgerError):
            self.acquire(att)
        self.assertEqual(self.dump(), before)
        self.grant(rev=1)
        self.assertEqual(self.acquire(att)["policy_revision"], 2)
        self.switch("budget-first", 2)
        second = self.attempt(PAID)
        with self.assertRaises(LedgerError):
            self.acquire(second)

    def test_revoked_task_authorization_does_not_fallback_to_project(self):
        self.grant()
        self.grant("task", self.task["id"], models=())
        att = self.attempt()
        before = self.dump()
        with self.assertRaises(LedgerError):
            self.acquire(att)
        self.assertEqual(self.dump(), before)

    def test_handoff_expiry_never_renews_original_identity(self):
        self.grant()
        att = self.attempt()
        value = self.acquire(att)
        self.clock.advance(seconds=30)
        with self.assertRaises(LedgerError) as caught:
            check_handoff(value, self.clock.now_ms())
        self.assertEqual(caught.exception.code, "DELIVERY_ACTION_EXPIRED")
        self.failure("DELIVERY_ACTION_ALREADY_POSSIBLE", lambda: self.acquire(att))
        self.assertEqual(self.actions.task_inflight(self.task["id"])["total"], 1)

    def test_model_change_does_not_rebind_session(self):
        self.grant()
        att = self.attempt()
        self.failure("DELIVERY_MODEL_CHANGED", lambda: self.actions.attempt(att["id"],
            observation={"session_id": att["session_id"], "model": PAID}))

    def test_paid_revocation_preserves_old_permit_and_new_action_rechecks(self):
        self.grant()
        self.switch("latency-first")
        old = self.acquire(self.attempt(PAID))
        self.grant(models=(FREE,), rev=1)
        att = self.attempt(PAID)
        before = self.dump()
        with self.assertRaises(LedgerError):
            self.acquire(att)
        self.assertEqual(self.dump(), before)
        self.assertEqual(old["authorization"]["resource"]["cost"], "paid")

    def test_expired_authorization_denies_without_new_permit(self):
        self.grant()
        att = self.attempt()
        self.clock.advance(seconds=3601)
        before = self.dump()
        with self.assertRaises(LedgerError):
            self.acquire(att)
        self.assertEqual(self.dump(), before)

    def test_task_pause_and_provider_state_remain_independent(self):
        self.grant()
        att = self.attempt()
        self.ledger.transition_task(self.task["id"], "paused", reason="explicit")
        self.switch("latency-first")
        self.failure("DISPATCH_CONTROL_CHANGED", lambda: self.acquire(att))
        self.ledger.transition_task(self.task["id"], "dispatched", reason="explicit-resume")
        self.ledger.set_meta("provider_state", json.dumps({FREE: {"until": self.clock.now_ms()+1000}}))
        self.failure("DELIVERY_PROVIDER_BLOCKED", lambda: self.acquire(att))
        self.ledger.set_meta("provider_state", "{bad")
        self.failure("DELIVERY_PROVIDER_BLOCKED", lambda: self.acquire(att))

    def test_old_attempt_cannot_take_new_currents_permit(self):
        self.grant()
        old = self.attempt()
        self.attempt()
        self.failure("DISPATCH_CONTROL_CHANGED", lambda: self.acquire(old))

    def test_permit_audit_failure_rolls_back_the_whole_issue(self):
        self.grant()
        att = self.attempt()
        self.conn.execute("CREATE TEMP TRIGGER fail_action BEFORE INSERT ON audit "
            "WHEN NEW.action='delivery-action.possible' BEGIN SELECT RAISE(ABORT,'injected'); END")
        before = self.dump()
        with self.assertRaises(sqlite3.IntegrityError):
            self.acquire(att)
        self.assertEqual(self.dump(), before)

    def test_corrupt_existing_permit_is_not_new_send(self):
        self.grant()
        att = self.attempt()
        self.acquire(att)
        key = _key(att["session_id"], att["dispatch_msg_id"], "prompt")
        self.conn.execute("UPDATE meta SET value='{}' WHERE key=?", (key,))
        self.failure("DELIVERY_ACTION_CORRUPT", lambda: self.acquire(att))

    def test_managed_dispatch_identity_or_frozen_body_cannot_be_replaced(self):
        self.grant()
        att = self.attempt()
        for field in ("session_id", "dispatch_msg_id"):
            self.failure("DELIVERY_IDENTITY_IMMUTABLE", lambda: self.ledger.update_attempt(
                att["id"], fields={field: "replacement"}))
        metadata = {**att["metadata"], "dispatch": {**att["metadata"]["dispatch"], "prompt": "changed"}}
        self.failure("DELIVERY_PLAN_IMMUTABLE", lambda: self.ledger.update_attempt(att["id"], fields={"metadata": metadata}))

    def test_legacy_unknown_never_gets_a_new_permit_after_explicit_binding(self):
        self.grant()
        att = self.attempt()
        self.ledger.record_dispatch(att["id"], "unknown", reason="legacy-response-unknown")
        self.failure("DELIVERY_ACTION_ALREADY_POSSIBLE", lambda: self.acquire(att))

    def test_restart_retains_original_permit_and_body(self):
        self.grant()
        att = self.attempt()
        original = self.acquire(att)
        other = connect(self.home)
        self.addCleanup(other.close)
        actions = DeliveryActions(Ledger(other, self.home, clock=self.clock))
        with self.assertRaises(LedgerError):
            actions.attempt(att["id"], observation={"session_id": att["session_id"], "model": FREE})
        loaded = actions._load(_key(att["session_id"], att["dispatch_msg_id"], "prompt"))
        self.assertEqual(loaded, original)
        self.assertIn("交付策略", self.ledger.get_attempt(att["id"])["metadata"]["dispatch"]["prompt"])

    def test_new_task_inherits_default_snapshot_not_legacy_tasks(self):
        self.policies.set_project_default("p", policy=policy("elastic"), expected_revision=0,
            operation_id="default-1", actor="operator", reason="explicit")
        task = self.ledger.create_task(project="p", title="new")
        self.assertEqual(self.policies.task_snapshot(task["id"])["version"]["source"]["revision"], 1)
        self.policies.set_project_default("p", policy=policy("latency-first"), expected_revision=1,
            operation_id="default-2", actor="operator", reason="explicit")
        self.assertEqual(self.policies.task_snapshot(task["id"])["version"]["policy"]["preset"], "elastic")
        self.assertEqual(self.policies.task_snapshot(self.task["id"])["version"]["policy"]["preset"], "budget-first")

    def test_failed_new_task_binding_rolls_back_task(self):
        self.policies.set_project_default("p", policy=policy(), expected_revision=0,
            operation_id="default-1", actor="operator", reason="explicit")
        self.conn.execute("CREATE TEMP TRIGGER fail_binding BEFORE INSERT ON audit "
            "WHEN NEW.action='delivery-policy.draft-switch' BEGIN SELECT RAISE(ABORT,'injected'); END")
        before = self.dump()
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.create_task(project="p", title="rollback", task_id="T-rollback")
        self.assertEqual(self.dump(), before)

    def test_three_collaboration_limits_are_independent(self):
        self.grant()
        value = policy()
        value["limits"] = {"max_parallel": 3, "max_candidates": 1, "max_challenge_rounds": 0}
        self.policies.switch_task(self.task["id"], policy=value, expected_revision=1,
            operation_id="limits", actor="operator", reason="explicit")
        self.acquire(self.attempt(collaboration={"kind": "candidate", "round": 0, "reason": "eliminate ambiguity"}))
        second = self.attempt(collaboration={"kind": "candidate", "round": 0, "reason": "independent alternative"})
        self.failure("DELIVERY_CANDIDATE_LIMIT", lambda: self.acquire(second))
        challenge = self.attempt(collaboration={"kind": "challenge", "round": 1, "reason": "optional counterexample"})
        self.failure("DELIVERY_CHALLENGE_LIMIT", lambda: self.acquire(challenge))
        fourth = self.attempt(collaboration={"kind": "partition", "round": 0, "reason": "shortens integration"})
        self.failure("DELIVERY_PARALLEL_LIMIT", lambda: self.acquire(fourth))

    def test_uncontrolled_route_is_not_misreported_as_free(self):
        self.auth.replace("project", "p", resources=[{"model": FREE, "cost": "no-extra-cost",
            "evidence": "fixture", "internal_calls_controlled": False}],
            expires_at=self.clock.now_ms()+100_000, expected_revision=0,
            operation_id="uncontrolled", actor="operator", reason="test")
        att = self.attempt()
        with self.assertRaises(LedgerError):
            self.acquire(att)

    def test_application_dispatch_checks_before_network_outside_transaction(self):
        self.grant()
        original_prompt = self.client.prompt

        def prompt(*args, **kwargs):
            self.assertFalse(self.conn.in_transaction)
            self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM meta WHERE key LIKE ?", (PREFIX+"%",)).fetchone()[0], 1)
            return original_prompt(*args, **kwargs)

        self.client.prompt = prompt
        app = application(self.ledger, lambda: self.client, self.cfg, clock=self.clock)
        out = app.dispatch(self.task["id"], directory=str(self.home / "app"), coordinator="ses_coord", model=FREE)
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertIn("预算优先", self.client.prompt_calls[0]["text"])
        self.assertEqual(out["message_id"], self.client.prompt_calls[0]["message_id"])

    def test_unknown_dispatch_recovery_does_not_repost_original_input(self):
        self.grant()
        app = application(self.ledger, lambda: self.client, self.cfg, clock=self.clock)
        self.client.fail("prompt", TransportError("/prompt", "response unknown"))
        with self.assertRaises(ActionError) as caught:
            app.dispatch(self.task["id"], directory=str(self.home / "app"), coordinator="ses_coord", model=FREE)
        att = self.ledger.get_attempt(caught.exception.detail["attempt_id"])
        sup = Supervisor(self.home, cfg=self.cfg, client=self.client, clock=self.clock)
        self.addCleanup(sup.conn.close)
        for _ in range(2):
            sup.reconcile_dispatch(att)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM audit WHERE action='delivery-action.possible'").fetchone()[0], 1)
        self.assertEqual(self.client.inboxes.get(att["session_id"], []), [])
        self.assertEqual(self.ledger.get_attempt(att["id"])["dispatch_msg_id"], att["dispatch_msg_id"])

    def test_untracked_input_cannot_bypass_managed_task(self):
        self.grant()
        att = self.attempt()
        mi = make_managed_inputs(self.conn, self.home, client=lambda: self.client, clock=self.clock)
        with self.assertRaises(LedgerError) as caught:
            mi.send_tracked(att["session_id"], text="new execution", owner="ses_coord", untracked=True)
        self.assertEqual(caught.exception.code, "DELIVERY_TRACKING_REQUIRED")
        self.assertEqual(self.client.prompt_calls, [])

    def test_managed_send_and_park_flush_each_require_current_policy(self):
        self.grant()
        att = self.attempt()
        mi = make_managed_inputs(self.conn, self.home, client=lambda: self.client, clock=self.clock)
        out = mi.send_tracked(att["session_id"], text="execute next", owner="ses_coord",
            task_id=self.task["id"], attempt_id=att["id"], target_session="ses_coord")
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.assertEqual(self.actions.task_inflight(self.task["id"])["total"], 1)
        self.client.deliver_inbox(att["session_id"], at=self.clock.now_ms()+1)
        parked = mi.send(att["session_id"], intent="park", text="parked next", owner="ses_coord",
            request_id="park-1", project="p", expires_at=self.clock.now_ms()+500_000,
            flush_condition="manual", task_id=self.task["id"], attempt_id=att["id"],
            target_session="ses_coord")
        input_id = parked["input"]["input_id"]
        self.grant(models=(), rev=1)
        row = mi._ledger.get_managed_input(input_id)
        with self.assertRaises(LedgerError):
            mi.flush(input_id, owner="ses_coord", confirm=input_id,
                     expected_revision=row["revision"], reason="explicit-flush")
        self.assertEqual(self.client.inboxes[att["session_id"]][0]["delivery"], "queue")

    def test_wake_uses_own_authorization_and_rolls_back_provider_probe(self):
        self.grant()
        notice = self.ledger.enqueue_notification(kind="coordinator_manual_recovery", target_session="ses_coord",
            task_id=self.task["id"], project="p", dedupe_key="wake-resource", payload={})
        inp = self.ledger.reserve_target_input("ses_coord", notification_ids=[notice["id"]], owner="sup")["input"]
        fin = self.ledger.finalize_input(inp["input_id"], final_notification_ids=[notice["id"]],
            text_factory=lambda _: "wake", owner="sup", expected_revision=inp["revision"])["input"]
        gate = TargetGate(self.conn, self.clock)
        gate.observe_error("ses_coord", {"category": "rate_limit", "status": 429,
            "retry_after_ms": 1, "message": "fixture"}, evidence_key="wake-fixture", observed_at=self.clock.now_ms())
        self.clock.advance(seconds=2)
        before = self.dump()
        result = WakeAdmission(self.ledger, gate).claim("ses_coord", input_id=fin["input_id"],
            message_id=fin["message_id"], owner="sup", expected_revision=fin["revision"],
            model_observation={"session_id": "ses_coord", "model": PAID})
        self.assertFalse(result["allowed"])
        self.assertTrue(result["reason"].startswith("resource:"), result)
        self.assertEqual(self.dump(), before, "授权拒绝不得消耗provider探测或possible预算")
        self.grant("coordinator", "ses_coord", models=(PAID,))
        result = WakeAdmission(self.ledger, gate).claim("ses_coord", input_id=fin["input_id"],
            message_id=fin["message_id"], owner="sup", expected_revision=fin["revision"],
            model_observation={"session_id": "ses_coord", "model": PAID})
        self.assertTrue(result["allowed"], result)
        self.assertIsNone(result["permit"]["policy_revision"])
        self.assertTrue(result["permit"]["coordinator_cost_independent"])

    def test_permit_first_and_switch_second_have_deterministic_order(self):
        self.grant()
        att = self.attempt()
        held, switch_waiting, release = threading.Event(), threading.Event(), threading.Event()
        outputs, errors = {}, []
        original = DeliveryActions._issue

        def issue(store, **kwargs):
            result = original(store, **kwargs)
            held.set()
            if not release.wait(10):
                raise RuntimeError("test synchronization timed out")
            return result

        def permit_thread():
            conn = connect(self.home)
            try:
                outputs["permit"] = DeliveryActions(Ledger(conn, self.home, clock=self.clock)).attempt(
                    att["id"], observation={"session_id": att["session_id"], "model": FREE})
            except BaseException as exc:
                errors.append(exc)
            finally:
                conn.close()

        def switch_thread():
            conn = connect(self.home)
            try:
                conn.set_trace_callback(lambda sql: switch_waiting.set() if sql == "BEGIN IMMEDIATE" else None)
                outputs["switch"] = DeliveryPolicyStore(conn, self.clock).switch_task(self.task["id"],
                    policy=policy("latency-first"), expected_revision=1, operation_id="race-switch",
                    actor="operator", reason="race")
            except BaseException as exc:
                errors.append(exc)
            finally:
                conn.close()

        with patch.object(DeliveryActions, "_issue", issue):
            first = threading.Thread(target=permit_thread)
            second = threading.Thread(target=switch_thread)
            first.start()
            self.assertTrue(held.wait(10))
            second.start()
            try:
                self.assertTrue(switch_waiting.wait(10))
            finally:
                release.set()
            first.join(15)
            second.join(15)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(outputs["permit"]["policy_revision"], 1)
        self.assertEqual(outputs["switch"]["current"]["revision"], 2)
        audit = self.conn.execute("SELECT action FROM audit ORDER BY id DESC LIMIT 2").fetchall()
        self.assertEqual([r[0] for r in audit], ["delivery-policy.draft-switch", "delivery-action.possible"])

    def test_missing_model_observation_is_never_guessed(self):
        for value in (None, {}, {"id": "ses_worker", "model": {}},
                      {"id": "other", "model": {"providerID": "fixture", "id": "free"}}):
            with self.subTest(value=value), self.assertRaises(LedgerError):
                observed_model(value, "ses_worker")

    def test_supervisor_wake_actual_post_has_independent_permit(self):
        self.grant("coordinator", "ses_coord", models=(PAID,))
        self.client.set_session("ses_coord", model={"providerID": "fixture", "id": "paid"})
        sup = Supervisor(self.home, cfg=self.cfg, client=self.client, clock=self.clock)
        self.addCleanup(sup.conn.close)
        notice = self.ledger.enqueue_notification(kind="coordinator_manual_recovery", target_session="ses_coord",
            task_id=self.task["id"], project="p", dedupe_key="supervisor-wake", payload={})
        claimed = self.ledger.claim_notifications([notice["id"]], sup.instance)
        inp = self.ledger.reserve_target_input("ses_coord", notification_ids=[notice["id"]], owner=sup.instance)["input"]
        fin = self.ledger.finalize_input(inp["input_id"], final_notification_ids=[notice["id"]],
            text_factory=lambda _: "wake frozen", owner=sup.instance, expected_revision=inp["revision"])
        out = sup._send_wake_input("ses_coord", input_id=inp["input_id"], message_id=inp["message_id"],
            body=fin["text"], notification_ids=[notice["id"]], claimed=claimed,
            now=self.clock.now_ms(), revision=fin["input"]["revision"])
        self.assertEqual(out["bucket"], "sent", out)
        self.assertEqual(len(self.client.prompt_calls), 1)
        permit = self.actions._load(_key("ses_coord", inp["message_id"], "prompt"))
        self.assertEqual(permit["authorization"]["scope"], "coordinator")
        self.assertIsNone(permit["policy_revision"])

    def test_managed_execute_recovery_rechecks_revocation_before_steer(self):
        self.grant()
        att = self.attempt()
        mi = make_managed_inputs(self.conn, self.home, client=lambda: self.client, clock=self.clock)
        sent = mi.send_tracked(att["session_id"], text="execute", owner="ses_coord",
            task_id=self.task["id"], attempt_id=att["id"], target_session="ses_coord")
        input_id = sent["input"]["input_id"]
        self.grant(models=(), rev=1)
        self.clock.advance(seconds=10)
        out = mi.recover_pending_execute(input_id, grace_ms=0)
        self.assertFalse(out["recovered"])
        self.assertTrue(out["blocked"])
        self.assertEqual(self.client.inboxes[att["session_id"]][0]["delivery"], "queue")
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_rework_uses_new_policy_and_new_permit_without_rebinding_model(self):
        self.grant()
        app = application(self.ledger, lambda: self.client, self.cfg, clock=self.clock)
        first = app.dispatch(self.task["id"], directory=str(self.home / "app"), coordinator="ses_coord", model=FREE)
        # 仅离线夹具模拟已确认终态/释放；不是产品绕过终态证据入口。
        self.conn.execute("UPDATE attempts SET status='terminal_failed',terminal_at=?,site_released_at=? WHERE id=?",
                          (self.clock.now_ms(), self.clock.now_ms(), first["attempt_id"]))
        self.switch("latency-first")
        second = app.rework(self.task["id"], feedback="fix counterexample", coordinator="ses_coord")
        permit = self.actions._load(_key(second["session_id"], second["message_id"], "prompt"))
        self.assertEqual(permit["policy_revision"], 2)
        self.assertEqual(permit["model"], FREE)
        self.assertEqual(first["session_id"], second["session_id"])
        self.assertEqual(len(self.client.prompt_calls), 2)

    def test_prepared_recovery_after_switch_obtains_current_not_planned_revision(self):
        self.grant()
        att = self.attempt()
        self.switch("latency-first")
        sup = Supervisor(self.home, cfg=self.cfg, client=self.client, clock=self.clock)
        self.addCleanup(sup.conn.close)
        out = sup.reconcile_dispatch(att)
        self.assertIsNotNone(out)
        permit = self.actions._load(_key(att["session_id"], att["dispatch_msg_id"], "prompt"))
        self.assertEqual(att["metadata"]["delivery_plan"]["policy_revision"], 1)
        self.assertEqual(permit["policy_revision"], 2)
        self.assertEqual(permit["model"], FREE)
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_switch_first_blocks_old_paid_route_at_waiting_permit(self):
        self.grant()
        self.switch("latency-first")
        att = self.attempt(PAID)
        held, waiting, release = threading.Event(), threading.Event(), threading.Event()
        errors, outcomes = [], {}
        original = DeliveryPolicyStore._apply_in_tx

        def apply(store, intent, digest):
            out = original(store, intent, digest)
            held.set()
            if not release.wait(10):
                raise RuntimeError("test synchronization timed out")
            return out

        def switch_thread():
            conn = connect(self.home)
            try:
                outcomes["switch"] = DeliveryPolicyStore(conn, self.clock).switch_task(self.task["id"],
                    policy=policy(), expected_revision=2, operation_id="switch-first",
                    actor="operator", reason="tighten")
            except BaseException as exc:
                errors.append(exc)
            finally:
                conn.close()

        def permit_thread():
            conn = connect(self.home)
            try:
                conn.set_trace_callback(lambda sql: waiting.set() if sql == "BEGIN IMMEDIATE" else None)
                try:
                    DeliveryActions(Ledger(conn, self.home, clock=self.clock)).attempt(att["id"],
                        observation={"session_id": att["session_id"], "model": PAID})
                except LedgerError as exc:
                    outcomes["deny"] = exc.code
            except BaseException as exc:
                errors.append(exc)
            finally:
                conn.close()

        with patch.object(DeliveryPolicyStore, "_apply_in_tx", apply):
            first, second = threading.Thread(target=switch_thread), threading.Thread(target=permit_thread)
            first.start()
            self.assertTrue(held.wait(10))
            second.start()
            try:
                self.assertTrue(waiting.wait(10))
            finally:
                release.set()
            first.join(15)
            second.join(15)
        self.assertEqual(errors, [])
        self.assertFalse(first.is_alive() or second.is_alive())
        self.assertEqual(outcomes["deny"], "RESOURCE_AUTHORIZATION_PRESET_REJECTED")
        self.assertEqual(outcomes["switch"]["current"]["revision"], 3)
        self.assertIsNone(self.actions._load(_key(att["session_id"], att["dispatch_msg_id"], "prompt")))

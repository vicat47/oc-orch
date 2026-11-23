#!/usr/bin/env python3
"""S4 后续责任纯规则单元测试（design D9 / managed-input-lifecycle）。

只断言真实不变量与拒绝性场景，不镜像字面量：

- 状态模型没有死路、终态不可重开、执行结束不能跳过待核收；
- ACK/受理/空 inbox/execution succeeded 不能关闭业务，done/cancelled/转交
  需要各自证据，人工等待只能人工放行；
- 晚到回报在 current/generation/result_version/接续目标变化时只留历史；
- 期限与持久预算耗尽转可见人工，重启/旧字段形状不改变计数；
- 托管 execute 的归属/owner/接续目标/期限/材料/写权/封存 attempt 校验；
- 稳定 condition_key 不随观察时间变化；缺失责任诊断有界且不被租约刷新绕过。

零 I/O；记录形状与 responsibilities 持久行一致（deadline/budget_used/...）。
"""

from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))

from oc_orch.domain.responsibility import (  # noqa: E402
    ALLOWED_TRANSITIONS,
    EV_BUSINESS_COMPLETE,
    EV_MANUAL_RELEASED,
    EV_TASK_ACCEPTED,
    EV_TASK_CANCELLED,
    MODE_COMMUNICATION,
    OPEN_STATES,
    STATE_AWAITING_REVIEW,
    STATE_CANCELLED,
    STATE_DONE,
    STATE_HANDED_OVER,
    STATE_HANDLING,
    STATE_WAITING_DEPENDENCY,
    STATE_WAITING_EXECUTION,
    STATE_WAITING_HUMAN,
    STATE_WAITING_INPUT,
    TERMINAL_STATES,
    deadline_decision,
    derive_wait,
    missing_responsibility_check,
    next_check_at,
    responsibility_identity,
    staleness,
    transition,
    validate_managed_execute,
)

SHA_A = "a" * 64


def row(**overrides) -> dict:
    """responsibilities 持久行（字段与 adapters/responsibility_ledger 一致）。"""
    base = {
        "id": "R-1", "project": "proj", "task_id": "T-1",
        "attempt_id": "A-1", "generation": 1, "result_version": "rv1-abc",
        "input_id": "I-1", "session_id": "ses_worker", "message_id": "msg_1",
        "owner": "coordinator:main", "target_session": "coordinator:review",
        "state": STATE_WAITING_EXECUTION, "wait_kind": "worker-execution",
        "wait_condition": "worker-execution", "next_action": "wait",
        "next_check_at": 1_000, "deadline": 10_000, "condition_key": "R-key",
        "revision": 1, "budget_used": 0, "max_budget": 3, "evidence": "{}",
        "prior_id": None, "successor_id": "coordinator:review",
        "created_at": 1_000, "updated_at": 1_000, "closed_at": None,
        "close_reason": None,
    }
    base.update(overrides)
    return base


ATTRIBUTION = {"task_ids": ["T-1"], "successor_ids": ["coordinator:review"]}
ACTIVE_ATTEMPT = {"id": "A-1", "task_id": "T-1", "status": "running"}


def declaration(**overrides) -> dict:
    base = {
        "intent": "execute", "target_session": "ses_worker",
        "message_id": "msg_1", "task_id": "T-1", "attempt_id": "A-1",
        "owner": "coordinator:main", "claim_owner": "coordinator:main",
        "successor": "coordinator:review", "deadline": 2_000,
        "prepared_at": 1_000,
        "material_refs": [{"kind": "file", "ref": "docs/plan.md",
                           "sha256": SHA_A, "required": True}],
    }
    base.update(overrides)
    return base


def codes(result: dict) -> list[str]:
    return [e["code"] for e in result["errors"]]


class IdentityTests(unittest.TestCase):
    def test_identity_is_stable_across_observation_time(self):
        first = responsibility_identity(
            kind="managed-execute", project="proj", task_id="T-1",
            attempt_id="A-1", condition="worker-execution", now_ms=1_000)
        second = responsibility_identity(
            kind="managed-execute", project="proj", task_id="T-1",
            attempt_id="A-1", condition="worker-execution", now_ms=9_999,
            observed_at=42_000)
        self.assertEqual(first, second)
        self.assertTrue(first.startswith("R-"))

    def test_identity_changes_with_episode_inputs_not_observation(self):
        variants = [
            dict(kind="k", project="p", task_id="T-1"),
            dict(kind="k", project="p", task_id="T-2"),
            dict(kind="k", project="p", task_id="T-1", attempt_id="A-1"),
            dict(kind="k", project="p", task_id="T-1",
                 condition_version=2),
        ]
        identities = {responsibility_identity(**v) for v in variants}
        self.assertEqual(len(identities), len(variants))

    def test_identity_rejects_missing_discriminator(self):
        with self.assertRaises(ValueError):
            responsibility_identity(kind="")
        with self.assertRaises(ValueError):
            responsibility_identity(kind="managed-execute")


class StateInvariantTests(unittest.TestCase):
    def test_each_open_state_can_reach_terminal(self):
        for state in OPEN_STATES:
            seen, queue, reached = {state}, [state], False
            while queue and not reached:
                node = queue.pop()
                for nxt in ALLOWED_TRANSITIONS[node]:
                    if nxt in TERMINAL_STATES:
                        reached = True
                        break
                    if nxt not in seen:
                        seen.add(nxt)
                        queue.append(nxt)
            self.assertTrue(reached, f"{state} 无法到达终态（静默失管）")

    def test_terminal_states_have_no_outgoing_transitions(self):
        for state in TERMINAL_STATES:
            self.assertEqual(ALLOWED_TRANSITIONS[state], set())

    def test_execution_end_cannot_skip_review(self):
        self.assertNotIn(STATE_DONE, ALLOWED_TRANSITIONS[STATE_WAITING_EXECUTION])


class TransitionTests(unittest.TestCase):
    def test_execution_end_moves_to_awaiting_review(self):
        result = transition(row(), to_state=STATE_AWAITING_REVIEW,
                            now_ms=2_000, reason="worker ended")
        self.assertTrue(result["allowed"])
        self.assertEqual(result["updates"]["state"], STATE_AWAITING_REVIEW)
        self.assertEqual(result["updates"]["next_check_at"], 1_000)
        self.assertTrue(result["effects"]["wake_allowed"])

    def test_execution_cannot_complete_directly_without_task_verification(self):
        # 普通成功/业务完成仍须先待核收；EV_TASK_ACCEPTED 是唯一的权威例外
        succeeded = transition(row(), to_state=STATE_DONE, now_ms=2_000,
                               reason="succeeded")
        self.assertFalse(succeeded["allowed"])
        self.assertEqual(succeeded["code"], "review-required")
        business = transition(row(), to_state=STATE_DONE, now_ms=2_000,
                              reason="structured business complete",
                              evidence=[EV_BUSINESS_COMPLETE])
        self.assertFalse(business["allowed"])
        self.assertEqual(business["code"], "review-required")
        verified = transition(row(), to_state=STATE_DONE, now_ms=2_000,
                              reason="verify passed",
                              evidence=[EV_TASK_ACCEPTED])
        self.assertTrue(verified["allowed"], verified)
        self.assertEqual(verified["updates"]["state"], STATE_DONE)
        self.assertTrue(verified["effects"]["closed"])
        self.assertFalse(verified["effects"]["grants_write"])

    def test_done_requires_business_or_verification_evidence(self):
        pending = row(state=STATE_AWAITING_REVIEW)
        for evidence in ([], ["business-ack"], ["notification-ack"],
                         ["execution-succeeded"], ["inbox-empty"]):
            with self.subTest(evidence=evidence):
                result = transition(pending, to_state=STATE_DONE, now_ms=2_000,
                                    reason="done", evidence=evidence)
                self.assertFalse(result["allowed"])
                self.assertEqual(result["code"], "closure-evidence-required")
        accepted = transition(pending, to_state=STATE_DONE, now_ms=2_000,
                              reason="verify passed",
                              evidence=[EV_TASK_ACCEPTED])
        self.assertTrue(accepted["allowed"])
        self.assertEqual(accepted["updates"]["close_reason"], "verify passed")
        self.assertIsNotNone(accepted["updates"]["closed_at"])
        # next_check_at 是 NOT NULL 持久列：关闭后保留原值，由 closed_at 停止调度。
        self.assertEqual(accepted["updates"]["next_check_at"], 1_000)
        self.assertFalse(accepted["effects"]["grants_write"])

    def test_cancelled_requires_task_cancelled_evidence(self):
        denied = transition(row(state=STATE_HANDLING), to_state=STATE_CANCELLED,
                            now_ms=2_000, reason="stop")
        self.assertEqual(denied["code"], "cancellation-evidence-required")
        allowed = transition(row(state=STATE_HANDLING),
                             to_state=STATE_CANCELLED, now_ms=2_000,
                             reason="task cancelled",
                             evidence=[EV_TASK_CANCELLED])
        self.assertTrue(allowed["allowed"])
        self.assertTrue(allowed["effects"]["closed"])
        self.assertTrue(allowed["effects"]["stopped_external_execution"] is False)

    def test_terminal_rows_cannot_reopen_and_same_state_is_noop(self):
        noop = transition(row(), to_state=STATE_WAITING_EXECUTION,
                          now_ms=2_000, reason="observe")
        self.assertEqual(noop["code"], "no-op")
        self.assertEqual(noop["updates"], {})
        for closed in (row(state=STATE_DONE, closed_at=1_500),
                       row(state=STATE_CANCELLED, closed_at=1_500)):
            with self.subTest(state=closed["state"]):
                result = transition(closed, to_state=STATE_HANDLING,
                                    now_ms=2_000, reason="reopen",
                                    evidence=[EV_TASK_ACCEPTED])
                self.assertFalse(result["allowed"])
                self.assertEqual(result["code"], "already-terminal")

    def test_handoff_needs_successor_and_owner_without_write(self):
        missing = transition(row(), to_state=STATE_HANDED_OVER, now_ms=2_000,
                             reason="handoff")
        self.assertEqual(missing["code"], "successor-required")
        no_owner = transition(row(), to_state=STATE_HANDED_OVER, now_ms=2_000,
                              reason="handoff", successor_id="R-next")
        self.assertEqual(no_owner["code"], "successor-owner-required")
        self_ref = transition(row(), to_state=STATE_HANDED_OVER, now_ms=2_000,
                              reason="handoff", successor_id="R-1",
                              successor_owner="coordinator:next")
        self.assertEqual(self_ref["code"], "successor-invalid")
        done = transition(row(), to_state=STATE_HANDED_OVER, now_ms=2_000,
                          reason="handoff", successor_id="R-next",
                          successor_owner="coordinator:next")
        self.assertTrue(done["allowed"])
        self.assertEqual(done["updates"]["successor_id"], "R-next")
        self.assertEqual(done["updates"]["close_reason"], "transferred")
        self.assertTrue(done["effects"]["successor_persist_required"])
        self.assertFalse(done["effects"]["grants_write"])
        self.assertFalse(done["effects"]["delegates_ack"])

    def test_waiting_human_only_leaves_by_human_release(self):
        waiting = row(state=STATE_WAITING_HUMAN)
        automated = transition(waiting, to_state=STATE_HANDLING, now_ms=2_000,
                               reason="reconcile", evidence=[EV_MANUAL_RELEASED])
        self.assertEqual(automated["code"], "manual-control-holds")
        human = transition(waiting, to_state=STATE_HANDLING, now_ms=2_000,
                           reason="human resumed", actor="human",
                           evidence=[EV_MANUAL_RELEASED])
        self.assertTrue(human["allowed"])
        self.assertEqual(human["updates"]["state"], STATE_HANDLING)

    def test_waiting_update_keeps_flat_store_fields(self):
        result = transition(row(), to_state=STATE_WAITING_DEPENDENCY,
                            now_ms=2_000, reason="waiting upstream",
                            wait_kind="dependency",
                            wait_condition="upstream-task",
                            next_check_at=3_000)
        updates = result["updates"]
        self.assertEqual(updates["wait_kind"], "dependency")
        self.assertEqual(updates["wait_condition"], "upstream-task")
        self.assertEqual(updates["next_check_at"], 3_000)
        self.assertNotIn("deadline_at", updates)
        self.assertNotIn("budget", updates)
        self.assertEqual(updates["revision"], 2)


class WaitTests(unittest.TestCase):
    def test_task_terminal_evidence_or_manual_wait(self):
        cancelled = derive_wait(now_ms=1_000, task_status="cancelled")
        self.assertEqual(cancelled["state"], STATE_CANCELLED)
        self.assertEqual(cancelled["closure_evidence"], EV_TASK_CANCELLED)
        verified = derive_wait(now_ms=1_000, task_status="accepted",
                               task_verified=True)
        self.assertEqual(verified["state"], STATE_DONE)
        self.assertEqual(verified["closure_evidence"], EV_TASK_ACCEPTED)
        unverified = derive_wait(now_ms=1_000, task_status="accepted")
        self.assertEqual(unverified["state"], STATE_WAITING_HUMAN)
        self.assertIsNone(unverified["closure_evidence"])
        self.assertFalse(unverified["wake_allowed"])

    def test_manual_controls_are_visible_waits(self):
        for kwargs in ({"task_status": "paused"}, {"manual_paused": True},
                       {"stop_requested": True}):
            with self.subTest(**kwargs):
                view = derive_wait(now_ms=1_000, **kwargs)
                self.assertEqual(view["state"], STATE_WAITING_HUMAN)
                self.assertEqual(view["wait_kind"], "manual")
                self.assertFalse(view["wake_allowed"])
                self.assertFalse(view["auto_resume"])

    def test_cooldown_blocks_until_expiry(self):
        active = derive_wait(now_ms=1_000, cooldown_until=5_000)
        self.assertEqual(active["state"], STATE_WAITING_DEPENDENCY)
        self.assertEqual(active["next_check_at"], 5_000)
        self.assertFalse(active["wake_allowed"])
        expired = derive_wait(now_ms=5_000, cooldown_until=5_000)
        self.assertEqual(expired["state"], STATE_HANDLING)
        self.assertTrue(expired["auto_resume"])

    def test_unknown_task_status_is_not_assumed_progress(self):
        view = derive_wait(now_ms=1_000, task_status="queued")
        self.assertEqual(view["state"], STATE_WAITING_HUMAN)
        self.assertFalse(view["auto_resume"])


class StalenessTests(unittest.TestCase):
    CURRENT = {"current_attempt_id": "A-1", "generation": 1,
               "result_version": "rv1-abc",
               "successor_id": "coordinator:review"}

    def test_matching_attribution_is_fresh(self):
        result = staleness(row(), current=self.CURRENT)
        self.assertEqual(result["status"], "fresh")
        self.assertTrue(result["apply"])
        self.assertFalse(result["recheck"])

    def test_any_attribution_change_invalidates_late_report(self):
        changes = [
            dict(self.CURRENT, current_attempt_id="A-2"),
            dict(self.CURRENT, generation=2),
            dict(self.CURRENT, result_version="rv2"),
            dict(self.CURRENT, successor_id="coordinator:other"),
        ]
        for current in changes:
            with self.subTest(current=current):
                result = staleness(row(), current=current)
                self.assertEqual(result["status"], "stale")
                self.assertFalse(result["apply"])
                self.assertTrue(result["preserve_history"])
                self.assertTrue(result["recheck"])

    def test_unresolved_current_requires_recheck(self):
        for current in (dict(self.CURRENT, current_attempt_id=None),
                        dict(self.CURRENT, generation=None),
                        dict(self.CURRENT, result_version=None)):
            with self.subTest(current=current):
                result = staleness(row(), current=current)
                self.assertEqual(result["status"], "unconfirmed")
                self.assertFalse(result["apply"])
                self.assertTrue(result["recheck"])

    def test_unbound_result_version_cannot_be_applied(self):
        result = staleness(row(result_version=None),
                           current=dict(self.CURRENT, result_version="rv1-abc"))
        self.assertEqual(result["code"], "result-version-unbound")
        self.assertFalse(result["apply"])

    def test_row_without_attribution_is_unconfirmed(self):
        result = staleness(row(attempt_id=None, generation=None,
                               result_version=None, successor_id=None),
                           current=self.CURRENT)
        self.assertEqual(result["code"], "attribution-unrecorded")
        self.assertFalse(result["apply"])


class DeadlineTests(unittest.TestCase):
    def test_deadline_exhaustion_routes_to_visible_manual(self):
        result = deadline_decision(row(), now_ms=10_000)
        self.assertTrue(result["exhausted"])
        self.assertEqual(result["code"], "deadline-exhausted")
        self.assertEqual(result["state"], STATE_WAITING_HUMAN)
        self.assertTrue(result["owner"])

    def test_persisted_budget_exhaustion(self):
        result = deadline_decision(row(deadline=100_000, budget_used=3),
                                   now_ms=1_000)
        self.assertTrue(result["exhausted"])
        self.assertEqual(result["code"], "budget-exhausted")
        self.assertEqual(result["budget_used"], 3)

    def test_budget_available_reports_remaining(self):
        result = deadline_decision(row(), now_ms=1_000)
        self.assertFalse(result["exhausted"])
        self.assertEqual(result["code"], "budget-available")
        self.assertEqual(result["max_budget"], 3)

    def test_legacy_fields_and_restart_do_not_reset_budget(self):
        legacy = row(deadline=None, budget_used=None, max_budget=None,
                     deadline_at=2_000, budget={"checks": 3, "max_checks": 3})
        first = deadline_decision(legacy, now_ms=1_000)
        second = deadline_decision(legacy, now_ms=9_000)
        self.assertEqual(first["code"], "budget-exhausted")
        self.assertEqual(first["budget_used"], 3)
        self.assertEqual(second["budget_used"], 3)
        self.assertEqual(next_check_at(row(budget_used=3, deadline=100_000),
                                       now_ms=1_000, base_ms=100,
                                       max_ms=10_000), 1_800)
        self.assertEqual(next_check_at(row(budget_used=3, deadline=1_500),
                                       now_ms=1_000, base_ms=100,
                                       max_ms=10_000), 1_500)


class SendValidationTests(unittest.TestCase):
    def test_valid_managed_execute_has_no_implicit_write(self):
        result = validate_managed_execute(declaration(),
                                          attribution=ATTRIBUTION,
                                          attempt=ACTIVE_ATTEMPT)
        self.assertTrue(result["ok"])
        self.assertTrue(result["applies"])
        self.assertEqual(result["boundary"], "managed")
        self.assertFalse(result["write_authority"])
        self.assertEqual(result["errors"], [])

    def test_required_owner_successor_deadline_and_materials(self):
        result = validate_managed_execute(
            declaration(owner=None, successor=None, deadline=None,
                        material_refs=[{"kind": "file", "ref": "docs/a.md",
                                        "required": True,
                                        "status": "missing"}]),
            attribution=ATTRIBUTION)
        self.assertFalse(result["ok"])
        self.assertEqual(set(codes(result)), {
            "OWNER_REQUIRED", "SUCCESSOR_REQUIRED", "DEADLINE_INVALID",
            "MATERIAL_REQUIRED_UNAVAILABLE"})

    def test_deadline_must_be_valid_and_in_future(self):
        invalid = validate_managed_execute(declaration(deadline=None),
                                           attribution=ATTRIBUTION)
        self.assertIn("DEADLINE_INVALID", codes(invalid))
        past = validate_managed_execute(
            declaration(deadline=500, prepared_at=1_000),
            attribution=ATTRIBUTION)
        self.assertIn("DEADLINE_NOT_FUTURE", codes(past))

    def test_ambiguous_or_unresolved_attribution_is_rejected(self):
        ambiguous = validate_managed_execute(
            declaration(),
            attribution=dict(ATTRIBUTION, task_ids=["T-1", "T-2"]))
        self.assertIn("ATTRIBUTION_AMBIGUOUS", codes(ambiguous))
        unresolved = validate_managed_execute(declaration(), attribution=None)
        self.assertIn("ATTRIBUTION_UNRESOLVED", codes(unresolved))
        wrong_task = validate_managed_execute(
            declaration(task_id="T-9"),
            attribution=dict(ATTRIBUTION, task_ids=["T-1"]))
        self.assertIn("TASK_NOT_ATTRIBUTED", codes(wrong_task))

    def test_sealed_attempt_requires_new_attempt_for_writes(self):
        sealed = {"id": "A-1", "task_id": "T-1", "status": "result_collected"}
        write = validate_managed_execute(
            declaration(claims_write=True, modifies_deliverables=True),
            attribution=ATTRIBUTION, attempt=sealed)
        self.assertIn("NEW_ATTEMPT_REQUIRED", codes(write))
        self.assertFalse(write["write_authority"])
        read_only = validate_managed_execute(
            declaration(), attribution=ATTRIBUTION, attempt=sealed)
        self.assertTrue(read_only["ok"])
        self.assertFalse(read_only["write_authority"])

    def test_write_claim_needs_matching_owner_and_active_attempt(self):
        mismatched = validate_managed_execute(
            declaration(claims_write=True, claim_owner="other:owner"),
            attribution=ATTRIBUTION, attempt=ACTIVE_ATTEMPT)
        self.assertIn("WRITE_CLAIM_UNAUTHORIZED", codes(mismatched))
        no_attempt = validate_managed_execute(
            declaration(claims_write=True), attribution=ATTRIBUTION)
        self.assertIn("WRITE_CLAIM_UNVERIFIED", codes(no_attempt))
        valid = validate_managed_execute(
            declaration(claims_write=True), attribution=ATTRIBUTION,
            attempt=ACTIVE_ATTEMPT)
        self.assertTrue(valid["ok"])
        self.assertTrue(valid["write_authority"])

    def test_modifying_deliverables_requires_claim(self):
        result = validate_managed_execute(
            declaration(modifies_deliverables=True), attribution=ATTRIBUTION,
            attempt=ACTIVE_ATTEMPT)
        self.assertIn("WRITE_CLAIM_REQUIRED", codes(result))
        self.assertFalse(result["write_authority"])

    def test_non_managed_intents_are_not_governed_but_marked_unmanaged(self):
        for intent, mode, boundary in (("park", None, None),
                                       ("record", None, None),
                                       ("execute", MODE_COMMUNICATION,
                                        "unmanaged")):
            with self.subTest(intent=intent, mode=mode):
                result = validate_managed_execute(
                    declaration(intent=intent, mode=mode, tracked=False))
                self.assertTrue(result["ok"])
                self.assertFalse(result["applies"])
                self.assertEqual(result["boundary"], boundary)
                self.assertFalse(result["write_authority"])


class MissingResponsibilityTests(unittest.TestCase):
    TASK = {"id": "T-9", "status": "result_received", "project": "proj",
            "coordinator_session": "ses_coord"}

    def test_registers_on_second_reconcile_with_stable_key(self):
        first = missing_responsibility_check(task=self.TASK, observed_rounds=0,
                                             now_ms=1_000)
        self.assertEqual(first["reason"], "grace-rounds")
        second = missing_responsibility_check(task=self.TASK, observed_rounds=1,
                                              now_ms=9_000)
        self.assertTrue(second["register"])
        self.assertEqual(second["state"], STATE_WAITING_HUMAN)
        self.assertEqual(second["owner"], "ses_coord")
        again = missing_responsibility_check(task=self.TASK, observed_rounds=1,
                                             now_ms=42_000)
        self.assertEqual(second["condition_key"], again["condition_key"])

    def test_open_responsibility_suppresses_but_closed_does_not(self):
        open_row = row(state=STATE_AWAITING_REVIEW, condition_key="R-open",
                       task_id="T-9", close_reason=None)
        suppressed = missing_responsibility_check(
            task=self.TASK, observed_rounds=5, responsibilities=[open_row])
        self.assertEqual(suppressed["reason"], "responsibility-exists")
        self.assertEqual(suppressed["condition_key"], "R-open")
        closed = dict(open_row, state=STATE_DONE, closed_at=1_000)
        self.assertTrue(missing_responsibility_check(
            task=self.TASK, observed_rounds=5, responsibilities=[closed]
        )["register"])

    def test_pending_or_terminal_or_unknown_task_suppresses(self):
        pending = missing_responsibility_check(
            task=self.TASK, observed_rounds=5, pending_notifications=1)
        self.assertEqual(pending["reason"], "notification-pending")
        accepted = missing_responsibility_check(
            task=dict(self.TASK, status="accepted"), observed_rounds=5)
        self.assertEqual(accepted["reason"], "task-terminal")
        unknown = missing_responsibility_check(
            task=dict(self.TASK, status="queued"), observed_rounds=5)
        self.assertEqual(unknown["reason"], "task-status-unknown")

    def test_lease_and_next_check_refresh_cannot_close_responsibility(self):
        result = missing_responsibility_check(
            task=dict(self.TASK, next_check_at=99_999), observed_rounds=1,
            lease_expired=True, task_next_check_at=99_999, now_ms=100_000)
        self.assertTrue(result["register"])

    def test_paused_task_registers_manual_without_wake(self):
        result = missing_responsibility_check(
            task=dict(self.TASK, status="paused", coordinator_session=None),
            observed_rounds=1, fallback_owner="manual:ops")
        self.assertTrue(result["register"])
        self.assertEqual(result["target"], "manual")
        self.assertEqual(result["owner"], "manual:ops")
        self.assertFalse(result["wake_allowed"])


class TerminalFromManualWaitTests(unittest.TestCase):
    """人工 hold 只限制恢复为开放态；终态关闭仍需权威证据（task 真验收/取消）。"""

    def test_waiting_human_closes_on_authoritative_terminal_evidence(self):
        waiting = row(state=STATE_WAITING_HUMAN)
        done = transition(waiting, to_state=STATE_DONE, now_ms=2_000,
                          reason="task-accepted", evidence=[EV_TASK_ACCEPTED])
        self.assertTrue(done["allowed"], done)
        self.assertEqual(done["updates"]["state"], STATE_DONE)
        self.assertTrue(done["effects"]["closed"])
        cancelled = transition(waiting, to_state=STATE_CANCELLED, now_ms=2_000,
                               reason="task-cancelled",
                               evidence=[EV_TASK_CANCELLED])
        self.assertTrue(cancelled["allowed"], cancelled)
        handed = transition(waiting, to_state=STATE_HANDED_OVER, now_ms=2_000,
                            reason="superseded", successor_id="R-next",
                            successor_owner="ops")
        self.assertTrue(handed["allowed"], handed)
        self.assertTrue(handed["effects"]["successor_persist_required"])

    def test_waiting_human_terminal_still_needs_authoritative_evidence(self):
        waiting = row(state=STATE_WAITING_HUMAN)
        no_evidence = transition(waiting, to_state=STATE_DONE, now_ms=2_000,
                                 reason="ack")
        self.assertEqual(no_evidence["code"], "closure-evidence-required")
        no_cancel = transition(waiting, to_state=STATE_CANCELLED, now_ms=2_000,
                               reason="inbox-empty")
        self.assertEqual(no_cancel["code"], "cancellation-evidence-required")
        no_successor = transition(waiting, to_state=STATE_HANDED_OVER,
                                  now_ms=2_000, reason="superseded")
        self.assertEqual(no_successor["code"], "successor-required")

    def test_manual_hold_still_blocks_every_open_target(self):
        waiting = row(state=STATE_WAITING_HUMAN)
        for target in (STATE_HANDLING, STATE_WAITING_INPUT,
                       STATE_WAITING_EXECUTION, STATE_WAITING_DEPENDENCY,
                       STATE_AWAITING_REVIEW):
            with self.subTest(target=target):
                blocked = transition(waiting, to_state=target, now_ms=2_000,
                                     reason="auto-resume",
                                     evidence=[EV_MANUAL_RELEASED])
                self.assertEqual(blocked["code"], "manual-control-holds")
        # 人工放行只恢复到 handling；其余开放态由状态图拒绝（不会一步跳回执行）
        resumed = transition(waiting, to_state=STATE_HANDLING, now_ms=2_000,
                             reason="human resumed", actor="human",
                             evidence=[EV_MANUAL_RELEASED])
        self.assertTrue(resumed["allowed"], resumed)
        jump = transition(waiting, to_state=STATE_WAITING_EXECUTION,
                          now_ms=2_000, reason="human resumed", actor="human",
                          evidence=[EV_MANUAL_RELEASED])
        self.assertEqual(jump["code"], "transition-not-allowed")

    def test_execution_cannot_jump_to_done_without_task_verification(self):
        # worker succeeded / 结构化业务完成不能跳过待核收；真实 verify accepted 可终结
        executing = row(state=STATE_WAITING_EXECUTION)
        for evidence in ([], [EV_BUSINESS_COMPLETE], ["execution-succeeded"],
                         ["structured-complete"]):
            with self.subTest(evidence=evidence):
                result = transition(executing, to_state=STATE_DONE, now_ms=2_000,
                                    reason="succeeded", evidence=evidence)
                self.assertEqual(result["code"], "review-required")
        accepted = transition(executing, to_state=STATE_DONE, now_ms=2_000,
                              reason="task-accepted", evidence=[EV_TASK_ACCEPTED])
        self.assertTrue(accepted["allowed"], accepted)
        self.assertEqual(accepted["updates"]["state"], STATE_DONE)
        self.assertIsNotNone(accepted["updates"]["closed_at"])

    def test_terminal_cannot_be_reopened(self):
        for state in TERMINAL_STATES:
            with self.subTest(state=state):
                closed = row(state=state, closed_at=1_000)
                reopened = transition(closed, to_state=STATE_HANDLING,
                                      now_ms=2_000, reason="reopen")
                self.assertEqual(reopened["code"], "already-terminal")


if __name__ == "__main__":
    unittest.main()

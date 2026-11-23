"""S4 后续责任纯规则：状态、关闭/转交证据、失效判定与发送前校验。

零 I/O。输入输出直接用 responsibilities 持久行字段（state/deadline/
budget_used/max_budget/next_check_at/condition_key），兼容旧 deadline_at 与
嵌套 budget。ACK/受理/空 inbox/执行 succeeded 不是业务完成；转交与关闭不授予
写权；封存 attempt 后修改交付物须新建 attempt。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .rules import ATTEMPT_ACTIVE, ATTEMPT_TERMINAL, TASK_STATUSES, TASK_TERMINAL

STATE_HANDLING = "handling"
STATE_WAITING_INPUT = "waiting_input"
STATE_WAITING_EXECUTION = "waiting_execution"
STATE_WAITING_DEPENDENCY = "waiting_dependency"
STATE_AWAITING_REVIEW = "awaiting_review"
STATE_WAITING_HUMAN = "waiting_human"
STATE_HANDED_OVER = "handed_over"
STATE_DONE = "done"
STATE_CANCELLED = "cancelled"

STATES = frozenset({
    STATE_HANDLING, STATE_WAITING_INPUT, STATE_WAITING_EXECUTION,
    STATE_WAITING_DEPENDENCY, STATE_AWAITING_REVIEW, STATE_WAITING_HUMAN,
    STATE_HANDED_OVER, STATE_DONE, STATE_CANCELLED})
TERMINAL_STATES = frozenset({STATE_HANDED_OVER, STATE_DONE, STATE_CANCELLED})
OPEN_STATES = STATES - TERMINAL_STATES

# 终态只能幂等留在原状态；waiting_execution 默认不得跳过待核收直达 done，
# 唯一例外由 transition 内部按权威任务验收证据 EV_TASK_ACCEPTED 放行（真实
# verify normal_effective accepted 可直接终结业务推进责任）。
ALLOWED_TRANSITIONS = {s: (OPEN_STATES | TERMINAL_STATES) - {s}
                       for s in OPEN_STATES}
ALLOWED_TRANSITIONS[STATE_WAITING_EXECUTION] = {
    STATE_WAITING_DEPENDENCY, STATE_AWAITING_REVIEW, STATE_WAITING_HUMAN,
    STATE_HANDED_OVER, STATE_CANCELLED}
ALLOWED_TRANSITIONS[STATE_WAITING_HUMAN] = {
    STATE_HANDLING, STATE_HANDED_OVER, STATE_DONE, STATE_CANCELLED}
ALLOWED_TRANSITIONS |= {s: set() for s in TERMINAL_STATES}

EV_BUSINESS_COMPLETE = "business-complete"
EV_TASK_ACCEPTED = "task-accepted-verified"
EV_TASK_CANCELLED = "task-cancelled"
EV_MANUAL_RELEASED = "manual-released"
CLOSING_EVIDENCE = frozenset({EV_BUSINESS_COMPLETE, EV_TASK_ACCEPTED})
MODE_MANAGED, MODE_COMMUNICATION = "managed", "communication"


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _int(value):
    return value if isinstance(value, int) and not isinstance(value, bool) \
        and value > 0 else None


def _evidence(value):
    return {value} if isinstance(value, str) else {
        item for item in (value or ()) if isinstance(item, str)}


def _budget(record):
    nested = record.get("budget") if isinstance(record.get("budget"), dict) else {}
    used, cap = record.get("budget_used"), record.get("max_budget")
    return (int(used if isinstance(used, int) else nested.get("checks") or 0),
            cap if isinstance(cap, int) else nested.get("max_checks"))


def responsibility_identity(*, kind, project=None, task_id=None, attempt_id=None,
                            input_id=None, session_id=None, message_id=None,
                            condition=None, condition_version=None,
                            now_ms=None, observed_at=None) -> str:
    """稳定逻辑去重身份（持久行 condition_key）；观察时间不参与哈希。"""
    if not _text(kind):
        raise ValueError("responsibility identity 需要非空 kind")
    fields = {"kind": kind.strip(), "project": project, "task_id": task_id,
              "attempt_id": attempt_id, "input_id": input_id,
              "session_id": session_id, "message_id": message_id,
              "condition": condition, "condition_version": condition_version}
    fields = {k: None if v is None else str(v) for k, v in fields.items()}
    if all(v is None for k, v in fields.items() if k != "kind"):
        raise ValueError("responsibility identity 缺少稳定判别字段")
    text = json.dumps(fields, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
    return "R-" + hashlib.sha256(text.encode()).hexdigest()[:20]


def closes_business(evidence: Any = ()) -> bool:
    """只有验收通过/结构化业务完成可关闭业务责任。"""
    return bool(CLOSING_EVIDENCE & _evidence(evidence))


def transition(record, *, to_state, now_ms, reason, actor="system",
               successor_id=None, successor_owner=None, evidence=(),
               wait_kind=None, wait_condition=None, next_check_at=None) -> dict:
    """纯状态推进：done/cancelled 需对应证据，转交需后继身份与责任人。

    人工 hold 只限制**恢复为开放态**（handling/waiting_*/awaiting_review）：必须
    ``actor="human"`` 且带 ``manual-released`` 证据。终态关闭（done/cancelled/
    handed_over）不经过该 hold，但仍须各自的权威证据门——``done`` 需验收/结构化
    业务完成，``cancelled`` 需真实 task 取消，``handed_over`` 需后继身份与责任人。
    这样 task 已真实 accepted/cancelled 时，waiting_human 上的推进责任可以被关闭，
    而不会因“人工等待”永久 open。终态不可重开、不授予写权。
    """
    state = record.get("state") if isinstance(record, dict) else None
    effects = {"closed": False, "successor_persist_required": False,
               "grants_write": False, "delegates_ack": False,
               "stopped_external_execution": False, "wake_allowed": False}

    def reject(code):
        return {"allowed": False, "code": code, "updates": {}, "effects": effects}

    if not isinstance(record, dict) or state not in STATES \
            or to_state not in STATES:
        return reject("state-unknown")
    if not _text(reason):
        return reject("reason-required")
    if state == to_state:
        return {"allowed": True, "code": "no-op", "updates": {},
                "effects": effects}
    if record.get("closed_at") is not None or state in TERMINAL_STATES:
        return reject("already-terminal")
    kinds = _evidence(evidence)
    # 人工 hold 只拦“恢复为开放态”；终态关闭交给下面的权威证据门
    # （done 需 closes_business，cancelled 需 task-cancelled，handed_over 需
    # 后继身份），否则真实 accepted/cancelled 任务上的等待责任会永久 open。
    if (state == STATE_WAITING_HUMAN and to_state in OPEN_STATES
            and (actor != "human" or EV_MANUAL_RELEASED not in kinds)):
        return reject("manual-control-holds")
    # waiting_execution → done：只有权威任务验收（EV_TASK_ACCEPTED）可直接终结
    # 业务推进责任；EV_BUSINESS_COMPLETE（结构化业务完成/worker 自报成功）仍须
    # 先经 awaiting_review 由协调者核收。
    # waiting_execution → done：默认必须先经 awaiting_review 核收（review-required）。
    # 唯一例外是权威任务验收证据 EV_TASK_ACCEPTED（真实 verify normal_effective
    # accepted）可直接终结业务推进责任；EV_BUSINESS_COMPLETE/worker succeeded
    # 仍必须经过核收，不得业务完成。
    task_accepted = EV_TASK_ACCEPTED in kinds
    if state == STATE_WAITING_EXECUTION and to_state == STATE_DONE \
            and not task_accepted:
        return reject("review-required")
    if to_state not in ALLOWED_TRANSITIONS[state] and not (
            state == STATE_WAITING_EXECUTION and to_state == STATE_DONE
            and task_accepted):
        return reject("transition-not-allowed")
    if to_state == STATE_DONE and not closes_business(kinds):
        return reject("closure-evidence-required")
    if to_state == STATE_CANCELLED and EV_TASK_CANCELLED not in kinds:
        return reject("cancellation-evidence-required")
    if to_state == STATE_HANDED_OVER:
        if not _text(successor_id) or not _text(successor_owner):
            return reject("successor-required" if not _text(successor_id)
                          else "successor-owner-required")
        if successor_id.strip() == record.get("id"):
            return reject("successor-invalid")
    if to_state == STATE_WAITING_HUMAN and not _text(record.get("owner")):
        return reject("manual-owner-required")

    terminal = to_state in TERMINAL_STATES
    updates = {"state": to_state, "updated_at": now_ms,
               "closed_at": now_ms if terminal else None,
               "close_reason": reason if terminal else None,
               "revision": int(record.get("revision") or 0) + 1,
               "wait_kind": wait_kind or record.get("wait_kind") or to_state,
               "wait_condition": wait_condition or reason,
               "next_check_at": next_check_at if next_check_at is not None
               else record.get("next_check_at") or now_ms}
    if terminal:
        effects["closed"] = True
        if to_state == STATE_HANDED_OVER:
            updates |= {"successor_id": successor_id.strip(),
                        "close_reason": "transferred"}
            effects["successor_persist_required"] = True
    effects["wake_allowed"] = to_state == STATE_AWAITING_REVIEW
    return {"allowed": True, "code": "ok", "updates": updates,
            "effects": effects}


def _wait_view(state, kind, now_ms, condition=None, wake=False, resume=False,
               next_at=None, evidence=None):
    return {"state": state, "wait_kind": kind, "wait_condition": condition,
            "wake_allowed": wake, "auto_resume": resume,
            "next_check_at": now_ms if next_at is None else next_at,
            "closure_evidence": evidence}


def derive_wait(*, now_ms, task_status=None, task_verified=False,
                manual_paused=False, stop_requested=False, dependency=None,
                cooldown_until=None, worker_active=None, input_pending=None,
                review_ready=False) -> dict:
    """把控制/依赖/冷却观察映射为可见等待；不解除人工控制；accepted 未验收
    仍转人工，未知 Task 状态不猜可推进，cancelled/accepted 给出关闭证据。
    """
    if task_status is not None and task_status not in TASK_STATUSES:
        return _wait_view(STATE_WAITING_HUMAN, "manual", now_ms,
                          "task-status-unknown")
    if task_status == "cancelled":
        return _wait_view(STATE_CANCELLED, "cancelled", now_ms,
                          evidence=EV_TASK_CANCELLED)
    if task_status == "accepted":
        if task_verified:
            return _wait_view(STATE_DONE, "done", now_ms,
                              evidence=EV_TASK_ACCEPTED)
        return _wait_view(STATE_WAITING_HUMAN, "manual", now_ms,
                          "verification-missing")
    if stop_requested or task_status == "paused" or manual_paused:
        return _wait_view(STATE_WAITING_HUMAN, "manual", now_ms,
                          "stop-requested" if stop_requested else "manual-pause")
    cooldown = _int(cooldown_until)
    if cooldown is not None and cooldown > now_ms:
        return _wait_view(STATE_WAITING_DEPENDENCY, "dependency", now_ms,
                          f"cooldown-until:{cooldown}", next_at=cooldown)
    if dependency not in (None, "", False):
        return _wait_view(STATE_WAITING_DEPENDENCY, "dependency", now_ms,
                          str(dependency))
    if review_ready:
        return _wait_view(STATE_AWAITING_REVIEW, "review", now_ms, wake=True)
    if input_pending:
        return _wait_view(STATE_WAITING_INPUT, "input-pending", now_ms)
    if worker_active:
        return _wait_view(STATE_WAITING_EXECUTION, "worker-execution", now_ms)
    return _wait_view(STATE_HANDLING, "ready", now_ms, resume=True)


def staleness(record, *, current) -> dict:
    """晚到回报/旧 ACK 失效判定：归属无法证明或已变化时只留历史并重查。"""
    current = current if isinstance(current, dict) else {}

    def result(status, code):
        fresh = status == "fresh"
        return {"status": status, "code": code, "apply": fresh,
                "recheck": not fresh, "preserve_history": True}

    attempt, generation = record.get("attempt_id"), record.get("generation")
    version = record.get("result_version")
    successor = record.get("successor_id") or record.get("successor")
    if all(v is None for v in (attempt, generation, version, successor)):
        return result("unconfirmed", "attribution-unrecorded")
    if _text(attempt):
        if not _text(current.get("current_attempt_id")):
            return result("unconfirmed", "current-unresolved")
        if current["current_attempt_id"] != attempt:
            return result("stale", "superseded-current-attempt")
    if generation is not None:
        cur_gen = current.get("generation")
        if isinstance(cur_gen, bool) or not isinstance(cur_gen, int):
            return result("unconfirmed", "generation-unresolved")
        if cur_gen != generation:
            return result("stale", "generation-changed")
    cur_ver = current.get("result_version")
    if version is not None:
        if not _text(cur_ver):
            return result("unconfirmed", "current-result-version-unresolved")
        if cur_ver != version:
            return result("stale", "result-version-changed")
    elif _text(cur_ver):
        return result("unconfirmed", "result-version-unbound")
    if _text(successor):
        if not _text(current.get("successor_id")):
            return result("unconfirmed", "successor-unresolved")
        if current["successor_id"] != successor:
            return result("stale", "successor-changed")
    return result("fresh", "current-attribution-matches")


def deadline_decision(record, *, now_ms) -> dict:
    """期限/持久预算耗尽 → 可见人工；计数只读持久行，不重置。"""
    used, cap = _budget(record)
    deadline = _int(record.get("deadline") or record.get("deadline_at"))
    if record.get("closed_at") is not None \
            or record.get("state") in TERMINAL_STATES:
        return {"exhausted": False, "code": "already-terminal",
                "state": record.get("state"), "budget_used": used}
    reasons = [name for name, hit in (
        ("deadline", deadline is not None and now_ms >= deadline),
        ("budget", cap is not None and used >= cap)) if hit]
    if not reasons:
        return {"exhausted": False, "code": "budget-available",
                "budget_used": used, "max_budget": cap, "reasons": []}
    code = ("deadline-and-budget-exhausted" if len(reasons) == 2
            else f"{reasons[0]}-exhausted")
    owner = record.get("owner")
    return {"exhausted": True, "code": code, "reasons": reasons,
            "state": STATE_WAITING_HUMAN,
            "owner": owner if _text(owner) else None, "budget_used": used,
            "max_budget": cap,
            "next_action": record.get("next_action")
            or "转明确人工责任；不重置期限/预算"}


def next_check_at(record, *, now_ms, base_ms, max_ms) -> int:
    """有界指数退避；按持久 budget_used 增长，封顶且不越过 deadline。"""
    if any(_int(v) is None for v in (now_ms, base_ms, max_ms)):
        raise ValueError("now_ms/base_ms/max_ms 必须是正整数")
    used, _ = _budget(record)
    candidate = now_ms + min(base_ms * 2 ** min(used, 6), max_ms)
    deadline = _int(record.get("deadline") or record.get("deadline_at"))
    return max(now_ms, min(candidate, deadline)) if deadline else candidate


def _material_errors(refs):
    if refs in (None, "", []):
        return []
    if not isinstance(refs, (list, tuple)):
        return [{"code": "MATERIAL_REF_INVALID"}]
    errors, seen = [], set()
    for raw in refs:
        if isinstance(raw, str):
            kind, ref, required, status = "reference", raw.strip(), False, None
        elif isinstance(raw, dict):
            kind, ref = raw.get("kind") or "reference", raw.get("ref")
            ref = ref.strip() if isinstance(ref, str) else None
            required, status = bool(raw.get("required")), raw.get("status")
        else:
            errors.append({"code": "MATERIAL_REF_INVALID"})
            continue
        if not _text(kind) or not ref:
            errors.append({"code": "MATERIAL_REF_INVALID"})
            continue
        key = (kind.strip(), ref)
        if key in seen:
            errors.append({"code": "MATERIAL_REF_DUPLICATE"})
            continue
        seen.add(key)
        if required and status in ("missing", "unknown"):
            errors.append({"code": "MATERIAL_REQUIRED_UNAVAILABLE"})
    return errors


def validate_managed_execute(declaration, *, attribution=None, attempt=None):
    """托管 execute 发送前校验（在任何 prompt 之前）。

    intent/纯通信由 application/managed_inputs 治理；非 execute 或纯通信返回
    applies=False 与显式 boundary。必填 target_session/message_id/owner/唯一 Task
    归属/唯一接续目标/期限；写权需匹配 claim 与活动 attempt。
    """
    errors = []

    def err(code):
        errors.append({"code": code})

    if not isinstance(declaration, dict):
        return {"ok": False, "applies": True, "mode": None, "boundary": None,
                "reason": "DECLARATION_INVALID",
                "errors": [{"code": "DECLARATION_INVALID"}],
                "write_authority": False}
    mode = declaration.get("mode") or MODE_MANAGED
    if mode not in (MODE_MANAGED, MODE_COMMUNICATION):
        return {"ok": False, "applies": True, "mode": mode, "boundary": None,
                "reason": "MODE_INVALID", "errors": [{"code": "MODE_INVALID"}],
                "write_authority": False}
    if declaration.get("intent") != "execute" or mode != MODE_MANAGED:
        return {"ok": True, "applies": False, "mode": mode,
                "boundary": None if mode == MODE_MANAGED else "unmanaged",
                "reason": "not-managed-execute", "errors": [],
                "write_authority": False}
    if not _text(declaration.get("target_session")):
        err("TARGET_SESSION_REQUIRED")
    if not _text(declaration.get("message_id")):
        err("MESSAGE_ID_REQUIRED")
    owner, task_id = declaration.get("owner"), declaration.get("task_id")
    successor = declaration.get("successor")
    deadline = _int(declaration.get("deadline") or declaration.get("deadline_at"))
    prepared = _int(declaration.get("prepared_at"))
    if not _text(owner):
        err("OWNER_REQUIRED")
    if not _text(task_id):
        err("TASK_REQUIRED")
    if not isinstance(attribution, dict):
        err("ATTRIBUTION_UNRESOLVED")
    else:
        tasks = attribution.get("task_ids")
        succs = attribution.get("successor_ids")
        if not isinstance(tasks, (list, tuple)) or not tasks:
            err("ATTRIBUTION_UNRESOLVED")
        elif len(tasks) > 1:
            err("ATTRIBUTION_AMBIGUOUS")
        elif _text(task_id) and task_id not in tasks:
            err("TASK_NOT_ATTRIBUTED")
        if _text(successor):
            if not isinstance(succs, (list, tuple)) or not succs:
                err("SUCCESSOR_UNRESOLVED")
            elif len(succs) > 1:
                err("SUCCESSOR_AMBIGUOUS")
            elif successor not in succs:
                err("SUCCESSOR_NOT_ATTRIBUTED")
    if not _text(successor):
        err("SUCCESSOR_REQUIRED")
    if deadline is None:
        err("DEADLINE_INVALID")
    elif prepared is not None and deadline <= prepared:
        err("DEADLINE_NOT_FUTURE")
    errors.extend(_material_errors(declaration.get("material_refs")))

    claims = bool(declaration.get("claims_write"))
    modifies = bool(declaration.get("modifies_deliverables"))
    if modifies and not claims:
        err("WRITE_CLAIM_REQUIRED")
    sealed = isinstance(attempt, dict) and (
        attempt.get("status") in ATTEMPT_TERMINAL
        or attempt.get("site_released_at") is not None)
    if isinstance(attempt, dict) and _text(task_id) and _text(
            attempt.get("task_id")) and attempt["task_id"] != task_id:
        err("ATTEMPT_TASK_MISMATCH")
    if sealed and (claims or modifies or declaration.get("write_scope")):
        err("NEW_ATTEMPT_REQUIRED")
    if claims:
        claim_owner = declaration.get("claim_owner")
        if not _text(claim_owner):
            err("WRITE_CLAIM_UNVERIFIED")
        elif claim_owner != owner:
            err("WRITE_CLAIM_UNAUTHORIZED")
        if not isinstance(attempt, dict) \
                or attempt.get("status") not in ATTEMPT_ACTIVE:
            err("WRITE_CLAIM_UNVERIFIED")
    return {"ok": not errors, "applies": True, "mode": mode,
            "boundary": "managed",
            "reason": errors[0]["code"] if errors else "managed-execute-ready",
            "errors": errors, "write_authority": claims and not errors}


def missing_responsibility_check(
    *, task, responsibilities=(), pending_notifications=0, observed_rounds=0,
    max_rounds=2, fallback_owner="manual:unassigned",
    lease_expired=None, task_next_check_at=None, now_ms=None,
) -> dict:
    """缺失责任诊断：非终态、无开放责任、无待投递通知时第二次 reconcile 登记；
    租约/next_check_at 刷新/now_ms 显式忽略，不能当闭环；须持久化 rounds。"""
    status = task.get("status") if isinstance(task, dict) else None
    if status not in TASK_STATUSES:
        return {"register": False, "reason": "task-status-unknown",
                "observed_rounds": max(0, int(observed_rounds or 0))}
    if status in TASK_TERMINAL:
        return {"register": False, "reason": "task-terminal", "task_status": status}
    task_id = task.get("id") if _text(task.get("id")) else None
    for row in responsibilities or ():
        if isinstance(row, dict) and row.get("closed_at") is None \
                and row.get("state") in OPEN_STATES \
                and (task_id is None or row.get("task_id") == task_id):
            return {"register": False, "reason": "responsibility-exists",
                    "condition_key": row.get("condition_key"),
                    "state": row.get("state")}
    if isinstance(pending_notifications, int) and pending_notifications > 0:
        return {"register": False, "reason": "notification-pending"}
    rounds = max(0, int(observed_rounds or 0)) + 1
    if rounds < max(1, int(max_rounds)):
        return {"register": False, "reason": "grace-rounds",
                "observed_rounds": rounds, "next_round": rounds + 1}
    coordinator = (task.get("coordinator_session")
                   if _text(task.get("coordinator_session")) else None)
    project = task.get("project") if _text(task.get("project")) else None
    return {
        "register": True, "reason": "missing-responsibility",
        "condition_key": responsibility_identity(
            kind="missing-responsibility", project=project, task_id=task_id,
            condition="missing-responsibility", condition_version=1),
        "state": STATE_WAITING_HUMAN,
        "owner": coordinator or fallback_owner, "observed_rounds": rounds,
        "target": "coordinator" if coordinator else "manual",
        "wake_allowed": status != "paused",
        "next_action": "登记有效推进/等待责任；重查 current 与人工控制，"
                       "不得把空通知队列或 next_check_at 刷新当作已处理"}


__all__ = ["responsibility_identity", "closes_business", "transition",
           "derive_wait", "staleness", "deadline_decision", "next_check_at",
           "validate_managed_execute", "missing_responsibility_check"]

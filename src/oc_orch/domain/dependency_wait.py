"""依赖等待结构化条件与业务健康纯规则（tasks 3.1–3.4；C13–C16、C21/C22）。

零 I/O。职责：

- 把“依赖等待”从自由字符串升级为**可评估的有限条件契约**：
  显式 Task ID 集合 + all/any 语义 + 所需有效验收状态/结果版本 + 条件版本；
- 评估返回 ``satisfied / waiting / unknown / invalid / broken``。只有可证明的
  有效验收事实才算 satisfied；缺失、循环、终止、未知与旧字符串条件都不得猜完成，
  统一转稳定人工责任；
- 满足后以（责任 id + 条件版本 + 证据版本）生成幂等复核去重键，不依赖每轮时间；
- 后继关系必须显式声明（``successor``），不按标题猜测；不把旧 rejected 自动
  accepted；
- C22 业务健康投影：区分**进程健康**与**业务进展**。单纯 heartbeat /
  next_check_at / updated_at 观察不是进展；进展只认条件满足发布的复核、责任
  转交/关闭、任务真验收、结果版本变更等可核验事实。

默认策略参数与“责任/期限/预算”形状兼容 ``domain.responsibility``（持久行字段
deadline/budget_used/next_check_at/evidence），调用方仍以该模块为准。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .responsibility import (
    OPEN_STATES,
    STATE_AWAITING_REVIEW,
    STATE_WAITING_DEPENDENCY,
    STATE_WAITING_HUMAN,
)
from .rules import TASK_STATUSES, TASK_TERMINAL

# ---- 有限条件类型 -----------------------------------------------------------
CONDITION_TASK = "task_state"
DEPENDENCY_MODES = frozenset({"all", "any"})
REQUIRE_ACCEPTED = "accepted"
REQUIRE_CANCELLED = "cancelled"
DEPENDENCY_REQUIRES = frozenset({REQUIRE_ACCEPTED, REQUIRE_CANCELLED})

# 评估结果：satisfied 满足 / waiting 继续等 / unknown 证据不够 / invalid 条件坏 /
# broken 依赖缺失或永远无法满足（取消）→ 稳定人工责任。
SATISFIED = "satisfied"
WAITING = "waiting"
UNKNOWN = "unknown"
INVALID = "invalid"
BROKEN = "broken"
EVALUATIONS = frozenset({SATISFIED, WAITING, UNKNOWN, INVALID, BROKEN})

# 证据中的结构化依赖事实段名与 schema 标识。
DEPENDENCY_KEY = "dependency"
CONDITION_SCHEMA = "oc-orch.dependency-condition.v1"
OBSERVED_KEY = "observed_count"

DEFAULT_DEPENDENCY_RECHECK_BASE_MS = 60_000
DEFAULT_DEPENDENCY_RECHECK_MAX_MS = 15 * 60_000
DEFAULT_DEPENDENCY_DEADLINE_MS = 24 * 3600 * 1000

# C22 业务状态枚举。
STATE_DONE = "done"
STATE_EXECUTING = "executing"
STATE_PAUSED = "paused"
STATE_WAITING_DEP = "waiting_dependency"
STATE_UNKNOWN = "unknown"
STATE_NEEDS_HUMAN = "needs_human"
STATE_ACTIONABLE = "actionable"
STATE_NO_PROGRESS = "no_progress"

BUSINESS_STATES = frozenset({
    STATE_DONE, STATE_EXECUTING, STATE_PAUSED, STATE_WAITING_DEP,
    STATE_UNKNOWN, STATE_NEEDS_HUMAN, STATE_ACTIONABLE, STATE_NO_PROGRESS,
})

# 有业务推进含义的“进展事实”kind（事件供独立邮件出口去重使用）。
ACTIONABLE_KINDS = frozenset({
    "task-unaccepted-no-progress", "dependency-unresolvable", "dependency-invalid",
    "dependency-deadline-exhausted", "task-status-unknown", "verification-missing",
})

__all__ = [
    "CONDITION_TASK", "DEPENDENCY_MODES", "REQUIRE_ACCEPTED", "REQUIRE_CANCELLED",
    "DEPENDENCY_REQUIRES", "SATISFIED", "WAITING", "UNKNOWN", "INVALID", "BROKEN",
    "EVALUATIONS", "DEPENDENCY_KEY", "CONDITION_SCHEMA", "OBSERVED_KEY",
    "DEFAULT_DEPENDENCY_RECHECK_BASE_MS", "DEFAULT_DEPENDENCY_RECHECK_MAX_MS",
    "DEFAULT_DEPENDENCY_DEADLINE_MS",
    "STATE_DONE", "STATE_EXECUTING", "STATE_PAUSED", "STATE_WAITING_DEP",
    "STATE_UNKNOWN", "STATE_NEEDS_HUMAN", "STATE_ACTIONABLE", "STATE_NO_PROGRESS",
    "BUSINESS_STATES", "ACTIONABLE_KINDS",
    "normalize_condition", "validate_condition", "condition_identity",
    "describe_condition", "evaluate_task_condition", "dedupe_review_key",
    "detect_cycle", "extract_dependency_condition", "record_condition_key",
]


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---- 条件规范化 / 校验 ------------------------------------------------------
_COND_FIELDS = ("type", "require", "task_ids", "mode", "result_version",
                "revision", "successor")


def normalize_condition(value: Any) -> dict:
    """规范化条件为唯一权威形状（排序键、task_ids 去重有序、默认补全）。

    非法条件抛 ``ValueError``（错误码列表见 ``validate_condition``）。注意
    ``task_ids`` 为空、重复、非字符串、mode/require 非法、result_version 形状
    错误等都属于“结构化无效”而不是可等待状态。
    """
    errors = validate_condition(value)
    if errors:
        raise ValueError(",".join(errors))
    cond = dict(value)
    mode = cond.get("mode") or "all"
    # 排序去重：all/any 集合的目标顺序不参与身份与事件键，避免两种书写顺序
    # （[T-2,T-3] 与 [T-3,T-2]）生成不同 condition_identity/事件。
    task_ids = sorted({str(t).strip() for t in cond["task_ids"]})
    revision = cond.get("revision", 1)
    norm_rev = 1 if revision is None else int(revision)
    if norm_rev < 1:
        raise ValueError("CONDITION_REVISION_INVALID")
    out = {
        "type": CONDITION_TASK, "require": cond["require"], "mode": mode,
        "task_ids": task_ids,
        "result_version": cond.get("result_version"),
        "revision": norm_rev,
        "successor": cond.get("successor"),
    }
    return {k: out[k] for k in _COND_FIELDS}


def validate_condition(value: Any) -> list[str]:
    """返回错误码列表；空列表表示结构合法（合法≠已满足）。"""
    if not isinstance(value, dict):
        return ["CONDITION_INVALID"]
    errors: list[str] = []
    if value.get("type") != CONDITION_TASK:
        errors.append("CONDITION_TYPE_UNSUPPORTED")
    require = value.get("require")
    if require not in DEPENDENCY_REQUIRES:
        errors.append("CONDITION_REQUIRE_INVALID")
    mode = value.get("mode")
    if mode is not None and mode not in DEPENDENCY_MODES:
        errors.append("CONDITION_MODE_INVALID")
    ids = value.get("task_ids")
    if not isinstance(ids, (list, tuple)) or not ids:
        errors.append("CONDITION_TASK_IDS_REQUIRED")
    else:
        seen: set[str] = set()
        for raw in ids:
            if not _text(raw) or raw.strip() in seen:
                errors.append("CONDITION_TASK_IDS_DUPLICATE")
                break
            seen.add(raw.strip())
    rv = value.get("result_version")
    if rv is not None and not _text(rv):
        errors.append("CONDITION_RESULT_VERSION_INVALID")
    revision = value.get("revision")
    if revision is not None and (not _int(revision) or revision <= 0):
        errors.append("CONDITION_REVISION_INVALID")
    successor = value.get("successor")
    if successor is not None and not _text(successor):
        errors.append("CONDITION_SUCCESSOR_INVALID")
    return errors


def condition_identity(value: Any) -> str:
    """稳定条件身份：含类型/要求/目标集合/语义/版本/后继，不含观测时间。"""
    cond = normalize_condition(value)
    return "DC-" + _sha256(_json_dumps(cond))[:20]


def describe_condition(value: Any) -> str:
    """可展示的条件摘要（wait_condition 持久列用；结构化条件在 evidence）。"""
    try:
        cond = normalize_condition(value)
    except ValueError:
        return "dependency-condition-invalid"
    mode = cond["mode"]
    targets = ",".join(cond["task_ids"])
    rv = cond.get("result_version")
    rev = cond.get("revision")
    version = f":rv{rv}" if rv else ""
    successor = f"->{cond['successor']}" if cond.get("successor") else ""
    return (f"dep:{cond['require']}:{mode}:{targets}{version}:rev{rev}{successor}")


def record_condition_key(rid: str, condition: dict) -> str:
    """条件去重键（挂到责任证据用）：按条件身份哈希，稳定且不随轮次变化。"""
    if not _text(rid):
        raise ValueError("condition key 需要责任 id")
    try:
        cid = condition_identity(condition)
    except ValueError as exc:
        raise ValueError(f"condition-key 需要结构化条件：{exc}") from exc
    return f"dep-wait:{rid}:{cid}"


def extract_dependency_condition(row: Any) -> dict | None:
    """从责任行 evidence.dependency.condition 读取结构化条件；缺失返回 None。"""
    if not isinstance(row, dict):
        return None
    evidence = row.get("evidence")
    if not isinstance(evidence, dict):
        return None
    dep = evidence.get(DEPENDENCY_KEY)
    if not isinstance(dep, dict):
        return None
    condition = dep.get("condition")
    if not isinstance(condition, dict):
        return None
    return condition


# ---- 条件评估 ---------------------------------------------------------------
def _fact_task_status(fact: Any) -> tuple[str, str]:
    """任务事实状态：未知/缺失返回 (UNKNOWN/BROKEN, reason)；否则 (status, None)。"""
    if not isinstance(fact, dict) or not fact.get("exists"):
        return (BROKEN, "dependency-missing" if isinstance(fact, dict)
                else "dependency-fact-invalid")
    status = fact.get("status")
    if status not in TASK_STATUSES:
        return (UNKNOWN, "task-status-unknown")
    return (status, None)


def _dep_evaluation(task_id: str, fact: Any, require: str,
                    expected_version: str | None) -> dict:
    """单个依赖任务的评估。返回 ``{"key", "status", "reason", "verifiable"}``。"""
    base = {"key": task_id}
    status, reason = _fact_task_status(fact)
    if status in (BROKEN, UNKNOWN):
        return {**base, "status": status, "reason": reason, "verifiable": False}
    if status in TASK_TERMINAL:
        if status == "cancelled":
            if require == REQUIRE_CANCELLED and fact.get("closed_at") is not None:
                return {**base, "status": SATISFIED, "reason": "cancelled",
                        "verifiable": True}
            return {**base, "status": BROKEN, "reason": "dependency-cancelled",
                    "verifiable": False}
        # status == 'accepted'
        if require == REQUIRE_ACCEPTED:
            if not fact.get("effective"):
                return {**base, "status": UNKNOWN,
                        "reason": "accepted-without-verification",
                        "verifiable": False}
            version = fact.get("result_version")
            if expected_version is not None and expected_version != version:
                return {**base, "status": UNKNOWN, "reason": "result-version-mismatch",
                        "verifiable": False}
            return {**base, "status": SATISFIED, "reason": "accepted-verified",
                    "verifiable": True}
        return {**base, "status": BROKEN, "reason": "dependency-accepted",
                "verifiable": False}
    # 未终结任务：接受类要求仍在等待；取消类要求等待取消事实。
    if require == REQUIRE_CANCELLED:
        return {**base, "status": WAITING, "reason": "waiting-cancellation",
                "verifiable": False}
    return {**base, "status": WAITING, "reason": "waiting-acceptance",
            "verifiable": False}


def evaluate_task_condition(condition: Any, facts: Any) -> dict:
    """对结构化条件求值。

    ``facts``: ``{task_id: {exists, status, accepted_at, closed_at,
    result_version, effective, verification}}``。返回：:

        {"status": satisfied|waiting|unknown|invalid|broken, "reason": str,
         "per_task": {task_id: {...}}, "evidence_version": str|None}

    满足的证据版本是稳定的（剔除时间类字段），用于复核去重；无法满足/未知时
    为 None。旧字符串条件（非 dict）一律 ``invalid``。
    """
    if not isinstance(condition, dict):
        return {"status": INVALID, "reason": "condition-not-structured",
                "per_task": {}, "evidence_version": None}
    errors = validate_condition(condition)
    if errors:
        return {"status": INVALID, "reason": ",".join(errors),
                "per_task": {}, "evidence_version": None}
    facts = facts if isinstance(facts, dict) else {}
    cond = normalize_condition(condition)
    per_task: dict[str, dict] = {}
    satisfied_facts: dict[str, dict] = {}
    for task_id in cond["task_ids"]:
        evaluated = _dep_evaluation(task_id, facts.get(task_id),
                                    cond["require"],
                                    cond.get("result_version"))
        per_task[task_id] = evaluated
        if evaluated["status"] == SATISFIED:
            fact = facts.get(task_id) or {}
            satisfied_facts[task_id] = {
                "status": fact.get("status"),
                "result_version": fact.get("result_version"),
                "accepted_at": fact.get("accepted_at"),
            }
    statuses = [v["status"] for v in per_task.values()]
    mode = cond["mode"]
    if mode == "all":
        if any(s in (BROKEN, INVALID) for s in statuses):
            status, reason = BROKEN, "dependency-broken"
        elif any(s == UNKNOWN for s in statuses):
            status, reason = UNKNOWN, "dependency-unknown"
        elif all(s == SATISFIED for s in statuses):
            status, reason = SATISFIED, "all-dependencies-satisfied"
        else:
            status, reason = WAITING, "dependencies-pending"
    else:  # any
        if any(s == SATISFIED for s in statuses):
            status, reason = SATISFIED, "any-dependency-satisfied"
        elif any(s in (BROKEN, INVALID) for s in statuses):
            status, reason = BROKEN, "dependency-broken"
        elif any(s == UNKNOWN for s in statuses):
            status, reason = UNKNOWN, "dependency-unknown"
        else:
            status, reason = WAITING, "dependencies-pending"
    evidence_version = None
    if status == SATISFIED and satisfied_facts:
        evidence_version = ("ev-" + _sha256(_json_dumps(satisfied_facts))[:16])
    return {"status": status, "reason": reason, "per_task": per_task,
            "evidence_version": evidence_version}


def dedupe_review_key(rid: str, condition: Any, evidence_version: str) -> str:
    """复核事实去重键：责任 id + 条件身份 + 证据版本；不含时间，跨重启稳定。"""
    if not _text(rid):
        raise ValueError("dedupe key 需要责任 id")
    if not _text(evidence_version):
        raise ValueError("dedupe key 需要证据版本（satisfied 才有）")
    cid = condition_identity(condition)
    payload = _json_dumps({"responsibility_id": rid, "condition": cid,
                           "evidence_version": evidence_version})
    return "dep-review:" + _sha256(payload)[:32]


# ---- 依赖循环检测 -----------------------------------------------------------
def detect_cycle(edges: Any, start: str) -> list[str] | None:
    """``start`` 可达子图中的环路检测；返回环上节点列表或 None。

    ``edges``: ``{node_id: [依赖节点, ...]}``（缺失节点视为无出边）。纯函数，
    不扫描全局 session：调用方只喂“同一任务已登记的显式依赖边”。
    """
    if not _text(start):
        return None
    edges = edges if isinstance(edges, dict) else {}
    state: dict[str, int] = {}  # 0=未访问 1=路径中 2=已完成
    path: list[str] = []

    def visit(node: str) -> tuple[bool, list[str]]:
        mark = state.get(node)
        if mark == 1:
            cut = path.index(node)
            return True, path[cut:] + [node]
        if mark == 2:
            return False, []
        state[node] = 1
        path.append(node)
        for nxt in (edges.get(node) or ()):
            if not _text(nxt):
                continue
            found, cycle = visit(str(nxt))
            if found:
                return True, cycle
        path.pop()
        state[node] = 2
        return False, []

    found, cycle = visit(start)
    return cycle if found else None


# ---- 业务健康投影（C21/C22 纯规则） ----------------------------------------
def wait_health_projection(
    *, task: Any, attempt: Any = None, responsibilities: Any = (),
    heartbeat_at: int | None = None, now: int | None = None,
    heartbeat_stale_ms: int = 300_000,
    business_event_at: int | None = None, latest_result_version: str | None = None,
    prior_result_version: str | None = None,
    provider_blocked: bool = False, pending_notifications: int = 0,
    evidence_coverage: bool = True,
) -> dict:
    """把台账事实投影为“进程健康 vs 业务进展”的显式业务状态。

    规则（C21/C22）：

    - **进程健康**只来自 heartbeat 新鲜度；``business_event_at``（责任创建/
      复核发布/转交/关闭/任务真验收/结果版本变更等事实时刻）才是业务进展。
      next_check_at/updated_at 刷新、SSE 触发、重复告警都不算进展；
    - 显式暂停（task paused）与 provider 阻塞、未知执行分开显示；
    - 无进展依据 → ``no_progress``（复用缺失责任诊断，不靠“继续干活”保活）；
    - ``evidence_coverage=False``（归属/验收证据不足）→ ``unknown`` 且覆盖标记
      可见，不猜完成。
    """
    facts: dict[str, Any] = {}
    task_status = None
    if isinstance(task, dict):
        task_status = task.get("status")
        facts["task_status"] = task_status
    if task_status not in TASK_STATUSES:
        return _projection(UNKNOWN, ["task-status-unknown"],
                           "任务状态未知：登记可见诊断，不猜可推进", facts,
                           heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at, evidence_coverage=False)
    reasons: list[str] = []
    responsibility_states = [
        r.get("state") for r in (responsibilities or ())
        if isinstance(r, dict) and r.get("closed_at") is None
    ]
    facts["responsibility_states"] = sorted(responsibility_states)

    if task_status in ("accepted", "cancelled"):
        if task_status == "accepted" and not _effective_acceptance_fact(task, attempt):
            return _projection(STATE_NEEDS_HUMAN, ["accepted-without-verification"],
                               "任务标 accepted 但缺少可核验验收证据：登记人工诊断，"
                               "不把字符串 accepted 当有效验收", facts,
                               heartbeat_at, now, heartbeat_stale_ms,
                               business_event_at, evidence_coverage=False)
        return _projection(STATE_DONE, [], "任务已按权威事实收口", facts,
                           heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at, evidence_coverage=True)
    if task_status == "paused":
        return _projection(STATE_PAUSED, ["task-paused"],
                           "显式暂停：保留在途事实，不自动唤醒协调", facts,
                           heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at, evidence_coverage=True)
    if STATE_WAITING_HUMAN in responsibility_states:
        return _projection(STATE_NEEDS_HUMAN, ["waiting-human-responsibility"],
                           "存在明确人工责任：提供 owner/下一步，不自动解除",
                           facts, heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at)
    if STATE_AWAITING_REVIEW in responsibility_states:
        return _projection(STATE_ACTIONABLE, ["review-ready"],
                           "存在待核收接续：经统一守卫路由当前合法协调者",
                           facts, heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at)
    if provider_blocked:
        return _projection(STATE_UNKNOWN, ["provider-blocked"],
                           "provider 执行条件受阻：保留原身份与预算，不重投",
                           facts, heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at)
    if not evidence_coverage:
        return _projection(STATE_UNKNOWN, ["evidence-coverage-missing"],
                           "执行/验收证据覆盖不足：保持未知，不猜完成",
                           facts, heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at, evidence_coverage=False)
    has_open_attempt = bool(
        isinstance(attempt, dict) and attempt.get("id")
        and attempt.get("status") not in TASK_TERMINAL
        and (attempt.get("site_released_at") or attempt.get("closed_at")) is None)
    if has_open_attempt:
        state = (STATE_EXECUTING if attempt.get("status") in (
            "running", "dispatched", "creating", "dispatching",
            "dispatch_unknown", "blocked_permission", "blocked_form",
            "provider_retry", "idle_unconfirmed", "stop_requested")
            else STATE_WAITING_DEP)
        return _projection(state, ["open-attempt"] if state == STATE_EXECUTING
                           else ["waiting-execution"],
                           "当前尝试仍在推进" if state == STATE_EXECUTING
                           else "当前尝试等待执行结果",
                           facts, heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at)
    if STATE_WAITING_DEP in responsibility_states:
        return _projection(STATE_WAITING_DEP, ["waiting-dependency"],
                           "依赖条件仍在等待：按 deadline/预算有界复查",
                           facts, heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at)
    result_version_changed = bool(
        latest_result_version and prior_result_version
        and latest_result_version != prior_result_version)
    if business_event_at is not None or result_version_changed:
        return _projection(STATE_ACTIONABLE, ["business-progress-fact"],
                           "存在可核验业务进展事实：安排有效接续",
                           facts, heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at)
    if isinstance(pending_notifications, int) and pending_notifications > 0:
        return _projection(STATE_ACTIONABLE, ["notification-pending"],
                           "存在适用当前归属的未结通知：按既有 ACK/发送语义处理",
                           facts, heartbeat_at, now, heartbeat_stale_ms,
                           business_event_at)
    return _projection(
        STATE_NO_PROGRESS, ["no-open-responsibility"],
        "已托管未完成工作缺少有效推进/等待责任：复用缺失责任机制登记待办；"
        "空通知队列/租约刷新不等同于已处理",
        facts, heartbeat_at, now, heartbeat_stale_ms, business_event_at)


def _projection(state: str, reasons: list[str], next_action: str, facts: dict,
                heartbeat_at, now, heartbeat_stale_ms, business_event_at,
                evidence_coverage: bool = True) -> dict:
    fresh = (isinstance(heartbeat_at, int) and not isinstance(heartbeat_at, bool)
             and isinstance(now, int) and now < heartbeat_at + int(heartbeat_stale_ms))
    return {
        "state": state,
        "process_healthy": fresh if heartbeat_at is not None else None,
        "business_progress": bool(business_event_at is not None),
        "blocked": state in (STATE_NEEDS_HUMAN, STATE_UNKNOWN, STATE_WAITING_DEP,
                             STATE_NO_PROGRESS) or bool(reasons),
        "reasons": list(reasons),
        "next_action": next_action,
        "facts": facts,
        "evidence_coverage": bool(evidence_coverage),
    }


def _effective_acceptance_fact(task: Any, attempt: Any) -> bool:
    """任务 accepted 是否具备可核验的验收证据（与 adapter 快照同口径）。

    有效验收 = 任务真实 accepted + **当前 attempt 归属** + 结构化 verification
    （schema v1、result=pass、非 force）+ **收集结果版本与验收绑定一致**
    （``result_snapshot.result_version == metadata.result.version``）；任一缺失
    或版本不一致 → False。旧台账只有字符串 accepted 而无验证记录 → False；
    force 不算正常有效交付（北极星口径）。
    """
    if not isinstance(task, dict) or not isinstance(attempt, dict):
        return False
    if not attempt.get("id") or attempt.get("id") != task.get("current_attempt_id"):
        return False  # 非当前 attempt 的旧验收证据不得当有效验收
    verification = attempt.get("verification")
    if not isinstance(verification, dict):
        return False
    if verification.get("schema") != "oc-orch.verification.v1":
        return False
    if verification.get("result") != "pass":
        return False
    if bool(verification.get("forced")):
        return False  # force 不算正常有效交付（北极星口径）
    stored = ((attempt.get("metadata") or {}).get("result") or {}).get("version")
    if not isinstance(stored, str) or not stored:
        return False
    rs = verification.get("result_snapshot")
    bound = None
    if isinstance(rs, dict) and isinstance(rs.get("result_version"), str) \
            and rs["result_version"]:
        bound = rs["result_version"]
    elif isinstance(verification.get("result_version"), str) \
            and verification["result_version"]:
        bound = verification["result_version"]
    return bound == stored  # 收集结果版本与验收绑定必须一致


def actionable_event(*, kind: str, task_id: str, project: str = "",
                     reason: str, owner: str = "manual:unassigned",
                     next_action: str, condition_key: str | None = None,
                     condition_identity: str | None = None,
                     at_ms: int | None = None) -> dict:
    """C22 结构化事件（供独立邮件出口去重；不发送、不解析回复执行控制）。

    ``kind`` 从 ``ACTIONABLE_KINDS`` 选择；事件键稳定（不含每轮 now），避免
    重复观察生成新事件。
    """
    if kind not in ACTIONABLE_KINDS:
        kind = "task-unaccepted-no-progress"
    payload = {
        "kind": kind, "task_id": task_id, "project": project,
        "reason": reason, "owner": owner, "next_action": next_action,
        "condition_key": condition_key, "condition": condition_identity,
    }
    event_key = "event:" + _sha256(_json_dumps(payload))[:24]
    out = dict(payload, event_key=event_key)
    if isinstance(at_ms, int) and not isinstance(at_ms, bool):
        out["at_ms"] = at_ms
    return out
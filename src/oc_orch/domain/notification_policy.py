"""S1 通知有效性纯规则（wake-delivery-control Requirement 1 / design D1）。

零 I/O、零台账写；不修改通知、不写 ACK/结果版本/current/写权。决策结果由
上层（ledger/supervisor）落地：

- ``send``：允许作为正常协调唤醒投递。无 task_id/attempt_id 的已登记旧全局
  通知按 ``legacy-unscoped`` 兼容发送（不推断不存在的 generation）。
- ``defer``：保持 pending、暂不发送；保留为可见待决责任，供有界补证（S2+）。
  不代表已消费、不烧预算、不入 ACK。
- ``suppress``：通知已过时/条件已解除；上层记 non-ACK 的 dead/resolved
  （保留原 ID、原因与审计），不伪装成 ACK，不消耗投递预算。
- ``cleanup_required``：旧执行仍有副作用/停止未确认/写权未释放，需要去重
  可见清理责任；由 ledger 以 attempt + 清理条件形成去重条目，绝不因过滤
  通知而停止观察旧执行或释放写权。

kind 类别（枚举自 runtime/supervisor.py、application/collection.py、
entrypoints/cli.py 的实际产生端）：

- current_only：必须匹配任务 current attempt 且任务未结束，才作为“当前
  进展/状态”唤醒；旧代次/任务结束/条件已解除一律抑制，人工暂停转为 defer。
- cleanup：旧执行清理责任，不受代次与人工结束自动抑制；仅凭明确清理证据
  （site_released_at 且无明确活动矛盾）解除。
- coordinator：按目标 session 与租约/故障条件路由，不套用 attempt 代次过滤。
- unknown：未登记 kind，保留诊断并转为可见待决责任（defer）。

归属不变量：

- 已登记 kind 的 task_id/attempt_id 皆空 → ``legacy-unscoped`` send（仅无
  attempt 锚；带 attempt 却缺归属仍 defer；cleanup/coordinator 亦然）。
- 带 attempt_id 却无法证明归属（attempt 缺失/ID 不符/与 task 不一致）→ defer。
- 条件/状态“已知”由 domain.rules 的 ATTEMPT_STATUSES / TASK_STATUSES 枚举
  验证：枚举外的字符串视为 unknown 而非已知，条件解除不凭缺字段/未知值猜测。
- task.current_attempt_id 缺失不能视为已匹配/未取代已知 attempt → defer。

动态构造的 kind（如 result_ready 出自 results.CLASSIFICATION_TABLE、
f"coordinator_{outcome}" 属 alert）无法从 callsite 静态追溯，覆盖限度详见
tests/test_notification_policy.py 的 AST 提取与手工盘点表。
"""

from __future__ import annotations

from typing import Any

from .rules import ATTEMPT_STATUSES, TASK_STATUSES

# ---- 类别 ------------------------------------------------------------------
CATEGORY_CURRENT_ONLY = "current_only"
CATEGORY_CLEANUP = "cleanup"
CATEGORY_COORDINATOR = "coordinator"
CATEGORY_UNKNOWN = "unknown"

# 当前进展/状态：current attempt + 任务未结束才允许唤醒。
CURRENT_ONLY_KINDS = frozenset({
    "result_ready",
    "result_missing",
    "result_collect_failed",
    "execution_succeeded",
    "execution_failed",
    "execution_interrupted",
    "attempt_lost",
    "stall",
    "idle_unconfirmed",
    "provider_retry",
    "provider_blocked",
    "blocked_permission",
    "blocked_form",
    "dispatch_unknown",
    # S4 执行后接续：待核收事实与缺失责任诊断。二者都是“当前任务仍需
    # 处理”的可行动事实；任务结束/暂停/未知按既有优先级先行处理。
    "continuation_ready",
    "responsibility_missing",
})

# 任务级后续责任 kind：没有 attempt 锚时仍应放行（否则会被当作
# attempt-unresolved 永久 defer）。它们必须绑定当前 task 且任务未结束。
TASK_LEVEL_RESPONSIBILITY_KINDS = frozenset({
    "continuation_ready",
    "responsibility_missing",
})

# 旧执行清理：仍活动的 shell/子 session、未确认停止、写权冲突保留事实。
CLEANUP_KINDS = frozenset({
    "side_effects_running",
    "execution_unconfirmed",
})

# coordinator/基础设施：按目标 session 及租约/故障条件判断。
COORDINATOR_KINDS = frozenset({
    "coordinator_recovery",
    "coordinator_blocked_permission",
    "coordinator_manual_recovery",
})

ALL_KINDS = (CURRENT_ONLY_KINDS | CLEANUP_KINDS | COORDINATOR_KINDS)

# 条件类 kind → 仍“待处理”的 attempt 状态集合。attempt 状态仍属于该集合且为
# 枚举内已知 → 条件仍待处理；已离开集合且为枚举内已知 → 条件已解除（明确
# 证据）；状态缺失/枚举外 → 不得猜解除，转 defer。
CONDITION_PENDING_STATUS: dict[str, frozenset[str]] = {
    "stall": frozenset({"stalled"}),
    "idle_unconfirmed": frozenset({"idle_unconfirmed"}),
    "provider_retry": frozenset({"provider_retry"}),
    "provider_blocked": frozenset({"provider_retry"}),
    "blocked_permission": frozenset({"blocked_permission"}),
    "blocked_form": frozenset({"blocked_form"}),
    "dispatch_unknown": frozenset({"dispatch_unknown"}),
}

# ---- 动作与稳定原因码 -------------------------------------------------------
SEND = "send"
DEFER = "defer"
SUPPRESS = "suppress"
CLEANUP_REQUIRED = "cleanup_required"

# 稳定原因码（上层/审计据此保持稳定语义，不得随意改拼写）。
R_CURRENT_ONLY_ACTIVE = "current-only-active"
R_CONDITION_RESOLVED = "condition-resolved"
R_SUPERSEDED = "superseded"
R_TASK_PAUSED = "task-paused"
R_TASK_CANCELLED = "task-cancelled"
R_TASK_ACCEPTED = "task-accepted"
R_TASK_CLOSED = "task-closed"
R_SITE_RELEASED = "site-released"
R_SIDE_EFFECTS_ACTIVE = "side-effects-active"
R_STOP_UNCONFIRMED = "stop-unconfirmed"
R_WRITE_SITE_HELD = "write-site-held"
# 已登记 kind 无 task_id/attempt_id（无 attempt 锚）的 legacy 全局兼容发送。
R_LEGACY_UNSCOPED = "legacy-unscoped"
# current_only 带 task_id 但无 attempt：无法判 current/generation。
R_ATTEMPT_UNRESOLVED = "attempt-unresolved"
# task.current_attempt_id 缺失：不能视为已匹配/未取代已知 attempt。
R_NO_CURRENT = "current-unresolved"
# task.status 不在 TASK_STATUSES 枚举内（未知），不能据此判断人工控制/结束。
R_TASK_STATUS_UNKNOWN = "task-status-unknown"
R_ATTEMPT_UNATTRIBUTABLE = "attempt-unattributable"
R_ATTEMPT_TASK_MISMATCH = "attempt-task-mismatch"
R_TASK_UNATTRIBUTABLE = "task-unattributable"
R_STATUS_UNKNOWN = "status-unknown"
R_UNKNOWN_KIND = "unknown-kind"
R_INVALID_NOTIFICATION = "invalid-notification"
# S4：任务级后续责任带 attempt 锚时的版本/代次对照（发前反例守卫）。
R_RESULT_VERSION_CHANGED = "result-version-changed"
R_GENERATION_CHANGED = "generation-changed"
R_CURRENT_RESULT_VERSION_UNKNOWN = "current-result-version-unresolved"
# S4：任务级后续责任（continuation_ready/responsibility_missing）在任务仍
# 未结束、未暂停且归属可证时放行；ACK 不关闭责任。
R_RESPONSIBILITY_LIVE = "responsibility-live"
# 保留兼容码：旧“两 ID 皆空缺归属”语义已被 R_LEGACY_UNSCOPED 取代，不再返回。
R_MISSING_ATTRIBUTION = "missing-attribution"
# coordinator 带可证明 attempt 时（异常形状）仍按 coordinator 语义 send。
R_COORDINATOR_LIVE = "coordinator-live"

_TASK_CANCELLED = "cancelled"
_TASK_ACCEPTED = "accepted"
_TASK_PAUSED = "paused"


def kind_category(kind: str) -> str:
    """kind → 类别稳定映射；未登记 kind 返回 'unknown'。"""
    if not isinstance(kind, str):
        return CATEGORY_UNKNOWN
    if kind in CURRENT_ONLY_KINDS:
        return CATEGORY_CURRENT_ONLY
    if kind in CLEANUP_KINDS:
        return CATEGORY_CLEANUP
    if kind in COORDINATOR_KINDS:
        return CATEGORY_COORDINATOR
    return CATEGORY_UNKNOWN


def _status(value: Any, *, valid: frozenset[str]) -> str | None:
    """取状态字段：仅枚举内非空字符串视为“已知”；缺失/空/枚举外 → 未知。"""
    if isinstance(value, str) and value and value in valid:
        return value
    return None


def _decide(action: str, reason: str, base: dict,
            *, result_version: str | None = None,
            attempt_status: str | None = None,
            task_status: str | None = None) -> dict:
    out = dict(base)
    out["action"] = action
    out["reason"] = reason
    out["attempt_status"] = attempt_status
    out["task_status"] = task_status
    if result_version:
        out["result_version"] = result_version
    return out


def _payload_result_version(notification: dict) -> str | None:
    payload = notification.get("payload")
    pv = payload.get("result_version") if isinstance(payload, dict) else None
    return pv if isinstance(pv, str) and pv else None


def notification_decision(
    notification: dict,
    *,
    task: dict | None = None,
    attempt: dict | None = None,
) -> dict:
    """通知有效性的纯规则决策。

    参数：
      notification：通知行（含 kind/task_id/attempt_id/payload/state 等）。
      task：任务行（含 status/current_attempt_id/closed_at），可 None。
      attempt：尝试行（含 id/task_id/status/site_released_at/metadata），可 None。

    返回：
      action：send / defer / suppress / cleanup_required；
      reason：稳定原因码；
      kind_category / kind / task_id / attempt_id / current_attempt_id /
      attempt_status / task_status；payload 带 result_version 时原样透传
      （不修改结果版本与 ACK 不变量）。
    """
    if not isinstance(notification, dict):
        return _decide(
            DEFER, R_INVALID_NOTIFICATION,
            {"kind": None, "kind_category": CATEGORY_UNKNOWN,
             "task_id": None, "attempt_id": None, "current_attempt_id": None})

    kind = notification.get("kind") or ""
    notif_task_id = notification.get("task_id")
    notif_attempt_id = notification.get("attempt_id")
    category = kind_category(kind)
    result_version = _payload_result_version(notification)

    task_status = _status(task.get("status"), valid=TASK_STATUSES) \
        if isinstance(task, dict) else None
    base: dict[str, Any] = {
        "kind": kind,
        "kind_category": category,
        "task_id": notif_task_id,
        "attempt_id": notif_attempt_id,
        "current_attempt_id": task.get("current_attempt_id")
        if isinstance(task, dict) else None,
    }

    if not isinstance(notification.get("payload", {}), dict):
        return _decide(DEFER, R_INVALID_NOTIFICATION, base,
                       task_status=task_status)

    if not kind:
        return _decide(DEFER, R_UNKNOWN_KIND, base,
                       result_version=result_version, task_status=task_status)
    if category == CATEGORY_UNKNOWN:
        # 未登记 kind：保留诊断转为可见待决责任。
        return _decide(DEFER, R_UNKNOWN_KIND, base,
                       result_version=result_version, task_status=task_status)

    # ---- attempt 锚点归属证明：带 attempt_id 却无法证明归属 → defer ---------
    if notif_attempt_id:
        if not isinstance(attempt, dict) or attempt.get("id") != notif_attempt_id:
            return _decide(DEFER, R_ATTEMPT_UNATTRIBUTABLE, base,
                           result_version=result_version, task_status=task_status)
        if notif_task_id and notif_task_id != attempt.get("task_id"):
            return _decide(DEFER, R_ATTEMPT_TASK_MISMATCH, base,
                           result_version=result_version, task_status=task_status)
    attempt_status = _status(attempt.get("status"), valid=ATTEMPT_STATUSES) \
        if isinstance(attempt, dict) else None
    # 有 attempt 锚时以 attempt 推导任务归属（输出可见、可诊断）。
    if notif_attempt_id and isinstance(attempt, dict):
        base["task_id"] = notif_task_id or attempt.get("task_id")

    # ---- 无 attempt 锚：已登记 kind 的 legacy-unscoped 全局兼容 -------------
    if not notif_attempt_id:
        if category == CATEGORY_CURRENT_ONLY and notif_task_id:
            # 带 task_id 无 attempt：current-only 无法判 current/generation。
            # 先按人工控制/结束处理（已知状态），其余 defer。
            return _current_only_task_only(
                base, task, task_status=task_status,
                attempt_status=attempt_status, result_version=result_version)
        if category == CATEGORY_CLEANUP and notif_task_id:
            # 清理责任也需具体执行身份；不能把部分缺归属伪装为全局兼容。
            return _decide(DEFER, R_ATTEMPT_UNRESOLVED, base,
                           result_version=result_version,
                           attempt_status=attempt_status, task_status=task_status)
        # current_only/cleanup 两 ID 皆空 / coordinator 任意任务锚：
        # 无 attempt 锚 → legacy 全局兼容 send（不推断不存在代次）。
        return _decide(SEND, R_LEGACY_UNSCOPED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)

    # ---- 有 attempt 锚（已证明归属）---------------------------------------
    if category == CATEGORY_COORDINATOR:
        return _decide(SEND, R_COORDINATOR_LIVE, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if category == CATEGORY_CLEANUP:
        return _cleanup_decision(
            base, attempt, attempt_status=attempt_status,
            task_status=task_status, result_version=result_version)

    # ---- current_only：需要 task 证明 current/已结束 ------------------------
    derived_task_id = notif_task_id or attempt.get("task_id")
    if not isinstance(task, dict) or task.get("id") != derived_task_id:
        return _decide(DEFER, R_TASK_UNATTRIBUTABLE, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)

    task_status = _status(task.get("status"), valid=TASK_STATUSES)
    # 人工控制/结束优先（顺序即稳定原因码优先级）。
    if task_status == _TASK_CANCELLED:
        return _decide(SUPPRESS, R_TASK_CANCELLED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if task_status == _TASK_ACCEPTED:
        return _decide(SUPPRESS, R_TASK_ACCEPTED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if task_status == _TASK_PAUSED:
        return _decide(DEFER, R_TASK_PAUSED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if task.get("closed_at") is not None:
        return _decide(SUPPRESS, R_TASK_CLOSED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if task_status is None:
        # task.status 未知/枚举外：不能据此判断人工控制 → 不猜、不发送。
        return _decide(DEFER, R_TASK_STATUS_UNKNOWN, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)

    # obsolete generation：current 指针缺失不能视为未取代；有指针且指向
    # 更晚代次才抑制（保留原 ID/原因，不烧预算、不入 ACK）。
    current_attempt_id = task.get("current_attempt_id")
    if current_attempt_id is None:
        return _decide(DEFER, R_NO_CURRENT, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if current_attempt_id != notif_attempt_id:
        return _decide(SUPPRESS, R_SUPERSEDED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)

    # S4 场景 28：任务级后续责任带 attempt 锚时，除 current 外还必须比较
    # 已绑定 result_version / generation；已知不匹配 → 旧通知不得发送；
    # 当前版本缺失 → defer（不伤 S1 kind / legacy 无版本合法输入型责任）。
    if base.get("kind") in TASK_LEVEL_RESPONSIBILITY_KINDS \
            and isinstance(attempt, dict):
        meta = attempt.get("metadata") if isinstance(attempt.get("metadata"),
                                                     dict) else {}
        result = meta.get("result") if isinstance(meta.get("result"), dict) else {}
        stored = result.get("version")
        if result_version:
            if not isinstance(stored, str) or not stored:
                return _decide(DEFER, R_CURRENT_RESULT_VERSION_UNKNOWN, base,
                               result_version=result_version,
                               attempt_status=attempt_status,
                               task_status=task_status)
            if stored != result_version:
                return _decide(SUPPRESS, R_RESULT_VERSION_CHANGED, base,
                               result_version=result_version,
                               attempt_status=attempt_status,
                               task_status=task_status)
        payload_generation = (notification.get("payload") or {}).get("generation")
        attempt_generation = attempt.get("generation")
        if isinstance(payload_generation, int) \
                and not isinstance(payload_generation, bool) \
                and isinstance(attempt_generation, int) \
                and not isinstance(attempt_generation, bool) \
                and attempt_generation != payload_generation:
            return _decide(SUPPRESS, R_GENERATION_CHANGED, base,
                           result_version=result_version,
                           attempt_status=attempt_status,
                           task_status=task_status)

    # 条件类 kind：按 attempt 状态的明确证据判定“条件是否仍待处理”。
    pending = CONDITION_PENDING_STATUS.get(kind)
    if pending is not None:
        if attempt_status in pending:
            return _decide(SEND, R_CURRENT_ONLY_ACTIVE, base,
                           result_version=result_version,
                           attempt_status=attempt_status, task_status=task_status)
        if attempt_status is None:
            # 状态缺失/枚举外：不能凭缺字段猜测条件已解除。
            return _decide(DEFER, R_STATUS_UNKNOWN, base,
                           result_version=result_version,
                           attempt_status=attempt_status, task_status=task_status)
        return _decide(SUPPRESS, R_CONDITION_RESOLVED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)

    # 其余 current_only：当前且未结束 → 允许唤醒。
    return _decide(SEND, R_CURRENT_ONLY_ACTIVE, base,
                   result_version=result_version,
                   attempt_status=attempt_status, task_status=task_status)


def _current_only_task_only(base: dict, task: dict | None, *,
                            task_status: str | None, attempt_status: str | None,
                            result_version: str | None) -> dict:
    """current_only + 带 task_id 无 attempt：人工控制/结束优先，其余 defer。"""
    if not isinstance(task, dict) or task.get("id") != base.get("task_id"):
        return _decide(DEFER, R_TASK_UNATTRIBUTABLE, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    task_status = _status(task.get("status"), valid=TASK_STATUSES) or task_status
    if task_status == _TASK_CANCELLED:
        return _decide(SUPPRESS, R_TASK_CANCELLED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if task_status == _TASK_ACCEPTED:
        return _decide(SUPPRESS, R_TASK_ACCEPTED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if task_status == _TASK_PAUSED:
        return _decide(DEFER, R_TASK_PAUSED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if task.get("closed_at") is not None:
        return _decide(SUPPRESS, R_TASK_CLOSED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if task_status is None:
        return _decide(DEFER, R_TASK_STATUS_UNKNOWN, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    if base.get("kind") in TASK_LEVEL_RESPONSIBILITY_KINDS:
        # 任务级后续责任：任务仍活且归属已证 → 放行。责任是否已被关闭/转交
        # 由 runtime 在发送前按 revision/条件版本复核；旧 late 通知不在此处
        # 猜测，而是由责任身份去重与发送前复核处理。
        return _decide(SEND, R_RESPONSIBILITY_LIVE, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    # 无 attempt 锚无法判 current/generation → 保留诊断待决。
    return _decide(DEFER, R_ATTEMPT_UNRESOLVED, base,
                   result_version=result_version,
                   attempt_status=attempt_status, task_status=task_status)


def _cleanup_decision(base: dict, attempt: dict, *,
                      attempt_status: str | None, task_status: str | None,
                      result_version: str | None) -> dict:
    """cleanup 类：仅凭“已释放且无明确活动矛盾”解除；否则保持可见清理责任。"""
    raw_meta = attempt.get("metadata", {})
    metadata_unknown = not isinstance(raw_meta, dict)
    meta = raw_meta if isinstance(raw_meta, dict) else {}
    raw_checks = meta.get("side_effect_checks", {})
    checks_unknown = not isinstance(raw_checks, dict)
    side_checks = raw_checks if isinstance(raw_checks, dict) else {}
    # 只有明确 inactive 才是无活动证据；坏形状/缺状态不能冒充已停止。
    active_or_unknown = metadata_unknown or checks_unknown or any(
        not isinstance(v, dict) or v.get("state") != "inactive"
        for v in side_checks.values())
    runs = meta.get("running_shells") or meta.get("active_shells")
    children = meta.get("active_children")
    has_active_evidence = bool(active_or_unknown or runs or children)

    if attempt.get("site_released_at") is not None and not has_active_evidence:
        return _decide(SUPPRESS, R_SITE_RELEASED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)

    kind = base.get("kind")
    if kind == "execution_unconfirmed":
        # 终态候选被拒/停止未确认/副作用未知：写权与事实保持，清理可见。
        # 释放标记与明确活动证据矛盾时同样保持清理可见。
        return _decide(CLEANUP_REQUIRED, R_STOP_UNCONFIRMED, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)

    # side_effects_running：仍明确活动/未知则标记 active 矛盾，否则写权未释放。
    if has_active_evidence:
        return _decide(CLEANUP_REQUIRED, R_SIDE_EFFECTS_ACTIVE, base,
                       result_version=result_version,
                       attempt_status=attempt_status, task_status=task_status)
    return _decide(CLEANUP_REQUIRED, R_WRITE_SITE_HELD, base,
                   result_version=result_version,
                   attempt_status=attempt_status, task_status=task_status)


__all__ = [
    # 类别与注册表
    "ALL_KINDS",
    "CURRENT_ONLY_KINDS",
    "CLEANUP_KINDS",
    "COORDINATOR_KINDS",
    "TASK_LEVEL_RESPONSIBILITY_KINDS",
    "CATEGORY_CURRENT_ONLY",
    "CATEGORY_CLEANUP",
    "CATEGORY_COORDINATOR",
    "CATEGORY_UNKNOWN",
    "kind_category",
    # 动作与决策接口
    "SEND",
    "DEFER",
    "SUPPRESS",
    "CLEANUP_REQUIRED",
    "notification_decision",
    # 稳定原因码
    "R_CURRENT_ONLY_ACTIVE",
    "R_CONDITION_RESOLVED",
    "R_SUPERSEDED",
    "R_TASK_PAUSED",
    "R_TASK_CANCELLED",
    "R_TASK_ACCEPTED",
    "R_TASK_CLOSED",
    "R_SITE_RELEASED",
    "R_SIDE_EFFECTS_ACTIVE",
    "R_STOP_UNCONFIRMED",
    "R_WRITE_SITE_HELD",
    "R_LEGACY_UNSCOPED",
    "R_ATTEMPT_UNRESOLVED",
    "R_NO_CURRENT",
    "R_TASK_STATUS_UNKNOWN",
    "R_ATTEMPT_UNATTRIBUTABLE",
    "R_ATTEMPT_TASK_MISMATCH",
    "R_TASK_UNATTRIBUTABLE",
    "R_STATUS_UNKNOWN",
    "R_UNKNOWN_KIND",
    "R_INVALID_NOTIFICATION",
    "R_RESPONSIBILITY_LIVE",
    "R_RESULT_VERSION_CHANGED",
    "R_GENERATION_CHANGED",
    "R_CURRENT_RESULT_VERSION_UNKNOWN",
    "R_MISSING_ATTRIBUTION",
    "R_COORDINATOR_LIVE",
]

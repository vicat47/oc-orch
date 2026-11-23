"""纯业务规则：状态常量、验收门禁函数与结果产物标识。

只依赖标准库与同层 domain 模块；**不得** import util 等副作用混合模块
（本模块被 ledger/results/verify 等共用，必须保持零外部 I/O 副作用）。

ledger.py 头部的对应常量与 verification_gate_rows 将由主 agent 迁移为对本
模块的导入；本文件是这些定义的单一权威来源，旧位置仅转调。
"""

from __future__ import annotations

import json
from typing import Any

TASK_STATUSES = {
    "draft", "dispatched", "running", "blocked", "paused", "stalled",
    "result_pending", "result_received", "rejected", "accepted", "cancelled",
}
TASK_TERMINAL = {"accepted", "cancelled"}

ATTEMPT_STATUSES = {
    "creating", "dispatching", "dispatch_unknown", "dispatched", "running",
    "blocked_permission", "blocked_form", "provider_retry", "idle_unconfirmed",
    "stalled", "stop_requested", "terminal_succeeded", "terminal_failed",
    "terminal_interrupted", "result_collected", "result_missing",
    "verification_failed", "accepted", "rejected", "cancelled", "lost",
}
ATTEMPT_ACTIVE = {
    "creating", "dispatching", "dispatch_unknown", "dispatched", "running",
    "blocked_permission", "blocked_form", "provider_retry", "idle_unconfirmed",
    "stalled", "stop_requested",
}
ATTEMPT_TERMINAL = {
    "terminal_succeeded", "terminal_failed", "terminal_interrupted",
    "result_collected", "result_missing", "verification_failed",
    "accepted", "rejected", "cancelled", "lost",
}
# 终态之后允许的可解释推进；不允许 succeeded/failed 互相改写
ALLOWED_POST_TERMINAL = {
    ("terminal_succeeded", "result_collected"),
    ("terminal_succeeded", "result_missing"),
    ("terminal_succeeded", "accepted"),
    ("terminal_succeeded", "rejected"),
    ("terminal_succeeded", "cancelled"),
    ("terminal_failed", "cancelled"),
    ("terminal_interrupted", "cancelled"),
    ("terminal_interrupted", "rejected"),
    ("result_collected", "verification_failed"),
    ("result_collected", "accepted"),
    ("result_collected", "rejected"),
    ("result_collected", "cancelled"),
    ("result_missing", "verification_failed"),
    ("result_missing", "accepted"),
    ("result_missing", "result_collected"),
    ("result_missing", "rejected"),
    ("result_missing", "cancelled"),
    ("verification_failed", "cancelled"),
    ("verification_failed", "accepted"),
    ("accepted", "accepted"),
    ("rejected", "rejected"),
}
# 写权现场（site）持有判断：任何尝试状态都可能仍持有外部副作用——执行终态、
# 验收结论、force 接受都不证明外部 shell/session 已停止。因此持有集与全部
# 尝试状态对齐，现场释放只以显式 release（site_released_at）为准。
SITE_HOLDING = set(ATTEMPT_STATUSES)

STATUS_TASK_MAP = {
    "running": ("running", None),
    "blocked_permission": ("blocked", "permission"),
    "blocked_form": ("blocked", "form"),
    "provider_retry": ("blocked", "provider_backoff"),
    "idle_unconfirmed": ("blocked", "idle_unconfirmed"),
    "stalled": ("stalled", "no_progress"),
    "stop_requested": ("blocked", "stop_requested"),
    "terminal_succeeded": ("result_pending", None),
    "terminal_failed": ("blocked", "execution_failed"),
    "terminal_interrupted": ("blocked", "interrupted"),
    "result_collected": ("result_received", None),
    "result_missing": ("result_received", "result_missing"),
    "verification_failed": ("rejected", "verification_failed"),
    "accepted": ("accepted", None),
    "lost": ("blocked", "session_lost"),
}

# 可验收的执行后状态；force 可豁免收集/交付证据缺项，但不能把 active/未终态
# 尝试通过 force 变得可接受。正常 pass 另外要求"已收集标记 + 有效结果产物"。
VERIFIABLE_STATES = ("terminal_succeeded", "result_collected", "result_missing")

# transition_* 的 extra 白名单：避免未受信调用注入覆盖 status/verification 等
# 守卫字段（旧实现 extra 无白名单，SET 重复列时后写生效可破坏守卫）。
ATTEMPT_TRANSITION_EXTRA = {
    "metadata", "session_meta", "started_at", "last_progress_at",
    "result_collected_at", "stop_requested_at", "terminal_reason",
}
TASK_TRANSITION_EXTRA = {
    "metadata", "closed_at", "current_attempt_id", "next_check_at",
    "blocked_reason",
}

# 验收证据守卫列：指纹只覆盖这些"相关行值"，不把毫秒 updated_at 当唯一版本。
VERIFICATION_GUARD_COLUMNS = (
    "status", "generation", "task_id", "session_id", "directory",
    "result_collected_at", "result_path", "result_summary",
    "verification", "metadata",
)
TASK_GUARD_COLUMNS = (
    "status", "current_attempt_id", "accepted_at", "closed_at",
    "blocked_reason", "coordinator_session",
)

# collect_attempt 产出结果文件的 schema 标识；验收证据快照与提交都用它核对。
RESULT_ARTIFACT_SCHEMA = "oc-orch.result.v1"


def _json_loads(text: str | None, default: Any = None) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


def verification_gate_rows(deliverables: list[dict]) -> dict:
    """以持久证据判定声明门禁；prepare 与原子提交使用同一规则。"""
    missing, unverified = [], []
    for d in deliverables:
        if not d["required"]:
            continue
        if d["status"] in ("missing", "expected"):
            missing.append(d)
            continue
        evidence = _json_loads(d["evidence"], None)
        passed = (d["status"] in ("present", "verified")
                  and d["checked_at"] is not None and evidence is not None)
        if d["kind"] == "command":
            passed = (passed and isinstance(evidence, dict)
                      and evidence.get("command") == d["target"]
                      and evidence.get("exit") == 0 and not evidence.get("timed_out"))
        if not passed:
            unverified.append(d)
    return {"deliverables": deliverables, "missing": missing, "unverified": unverified}
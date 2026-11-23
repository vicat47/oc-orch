"""提供商故障知会的纯规则（provider-fault-notifications N01–N12 的域侧）。

零 I/O、零台账写。本模块只推导：

- 事件类别与稳定事件身份（事件去重/更新预算的域规则）；
- “故障驱动切换”与“普通模型调整/正常暂停”的区分（N01/N02/N09）；
- 快速恢复后**仍应知会**首封摘要（N07）——与普通人工催办的“恢复前未提交则
  抑制孤立恢复”规则不同：故障切换是重要已发生事实，不只当前待办；
- 同一事故去重、已可能提交后的有限更新（N10/N11）。

事件类别：
- ``EVENT_PROVIDER_FAULT``：需要人工（协调者/共享 session 故障、备用失败、
  授权缺失、切换被拒）——即使没有模型切换也通知（N03）。
- ``EVENT_PROVIDER_FAILOVER``：故障驱动的自动处置知会（切换尝试/确认/恢复）。
  普通主动换模型、成本调整不生成此类事件（N02）。
"""

from __future__ import annotations

import hashlib

from typing import Any

from ..domain.failover import (
    EPISODE_BLOCKED_MANUAL,
    EPISODE_CANCELLED,
    EPISODE_CONTINUATION_REQUESTED,
    EPISODE_CONTINUATION_UNKNOWN,
    EPISODE_OPEN,
    EPISODE_RESUMED,
    EPISODE_SUPERSEDED,
    EPISODE_SWITCH_CONFIRMED,
    EPISODE_SWITCH_REQUESTED,
    EPISODE_SWITCH_UNKNOWN,
    EPISODE_WAITING_QUIESCENCE,
)

# 事件类别（区别于既有 outbox 的 first/update/recovery kind；kind 仍复用）。
EVENT_PROVIDER_FAULT = "provider-fault"        # 需要人工处理
EVENT_PROVIDER_FAILOVER = "provider-failover"  # 故障驱动自动处置知会
EVENT_TYPES = frozenset({EVENT_PROVIDER_FAULT, EVENT_PROVIDER_FAILOVER})

# 事件 kind（复用既有 outbox 的 EVENT_KINDS 语义，但语义类别独立保存）。
KIND_FIRST = "first"
KIND_UPDATE = "update"
KIND_RECOVERY = "recovery"

# episode 状态 → 事件类别（自动处理 vs 需要人工）。
_FAULT_STATES = frozenset({EPISODE_BLOCKED_MANUAL})
_FAILOVER_STATES = frozenset({
    EPISODE_OPEN, EPISODE_WAITING_QUIESCENCE, EPISODE_SWITCH_REQUESTED,
    EPISODE_SWITCH_CONFIRMED, EPISODE_SWITCH_UNKNOWN,
    EPISODE_CONTINUATION_REQUESTED, EPISODE_CONTINUATION_UNKNOWN,
    EPISODE_RESUMED, EPISODE_SUPERSEDED, EPISODE_CANCELLED,
})


def event_type_for_state(state: str) -> str:
    """按 episode 状态给出事件类别；未知状态 → None（不产生邮件）。"""
    if state in _FAULT_STATES:
        return EVENT_PROVIDER_FAULT
    if state in _FAILOVER_STATES:
        return EVENT_PROVIDER_FAILOVER
    return ""


def _stable_str(value: Any, what: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{what} 必须是非空字符串（收到 {value!r}）")
    return value.strip()


def base_key(episode_id: str, incident_type: str, recipient_ref: str) -> str:
    """稳定事故身份：episode + 事件类别 + 收件范围（不含时间/观察计数）。"""
    return hashlib.sha256(
        f"{_stable_str(episode_id, 'episode_id')}\x00"
        f"{_stable_str(incident_type, 'incident_type')}\x00"
        f"{_stable_str(recipient_ref, 'recipient_ref')}".encode("utf-8")
    ).hexdigest()[:24]


def event_key(*, episode_id: str, incident_type: str, recipient_ref: str,
              version: int, kind: str) -> str:
    """稳定事件身份：同一事故/类别/收件 + 版本 + kind 去重（N10）。"""
    version = int(version)
    if version < 1:
        raise ValueError("version 必须为正整数")
    raw = f"{_stable_str(episode_id, 'episode_id')}\x00" \
          f"{_stable_str(incident_type, 'incident_type')}\x00" \
          f"{_stable_str(recipient_ref, 'recipient_ref')}\x00" \
          f"{kind}\x00{version}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def decide(*, version: int, rows: list[dict], resolved: bool = False,
           max_updates: int = 3) -> dict:
    """由既有同 base 行推导应建立的事件。

    - 无既有提醒行：KIND_FIRST（即使已恢复也发首封摘要，N07）。
    - ``resolved=True`` 且已有提醒：KIND_RECOVERY 一次（关联原问题，N11）。
    - 同版本轮询 -> None；已可能提交后，仅实质版本更新且未超预算 -> KIND_UPDATE。
    - 已 possible/unknown 的旧行不修改，只能新增更新版本（N10）。
    """
    if not isinstance(rows, list):
        rows = []
    reminders = [r for r in rows if r.get("kind") in (KIND_FIRST, KIND_UPDATE)]
    updates = [r for r in rows if r.get("kind") == KIND_UPDATE]
    prior_possible = bool(reminders)
    if not reminders:
        return {"event": KIND_FIRST, "reason": "first-notify",
                "prior_possible": False}
    if resolved:
        if any(r.get("kind") == KIND_RECOVERY for r in rows):
            return {"event": None, "reason": "duplicate-recovery",
                    "prior_possible": True}
        return {"event": KIND_RECOVERY, "reason": "resolved",
                "prior_possible": True}
    newest = max((int(r.get("condition_version") or 0) for r in reminders),
                 default=0)
    if int(version) <= newest:
        return {"event": None, "reason": "duplicate-observation",
                "prior_possible": prior_possible}
    # max_updates applies to updates after the first notice, not to the first
    # notice itself. Otherwise the default of three would silently allow only
    # two substantive updates per incident.
    if len(updates) >= int(max_updates):
        return {"event": None, "reason": "update-budget-exhausted",
                "prior_possible": prior_possible}
    return {"event": KIND_UPDATE, "reason": "condition-version",
            "prior_possible": prior_possible}


def content(*, incident_type: str, state: str, project: str, task_id: str,
            session_id: str | None, route: str, category: str | None = None,
            target_route: str | None = None, status: str | None = None,
            next_action: str | None = None) -> dict:
    """最小化脱敏摘要（不包含完整聊天/凭据/正文）。"""
    lines = [
        f"项目：{_stable_str(project, 'project')[:100]}",
        f"任务：{_stable_str(task_id, 'task_id')[:80]}",
        f"事件：{_stable_str(incident_type, 'incident_type')}（{state}）",
        f"实际路线：{_stable_str(route, 'route')[:120] if route else '未知'}",
    ]
    if category:
        lines.append(f"故障类别：{category[:80]}")
    if target_route:
        lines.append(f"切换目标：{target_route[:120]}")
    if status:
        lines.append(f"状态：{status[:120]}")
    if session_id:
        lines.append(f"会话：{_stable_str(session_id, 'session_id')[:80]}")
    if next_action:
        lines.append(f"下一动作：{next_action[:200]}")
    body = "\n".join(lines)
    subject = f"[oc-orch] {incident_type} {project[:60]}"
    content_hash = hashlib.sha256(
        f"{subject}\x00{body}".encode("utf-8")).hexdigest()
    return {"subject": subject, "body": body, "content_hash": content_hash}


__all__ = [
    "EVENT_PROVIDER_FAULT", "EVENT_PROVIDER_FAILOVER", "EVENT_TYPES",
    "KIND_FIRST", "KIND_UPDATE", "KIND_RECOVERY",
    "event_type_for_state", "base_key", "event_key", "decide", "content",
]

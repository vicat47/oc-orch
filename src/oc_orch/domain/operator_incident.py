"""业务 incident 纯规则：从结构化台账事实推导可重验 incident（roadmap R3）。

零 I/O、零台账写、零网络。本模块只推导“什么值得通知、何时算显著变化/升级/
恢复/重开”的确定性决策；持久化在 :mod:`oc_orch.adapters.operator_incident`
（meta 自有命名空间），发送复用既有 outbox（``OperatorEmailService``），
不造第二套发送器。

首批事实（只认结构化 Task/Responsibility/条件版本与可信覆盖，不解析聊天、
``alert.detail`` 或邮件回复）：

- ``waiting-human-responsibility``：责任行 ``state=waiting_human`` 且未关闭；
- ``responsibility-due``：责任行到期/期限/预算耗尽（``deadline_decision``
  exhausted）且未关闭；
- ``task-no-progress``：未完成任务确证无推进责任（无在途 attempt、无未关闭
  责任、无未结通知、非暂停、非 provider 冷却）。

安全不变量（对应 R3 关键安全与父核收清单）：

- **稳定身份**：``incident_id`` 只由 project + incident_type + 源身份决定；
- **签名含归属**：``source_signature`` 包含 project/task_id/incident_type/
  条件键/状态/代次/结果版本/owner/任务状态/责任状态集/current attempt/期限
  ——任何归属篡改、换代或期限变化都会实质改变签名；不含时间/观察计数；
- **重复观察不推进版本**：相同签名 → observe，不 bump ``condition_version``、
  不重置 deadline；
- **有界升级**：签名实质变化 → update/ escalate（版本 +1）；时间性升级只在
  实际越过既有级别时发生一次；
- **重开（recurrence）**：已 recovered/suppressed 的 incident，当同一源再次
  出现同一阻塞条件或出现了新条件（签名变化）时以 **新版本重开**，绝不复用
  旧 event_key 重发已受理/未知邮件；
- **源绑定**：``build_binding`` 携带源身份 + project/task 归属 + condition_key
  + 签名 + 期望状态 + 会话/模型（同 conn 可重验）；恢复事件绑定到已恢复的
  incident 记录，claim 时重验 underlying source 仍为 definitely-resolved。

源重验三/五态（``recheck_binding`` 返回，供 outbox claim 与 incident 恢复
扫描使用）：

- active：源原样可行动（可发送/可保持 open）；
- resolved：独立确定底层条件已解除（责任关闭 / 任务收口 / no_progress 已
  有推进机制）→ 才能恢复；
- stale：源存在但绑定不再吻合（状态/条件键/签名/归属/换代变化）→ 不发送、
  不恢复、抑制；
- paused：项目暂停 / 回合 hold+屏障 / provider 冷却（正常）→ 不发送、不恢复、
  保持待发（暂停不是故障恢复，绝不把 paused 抑制当 recovered）；
- unknown：读不到/损坏/缺数据 → fail-closed，不发送、不恢复、保持 open。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .operator_email import CONDITION_HUMAN, CONDITION_PAUSE, CONDITION_UNKNOWN

# ---- incident 类型（首批事实） ---------------------------------------------
INCIDENT_WAITING_HUMAN = "waiting-human-responsibility"
INCIDENT_RESPONSIBILITY_DUE = "responsibility-due"
INCIDENT_NO_PROGRESS = "task-no-progress"
INCIDENT_TYPES = frozenset({
    INCIDENT_WAITING_HUMAN, INCIDENT_RESPONSIBILITY_DUE, INCIDENT_NO_PROGRESS,
})

# ---- 源绑定 kind -----------------------------------------------------------
SOURCE_KIND_RESPONSIBILITY = "responsibility"
SOURCE_KIND_TASK = "task"
SOURCE_KIND_INCIDENT = "incident"   # 恢复事件绑定到已恢复的 incident 记录
SOURCE_KINDS = frozenset({SOURCE_KIND_RESPONSIBILITY, SOURCE_KIND_TASK,
                          SOURCE_KIND_INCIDENT})

# ---- incident 自身状态 -----------------------------------------------------
INCIDENT_OPEN = "open"
INCIDENT_RECOVERED = "recovered"
INCIDENT_SUPPRESSED = "suppressed"
INCIDENT_STATES = frozenset({INCIDENT_OPEN, INCIDENT_RECOVERED,
                             INCIDENT_SUPPRESSED})

# ---- 源重验判定（五态） -----------------------------------------------------
VERDICT_ACTIVE = "active"       # 可行动（可发送/可保持 open）
VERDICT_RESOLVED = "resolved"   # 独立确定底层条件已解除 → 才可恢复
VERDICT_STALE = "stale"         # 条件变化/换代/归属不吻合 → 抑制、不恢复
VERDICT_PAUSED = "paused"       # 正常暂停/回合 hold/冷却 → 不发送、不恢复
VERDICT_UNKNOWN = "unknown"     # 读不到/坏数据 → fail-closed
VERDICTS = frozenset({VERDICT_ACTIVE, VERDICT_RESOLVED, VERDICT_STALE,
                      VERDICT_PAUSED, VERDICT_UNKNOWN})

# ---- 通道状态（可见投影；不是发送能力） -----------------------------------
CHANNEL_NOT_CONFIGURED = "not-configured"   # 无收件范围 → 仅本地可见待配置
CHANNEL_EMAIL_DISABLED = "email-disabled"   # 有范围但邮件策略未启用 → pending-auth
CHANNEL_EMAIL_ENABLED = "email-enabled"     # 策略启用且在范围内才可能发送

MAX_CLOCK_MS = 8 * 10 ** 12

# 摘要/原因限长（与 outbox digest 一致的最小化）。
_MAX_FIELD = 200
_MAX_ACTION = 200


def _json_dumps(value: Any) -> str:
    """纯标准库稳定 JSON（domain 层只允许 domain/stdlib）。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _text(value: Any, what: str = "字段") -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{what} 必须是非空字符串（收到 {value!r}）")
    return value.strip()


def _int_or_none(value: Any, what: str = "字段") -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{what} 必须是非负整数或 None（收到 {value!r}）")
    return value


def _clock_or_none(value: Any, what: str = "字段") -> int | None:
    raw = _int_or_none(value, what)
    if raw is not None and raw > MAX_CLOCK_MS:
        raise ValueError(f"{what} 超出有效时钟边界（收到 {value!r}）")
    return raw


def _opt_str(value: Any, what: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{what} 必须是非空字符串或 None（收到 {value!r}）")
    return value.strip()


# ---- 归一化 ----------------------------------------------------------------
def normalize_source(source: Any) -> dict:
    """严格归一化源快照；任何坏字段抛 ValueError（fail-closed）。

    新增归属/会话/模型字段（``source_project``/``source_task_id``/
    ``source_session``/``source_model``）为可选（None 或非空字符串），
    ``build_binding`` 会以它们落入源绑定供同 conn 重验。
    """
    if not isinstance(source, dict):
        raise ValueError("source 必须为 dict")
    kind = _text(source.get("source_kind"), "source_kind")
    if kind not in SOURCE_KINDS:
        raise ValueError(f"source_kind 非法：{kind!r}")
    source_id = _text(source.get("source_id"), "source_id")
    project = _text(source.get("project"), "project")
    incident_type = _text(source.get("incident_type"), "incident_type")
    if incident_type not in INCIDENT_TYPES:
        raise ValueError(f"incident_type 非法：{incident_type!r}")
    task_id = (_text(source.get("task_id") or "", "task_id")
               if source.get("task_id") else "")
    condition_key = _text(source.get("condition_key"), "condition_key")
    condition_state = _text(source.get("condition_state"), "condition_state")
    ckind = _text(source.get("condition_kind") or CONDITION_UNKNOWN,
                  "condition_kind")
    if ckind not in (CONDITION_HUMAN, CONDITION_PAUSE, CONDITION_UNKNOWN):
        raise ValueError(f"condition_kind 非法：{ckind!r}")
    escalation = source.get("escalation_level")
    if isinstance(escalation, bool) or not isinstance(escalation, int) \
            or escalation < 0:
        raise ValueError(f"escalation_level 非法：{escalation!r}")
    observed_at = _clock_or_none(source.get("observed_at"), "observed_at")
    first_at = _clock_or_none(source.get("first_evidence_at"), "first_evidence_at")
    deadline = _clock_or_none(source.get("deadline"), "deadline")
    recovered = source.get("recovered", False)
    if not isinstance(recovered, bool):
        raise ValueError("recovered 必须为 bool")
    rstates = source.get("responsibility_states") or ()
    if not isinstance(rstates, (list, tuple)):
        raise ValueError("responsibility_states 必须为 list/tuple")
    states = tuple(sorted(str(s) for s in rstates))
    cur_attempt = source.get("current_attempt_id")
    if cur_attempt is not None and not (isinstance(cur_attempt, str)
                                        and cur_attempt.strip()):
        raise ValueError("current_attempt_id 必须是非空字符串或 None")
    task_status = source.get("task_status")
    if task_status is not None and not (isinstance(task_status, str)
                                        and task_status.strip()):
        raise ValueError("task_status 必须是非空字符串或 None")
    src_project = _opt_str(source.get("source_project"), "source_project")
    src_task = _opt_str(source.get("source_task_id"), "source_task_id")
    src_session = _opt_str(source.get("source_session"), "source_session")
    src_model = _opt_str(source.get("source_model"), "source_model")
    return {
        "source_kind": kind,
        "source_id": source_id,
        "project": project,
        "incident_type": incident_type,
        "task_id": task_id,
        "condition_key": condition_key,
        "condition_state": condition_state,
        "generation": _int_or_none(source.get("generation"), "generation"),
        "result_version": (str(source["result_version"]).strip()
                           if source.get("result_version") is not None else None),
        "owner": _text(source.get("owner") or "manual:unassigned", "owner"),
        "next_action": _text(source.get("next_action") or "检查人工待办",
                             "next_action")[:_MAX_ACTION],
        "blocked_reason": str(source.get("blocked_reason") or "")[:_MAX_FIELD],
        "impact": str(source.get("impact") or "")[:_MAX_FIELD],
        "condition_kind": ckind,
        "escalation_level": escalation,
        "deadline": deadline,
        "first_evidence_at": first_at,
        "observed_at": observed_at,
        "task_status": task_status,
        "responsibility_states": states,
        "current_attempt_id": (cur_attempt.strip()
                               if (cur_attempt and cur_attempt.strip()) else None),
        "source_project": src_project,
        "source_task_id": src_task,
        "source_session": src_session,
        "source_model": src_model,
        "recovered": recovered,
    }


def incident_id(project: str, incident_type: str, source_id: str) -> str:
    """稳定 incident 身份；轮询时间不参与身份（M06）。"""
    raw = "\x1f".join((project or "", incident_type or "", source_id or ""))
    return "I-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def source_signature(source: dict) -> str:
    """源签名包含归属、换代、期限及真实路由；不包含轮询时间。

    相同签名 → 重复观察（observe）；签名实质变化才是显著变化（update/
    escalate/reopen）。调用方必须先过 ``normalize_source``。
    """
    src = normalize_source(source)
    payload = {
        "source_kind": src["source_kind"],
        "source_id": src["source_id"],
        "project": src["project"],
        "incident_type": src["incident_type"],
        "task_id": src["task_id"],
        "condition_key": src["condition_key"],
        "condition_state": src["condition_state"],
        "generation": src["generation"],
        "result_version": src["result_version"],
        "owner": src["owner"],
        "next_action": src["next_action"],
        "task_status": src["task_status"],
        "responsibility_states": tuple(src["responsibility_states"]),
        "current_attempt_id": src["current_attempt_id"],
        "deadline": src["deadline"],
        "source_session": src["source_session"],
        "source_model": src["source_model"],
    }
    return "sig-" + hashlib.sha256(
        _json_dumps(payload).encode("utf-8")).hexdigest()[:32]


def decide(source: dict, stored: dict | None, *, now: int) -> dict:
    """由源快照与既有 incident 记录推导动作。

    - create：首次事实 → 版本 1；
    - observe：签名未变 → 保留版本、不重置 deadline（M06）；
    - update：签名实质变化 → 版本 +1（有界显著变化）；
    - escalate：签名变化或时间性升级越过既有级别 → 版本 +1 + 新级别（一次）；
    - reopen：已 recovered/suppressed 的 incident 重新出现同一条件状态或
      新条件（签名变化）→ 以新版本重开（不吞第二次故障，不复用旧 event_key）；
    - noop：无变化且未到重开条件。
    """
    src = normalize_source(source)
    now = int(now)
    if src["observed_at"] is None:
        src["observed_at"] = now
    if stored is None:
        return {"action": "create", "condition_version": 1,
                "escalation_level": src["escalation_level"], "recovered": False,
                "reason": "first-observation"}
    old_version = int(stored.get("condition_version") or 1)
    if stored.get("state") != INCIDENT_OPEN:
        new_sig = source_signature(src)
        recurred = (new_sig != stored.get("source_signature")
                    or src["condition_state"]
                    == stored.get("condition_state"))
        if recurred:
            return {"action": "reopen", "condition_version": old_version + 1,
                    "escalation_level": src["escalation_level"],
                    "recovered": False, "reason": "condition-recurred",
                    "signature_changed": True}
        return {"action": "noop",
                "condition_version": old_version,
                "escalation_level": int(stored.get("escalation_level") or 0),
                "recovered": bool(stored.get("state") == INCIDENT_RECOVERED),
                "reason": "incident-not-open", "signature_changed": False}
    new_sig = source_signature(src)
    old_sig = stored.get("source_signature")
    esc_new = src["escalation_level"]
    esc_old = int(stored.get("escalation_level") or 0)
    if new_sig == old_sig:
        if esc_new > esc_old:
            # 时间性升级（期限/预算由扫描侧按当前时钟推导）：只在实际升级
            # 越过既有级别时推进一次版本（有限、不随轮询反复）。
            return {"action": "escalate", "condition_version": old_version + 1,
                    "escalation_level": esc_new, "recovered": False,
                    "reason": "escalation-point", "signature_changed": False}
        return {"action": "observe",
                "condition_version": old_version,
                "escalation_level": esc_old, "recovered": False,
                "reason": "duplicate-observation", "signature_changed": False}
    new_version = old_version + 1
    if esc_new > esc_old:
        return {"action": "escalate", "condition_version": new_version,
                "escalation_level": esc_new, "recovered": False,
                "reason": "escalation-point", "signature_changed": True}
    return {"action": "update", "condition_version": new_version,
            "escalation_level": esc_new, "recovered": False,
            "reason": "condition-changed", "signature_changed": True}


# ---- 源绑定 ------------------------------------------------------------
BINDING_REQUIRED_KEYS = ("source_kind", "source_id", "source_condition_key",
                         "source_version", "source_state", "source_incident_type",
                         "source_project", "source_task_id")
BINDING_OPTIONAL_KEYS = ("source_session", "source_model")


def build_binding(source: dict, *, expected_state: str | None = None) -> dict:
    """源绑定快照：入 outbox / 发送 claim 前在同一事务内重验的纯本地证据。

    含归属（``source_project``/``source_task_id``）与可选的
    ``source_session``/``source_model``（同 conn 复核项目暂停/回合 hold/
    provider 冷却用）。恢复事件请用 :func:`recovery_binding`。
    """
    src = normalize_source(source)
    state = expected_state or src["condition_state"]
    return {
        "source_kind": src["source_kind"],
        "source_id": src["source_id"],
        "source_condition_key": src["condition_key"],
        "source_version": source_signature(src),
        "source_state": state,
        "source_incident_type": src["incident_type"],
        "source_project": src["source_project"] or src["project"],
        "source_task_id": src["source_task_id"] or src["task_id"],
        "source_session": src["source_session"],
        "source_model": src["source_model"],
    }


def recovery_binding(incident: dict, *, recovered_at: int) -> dict:
    """恢复事件的源绑定：绑定到 incident 记录自身（台账内证据）。

    重验 = 同一事务内读取 incident 记录且仍为 recovered，**并重验其底层
    source 仍为 definitely-resolved**（不允许只信本地 ``state=recovered``）。
    """
    return {
        "source_kind": SOURCE_KIND_INCIDENT,
        "source_id": str(incident.get("incident_id") or ""),
        "source_condition_key": str(incident.get("condition_key") or ""),
        "source_version": (f"recovered@{int(recovered_at)}:"
                           f"{incident['condition_version']}:{incident['revision']}"),
        "source_state": INCIDENT_RECOVERED,
        "source_incident_type": str(incident.get("incident_type") or ""),
        "source_project": str(incident.get("project") or ""),
        "source_task_id": str(incident.get("task_id") or ""),
        "source_session": None,
        "source_model": None,
    }


# ---- 可见投影（list/status 只读脱敏） --------------------------------------
def masked_projection(incident: dict, *, channel: str) -> dict:
    """脱敏投影：不含地址、完整聊天、原始证据正文；自由文本统一截断。"""
    from .operator_email import redact_text

    def r(v, limit: int = _MAX_FIELD) -> str:
        return redact_text(str(v or ""), limit=limit)

    return {
        "incident_id": str(incident.get("incident_id") or ""),
        "project": r(incident.get("project"), 80),
        "incident_type": str(incident.get("incident_type") or ""),
        "source_kind": str(incident.get("source_kind") or ""),
        "source_id": r(incident.get("source_id"), 80),
        "task_id": r(incident.get("task_id"), 80),
        "condition_key": r(incident.get("condition_key"), 120),
        "condition_state": r(incident.get("condition_state"), 80),
        "condition_version": int(incident.get("condition_version") or 1),
        "escalation_level": int(incident.get("escalation_level") or 0),
        "state": str(incident.get("state") or INCIDENT_OPEN),
        "owner": r(incident.get("owner"), 80),
        "next_action": r(incident.get("next_action")),
        "blocked_reason": r(incident.get("blocked_reason")),
        "first_seen_at": int(incident.get("first_seen_at") or 0),
        "last_observed_at": int(incident.get("last_observed_at") or 0),
        "deadline": incident.get("deadline"),
        "received_at": int(incident.get("created_at") or 0),
        "channel": channel,
        "recipient_ref": r(incident.get("recipient_scope") or "", 40),
    }


def channel_state(*, recipient_refs: tuple, email_policy_state: str | None) -> str:
    """只读通道状态：无收件范围 → not-configured；有范围但邮件未启用 →
    email-disabled；策略 enabled → email-enabled。不是发送能力承诺。"""
    if not recipient_refs:
        return CHANNEL_NOT_CONFIGURED
    if email_policy_state != "enabled":
        return CHANNEL_EMAIL_DISABLED
    return CHANNEL_EMAIL_ENABLED


__all__ = [
    "INCIDENT_WAITING_HUMAN", "INCIDENT_RESPONSIBILITY_DUE", "INCIDENT_NO_PROGRESS",
    "INCIDENT_TYPES",
    "SOURCE_KIND_RESPONSIBILITY", "SOURCE_KIND_TASK", "SOURCE_KIND_INCIDENT",
    "SOURCE_KINDS",
    "INCIDENT_OPEN", "INCIDENT_RECOVERED", "INCIDENT_SUPPRESSED", "INCIDENT_STATES",
    "VERDICT_ACTIVE", "VERDICT_RESOLVED", "VERDICT_STALE", "VERDICT_PAUSED",
    "VERDICT_UNKNOWN", "VERDICTS",
    "BINDING_REQUIRED_KEYS", "BINDING_OPTIONAL_KEYS",
    "CHANNEL_NOT_CONFIGURED", "CHANNEL_EMAIL_DISABLED", "CHANNEL_EMAIL_ENABLED",
    "MAX_CLOCK_MS",
    "normalize_source", "incident_id", "source_signature", "decide",
    "build_binding", "recovery_binding", "masked_projection", "channel_state",
]

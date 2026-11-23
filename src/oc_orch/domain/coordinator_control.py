"""对话控制纯规则：项目调谐意图、回合 hold、用户回合屏障与判定快照。

零 I/O（只读状态机与严格校验）；持久化在
``oc_orch.adapters.coordinator_control.CoordinatorControlLedger``（meta 命名
空间 + 短事务），共享用例在
``oc_orch.application.coordinator_control.CoordinatorControlService``。

四维度相互独立（openspec improve-coordinator-continuity D2）：项目调谐意图
（running / explicitly_paused，带 revision 与原指令）、session 回合 hold、用户
回合屏障与 provider 执行条件。任何恢复只改自己维度：``resume`` 只改项目记录；
hold/begin/finish 只改 session 记录；二者都不删除 target-gate 命名空间的
provider 门，也不改写 Task/ACK/验收。会话入口负责把真实用户意图转换为结构化
调用，本模块**不**做关键词/自然语言控制（C03/C04）。

信任边界（fail-closed）：``begin_user_turn`` 只有 ``trusted is True``（由可证明
的显式本地/已核实来源判定）才建立屏障；未登记 REST 消息、任意 metadata 与
自动 wake 一律视为不可信，绝不自动视为真人（D1/C04/C12）。

幂等边界：项目操作以**持久稳定 operation 索引**（adapter 的 meta 命名空间
``coordinator-control:operation:{id}``）为准。同 operation_id + 同参数 → 幂等
``already-applied``（不修改当前状态）；同 operation_id + 参数变更 →
``operation-conflict``（历史重放不得成为新暂停，也不得冒认为未出现）。

session 记录带单调 ``watermark``：任何新事件（hold/输入/结束证据）必须严格
新于水位，保证“finish 后迟到旧中断不能重建 hold”“旧 idle 不能清更新屏障”。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

CONTROL_SCHEMA = 1
PROJECT_RUNNING = "running"
PROJECT_PAUSED = "paused"
PROJECT_STATES = frozenset({PROJECT_RUNNING, PROJECT_PAUSED})

# 有效时钟边界（毫秒）：与 target-gate 一致，拒绝 10**30 类垃圾时间。
MAX_CLOCK_MS = 8 * 10 ** 12

# 项目记录键（持久 JSON 字段）。
P_KEY_SCHEMA = "schema"
P_KEY_PROJECT = "project"
P_KEY_STATUS = "status"
P_KEY_REVISION = "revision"
P_KEY_SESSION = "session_id"          # 最后一次成功操作的调用 session
P_KEY_OPERATION = "operation_id"      # 最后一次成功操作的原指令引用
P_KEY_LAST_OPERATION = "last_operation"  # "pause" | "resume"（展示缓存）
P_KEY_FINGERPRINT = "last_fingerprint"
P_KEY_REASON = "reason"
P_KEY_EVIDENCE = "evidence"
P_KEY_UPDATED = "updated_at"

# session 记录键（持久 JSON 字段）。
S_KEY_SCHEMA = "schema"
S_KEY_SESSION = "session_id"
S_KEY_REVISION = "revision"
S_KEY_HOLD = "hold"
S_KEY_BARRIER = "barrier"
S_KEY_WATERMARK = "watermark"         # 单调水位：事件时间必须严格更新
S_KEY_UPDATED = "updated_at"

# 稳定原因代码（adapter/service 原样透传；拒绝路径 refuse 使用同一代码）。
R_INVALID_PROJECT = "invalid-project"
R_INVALID_SESSION = "invalid-session"
R_INVALID_OPERATION_ID = "invalid-operation-id"
R_INVALID_REASON = "invalid-reason"
R_INVALID_REVISION = "invalid-revision"
R_INVALID_EPOCH = "invalid-epoch"
R_INVALID_EVIDENCE = "invalid-evidence"
R_OPERATION_CONFLICT = "operation-conflict"   # 同 operation_id、参数变更不得冒认重复
R_ALREADY_PAUSED = "already-paused"
R_NOT_PAUSED = "not-paused"
R_REVISION_CHANGED = "revision-changed"       # CAS：旧恢复/旧输入不能覆盖新状态
R_INVALID_EVENT_ID = "invalid-event-id"
R_INVALID_ORIGIN_AT = "invalid-origin-at"
R_INVALID_MESSAGE_ID = "invalid-message-id"
R_INVALID_IDLE_ID = "invalid-idle-id"
R_INVALID_IDLE_AT = "invalid-idle-at"
R_STALE_EVENT = "stale-event"         # 迟到旧事件（<= watermark：旧中断/旧输入）
R_STALE_TERMINATION = "stale-termination"   # 旧 idle 不能清更新屏障
R_MESSAGE_MISMATCH = "message-mismatch"     # 结束证据不归属当前原输入
R_NO_ACTIVE_TURN = "no-active-turn"
R_INTERRUPTED_TURN = "interrupted-turn"   # 当前回合曾被中断，回合自身结束不解除
R_UNTRUSTED = "untrusted"             # 不可信来源不得建立用户回合屏障
R_ALREADY_APPLIED = "already-applied"     # 幂等重复（executed=True, applied=False）
R_NO_ACTIVE_HOLD = "no-active-hold"   # 重复解除：当前无 hold（幂等成功，不写）
R_TURN_ACTIVE = "turn-active"         # 已有用户回合屏障：先结束/观察该回合
R_HOLD_NOT_RELEASABLE = "hold-not-releasable"  # 只允许显式运维解除回合中断类 hold

# 仅这些“只停止当前回答”的回合 hold 允许显式运维解除；provider/暂停/未知
# 来源不提供一键放行。
RELEASABLE_HOLD_REASONS = frozenset({"coordinator-interrupted"})

_COERCE_FAILED = object()


class CoordinatorControlStateError(RuntimeError):
    """控制记录损坏/结构非法/schema 不兼容。

    调用方必须视作状态未知：保留原输入与身份，转可见告警/人工处置，绝不
    视为无控制放行（fail-closed）。
    """


def is_nk_str(value: Any) -> bool:
    """非空字符串。"""
    return isinstance(value, str) and bool(value.strip())


def valid_ms(value: Any) -> int | None:
    """非负纯数值（int 非 bool）且在有效时钟边界内；否则 None。"""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if 0 <= value <= MAX_CLOCK_MS else None
    return None


def _is_rev(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def coerce_reason(reason: Any, *, limit: int = 200) -> str | None:
    """原因必须是字符串且非空白；规范化（截断 200）后返回，否则 None。"""
    if reason is None:
        return None
    if not isinstance(reason, str):
        return None
    text = reason.strip()
    if not text:
        return None
    return text[:limit]


def canonic_value(value: Any) -> Any:
    """JSON 规范形（键序归一、可哈希），失败返回 _COERCE_FAILED 哨兵。"""
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), default=str)
        return json.loads(text)
    except (TypeError, ValueError):
        return _COERCE_FAILED


def operation_fingerprint(action: str, *, project: str, session_id: str,
                          operation_id: str, reason: str,
                          evidence: Any) -> str:
    """操作意图指纹：动作 + 全部业务参数（不含 expected_revision/epoch）。

    同 operation_id 重试时，参数任一变化都产生不同指纹 → 拒绝
    ``operation-conflict``，防止参数变更冒认重复。
    """
    payload = {
        "action": action, "project": project, "session_id": session_id,
        "operation_id": operation_id, "reason": reason, "evidence": evidence,
    }
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---- 记录结构严格校验（fail-closed） -------------------------------
def _bad(session_or_project: str, msg: str, *, entity: str) -> CoordinatorControlStateError:
    return CoordinatorControlStateError(
        f"coordinator-control {entity} 记录非法（{session_or_project}）：{msg}")


def validate_project_record(rec: Any, project: str) -> None:
    """持久项目记录验证；任何坏字段抛 CoordinatorControlStateError。"""
    entity = "project"
    if not isinstance(rec, dict):
        raise _bad(project, "非 dict", entity=entity)
    if rec.get(P_KEY_SCHEMA) != CONTROL_SCHEMA:
        raise _bad(project, f"schema={rec.get(P_KEY_SCHEMA)!r}", entity=entity)
    if rec.get(P_KEY_PROJECT) != project:
        raise _bad(project, f"project 不匹配 {rec.get(P_KEY_PROJECT)!r}",
                   entity=entity)
    if rec.get(P_KEY_STATUS) not in PROJECT_STATES:
        raise _bad(project, f"status={rec.get(P_KEY_STATUS)!r}", entity=entity)
    if not _is_rev(rec.get(P_KEY_REVISION)):
        raise _bad(project, "revision 非非负整数", entity=entity)
    if not is_nk_str(rec.get(P_KEY_SESSION)):
        raise _bad(project, "session_id 缺失/非法", entity=entity)
    if not is_nk_str(rec.get(P_KEY_OPERATION)):
        raise _bad(project, "operation_id 缺失/非法", entity=entity)
    if rec.get(P_KEY_LAST_OPERATION) not in ("pause", "resume"):
        raise _bad(project, f"last_operation={rec.get(P_KEY_LAST_OPERATION)!r}",
                   entity=entity)
    if not is_nk_str(rec.get(P_KEY_FINGERPRINT)):
        raise _bad(project, "last_fingerprint 缺失/非法", entity=entity)
    if not isinstance(rec.get(P_KEY_REASON), str):
        raise _bad(project, "reason 非 str", entity=entity)
    if rec.get(P_KEY_EVIDENCE) is not None and not isinstance(
            rec.get(P_KEY_EVIDENCE), dict):
        raise _bad(project, "evidence 非 dict/None", entity=entity)
    if valid_ms(rec.get(P_KEY_UPDATED)) is None:
        raise _bad(project, "updated_at 超界/非法", entity=entity)


def _validate_hold(rec: Any, session_id: str) -> None:
    entity = "session"
    hold = rec.get(S_KEY_HOLD)
    if hold is None:
        return
    if not isinstance(hold, dict):
        raise _bad(session_id, "hold 非 dict/None", entity=entity)
    if not is_nk_str(hold.get("event_id")):
        raise _bad(session_id, "hold.event_id 缺失/非法", entity=entity)
    if valid_ms(hold.get("origin_at")) is None:
        raise _bad(session_id, "hold.origin_at 超界/非法", entity=entity)
    if not isinstance(hold.get("reason"), str):
        raise _bad(session_id, "hold.reason 非 str", entity=entity)
    if valid_ms(hold.get("held_at")) is None:
        raise _bad(session_id, "hold.held_at 超界/非法", entity=entity)


def _validate_barrier(rec: Any, session_id: str) -> None:
    entity = "session"
    barrier = rec.get(S_KEY_BARRIER)
    if barrier is None:
        return
    if not isinstance(barrier, dict):
        raise _bad(session_id, "barrier 非 dict/None", entity=entity)
    if not is_nk_str(barrier.get("message_id")):
        raise _bad(session_id, "barrier.message_id 缺失/非法", entity=entity)
    if valid_ms(barrier.get("origin_at")) is None:
        raise _bad(session_id, "barrier.origin_at 超界/非法", entity=entity)
    if not is_nk_str(barrier.get("operation_id")):
        raise _bad(session_id, "barrier.operation_id 缺失/非法", entity=entity)
    if barrier.get("trusted") is not True:
        raise _bad(session_id, "barrier.trusted 必须严格为 true（bool）",
                   entity=entity)
    if valid_ms(barrier.get("began_at")) is None:
        raise _bad(session_id, "barrier.began_at 超界/非法", entity=entity)


def validate_session_record(rec: Any, session_id: str) -> None:
    """持久 session 记录验证；任何坏字段抛 CoordinatorControlStateError。"""
    entity = "session"
    if not isinstance(rec, dict):
        raise _bad(session_id, "非 dict", entity=entity)
    if rec.get(S_KEY_SCHEMA) != CONTROL_SCHEMA:
        raise _bad(session_id, f"schema={rec.get(S_KEY_SCHEMA)!r}", entity=entity)
    if rec.get(S_KEY_SESSION) != session_id:
        raise _bad(session_id, f"session_id 不匹配 {rec.get(S_KEY_SESSION)!r}",
                   entity=entity)
    if not _is_rev(rec.get(S_KEY_REVISION)):
        raise _bad(session_id, "revision 非非负整数", entity=entity)
    wm = rec.get(S_KEY_WATERMARK)
    if not _is_rev(wm):
        raise _bad(session_id, "watermark 非非负整数", entity=entity)
    _validate_hold(rec, session_id)
    _validate_barrier(rec, session_id)
    if valid_ms(rec.get(S_KEY_UPDATED)) is None:
        raise _bad(session_id, "updated_at 超界/非法", entity=entity)


# ---- 干净默认快照 ---------------------------------------------------
def project_default(project: str) -> dict:
    """无记录时的清晰默认：running / revision=0 / 无原指令。

    revision=0 恒表示“从未显式暂停”。paused 只能由显式操作产生，因此
    无记录 = 允许调谐（与既有行为一致）。
    """
    return {
        "project": project, "status": PROJECT_RUNNING, "revision": 0,
        "recorded": False, "schema": CONTROL_SCHEMA,
        "session_id": None, "operation_id": None, "last_operation": None,
        "reason": None, "evidence": None, "updated_at": None,
        "paused": False, "pause": None,
        "holder_session": None, "epoch": None,
    }


def session_default(session_id: str, projects: list[str] | None = None) -> dict:
    """无记录时的清晰默认：revision=0 / 无 hold / 无 barrier / 水位 0。"""
    return {
        "session_id": session_id, "revision": 0, "recorded": False,
        "schema": CONTROL_SCHEMA,
        "hold": None, "barrier": None, "turn": None,
        "hold_origin_at": None, "barrier_origin_at": None,
        "barrier_message_id": None,
        "watermark": 0,
        "projects": sorted({p for p in (projects or ()) if is_nk_str(p)}),
        "updated_at": None,
    }


def project_snapshot(rec: dict, lease: dict | None = None) -> dict:
    """持久项目记录 → 展示快照（含 lease holder/epoch 只读引用）。"""
    paused = rec[P_KEY_STATUS] == PROJECT_PAUSED
    holder = lease.get("holder_session") if lease else None
    epoch = lease.get("epoch") if lease else None
    return {
        "project": rec[P_KEY_PROJECT], "status": rec[P_KEY_STATUS],
        "revision": rec[P_KEY_REVISION], "recorded": True,
        "schema": rec[P_KEY_SCHEMA],
        "session_id": rec[P_KEY_SESSION], "operation_id": rec[P_KEY_OPERATION],
        "last_operation": rec[P_KEY_LAST_OPERATION],
        "reason": rec[P_KEY_REASON],
        "evidence": (dict(rec[P_KEY_EVIDENCE])
                     if isinstance(rec.get(P_KEY_EVIDENCE), dict) else None),
        "updated_at": rec[P_KEY_UPDATED],
        "paused": paused,
        "pause": {
            "session_id": rec[P_KEY_SESSION],
            "operation_id": rec[P_KEY_OPERATION],
            "reason": rec[P_KEY_REASON],
            "evidence": (dict(rec[P_KEY_EVIDENCE])
                         if isinstance(rec.get(P_KEY_EVIDENCE), dict) else None),
            "updated_at": rec[P_KEY_UPDATED],
        } if paused else None,
        "holder_session": holder, "epoch": epoch,
    }


def session_snapshot(rec: dict, projects: list[str] | None = None) -> dict:
    """持久 session 记录 → 展示快照（含 hold/barrier 及便捷别名）。"""
    hold = dict(rec[S_KEY_HOLD]) if rec.get(S_KEY_HOLD) else None
    barrier = dict(rec[S_KEY_BARRIER]) if rec.get(S_KEY_BARRIER) else None
    return {
        "session_id": rec[S_KEY_SESSION], "revision": rec[S_KEY_REVISION],
        "recorded": True, "schema": rec[S_KEY_SCHEMA],
        "hold": hold, "barrier": barrier, "turn": barrier,
        "hold_origin_at": hold["origin_at"] if hold else None,
        "barrier_origin_at": barrier["origin_at"] if barrier else None,
        "barrier_message_id": barrier["message_id"] if barrier else None,
        "watermark": rec.get(S_KEY_WATERMARK, 0),
        "projects": sorted({p for p in (projects or ()) if is_nk_str(p)}),
        "updated_at": rec[S_KEY_UPDATED],
    }


def _new_project_record(project: str) -> dict:
    return {
        P_KEY_SCHEMA: CONTROL_SCHEMA, P_KEY_PROJECT: project,
        P_KEY_STATUS: PROJECT_RUNNING, P_KEY_REVISION: 0,
        P_KEY_SESSION: "", P_KEY_OPERATION: "", P_KEY_LAST_OPERATION: "",
        P_KEY_FINGERPRINT: "", P_KEY_REASON: "", P_KEY_EVIDENCE: None,
        P_KEY_UPDATED: 0,
    }


def _new_session_record(session_id: str) -> dict:
    return {
        S_KEY_SCHEMA: CONTROL_SCHEMA, S_KEY_SESSION: session_id,
        S_KEY_REVISION: 0, S_KEY_HOLD: None, S_KEY_BARRIER: None,
        S_KEY_WATERMARK: 0, S_KEY_UPDATED: 0,
    }


def _decision(*, accepted: bool, changed: bool, reason: str, next_rec=None,
              from_state=None, to_state=None) -> dict:
    return {"accepted": accepted, "changed": changed, "reason": reason,
            "next": next_rec, "from_state": from_state, "to_state": to_state}


def _reject(code: str) -> dict:
    return _decision(accepted=False, changed=False, reason=code)


def check_project_arguments(*, project, session_id, operation_id, reason,
                            expected_revision, expected_epoch=None,
                            evidence=None):
    """项目操作公共参数校验（不读存储）：稳定拒绝代码或 None（通过）。

    与 decide_project_* 共用同一校验，供 adapter 在租约/事务守卫前快速
    fail-closed，避免把坏参数误报成租约/CAS 错误。
    """
    if not is_nk_str(project):
        return R_INVALID_PROJECT
    if not is_nk_str(session_id):
        return R_INVALID_SESSION
    if not is_nk_str(operation_id):
        return R_INVALID_OPERATION_ID
    if coerce_reason(reason) is None:
        return R_INVALID_REASON
    if not _is_rev(expected_revision):
        return R_INVALID_REVISION
    if expected_epoch is not None:
        if (not isinstance(expected_epoch, int)
                or isinstance(expected_epoch, bool) or expected_epoch < 1):
            return R_INVALID_EPOCH
    if evidence is not None:
        canon = canonic_value(evidence)
        if canon is _COERCE_FAILED or not isinstance(canon, dict):
            return R_INVALID_EVIDENCE
    return None


def _normalize_project_params(*, project, session_id, operation_id, reason,
                              expected_revision, evidence):
    """公共参数校验：返回规范形 (project, session_id, operation_id, reason,
    canon_evidence) 或 None（失败原因见 _reject 调用处）。"""
    if not is_nk_str(project):
        return None, R_INVALID_PROJECT
    if not is_nk_str(session_id):
        return None, R_INVALID_SESSION
    if not is_nk_str(operation_id):
        return None, R_INVALID_OPERATION_ID
    norm_reason = coerce_reason(reason)
    if norm_reason is None:
        return None, R_INVALID_REASON
    if not _is_rev(expected_revision):
        return None, R_INVALID_REVISION
    canon = None
    if evidence is not None:
        canon = canonic_value(evidence)
        if canon is _COERCE_FAILED or not isinstance(canon, dict):
            return None, R_INVALID_EVIDENCE
    return (project, session_id, operation_id, norm_reason, canon)


def _history_conflict(applied_fingerprint, fp):
    """持久 operation 索引判定：None 表示新操作；(code, idempotent) 否则。"""
    if applied_fingerprint is None:
        return None
    if applied_fingerprint == fp:
        return (R_ALREADY_APPLIED, True)
    return (R_OPERATION_CONFLICT, False)


def decide_project_pause(current, *, project, session_id, operation_id,
                         reason, expected_revision, evidence,
                         applied_fingerprint=None, now=0):
    """项目暂停纯决策：operation 索引幂等 + CAS + 状态转移（不读租约）。

    返回 ``{accepted, changed, reason, next, from_state, to_state}``：
    - accepted=True, changed=True：写入 ``next`` 并审计（from→to）；
    - accepted=True, changed=False：幂等重复（``already-applied``），不写；
    - accepted=False：稳定拒绝原因（CAS/冲突/已是目标态/历史重放）。
    """
    ok = _normalize_project_params(
        project=project, session_id=session_id, operation_id=operation_id,
        reason=reason, expected_revision=expected_revision, evidence=evidence)
    if ok[0] is None:
        return _reject(ok[1])
    _, _, _, norm_reason, canon = ok
    fp = operation_fingerprint("pause", project=project, session_id=session_id,
                               operation_id=operation_id, reason=norm_reason,
                               evidence=canon)
    hist = _history_conflict(applied_fingerprint, fp)
    if hist is not None:
        return _decision(accepted=True, changed=False, reason=hist[0]) \
            if hist[1] else _reject(hist[0])
    # 记录级兜底（历史索引缺失时）：同 op 同指纹视为已应用
    if current is not None:
        if (current.get(P_KEY_LAST_OPERATION) == "pause"
                and current.get(P_KEY_OPERATION) == operation_id):
            if current.get(P_KEY_FINGERPRINT) == fp:
                return _decision(accepted=True, changed=False,
                                 reason=R_ALREADY_APPLIED)
            return _reject(R_OPERATION_CONFLICT)
        if current[P_KEY_STATUS] == PROJECT_PAUSED:
            return _reject(R_ALREADY_PAUSED)
    cur_rev = current[P_KEY_REVISION] if current else 0
    if expected_revision != cur_rev:
        return _reject(R_REVISION_CHANGED)
    from_state = current[P_KEY_STATUS] if current else PROJECT_RUNNING
    base = dict(current) if current else _new_project_record(project)
    next_rec = dict(base)
    next_rec.update({
        P_KEY_STATUS: PROJECT_PAUSED,
        P_KEY_REVISION: cur_rev + 1,
        P_KEY_SESSION: session_id,
        P_KEY_OPERATION: operation_id,
        P_KEY_LAST_OPERATION: "pause",
        P_KEY_FINGERPRINT: fp,
        P_KEY_REASON: norm_reason,
        P_KEY_EVIDENCE: canon,
        P_KEY_UPDATED: now,
    })
    return _decision(accepted=True, changed=True, reason="paused",
                     next_rec=next_rec, from_state=from_state,
                     to_state=PROJECT_PAUSED)


def decide_project_resume(current, *, project, session_id, operation_id,
                          reason, expected_revision, evidence,
                          applied_fingerprint=None, now=0):
    """项目继续纯决策：幂等；未暂停或 CAS 失败拒绝；只改项目维度。"""
    ok = _normalize_project_params(
        project=project, session_id=session_id, operation_id=operation_id,
        reason=reason, expected_revision=expected_revision, evidence=evidence)
    if ok[0] is None:
        return _reject(ok[1])
    _, _, _, norm_reason, canon = ok
    fp = operation_fingerprint("resume", project=project,
                               session_id=session_id,
                               operation_id=operation_id, reason=norm_reason,
                               evidence=canon)
    hist = _history_conflict(applied_fingerprint, fp)
    if hist is not None:
        return _decision(accepted=True, changed=False, reason=hist[0]) \
            if hist[1] else _reject(hist[0])
    if current is None:
        return _reject(R_NOT_PAUSED)
    cur_rev = current[P_KEY_REVISION]
    if (current.get(P_KEY_LAST_OPERATION) == "resume"
            and current.get(P_KEY_OPERATION) == operation_id):
        if current.get(P_KEY_FINGERPRINT) == fp:
            return _decision(accepted=True, changed=False,
                             reason=R_ALREADY_APPLIED)
        return _reject(R_OPERATION_CONFLICT)
    if current[P_KEY_STATUS] == PROJECT_RUNNING:
        return _reject(R_NOT_PAUSED)
    if expected_revision != cur_rev:
        return _reject(R_REVISION_CHANGED)
    next_rec = dict(current)
    next_rec.update({
        P_KEY_STATUS: PROJECT_RUNNING,
        P_KEY_REVISION: cur_rev + 1,
        P_KEY_SESSION: session_id,
        P_KEY_OPERATION: operation_id,
        P_KEY_LAST_OPERATION: "resume",
        P_KEY_FINGERPRINT: fp,
        P_KEY_REASON: norm_reason,
        P_KEY_EVIDENCE: canon,
        P_KEY_UPDATED: now,
    })
    return _decision(accepted=True, changed=True, reason="resumed",
                     next_rec=next_rec, from_state=PROJECT_PAUSED,
                     to_state=PROJECT_RUNNING)


def decide_hold(current, *, session_id, event_id, origin_at, reason, now):
    """回合 hold 纯决策：迟到旧事件（<= 水位）拒绝；同事件幂等；不清 barrier。

    hold 事件必须严格新于当前水位（含当前 hold/barrier 的 origin），保证
    “finish 后迟到旧中断不能重建 hold”。
    """
    if not is_nk_str(session_id):
        return _reject(R_INVALID_SESSION)
    if not is_nk_str(event_id):
        return _reject(R_INVALID_EVENT_ID)
    at = valid_ms(origin_at)
    if at is None:
        return _reject(R_INVALID_ORIGIN_AT)
    norm_reason = coerce_reason(reason)
    if norm_reason is None:
        return _reject(R_INVALID_REASON)
    cur_hold = current[S_KEY_HOLD] if current else None
    cur_barrier = current[S_KEY_BARRIER] if current else None
    wm = current[S_KEY_WATERMARK] if current else 0
    # 同事件同刻 → 幂等
    if cur_hold is not None and cur_hold["event_id"] == event_id \
            and at == cur_hold["origin_at"]:
        return _decision(accepted=True, changed=False, reason=R_ALREADY_APPLIED)
    # 迟到旧事件：不得早于/等于当前水位（水位 >= 当前 hold/barrier origin）
    limit = max(wm, cur_barrier["origin_at"] if cur_barrier else 0,
                cur_hold["origin_at"] if cur_hold else 0)
    if at <= limit:
        return _reject(R_STALE_EVENT)
    base = dict(current) if current else _new_session_record(session_id)
    next_rec = dict(base)
    next_rec.update({
        S_KEY_HOLD: {"event_id": event_id, "origin_at": at,
                     "reason": norm_reason, "held_at": now},
        S_KEY_WATERMARK: at,
        S_KEY_REVISION: (base[S_KEY_REVISION] or 0) + 1,
        S_KEY_UPDATED: now,
    })
    return _decision(accepted=True, changed=True, reason="held",
                     next_rec=next_rec,
                     from_state="held" if cur_hold else "no-hold",
                     to_state=f"held:{event_id}")


def decide_begin_turn(current, *, session_id, message_id, origin_at,
                      operation_id, trusted, now):
    """建立/更新用户回合屏障纯决策。

    ``trusted`` 必须严格 ``is True``（fail-closed）：未登记 REST 消息、metadata、
    自动 wake 绝不能自动视为真人。输入必须严格新于当前水位（跟随最新有效
    输入，C11）；不清 hold，也不解除项目暂停。
    """
    if trusted is not True:
        return _reject(R_UNTRUSTED)
    if not is_nk_str(session_id):
        return _reject(R_INVALID_SESSION)
    if not is_nk_str(message_id):
        return _reject(R_INVALID_MESSAGE_ID)
    if not is_nk_str(operation_id):
        return _reject(R_INVALID_OPERATION_ID)
    at = valid_ms(origin_at)
    if at is None:
        return _reject(R_INVALID_ORIGIN_AT)
    cur_barrier = current[S_KEY_BARRIER] if current else None
    cur_hold = current[S_KEY_HOLD] if current else None
    wm = current[S_KEY_WATERMARK] if current else 0
    # 同输入同刻 → 幂等
    if cur_barrier is not None and cur_barrier["message_id"] == message_id \
            and at == cur_barrier["origin_at"]:
        return _decision(accepted=True, changed=False, reason=R_ALREADY_APPLIED)
    # 迟到旧输入：必须严格新于水位（含当前 hold/barrier 的 origin）
    limit = max(wm, cur_barrier["origin_at"] if cur_barrier else 0,
                cur_hold["origin_at"] if cur_hold else 0)
    if at <= limit:
        return _reject(R_STALE_EVENT)
    base = dict(current) if current else _new_session_record(session_id)
    next_rec = dict(base)
    next_rec.update({
        S_KEY_BARRIER: {"message_id": message_id, "origin_at": at,
                        "operation_id": operation_id, "trusted": True,
                        "began_at": now},
        S_KEY_WATERMARK: at,
        S_KEY_REVISION: (base[S_KEY_REVISION] or 0) + 1,
        S_KEY_UPDATED: now,
    })
    return _decision(accepted=True, changed=True, reason="barrier-set",
                     next_rec=next_rec,
                     from_state="barrier" if cur_barrier else "no-barrier",
                     to_state=f"barrier:{message_id}")


def decide_finish_turn(current, *, session_id, message_id, idle_id, idle_at,
                       expected_revision, now):
    """用户回合结束纯决策（精确原输入 + 唯一终态证据契约，fail-closed）。

    - 必须精确关联当前 barrier 的 message（旧输入结束拒绝 message-mismatch）；
    - idle_at 必须严格新于水位/barrier 起点（旧 idle 拒绝 stale-termination；
      完成后水位推进，迟到的旧中断/旧证据不能重建 hold 或清更新屏障）；
    - 当前回合曾被中断（hold 不早于 barrier）→ 回合自身结束不解除 hold/屏障
      （interrupted-turn，C10/C11）；
    - 未被中断的新回合（hold 早于 barrier）结束 → 同时解除对应 hold（C10）；
    - expected_revision 与 revision 不符 → revision-changed（CAS）。
    """
    if not is_nk_str(session_id):
        return _reject(R_INVALID_SESSION)
    if not _is_rev(expected_revision):
        return _reject(R_INVALID_REVISION)
    cur_rev = current[S_KEY_REVISION] if current else 0
    if expected_revision != cur_rev:
        return _reject(R_REVISION_CHANGED)
    if current is None or current[S_KEY_BARRIER] is None:
        return _reject(R_NO_ACTIVE_TURN)
    barrier = current[S_KEY_BARRIER]
    if (not is_nk_str(message_id)
            or message_id != barrier["message_id"]):
        return _reject(R_MESSAGE_MISMATCH)
    if not is_nk_str(idle_id):
        return _reject(R_INVALID_IDLE_ID)
    at = valid_ms(idle_at)
    if at is None:
        return _reject(R_INVALID_IDLE_AT)
    wm = current[S_KEY_WATERMARK] if current else 0
    if at <= max(wm, barrier["origin_at"]):
        return _reject(R_STALE_TERMINATION)
    hold = current[S_KEY_HOLD]
    if hold is not None and hold["origin_at"] >= barrier["origin_at"]:
        return _reject(R_INTERRUPTED_TURN)
    release_hold = hold is not None and hold["origin_at"] < barrier["origin_at"]
    next_rec = dict(current)
    next_rec.update({
        S_KEY_BARRIER: None,
        S_KEY_WATERMARK: max(wm, at),
        S_KEY_REVISION: cur_rev + 1,
        S_KEY_UPDATED: now,
    })
    if release_hold:
        next_rec[S_KEY_HOLD] = None
    return _decision(
        accepted=True, changed=True,
        reason="finished-with-hold-release" if release_hold else "finished",
        next_rec=next_rec, from_state=f"barrier:{barrier['message_id']}",
        to_state="no-barrier")


def decide_release_hold(current, *, session_id, operation_id, reason,
                        expected_revision, evidence=None, now=0):
    """显式运维解除回合 hold 纯决策（只改 session hold 维度）。

    - 只允许 ``RELEASABLE_HOLD_REASONS`` 中的原因；显式项目暂停、provider
      条件与用户回合屏障都不在本函数处理，避免“一键恢复”变成跨维度放行；
    - 有 barrier（用户回合进行中/待核验）时拒绝，先由可信回合结束或观察器
      结束对应屏障；无 hold 是幂等成功（changed=False，不写、不审计）；
    - CAS ``expected_revision``：读取旧版本后若出现新中断/新回合，旧解除被拒；
    - 不清 watermark：解除后迟到的旧中断仍被水位拒绝，新的真实中断可重建 hold。
    """
    if not is_nk_str(session_id):
        return _reject(R_INVALID_SESSION)
    if not is_nk_str(operation_id):
        return _reject(R_INVALID_OPERATION_ID)
    norm_reason = coerce_reason(reason)
    if norm_reason is None:
        return _reject(R_INVALID_REASON)
    if not _is_rev(expected_revision):
        return _reject(R_INVALID_REVISION)
    canon = None
    if evidence is not None:
        canon = canonic_value(evidence)
        if canon is _COERCE_FAILED or not isinstance(canon, dict):
            return _reject(R_INVALID_EVIDENCE)
    if current is None or current.get(S_KEY_HOLD) is None:
        return _decision(accepted=True, changed=False, reason=R_NO_ACTIVE_HOLD)
    hold = current[S_KEY_HOLD]
    if current.get(S_KEY_BARRIER) is not None:
        return _reject(R_TURN_ACTIVE)
    if hold.get("reason") not in RELEASABLE_HOLD_REASONS:
        return _reject(R_HOLD_NOT_RELEASABLE)
    cur_rev = current[S_KEY_REVISION]
    if expected_revision != cur_rev:
        return _reject(R_REVISION_CHANGED)
    next_rec = dict(current)
    next_rec.update({
        S_KEY_HOLD: None,
        S_KEY_REVISION: cur_rev + 1,
        S_KEY_UPDATED: now,
    })
    return _decision(accepted=True, changed=True, reason="hold-released",
                     next_rec=next_rec,
                     from_state=f"held:{hold.get('event_id')}",
                     to_state="no-hold")


def decide_permits(project_snapshot: dict, session_snapshot: dict) -> dict:
    """自动动作的必要条件判定（纯组合；不授予任何权利）。

    只检查本控制底座的三维（项目暂停 / 回合 hold / 用户回合屏障）；
    provider 执行条件（target-gate）与任务/写权/路由守卫由 runtime 另行组合。
    """
    blocking: list[str] = []
    if project_snapshot.get("status") == PROJECT_PAUSED:
        blocking.append("project-paused")
    if session_snapshot.get("barrier") is not None:
        blocking.append("user-turn-active")
    if session_snapshot.get("hold") is not None:
        blocking.append("turn-held")
    return {"permits": not blocking, "blocking": blocking}
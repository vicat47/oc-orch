"""S4 受管输入纯规则：意图校验、人工确认、闸门/队列分类与四态收据视图。

零 I/O、零台账写、不反向依赖 adapters；application/managed_inputs.py 用它做
前置校验与视图派生，Ledger 只持久事实。安全边界（用户 2026-11-09 确认）：

- execute / park / record 意图必须显式；execute→resume=True，park→resume=False，
  record 无发送阶段（纯本地留档，不构造 Client、不读目标）。
- park 必须带 owner / flush_condition / expires_at，到期策略仅 ``alert``：
  到期只告警，绝不自动 cancel / flush / 执行。
- 收据四态统一为字符串派生字段：``admission``（原受理状态）、``delivery`` ∈
  pending/delivered/cancelled/unknown/not-applicable、``execution`` ∈
  unknown/succeeded/failed/interrupted/not-applicable、``business_ack`` ∈
  unacknowledged/acknowledged/not-applicable。HTTP 200、旧 session outcome、
  inbox 消失都不单独冒充 delivered/executed。
- 列表默认不含正文（body）；只有单输入 get 可显示正文。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .rules import ATTEMPT_ACTIVE, TASK_TERMINAL

# ---- 托管发送 / 后续责任边界 ------------------------------------------------
TRACKED = "tracked"
UNTRACKED = "untracked"
WORK_OBSERVE = "observe"
WORK_READ_ONLY = "read_only"
WORK_WRITE = "write"
WORK_DECLARED_KINDS = (WORK_OBSERVE, WORK_READ_ONLY, WORK_WRITE)
WORK_KINDS = (WORK_OBSERVE, WORK_WRITE)
DELIVERY_MODES = ("queue", "steer")
# 结构化接续报告协议：kind/id 字段固定，禁止只看自然语言文本。
CONTINUATION_SCHEMA = "oc-orch.continuation.v1"
CONTINUATION_BINDING = ("input_id", "session_id", "message_id", "task_id",
                        "attempt_id", "generation", "result_version")
REPORT_KINDS = ("completed", "blocked", "needs_material", "needs_decision")
# work_kind=None 表示调用方未声明；不得默认成已授权只读，也不得默认成写。
UNTRACKED_BOUNDARY = (
    "untracked 未托管纯通信模式：未登记任务写权/后续责任，不保证托管 worker 的"
    "执行后接续；需要托管推进时请提供 task/attempt 归属并关闭 untracked")
EXTERNAL_INPUT_BOUNDARY = (
    "external-owner-watch：非任务托管外部分支；输入执行/核收责任归显式 owner，"
    "不静默宣称 Task/Attempt 托管，也不授予任务写权")

# ---- 意图与 resume 位 -------------------------------------------------------
INTENT_EXECUTE = "execute"
INTENT_PARK = "park"
INTENT_RECORD = "record"
INTENTS = (INTENT_EXECUTE, INTENT_PARK, INTENT_RECORD)

# intent → prompt resume 位（record 不发送）。
RESUME_BY_INTENT = {INTENT_EXECUTE: True, INTENT_PARK: False, INTENT_RECORD: None}

# 本轮用户确认：park 到期策略只能是告警。
EXPIRE_POLICY_ALERT = "alert"

# ---- 四态视图字符串 --------------------------------------------------------
DELIVERY_PENDING = "pending"
DELIVERY_DELIVERED = "delivered"
DELIVERY_CANCELLED = "cancelled"
DELIVERY_UNKNOWN = "unknown"
DELIVERY_NA = "not-applicable"
DELIVERY_VALUES = (DELIVERY_PENDING, DELIVERY_DELIVERED, DELIVERY_CANCELLED,
                   DELIVERY_UNKNOWN, DELIVERY_NA)

EXECUTION_UNKNOWN = "unknown"
EXECUTION_NA = "not-applicable"
EXECUTION_OUTCOMES = ("succeeded", "failed", "interrupted")
EXECUTION_VALUES = (EXECUTION_UNKNOWN, *EXECUTION_OUTCOMES, EXECUTION_NA)

ACK_UNACKNOWLEDGED = "unacknowledged"
ACK_ACKNOWLEDGED = "acknowledged"
ACK_NA = "not-applicable"
ACK_VALUES = (ACK_UNACKNOWLEDGED, ACK_ACKNOWLEDGED, ACK_NA)

# 闸门允许 act 的状态：无闸门（None）或 probe-ready（无活动阻塞）。
GATE_READY_STATE = "probe-ready"

# 处于“排队/待投递”的受理状态（delivery 派生为 pending）。
_PENDING_ADMISSION = ("prepared", "possible", "admitted")
_UNKNOWN_ADMISSION = ("unknown", "expired", "released")
_RECORD_ADMISSION = ("recorded",)


# ---- 校验 ------------------------------------------------------------------
def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _int_not_bool(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def validate_send_payload(session_id, *, intent, text, owner, request_id,
                          project="", expires_at=None,
                          flush_condition=None) -> dict:
    """发送意图/责任校验；纯函数，任何缺项在 Client I/O 之前拒绝。

    返回 ``{"valid": bool, "reason": str|None, "errors": [str...]}``。
    park 必须提供 expires_at（非负 int）与 flush_condition（非空 str）。
    """
    errors: list[str] = []
    if intent not in INTENTS:
        errors.append(f"intent 必须是 {'/'.join(INTENTS)}（收到 {intent!r}）")
    if not _non_empty_str(session_id):
        errors.append("session_id 必须是非空字符串")
    if not _non_empty_str(text):
        errors.append("text 正文必须是非空字符串")
    if not _non_empty_str(owner):
        errors.append("owner 必须是非空字符串")
    if not _non_empty_str(request_id):
        errors.append("request_id 必须是非空字符串")
    if project is not None and not isinstance(project, str):
        errors.append("project 必须是字符串")
    if intent == INTENT_PARK:
        if not _int_not_bool(expires_at) or expires_at < 0:
            errors.append("park 必须提供非负 int expires_at")
        if not _non_empty_str(flush_condition):
            errors.append("park 必须提供非空 flush_condition")
    return {"valid": not errors, "reason": "; ".join(errors) or None,
            "errors": errors}


def validate_confirmation(*, object_id, confirm, owner, expected_revision,
                          reason="", require_reason=True) -> dict:
    """人工操作对象确认：confirm 必须等于对象 ID，且 owner/revision(/reason) 齐。

    flush/cancel 的 object_id 是 input_id；resume_target 的 object_id 是
    session_id。返回 ``{"valid": bool, "reason": str|None, "errors": [...]}``。
    """
    errors: list[str] = []
    if confirm != object_id:
        errors.append(f"确认对象不匹配：confirm={confirm!r} != {object_id!r}")
    if not _non_empty_str(owner):
        errors.append("owner 必须是非空字符串")
    if not _int_not_bool(expected_revision) or expected_revision < 1:
        errors.append("expected_revision 必须是正整数")
    if require_reason and not _non_empty_str(reason):
        errors.append("reason 必须是非空原因")
    return {"valid": not errors, "reason": "; ".join(errors) or None,
            "errors": errors}


def normalize_work_kind(work_kind) -> str | None:
    """把显式工作意图规范化；None 保持未声明（不得默认 observe/write）。

    ``read_only`` 是 observe 的显式同义词；其他值返回原值由调用方拒绝。
    """
    if work_kind is None:
        return None
    if work_kind == WORK_READ_ONLY:
        return WORK_OBSERVE
    return work_kind


def validate_tracked_send(session_id, *, text, delivery, work_kind, untracked,
                          task_id=None, attempt_id=None, target_session=None,
                          owner=None, message_id=None, deadline=None,
                          materials=None) -> dict:
    """托管 send_tracked 的前置校验（纯函数，任何 I/O 之前拒绝）。

    ``work_kind=None`` 表示调用方未声明；不得默认成已授权只读或写。显式
    ``untracked=True`` 且 ``work_kind='write'`` 必须拒绝：纯通信不能借用托管
    入口取得任务写权。归属推断在 application 层完成，本函数只校验显式参数
    形状，不把缺失 task/attempt 当成已托管。
    """
    errors: list[str] = []
    if not _non_empty_str(session_id):
        errors.append("session_id 必须是非空字符串")
    if not _non_empty_str(text):
        errors.append("text 正文必须是非空字符串")
    if delivery not in DELIVERY_MODES:
        errors.append(f"delivery 必须是 {'/'.join(DELIVERY_MODES)}（收到 {delivery!r}）")
    if work_kind is not None and work_kind not in WORK_DECLARED_KINDS:
        errors.append(
            f"work_kind 必须是 {'/'.join(WORK_DECLARED_KINDS)} 或 None"
            f"（收到 {work_kind!r}）")
    if not isinstance(untracked, bool):
        errors.append("untracked 必须是 bool")
    if untracked and normalize_work_kind(work_kind) == WORK_WRITE:
        errors.append("untracked 纯通信不得声明 work_kind='write'（不授予任务写权）")
    for name, value in (("task_id", task_id), ("attempt_id", attempt_id),
                        ("target_session", target_session), ("owner", owner),
                        ("message_id", message_id)):
        if value is not None and not _non_empty_str(value):
            errors.append(f"{name} 必须是非空字符串/None")
    if deadline is not None and (not _int_not_bool(deadline) or deadline < 0):
        errors.append("deadline 必须是非负整数毫秒/None")
    if materials is not None and not isinstance(materials, (list, tuple, dict)):
        errors.append("materials 必须是 list/tuple/dict/None")
    return {"valid": not errors, "reason": "; ".join(errors) or None,
            "errors": errors}


def normalize_materials(materials) -> list:
    """把材料引用规范成有界 JSON 列表；只保留可定位引用，不读取文件内容。"""
    if materials is None:
        return []
    if isinstance(materials, dict):
        return [materials]
    return [m for m in list(materials) if isinstance(m, (str, dict))]


def write_send_block_reason(attempt: Any, task: Any) -> str | None:
    """已托管 worker 的写发送前置条件；返回拒绝原因或 None。

    旧 attempt 已执行结束/结果已收集/释放现场后，不能用继续输入重开旧终态，
    必须由 Orchestration.rework 以新 attempt 重新认领写范围。
    """
    if not isinstance(attempt, dict) or not attempt.get("id"):
        return "no-attempt"
    if not isinstance(task, dict) or not task.get("id"):
        return "no-task"
    if task.get("status") in TASK_TERMINAL:
        return "task-terminal"
    if attempt.get("site_released_at") is not None:
        return "site-released"
    if attempt.get("stop_requested_at") is not None or \
            attempt.get("stop_confirmed_at") is not None:
        return "stop-requested"
    if attempt.get("status") == "stop_requested":
        return "stop-requested"
    if attempt.get("status") not in ATTEMPT_ACTIVE:
        return "attempt-archived"
    if task.get("current_attempt_id") != attempt.get("id"):
        return "not-current"
    return None


def responsibility_condition_key(task_id, attempt_id, input_id,
                                 purpose="execution") -> str:
    """稳定责任条件指纹：同一输入/任务/尝试/用途重试不新建新业务身份。"""
    raw = "|".join(str(v or "") for v in (task_id, attempt_id, input_id, purpose))
    return "managed-responsibility:" + hashlib.sha256(
        raw.encode("utf-8")).hexdigest()[:24]


def validate_continuation_report(input_row: dict, report: Any,
                                 source_message_id: str) -> dict:
    """纯校验 oc-orch.continuation.v1；id/契约字段必须匹配原输入与 tracking。

    ``kind=completed`` 只表示 worker 结构化回报完成，不等于 Task accepted；
    任何 next_action/materials 文本都不参与状态推断。
    """
    errors: list[str] = []
    if not isinstance(report, dict):
        return {"valid": False, "reason": "report 必须是 dict",
                "errors": ["report 必须是 dict"], "report": None}
    if report.get("schema") != CONTINUATION_SCHEMA:
        errors.append(f"schema 必须是 {CONTINUATION_SCHEMA}")
    input_row = input_row if isinstance(input_row, dict) else {}
    for key, expected in (
        ("input_id", input_row.get("input_id")),
        ("session_id", input_row.get("session_id")),
        ("message_id", input_row.get("message_id")),
    ):
        if not isinstance(report.get(key), str) or report.get(key) != expected:
            errors.append(f"{key} 必须等于原输入 {expected!r}")
    if not _non_empty_str(source_message_id) or \
            source_message_id == input_row.get("message_id"):
        errors.append("source_message_id 必须是真实 assistant 报告来源消息，"
                      "不能用原 user 输入消息冒充归属")
    kind = report.get("kind")
    if kind not in REPORT_KINDS:
        errors.append(f"kind 必须是 {'/'.join(REPORT_KINDS)}（收到 {kind!r}）")
    if not _non_empty_str(report.get("next_action")):
        errors.append("next_action 必须是非空字符串")
    tracking = _tracking(input_row)
    for key in ("task_id", "attempt_id", "generation", "result_version"):
        expected = tracking.get(key)
        if expected is None:
            continue
        if key not in report or report.get(key) != expected:
            errors.append(f"{key} 必须匹配输入 tracking {expected!r}")
    normalized: dict[str, Any] = {}
    for key in ("schema", "input_id", "session_id", "message_id", "kind",
                "next_action", "task_id", "attempt_id", "generation",
                "result_version"):
        if key in report:
            normalized[key] = report.get(key)
    for key in ("materials", "missing_materials", "evidence_refs"):
        value = report.get(key)
        if value is None:
            continue
        if not isinstance(value, list) or any(
                not isinstance(item, (str, dict)) for item in value):
            errors.append(f"{key} 必须是 str/dict 列表")
            continue
        normalized[key] = list(value)
    decision = report.get("decision")
    if decision is not None:
        if not isinstance(decision, (str, dict)):
            errors.append("decision 必须是 str/dict/None")
        else:
            normalized["decision"] = decision
    return {"valid": not errors, "reason": "; ".join(errors) or None,
            "errors": errors, "report": normalized if not errors else None}


# ---- 闸门 / 队列分类 -------------------------------------------------------
def gateway_allows(gate: Any) -> str | None:
    """目标闸门放行判定：None/无阻塞 → None；否则返回阻止原因（fail-closed）。

    ``gate`` 为 ``target_gate.get`` 的快照（含 state）或 None。任何非 dict、
    缺 state 或非 ``probe-ready`` 的实际状态都按阻止处理——人工确认不得绕过。
    """
    if gate is None:
        return None
    if not isinstance(gate, dict):
        return "gate:malformed"
    state = gate.get("state")
    if state is None:
        return "gate:malformed"
    if state == GATE_READY_STATE:
        return None
    return f"gate:{state}"


def execute_queue_allowed(inbox_items, known_message_ids=()) -> dict:
    """execute 发送会 resume 并消费整个队列：非空（foreign 或自有 park）即拒绝。

    返回 ``{"allowed": bool, "reason": str, "detail": [...]}``。空队列才允许
    新建 resume=True 输入；不通过新增 prompt 绕过队头。
    """
    items = list(inbox_items or [])
    known = set(known_message_ids or ())
    detail = [{"id": (i or {}).get("id") if isinstance(i, dict) else None,
               "own": bool(isinstance(i, dict) and i.get("id") in known)}
              for i in items]
    if not items:
        return {"allowed": True, "reason": "empty-queue", "detail": detail}
    for i in items:
        if not isinstance(i, dict) or not _non_empty_str(i.get("id")):
            return {"allowed": False, "reason": "unknown-shape-item",
                    "detail": detail}
    return {"allowed": False, "reason": "nonempty-queue", "detail": detail}


def flush_queue_allowed(inbox_items, known_message_ids,
                        target_message_id) -> dict:
    """flush 要求完整队列可信且“唯一自有目标输入”。

    返回 ``{"allowed": bool, "reason": str, "detail": [...]}``。foreign / 其他
    自有 park / 未知形状一律 fail-closed；目标不在队列（target-absent）或空
    队列表示无内容可 steer（调用方按 no-op 处理，不假装执行）。
    """
    items = list(inbox_items or [])
    known = set(known_message_ids or ())
    detail = [{"id": (i or {}).get("id") if isinstance(i, dict) else None,
               "own": bool(isinstance(i, dict) and i.get("id") in known)}
              for i in items]
    if not items:
        return {"allowed": False, "reason": "target-absent", "detail": detail}
    for i in items:
        if not isinstance(i, dict) or not _non_empty_str(i.get("id")):
            return {"allowed": False, "reason": "unknown-shape-item",
                    "detail": detail}
    foreign = [i["id"] for i in items if i["id"] not in known]
    if foreign:
        return {"allowed": False, "reason": "foreign-queue",
                "detail": detail, "foreign": foreign}
    others = [i["id"] for i in items
              if i["id"] in known and i["id"] != target_message_id]
    if others:
        return {"allowed": False, "reason": "other-own-queued",
                "detail": detail, "others": others}
    if not any(i["id"] == target_message_id for i in items):
        return {"allowed": False, "reason": "target-absent", "detail": detail}
    return {"allowed": True, "reason": "own-unique", "detail": detail}


# ---- 视图派生 --------------------------------------------------------------
def _managed_block(input_row: dict) -> dict:
    raw = input_row.get("metadata")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            raw = None
    if not isinstance(raw, dict):
        return {}
    block = raw.get("managed_input")
    return block if isinstance(block, dict) else {}


def _tracking(input_row: dict) -> dict:
    """受管输入的任务/接续/责任引用块；旧输入没有该块时返回空。"""
    block = _managed_block(input_row)
    tracking = block.get("tracking")
    return tracking if isinstance(tracking, dict) else {}


def tracking_block(input_row: dict) -> dict:
    """公开只读视图：返回受管输入的 tracking 引用块副本。"""
    return dict(_tracking(input_row))



def continuation_binding(row: dict) -> dict:
    tracking = tracking_block(row)
    return {key: row[key] if key in ("input_id", "session_id", "message_id")
            else tracking.get(key) for key in CONTINUATION_BINDING}


def execution_prompt(row: dict) -> str:
    body = row["body"]
    if not tracking_block(row).get("same_candidate"):
        return body
    identity = dict(schema=CONTINUATION_SCHEMA, **continuation_binding(row))
    return (body + "\n\n处理这条同候选补充后，请在 assistant 回复中附独立的 JSON 代码块。"
            "保留下列全部标识字段，按事实添加 kind（completed / blocked / "
            "needs_material / needs_decision）及非空 next_action。另行给出业务验收结论。\n"
            + json.dumps(identity, ensure_ascii=False))


def _evidence(block: dict) -> dict:
    ev = block.get("evidence")
    return ev if isinstance(ev, dict) else {}


def _int_or_none(value: Any) -> int | None:
    return value if _int_not_bool(value) else None


_WATCH_OBS_FIELDS = ("session_id", "owner", "project", "trusted", "pending",
                     "pending_total", "pending_lower_bound", "truncated",
                     "coverage", "oldest_age_ms", "oldest_age_lower_bound_ms",
                     "aged_out", "blocking_reason", "next_action")


def pending_condition_key(ids) -> str:
    """稳定的 pending 条件指纹：ID 集合摘要（不含动态 age/正文），供告警去重。"""
    normalized = sorted(str(i) for i in (ids or ()) if i)
    return "watch-pending:" + hashlib.sha256(
        ",".join(normalized).encode("utf-8")).hexdigest()[:20]


def _last_observed_at(row: dict) -> int | None:
    obs = row.get("observations")
    if isinstance(obs, list) and obs:
        last = obs[-1]
        if isinstance(last, dict) and _int_not_bool(last.get("_at")):
            return last["_at"]
    last = row.get("last_observation")
    if isinstance(last, dict) and _int_not_bool(last.get("_at")):
        return last["_at"]
    return None


def _sanitize_watch_observation(row: dict) -> dict | None:
    """白名单脱敏最近观察；撤销/代次变更的旧观察标 stale+trusted=False。

    只暴露稳定诊断字段，绝不透传原始 error/正文/observations。``watch_revision``
    （C 写入）与当前 ``revision`` 不一致或 ``enabled=False`` → 旧代不再可信。
    """
    last = row.get("last_observation")
    if not isinstance(last, dict):
        return None
    out = {k: last.get(k) for k in _WATCH_OBS_FIELDS}
    obs_rev = last.get("watch_revision")
    out["watch_revision"] = obs_rev if _int_not_bool(obs_rev) else None
    current_rev = row.get("revision")
    stale = (not row.get("enabled", False)) or (
        _int_not_bool(obs_rev) and _int_not_bool(current_rev)
        and obs_rev != current_rev)
    out["stale"] = bool(stale)
    if stale:
        out["trusted"] = False
    return out


def delivery_state(input_row: dict) -> str:
    """仅派生当前 delivery 四态字符串（供用例判断终态，不含完整视图）。"""
    input_row = input_row if isinstance(input_row, dict) else {}
    block = _managed_block(input_row)
    return _delivery(input_row, block, input_row.get("intent") or "unknown")


def _delivery(input_row: dict, block: dict, intent: str) -> str:
    if intent == INTENT_RECORD:
        return DELIVERY_NA
    ev = _evidence(block)
    if ev.get("cancelled"):
        return DELIVERY_CANCELLED
    admission = input_row.get("admission_state")
    if admission == "delivered" or ev.get("delivered"):
        return DELIVERY_DELIVERED
    if admission in _PENDING_ADMISSION:
        return DELIVERY_PENDING
    if admission in ("unknown", "expired", "released"):
        return DELIVERY_UNKNOWN
    return DELIVERY_UNKNOWN


def _execution(block: dict, intent: str) -> str:
    if intent == INTENT_RECORD:
        return EXECUTION_NA
    fact = _evidence(block).get("execution")
    if isinstance(fact, dict):
        ref = (fact.get("detail") or {}).get("reference")
        outcome = ref.get("outcome") if isinstance(ref, dict) else None
        if outcome in EXECUTION_OUTCOMES:
            return outcome
    # 无充分归属证据：显示 unknown，绝不把旧 session outcome 冒充本输入执行。
    return EXECUTION_UNKNOWN


def _business_ack(block: dict, intent: str) -> str:
    if intent == INTENT_RECORD:
        return ACK_NA
    explicit = block.get("business_ack")
    if isinstance(explicit, dict) and isinstance(explicit.get("value"), bool):
        # 显式确认优先：runtime 晚到观察不覆盖已确认/未确认事实。
        return ACK_ACKNOWLEDGED if explicit["value"] else ACK_UNACKNOWLEDGED
    fact = _evidence(block).get("business_ack")
    if not isinstance(fact, dict):
        return ACK_UNACKNOWLEDGED
    detail = fact.get("detail") if isinstance(fact.get("detail"), dict) else {}
    ref = detail.get("reference") if isinstance(detail.get("reference"), dict) else {}
    # 只认显式 bool；value=False / acked=False 不得因 dict 真值显示 acknowledged。
    for key in ("value", "acked"):
        if isinstance(ref.get(key), bool):
            return ACK_ACKNOWLEDGED if ref[key] else ACK_UNACKNOWLEDGED
    return ACK_UNACKNOWLEDGED


def next_action(input_row: dict, *, now: int) -> str:
    """给 owner 的稳定下一动作建议（阻塞有原因、责任与下一步）。"""
    intent = input_row.get("intent") or "unknown"
    if intent == INTENT_RECORD:
        return "留档完成：纯本地记录，无远端发送与执行责任"
    block = _managed_block(input_row)
    delivery = _delivery(input_row, block, intent)
    if delivery == DELIVERY_DELIVERED:
        return "已投递：等待本输入归属执行证据与独立业务确认；执行证据不足保持 unknown"
    if delivery == DELIVERY_CANCELLED:
        return "已取消：无待处理动作；保留原回执供审计"
    if delivery == DELIVERY_UNKNOWN:
        return "结果未知：只按原 ID 对账，不重复 POST/PATCH/DELETE"
    flush = block.get("flush")
    if isinstance(flush, dict) and flush.get("flushed_at") is not None:
        return ("已 flush 转 execute（原 park 历史保留）：等待输入执行/核收；"
                "不再按 park 到期告警")
    expires_at = _int_or_none(block.get("expires_at"))
    if intent == INTENT_PARK and expires_at is not None and now > expires_at:
        return "park 已到期：按本轮策略只告警；人工决定唤醒，绝不自动 flush/cancel/执行"
    if intent == INTENT_PARK:
        return "park 等待中：到期只告警，不自动唤醒"
    return "已受理待投递：短观察窗口不判失败；到达对账期限后按原 ID 对账"


def receipt_view(input_row: dict, *, now: int, include_body: bool = False) -> dict:
    """构造统一收据视图（列表不返回正文；get/send 可返回正文）。

    固定字段：input_id/message_id/intent/resume/owner/revision/expires_at/
    pending_age_ms/next_action + 四态 admission/delivery/execution/business_ack。
    列表与 get 共用本函数，``include_body`` 控制是否暴露正文。
    """
    input_row = input_row if isinstance(input_row, dict) else {}
    block = _managed_block(input_row)
    intent = input_row.get("intent") or "unknown"
    delivery = _delivery(input_row, block, intent)
    created_at = _int_or_none(input_row.get("created_at")) or 0
    pending_age = max(0, now - created_at) if delivery == DELIVERY_PENDING else 0
    view = {
        "input_id": input_row.get("input_id"),
        "session_id": input_row.get("session_id"),
        "message_id": input_row.get("message_id"),
        "intent": intent,
        "resume": (block.get("resume") if isinstance(block.get("resume"), bool)
                   else RESUME_BY_INTENT.get(intent)),
        "owner": input_row.get("owner"),
        "project": block.get("project") or "",
        "request_id": block.get("request_id"),
        "revision": _int_or_none(input_row.get("revision")) or 1,
        "admission": input_row.get("admission_state") or "unknown",
        "delivery": delivery,
        "execution": _execution(block, intent),
        "business_ack": _business_ack(block, intent),
        "expires_at": _int_or_none(block.get("expires_at")),
        "flush_condition": block.get("flush_condition"),
        "pending_age_ms": pending_age,
        "next_action": next_action(input_row, now=now),
        "created_at": created_at,
        "updated_at": _int_or_none(input_row.get("updated_at")),
        "observed_at": _int_or_none(input_row.get("observed_at")),
    }
    tracking = _tracking(input_row)
    evidence = _evidence(block)
    legacy = (input_row.get("source") == "legacy-receipt"
              or bool(block.get("legacy_receipt")))
    view["legacy_receipt"] = legacy
    view["readonly"] = legacy
    view["execution_observed"] = bool(evidence.get("execution"))
    view["continuation_managed"] = bool(tracking.get("tracked"))
    continuation = evidence.get("continuation")
    if isinstance(continuation, dict):
        view["continuation"] = continuation
    flush = block.get("flush")
    if isinstance(flush, dict):
        view["flushed_at"] = _int_or_none(flush.get("flushed_at"))
        view["effective_intent"] = flush.get("effective_intent")
        view["effective_resume"] = flush.get("effective_resume")
        if isinstance(flush.get("effective_resume"), bool):
            # 原 park 保留，但 resume 展示实际执行位（flush 后为 True）。
            view["resume"] = flush["effective_resume"]
    if legacy:
        view["next_action"] = (
            "历史 legacy-receipt 只读：状态未知；仅可按原 ID 查证，不 flush/"
            "cancel/重放，不冒充托管责任，也不补造 owner")
    elif tracking and not tracking.get("tracked"):
        view["ack_boundary"] = tracking.get("boundary") or (
            "owner-manual-receipt：非托管外部分支，由 owner 人工核收")
        view["next_action"] = (
            "外部分支：由 owner 人工核收本输入执行；无托管 continuation "
            "producer，不承诺自动接续，也不授予任务写权")
    if tracking:
        view["tracking"] = TRACKED if tracking.get("tracked") else UNTRACKED
        for key in ("task_id", "attempt_id", "generation", "result_version",
                    "work_kind", "target_session", "responsibility_id",
                    "responsibility_revision", "wait_condition", "next_check_at",
                    "deadline", "boundary", "same_candidate"):
            if key in tracking:
                view[key] = tracking.get(key)
    else:
        view["tracking"] = None
    if include_body:
        view["body"] = input_row.get("body") or ""
    return view


def watch_view(row: Any) -> dict:
    """白名单登记视图（无正文；含 enabled/revision 与脱敏 last_observation）。"""
    row = row if isinstance(row, dict) else {}
    return {
        "session_id": row.get("session_id"),
        "owner": row.get("owner"),
        "project": row.get("project"),
        "max_pending_age_ms": row.get("max_pending_age_ms"),
        "enabled": bool(row.get("enabled", False)),
        "revision": _int_or_none(row.get("revision")) or 1,
        "registered_at": _int_or_none(row.get("registered_at")),
        "updated_at": _int_or_none(row.get("updated_at")),
        "unregistered_at": _int_or_none(row.get("unregistered_at")),
        "last_observed_at": _last_observed_at(row),
        "last_observation": _sanitize_watch_observation(row),
    }


__all__ = [
    "INTENT_EXECUTE", "INTENT_PARK", "INTENT_RECORD", "INTENTS",
    "RESUME_BY_INTENT", "EXPIRE_POLICY_ALERT",
    "TRACKED", "UNTRACKED", "WORK_OBSERVE", "WORK_READ_ONLY", "WORK_WRITE",
    "WORK_KINDS", "WORK_DECLARED_KINDS", "DELIVERY_MODES", "UNTRACKED_BOUNDARY",
    "EXTERNAL_INPUT_BOUNDARY",
    "DELIVERY_PENDING", "DELIVERY_DELIVERED", "DELIVERY_CANCELLED",
    "DELIVERY_UNKNOWN", "DELIVERY_NA", "DELIVERY_VALUES",
    "EXECUTION_UNKNOWN", "EXECUTION_NA", "EXECUTION_OUTCOMES", "EXECUTION_VALUES",
    "ACK_UNACKNOWLEDGED", "ACK_ACKNOWLEDGED", "ACK_NA", "ACK_VALUES",
    "validate_send_payload", "validate_confirmation", "validate_tracked_send",
    "normalize_materials", "normalize_work_kind", "write_send_block_reason",
    "responsibility_condition_key", "tracking_block", "gateway_allows",
    "CONTINUATION_SCHEMA", "REPORT_KINDS", "validate_continuation_report",
    "execute_queue_allowed", "flush_queue_allowed", "pending_condition_key",
    "delivery_state", "next_action", "receipt_view", "watch_view",
]

"""纯执行归属规则：SSE/REST 终态候选对照冻结执行边界的统一判定。

设计来源：openspec/changes/fix-attempt-completion-integrity/design.md D2。

规则要点：
- SSE 与 REST 只是不同证据来源，归属判定唯一。
- 冻结旧回合基线（session_before）优先：旧 outcome/idle 即使落在新尝试
  两秒容差内、乃至同毫秒，也不是新终态（回合身份没有容差）。
- 无归属终态不使用 now() 补造时间；缺时间即 unconfirmed。
- 判定顺序：身份 → 结果/时间合法性 → duplicate → 基线排除 → 输入投递 →
  回合区间（epoch/消息区间）→ 冲突（更新活动/多尝试归属不明）。
- 输出稳定原因码，供审计、告警与恢复编排使用。

本模块零 I/O；被 domain/domain 内部与 adapters/sqlite_ledger、runtime/supervisor
经纯值调用，不得 import util 等副作用模块。
"""

from __future__ import annotations

from typing import Any

EXECUTION_SCHEMA = "oc-orch.execution.v1"

# 时钟容差：仅用于候选查找窗口与等价来源比较；回合身份/已知逆序没有容差。
CLOCK_TOLERANCE_MS = 2000

OUTCOMES = ("succeeded", "failed", "interrupted")

# 稳定原因码（审计/告警/下一步动作据此解释）
R_SESSION_MISMATCH = "session-mismatch"
R_INVALID_OUTCOME = "invalid-outcome"
R_MISSING_END_TIME = "missing-end-time"
R_ALREADY_TERMINAL_OTHER = "already-terminal-other-outcome"
R_BASELINE_OUTCOME_EQUAL = "baseline-outcome-equal"
R_TERMINAL_BEFORE_BASELINE = "terminal-before-baseline"
R_OLDER_EPOCH_BUSY = "older-epoch-busy"
R_INPUT_UNDELIVERED = "input-undelivered"
R_INPUT_DELIVERY_UNKNOWN = "input-delivery-unknown"
R_INPUT_IDENTITY_UNPROVEN = "input-identity-unproven"
R_INPUT_AFTER_END = "input-after-terminal"
R_INPUT_OWNERSHIP_AMBIGUOUS = "input-ownership-ambiguous"
R_ENDED_BEFORE_STARTED = "ended-before-started"
R_NEWER_BUSY = "newer-busy-after-idle"
R_EPOCH = "epoch-start"
R_INTERVAL = "input-to-idle-interval"
R_INTERVAL_LEGACY = "legacy-interval-evidence"
R_NO_EPOCH = "no-attributable-start"
R_LEGACY_NO_BASELINE = "legacy-no-baseline"

STATUS_CONFIRMED = "confirmed"
STATUS_STALE = "stale"          # 证据明确属于旧回合/他人——永久不再用同候选重试
STATUS_UNCONFIRMED = "unconfirmed"  # 证据不足——等待补证，有下一动作
STATUS_DUPLICATE = "duplicate"  # 与已记录终态等价来源重复


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    return None
def blank_execution(mode: str, *, session_id: str | None = None,
                    message_id: str | None = None, generation: int | None = None,
                    sealed_at: int | None = None,
                    session_before: dict | None = None,
                    baseline: dict | None = None,
                    allow_send: bool = False) -> dict:
    """构造空白执行边界（D1）；派单封存/恢复重建共用同一形状。"""
    return {
        "schema": EXECUTION_SCHEMA,
        "mode": mode,  # new / reuse / attach / legacy
        "session_id": session_id,
        "dispatch": {
            "message_id": message_id,
            "generation": generation,
            "sealed_at": sealed_at,
            "allow_send": allow_send,
        },
        "session_before": session_before or {},
        "baseline": baseline or {},   # 交付基线引用（D5，slice 4 填充）
        "admission": {},              # 输入投递证据（排队/已投递 + 时间）
        "epoch": {},                  # 可归属的 started/busy（D1）
        "terminal": {},               # 经校验的终态（D3 由 record_terminal 写入）
        "unconfirmed": {},
    }


def build_candidate(*, outcome: str | None, source: str,
                    session_id: str | None = None, ended_at: Any = None,
                    busy_at: Any = None, started_at: Any = None,
                    seq: int | None = None, event_id: str | None = None,
                    input_in_inbox: bool | None = None,
                    input_owner: str | None = None,
                    input_expected: bool | None = None,
                    input_at: Any = None,
                    interval: dict | None = None) -> dict:
    """统一候选形状：SSE 事件与 REST 观察都转成同一输入。

    input_expected：该 attempt 有真实原输入（dispatch_msg_id 非空），此时
    输入投递/身份/顺序门（排队中、未知、锚点不明、输入晚于终态）即使存在
    epoch 也必须生效；false 表示附着/纯观察无原输入，输入门不适用。
    """
    return {
        "outcome": outcome,
        "source": source,          # sse / rest
        "session_id": session_id,
        "ended_at": _int(ended_at),
        "busy_at": _int(busy_at),
        "started_at": _int(started_at),
        "seq": seq,
        "event_id": event_id,
        "input_in_inbox": input_in_inbox,
        "input_owner": input_owner,  # unique / ambiguous / None
        "input_expected": bool(input_expected),
        "input_at": _int(input_at),
        "interval": interval or {},  # {proven: bool, input_at: int, ...}
    }


def attribute(execution: dict | None, candidate: dict) -> dict:
    """执行归属判定（纯函数）。execution 为 attempt.metadata.execution。

    返回 {"status": confirmed|stale|unconfirmed|duplicate,
          "reason": 稳定原因码, "evidence": {关键字段摘要}}。

    判定顺序：身份 → 结果/时间合法性（含 bool/非法 raw）→ duplicate →
    输入投递/身份/顺序门（有原输入时 epoch 也不跳过）→ 多尝试归属 →
    新 busy 冲突 → 基线排除 → 回合区间（epoch/消息区间）。
    legacy（exec_=None）也共用身份/输入/时间冲突门，不能绕过排队/多尝试。
    """
    outcome = candidate.get("outcome")
    if outcome not in OUTCOMES:
        return {"status": STATUS_UNCONFIRMED, "reason": R_INVALID_OUTCOME,
                "evidence": {"outcome": outcome}}
    # 时间合法性：直接拒绝 bool/非法 raw ended，不用 build_candidate 的 _int 兜底
    ended_raw = candidate.get("ended_at")
    ended = _int(ended_raw)
    if ended is None:
        return {"status": STATUS_UNCONFIRMED, "reason": R_MISSING_END_TIME,
                "evidence": {"outcome": outcome, "ended_at_raw": ended_raw}}
    busy = _int(candidate.get("busy_at"))

    exec_ = execution if isinstance(execution, dict) else None

    # ---- 身份（exec_ 存在时） ----
    if exec_ is not None:
        exp_session = exec_.get("session_id") or (exec_.get("dispatch") or {}).get(
            "session_id")
        cand_session = candidate.get("session_id")
        if exp_session and cand_session and cand_session != exp_session:
            return {"status": STATUS_STALE, "reason": R_SESSION_MISMATCH,
                    "evidence": {"expected": exp_session, "got": cand_session}}

    # ---- duplicate：与已记录终态等价来源（同 outcome、时间等价） ----
    if exec_ is not None:
        terminal = exec_.get("terminal") or {}
        if terminal.get("outcome"):
            t_end = _int(terminal.get("end_at"))
            if terminal.get("outcome") == outcome and t_end is not None:
                if abs(ended - t_end) <= CLOCK_TOLERANCE_MS:
                    return {"status": STATUS_DUPLICATE,
                            "reason": "equivalent-terminal",
                            "evidence": {"end_at": t_end, "candidate_end": ended}}
            if terminal.get("outcome") != outcome:
                return {"status": STATUS_UNCONFIRMED,
                        "reason": R_ALREADY_TERMINAL_OTHER,
                        "evidence": {"recorded": terminal.get("outcome"),
                                     "candidate": outcome}}

    # ---- 输入门（有原输入时对 epoch/legacy 一视同仁，D2/D3 补充） ----
    if candidate.get("input_expected"):
        inbox = candidate.get("input_in_inbox")
        if inbox is True:
            # 输入仍在 inbox 排队、未执行：即使已收到 started 也不能确认（T10）
            return {"status": STATUS_UNCONFIRMED, "reason": R_INPUT_UNDELIVERED,
                    "evidence": {"ended_at": ended}}
        if inbox is None:
            # 投递状态未知（查询失败/不可用）：不能把未知当成可确认
            return {"status": STATUS_UNCONFIRMED,
                    "reason": R_INPUT_DELIVERY_UNKNOWN,
                    "evidence": {"ended_at": ended}}
        input_at = _int(candidate.get("input_at"))
        if input_at is None:
            # 已收到 started ≠ 已证明 started 属于原输入：锚点身份不明
            return {"status": STATUS_UNCONFIRMED,
                    "reason": R_INPUT_IDENTITY_UNPROVEN,
                    "evidence": {"ended_at": ended}}
        if ended < input_at:
            # 原输入晚于候选终态：不能把队列后的旧成功归属本轮
            return {"status": STATUS_STALE, "reason": R_INPUT_AFTER_END,
                    "evidence": {"input_at": input_at, "ended_at": ended}}

    # ---- 多尝试共享：无法唯一归属时 unconfirmed（不按 latest+2s 猜测） ----
    if candidate.get("input_owner") == "ambiguous":
        return {"status": STATUS_UNCONFIRMED,
                "reason": R_INPUT_OWNERSHIP_AMBIGUOUS,
                "evidence": {"ended_at": ended}}

    # ---- 新 busy 冲突（T11/F1）：候选 idle 后有更晚 busy 区间 ----
    if busy is not None and busy > ended:
        return {"status": STATUS_UNCONFIRMED, "reason": R_NEWER_BUSY,
                "evidence": {"busy_at": busy, "ended_at": ended,
                             "outcome": outcome}}

    # ---- legacy：无执行边界，只有可完整读取的输入→idle 区间可证明 ----
    if exec_ is None:
        interval = candidate.get("interval") or {}
        if interval.get("proven"):
            input_at = _int(interval.get("input_at"))
            if input_at is not None and ended < input_at:
                return {"status": STATUS_STALE, "reason": R_ENDED_BEFORE_STARTED,
                        "evidence": {"input_at": input_at, "ended_at": ended}}
            return {"status": STATUS_CONFIRMED, "reason": R_INTERVAL_LEGACY,
                    "evidence": {"interval": interval}}
        return {"status": STATUS_UNCONFIRMED, "reason": R_LEGACY_NO_BASELINE,
                "evidence": {"ended_at": ended, "outcome": outcome}}

    # ---- 基线排除：冻结的旧回合 outcome/idle/busy（D2 规则 2） ----
    before = exec_.get("session_before") or {}
    b_idle = _int(before.get("idle"))
    b_outcome = before.get("outcome")
    b_busy = _int(before.get("busy"))
    if b_idle is not None and ended <= b_idle:
        reason = (R_BASELINE_OUTCOME_EQUAL if b_outcome == outcome
                  else R_TERMINAL_BEFORE_BASELINE)
        return {"status": STATUS_STALE, "reason": reason,
                "evidence": {"baseline_idle": b_idle, "baseline_outcome": b_outcome,
                             "ended_at": ended, "candidate_outcome": outcome}}
    if busy is not None and b_idle is not None and busy <= b_idle:
        # busy 本身早于基线空闲：整个执行窗口都在旧回合内
        return {"status": STATUS_STALE, "reason": R_OLDER_EPOCH_BUSY,
                "evidence": {"busy_at": busy, "baseline_idle": b_idle}}

    # ---- 回合区间：epoch started 或可完整读取的输入→idle 消息区间 ----
    epoch = exec_.get("epoch") or {}
    e_start = _int(epoch.get("started_at"))
    started_hint = candidate.get("started_at") or e_start
    if e_start is not None:
        if ended < e_start:
            # 已知逆序（含 turn 逆转序）：拒绝，无容差（T02）
            return {"status": STATUS_STALE, "reason": R_ENDED_BEFORE_STARTED,
                    "evidence": {"started_at": e_start, "ended_at": ended}}
        return {"status": STATUS_CONFIRMED, "reason": R_EPOCH,
                "evidence": {"started_at": e_start, "ended_at": ended}}
    interval = candidate.get("interval") or {}
    if interval.get("proven"):
        input_at = _int(interval.get("input_at"))
        if input_at is not None and ended < input_at:
            return {"status": STATUS_STALE, "reason": R_ENDED_BEFORE_STARTED,
                    "evidence": {"input_at": input_at, "ended_at": ended}}
        return {"status": STATUS_CONFIRMED, "reason": R_INTERVAL,
                "evidence": {"interval": interval}}
    # 新 session 无旧回合可排除，但既无 started 也无区间证据 → 继续补证
    return {"status": STATUS_UNCONFIRMED, "reason": R_NO_EPOCH,
            "evidence": {"ended_at": ended, "busy_at": busy,
                         "started_hint": started_hint,
                         "interval": {k: v for k, v in interval.items() if k != "error"}}}


def unconfirmed_next_action(reason: str, interval: dict | None = None) -> str:
    """unconfirmed/stale 的可操作下一动作（供 metadata 和通知引用）。"""
    if reason == R_NO_EPOCH and (interval or {}).get("reason") == "multi-round-interval":
        return "核对区间内的输入与 idle；同候选逐条查消费回执，换版则用新 attempt 独审"
    mapping = {
        R_MISSING_END_TIME: "等待服务端补齐 idle/outcome 时间后对账；不得用观察时间补造",
        R_INPUT_UNDELIVERED: "输入仍在 inbox 排队；等待投递，不得提前判定完成",
        R_INPUT_DELIVERY_UNKNOWN: "输入投递状态未知；重查 inbox/inbox 后重新对账",
        R_INPUT_IDENTITY_UNPROVEN: "已收到 started 但原输入锚点不可读；定点读原 message 补证归属",
        R_INPUT_AFTER_END: "原输入晚于候选终态；候选是更早回合，不得归属本轮",
        R_INPUT_OWNERSHIP_AMBIGUOUS: "同 session 多尝试共享区间；人工按输入归属核实",
        R_NEWER_BUSY: "候选 idle 后有更晚 busy/活动事实；刷新 session 事实后重新对账",
        R_NO_EPOCH: "等待 execution.started 或读取消息区间补证",
        R_LEGACY_NO_BASELINE: "历史尝试缺基线；显式补证或人工核实后 collect",
        R_ALREADY_TERMINAL_OTHER: "已记录不同终态；保留审计，不自动改写",
        "session-snapshot-unavailable": "最新 session 快照不可读/缺失；重查后对账，不得以缺失当作已停止",
        "session-active": "会话仍活动；刷新 active/session 事实后重新对账",
        "activity-unknown": "活动状态查询失败=未知；重试 active 查询后重新对账",
    }
    return mapping.get(reason, "保留责任待补证；详见执行边界 unconfirmed 记录")

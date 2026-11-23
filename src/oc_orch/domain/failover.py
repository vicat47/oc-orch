"""提供商故障切换/知会的纯规则域（provider-fault-failover）。

零 I/O、零台账写、零网络。本模块只推导：

- 故障归一与**真实路线归属**（实际失败 provider/model，不依赖台账目标值）；
- 故障链预算与瞬态观察窗判定（跨重启语义由调用方持久化，域侧只做纯数值）；
- episode 状态机合法迁移；
- 模型身份比较与切换策略授权交集（纯规则，资源授权数据由调用方注入）。

安全不变量（对应 specs/provider-fault-failover F01–F26）：

- 只有可归属到 session/Attempt/输入/路线的结构化错误才成为故障候选；未知/产品
  失败/控制接口超时不得凭关键词推定为 provider 故障（F03）。
- 硬故障分“可自动切换”（quota/model_unavailable）与“只转人工”（auth 等）：
  认证/内容策略拒绝不是换模型能解决的，绝不自动换（F03、F06）。
- 普通 429/可恢复 503/超时先走有界重试；预算（次数+观察窗）耗尽才进入
  故障决策，不在域内无界放大（F02）。
- 每次 episode 自动切换上限 ``max_switches``（默认 1）；rework/重启/备用失败
  不刷新预算；同路由重复错误去重（F04、F05、F06）。
- 切换成功与否与执行恢复、业务验收严格分开表达（F09、F21、F22）。
"""

from __future__ import annotations

import hashlib
import json

from typing import Any

from .classify import classify_provider_error

# ---- 状态机与状态 ------------------------------------------------
EPISODE_OPEN = "open"
EPISODE_WAITING_QUIESCENCE = "waiting_quiescence"
EPISODE_SWITCH_REQUESTED = "switch_requested"
EPISODE_SWITCH_CONFIRMED = "switch_confirmed"
EPISODE_SWITCH_UNKNOWN = "switch_unknown"
EPISODE_CONTINUATION_REQUESTED = "continuation_requested"
EPISODE_CONTINUATION_UNKNOWN = "continuation_unknown"
EPISODE_RESUMED = "resumed"
EPISODE_BLOCKED_MANUAL = "blocked_manual"
EPISODE_SUPERSEDED = "superseded"
EPISODE_CANCELLED = "cancelled"

# 非终态：仍可被恢复调谐推进。
ACTIVE_EPISODE_STATES = frozenset({
    EPISODE_OPEN, EPISODE_WAITING_QUIESCENCE, EPISODE_SWITCH_REQUESTED,
    EPISODE_SWITCH_CONFIRMED, EPISODE_SWITCH_UNKNOWN,
    EPISODE_CONTINUATION_REQUESTED, EPISODE_CONTINUATION_UNKNOWN,
})
# 终态。
TERMINAL_EPISODE_STATES = frozenset({
    EPISODE_RESUMED, EPISODE_BLOCKED_MANUAL, EPISODE_SUPERSEDED,
    EPISODE_CANCELLED,
})

# 合法迁移：允许从 A 到 B（None 起始 = 新建）。只做保守白名单，防止
# 恢复乱跳覆盖已完成步骤；晚回执只更新已知状态，不产生伪迁移。
_VALID_TRANSITIONS: dict[str, frozenset[str]] = {
    None: frozenset({EPISODE_OPEN, EPISODE_WAITING_QUIESCENCE,
                     EPISODE_BLOCKED_MANUAL}),
    EPISODE_OPEN: frozenset({EPISODE_WAITING_QUIESCENCE,
                             EPISODE_SWITCH_REQUESTED, EPISODE_BLOCKED_MANUAL,
                             EPISODE_SUPERSEDED, EPISODE_CANCELLED}),
    EPISODE_WAITING_QUIESCENCE: frozenset({EPISODE_SWITCH_REQUESTED,
                                           EPISODE_BLOCKED_MANUAL,
                                           EPISODE_SUPERSEDED,
                                           EPISODE_CANCELLED}),
    EPISODE_SWITCH_REQUESTED: frozenset({EPISODE_SWITCH_CONFIRMED,
                                         EPISODE_SWITCH_UNKNOWN,
                                         EPISODE_BLOCKED_MANUAL}),
    EPISODE_SWITCH_UNKNOWN: frozenset({EPISODE_SWITCH_CONFIRMED,
                                       EPISODE_SWITCH_UNKNOWN,
                                       EPISODE_BLOCKED_MANUAL,
                                       EPISODE_CANCELLED}),
    EPISODE_SWITCH_CONFIRMED: frozenset({EPISODE_CONTINUATION_REQUESTED,
                                         EPISODE_BLOCKED_MANUAL,
                                         EPISODE_SUPERSEDED}),
    EPISODE_CONTINUATION_REQUESTED: frozenset({
        EPISODE_CONTINUATION_UNKNOWN, EPISODE_RESUMED, EPISODE_BLOCKED_MANUAL,
        EPISODE_SUPERSEDED}),
    EPISODE_CONTINUATION_UNKNOWN: frozenset({
        EPISODE_CONTINUATION_UNKNOWN, EPISODE_RESUMED, EPISODE_BLOCKED_MANUAL}),
    EPISODE_RESUMED: frozenset({EPISODE_SUPERSEDED}),
    EPISODE_CANCELLED: frozenset(),
    EPISODE_BLOCKED_MANUAL: frozenset({EPISODE_BLOCKED_MANUAL,
                                       EPISODE_SUPERSEDED}),
    EPISODE_SUPERSEDED: frozenset(),
}


def can_transition(current: str | None, target: str) -> bool:
    """判定 episode 状态迁移是否在白名单内。"""
    if target not in _VALID_TRANSITIONS.get(None, frozenset()) \
            and target not in TERMINAL_EPISODE_STATES | ACTIVE_EPISODE_STATES:
        return False
    allowed = _VALID_TRANSITIONS.get(current)
    return allowed is not None and target in allowed


# ---- 默认预算（design D1/D2；可由策略覆盖） -----------------------
DEFAULT_MAX_ATTEMPTS = 3                # 原路线总尝试上限（含首次）
DEFAULT_TRANSIENT_WINDOW_MS = 15 * 60 * 1000
DEFAULT_MAX_SWITCHES = 1                # 每故障链自动切换上限
DEFAULT_CONFIRM_QUERIES = 3             # unknown 切换补证查询上限
DEFAULT_CONFIRM_INTERVAL_MS = 30 * 1000
DEFAULT_CONFIRM_DEADLINE_MS = 5 * 60 * 1000
DEFAULT_NOTIFY_UPDATES = 3              # 每 episode 知会更新上限（首封之外）

# 硬故障类别；不可切换的硬类别（认证等）只转人工。
HARD_CATEGORIES = frozenset({"quota", "auth", "model_unavailable"})
# 备用模型能解决的硬类别 → 授权范围内可自动切换。
AUTO_FAILOVER_CATEGORIES = frozenset({"quota", "model_unavailable"})
# 仅重试类别：不进入故障决策，直到预算耗尽。
RETRY_ONLY_CATEGORIES = frozenset({"rate_limit", "server"})


def _strict_str(value: Any, what: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{what} 必须是非空字符串（收到 {value!r}）")
    return value.strip()


def model_ref(provider: str, model: str, variant: str | None = None) -> dict:
    """构造完整模型身份；provider/model 必须非空，variant 可选。"""
    out = {"providerID": _strict_str(provider, "provider"),
           "id": _strict_str(model, "model")}
    if variant:
        out["variant"] = _strict_str(variant, "variant")
    return out


def model_ref_from_dict(value: Any) -> dict | None:
    """严格从 dict 读取模型身份；非法/缺失返回 None（fail-closed）。"""
    if not isinstance(value, dict):
        return None
    provider, model = value.get("providerID"), value.get("id")
    if not isinstance(provider, str) or not provider.strip():
        return None
    if not isinstance(model, str) or not model.strip():
        return None
    variant = value.get("variant")
    if variant is not None and (not isinstance(variant, str) or not variant.strip()):
        return None
    out = {"providerID": provider.strip(), "id": model.strip()}
    if isinstance(variant, str) and variant.strip():
        out["variant"] = variant.strip()
    return out


def models_equal(a: dict | None, b: dict | None) -> bool:
    """完整身份（provider/id/variant）相等；任一侧无法解析 → False。

    variant 缺失（None）与显式缺键视为相等：切换请求未指定 variant 时，
    目标模型不带 variant，而 session 可能回报默认 variant。
    """
    na, nb = model_ref_from_dict(a), model_ref_from_dict(b)
    if na is None or nb is None:
        return False
    return na["providerID"] == nb["providerID"] and na["id"] == nb["id"] \
        and (na.get("variant") or None) == (nb.get("variant") or None)


def base_route(route: str) -> str:
    """去掉 variant 后缀的基础路由；用于策略列表/阻断键匹配。"""
    return str(route or "").split("#", 1)[0]


def episode_id(project: str, task_id: str, route: str) -> str:
    """稳定故障链身份：project+task+失败路由（不含时间/观察计数）。"""
    import hashlib

    raw = f"{project}\x00{task_id}\x00{route}"
    return "EP-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]

def route_key(provider: str, model: str, variant: str | None = None) -> str:
    """稳定路由键 "provider/model[#variant]"（凭据级聚合由调用方决定）。"""
    key = f"{_strict_str(provider, 'provider')}/{_strict_str(model, 'model')}"
    if variant:
        key += f"#{_strict_str(variant, 'variant')}"
    return key


# ---- 故障归一与真实路线归属 ------------------------------------------
def _route_from_error(error: dict | None) -> tuple[str, str, str | None] | None:
    """从 provider 错误/元数据中提取结构化 route（仅信任显式字段）。"""
    if not isinstance(error, dict):
        return None
    raw = error
    for key in ("model", "request", "data", "error", "body"):
        node = error.get(key)
        if isinstance(node, dict) and isinstance(node.get("providerID"), str) \
                and isinstance(node.get("id"), str) and node["providerID"] \
                and node["id"]:
            raw = node
            break
    provider = raw.get("providerID")
    model = raw.get("id")
    variant = raw.get("variant")
    if not (isinstance(provider, str) and provider.strip()
            and isinstance(model, str) and model.strip()):
        return None
    return provider.strip(), model.strip(), (
        variant.strip() if isinstance(variant, str) and variant.strip() else None)


def _error_fingerprint(error: dict | None) -> str:
    """稳定去重指纹：只取分类相关的窄字段，不保完整正文。"""
    if not isinstance(error, dict):
        return "none"
    kept: dict[str, Any] = {}
    for key in ("code", "tag", "_tag", "status", "type", "provider"):
        value = error.get(key)
        if value is not None:
            kept[key] = value
    message = error.get("message")
    if isinstance(message, str):
        kept["msg"] = message[:300]
    body = error.get("body")
    if isinstance(body, str):
        kept["body"] = body[:200]
    dump = json.dumps(kept, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))
    return hashlib.sha256(dump.encode("utf-8")).hexdigest()[:16]


def classify_fault_source(source: dict) -> dict:
    """统一故障归一；输入 ``source`` 含 ``error`` 与可选归属证据：

    - ``error``：provider 错误对象（SSE retry.scheduled / REST 终态 error）。
    - ``route_provider/route_model/route_variant``：调用方已核实归属的路由
      （优先后备——**只信任显式证据**，不反推台账）。
    - ``event_id/message_id``：稳定来源证据身份。
    - ``origin_at``：来源时间（缺失回退 0，由调用方落 observed_at）。

    返回：category/transient/hard/auto_failover/status/message/code/
    retry_after_ms/route(key+provider+model+variant)/dedupe_key/
    origin_key/hard_categories。

    判定顺序：
      1. 复用 ``classify_provider_error``（订阅限额 → code/tag 路由 →
         status+关键词回退，402/Insufficient Balance 已归 quota）。
      2. ``hard`` = 类别在 HARD_CATEGORIES；``auto_failover`` = hard 且类别
         在 AUTO_FAILOVER_CATEGORIES（auth 等只转人工，不自动换模型）。
      3. route 归属：error 结构化字段优先；其次调用方核实证据；二者都无 →
         route_unknown，标记 unknown_route（F04）。
      4. status==402 且无更明确类别时为 quota 硬故障。
    """
    if not isinstance(source, dict):
        source = {}
    error = source.get("error")
    if not isinstance(error, dict):
        error = {}
    cls = classify_provider_error(error)
    category = str(cls.get("category") or "unknown")
    transient = bool(cls.get("transient", True))
    status = cls.get("status")
    # 402 兜底：未归类的 402 一律视为配额硬故障（余额错误通常无结构化 code）。
    if status == 402 and category in ("unknown", "request"):
        category = "quota"
        transient = False
    codes = cls.get("code")
    retry_after = cls.get("retry_after_ms")
    if not isinstance(retry_after, int) or retry_after <= 0:
        retry_after = None
    hard = category in HARD_CATEGORIES
    auto_failover = hard and category in AUTO_FAILOVER_CATEGORIES

    route = _route_from_error(error) or (
        (source.get("route_provider"), source.get("route_model"),
         source.get("route_variant"))
        if isinstance(source.get("route_provider"), str)
        and source.get("route_provider").strip()
        and isinstance(source.get("route_model"), str)
        and source.get("route_model").strip()
        else None)
    if route is None:
        provider = model = variant = None
        route = "route_unknown"
        unknown_route = True
    else:
        provider, model, variant = route
        route = route_key(provider, model, variant)
        unknown_route = False

    origin_key = "|".join([
        str(source.get("event_id") or ""), str(source.get("message_id") or "")])
    dedupe_key = "{route}:{category}:{status}:{fp}".format(
        route=route, category=category, status=str(status or 0),
        fp=_error_fingerprint(error))

    return {
        "category": category, "transient": transient,
        "hard": hard, "auto_failover": auto_failover,
        "status": status, "message": str(cls.get("message") or ""),
        "code": codes, "retry_after_ms": retry_after,
        "route": route, "route_provider": provider, "route_model": model,
        "route_variant": variant, "unknown_route": unknown_route,
        "dedupe_key": dedupe_key, "origin_key": origin_key,
        "origin_at": source.get("origin_at") or 0,
    }


# ---- 预算与瞬态决策（纯数值，持久化由调用方负责） -------------------
def attempts_remaining(attempts_used: int, max_attempts: int = DEFAULT_MAX_ATTEMPTS) -> int:
    """原路线剩余尝试次数；<=0 表示预算耗尽。"""
    return max(0, int(max_attempts) - max(0, int(attempts_used or 0)))


def transients_exhausted(*, failure_count: int, first_at: int, now: int,
                         window_ms: int = DEFAULT_TRANSIENT_WINDOW_MS,
                         max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                         trusted_retry_after: int | None = None) -> dict:
    """瞬态分类是否达到故障决策点。

    - 可信 retry_after 存在且 now 未到 → ``waiting``（遵守可信窗口，不提前）。
    - 次数 >= max_attempts，或 now >= first_at + window_ms → ``exhausted``。
    - 否则 → ``retryable``。
    window_ms <= 0 视为无观察窗（仅次数判定）；预算跨重启由调用方持久计数。
    """
    count = max(0, int(failure_count or 0))
    if isinstance(trusted_retry_after, int) and trusted_retry_after > 0:
        if now < first_at + trusted_retry_after:
            return {"decision": "waiting", "reason": "trusted-retry-after",
                    "retry_at": first_at + trusted_retry_after,
                    "remaining": attempts_remaining(count, max_attempts)}
    if count >= max_attempts:
        return {"decision": "exhausted", "reason": "attempts",
                "remaining": 0}
    if window_ms > 0 and now >= first_at + window_ms:
        return {"decision": "exhausted", "reason": "window",
                "remaining": 0}
    return {"decision": "retryable", "reason": "within-budget",
            "remaining": attempts_remaining(count, max_attempts)}


def switch_allowed(*, switches_used: int, max_switches: int = DEFAULT_MAX_SWITCHES,
                   authorized: bool = True, target_available: bool = True,
                   same_route: bool = False) -> dict:
    """自动切换决策（F05/F06/F08）。

    - ``switches_used >= max_switches`` → denied（预算耗尽，转人工）。
    - ``authorized=False`` 或 ``target_available=False`` → denied（转人工）。
    - ``same_route``（目标与失败路由相同）→ denied。
    - 否则 allowed=True，消耗一次切换（由调用方在事务内落 switches_used+1）。
    """
    if same_route:
        return {"allowed": False, "reason": "same-route"}
    if not authorized:
        return {"allowed": False, "reason": "not-authorized"}
    if not target_available:
        return {"allowed": False, "reason": "target-unavailable"}
    used = max(0, int(switches_used or 0))
    if used >= max_switches:
        return {"allowed": False, "reason": "budget-exhausted"}
    return {"allowed": True, "reason": "authorized", "switches_used": used + 1}


def notify_update_allowed(*, updates_used: int,
                          max_updates: int = DEFAULT_NOTIFY_UPDATES) -> bool:
    """同一事故首封之外的状态更新预算（N11）；轮询/重复观察不占额。"""
    return int(updates_used or 0) < int(max_updates)


__all__ = [
    "EPISODE_OPEN", "EPISODE_WAITING_QUIESCENCE", "EPISODE_SWITCH_REQUESTED",
    "EPISODE_SWITCH_CONFIRMED", "EPISODE_SWITCH_UNKNOWN",
    "EPISODE_CONTINUATION_REQUESTED", "EPISODE_CONTINUATION_UNKNOWN",
    "EPISODE_RESUMED", "EPISODE_BLOCKED_MANUAL", "EPISODE_SUPERSEDED",
    "EPISODE_CANCELLED", "ACTIVE_EPISODE_STATES", "TERMINAL_EPISODE_STATES",
    "can_transition", "base_route", "episode_id", "model_ref", "model_ref_from_dict", "models_equal",
    "route_key", "classify_fault_source", "attempts_remaining",
    "transients_exhausted", "switch_allowed", "notify_update_allowed",
    "HARD_CATEGORIES", "AUTO_FAILOVER_CATEGORIES", "RETRY_ONLY_CATEGORIES",
    "DEFAULT_MAX_ATTEMPTS", "DEFAULT_TRANSIENT_WINDOW_MS",
    "DEFAULT_MAX_SWITCHES", "DEFAULT_CONFIRM_QUERIES",
    "DEFAULT_CONFIRM_INTERVAL_MS", "DEFAULT_CONFIRM_DEADLINE_MS",
    "DEFAULT_NOTIFY_UPDATES",
]
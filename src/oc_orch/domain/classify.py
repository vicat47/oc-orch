"""纯函数：事件与错误分类（无网络、无数据库，便于合成输入验证）。

兼容旧导入路径 oc_orch.classify（root classify.py 为 sys.modules 别名指向本
模块）；本文件是分类逻辑的单一权威实现。
"""

from __future__ import annotations

import json

from typing import Any

CONTENT_TYPES = {
    "session.text.delta", "session.text.started", "session.text.ended",
    "session.reasoning.delta", "session.reasoning.started", "session.reasoning.ended",
    "session.tool.input.delta", "session.tool.input.started", "session.tool.input.ended",
    "session.tool.progress", "session.step.streamed", "session.synthetic",
    "session.compaction.delta", "session.usage.updated",
}

TERMINAL_EVENTS = {
    "session.execution.succeeded": "succeeded",
    "session.execution.failed": "failed",
    "session.execution.interrupted": "interrupted",
}

PROGRESS_EVENTS = {
    "session.execution.started",
    "session.tool.called",
    "session.tool.success",
    "session.tool.failed",
    "session.step.ended",
    "session.step.failed",
    "session.inbox.enqueued",
    "session.inbox.delivered",
}

PERMISSION_EVENTS = {"permission.asked"}
PERMISSION_REPLIED = {"permission.replied"}
FORM_EVENTS = {"form.created"}
FORM_SETTLED = {"form.replied", "form.cancelled"}
INBOX_EVENTS = {
    "session.inbox.enqueued",
    "session.inbox.delivered",
    "session.inbox.cancelled",
    "session.inbox.delivery.changed",
}

SUBSCRIBE_TYPES = [
    "session.created",
    "session.execution.started",
    "session.execution.succeeded",
    "session.execution.failed",
    "session.execution.interrupted",
    "session.retry.scheduled",
    "session.tool.called",
    "session.tool.success",
    "session.tool.failed",
    "session.step.ended",
    "session.step.failed",
    "session.inbox.enqueued",
    "session.inbox.delivered",
    "session.inbox.cancelled",
    "session.inbox.delivery.changed",
    "permission.asked",
    "permission.replied",
    "form.created",
    "form.replied",
    "form.cancelled",
    "session.shell.started",
    "session.shell.ended",
]


def extract_session_id(obj: Any, depth: int = 0) -> str | None:
    """从事件 data 中提取 sessionID；优先显式字段，其次 durable.aggregateID。"""
    if depth > 6 or obj is None:
        return None
    if isinstance(obj, dict):
        for key in ("sessionID", "sessionId", "session_id"):
            val = obj.get(key)
            if isinstance(val, str) and val:
                return val
        durable = obj.get("durable")
        if isinstance(durable, dict):
            agg = durable.get("aggregateID")
            if isinstance(agg, str) and agg.startswith("ses"):
                return agg
        for key in ("data", "item", "payload", "request"):
            if key in obj:
                found = extract_session_id(obj[key], depth + 1)
                if found:
                    return found
        for val in obj.values():
            if isinstance(val, (dict, list)):
                found = extract_session_id(val, depth + 1)
                if found:
                    return found
    elif isinstance(obj, list):
        for val in obj:
            found = extract_session_id(val, depth + 1)
            if found:
                return found
    return None


def classify_event(envelope: dict) -> dict:
    """返回 {action, session_id, terminal, content, reason, error}。

    action 取值：
      connect / terminal / started / permission / permission_replied /
      form / form_settled / progress / retry / inbox / ignored

    content 类事件即使被误订阅也标记 content=True，调用方不得落盘正文。
    """
    if not isinstance(envelope, dict):
        return {"action": "ignored", "session_id": None, "content": False}
    etype = envelope.get("type")
    if not isinstance(etype, str):
        return {"action": "ignored", "session_id": None, "content": False}
    if etype == "server.connected":
        return {"action": "connect", "session_id": None, "content": False}
    session_id = extract_session_id(envelope.get("data"))
    base = {"session_id": session_id, "content": etype in CONTENT_TYPES}
    if etype in CONTENT_TYPES:
        return {**base, "action": "ignored", "content": True}
    if etype in TERMINAL_EVENTS:
        data = envelope.get("data") if isinstance(envelope.get("data"), dict) else {}
        return {
            **base,
            "action": "terminal",
            "terminal": TERMINAL_EVENTS[etype],
            "reason": data.get("reason"),
        }
    if etype == "session.execution.started":
        return {**base, "action": "started"}
    if etype in PERMISSION_EVENTS:
        return {**base, "action": "permission"}
    if etype in PERMISSION_REPLIED:
        return {**base, "action": "permission_replied"}
    if etype in FORM_EVENTS:
        return {**base, "action": "form"}
    if etype in FORM_SETTLED:
        return {**base, "action": "form_settled"}
    if etype == "session.retry.scheduled":
        data = envelope.get("data") if isinstance(envelope.get("data"), dict) else {}
        return {**base, "action": "retry", "error": data.get("error") or {}}
    if etype in INBOX_EVENTS:
        return {**base, "action": "inbox"}
    if etype in PROGRESS_EVENTS:
        return {**base, "action": "progress"}
    if etype.startswith("session."):
        return {**base, "action": "ignored"}
    return {**base, "action": "ignored"}


# ---- 结构化错误信号（S3.1 / D4）------------------------------------------
# 订阅共享额度：结构化 code/tag 必须优先于普通 429（D4.1）。
_SUBSCRIPTION_LIMIT_CODES = frozenset({
    "provider.chatgpt-limit",
    "subscription_sharing_usage_limit_exceeded",
})
_SUBSCRIPTION_LIMIT_PHRASE = "subscription sharing usage limit exceeded"

_AUTH_CODES = frozenset({
    "invalid_api_key", "unauthorized", "authentication_error", "auth_error",
    "access_denied", "permission_denied", "model_permission", "forbidden",
    # 真实网关/代理的 tag 拼写（结构化 tag 兼容）
    "authenticationerror", "invalidapikey",
})
_RATE_LIMIT_CODES = frozenset({
    "rate_limit_exceeded", "too_many_requests", "rate_limited",
    "rate_limit_error", "ratelimiterror", "requests_rate_limited",
})
_MODEL_UNAVAILABLE_CODES = frozenset({
    "model_not_found", "model_unavailable", "model_does_not_exist",
    "no_available_channel", "channel_not_found", "invalid_model",
    "model_not_found_error", "modelnotfounderror",
})
# 通用额度：明确结构化信号才归类 quota，不凭任意文本拼接断言共享 credential。
_QUOTA_CODES = frozenset({
    "insufficient_quota", "quota_exceeded", "billing_not_active",
    "balance_insufficient", "quota_error", "insufficientquotaerror",
    "billing_hard_limit_reached",
})

_RETRY_AFTER_MS_KEYS = frozenset({"retry_after_ms", "retryAfterMs"})
_RETRY_AFTER_SEC_KEYS = frozenset(
    {"retry_after", "retryAfter", "retry-after", "Retry-After"})

# 嵌套 error/response.body 扫描边界，避免病态输入无限展开。
_SCAN_MAX_DEPTH = 6
_SCAN_MAX_ITEMS = 256
_SCAN_MAX_JSON_LEN = 50_000


def _trusted_retry_number(value: Any):
    """仅信任纯数值（含整数秒字符串）；bool/文本/零/负值视为不可信。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        num = float(value)
    elif isinstance(value, str) and value.strip().isdigit():
        num = float(value.strip())
    else:
        return None
    if 0 < num < 10 ** 11:  # 有界且为正
        return int(num) if not isinstance(value, float) else num
    return None


def _collect_error_features(error: dict):
    """在合理嵌套的 error/response.body 内收集结构信息。

    返回 ``(codes, messages, retry_after_ms)``：

    - ``codes`` -- 小写 code/tag 字符串集合（含嵌套 error 与可解析 JSON body）；
    - ``messages`` -- 小写 message 文本集合；
    - ``retry_after_ms`` -- 优先显式毫秒键，其次秒级 Retry-After（换算毫秒），
      只信任纯数值；无则 None。

    不做网络；深度/遍历项/JSON 长度均有界。
    """
    codes: set[str] = set()
    messages: set[str] = set()
    first_ms: list[int | None] = [None]
    first_sec_ms: list[int | None] = [None]
    budget = {"items": _SCAN_MAX_ITEMS}

    def walk(node, depth: int) -> None:
        if budget["items"] <= 0 or depth > _SCAN_MAX_DEPTH or node is None:
            return
        budget["items"] -= 1
        if isinstance(node, dict):
            for key, value in node.items():
                if key in ("code", "tag", "_tag"):
                    if isinstance(value, str) and value.strip():
                        codes.add(value.strip().lower())
                elif key == "message":
                    if isinstance(value, str):
                        messages.add(value.lower())
                elif key in _RETRY_AFTER_MS_KEYS:
                    num = _trusted_retry_number(value)
                    if num is not None and first_ms[0] is None:
                        first_ms[0] = int(num)
                elif key in _RETRY_AFTER_SEC_KEYS:
                    num = _trusted_retry_number(value)
                    if num is not None and first_sec_ms[0] is None:
                        first_sec_ms[0] = int(num * 1000)
                elif isinstance(value, (dict, list, str)):
                    walk(value, depth + 1)
        elif isinstance(node, list):
            for item in node:
                walk(item, depth + 1)
        elif isinstance(node, str):
            text = node.strip()
            if (len(text) > _SCAN_MAX_JSON_LEN
                    or not (text.startswith("{") or text.startswith("["))):
                return
            try:
                parsed = json.loads(text)
            except (ValueError, TypeError):
                return
            if isinstance(parsed, (dict, list)):
                walk(parsed, depth + 1)

    walk(error, 0)
    retry_after_ms = first_ms[0] if first_ms[0] is not None else first_sec_ms[0]
    return codes, messages, retry_after_ms


def _normalize_status(raw: Any) -> int | None:
    """接受 int 或纯数字字符串；bool/其他文本视为无 status。"""
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return None


def _base_result(category: str, transient: bool, status: int | None,
                 message: str, action: str) -> dict:
    return {"category": category, "transient": transient, "status": status,
            "message": message, "action": action}


def classify_provider_error(error: dict | None) -> dict:
    """把 session.retry.scheduled 的 error 归一为可执行分类。

    判定顺序（D4.1）：
      1. 订阅限额：结构化 code/tag 或明确消息 ``Subscription Sharing usage
         limit exceeded`` 优先于普通 429；结果带 ``limit_kind='subscription'``，
         并按需附 ``code`` 与可信 ``retry_after_ms``。
      2. 结构化 code/tag 路由：auth / model_unavailable / quota / rate_limit。
      3. 原有 status + 消息关键词回退：auth → rate_limit → quota →
         model_unavailable → server(5xx) → request(4xx) → unknown。

    只描述既有错误，不替代 provider 自身的重试；不连接网络。返回键保持
    category/transient/status/message/action 兼容，订阅限额追加可选键。
    """
    if not isinstance(error, dict):
        return _base_result("unknown", True, None, "", "observe")
    status = _normalize_status(error.get("status"))
    msg = str(error.get("message") or "")
    codes, messages, retry_after_ms = _collect_error_features(error)

    hay = " ".join(messages)
    body = error.get("response")
    if isinstance(body, dict):
        raw = body.get("body")
        if isinstance(raw, str):
            hay += " " + raw.lower()
        elif isinstance(raw, (dict, list)):
            hay += " " + json.dumps(raw, ensure_ascii=False).lower()
    hay = hay.strip()

    # 1) 订阅限额：结构化 code/tag 或明确消息，普通 429 之前识别。
    sub_code = next((c for c in _SUBSCRIPTION_LIMIT_CODES if c in codes), None)
    if sub_code is not None or any(_SUBSCRIPTION_LIMIT_PHRASE in m for m in messages):
        result = _base_result("quota", False, status, msg[:500],
                              "block-and-alert")
        result["limit_kind"] = "subscription"
        if sub_code is not None:
            result["code"] = sub_code
        if retry_after_ms is not None:
            result["retry_after_ms"] = retry_after_ms
        return result

    # 2) 结构化 code/tag 路由（先于 status 文本回退，避免被状态掩盖）。
    if codes & _AUTH_CODES:
        return _base_result("auth", False, status, msg[:500], "block-and-alert")
    if codes & _MODEL_UNAVAILABLE_CODES:
        return _base_result("model_unavailable", False, status, msg[:500],
                            "block-and-alert")
    if codes & _QUOTA_CODES:
        return _base_result("quota", False, status, msg[:500], "block-and-alert")
    if codes & _RATE_LIMIT_CODES:
        return _base_result("rate_limit", True, status, msg[:500],
                            "backoff-and-observe")

    # 3) status 与消息关键词回退（与原分类顺序一致，含 permission_denied/模型权限）。
    if (status in (401, 403) or "unauthorized" in hay or "invalid api key" in hay
            or "permission_denied" in hay or "model_permission" in hay):
        return _base_result("auth", False, status, msg[:500], "block-and-alert")
    if status == 429 or "rate limit" in hay or "too many requests" in hay:
        return _base_result("rate_limit", True, status, msg[:500],
                            "backoff-and-observe")
    if "quota" in hay or "insufficient" in hay or "balance" in hay:
        return _base_result("quota", False, status, msg[:500], "block-and-alert")
    if ("model_not_found" in hay or "no available channel" in hay
            or "not found" in hay):
        return _base_result("model_unavailable", False, status, msg[:500],
                            "block-and-alert")
    if status and 500 <= status < 600:
        return _base_result("server", True, status, msg[:500],
                            "observe-provider-retry")
    if status and 400 <= status < 500:
        return _base_result("request", False, status, msg[:500],
                            "block-and-alert")
    return _base_result("unknown", True, status, msg[:500], "observe")


def classify_http_error(status: int | None, tag: str | None = None,
                        message: str = "") -> dict:
    """协调者/唤醒通道的 HTTP 错误分类。"""
    low = (message or "").lower()
    if status == 429:
        return {"category": "rate_limit", "transient": True, "action": "backoff"}
    if status in (401, 403):
        return {"category": "auth", "transient": False, "action": "block-and-alert"}
    if status == 404:
        return {"category": "not_found", "transient": False, "action": "mark-target-lost"}
    if status == 409 or tag == "ConflictError":
        return {"category": "conflict", "transient": True, "action": "reconcile-inbox"}
    if status and 500 <= status < 600:
        return {"category": "server", "transient": True, "action": "backoff"}
    if "quota" in low or "insufficient" in low:
        return {"category": "quota", "transient": False, "action": "block-and-alert"}
    return {"category": "unknown", "transient": True, "action": "backoff"}
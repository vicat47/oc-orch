"""本地邮件授权配置：默认关闭、严格类型、真实地址只在本机。

配置位于 ``$OC_ORCH_HOME/email-policy.json``（文件权限建议 600，home 目录
本身是 700）。未配置/``enabled=false`` 时返回默认关闭策略；任何字段缺失、
类型错误、范围为空或发件绑定未核实都 fail-closed，不猜测、不从环境变量或
个人配置推断地址。

该模块只构造 domain 策略与 Agently 发件绑定，不导入网络/CLI；真实发送由
``runtime.email_executor`` 的独立有限 tick 执行。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from .agently_mail import SenderBinding
from ..domain.operator_email import (
    EVENT_KINDS,
    EmailPolicy,
    Recipient,
    Sender,
    POLICY_ENABLED,
    policy_state,
)

CONFIG_NAME = "email-policy.json"
CONFIG_SCHEMA = "oc-orch.email-policy.v1"
MAX_RECIPIENTS = 20
MAX_PROJECTS = 200
MAX_TIMEOUT_SECONDS = 120.0
MAX_RECEIPT_LIMIT = 10


class EmailConfigError(RuntimeError):
    """配置不可用/不完整；调用方必须保持关闭，不得部分放行。"""


@dataclass(frozen=True)
class EmailRuntime:
    policy: EmailPolicy
    sender_binding: SenderBinding | None
    recipient_refs: tuple[str, ...]
    source_path: str
    state: str

    @property
    def enabled(self) -> bool:
        return self.state == POLICY_ENABLED


def _nk(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise EmailConfigError(f"{field} 必须为非空且无首尾空白的字符串")
    if any(ch in value for ch in "\r\n\x00"):
        raise EmailConfigError(f"{field} 不得包含控制字符/换行")
    return value


def _bool(value: Any, field: str) -> bool:
    if type(value) is not bool:
        raise EmailConfigError(f"{field} 必须为严格布尔")
    return value


def _int(value: Any, field: str, *, minimum: int, maximum: int) -> int:
    if type(value) is not int or not (minimum <= value <= maximum):
        raise EmailConfigError(f"{field} 必须为 {minimum}..{maximum} 的整数")
    return value


def _float(value: Any, field: str, *, minimum: float,
           maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EmailConfigError(f"{field} 必须为数值")
    number = float(value)
    if not (minimum <= number <= maximum):
        raise EmailConfigError(f"{field} 必须在 {minimum}..{maximum}")
    return number


def _disabled(path: str) -> EmailRuntime:
    return EmailRuntime(policy=EmailPolicy(), sender_binding=None,
                        recipient_refs=(), source_path=path,
                        state="disabled")


def _read_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        raise EmailConfigError(f"无法读取/解析 {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise EmailConfigError("配置根必须是 JSON 对象")
    if raw.get("schema") != CONFIG_SCHEMA:
        raise EmailConfigError(f"schema 必须为 {CONFIG_SCHEMA}")
    return raw


def load_email_runtime(home: str) -> EmailRuntime:
    """读取并严格校验本机邮件策略；缺失/关闭均返回明确 disabled。"""
    path = os.path.join(home, CONFIG_NAME)
    raw = _read_json(path)
    if not raw or raw.get("enabled") is not True:
        # 关闭态不解析其余字段，保持零范围、零发送。
        return _disabled(path)
    _bool(raw.get("enabled"), "enabled")
    sender_raw = raw.get("sender")
    if not isinstance(sender_raw, dict):
        raise EmailConfigError("sender 必须为对象")
    sender_ref = _nk(sender_raw.get("ref"), "sender.ref")
    sender_identity = _nk(sender_raw.get("identity"), "sender.identity")
    verified = _bool(sender_raw.get("verified"), "sender.verified")
    if verified is not True:
        raise EmailConfigError("sender.verified 必须为 true")
    version = _nk(sender_raw.get("version"), "sender.version")
    recipients_raw = raw.get("recipients")
    if not isinstance(recipients_raw, list) or not recipients_raw:
        raise EmailConfigError("recipients 必须为非空数组")
    if len(recipients_raw) > MAX_RECIPIENTS:
        raise EmailConfigError(f"recipients 最多 {MAX_RECIPIENTS} 项")
    recipients: list[Recipient] = []
    seen_refs: set[str] = set()
    for index, item in enumerate(recipients_raw):
        if not isinstance(item, dict):
            raise EmailConfigError(f"recipients[{index}] 必须为对象")
        ref = _nk(item.get("ref"), f"recipients[{index}].ref")
        address = _nk(item.get("address"), f"recipients[{index}].address")
        if "@" not in address:
            raise EmailConfigError(f"recipients[{index}].address 不像邮箱地址")
        if ref in seen_refs:
            raise EmailConfigError("recipients.ref 不得重复")
        seen_refs.add(ref)
        recipients.append(Recipient(ref=ref, address=address))
    projects_raw = raw.get("projects")
    if not isinstance(projects_raw, list) or not projects_raw:
        raise EmailConfigError("projects 必须为非空数组")
    if len(projects_raw) > MAX_PROJECTS:
        raise EmailConfigError(f"projects 最多 {MAX_PROJECTS} 项")
    projects = frozenset(
        _nk(item, f"projects[{index}]") for index, item in enumerate(projects_raw))
    kinds_raw = raw.get("event_kinds")
    if not isinstance(kinds_raw, list) or not kinds_raw:
        raise EmailConfigError("event_kinds 必须为非空数组")
    kinds = frozenset(_nk(item, "event_kinds[]") for item in kinds_raw)
    unknown = kinds - EVENT_KINDS
    if unknown:
        raise EmailConfigError(f"event_kinds 含未知类别：{sorted(unknown)}")
    retry_min = _int(raw.get("retry_min_ms", 60_000), "retry_min_ms",
                     minimum=10_000, maximum=86_400_000)
    retry_max = _int(raw.get("retry_max_ms", 3_600_000), "retry_max_ms",
                     minimum=10_000, maximum=86_400_000)
    if retry_min > retry_max:
        raise EmailConfigError("retry_min_ms 不得大于 retry_max_ms")
    policy = EmailPolicy(
        enabled=True,
        sender=Sender(ref=sender_ref, identity=sender_identity),
        recipients=tuple(recipients),
        projects=projects,
        event_kinds=kinds,
        unattended=_bool(raw.get("unattended"), "unattended"),
        receipt_queries_authorized=_bool(
            raw.get("receipt_queries_authorized", False),
            "receipt_queries_authorized"),
        retry_min_ms=retry_min,
        retry_max_ms=retry_max,
        max_attempts=_int(raw.get("max_attempts", 3), "max_attempts",
                          minimum=1, maximum=10),
        send_timeout_seconds=_float(
            raw.get("send_timeout_seconds", 30.0),
            "send_timeout_seconds", minimum=1.0,
            maximum=MAX_TIMEOUT_SECONDS),
        search_timeout_seconds=_float(
            raw.get("search_timeout_seconds", 30.0),
            "search_timeout_seconds", minimum=1.0,
            maximum=MAX_TIMEOUT_SECONDS),
        receipt_check_limit=_int(
            raw.get("receipt_check_limit", 3), "receipt_check_limit",
            minimum=1, maximum=MAX_RECEIPT_LIMIT),
    )
    state = policy_state(policy)
    if state != POLICY_ENABLED:
        raise EmailConfigError(f"策略状态不是 enabled：{state}")
    return EmailRuntime(
        policy=policy,
        sender_binding=SenderBinding(
            ref=sender_ref, identity=sender_identity, verified=True,
            version=version),
        recipient_refs=tuple(r.ref for r in recipients),
        source_path=path,
        state=state,
    )

__all__ = ["CONFIG_NAME", "CONFIG_SCHEMA", "EmailConfigError",
           "EmailRuntime", "load_email_runtime"]
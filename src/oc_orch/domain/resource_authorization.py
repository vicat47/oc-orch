"""独立资源授权：严格条目、不可变版本/CAS/操作幂等与只读授权检查。

本模块刻意不把“模式/预设”当作授权本身：只有显式记录在资源授权版本中的
条目才是独立授权来源；免费属性只来自记录的 cost，不读模型目录或价格猜测。
authorize 只做只读检查、不产生单次动作许可（许可由上层在同一连接事务内
与模式切换一起组装）。
"""

from __future__ import annotations

import hashlib
import json

from .errors import LedgerError

SCHEMA = 1
SCOPES = ("project", "task", "coordinator")
COSTS = ("no-extra-cost", "paid")
PRESETS = ("budget-first", "elastic", "latency-first")

# 授权拒绝/损坏的稳定错误码，供上层在许可与回退决策中使用。
CODE_CORRUPT = "RESOURCE_AUTHORIZATION_CORRUPT"
CODE_INVALID = "RESOURCE_AUTHORIZATION_INVALID"
CODE_MISSING = "RESOURCE_AUTHORIZATION_MISSING"
CODE_REVOKED = "RESOURCE_AUTHORIZATION_REVOKED"
CODE_EXPIRED = "RESOURCE_AUTHORIZATION_EXPIRED"
CODE_PRESET = "RESOURCE_AUTHORIZATION_PRESET_REJECTED"
CODE_TASK_MISSING = "RESOURCE_AUTHORIZATION_TASK_MISSING"
CODE_REVISION = "RESOURCE_AUTHORIZATION_REVISION_CONFLICT"
CODE_OPERATION = "RESOURCE_AUTHORIZATION_OPERATION_CONFLICT"


def invalid(message: str) -> None:
    """调用方参数/形状错误。"""
    raise LedgerError(CODE_INVALID, message)


def corrupt(message: str) -> None:
    """持久记录损坏/半截状态，失败关闭。"""
    raise LedgerError(CODE_CORRUPT, message)


def identity(value, label: str) -> str:
    if (not isinstance(value, str) or not value.strip() or value != value.strip()
            or len(value) > 500 or any(ord(c) < 32 for c in value)):
        invalid(f"{label}必须为无首尾空白/控制符的非空字符串（最多500字符）")
    return value


def revision(value) -> int:
    if type(value) is not int or value < 0:
        invalid("expected_revision必须为非负整数")
    return value


def future_expiry(value, now_ms: int) -> int:
    """expires_at 必须严格未来（int 毫秒），过期不删历史、只拒绝使用。"""
    if type(value) is not int or value <= now_ms:
        invalid(f"expires_at必须为严格未来的毫秒时间戳（当前{now_ms}）")
    return value


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def fingerprint(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def model_identity(value) -> str:
    value = identity(value, "model")
    parts = value.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1] or any(p != p.strip() for p in parts):
        invalid("model必须是明确的provider/model身份，不能猜默认模型")
    return value


def normalize_resources(resources) -> list:
    """严格条目；模型唯一；空列表表示撤销。未知字段/坏类型一律拒绝。"""
    if not isinstance(resources, list):
        invalid("resources必须为列表（空列表表示撤销）")
    normalized: list[dict] = []
    seen: set[str] = set()
    for item in resources:
        if not isinstance(item, dict) or set(item) != {"model", "cost", "evidence",
                                                      "internal_calls_controlled"}:
            invalid("资源条目必须严格包含且仅包含model/cost/evidence/internal_calls_controlled")
        model = model_identity(item["model"])
        cost = item["cost"]
        if not isinstance(cost, str) or cost not in COSTS:
            invalid("cost必须为no-extra-cost或paid")
        evidence = identity(item["evidence"], "evidence")
        if type(item["internal_calls_controlled"]) is not bool:
            invalid("internal_calls_controlled必须为bool")
        if model in seen:
            invalid("资源模型必须唯一")
        seen.add(model)
        normalized.append({"model": model, "cost": cost, "evidence": evidence,
                           "internal_calls_controlled": item["internal_calls_controlled"]})
    return normalized


def validate_version(record, *, scope: str, scope_id: str, project,
                     expected_revision: int) -> dict:
    """持久格式严格校验；持久读取路径上的形状错误应被包装为损坏。"""
    if scope not in SCOPES:
        invalid("未知scope")
    fields = {"schema", "scope", "scope_id", "project", "revision", "resources",
              "expires_at", "actor", "reason", "operation_id", "created_at"}
    if not isinstance(record, dict) or set(record) != fields:
        invalid("授权版本字段不完整或未知")
    if record["scope"] != scope or record["scope_id"] != scope_id:
        invalid("授权版本scope/scope_id不匹配")
    if (type(record["schema"]) is not int or record["schema"] != SCHEMA
            or type(record["revision"]) is not int
            or record["revision"] != expected_revision or expected_revision < 1):
        invalid("授权版本schema/revision不匹配")
    identity(scope_id, "scope_id")
    if scope == "project":
        expected_project = scope_id
    elif scope == "task":
        expected_project = project
    else:  # coordinator：独立 session 授权，不携带/不猜 project
        expected_project = None
    if expected_project is not None:
        identity(expected_project, "project")
        if record["project"] != expected_project:
            invalid("授权版本project不匹配")
    else:
        if record["project"] is not None:
            invalid("coordinator授权不得携带project")
    for key in ("actor", "reason", "operation_id"):
        identity(record[key], key)
    if type(record["created_at"]) is not int or record["created_at"] < 0:
        invalid("created_at非法")
    if type(record["expires_at"]) is not int or record["expires_at"] < 0:
        invalid("expires_at非法")
    return {**record, "resources": normalize_resources(record["resources"])}


def check_authorization(resources, *, model: str, preset: str, now_ms: int,
                        expires_at: int) -> dict:
    """在已核验的版本上做只读授权检查；任何拒绝抛稳定码，不产生许可。

    - 过期：now_ms >= expires_at 即过期，拒绝而不是删历史；
    - 无model覆盖：MISSING；空资源撤销由适配器在调用前以REVOKED拒绝；
    - budget-first：只允许 no-extra-cost 且 internal_calls_controlled=True；
      否则不能宣称整条路线无额外费用；
    - elastic/latency-first：独立授权覆盖内的 paid 允许，no-extra-cost 允许。
    """
    validate_check_args(model=model, preset=preset, now_ms=now_ms)
    if now_ms >= expires_at:
        raise LedgerError(CODE_EXPIRED, "资源授权已过期",
                          {"expires_at": expires_at, "now_ms": now_ms})
    entry = next((e for e in resources if e["model"] == model), None)
    if entry is None:
        raise LedgerError(CODE_MISSING, "该模型未被独立授权",
                          {"model": model, "preset": preset})
    if preset == "budget-first":
        if entry["cost"] != "no-extra-cost" or entry["internal_calls_controlled"] is not True:
            raise LedgerError(
                CODE_PRESET, "预算优先只允许内部调用受控的无额外费用资源",
                {"model": model, "cost": entry["cost"],
                 "internal_calls_controlled": entry["internal_calls_controlled"]})
    return {"entry": entry}


def validate_check_args(*, model, preset, now_ms) -> None:
    """authorize 参数先行校验：坏参数无论当前状态一律 INVALID，不读库。"""
    model_identity(model)
    if not isinstance(preset, str) or preset not in PRESETS:
        invalid("未知preset")
    if type(now_ms) is not int or now_ms < 0:
        invalid("now必须为非负毫秒时间戳")

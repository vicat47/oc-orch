"""交付意图与动态指令的纯规则；不是资源授权或执行生效证明。"""

from __future__ import annotations

import hashlib
import json

from .errors import LedgerError

SCHEMA = 1
PRESETS = {
    "budget-first": "预算优先",
    "elastic": "弹性加速",
    "latency-first": "时效优先",
}
STAGES = {"standard": "正式交付", "prototype": "快速原型"}
LIMITS = {"max_parallel": (1, 32), "max_candidates": (1, 8),
          "max_challenge_rounds": (0, 8)}


def invalid(message: str) -> None:
    raise LedgerError("DELIVERY_POLICY_INVALID", message)


def identity(value, label: str) -> str:
    if (not isinstance(value, str) or not value.strip() or value != value.strip()
            or len(value) > 500 or any(ord(c) < 32 for c in value)):
        invalid(f"{label}必须为无首尾空白/控制符的非空字符串（最多500字符）")
    return value


def revision(value) -> int:
    if type(value) is not int or value < 0:
        invalid("expected_revision必须为非负整数")
    return value


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def fingerprint(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def normalize_policy(value) -> dict:
    """拒绝未知字段、bool上限及供应商/权限字段，不静默猜默认策略。"""
    if not isinstance(value, dict) or set(value) != {"preset", "stage", "limits"}:
        invalid("策略必须且只能包含preset/stage/limits")
    preset, stage, limits = value["preset"], value["stage"], value["limits"]
    if not isinstance(preset, str) or preset not in PRESETS:
        invalid("未知preset")
    if not isinstance(stage, str) or stage not in STAGES:
        invalid("未知stage")
    if not isinstance(limits, dict) or set(limits) != set(LIMITS):
        invalid("limits必须包含三个独立上限")
    for key, (lo, hi) in LIMITS.items():
        if type(limits[key]) is not int or not lo <= limits[key] <= hi:
            invalid(f"{key}必须为{lo}..{hi}整数")
    return {"preset": preset, "stage": stage,
            "limits": {key: limits[key] for key in LIMITS}}


def validate_version(record, *, scope: str, scope_id: str, project: str,
                     expected_revision: int) -> dict:
    """持久格式严格校验；调用方将参数错误转为损坏，不能当缺记录放行。"""
    fields = {"schema", "scope", "scope_id", "project", "revision", "policy",
              "source", "actor", "reason", "operation_id", "created_at"}
    if not isinstance(record, dict) or set(record) != fields:
        invalid("版本字段不完整或未知")
    if scope not in ("project", "task") or (scope == "project" and scope_id != project):
        invalid("版本scope非法")
    identity(scope_id, "scope_id")
    if (type(record["schema"]) is not int or record["schema"] != SCHEMA
            or record["scope"] != scope or record["scope_id"] != scope_id
            or record["project"] != project or type(record["revision"]) is not int
            or record["revision"] != expected_revision or expected_revision < 1):
        invalid("版本身份/schema/revision不匹配")
    identity(project, "project")
    for key in ("actor", "reason", "operation_id"):
        identity(record[key], key)
    if type(record["created_at"]) is not int or record["created_at"] < 0:
        invalid("created_at非法")
    normalize_policy(record["policy"])
    source = record["source"]
    if source is not None:
        if (scope != "task" or not isinstance(source, dict)
                or set(source) != {"scope", "scope_id", "revision"}
                or source["scope"] != "project" or source["scope_id"] != project
                or type(source["revision"]) is not int or source["revision"] < 1):
            invalid("继承来源非法")
    return record


def render_instructions(record: dict) -> str:
    """仅投影已核验版本，不改Task验收、模型白名单或用户原文。"""
    if not isinstance(record, dict):
        invalid("指令投影需要合法策略版本")
    validate_version(record, scope=record.get("scope"), scope_id=record.get("scope_id"),
                     project=record.get("project"), expected_revision=record.get("revision"))
    policy = normalize_policy(record["policy"])
    mode, limits = policy["preset"], policy["limits"]
    lines = [f"交付策略：{PRESETS[mode]} / {STAGES[policy['stage']]}；"
             f"{record['scope']} {record['scope_id']} / 版本 {record['revision']}。",
             "快：缩短可信交付关键路径；准：以需求、事实和必要验证裁决；"
             "狠：集中攻坚、停止无效循环；优：优先简单、稳定、可维护。",
             "只在既有资源/费用授权内行动；预设不是授权，不改变父模型或扩大工具权限。"]
    if mode == "budget-first":
        lines.append("仅使用已确认对本用户无额外费用且获授权的资源；费用未知不得当免费。")
    elif mode == "elastic":
        lines.append("免费路线为主；关键环节只可使用已独立授权的加速资源，不自动升档或付费。")
    else:
        lines.append("在权限与共同质量底线内缩短预计剩余交付时间；计入排队、上下文重建和协调成本。")
    lines.extend([
        f"分工并发上限{limits['max_parallel']}，完整候选上限{limits['max_candidates']}，"
        f"可选挑战轮次上限{limits['max_challenge_rounds']}；上限不是必须用满。",
        "简单明确工作不强制多agent；新增分工说明关键路径收益，新增候选说明待消除的不确定性。",
        "短计划、增量处理；达到必要验收及时收口；可选挑战早停不替代注册检查或collect/verify。",
        "模式切换不抢占在途、不重播未知输入，不清项目/回合/provider/任务控制，不改既有验收。",
    ])
    if policy["stage"] == "prototype":
        lines.append("本轮先交付可体验代表路径；明确真实接入/mock模拟/未实现占位/受阻，"
                     "不得伪造保存成功或把接口错误当空数据。缺口保留稳定ID、责任、下一动作和补齐证据；"
                     "不降低原正式任务承诺，不为未来扩展先造通用框架。")
    else:
        lines.append("按既有正式验收核收；未完成、未知或受阻必须明确，不以聊天完成回执代替证据。")
    return "\n".join(lines)

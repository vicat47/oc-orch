"""提示词层的工作模式：只影响 prompt，不产生资源/模型/费用授权。

默认 ``latency-first``（时效优先）。``fast``/``budget`` 是 CLI 别名；持久/展示
统一使用交付策略既有 preset 名称，避免两套语义。该模块不读取 DB、不调用
``delivery_policy`` 版本、不检查 delivery 写契约，因此可在策略未启用时安全使用。
"""

from __future__ import annotations

from .errors import LedgerError

MODES = ("latency-first", "budget-first", "elastic")
ALIASES = {"fast": "latency-first", "budget": "budget-first",
           "elastic": "elastic"}
DEFAULT_MODE = "latency-first"

_LABELS = {
    "latency-first": "时效优先",
    "budget-first": "预算优先",
    "elastic": "弹性加速",
}

_PREFIX = ("工作模式（提示词提示，不是资源/模型强制）：{mode}（{label}）。\n"
           "- 先关键路径与最小充分验证；避免重复调研、重复实现和不必要并行；"
           "达到必要验收及时收口。\n"
           "- 只在既有权限、已授权资源和共同质量底线内加速；不降低任务验收标准。\n"
           "- 该模式不改变模型/provider/工具权限，也不代表费用或预算已被锁定。")


def normalize_work_mode(value: str | None) -> str:
    """严格规范模式/别名；空值使用默认，未知值 fail-closed。"""
    if value is None or value == "":
        return DEFAULT_MODE
    if not isinstance(value, str) or value != value.strip():
        raise LedgerError("WORK_MODE_INVALID", "工作模式必须为无空白的字符串")
    mode = ALIASES.get(value, value)
    if mode not in MODES:
        raise LedgerError(
            "WORK_MODE_INVALID",
            f"未知工作模式：{value}；可选 {', '.join(MODES)} 或 fast/budget/elastic")
    return mode


def render_work_mode(value: str | None = None) -> str:
    """渲染提示词块；调用方负责把它放在任务目标之后、正文之前。"""
    mode = normalize_work_mode(value)
    text = _PREFIX.format(mode=mode, label=_LABELS[mode])
    if mode == "budget-first":
        text = text.replace(
            "- 先关键路径与最小充分验证；避免重复调研、重复实现和不必要并行；达到必要验收及时收口。",
            "- 只用已确认无额外费用且已授权的资源；费用未知不得当免费；"
            "先关键路径与最小充分验证，避免重复调研和不必要并行。")
    elif mode == "elastic":
        text = text.replace(
            "- 先关键路径与最小充分验证；避免重复调研、重复实现和不必要并行；达到必要验收及时收口。",
            "- 免费路线为主；关键环节只使用已独立授权的加速资源，不自动升档或付费；"
            "先关键路径与最小充分验证，达到必要验收及时收口。")
    return text


__all__ = ["ALIASES", "DEFAULT_MODE", "MODES", "normalize_work_mode",
           "render_work_mode"]
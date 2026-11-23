"""运行配置适配：默认值 + $OC_ORCH_HOME/config.json 覆盖。

兼容旧导入路径 oc_orch.config（root config.py 为 sys.modules 别名指向本模块）；
本文件是配置加载的单一权威实现。
"""

from __future__ import annotations

import json
import os

DEFAULTS = {
    # 调度节奏
    "reconcile_interval": 15,
    "heartbeat_interval": 5,
    "event_socket_timeout": 90,
    # 通知/唤醒
    "ack_timeout": 900,
    "max_wake_attempts": 3,
    "notification_batch": 20,
    "notification_retry_min": 30,
    "notification_retry_max": 900,
    # 空闲目标上自有排队唤醒的原 ID 恢复（秒）；超过宽限才 steer，
    # 每条消息最多尝试一次，仍滞留则转可见告警。
    "wake_inbox_recover_grace": 120,
    "max_wake_inbox_recover": 1,
    # 停滞/阻塞
    "stall_after": 1200,
    "startup_grace": 120,
    "tool_expected_grace": 900,
    "blocked_escalate_after": 3600,
    "dispatch_unknown_grace": 120,
    "dispatch_max_retries": 5,
    "provider_retry_alert_threshold": 5,
    "provider_retry_alert_after": 600,
    # 结果
    "auto_collect": True,
    "result_max_messages": 100,
    "result_store_limit": 200000,
    # 结果收集预算与扫描边界（D10；正值校验在 _validate）
    "collect_retry_interval": 15,
    "collect_max_attempts": 5,
    "result_scan_max_pages": 20,
    "deliverable_scan_max_entries": 10000,
    "deliverable_scan_max_bytes": 536870912,
    # 协调者恢复
    "auto_coordinator_recovery": True,
    "max_coordinator_recovery": 2,
    "recovery_delay": 60,
    "coordinator_lease_ttl": 300,
    # 告警
    "desktop_alerts": True,
}


_POSITIVE_INT_KEYS = (
    "collect_retry_interval", "collect_max_attempts", "result_scan_max_pages",
    "deliverable_scan_max_entries", "deliverable_scan_max_bytes",
    "wake_inbox_recover_grace", "max_wake_inbox_recover",
)


def _apply_override(cfg: dict, user: dict) -> None:
    """合并用户覆盖；新增预算/扫描键必须为正整数，否则忽略（D10）。"""
    for key, value in user.items():
        if key not in DEFAULTS:
            continue
        if key in _POSITIVE_INT_KEYS:
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                continue
        cfg[key] = value


def load_config(home: str) -> dict:
    cfg = dict(DEFAULTS)
    path = os.path.join(home, "config.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                user = json.load(f)
            if isinstance(user, dict):
                _apply_override(cfg, user)
        except (OSError, ValueError):
            pass
    env = os.environ.get("OC_ORCH_CONFIG_JSON")
    if env:
        try:
            user = json.loads(env)
            if isinstance(user, dict):
                _apply_override(cfg, user)
        except ValueError:
            pass
    return cfg
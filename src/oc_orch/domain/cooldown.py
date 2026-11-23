"""纯冷却时长规则。

只做数值计算：不读台账/配置，不执行任何等待，不依赖时钟。供 runtime 在
后续切片（S3.2/S3.3）中编排目标级冷却门与半开探测时使用。参数均为初始
拟议值，接入前需用本地可控时钟校验；等待逻辑本身由 runtime 负责。
"""

from __future__ import annotations

import math

COOLDOWN_MS_15 = 15 * 60 * 1000
COOLDOWN_MS_30 = 30 * 60 * 1000
COOLDOWN_MS_60 = 60 * 60 * 1000

# 订阅限额无窗口信息时的候选退避阶梯：15 → 30 → 60 分钟（D4.5）。
SUBSCRIPTION_COOLDOWN_STEPS_MS = (COOLDOWN_MS_15, COOLDOWN_MS_30, COOLDOWN_MS_60)

# 每个阻塞 episode 最多 3 次单路半开探测（D4.6；实际记账由 runtime 完成）。
MAX_HALF_OPEN_PROBES = 3

# 短时限流/服务瞬时的指数退避：30s 起步，上限 15 分钟（与既有 transient
# 上限 900s 一致）；jitter 由 runtime 应用，此处只给基数。
RATE_LIMIT_BASE_MS = 30 * 1000
RATE_LIMIT_CAP_MS = 15 * 60 * 1000

# 认证/模型不可用/请求等硬阻塞的建议等待（与现有非 transient 处理一致）。
HARD_BLOCK_MS = 60 * 60 * 1000


def cooldown_delay_ms(category: str, failure_count: int,
                      retry_after_ms: int | None = None) -> int:
    """纯冷却等待毫秒数。

    优先采用可信 retry_after_ms（>0 的数值）；无窗口信息时按类别回退：

    - ``quota``：订阅限额阶梯 15/30/60 分钟，按 failure_count 1/2/≥3 封顶；
    - ``rate_limit`` / ``server``：30s * 2^n 秒级指数退避，上限 15 分钟；
    - 其他硬阻塞（auth/model_unavailable/request/unknown）：固定 1 小时。

    failure_count 从 1 起算，<=0 按 1 处理。
    """
    if isinstance(retry_after_ms, (int, float)) and not isinstance(retry_after_ms, bool):
        if math.isfinite(retry_after_ms) and retry_after_ms > 0:
            return int(retry_after_ms)
    n = max(1, int(failure_count or 0))
    if category == "quota":
        idx = min(n, len(SUBSCRIPTION_COOLDOWN_STEPS_MS)) - 1
        return int(SUBSCRIPTION_COOLDOWN_STEPS_MS[idx])
    if category in ("rate_limit", "server"):
        return min(RATE_LIMIT_CAP_MS, RATE_LIMIT_BASE_MS * (2 ** min(n, 5)))
    return HARD_BLOCK_MS
"""唤醒抑制策略：避免在目标 session 已有排队/未确认唤醒时继续压栈。

兼容旧导入路径 oc_orch.wake_policy（root wake_policy.py 为 sys.modules 别名
指向本模块）；本文件是唤醒抑制逻辑的单一权威实现。

V2 语义：
- `delivery=queue` 会持久入队；目标 busy 时等当前回合结束才投递。
- `session.inbox` 列出尚未投递的输入；它不是“已处理”。
- 一条 `sent` 只表示 HTTP 已受理，不表示协调者已读/已 ack。

策略：
1. 观察 unknown（active/inbox 查询失败）→ 抑制；查询失败≠空队列/空闲。
2. 目标 inbox 非空 → 已有排队输入，不再追加。
3. 存在较新的未 ack sent → 等它被消费/ack，不追加。
4. 目标 active 且仍有未 ack sent → 不追加。
5. 只有“空闲、无 inbox、旧 sent 超过 ack_timeout”才允许一次有限 nudge。
"""

from __future__ import annotations


def should_suppress_wake(
    *,
    outstanding: list[dict],
    inbox: list[dict],
    is_active: bool,
    now_ms: int,
    ack_timeout_ms: int,
    known: bool = True,
) -> tuple[bool, str]:
    """判断是否抑制自动唤醒；返回 (suppress, stable_reason)。

    ``known`` 是兼容可选标志（默认 True 保持既有调用不变）：发送前的
    active/inbox 观察若存在查询失败，必须由调用方传 ``known=False``——
    失败不等于“空队列/空闲”，不能借空观察追加模型输入。
    """
    if not known:
        return True, "observation-unknown"
    if inbox:
        return True, "inbox-pending"
    young = [
        n for n in outstanding
        if n.get("sent_at") and now_ms - int(n["sent_at"]) < ack_timeout_ms
    ]
    if young:
        return True, "unacked-recent"
    if is_active and outstanding:
        return True, "target-active-with-unacked"
    return False, ""
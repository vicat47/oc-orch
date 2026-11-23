"""目标级额度闸门端口契约（S3.2/S3.3 最小 API）。

runtime/application 只依赖本窄端口；默认实现是
``oc_orch.adapters.target_gate.TargetGate``（SQLite/meta 持久化）。所有方法
返回纯数据 dict，不抛业务异常（结构错误如 schema 版本不兼容除外）；不做网络。

设计不变量（详见 docs/…/D-gate-api.md 与 openspec D4）：

- 以目标 session 为最小作用域：worker 自己的限额错误不阻塞其他目标/coordinator。
- ``blocked_until/category/source/revision/failure_count/probe_count`` 持久保存；
  重复错误不延长窗口、不重置探测预算；等待不消耗通知次数。
- 每个阻塞 episode 最多 ``MAX_HALF_OPEN_PROBES`` 次单路半开探测；耗尽返回
  ``exhausted``（由 runtime 转告警责任）。
- ``claim_probe`` 必须在真实发送前原子占用（单半开）；普通 ``get`` 不消耗预算。
- 只有可归属到本输入半开探测的新成功能清除闸门；旧成功/started 不能清除。
- 历史人工门（``block_manual``）保守保持，仅显式 ``clear_manual`` 可解锁。
  新项目暂停/回合hold/用户屏障采用独立控制台账；runtime不因陌生输入清门。
- 持久记录统一验证并 fail-closed：schema/session_id/必需控制字段与类型/
  probe 关联一致性/时钟边界；坏字段一律 ``TargetGateStateError``（5 方法均拒绝
  且回滚不改记录，缺失必需字段不填默认放行）。
- 有效时钟边界 ``MAX_CLOCK_MS``：荒谬时间（如 10**30）不产生天文 deadline。
"""

from __future__ import annotations

from typing import Any, Protocol


class TargetGatePort(Protocol):
    """目标级额度闸门：单目标持久冷却 + 有界半开探测。"""

    def observe_error(
        self,
        session_id: str,
        classification: dict[str, Any],
        *,
        evidence_key: str,
        observed_at: int,
        source: str = "provider",
        origin_at: int | None = None,
    ) -> dict[str, Any]:
        """登记一次目标 provider 错误。

        - 无闸门 → 开启新 episode，按 ``cooldown_delay_ms`` 计算 ``blocked_until``
          （可信 Retry-After 优先；订阅限额 15/30/60 分钟阶梯）。
        - 窗内/同证据重复 → 不延长窗口、不重置 failure/probe 预算。
        - 窗口已过的失败回合推进 failure_count 并按阶梯重算窗口；探测预算耗尽时
          置 ``exhausted``（不再自动探测）。
        - 人工（manual）状态优先：provider 错误不覆盖、不自动解锁。
        - ``origin_at``（兼容追加，可选）：证据的**稳定源时间**（如事件 created_ms /
          canonical 执行输入）。事实判定一律用 origin_at（缺省回退 observed_at），
          观察时间只作展示——过期重复事实不能被刷新时间冒充新失败（G2）。
        - 荒谬时间（超出有效时钟边界的 observed_at / origin_at）返回
          ``invalid-timestamp`` 且不清现有闸门、不造天文 deadline（G6）。

        返回 ``{session_id, gate, changed, reason}``；持久记录结构非法时抛
        :class:`oc_orch.adapters.target_gate.TargetGateStateError`（fail-closed）。
        """

    def get(self, session_id: str) -> dict[str, Any] | None:
        """只读快照：当前闸门状态或 None（无活动闸门）。不消耗任何预算。"""

    def claim_probe(
        self,
        session_id: str,
        *,
        owner: str,
        message_id: str,
    ) -> dict[str, Any]:
        """原子占用单路半开探测（必须在真实发送前调用）。

        返回 ``{session_id, allowed, reason, spent, state}``：

        - ``allowed=True, spent=True`` 取得探测权（probe_count+1）；
        - ``allowed=True, spent=False`` 无活跃冷却（不耗预算）；
        - ``allowed=False`` 冷却中（cooldown）、人工（manual）、预算耗尽
          （exhausted）、已有未决半开（probe-outstanding）或半开超时未知
          （probe-unknown，保留原 message 身份、转可见阻塞）。
        """

    def record_success(
        self,
        session_id: str,
        *,
        message_id: str,
        observed_at: int,
        evidence_key: str,
        expected_revision: int | None = None,
    ) -> dict[str, Any]:
        """只有可归属到本输入半开探测的新成功才能清除闸门。

        - 无活动闸门 / message 与 probe_message_id 不匹配 / 早于 probe_started_at
          （旧 success/started）/ 人工态 → 不清除。
        - 匹配且可归属 → 原子清除并返回 ``cleared=True``。
        - ``expected_revision`` 为查询前取得的版本；网络期间控制变化则不清。
          成功不得早于/等于已接受的新错误起源水位，清除和证据审计同事务。

        返回 ``{session_id, cleared, reason, evidence_key, gate}``。
        """

    def claim_probe_observation(
        self, session_id: str, *, message_id: str,
        expected_revision: int, idle_at: int,
    ) -> dict[str, Any]:
        """原子取得一次原 probe 的只读补证资格，不授予发送或重投权限。

        预算/期限/退避持久保存，重复结束事实和重启不重置；新的可信结束
        事实可核验一次。返回 ``{allowed, reason, gate}``；后续成功提交必须
        使用返回 gate 的 revision，结构损坏 fail-closed。
        """

    def block_manual(
        self,
        session_id: str,
        *,
        reason: str,
    ) -> dict[str, Any]:
        """人工中断优先：不自动解锁（blocked_until=None），provider 错误不覆盖。

        返回 ``{session_id, gate, changed, reason}``。
        """

    def clear_manual(
        self,
        session_id: str,
        *,
        reason: str = "manual-recovery",
    ) -> dict[str, Any]:
        """显式清除人工中断闸门；provider 冷却/探测态拒绝清除。

        - 仅当 ``source=manual`` 时删除记录并返回 ``cleared=True``；
        - 无闸门/非 manual 返回 ``cleared=False`` 及原因；
        - 记录损坏时 fail-closed，不删除。

        返回 ``{session_id, cleared, reason, gate}``。
        """

    def clear_provider(
        self,
        session_id: str,
        *,
        reason: str = "operator-recovery",
    ) -> dict[str, Any]:
        """显式恢复 provider 目标调度（operator 授权入口，2026-11-19）。

        把 provider 阻塞态原子复位为 ``probe-ready``，**保留** ``source=provider``
        与 ``recent_errors``/``max_origin_at``/``failure_count`` 证据守卫：新的
        provider 错误仍由 :meth:`observe_error` 重新登记并冷却（fail-closed 不变量
        不变，不是持久绕过）。manual 闸门仍走 :meth:`clear_manual`，不经此处。

        - 无活动闸门 → ``no-active-gate``；
        - manual → ``not-provider``；
        - provider → 返回 ``cleared=True``（reason=``provider-cleared``）。

        返回 ``{session_id, cleared, reason, gate}``。
        """

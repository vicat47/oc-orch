"""后续责任窄端口（S4-A）：application 只依赖本 Protocol，不导入 adapter。

责任 ≠ Task / Attempt / Turn / Notification。责任表达“谁在等待什么、什么事实
到达后交给谁处理”；Notification 只是提醒渠道，其 ACK 不等于责任完成，Task
accepted 也仍须既有 verify 门禁。

默认实现是 ``oc_orch.adapters.responsibility_ledger.ResponsibilityStore``
（组合对象，不并入 Ledger mixin），由组合根装配。台账方法不做任何网络/Client
调用；外部 I/O 由用例在短事务之外完成。

稳定错误码（实现侧 LedgerError.code）：

- ``RESPONSIBILITY_NOT_FOUND`` / ``RESPONSIBILITY_TERMINAL``（已关闭）
- ``RESPONSIBILITY_REVISION_CONFLICT``（CAS 失败：旧快照/并发写）
- ``RESPONSIBILITY_INVALID``（必填缺失/类型错误/未知状态/非法更新字段）
- ``RESPONSIBILITY_TASK_TERMINAL``（任务已 accepted/cancelled/closed）
- ``RESPONSIBILITY_TASK_PAUSED``（暂停中只允许 waiting_human，不自动推进）
- ``RESPONSIBILITY_BUDGET_EXHAUSTED``（budget_used 超 max_budget）
- ``RESPONSIBILITY_STALE``（generation/result_version 已被新代次取代）
- ``RESPONSIBILITY_NOT_REVIEWABLE``（publish 要求 awaiting_review）
- ``RESPONSIBILITY_CLOSE_REFUSED``（业务未终结且无后继时凭 ACK/succeeded 关闭）
- ``RESPONSIBILITY_SCHEMA_MISSING``（未按 schema v3 初始化）
"""

from __future__ import annotations

from typing import Any, Protocol


class ResponsibilityPort(Protocol):
    """未完成工作的持久后续责任。"""

    def open_responsibility(
        self,
        owner: str,
        project: str,
        task_id: str,
        attempt_id: str | None = None,
        generation: int | None = None,
        result_version: str | None = None,
        input_id: str | None = None,
        session_id: str | None = None,
        message_id: str | None = None,
        target_session: str | None = None,
        *,
        state: str = "waiting_input",
        wait_kind: str,
        wait_condition: str,
        next_action: str,
        next_check_at: int,
        deadline: int,
        condition_key: str,
        evidence: dict | None = None,
        max_budget: int = 3,
        actor: str = "coordinator",
    ) -> dict:
        """登记一条后续责任，返回**责任行本身**（含 id/deadline/revision）。

        非持久附加键（不写入台账）：

        - ``responsibility_id``：``id`` 的别名（兼容按责任 id 读取的调用方）；
        - ``created``：本次是否真的新建；
        - ``responsibility``：同一责任行的副本。

        同一 (task, condition_key, generation, result_version) 的未关闭责任 →
        返回原行 ``created=False``，不新建业务身份（避免每轮 reconcile 生成新
        责任/新通知）。任务已 accepted/cancelled/closed 拒绝；暂停中只允许
        ``state='waiting_human'``。前 10 个参数允许位置传递，其余必须关键字。
        """

    def get_responsibility(self, rid: str) -> dict | None:
        """单条责任（含 evidence dict）；不存在返回 None。"""

    def list_open_responsibilities(self, *, task_id: str | None = None,
                                   owner: str | None = None,
                                   limit: int = 200) -> list[dict]:
        """未关闭责任（``closed_at IS NULL``），按 ``next_check_at`` 升序有界返回。"""

    def list_due_responsibilities(self, *, now: int,
                                  limit: int = 200) -> list[dict]:
        """到期检查的未关闭责任（``next_check_at <= now``），不按每轮时间造新身份。"""

    def list_open_responsibilities_page(self, *, after_id: str | None = None,
                                        session_id: str | None = None,
                                        limit: int = 100) -> list[dict]:
        """未关闭责任的 keyset 分页（``id`` 升序，``id > after_id``）。

        调谐循环持持久游标扫描：不依赖外层 SQL，也不靠不断提高 limit。
        ``session_id`` 可选过滤；尾页后游标复位 ``None`` 回绕。无状态、不扫
        全局 session。
        """

    def list_unfinished_tasks_page(self, *, after_id: str | None = None,
                                   limit: int = 100) -> list[dict]:
        """未完成任务（``status NOT IN ('accepted','cancelled')`` 且未关闭）的
        keyset 分页，``id`` 升序；``metadata`` 已反序列化为 dict。

        只扫本台账已登记 Task；尾页后游标复位 ``None`` 回绕。
        """

    def update_responsibility(self, rid: str, *, expected_revision: int,
                              fields: dict[str, Any],
                              actor: str = "supervisor",
                              release_manual_confirmation: str | None = None
                              ) -> dict:
        """CAS 更新白名单字段；失败抛 LedgerError（不静默覆盖）。

        白名单：state/wait_kind/wait_condition/next_action/next_check_at/
        deadline/target_session/evidence/budget_used/max_budget。

        - ``owner`` 与身份字段（task/attempt/generation/result_version/input/
          session/message/condition）**不可**改写：换人/换身份必须走
          ``transfer_responsibility``（因此不能用 update 把旧责任“洗成”新版本）。
        - ``budget_used`` 单调不减且不得超过 ``max_budget``；``max_budget``
          不得上调（``RESPONSIBILITY_BUDGET_INCREASE_REFUSED``），预算跨重启
          不重置。
        - 状态推进交给 ``domain.responsibility.transition``：离开
          ``waiting_human`` 需 ``release_manual_confirmation == rid``（显式人工
          确认，不是 actor 字符串）；进入活跃态前复核 current/代次/结果版本，
          已被取代 → ``RESPONSIBILITY_STALE``。
        """

    def resume_responsibility(self, rid: str, *, expected_revision: int,
                              confirm: str, reason: str,
                              actor: str = "human") -> dict:
        """人工确认后离开 ``waiting_human``（→ ``handling``）。

        权限来自 ``confirm == rid``；不重开终态、不授予写权、不触发执行。
        """

    def transfer_responsibility(self, rid: str, *, expected_revision: int,
                                successor: dict, reason: str,
                                actor: str = "coordinator",
                                release_manual_confirmation: str | None = None
                                ) -> dict:
        """单事务转交：旧责任 ``handed_over`` + 后继已持久登记。

        返回 ``{"previous": dict, "successor": dict}``；任一步失败整体回滚。
        - 后继不得跨 ``task_id``/``project``、不得降低 ``budget_used``、不得提高
          ``max_budget``、不得延长 ``deadline``、不得改写 ``result_version``；
        - ``waiting_human`` 只能转给另一个 ``waiting_human``；转活跃需
          ``release_manual_confirmation == rid``；
        - 已被新代次/结果版本取代的责任不能转成活跃责任（旧人工/代次守卫延续）；
        - 转交本身不授予发送权或写权。
        """

    def close_responsibility(self, rid: str, *, expected_revision: int,
                             reason: str, actor: str = "coordinator",
                             superseded_by: str | None = None) -> dict:
        """CAS 关闭责任：只有**事实**能终结业务推进。

        - 任务真实 ``accepted`` → ``done``；真实 ``cancelled`` → ``cancelled``
          （权威 task 证据，``closed_at`` 非空不算 accepted 证据）；
        - ``superseded_by=<责任 id>``：需该后继仍 open、同任务，且本责任已被
          domain.staleness 判为 stale（当前替代已确证）→ ``handed_over``；
        - 其余（``manual-close``/``duplicate``/``transferred``/任意
          ``superseded:*`` 文本、ACK、succeeded、空 inbox）→
          ``RESPONSIBILITY_CLOSE_REFUSED``；人工放弃走 task cancel 或
          ``waiting_human``；
        - 任务不存在 → 转 ``waiting_human`` 可见诊断后返回，不伪造成功关闭。
        """

    def has_pending_task_notifications(self, task_id: str, *,
                                       result_version: str | None = None) -> int:
        """该任务是否仍有**适用当前归属**的待处理通知（0 = 无）。

        只数 ``pending/inflight/sent``；任务有 current attempt 时只算任务级
        （``attempt_id IS NULL``）或该 current attempt 的通知，旧 attempt 的未清
        通知不算“当前已有待办”。给 ``result_version`` 时进一步要求 payload 未
        标版本或版本相同。供缺失责任检查使用（调用方不做外层 SQL）。
        """

    def publish_responsibility(self, rid: str, *, expected_revision: int,
                               kind: str = "continuation_ready",
                               payload: dict,
                               actor: str = "supervisor") -> dict:
        """把可发布的开放责任规范为 ``awaiting_review`` 并同事务发布通知。

        - 发布门只认结构化事实：``evidence.execution.ended_at``；或
          ``condition_key == 'missing-responsibility'`` /
          ``evidence.reason == 'no-open-responsibility'``；或
          ``evidence.continuation``（除 ``kind`` 外必须带可与责任行核对的
          input/message/session/responsibility 绑定）。纯文本/任意 dict →
          ``RESPONSIBILITY_NOT_REVIEWABLE``；
        - 通知去重键 = 责任 id + condition_key + 条件版本（generation/
          result_version），**不含每轮 now**；重复发布返回同一逻辑通知，ACK
          后不反复新建；
        - 可发布状态：handling/waiting_input/waiting_execution/
          waiting_dependency/awaiting_review；``waiting_human`` 拒绝自动发布；
        - 状态变更与 notification 同一事务；期限/预算耗尽 → ``waiting_human``
          + 告警；已被新代次/结果版本取代 → ``RESPONSIBILITY_STALE`` 且不写通知；
        - 路由取 ``target_session``，并与任务 ``coordinator_session``/项目租约
          holder 复核；无可用路由或暂停 → 转 ``waiting_human`` 并落去重告警。
        返回责任行字段 + ``published``/``notification``/``target``/``reason``。
        """


__all__ = ["ResponsibilityPort"]

"""S4 受管输入台账/闸门窄端口（C 用例依赖的 Protocol；实现由 B 提供）。

应用层 ``ManagedInputs`` 只依赖本 Protocol，不导入任何 adapter。默认实现见
``oc_orch.adapters.managed_input_ledger.ManagedInputLedger`` 与
``oc_orch.adapters.target_gate.TargetGate``，由组合根
``composition.make_managed_inputs`` 装配。完整语义与稳定错误码见
``evidence/2026-11-09-managed-input-development/B-api.md``。

所有方法的时间参数 ``now`` 为毫秒 int（界内），列表 ``limit`` 有界。台账方法
**不做任何网络/Client 调用**；外部 I/O 由用例在短事务之外完成。

方法名与参数在 S4 切片中固定（见
``openspec/changes/improve-wake-delivery-reliability/s4-offline-slice.md``）。
"""

from __future__ import annotations

from typing import Any, Protocol


class ManagedLedgerPort(Protocol):
    """受管输入回执 + 显式白名单观察的窄台账能力。"""

    def prepare_managed_input(
        self, session_id: str, *, intent: str, text: str, owner: str,
        request_id: str, project: str = "", expires_at: int | None = None,
        flush_condition: str | None = None, now: int,
    ) -> dict:
        """持久准备一次受管输入；``request_id`` 在 session 作用域幂等。

        - 返回 ``{"input": <事实结构>, "created": bool}``。
        - 同 session + 同 request_id 且正文/intent/owner/project（及 park 的
          expires_at/flush_condition）全相同 → 原行 ``created=False``（不重写）；
          任一关键字段不同 → ``LedgerError(REQUEST_ID_CONFLICT)``。
        - park 缺 owner/flush_condition/expires_at → ``LedgerError``（高层已拒绝）。
        - intent=record → ``admission_state='recorded'``（无发送阶段）；execute/
          park → ``'prepared'``。候选 message_id 在写入时持久化（任何 POST 前）。
        """

    def get_managed_input(self, input_id: str) -> dict | None:
        """单输入完整事实（含 body）；不存在返回 None。"""

    def list_managed_inputs(self, *, session_id: str | None = None,
                            limit: int = 100) -> list[dict]:
        """受管输入列表（默认**不含 body**，保留 body_hash）。"""

    def list_pending_managed_inputs_page(
        self, *, after_id: str | None = None, limit: int = 100,
    ) -> list[dict]:
        """按 input_id 升序的有界待对账页；只 source=managed_input 非 record，
        排除已 delivered/cancelled/released；不扫全局 session。"""

    def list_legacy_inputs(self, *, session_id: str | None = None,
                           limit: int = 100) -> list[dict]:
        """历史 legacy-receipt 只读列表；不猜测 delivered、不重放。"""

    def claim_managed_operation(
        self, input_id: str, *, operation: str, owner: str,
        expected_revision: int, reason: str, now: int,
    ) -> dict:
        """owner + revision CAS 一次赢家 claim（send/flush/cancel）。

        - 成功返回 ``{"input", "changed": True, "revision"}``；任何拒绝 raise
          ``LedgerError``（``INPUT_OWNER_MISMATCH`` / ``INPUT_REVISION_CONFLICT`` /
          ``INPUT_TERMINAL`` / ``INPUT_OPERATION_INFLIGHT`` / ``INPUT_ALREADY_ADMITTED`` /
          ``FLUSH_NOT_QUEUED`` / ``ALREADY_DELIVERED`` …）。
        - **未收敛操作保护**：任一 send/flush/cancel 仍处于 ``claimed``/``unknown``
          未收敛时，返回 ``{"input", "changed": False, "revision": <该 claim 的
          revision>, "reason": "operation-unresolved"}``（owner+expected_revision
          先验仍校验）。调用方**只有 changed 赢家可做外部 I/O**。
        - send：prepared/possible/unknown → possible 且 send_count+1；flush：
          possible/unknown/admitted；cancel：prepared/possible/unknown/admitted。
        - 原 message_id 恒不变。
        """

    def record_managed_observation(self, input_id: str, *, evidence: dict,
                                   now: int) -> dict:
        """记录一次外部观察（四态单调，不伪造 ACK/执行）。

        ``evidence`` 必须含 ``message_id``（等于原身份）、``operation``
        （send/flush/cancel，且对应 claim 的 ``operation_revision`` 相符）、可选的
        ``via``，以及至少一个状态指示：``admission`` / ``delivery`` /
        ``cancelled`` / ``unknown``；已 delivery 后允许仅 ``execution`` /
        ``business_ack`` 的归属证据。``cancelled`` 必须显式 ``trusted=True``，
        DELETE no-op/异常不得伪造取消。

        返回 ``{"input", "changed", "recorded", "admission_state", "conflict",
        "reason"}``。重复/冲突/终态晚到返回 ``changed=False``（不重开责任）；
        结构非法 raise。
        """

    def bind_managed_responsibility(
        self, input_id: str, *, responsibility_id: str,
        responsibility_revision: int, now: int,
        wait_condition: str | None = None, next_check_at: int | None = None,
        deadline: int | None = None, actor: str = "system",
        touch_revision: bool = False,
    ) -> dict:
        """把后续责任 ID/revision 写回输入 tracking（发送前/证据更新用）。

        只更新 metadata.tracking 与 updated_at，不改 admission_state、正文或
        操作 claim；默认不递增输入 revision。返回 ``{"input", "revision"}``。
        """

    def mark_managed_flushed(
        self, input_id: str, *, now: int, flush_operation_revision: int,
        effective_intent: str = "execute", effective_resume: bool = True,
        actor: str = "system",
    ) -> dict:
        """PATCH steer 成功后落 flushed_at/effective intent/resume（保留原 intent）。"""

    def record_managed_continuation(
        self, input_id: str, *, report: dict, source_message_id: str,
        via: str = "explicit", now: int,
    ) -> dict:
        """持久结构化 continuation；不改 admission/执行/业务 ACK。"""

    def acknowledge_managed_input(
        self, input_id: str, *, value: bool, owner: str,
        expected_revision: int, reason: str, now: int,
    ) -> dict:
        """显式业务 ACK：BEGIN IMMEDIATE 内 owner/状态/revision CAS；
        metadata.managed_input.business_ack 写 bool + revision+1 + audit。"""

    def note_managed_checked(self, input_id: str, *, now: int,
                             actor: str = "supervisor") -> dict:
        """持久 last_checked_at/observed_at；reconcile 页 60s 退避（失败同）。"""

    def register_managed_watch(self, session_id: str, *, owner: str, project: str,
                               max_pending_age_ms: int, now: int) -> dict:
        """登记/更新显式白名单目标（revision CAS，不同 owner 拒绝）。

        返回 ``{"watch", "created", "changed"}``。
        """

    def unregister_managed_watch(self, session_id: str, *, owner: str,
                                 expected_revision: int, now: int) -> dict:
        """撤销白名单（owner + revision CAS）：置 enabled=False，不删记录。"""

    def get_managed_watch(self, session_id: str) -> dict | None:
        """单目标白名单记录；未登记返回 None（含 enabled/revision）。"""

    def list_managed_watches(self, *, limit: int = 100,
                             after_session: str | None = None,
                             enabled_only: bool = False) -> list[dict]:
        """白名单记录列表（有界；支持游标分页与仅启用过滤）。

        - ``after_session``：只返回 session_id 严格大于该值的记录（游标轮转）；
        - ``enabled_only=True``：在 SQL ``LIMIT`` 之前按 enabled 过滤（旧调用默认
          False 兼容）；
        - 包含 enabled=False 的历史撤销记录（供审计），除非 enabled_only。
        """

    def record_managed_watch_observation(self, session_id: str, *,
                                         expected_revision: int,
                                         observation: dict, now: int) -> dict:
        """写入一次有界只读观察（不递增 revision，不重新启用已撤销 watch）。

        - 已撤销/缺失 → ``{"recorded": False, "reason": "watch-disabled"|"watch-not-found"}``；
        - revision 不符 → raise ``LedgerError(WATCH_REVISION_CONFLICT)``；
        - ``observation`` 应含 ``owner/session_id/trusted/pending/oldest_age_ms/
          aged_out/blocking_reason/next_action/pending_condition_key/watch_revision``；
          其中 ``pending_condition_key`` 为稳定 ID 集合摘要（不含动态 age/正文）。
          事务内 enabled+rev 通过且 ``trusted=True & aged_out=True`` 时由 B 落持久
          owner 告警（按 condition 去重）；CAS 拒绝则不得持久告警。
        """

    def expire_managed_parks(self, *, now: int, limit: int = 100) -> dict:
        """park 到期只告警：持久 expiry_alert/condition_key，不 cancel/flush/烧预算。

        返回 ``{"checked", "alerts", "marked", "expired": [input_id, ...]}``。
        """


class ManagedTargetGatePort(Protocol):
    """目标闸门窄端口：只读快照 + 人工恢复（不触发 prompt/网络）。"""

    def get(self, session_id: str) -> dict | None:
        """当前闸门快照（含 ``state``）或 None。

        记录损坏/未来 schema 时抛稳定错误，调用方必须 fail-closed（不得当成无闸门）。
        """

    def clear_manual(self, session_id: str, *,
                     reason: str = "manual-recovery") -> dict:
        """统一人工恢复入口：只清 ``source='manual'``。

        - 返回 ``{"session_id", "cleared": bool, "reason", "gate"}``；
        - provider 冷却/半开探测/耗尽态 → ``cleared=False`` 且保持原状；
        - 无活动闸门 → ``no-active-gate``；坏记录 fail-closed；
        - **绝不**触发任何 prompt 或网络。
        """


class ManagedResponsibilityPort(Protocol):
    """后续责任窄端口（实现为独立 ResponsibilityStore(base_ledger)）。

    约束：不继承 Ledger、不授予写权；``base`` 暴露只读任务/尝试/租约查询，
    供应用用例在发送前复核 current。责任状态与 Notification ACK/Task accepted
    分别记账。
    """

    base: Any

    def open_responsibility(
        self, owner: str, project: str, task_id: str | None,
        attempt_id: str | None, generation: int | None,
        result_version: str | None, input_id: str, session_id: str,
        message_id: str, target_session: str, *,
        state: str = "waiting_input", wait_kind: str,
        wait_condition: str, next_action: str, next_check_at: int,
        deadline: int | None, condition_key: str, evidence: dict,
        max_budget: int = 3, actor: str,
    ) -> dict:
        """持久登记后续责任；返回责任行（含 responsibility_id/revision）。"""

    def update_responsibility(self, responsibility_id: str, *,
                              expected_revision: int, fields: dict,
                              actor: str = "supervisor") -> dict:
        """revision CAS 更新责任（等待条件/下一动作/期限/预算/证据）。"""


__all__ = ["ManagedLedgerPort", "ManagedTargetGatePort",
           "ManagedResponsibilityPort"]

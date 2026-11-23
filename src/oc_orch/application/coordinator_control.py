"""共享对话控制用例（任务 1/2 控制底座，spec C01–C12 的存储/规则/共享用例）。

本服务是 CLI/Web/未来会话入口共用的**唯一**业务入口；父集成经组合根
``composition.make_coordinator_control(conn, clock)`` 装配
（``CoordinatorControlLedger(conn, clock)`` + 本服务）。所有方法不做网络、
不解析自然语言、不扫描聊天关键词——把真实用户意图转换为结构化调用是上层
session 的职责（C03/C04/D1）。

公开 API 与返回形状（父集成 / 各入口接线以此为准）：

- ``get_project(project)`` -> 项目控制快照 dict；**无记录返回清晰默认**
  （``status="running", revision=0, recorded=False``）。字段：
  ``project/status/revision/recorded/schema/session_id/operation_id/
  last_operation/reason/evidence/updated_at/paused/pause/holder_session/epoch``。

- ``get_session(session_id)`` -> session 回合快照 dict；无记录返回
  ``revision=0, recorded=False``。字段：``session_id/revision/recorded/schema/
  hold/barrier/turn(barrier 别名)/hold_origin_at/barrier_origin_at/
  barrier_message_id/projects/updated_at``。

- ``pause/proj_resume``（``pause(project, *, session_id, operation_id, reason,
  expected_revision, expected_epoch=None, evidence=None)``，resume 同参数）->：
  ``{"executed": bool, "refused": str|None, "refused_detail": dict|None,
  "applied": bool, "reason": 稳定代码, "project": str, "session_id": str,
  "operation_id": str, "revision": int, "epoch": int|None,
  "snapshot": <project 快照>}``。
  成功（含幂等重复）``executed=True``；拒绝``executed=False, refused=<代码>``；
  任何结果都携带**真实当前快照**，绝不口头宣称已暂停/已恢复。

- ``hold_session(session_id, *, event_id, origin_at, reason)`` /
  ``begin_user_turn(session_id, *, message_id, origin_at, operation_id,
  trusted=False)`` / ``finish_user_turn(session_id, *, message_id, idle_id,
  idle_at, expected_revision)`` -> ``{"executed", "refused",
  "refused_detail", "applied", "reason", "session_id", "revision",
  "snapshot": <session 快照>}``。

- ``permits(project, session_id)`` -> ``{"permits": bool, "blocking": [str,...],
  "project", "session_id", "project_snapshot", "session_snapshot"}``。阻塞原因：
  ``project-paused`` / ``user-turn-active`` / ``turn-held``。只是**必要**
  条件，不授予发送/派单权利；provider 门与任务/写权/路由守卫由 runtime 组合。

- ``coordinator_lease(project)`` -> 租约快照（holder_session/epoch）或 None。

稳定拒绝代码（``refused``/``reason`` 同一取值）：``invalid-*``（参数）、
``lease-missing``/``lease-holder-mismatch``/``epoch-mismatch``（租约）、
``revision-changed``（CAS：旧恢复/旧输入被新状态拒绝）、``already-paused``/
``not-paused``、``operation-conflict``（同 operation_id 参数变更冒认重复）、
``already-applied``（幂等重复）、``stale-event``/``stale-termination``/
``message-mismatch``/``no-active-turn``/``interrupted-turn``/``untrusted``。
持久记录损坏时抛 :class:`CoordinatorControlStateError`（fail-closed：状态未知，
不得视作无控制放行）。
"""

from __future__ import annotations

from typing import Any, Protocol

from ..domain.coordinator_control import CoordinatorControlStateError

__all__ = ["CoordinatorControlService", "CoordinatorControlStateError",
           "CoordinatorControlPort"]


class CoordinatorControlPort(Protocol):
    """持久化端口的窄契约（由 composition 装配的 CoordinatorControlLedger 提供）。

    application 只依赖本 Protocol 与 domain 纯规则，不 import adapters 具体类；
    具体 adapter 由父 composition ``make_coordinator_control(conn, clock)``
    创建后经构造注入。方法返回形状见下（与 adapter 保持一致）。
    """

    def get_project(self, project: str) -> dict: ...

    def get_session(self, session_id: str) -> dict: ...

    def coordinator_lease(self, project: str) -> dict | None: ...

    def project_operation(self, operation_id: str, project: str, session_id: str) -> dict | None: ...

    def pause(
        self,
        project: str,
        *,
        session_id: str,
        operation_id: str,
        reason: str,
        expected_revision: int,
        expected_epoch: int | None = None,
        evidence: dict | None = None,
    ) -> dict: ...

    def resume(
        self,
        project: str,
        *,
        session_id: str,
        operation_id: str,
        reason: str,
        expected_revision: int,
        expected_epoch: int | None = None,
        evidence: dict | None = None,
    ) -> dict: ...

    def hold_session(self, session_id: str, *, event_id: str, origin_at: int,
                     reason: str) -> dict: ...

    def begin_user_turn(self, session_id: str, *, message_id: str,
                        origin_at: int, operation_id: str,
                        trusted: bool = False) -> dict: ...

    def finish_user_turn(self, session_id: str, *, message_id: str,
                         idle_id: str, idle_at: int,
                         expected_revision: int) -> dict: ...

    def release_hold(self, session_id: str, *, operation_id: str, reason: str,
                     expected_revision: int, evidence: dict | None = None,
                     actor: str = "operator") -> dict: ...

    def register_completed_turn(self, session_id: str, *, message_id: str,
                                origin_at: int, operation_id: str,
                                idle_id: str, idle_at: int,
                                expected_revision: int, reason: str,
                                evidence: dict | None = None,
                                actor: str = "operator") -> dict: ...

    def permits(self, project: str, session_id: str) -> dict: ...


class CoordinatorControlService:
    """共享对话控制用例；薄编排层封装 CoordinatorControlLedger 并固定 API。

    - 本项目暂停、session 回合 hold、用户回合屏障与 provider 条件相互独立，
      任何恢复只改自己维度（resume 不清 hold/屏障/provider 门）；
    - ``begin_user_turn`` 只有 ``trusted=True`` 才建立屏障（fail-closed），
      自动 wake / 未登记 REST 消息 / 任意 metadata 绝不自动视为真人；
    - ``finish_user_turn`` 需要精确原输入（message_id）与唯一终态证据
      （idle_id/idle_at）+ expected_revision（CAS）；旧 idle 不能清更新屏障；
    - 停止调谐不停止 worker、不改 Task 状态 / ACK / 验收。
    """

    def __init__(self, ledger: CoordinatorControlPort, clock: Any):
        self._ledger = ledger
        self._clock = clock

    # ---- 只读 --------------------------------------------------------
    def get_project(self, project: str) -> dict:
        """项目控制快照；无记录返回 running / revision=0 的清晰默认。"""
        return self._ledger.get_project(project)

    def get_session(self, session_id: str) -> dict:
        """session 回合/hold 快照；无记录返回 revision=0 的清晰默认。"""
        return self._ledger.get_session(session_id)

    def coordinator_lease(self, project: str) -> dict | None:
        """当前项目租约（holder_session/epoch）只读快照。"""
        return self._ledger.coordinator_lease(project)

    def project_operation(self, operation_id: str, project: str, session_id: str) -> dict | None:
        """读取原操作回执，不执行/重投控制请求。"""
        return self._ledger.project_operation(operation_id, project, session_id)

    def permits(self, project: str, session_id: str) -> dict:
        """组合判定自动动作的必要条件（只读，不授予权利）。"""
        return self._ledger.permits(project, session_id)

    # ---- 项目意图：暂停/继续 -----------------------------------------
    def pause(self, project: str, *, session_id: str, operation_id: str,
              reason: str, expected_revision: int,
              expected_epoch: int | None = None,
              evidence: dict | None = None) -> dict:
        """显式暂停项目自动调谐：只阻新自动派单/唤醒，不中断在途 worker。

        原子 CAS（expected_revision/expected_epoch）+ 租约 holder 复核 + 审计；
        重复 operation 幂等；参数变更不得冒认重复。结果携带真实快照。
        """
        return self._ledger.pause(
            project, session_id=session_id, operation_id=operation_id,
            reason=reason, expected_revision=expected_revision,
            expected_epoch=expected_epoch, evidence=evidence)

    def resume(self, project: str, *, session_id: str, operation_id: str,
               reason: str, expected_revision: int,
               expected_epoch: int | None = None,
               evidence: dict | None = None) -> dict:
        """显式继续项目自动调谐：幂等；只改项目维度，不清 hold/屏障/provider 门。

        恢复只恢复自动资格，不替代验收，不绕过任务级暂停/取消。
        """
        return self._ledger.resume(
            project, session_id=session_id, operation_id=operation_id,
            reason=reason, expected_revision=expected_revision,
            expected_epoch=expected_epoch, evidence=evidence)

    # ---- session 回合 hold / 用户屏障 ---------------------------------
    def hold_session(self, session_id: str, *, event_id: str, origin_at: int,
                     reason: str) -> dict:
        """回合级 hold：停止当前回答；不取消项目/worker，自动 wake 不自清。

        迟到旧事件（origin_at 早于当前 hold 或当前 barrier）拒绝
        （``stale-event``）；同事件重复幂等。
        """
        return self._ledger.hold_session(
            session_id, event_id=event_id, origin_at=origin_at, reason=reason)

    def begin_user_turn(self, session_id: str, *, message_id: str,
                        origin_at: int, operation_id: str,
                        trusted: bool = False) -> dict:
        """建立/更新用户回合屏障：可信新输入优先，暂缓自动待办注入。

        ``trusted`` 只能由调用方（可证明的显式本地/已核实来源）判定，绝不
        来自 request metadata；默认 False（fail-closed）。屏障跟随最新有效
        输入（C11），不清 hold、不解除项目暂停。
        """
        return self._ledger.begin_user_turn(
            session_id, message_id=message_id, origin_at=origin_at,
            operation_id=operation_id, trusted=trusted)

    def finish_user_turn(self, session_id: str, *, message_id: str,
                         idle_id: str, idle_at: int,
                         expected_revision: int) -> dict:
        """以精确原输入 + 唯一终态证据关闭当前用户回合（CAS 硬约束）。

        - ``message_id`` 必须精确关联当前 barrier 的原输入；
        - ``idle_id/idle_at`` 为可归属结束证据；空 inbox / session.updated
          不是完成依据（fail-closed）；
        - ``expected_revision`` 必须提供；更新输入/新中断后再结束由 CAS
          拒绝（``revision-changed``/``interrupted-turn``）。

        未被中断的新回合（hold 早于 barrier）结束后同时解除对应 hold（C10）；
        旧 idle 不能清更新屏障（C11/C12）。
        """
        return self._ledger.finish_user_turn(
            session_id, message_id=message_id, idle_id=idle_id,
            idle_at=idle_at, expected_revision=expected_revision)

    def release_hold(self, session_id: str, *, operation_id: str, reason: str,
                     expected_revision: int, evidence: dict | None = None,
                     actor: str = "operator") -> dict:
        """显式运维解除回合 hold：只清可解除的 `coordinator-interrupted`。

        CAS + barrier 守卫：有用户回合屏障时拒绝；无 hold 幂等成功；不清
        项目暂停/provider 条件、不 ACK 业务通知、不代替验收。
        """
        return self._ledger.release_hold(
            session_id, operation_id=operation_id, reason=reason,
            expected_revision=expected_revision, evidence=evidence,
            actor=actor)

    def register_completed_turn(self, session_id: str, *, message_id: str,
                                origin_at: int, operation_id: str,
                                idle_id: str, idle_at: int,
                                expected_revision: int, reason: str,
                                evidence: dict | None = None,
                                actor: str = "operator") -> dict:
        """登记已完成的真实用户回合并原子结束（begin+finish 同事务）。

        调用方必须先核验消息窗口（归属、唯一用户输入/唯一 idle、无新输入/
        新中断）；本层只做控制 CAS/顺序与维度守卫。成功时按 C10 解除早于该
        回合的 hold，不清项目暂停/provider。
        """
        return self._ledger.register_completed_turn(
            session_id, message_id=message_id, origin_at=origin_at,
            operation_id=operation_id, idle_id=idle_id, idle_at=idle_at,
            expected_revision=expected_revision, reason=reason,
            evidence=evidence, actor=actor)

"""对话控制的可信显式入口边界（任务 0.2 / 1.1，Spec C01–C04）。

背景事实（2026-11-14 只读核验，见 design.md D1 与
docs/agent-sessions/20261114-probe-unknown-recovery.md）：
OpenCode v2.2.5 的 ``Session.Message.User`` / inbox metadata 由请求方提供，
**没有可信 human-origin 字段**；idle 只有 id/time/outcome，不携带原 user ID。
因此「inp 是 None 或非 automatic_wake 就清门」的旧思路不能重新包装成可靠
的真人检测。本模块**不**实现扫聊天关键词或引用文本可触发的控制器，也**不**
新增任何网络请求或编造上游字段；native 对话 hook 至今仍未接通。

关于来源认证（ATESTATION 语义，勿夸大）：
- ``ATTESTATION_OPERATOR = "operator-cli"`` 是保留兼容的**本地 operator 调用层**标签：
  CLI与受loopback/Origin/只读模式/明确对象确认保护的显式Web入口可使用；**不是**
  把某个请求字段与字符串比较一次就完成了来源认证。
- 任何来自 HTTP/metadata 的值（``source=user``、``role=user``、inbox metadata
  等）都**永不**被接成该标签；Web只是一条显式本地运维通道，不证明真人来源。
  普通HTTP/native自动登记要取得可信用户回合权限，仍须独立且经核验的来源路径。structured-session 来源
  在 native hook 未经契约核验前不启用，不宣称可用。

信任边界（本模块强制，调用 service 之前完成）：
- 触发控制操作的只能是「显式本地 operator 动作」（CLI/受控loopback Web）；
  缺显式 project/session、原指令/operationID、原因、lease epoch/
  revision 或证据即拒，不猜最新任务、不自动接管。
- 幂等与 CAS 是硬约束：``expected_revision`` 缺失不调用 service；控制版本/
  租约代次变化由 service 同事务拒绝；本边界如实传递「未执行」，绝不口头成功。
- ``finish_user_turn`` 是**本地 operator 断言入口**：它只登记调用方提供的
  原输入关联 + 结束证据（idle_id/idle_at）+ CAS，**本身不查询/核验上游真实
  执行归属**。调用前置：调用方（或父接线的共享 runtime / REST 对账）必须先
  核验执行区间、无更新输入、无新中断等完整事实；本接口只在这些断言与 CAS
  齐备时原子登记，缺任一即拒（fail-closed）。不得把本接口运行结果宣称为
  「已验证真实执行」。

调用关系：CLI/未来入口经父 composition ``make_coordinator_control(conn, clock)``
获得 ``CoordinatorControlPort``（worker A 的 CoordinatorControlService），
再用本模块把显式结构化意图转换为 service 调用。返回形状为本模块文档化契约；
字段名如有出入由父集成时对账，不在本模块猜测。
"""

from __future__ import annotations

from typing import Any, Protocol

from ..domain.errors import ActionError

#: 唯一当前可信来源标签：**可信本地调用层**的固定标识（见模块 docstring，
#: 它不是独立鉴权；请求/metadata 永不被接成该值）。
ATTESTATION_OPERATOR = "operator-cli"
_ATTESTATIONS = frozenset({ATTESTATION_OPERATOR})


class CoordinatorControlPort(Protocol):
    """worker A 的 CoordinatorControlService 窄契约（composition 装配）。

    返回数据约定（本模块据此预检与如实传递；字段名对账由父集成负责）：

    - ``get_project(project)`` -> ``None | {"project", "holder_session",
      "epoch", "revision", "paused": bool, "pause": dict | None}``；
    - ``get_session(session_id)`` -> ``None | {"session_id",
      "projects": [str, ...], "hold": dict | None, "turn": dict | None}``；
    - ``pause/resume`` -> ``{"executed": bool, "refused": str | None, ...}``；
      成功（含幂等重复）必须 ``executed=True``；版本/持有者/代次不匹配等拒绝
      返回 ``executed=False`` 与稳定 ``refused`` 原因，**不得抛业务异常当成功**；
    - ``hold_session/begin_user_turn/finish_user_turn`` -> 同 executed 语义；
    - ``permits(project, session_id)`` -> ``{"permits": bool,
      "blocking": [str, ...]}``。
    """

    def get_project(self, project: str) -> dict | None: ...

    def pause(
        self,
        project: str,
        *,
        session_id: str,
        operation_id: str,
        reason: str,
        expected_revision: int,
        expected_epoch: int | None = None,
        evidence: list[str] | None = None,
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
        evidence: list[str] | None = None,
    ) -> dict: ...

    def get_session(self, session_id: str) -> dict | None: ...

    def hold_session(
        self,
        session_id: str,
        *,
        event_id: str,
        origin_at: int,
        reason: str,
    ) -> dict: ...

    def begin_user_turn(
        self,
        session_id: str,
        *,
        message_id: str,
        origin_at: int,
        operation_id: str,
        trusted: bool = False,
    ) -> dict: ...

    def finish_user_turn(
        self,
        session_id: str,
        *,
        message_id: str,
        idle_id: str,
        idle_at: int,
        expected_revision: int,
    ) -> dict: ...

    def release_hold(
        self,
        session_id: str,
        *,
        operation_id: str,
        reason: str,
        expected_revision: int,
        evidence: dict | None = None,
        actor: str = "operator",
    ) -> dict: ...

    def register_completed_turn(
        self,
        session_id: str,
        *,
        message_id: str,
        origin_at: int,
        operation_id: str,
        idle_id: str,
        idle_at: int,
        expected_revision: int,
        reason: str,
        evidence: dict | None = None,
        actor: str = "operator",
    ) -> dict: ...

    def permits(self, project: str, session_id: str) -> dict: ...


def _require(value: Any, label: str, *, kind: type = str,
             minimum: int | None = None) -> Any:
    """显式参数校验，全部 fail-closed：

    - ``kind=str``：只放行真实非空字符串（int/list/None/bool 一律拒绝）；
    - ``kind=int``：拒绝 bool 与非 int；``minimum`` 非空时同时拒绝过小值
      （epoch >= 1，revision/时间戳 >= 0）。
    """
    if value is None:
        raise ActionError("usage", f"{label} 缺失")
    if kind is int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ActionError("usage", f"{label} 必须为整数")
        if minimum is not None and value < minimum:
            raise ActionError(
                "usage", f"{label} 非法：不得小于 {minimum}",
                {"label": label, "value": value, "minimum": minimum},
            )
        return value
    if not isinstance(value, str):
        raise ActionError("usage", f"{label} 必须为非空字符串",
                          {"label": label, "actual": type(value).__name__})
    if not value.strip():
        raise ActionError("usage", f"{label} 不能为空字符串")
    return value


def _require_bool(value: Any, label: str) -> bool:
    """严格布尔校验：'false'/1/None 等一律拒绝，不做隐式 truthy 转换。"""
    if not isinstance(value, bool):
        raise ActionError("usage", f"{label} 必须为布尔值",
                          {"label": label, "actual": type(value).__name__})
    return value


class ConversationControl:
    """显式对话控制的共享入口用例：意图 → CoordinatorControlPort 操作。

    CLI/运维与（未来）结构化会话操作共用本类；本类在**调用 service 之前**
    强制进入字段与歧义守卫，并在 service 拒绝时如实报告「未执行」。

    - ``attestation`` 只接受固定的本地调用层标签（当前仅 ``operator-cli``）；
      调用方不得从 metadata 或其他任意字符串传来源（见模块 docstring）。
    - ``pause/resume`` 必须绑定 ``expected_revision``（CAS 硬约束，>=0）与
      ``operation_id``（原指令引用）；``expected_epoch`` 可选但 >=1。
    - 未显式提供 project 时只允许 session 唯一归属项目；多项目歧义或无可归属
      项目直接拒绝，不猜测。
    - ``begin_user_turn`` 的 ``trusted`` 必须为严格布尔；``trusted=False``
      **明确未执行**（无可信来源不建立屏障）。
    - ``finish_user_turn`` 是本地 operator 断言入口（见模块 docstring）：必须
      提供可归属结束证据（idle_id/idle_at）与 expected_revision；“空 inbox /
      session.updated”不是完成依据，本接口不提供该捷径，也不算真实执行归属。
    """

    def __init__(self, control: CoordinatorControlPort):
        self._control = control

    # ---- 内部守卫 -------------------------------------------------
    def _attestation(self, value: str) -> None:
        # 注意：这是「可信本地调用层」的固定标签，不是独立鉴权——不把任意
        # 请求字段/字面值与字符串比较一次就当作来源认证。HTTP/metadata 提供
        # 的值永不接成该标签；native 对话 hook 仍未接通。
        if value not in _ATTESTATIONS:
            raise ActionError(
                "usage",
                "不受信任的来源：控制操作只能由显式本地 operator 动作（固定"
                "标签）触发；metadata 声称的 user、未登记 REST 输入、外部引用"
                "或自动 wake 不能取得控制权，也不能靠改请求来源字段取得",
                {"attestation": value, "allowed": sorted(_ATTESTATIONS)},
            )

    def _resolve_project(self, session_id: str, project: str | None) -> str:
        if project:
            return project
        snap = self._control.get_session(session_id) or {}
        projects = [p for p in (snap.get("projects") or []) if p not in ("", None)]
        unique = sorted(set(projects))
        if not unique:
            raise ActionError(
                "usage",
                f"session {session_id} 没有可归属项目；必须显式 --project",
                {"session_id": session_id},
            )
        if len(unique) > 1:
            raise ActionError(
                "usage",
                f"session {session_id} 关联多个项目（{', '.join(unique)}）；"
                "目标不明确，必须显式 --project，不得猜测",
                {"session_id": session_id, "projects": unique},
            )
        return unique[0]

    def _check_holder(self, project: str, session_id: str) -> None:
        snap = self._control.get_project(project)
        if not snap:
            return  # 尚无控制记录：由 service 负责创建/CAS；此处不猜 holder
        holder = snap.get("holder_session")
        if holder and holder != session_id:
            raise ActionError(
                "usage",
                f"{session_id} 不是项目 {project} 的当前协调者；拒绝控制操作",
                {"project": project, "holder_session": holder,
                 "session_id": session_id},
            )

    def _outcome(self, action: str, result: Any, project: str | None,
                 session_id: str | None) -> dict:
        if not isinstance(result, dict):
            raise ActionError(
                "control_refused",
                f"{action} 返回不可解释结果，未确认执行；控制状态保持不变",
                {"action": action, "project": project,
                 "session_id": session_id, "result": result},
            )
        if result.get("executed") is True:
            return {
                "ok": True, "executed": True, "action": action,
                "project": project, "session_id": session_id,
                "refused": None, "refused_detail": None, "result": result,
            }
        refused = result.get("refused") or "executed=False"
        raise ActionError(
            "control_refused",
            f"{action} 未执行：{refused}",
            {"action": action, "project": project, "session_id": session_id,
             "refused": refused, "refused_detail": result.get("refused_detail"),
             "result": result},
        )

    # ---- 项目意图：暂停/继续 ---------------------------------------
    def _project_mutation(self, action: str, *, project, session_id,
                          operation_id, reason, expected_revision,
                          expected_epoch, evidence, attestation) -> dict:
        self._attestation(attestation)
        # project 允许缺省：由 session 唯一归属解析；显式提供必须是真实非空
        # 字符串（int/list 等不静默转字面）。纯空白视同未提供（fail-closed）。
        if project is not None:
            if not isinstance(project, str):
                raise ActionError(
                    "usage", "project 必须为非空字符串",
                    {"label": "project", "actual": type(project).__name__})
            project = project.strip() or None
        session_id = _require(session_id, "session")
        operation_id = _require(operation_id, "operation-id（原指令引用）")
        reason = _require(reason, "reason")
        expected_revision = _require(expected_revision, "expected-revision",
                                     kind=int, minimum=0)
        if expected_epoch is not None:
            expected_epoch = _require(expected_epoch, "expected-epoch",
                                      kind=int, minimum=1)
        # 对齐 worker A 真实 API：evidence 为 dict（JSON 对象）或 None，由
        # service 规范形化并持久（canonic_value）。非 dict（list/str/None 之外）
        # 一律 fail-closed。
        if evidence is not None and not isinstance(evidence, dict):
            raise ActionError(
                "usage", "evidence 必须为 JSON 对象或省略",
                {"label": "evidence", "actual": type(evidence).__name__})
        project = self._resolve_project(session_id, project)
        self._check_holder(project, session_id)
        method = self._control.pause if action == "pause" else self._control.resume
        result = method(
            project, session_id=session_id, operation_id=operation_id,
            reason=reason, expected_revision=expected_revision,
            expected_epoch=expected_epoch,
            evidence=evidence,
        )
        return self._outcome(action, result, project, session_id)

    def pause(self, *, project, session_id, operation_id, reason,
              expected_revision, expected_epoch=None, evidence=None,
              attestation=ATTESTATION_OPERATOR) -> dict:
        """显式暂停项目自动调谐：只阻新自动派单/唤醒，不中断在途 worker。"""
        return self._project_mutation(
            "pause", project=project, session_id=session_id,
            operation_id=operation_id, reason=reason,
            expected_revision=expected_revision, expected_epoch=expected_epoch,
            evidence=evidence, attestation=attestation)

    def resume(self, *, project, session_id, operation_id, reason,
               expected_revision, expected_epoch=None, evidence=None,
               attestation=ATTESTATION_OPERATOR) -> dict:
        """显式继续项目自动调谐：幂等；不清 provider/任务级控制，不伪造验收。"""
        return self._project_mutation(
            "resume", project=project, session_id=session_id,
            operation_id=operation_id, reason=reason,
            expected_revision=expected_revision, expected_epoch=expected_epoch,
            evidence=evidence, attestation=attestation)

    # ---- 回合级 hold / 用户回合 -------------------------------------
    def hold(self, *, session_id, event_id, origin_at, reason,
             attestation=ATTESTATION_OPERATOR) -> dict:
        """回合级 hold：停止当前回答；不取消项目/worker，自动 wake 不自清。"""
        self._attestation(attestation)
        session_id = _require(session_id, "session")
        event_id = _require(event_id, "event-id")
        origin_at = _require(origin_at, "origin-at", kind=int, minimum=0)
        reason = _require(reason, "reason")
        result = self._control.hold_session(
            session_id, event_id=event_id, origin_at=origin_at, reason=reason)
        return self._outcome("hold", result, None, session_id)

    def begin_user_turn(self, *, session_id, message_id, origin_at,
                        operation_id, trusted=False,
                        attestation=ATTESTATION_OPERATOR) -> dict:
        """建立用户回合屏障（仅当调用方显式判定可信输入）。

        - ``trusted`` 必须为严格布尔；``'false'``/``1``/``None`` 等一律拒绝；
        - ``trusted=False`` 时**明确未执行**：没有可信来源判定就不建立屏障，
          “观察到一条消息”不能当作真人回合（fail-closed）；
        - 本入口不查询上游来源字段、不新增网络请求。
        """
        self._attestation(attestation)
        session_id = _require(session_id, "session")
        message_id = _require(message_id, "message-id")
        origin_at = _require(origin_at, "origin-at", kind=int, minimum=0)
        operation_id = _require(operation_id, "operation-id（原指令引用）")
        trusted = _require_bool(trusted, "trusted")
        if not trusted:
            raise ActionError(
                "control_refused",
                "begin_user_turn 未执行：trusted=False 不能建立可信用户回合"
                "屏障；缺少可信来源判定（native 对话 hook 未接通）",
                {"session_id": session_id, "message_id": message_id},
            )
        result = self._control.begin_user_turn(
            session_id, message_id=message_id, origin_at=origin_at,
            operation_id=operation_id, trusted=True)
        return self._outcome("begin_user_turn", result, None, session_id)

    def finish_user_turn(self, *, session_id, message_id, idle_id, idle_at,
                         expected_revision,
                         attestation=ATTESTATION_OPERATOR) -> dict:
        """以可信结束证据关闭当前用户回合（本地 operator 断言入口；CAS）。

        **调用前置（本接口不替调用方核验，由调用方/父接线 runtime 负责）：**
        真实执行归属须由调用方（或共享 runtime 的 REST 对账）先核验——idle
        属于原输入执行区间、无更新用户输入、无新中断。本接口只原子登记：
        ``message_id`` 精确关联原输入 + ``idle_id/idle_at`` 可归属结束证据 +
        ``expected_revision`` 控制版本。缺任一即拒；“空 inbox/session.updated”
        不是完成依据。本接口**不查询/核验上游真实执行、不新增网络请求**，
        其执行结果不单独构成「已验证真实执行归属」的证明。
        """
        self._attestation(attestation)
        session_id = _require(session_id, "session")
        message_id = _require(message_id, "message-id（原输入）")
        idle_id = _require(idle_id, "idle-id（结束证据）")
        idle_at = _require(idle_at, "idle-at", kind=int, minimum=0)
        expected_revision = _require(expected_revision, "expected-revision",
                                     kind=int, minimum=0)
        result = self._control.finish_user_turn(
            session_id, message_id=message_id, idle_id=idle_id,
            idle_at=idle_at, expected_revision=expected_revision)
        return self._outcome("finish_user_turn", result, None, session_id)

    def release_hold(self, *, session_id, operation_id, reason,
                     expected_revision, evidence=None,
                     attestation=ATTESTATION_OPERATOR) -> dict:
        """显式运维解除回合 hold（只清 `coordinator-interrupted`）。

        需要原 operation、原因与 CAS revision；有用户回合屏障时拒绝；不解除
        项目暂停/provider 条件，不 ACK 业务通知，也不代替验收。
        """
        self._attestation(attestation)
        session_id = _require(session_id, "session")
        operation_id = _require(operation_id, "operation-id（原指令引用）")
        reason = _require(reason, "reason")
        expected_revision = _require(expected_revision, "expected-revision",
                                     kind=int, minimum=0)
        if evidence is not None and not isinstance(evidence, dict):
            raise ActionError("usage", "evidence 必须为 JSON 对象或省略")
        result = self._control.release_hold(
            session_id, operation_id=operation_id, reason=reason,
            expected_revision=expected_revision, evidence=evidence,
            actor="operator")
        return self._outcome("release_hold", result, None, session_id)

    def register_completed_turn(self, *, session_id, message_id, origin_at,
                                operation_id, idle_id, idle_at,
                                expected_revision, reason, evidence=None,
                                attestation=ATTESTATION_OPERATOR) -> dict:
        """登记已完成的真实用户回合并原子结束（begin+finish）。

        **调用前置**：调用方必须先从 OpenCode 会话窗口核验 message_id 是真实
        用户输入、created=origin_at、唯一 idle=idle_at、无更新输入/新中断，
        且不是 automatic_wake。本接口只做控制层 CAS/顺序校验；不接受
        “观察到一条消息”当作真人授权（C04/C12）。
        """
        self._attestation(attestation)
        session_id = _require(session_id, "session")
        message_id = _require(message_id, "message-id（原输入）")
        origin_at = _require(origin_at, "origin-at", kind=int, minimum=0)
        operation_id = _require(operation_id, "operation-id（原指令引用）")
        idle_id = _require(idle_id, "idle-id（结束证据）")
        idle_at = _require(idle_at, "idle-at", kind=int, minimum=0)
        expected_revision = _require(expected_revision, "expected-revision",
                                     kind=int, minimum=0)
        reason = _require(reason, "reason")
        if evidence is not None and not isinstance(evidence, dict):
            raise ActionError("usage", "evidence 必须为 JSON 对象或省略")
        result = self._control.register_completed_turn(
            session_id, message_id=message_id, origin_at=origin_at,
            operation_id=operation_id, idle_id=idle_id, idle_at=idle_at,
            expected_revision=expected_revision, reason=reason,
            evidence=evidence, actor="operator")
        return self._outcome("register_completed_turn", result, None, session_id)

    # ---- 只读状态 ---------------------------------------------------
    def status(self, *, project: str | None = None,
               session_id: str | None = None) -> dict:
        """只读：项目意图 / 会话 hold 与回合 / 自动资格。不触发任何写入。"""
        project_view = self._control.get_project(project) if project else None
        session_view = (self._control.get_session(session_id)
                        if session_id else None)
        permits_view = None
        if project and session_id:
            permits_view = self._control.permits(project, session_id) or None
        return {
            "ok": True, "project": project_view, "session": session_view,
            "permits": permits_view,
        }

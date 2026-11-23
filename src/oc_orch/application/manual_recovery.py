"""CLI/Web 共用的显式解锁用例。

缺省只接受人工中断产生的 ``source=manual`` 闸门；provider 冷却、半开探测和
耗尽态由 :meth:`TargetGatePort.clear_manual` 拒绝，避免把恢复入口变成默认的
额度绕过入口。2026-11-19 用户授权新增 ``allow_provider=True``：operator 可对
provider 闸门显式“恢复调度”（经 :meth:`TargetGatePort.clear_provider` 复位为
probe-ready，保留错误证据守卫；新错误仍重新冷却，非持久绕过）。
"""

from __future__ import annotations

import json
from typing import Any, Callable, Protocol

from ..domain.errors import ActionError
from .target_gate_port import TargetGatePort


class TurnControlPort(Protocol):
    """回合 hold 的显式解除/完成登记控制端口（实现见 CoordinatorControlService）。"""

    def release_hold(self, session_id: str, *, operation_id: str, reason: str,
                     expected_revision: int, evidence: dict | None = None,
                     actor: str = "operator") -> dict: ...

    def register_completed_turn(self, session_id: str, *, message_id: str,
                                origin_at: int, operation_id: str,
                                idle_id: str, idle_at: int,
                                expected_revision: int, reason: str,
                                evidence: dict | None = None,
                                actor: str = "operator") -> dict: ...


class ManualRecoveryLedger(Protocol):
    """解锁编排所需的台账能力；实现见 SQLite Ledger。"""

    def coordinator_lease(self, project: str) -> dict | None: ...

    def audit(self, actor: str, action: str, entity: str, entity_id: str,
              detail: Any = None) -> None: ...

    def reset_coordinator_failure(self, project: str, *, reason: str) -> None: ...

    def resurface_pending_notifications(self, session_id: str, *,
                                        next_attempt_at: int, reason: str,
                                        actor: str = "operator") -> int: ...

    def list_alerts(self, *, state: str | None = "new",
                    limit: int = 100) -> list[dict]: ...

    def ack_alert(self, alert_id: str, *, actor: str) -> dict: ...


def unlock_manual_gate(
    ledger: ManualRecoveryLedger,
    gate: TargetGatePort,
    *,
    project: str,
    session: str | None = None,
    reason: str = "manual-recovery",
    actor: str = "operator",
    now: int,
    allow_provider: bool = False,
) -> dict:
    """清除项目的闸门并恢复待办可见性（CLI/Web 共用用例）。

    2026-11-19 用户授权：``allow_provider=True`` 时允许对 provider 闸门执行
    “恢复调度”（经由 :meth:`TargetGatePort.clear_provider` 复位为 probe-ready）；
    缺省（False）保持原契约只清 ``source=manual``，provider 冷却仍拒绝，
    向后兼容既有 CLI/JSON 调用方。

    - 校验 session 必须是项目租约当前 holder；
    - manual → ``clear_manual``；provider(allow_provider) → ``clear_provider``；
    - 写审计、重置 coordinator failure/recovery_blocked、提前 pending 通知；
    - 按闸门来源 ACK 对应告警（manual/exhausted/probe-unknown/foreign_input_blocked）。
    """
    lease = ledger.coordinator_lease(project)
    if not lease:
        raise ActionError("usage", f"项目 {project} 没有 coordinator 租约")
    target = session or lease["holder_session"]
    if target != lease["holder_session"]:
        raise ActionError(
            "usage",
            f"{target} 不是项目 {project} 的当前协调者",
            {"holder_session": lease["holder_session"]},
        )

    # 按来源分派：provider 清门是授权 operator 动作，走独立审计与重置；无法
    # 分派（如 allow_provider 但闸门实际为 manual）回退到 manual 路径。
    via_provider = False
    result: dict | None = None
    if allow_provider:
        result = gate.clear_provider(target, reason=reason)
        if result.get("cleared"):
            via_provider = True
        elif result.get("reason") == "not-provider":
            # 实际是 manual 闸门：回退原路径，保持两类闸门互不干扰。
            result = gate.clear_manual(target, reason=reason)
    else:
        result = gate.clear_manual(target, reason=reason)
    if not result or not result.get("cleared"):
        refusal = (result or {}).get("reason")
        if refusal == "no-active-gate":
            return {
                "ok": True, "cleared": False, "project": project,
                "session": target, "reason": "no-active-gate",
            }
        raise ActionError("gate", f"无法解锁：{refusal}", result)

    audit_action = ("target-gate.clear-provider" if via_provider
                    else "target-gate.clear-manual")
    ledger.audit(
        actor, audit_action, "session", target,
        {"project": project, "reason": reason,
         "gate_reason": result.get("note") or result.get("reason"),
         "allow_provider": allow_provider},
    )
    ledger.reset_coordinator_failure(project, reason="manual-unlock")
    resurfaced = ledger.resurface_pending_notifications(
        target, next_attempt_at=now,
        reason="provider-recovery" if via_provider else "manual-unlock",
        actor=actor)
    acked = []
    ack_keys = {f"wake-gate:manual:{target}"}
    if via_provider:
        ack_keys = {f"wake-gate:exhausted:{target}",
                    f"wake-gate:probe-unknown:{target}",
                    f"wake-block:foreign_input_blocked:{target}"}
    for alert in ledger.list_alerts(state="new", limit=500):
        if alert.get("dedupe_key") in ack_keys:
            ledger.ack_alert(alert["id"], actor=actor)
            acked.append(alert["id"])
    return {
        "ok": True, "cleared": True, "project": project, "session": target,
        "reason": result.get("reason", "provider-cleared" if via_provider
                             else "manual-cleared"),
        "cleared_via": "provider" if via_provider else "manual",
        "resurfaced": resurfaced, "acked_alerts": acked,
    }


def _lease_target(ledger: ManualRecoveryLedger, project: str,
                  session: str | None, expected_epoch: int | None) -> str:
    lease = ledger.coordinator_lease(project)
    if not lease:
        raise ActionError("usage", f"项目 {project} 没有 coordinator 租约")
    target = session or lease["holder_session"]
    if target != lease["holder_session"]:
        raise ActionError(
            "usage", f"{target} 不是项目 {project} 的当前协调者",
            {"holder_session": lease["holder_session"]})
    if expected_epoch is not None and expected_epoch != lease.get("epoch"):
        raise ActionError(
            "epoch", "租约代次已变化，拒绝旧控制请求",
            {"holder_session": lease["holder_session"],
             "epoch": lease.get("epoch"), "expected_epoch": expected_epoch})
    return target


def _ack_interrupted_alerts(ledger: ManualRecoveryLedger, *, project: str,
                            session: str, actor: str) -> list[str]:
    acked: list[str] = []
    for alert in ledger.list_alerts(state="new", limit=500):
        if alert.get("kind") != "coordinator_interrupted":
            continue
        try:
            detail = json.loads(alert.get("detail") or "{}")
        except (TypeError, ValueError):
            continue
        if (isinstance(detail, dict) and detail.get("project") == project
                and detail.get("session_id") == session):
            ledger.ack_alert(alert["id"], actor=actor)
            acked.append(alert["id"])
    return acked


def release_turn_hold(
    control: TurnControlPort,
    ledger: ManualRecoveryLedger,
    *,
    project: str,
    session: str | None = None,
    operation_id: str,
    reason: str,
    expected_revision: int,
    expected_epoch: int | None = None,
    evidence: dict | None = None,
    actor: str = "operator",
    now: int,
) -> dict:
    """显式运维解除会话回合 hold（只清 `coordinator-interrupted`）。

    与 ``unlock_manual_gate`` 同级的本地 operator 入口：校验当前租约 holder、
    CAS revision/epoch；只清 session hold，不解除项目暂停/provider 条件，
    不 ACK 业务通知、不代替验收。成功时重置协调失败计数、把 pending 通知
    提前到当前时刻，并 ACK 对应的 interrupted 告警。
    """
    target = _lease_target(ledger, project, session, expected_epoch)
    result = control.release_hold(
        target, operation_id=operation_id, reason=reason,
        expected_revision=expected_revision, evidence=evidence, actor=actor)
    out = {"ok": bool(result.get("executed")), "project": project,
           "session": target, "result": result}
    if not result.get("executed"):
        return out
    if result.get("applied"):
        ledger.reset_coordinator_failure(
            project, reason="operator-released-turn-hold")
        out["resurfaced"] = ledger.resurface_pending_notifications(
            target, next_attempt_at=now,
            reason="operator-released-turn-hold", actor=actor)
        out["acked_alerts"] = _ack_interrupted_alerts(
            ledger, project=project, session=target, actor=actor)
    else:
        out["resurfaced"] = 0
        out["acked_alerts"] = []
    return out


def register_completed_user_turn(
    control: TurnControlPort,
    ledger: ManualRecoveryLedger,
    verify: Callable[[str, str], dict],
    *,
    project: str,
    session: str | None = None,
    operation_id: str,
    reason: str,
    expected_revision: int,
    expected_epoch: int | None = None,
    message_id: str,
    actor: str = "operator",
    now: int,
) -> dict:
    """登记一个已完成的真实用户回合并原子结束，按 C10 解除对应 hold。

    ``verify(session_id, message_id)`` 必须由调用方用 OpenCode 会话窗口核验
    原输入/唯一 idle/无更新输入/新中断，并返回证据；本用例只校验租约与 CAS，
    不把“观察到一条消息”当作真人授权（C04/C12）。失败不写控制状态、不
    留下半截 barrier。
    """
    target = _lease_target(ledger, project, session, expected_epoch)
    verification = verify(target, message_id)
    if not verification.get("finished"):
        return {"ok": False, "executed": False,
                "refused": verification.get("reason", "turn-not-verified"),
                "refused_detail": verification,
                "project": project, "session": target, "result": None}
    evidence = {k: v for k, v in verification.items()
                if k not in ("finished",)}
    result = control.register_completed_turn(
        target, message_id=message_id,
        origin_at=verification["origin_at"], operation_id=operation_id,
        idle_id=verification["idle_id"], idle_at=verification["idle_at"],
        expected_revision=expected_revision, reason=reason,
        evidence=evidence, actor=actor)
    out = {"ok": bool(result.get("executed")), "project": project,
           "session": target, "result": result,
           "verification": verification}
    if not result.get("executed"):
        return out
    if result.get("applied"):
        ledger.reset_coordinator_failure(
            project, reason="operator-registered-completed-turn")
        out["resurfaced"] = ledger.resurface_pending_notifications(
            target, next_attempt_at=now,
            reason="operator-registered-completed-turn", actor=actor)
        out["acked_alerts"] = _ack_interrupted_alerts(
            ledger, project=project, session=target, actor=actor)
    else:
        out["resurfaced"] = 0
        out["acked_alerts"] = []
    return out

"""提供商故障知会桥：把故障切换事实可靠入队并复用发送/回执（provider-fault-notifications）。

复用既有 ``OperatorEmailService`` 与 ``EmailOutbox``（事件去重/claim/回执/预算
不变），只替换**事件源**判断：

- ``enqueue_episode_event``：按 failover episode 状态推导事件类别
  （provider-fault 需人工 / provider-failover 处置知会）与 kind，写入
  failover 专属 outbox（同一条 EmailOutbox 契约，源校验用 FailoverLedger 的
  failover 事实 validator）。
- 发送/回执/过期扫描直接委托给包装的 ``email_svc``（OperatorEmailService）——
  dispatch/verify_receipt/reconcile_stale_claimed 逻辑完全复用，不再复制。

安全不变量（specs/provider-fault-notifications N01–N12）：

- 快速恢复（未首发即 recovered）仍发首封摘要（N07）：本服务不套用既有
  “恢复前未提交则抑制”规则——故障切换是重要已发生事实，不只当前待办。
- 换代/路由变化不使已发生事实失效（N08）：failover 绑定只验证 episode
  归属，不因 current attempt/模型变化返回 stale。
- 同事故去重、已可能提交后仅实质版本更新且不超预算（N10/N11）由
  ``failover_notify.decide`` 与 outbox 稳定事件身份保证。
- 邮件只提醒，绝不修改业务结论（ACK/验收/写权）——本服务只写 mail-outbox
  命名空间。
"""

from __future__ import annotations

from typing import Any

from ..domain import failover as fo
from ..domain import failover_notify as notify
from ..domain.operator_email import (
    POLICY_ENABLED,
    STATUS_PENDING,
    STATUS_PENDING_AUTH,
    authorize,
    policy_state,
)
from ..application.operator_email import (
    OperatorEmailService,
    _fingerprint_of,
    _sender_fingerprint,
    _sender_ref,
)


class FailoverEmailService:
    """故障知会入口：事件入队 + 委托既有发送/回执。"""

    def __init__(self, email: OperatorEmailService, clock: Any, *,
                 default_recipient_ref: str = "operator-primary"):
        self.email = email
        self.clock = clock
        self._recipient_ref = default_recipient_ref

    @property
    def _policy(self):
        return self.email._policy

    def _now_ms(self) -> int:
        try:
            now = self.clock.now_ms()
            return int(now)
        except Exception:
            import time

            return int(time.time() * 1000)

    # ---- 只读视图（复用既有 outbox 列表） ---------------------------------
    def list_items(self) -> list[dict]:
        return self.email.list_items()

    def policy_status(self) -> dict:
        return self.email.policy_status()

    # ---- 事件入队 -----------------------------------------------------------
    def enqueue_episode_event(self, episode: dict, *, now: int | None = None) -> dict:
        """按 episode 状态把一次故障事实事件写入 outbox。

        episode 需含 episode_id/project/task_id/session_id/route/category/
        status/revision，可选 metadata.target（切换目标路由）。
        """
        if not isinstance(episode, dict):
            return {"result": "invalid-episode"}
        now = now if isinstance(now, int) else self._now_ms()
        incident_type = notify.event_type_for_state(
            str(episode.get("status") or ""))
        if not incident_type:
            return {"result": "no-event-type", "status": episode.get("status")}
        recipient_ref = self._recipient_ref
        project = episode.get("project")
        if not isinstance(project, str) or not project:
            return {"result": "invalid-project"}
        bk = notify.base_key(str(episode.get("episode_id") or ""),
                             incident_type, recipient_ref)
        rows = self.email._outbox.items_for_base(bk) if hasattr(
            self.email, "_outbox") else []
        version = int(episode.get("revision") or 1)
        resolved = episode.get("status") == fo.EPISODE_RESUMED
        decision = notify.decide(version=version, rows=rows, resolved=resolved)
        kind = decision.get("event")
        if kind is None:
            return {"result": "no-new-event", "reason": decision.get("reason"),
                    "base_key": bk}
        ek = notify.event_key(
            episode_id=str(episode.get("episode_id") or ""),
            incident_type=incident_type, recipient_ref=recipient_ref,
            version=version, kind=kind)
        target = (episode.get("metadata") or {}).get("target") or {}
        target_route = None
        if isinstance(target, dict) and target.get("providerID") and target.get("id"):
            target_route = fo.route_key(target["providerID"], target["id"])
        content = notify.content(
            incident_type=incident_type, state=str(episode.get("status") or ""),
            project=project, task_id=str(episode.get("task_id") or ""),
            session_id=episode.get("session_id"),
            route=str(episode.get("route") or ""),
            category=str(episode.get("category") or ""),
            target_route=target_route,
            status=self._status_label(episode.get("status")),
            next_action=self._next_action(episode))
        auth = authorize(self._policy, project=project, kind=kind,
                         recipient_ref=recipient_ref)
        status = STATUS_PENDING if auth.get("allowed") else STATUS_PENDING_AUTH
        binding = self._binding(episode, incident_type, version)
        out = self._upsert(
            item_id=ek, base_key=bk, event_key=ek, project=project,
            incident_id=str(episode.get("episode_id") or ""),
            incident_type=incident_type, kind=kind, condition_version=version,
            escalation_level=0, recipient_ref=recipient_ref,
            sender_ref=_sender_ref(self._policy),
            recipient_fingerprint=_fingerprint_of(self._policy, "recipient",
                                                  recipient_ref),
            sender_fingerprint=_sender_fingerprint(self._policy),
            subject=content["subject"], body=content["body"],
            content_hash=content["content_hash"], status=status,
            first_seen_at=episode.get("created_at") or now,
            observed_at=now, summary={
                "episode_id": episode.get("episode_id"),
                "status": episode.get("status"),
                "condition_key": f"failover:{episode.get('episode_id')}",
                "version": version,
            },
            max_attempts=int(getattr(self._policy, "max_attempts", None) or 3),
            receipt_limit=int(getattr(self._policy, "receipt_check_limit", None)
                              or 3),
            deadline_at=None, now=now, source_binding=binding)
        if out.get("reason") == "source-recheck-failed":
            return {"result": "source-recheck-failed",
                    "reason": out.get("recheck_reason") or "source-stale",
                    "item_id": ek}
        return {"result": "enqueued", "kind": kind, "item_id": ek,
                "created": bool(out.get("created")),
                "base_key": bk, "incident_type": incident_type}

    def _bind_authorized(self, episode: dict, incident_type: str,
                         version: int) -> dict | None:
        return self._binding(episode, incident_type, version)

    def _binding(self, episode: dict, incident_type: str, version: int) -> dict:
        """failover 事实源绑定（满足 outbox 必填键；N08 按归属验证）。"""
        return {
            "source_kind": "failover-episode",
            "source_id": str(episode.get("episode_id") or ""),
            "source_condition_key": f"failover:{episode.get('episode_id')}",
            "source_version": f"v{int(version)}",
            "source_state": str(episode.get("status") or ""),
            "source_incident_type": incident_type,
            "source_project": str(episode.get("project") or ""),
            "source_task_id": str(episode.get("task_id") or ""),
            "source_session": episode.get("session_id"),
            "source_model": None,
        }

    @staticmethod
    def _status_label(state: Any) -> str:
        return {
            "open": "故障已登记，准备切换",
            "waiting_quiescence": "等待现场空闲",
            "switch_requested": "切换请求已受理",
            "switch_unknown": "切换结果未知",
            "switch_confirmed": "切换已确认",
            "continuation_requested": "后继执行已派发",
            "continuation_unknown": "后继执行待观察",
            "resumed": "已切换并恢复执行/待验收",
            "blocked_manual": "需要人工处理",
            "superseded": "已由新执行取代",
            "cancelled": "已取消",
        }.get(str(state), str(state) or "unknown")

    @staticmethod
    def _next_action(episode: dict) -> str:
        status = str(episode.get("status") or "")
        if status == fo.EPISODE_BLOCKED_MANUAL:
            return ("提供商故障无法自动恢复（无授权/备用失败/切换被拒）："
                    "需要你决定充值、换渠道或人工接管")
        if status == fo.EPISODE_RESUMED:
            return ("已切换至备用模型并恢复执行（如仍在运行则等待验收）；"
                    "此为故障事实知会，无需回复")
        return ("自动处理中（故障驱动切换）；此为事实知会，"
                "可待切换/恢复/验收进展更新")

    def _upsert(self, **kwargs):
        outbox = getattr(self.email, "_outbox", None)
        if outbox is None:
            raise RuntimeError("failover email outbox 未装配")
        return outbox.upsert_item(**kwargs)

    # ---- 发送/回执/过期：直接委托既有实现 ---------------------------------
    def dispatch_once(self, *, owner: str, item_id: str | None = None) -> dict:
        return self.email.dispatch_once(owner=owner, item_id=item_id)

    def verify_receipt(self, *, owner: str) -> dict:
        return self.email.verify_receipt(owner=owner)

    def reconcile_stale_claimed(self) -> dict:
        return self.email.reconcile_stale_claimed()


__all__ = ["FailoverEmailService"]
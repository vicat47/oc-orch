"""业务 incident → 邮件 outbox 隔离装配用例（roadmap R3；D8 三本账之一）。

``OperatorIncidentService(store, clock, *, email=None, recipient_refs=())``：

- ``store``：:class:`oc_orch.adapters.operator_incident.IncidentStore`（meta 自有
  命名空间 + 短事务；同时承载有界台账扫描与**纯本地源重验五态**）；
- ``clock``：注入时钟（FakeClock / SystemClock）；
- ``email``（可选）：复用既有 ``OperatorEmailService``——**不造第二套发送器**；
  ``None`` 表示未装配通道（仅本地可见 incident，零 outbox/零网络）；
- ``recipient_refs``（可选）：显式收件范围引用。空 → 只产生可见本地“待配置”
  incident；非空且邮件策略默认禁用 → 可登记 ``pending-auth``，绝不外发。

``reconcile(limit=50)``：三条来源扫描（waiting_human / due 超限 / no_progress）
各用**持久 keyset 公平游标**（分页尾项不饿死），恢复扫描用独立游标；只读
``list`` 不写任何游标。``limit`` 严格 int>=1（bool/非整数拒收），``truncated``
如实报告是否有队列未扫完。覆盖首批事实：

- ``waiting-human-responsibility``：责任行 ``state=waiting_human``；
- ``responsibility-due``：到期且 deadline/budget 已超限（``deadline_decision``
  exhausted）的责任；
- ``task-no-progress``：确证无推进责任的任务（正常 project pause / 会话 hold /
  provider 冷却不催，且**其他项目冷却不抑制本项目**）。

恢复扫描按源重验五态处理：active 保持；resolved（独立确定责任关闭/任务收口/
no_progress 已解除）才在同一事务重验后 CAS 恢复并桥接恢复事件；paused（正常
暂停/回合 hold/冷却）保持 open 且**绝不当作恢复**；stale（条件/换代/归属变化）
抑制（不是恢复）；unknown（读不到/坏数据）保持 open、不发恢复邮件、不转终态。

安全不变量（R3 关键安全）：

- **入 outbox 前重验**：桥接时把源绑定交给 ``enqueue_incident``，由
  ``EmailOutbox`` 在建立/升级行的**同一事务内**重验源（五态）；失败 →
  ``source-recheck-failed`` 可见计数，不建行；
- **真实发送 claim 前重验**：``EmailOutbox.claim`` 在认领事务内重验同一绑定；
  active 才发送；paused/unknown 不发送不写；resolved/stale 抑制；恢复事件
  claim 重验底层源仍 resolved（原子，不是 service 先读后放行）；
- 恢复事件的源绑定指向**已恢复的 incident 记录自身**，claim 时重验其底层
  source 仍为 definitely-resolved；旧未知/已受理不会因新绑定重发，晚到回执
  不盖新源（既有 outbox CAS 保留）；
- **重开**：已 recovered/suppressed 的 incident 重遇同一条件状态或新条件时以
  新版本重开，不复用旧 event_key 重发已受理/未知邮件；
- 网络从不进入 ``reconcile``（只写台账/meta 与 outbox 意图，发送在独立执行器）。
"""

from __future__ import annotations

from typing import Any, Protocol

from ..domain import operator_incident as inc
from ..domain.errors import LedgerError
from .ports import Clock

DEFAULT_RECONCILE_LIMIT = 50

# keyset 公平游标 kind（与 store 的 incident:cursor:{kind} 命名空间对应）。
_CURSOR_WH = "waiting-human"
_CURSOR_DUE = "due"
_CURSOR_NP = "no-progress"
_CURSOR_SWEEP = "sweep"


class IncidentStorePort(Protocol):
    """incident 存储窄端口（父级组合根注入的 IncidentStore）。"""

    conn: Any

    def list_waiting_human_sources_page(self, *, limit: int,
                                           after_id: str | None = None
                                           ) -> tuple[list[tuple[dict, str | None]],
                                                      str | None, bool]: ...
    def list_due_sources_page(self, *, now: int, limit: int,
                              after_id: str | None = None
                              ) -> tuple[list[tuple[dict, str | None]],
                                         str | None, bool]: ...
    def list_no_progress_sources_page(self, *, limit: int,
                                     after_id: str | None = None
                                     ) -> tuple[list[tuple[dict, str | None]],
                                                str | None, bool]: ...
    def list_incidents(self, *, project: str | None = None,
                       state: str | None = None, limit: int = 100) -> list[dict]: ...
    def inspect_incidents(self, *, project: str | None = None, state: str | None = None,
                          limit: int = 100, after_id: str | None = None) -> dict: ...
    def list_incidents_page(self, *, state: str | None, after_id: str | None,
                            limit: int) -> tuple[list[dict], str | None, bool]: ...
    def get_incident(self, incident_id: str) -> dict | None: ...
    def upsert(self, *, incident_id: str, source: dict, now: int,
               recipient_scope: str = "", summary: dict | None = None,
               metadata: dict | None = None) -> dict: ...
    def mark_suppressed(self, *, incident_id: str, expected_revision: int,
                        reason: str = "", now: int) -> dict: ...
    def recover_incident(self, *, incident_id: str, expected_revision: int,
                         evidence: str = "", now: int) -> dict: ...
    def recheck_binding(self, conn, binding: dict) -> dict: ...
    def get_cursor(self, kind: str) -> str | None: ...
    def set_cursor(self, kind: str, value: str | None) -> None: ...


def _strict_bounded(limit: Any, *, default: int = DEFAULT_RECONCILE_LIMIT) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
        raise ValueError("limit 必须为 1..200 的整数")
    return int(limit)


class OperatorIncidentService:
    """业务 incident → outbox 的隔离装配用例（默认零通道、零网络）。"""

    def __init__(self, store: IncidentStorePort, clock: Clock, *,
                 email: Any = None, recipient_refs: tuple = ()):
        self._store = store
        self._clock = clock
        self._email = email
        refs = tuple(recipient_refs or ())
        if not all(isinstance(r, str) and r.strip() for r in refs):
            raise ValueError("recipient_refs 必须为收件范围字符串元组")
        self._recipient_refs = refs

    # ---- 基本信息 ---------------------------------------------------------
    def _now(self) -> int:
        return int(self._clock.now_ms())

    def _email_policy_state(self) -> str | None:
        if self._email is None:
            return None
        try:
            status = self._email.policy_status()
        except Exception:
            return None
        return status.get("state") if isinstance(status, dict) else None

    def _channel(self) -> str:
        return inc.channel_state(recipient_refs=self._recipient_refs,
                                 email_policy_state=self._email_policy_state())

    # ---- reconcile ---------------------------------------------------------
    def reconcile(self, *, limit: int = DEFAULT_RECONCILE_LIMIT) -> dict:
        """有界公平扫描首批事实并登记 incident；桥接 outbox；恢复扫描。

        四条 keyset 游标各自独立（来源三条 + 恢复一条）；只读 list 不写游标。
        返回稳定计数 + 有界 ``details`` + ``truncated``/``queues``（诚实截断）。
        """
        limit = _strict_bounded(limit)
        now = self._now()
        counts: dict[str, int] = {
            "scanned": 0, "created": 0, "observed": 0, "updated": 0,
            "escalated": 0, "reopened": 0, "recovered": 0, "suppressed": 0,
            "paused": 0, "unknown": 0, "skipped": 0, "invalid": 0,
            "email_pending_auth": 0, "email_enqueued": 0, "email_blocked": 0,
        }
        details: list[dict] = []
        queues: dict[str, dict] = {}
        truncated_any = False

        def add(detail: dict) -> None:
            if len(details) < limit * 2:
                details.append(detail)

        def skip(reason: str, label: str = "") -> None:
            counts["skipped"] += 1
            if any(mark in reason for mark in ("unknown", "read-failed", "missing", "unresolved")):
                counts["unknown"] += 1
            add({"action": "skipped", "reason": reason, "label": label})

        def run_queue(kind: str, scan: Any) -> None:
            nonlocal truncated_any
            cursor = self._store.get_cursor(kind)
            entries, next_after, truncated = scan(after_id=cursor)
            truncated_any = truncated_any or bool(truncated)
            queues[kind] = {"scanned": len(entries),
                            "truncated": bool(truncated)}
            counts["scanned"] += len(entries)
            for entry in entries:
                source, skip_reason = entry
                if source is None:
                    skip(skip_reason or "source-unavailable")
                    continue
                self._record(self._process_source(source, now), counts, add,
                             now=now)
            self._store.set_cursor(kind, next_after or None)

        # 1) waiting_human 责任
        run_queue(_CURSOR_WH,
                  lambda after_id: self._store.list_waiting_human_sources_page(
                      limit=limit, after_id=after_id))
        # 2) 到期且超限责任（正常等待不催）
        run_queue(_CURSOR_DUE,
                  lambda after_id: self._store.list_due_sources_page(
                      now=now, limit=limit, after_id=after_id))
        # 3) 确证 no_progress（项目暂停/回合 hold/冷却按源逐条不催）
        run_queue(_CURSOR_NP,
                  lambda after_id: self._store.list_no_progress_sources_page(
                      limit=limit, after_id=after_id))
        # 4) 恢复扫描：独立游标，按源重验五态处理 open incident。
        sweep_cursor = self._store.get_cursor(_CURSOR_SWEEP)
        incidents, next_sweep, swept = self._store.list_incidents_page(
            state=inc.INCIDENT_OPEN, after_id=sweep_cursor, limit=limit)
        truncated_any = truncated_any or bool(swept)
        queues[_CURSOR_SWEEP] = {"scanned": len(incidents),
                                 "truncated": bool(swept)}
        counts["scanned"] += len(incidents)
        for incident in incidents:
            self._record(self._sweep_one(incident, now), counts, add, now=now)
        self._store.set_cursor(_CURSOR_SWEEP, next_sweep or None)

        return {**counts, "details": details, "truncated": truncated_any,
                "queues": queues}

    # ---- 单源处理 ---------------------------------------------------------
    def _process_source(self, source: dict, now: int) -> dict:
        try:
            src = inc.normalize_source(source)
        except (TypeError, ValueError) as exc:
            return {"action": "invalid", "reason": str(exc)[:200],
                    "incident_id": None, "email": None}
        if not src.get("observed_at"):
            src["observed_at"] = now
        incident_id = inc.incident_id(src["project"], src["incident_type"],
                                      src["source_id"])
        recipient_scope = self._recipient_refs[0] if self._recipient_refs else ""
        summary = {
            "condition_key": src["condition_key"],
            "condition_state": src["condition_state"],
            "owner": src["owner"],
            "next_action": src["next_action"],
            "escalation_level": src["escalation_level"],
        }
        try:
            out = self._store.upsert(incident_id=incident_id, source=src,
                                     now=now, recipient_scope=recipient_scope,
                                     summary=summary)
        except (ValueError, LedgerError) as exc:
            return {"action": "invalid", "reason": f"{type(exc).__name__}:{exc}"[:200],
                    "incident_id": incident_id, "email": None}
        incident_snapshot = out.get("incident") or {}
        bridge = self._bridge_incident(incident_snapshot, recovered=False,
                                       now=now)
        return {"action": out.get("action"), "incident_id": incident_id,
                "condition_version": out.get("condition_version"),
                "email": bridge,
                "label": (src["source_kind"], src["source_id"])}

    def _sweep_one(self, incident: dict, now: int) -> dict:
        incident_id = incident.get("incident_id") or ""
        binding = incident.get("source_binding")
        if not isinstance(binding, dict):
            return {"action": "invalid", "reason": "binding-missing",
                    "incident_id": incident_id, "email": None}
        try:
            verdict = self._store.recheck_binding(self._store.conn, binding)
        except Exception as exc:  # noqa: BLE001 - 重验失败保守保留 open
            return {"action": "unknown", "reason": f"recheck:{type(exc).__name__}"[:200],
                    "incident_id": incident_id, "email": None}
        state = verdict.get("state") if isinstance(verdict, dict) \
            else inc.VERDICT_UNKNOWN
        if state not in inc.VERDICTS:
            state = inc.VERDICT_UNKNOWN
        if state == inc.VERDICT_ACTIVE:
            return {"action": "active", "incident_id": incident_id,
                    "reason": "binding-verified", "email": None}
        if state == inc.VERDICT_PAUSED:
            # 正常暂停/回合 hold/冷却：保持 open，不恢复、不发邮件。
            return {"action": "paused", "incident_id": incident_id,
                    "reason": verdict.get("reason") or "normal-pause",
                    "email": None}
        if state == inc.VERDICT_UNKNOWN:
            # 读不到/坏数据：保留 open/unknown，不发恢复邮件、不转终态。
            return {"action": "unknown", "incident_id": incident_id,
                    "reason": verdict.get("reason") or "unknown",
                    "email": None}
        if state == inc.VERDICT_STALE:
            # 条件/换代/归属变化：抑制（不是恢复），不发送恢复邮件。
            try:
                out = self._store.mark_suppressed(
                    incident_id=incident_id,
                    expected_revision=int(incident.get("revision") or 0),
                    reason=verdict.get("reason") or "stale", now=now)
            except (ValueError, LedgerError) as exc:
                return {"action": "invalid", "reason": f"{type(exc).__name__}"[:200],
                        "incident_id": incident_id, "email": None}
            if not out.get("applied"):
                return {"action": "skipped",
                        "reason": out.get("reason") or "already-suppressed",
                        "incident_id": incident_id, "email": None}
            return {"action": "suppressed", "incident_id": incident_id,
                    "email": None}
        # resolved：独立确定底层已解除 → 同一事务重验后 CAS 恢复 + 恢复事件。
        evidence = f"source-resolved:{verdict.get('reason') or 'resolved'}"
        try:
            out = self._store.recover_incident(
                incident_id=incident_id,
                expected_revision=int(incident.get("revision") or 0),
                evidence=evidence, now=now)
        except (ValueError, LedgerError) as exc:
            return {"action": "invalid", "reason": f"{type(exc).__name__}"[:200],
                    "incident_id": incident_id, "email": None}
        if not out.get("applied"):
            return {"action": "skipped",
                    "reason": out.get("reason") or "already-recovered",
                    "incident_id": incident_id, "email": None}
        bridge = self._bridge_incident(out.get("incident") or {}, recovered=True,
                                       now=now)
        return {"action": "recovered", "incident_id": incident_id,
                "email": bridge}

    # ---- 桥接 outbox（复用既有发送器；网络在 dispatch 独立执行器） ---------
    def _bridge_incident(self, incident: dict, *, recovered: bool,
                         now: int) -> dict:
        if not incident or not self._recipient_refs or self._email is None:
            return {"outcome": "local-only"}
        recipient_ref = self._recipient_refs[0]
        fact = {
            "incident_id": incident.get("incident_id") or "",
            "project": incident.get("project") or "",
            "incident_type": incident.get("incident_type") or "",
            "condition_key": incident.get("condition_key") or "",
            "condition_version": int(incident.get("condition_version") or 1),
            "recipient_ref": recipient_ref,
            "condition_kind": incident.get("condition_kind")
            or "human-responsibility",
            "priority": "normal",
            "blocked_reason": incident.get("blocked_reason") or "",
            "impact": incident.get("impact") or "",
            "decision_needed": incident.get("next_action") or "",
            "entry_points": [("项目", incident.get("project") or ""),
                             ("来源", incident.get("source_id") or "")],
            "first_seen_at": int(incident.get("first_seen_at") or now),
            "observed_at": now,
            "recovered": bool(recovered),
            "recovered_evidence": (incident.get("recovered_evidence") or "")
            if recovered else "",
            "escalation_level": int(incident.get("escalation_level") or 0),
        }
        if recovered:
            # 恢复事件绑定到已恢复的 incident 记录（claim 时重验底层源仍 resolved）。
            binding = inc.recovery_binding(
                incident, recovered_at=int(incident.get("recovered_at") or now))
        else:
            binding = incident.get("source_binding")
        try:
            result = self._email.enqueue_incident(fact, source_binding=binding)
        except Exception as exc:  # noqa: BLE001 - 桥接失败不阻塞对账
            return {"outcome": "bridge-error", "reason": type(exc).__name__}
        return dict(result or {})

    # ---- 计数归类 ---------------------------------------------------------
    def _record(self, outcome: dict, counts: dict, add: Any, *, now: int) -> None:
        action = outcome.get("action")
        incident_id = outcome.get("incident_id") or ""
        bridge = outcome.get("email")
        bucket = {"create": "created", "observe": "observed",
                  "update": "updated", "escalate": "escalated",
                  "reopen": "reopened", "recovered": "recovered",
                  "suppressed": "suppressed", "paused": "paused",
                  "unknown": "unknown"}.get(action)
        if action == "invalid":
            counts["invalid"] += 1
            add({"action": "invalid", "incident_id": incident_id,
                 "reason": outcome.get("reason") or "invalid"})
            return
        if action in ("skipped", "active", "noop"):
            counts["skipped"] = counts.get("skipped", 0) + 1
            add({"action": action or "skipped", "incident_id": incident_id,
                 "reason": outcome.get("reason") or action})
            return
        if bucket is not None:
            counts[bucket] = counts.get(bucket, 0) + 1
        detail = {"action": action, "incident_id": incident_id,
                  "condition_version": outcome.get("condition_version"),
                  "label": outcome.get("label") or ""}
        if isinstance(bridge, dict):
            result = bridge.get("result") or bridge.get("outcome")
            detail["email"] = result
            if result == "pending-auth":
                counts["email_pending_auth"] += 1
            elif result == "enqueued":
                counts["email_enqueued"] += 1
            elif result == "source-recheck-failed":
                counts["email_blocked"] += 1
                detail["email_reason"] = bridge.get("reason")
        add(detail)

    # ---- 只读投影（list/status 脱敏，不写游标） ----------------------------
    def list(self, *, project: str | None = None,
             limit: int = DEFAULT_RECONCILE_LIMIT) -> list[dict]:
        """只读脱敏投影列表（不含地址/凭据/完整聊天/证据正文）。"""
        limit = _strict_bounded(limit)
        channel = self._channel()
        rows = self._store.list_incidents(project=project, limit=limit)
        return [inc.masked_projection(row, channel=channel) for row in rows]

    def status(self, *, project: str | None = None) -> dict:
        """状态概览：数量/类型/收件范围引用（ref 而非地址）/通道状态。"""
        snapshot = self._store.inspect_incidents(project=project, limit=200)
        return self._status(snapshot)

    def snapshot(self, *, project: str | None = None, limit: int = DEFAULT_RECONCILE_LIMIT,
                 after_id: str | None = None) -> dict:
        snapshot = self._store.inspect_incidents(project=project, limit=_strict_bounded(limit),
                                                 after_id=after_id)
        return {"ok": True, "incidents": [inc.masked_projection(row, channel=self._channel())
                                           for row in snapshot["incidents"]],
                "coverage": snapshot["coverage"], "status": self._status(snapshot)}

    def _status(self, snapshot: dict) -> dict:
        rows = snapshot["incidents"]
        by_state: dict[str, int] = {}
        by_type: dict[str, int] = {}
        for row in rows:
            state = str(row.get("state") or inc.INCIDENT_OPEN)
            by_state[state] = by_state.get(state, 0) + 1
            itype = str(row.get("incident_type") or "")
            by_type[itype] = by_type.get(itype, 0) + 1
        channel = self._channel()
        return {
            "total": snapshot["total"],
            "coverage": {**snapshot["coverage"], "counts_scope": "observed-page"},
            "by_state": by_state,
            "by_type": by_type,
            "recipient_refs": list(self._recipient_refs),
            "channel": channel,
            "email_configured": bool(self._recipient_refs
                                     and self._email is not None),
            "email_policy_enabled": self._email_policy_state() == "enabled",
        }


__all__ = ["OperatorIncidentService", "IncidentStorePort", "DEFAULT_RECONCILE_LIMIT",
           "_strict_bounded"]

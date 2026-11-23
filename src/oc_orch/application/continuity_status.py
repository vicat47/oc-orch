"""CLI/Web/运行时共用的本地业务投影；读取不推进对账游标或发出外部动作。"""

from contextlib import nullcontext

from .coordinator_control import CoordinatorControlPort
from .ports import Clock
from ..domain.errors import LedgerError


class ContinuityStatusService:
    def __init__(self, health, control: CoordinatorControlPort, target_gate, clock: Clock,
                 *, read_snapshot=nullcontext, identity=None):
        self._health, self._control = health, control
        self._gate, self._clock = target_gate, clock
        self._read_snapshot, self._identity = read_snapshot, identity

    def snapshot(self, *, project=None, session_id=None, limit=50, after_id=None,
                 process=None, advance_cursor=False, now=None) -> dict:
        with nullcontext() if advance_cursor else self._read_snapshot():
            out = self._project(project=project, session_id=session_id, limit=limit,
                                after_id=after_id, process=process,
                                advance_cursor=advance_cursor, now=now)
            if self._identity is not None:
                out["runtime"] = self._identity()
            return out

    def _project(self, *, project, session_id, limit, after_id, process,
                 advance_cursor, now) -> dict:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
            raise LedgerError("INVALID_LIMIT", "limit必须为1..200整数")
        for name, value in (("project", project), ("session_id", session_id), ("after_id", after_id)):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise LedgerError("INVALID_SCOPE", f"{name}必须为非空字符串或None")
        clock_now = self._clock.now_ms() if now is None else now
        result = self._health.business_health(
            limit=limit, project=project, session_id=session_id, after_id=after_id,
            advance_cursor=advance_cursor, now=clock_now)
        intentional = set()
        for item in result["tasks"]:
            if isinstance(process, dict):
                item["process_healthy"] = process.get("healthy") if isinstance(process.get("healthy"), bool) else None
            sid, target_project = item.get("coordinator_session"), item.get("project")
            if not sid or not target_project:
                continue
            try:
                decision = self._control.permits(target_project, sid)
                gate = self._gate.get(sid)
                lease = self._control.coordinator_lease(target_project)
                session = decision["session_snapshot"]
                blocking = list(decision["blocking"])
                if lease:
                    if lease["holder_session"] != sid:
                        blocking.append("lease-holder-changed")
                    if lease.get("recovery_blocked"):
                        blocking.append("lease-blocked")
                    if lease.get("expires_at") is not None and lease["expires_at"] <= clock_now:
                        blocking.append("lease-expired")
                item["control"] = {
                    "project_intent": decision["project_snapshot"]["status"],
                    "project_revision": decision["project_snapshot"]["revision"],
                    "session_revision": session["revision"], "blocking": blocking,
                    "turn_held": session["hold"] is not None,
                    "hold": session["hold"],
                    "hold_origin_at": session.get("hold_origin_at"),
                    "user_turn_active": session["barrier"] is not None,
                    "barrier_message_id": session.get("barrier_message_id"),
                    "holder_session": lease["holder_session"] if lease else None,
                    "epoch": lease["epoch"] if lease else None,
                    "provider_gate": gate["state"] if gate else None,
                    "provider": {k: gate.get(k) for k in (
                        "state", "revision", "blocked_until", "category", "failure_count",
                        "probe_count", "probe_started_at", "probe_unknown_since")} if gate else None,
                }
                if "project-paused" in blocking:
                    item.update(state="explicitly_paused", blocked=True,
                                next_action="显式继续前不发起新自动工作；在途结果仍保留")
                    intentional.add(item["task_id"])
                elif "user-turn-active" in blocking:
                    item.update(state="waiting_user_turn", blocked=True,
                                next_action="等待已登记用户回合的完整结束证据，不追加自动输入")
                    intentional.add(item["task_id"])
                elif "turn-held" in blocking:
                    item.update(state="turn_hold", blocked=True,
                                next_action="保留中断；新可信用户回合结束后核验对应hold")
                    intentional.add(item["task_id"])
                elif any(reason.startswith("lease-") for reason in blocking):
                    item.update(state="needs_human", blocked=True,
                                next_action="核对当前协调归属并显式续约/接管，不以租约过期自动接管")
                elif gate:
                    state = ("unknown_execution" if gate["state"] in (
                        "probe-outstanding", "probe-unknown") else
                        "needs_human" if gate["state"] in ("manual-blocked", "exhausted")
                        else "provider_wait")
                    item.update(state=state, blocked=True)
                    if state == "provider_wait":
                        item["next_action"] = "等待原provider条件到期后有限探测，不追加模型保活"
                        intentional.add(item["task_id"])
            except Exception:
                item.update(state="unknown", blocked=True, evidence_coverage=False,
                            next_action="控制状态不可读：停止新自动动作并人工核验")
                intentional.add(item["task_id"])
        result["events"] = [e for e in result["events"] if e.get("task_id") not in intentional]
        result.update(ok=True, schema="oc-orch.continuity-health.v1", observed_at=clock_now,
                      process={k: (process or {}).get(k) for k in (
                          "healthy", "connected", "heartbeat_age_s", "last_cycle_at")},
                      semantics="process-health-not-business-acceptance")
        return result

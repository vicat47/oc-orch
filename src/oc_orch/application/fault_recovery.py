"""提供商故障恢复共享用例（provider-fault-failover 任务3：编排，不承载网络细节）。

依赖窄端口 + 复用共享派单：
- ``failover``：:class:`oc_orch.adapters.failover_ledger.FailoverLedger`；
- ``ledger``：task/attempt/lease/audit 查询；
- ``client``：OpenCode 窄端口（``switch_model``/``session_model``/``get_session``）；
- ``dispatch``：共享 ``Orchestration.dispatch``，用于切换确认后的后继 Attempt；
- ``authorize``：资源授权判定回调（``authorize(model, preset, scope, scope_id)``）。

职责（specs/provider-fault-failover F01–F22 的应用层）：

- ``observe_failure``：把可归属故障归一、去重登记 episode；硬可切换 → 进入恢复
  队列；无法自动恢复 → blocked_manual。真实路线以错误结构化字段优先，其次
  ``session_model`` 实测，绝不信任 attempt 台账目标值（F04）。
- ``tick``：supervisor 每轮有界驱动；按状态机**每次至多推进一步**，幂等：
  open → (空闲确认) → switch_requested → (POST+GET 确认) → switch_confirmed
  → (共享派单后继) → continuation_requested → 观察新窗口 → resumed/blocked_manual。

安全不变量：

- 网络（switch/prompt）从不持有 SQLite 写锁：先在事务内记录可能提交
  （possible_submitted_at），再在事务外 POST；响应丢失只读补证（F12/F13）。
- 每次 tick 对同一 episode 最多一次切换 POST、最多一次后继派单（F19）。
- 人工控制（task paused/cancelled/accepted、session hold、协调租约失效）
  在每一步动作前复核；任一不符 → 保持/转人工，不穿越（F17/F18）。
"""

from __future__ import annotations

from typing import Any, Callable

from ..domain import failover as fo
from ..domain.errors import ApiError, LedgerError, TransportError

# 每轮对同一 episode 最多处理一个外部动作（切换 POST/后继派单），
# 避免 tick 内无限展开；确认（只读 GET）不计数。
_MAX_STEPS_PER_EPISODE = 3


def _strict_task(ledger, task_id: str) -> dict:
    task = ledger.get_task(task_id)
    if not isinstance(task, dict) or not task.get("id"):
        raise LedgerError("TASK_MISSING", f"task 不存在：{task_id}")
    return task


class FaultRecoveryService:
    """故障登记与有界恢复调谐。"""

    def __init__(self, *, ledger, failover, client, clock,
                 dispatch: Callable, authorize: Callable | None = None):
        self.ledger = ledger
        self.failover = failover
        self.client = client
        self.clock = clock
        self._dispatch = dispatch
        self._authorize = authorize or (lambda **kw: {"allowed": True,
                                                      "reason": "no-authorize-callback"})

    def _now_ms(self) -> int:
        return int(self.clock.now_ms())

    def _audit(self, action: str, entity_id: str, detail: dict) -> None:
        try:
            self.ledger.audit("fault-recovery", action, "failover", entity_id, detail)
        except Exception:
            pass

    # ---- 故障观察 ---------------------------------------------------------
    def observe_failure(self, *, error: dict | None, task_id: str, attempt_id: str,
                        session_id: str | None = None, event_id: str | None = None,
                        message_id: str | None = None, origin_at: int | None = None,
                        transients_exhausted: bool = False,
                        policy_scope: str | None = None) -> dict:
        """把一次可归属失败登记为故障 episode；返回稳定决策结果。"""
        task = _strict_task(self.ledger, task_id)
        project = task["project"]
        origin_at = origin_at if isinstance(origin_at, int) else self._now_ms()
        route_provider = route_model = route_variant = None
        if session_id:
            # 真实路线证据：优先错误结构，其次 session 实测模型（不信任台账目标值）。
            observed = self.client.session_model(session_id) if self.client else None
            if isinstance(observed, dict):
                route_provider = observed.get("providerID")
                route_model = observed.get("id")
                route_variant = observed.get("variant")
        source = {
            "error": error or {},
            "route_provider": route_provider, "route_model": route_model,
            "route_variant": route_variant, "event_id": event_id,
            "message_id": message_id, "origin_at": origin_at,
        }
        cls = fo.classify_fault_source(source)
        if cls["hard"]:
            try:
                self.failover.set_route_blocked(
                    cls["route"] or "route_unknown",
                    reason=f"provider-fault:{cls['category']}:{cls['message'][:200]}",
                    until_ms=None, now=origin_at)
            except Exception:
                pass
        # 瞬态且未耗尽：不进入故障 episode（走既有有界重试，F02）。
        if cls["transient"] and not transients_exhausted:
            return {"action": "retry", "route": cls["route"],
                    "category": cls["category"], "episode_id": None}
        if cls["unknown_route"] and not self._authorize_recovery(
                project, task_id, route=None):
            return {"action": "blocked-manual", "route": cls["route"],
                    "category": cls["category"], "reason": "route-unknown",
                    "episode_id": None}
        route = cls["route"]
        episode_id = fo.episode_id(project, task_id, route)
        now = self._now_ms()
        rec = {
            "episode_id": episode_id, "project": project, "task_id": task_id,
            "session_id": session_id, "route": route,
            "category": cls["category"], "status": fo.EPISODE_OPEN,
            "switches_used": 0, "attempts_used": 1,
            "old_attempt_id": attempt_id, "created_at": now, "updated_at": now,
            "metadata": {"error_message": cls["message"][:300],
                         "origin_at": origin_at, "event_id": event_id,
                         "message_id": message_id},
        }
        existing = self.failover.get_record("episode", episode_id)
        if existing is not None and existing.get("status") in fo.TERMINAL_EPISODE_STATES:
            # 已终态不复活；只补历史证据（F04 晚到事件）。
            return {"action": "terminal-kept", "episode_id": episode_id,
                    "status": existing["status"], "category": cls["category"]}
        if existing is not None:
            # 重复观察：只刷新时间/尝试计数，不重置预算（F04）。
            rec = {**dict(existing), "attempts_used": int(existing.get("attempts_used") or 0) + 1,
                   "updated_at": now,
                   "metadata": {**dict(existing.get("metadata") or {}),
                                "last_error_message": cls["message"][:300]}}
            self.failover.upsert_record("episode", episode_id, rec,
                                        expected_revision=int(existing["revision"]))
            return {"action": "observed", "episode_id": episode_id,
                    "status": rec["status"], "category": cls["category"]}
        allowed = (cls["hard"] and cls["auto_failover"]
                   and self._authorize_recovery(project, task_id,
                                                route=cls["route"]))
        if not allowed:
            rec["status"] = fo.EPISODE_BLOCKED_MANUAL
        self.failover.upsert_record("episode", episode_id, rec,
                                    expected_revision=None)
        self._audit("episode.create", episode_id,
                    {"route": route, "category": cls["category"],
                     "attempt": attempt_id})
        return {"action": "created", "episode_id": episode_id,
                "status": rec["status"], "category": cls["category"],
                "route": route}

    # ---- 策略/授权 --------------------------------------------------------
    def _policy_for(self, project: str, task_id: str) -> dict | None:
        task_policy = self.failover.get_policy("task", task_id)
        if task_policy is not None:
            return task_policy
        return self.failover.get_policy("project", project)

    def _authorize_recovery(self, project: str, task_id: str,
                            route: str | None) -> bool:
        policy = self._policy_for(project, task_id)
        if policy is None or not policy.get("enabled"):
            return False
        if route is not None:
            sources = policy.get("source_routes")
            if isinstance(sources, list) and sources:
                base = fo.base_route(route)
                if all(fo.base_route(r) != base for r in sources):
                    return False
        try:
            out = self._authorize(scope="task" if task_id else "project",
                                  scope_id=task_id or project,
                                  model="yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash",
                                  preset="failover")
            return bool(out.get("allowed"))
        except Exception:
            return False

    # ---- 恢复调谐 ---------------------------------------------------------
    def tick(self, *, project: str | None = None, limit: int = 10,
             now: int | None = None) -> dict:
        """有界恢复推进：open/中间态 episode 各推进一步（幂等）。"""
        now = now if isinstance(now, int) else self._now_ms()
        listed = self.failover.list_records("episode", project=project, limit=limit)
        counts: dict[str, int] = {"scanned": 0, "advanced": 0, "blocked": 0,
                                  "unknown": 0}
        for rec in listed["records"]:
            if rec.get("status") not in fo.ACTIVE_EPISODE_STATES:
                continue
            counts["scanned"] += 1
            try:
                out = self._advance(rec, now)
            except (LedgerError, FoStateError) as exc:
                counts["unknown"] += 1
                self._audit("episode.advance-error", rec.get("episode_id") or "",
                            {"error": str(exc)[:300]})
                continue
            if out.get("advanced"):
                counts["advanced"] += 1
            if out.get("blocked"):
                counts["blocked"] += 1
        return {**counts, "invalid_records": listed.get("invalid_records", 0),
                "more": listed.get("more", False)}

    def _advance(self, rec: dict, now: int) -> dict:
        """推进单个 episode 一步；返回 {advanced, blocked, next}。"""
        episode_id = rec["episode_id"]
        state = rec["status"]
        try:
            task = self.ledger.get_task(rec["task_id"])
        except LedgerError:
            return {"blocked": True, "reason": "task-missing"}
        if not isinstance(task, dict):
            return {"blocked": True, "reason": "task-missing"}
        if task["status"] in ("paused", "cancelled", "accepted") \
                or task.get("closed_at") is not None:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "task-controlled", now)
        if state in (fo.EPISODE_OPEN, fo.EPISODE_WAITING_QUIESCENCE):
            if not self._quiescent(rec, task, now):
                return self._keep(rec, fo.EPISODE_WAITING_QUIESCENCE,
                                  "not-quiescent", now)
            return self._begin_switch(rec, task, now)
        if state == fo.EPISODE_SWITCH_REQUESTED:
            return self._confirm_switch(rec, task, now)
        if state == fo.EPISODE_SWITCH_CONFIRMED:
            return self._begin_continuation(rec, task, now)
        if state in (fo.EPISODE_CONTINUATION_REQUESTED,
                     fo.EPISODE_CONTINUATION_UNKNOWN):
            return self._observe_resumed(rec, task, now)
        if state == fo.EPISODE_SWITCH_UNKNOWN:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "switch-unknown-or-budget", now)
        return {"active": True, "last": state}

    def _keep(self, rec: dict, target: str, reason: str, now: int) -> dict:
        rec["status"] = target
        rec["updated_at"] = now
        self._update(rec)
        return {"blocked": target == fo.EPISODE_BLOCKED_MANUAL,
                "reason": reason, "status": target}

    def _update(self, rec: dict) -> None:
        self.failover.upsert_record(rec["kind"], rec["episode_id"], rec,
                                    expected_revision=int(rec["revision"]))

    def _quiescent(self, rec: dict, task: dict, now: int) -> bool:
        """旧 Attempt 已确认终态失败，且 session 无未清活动/队列。"""
        old = None
        try:
            old = self.ledger.get_attempt(rec.get("old_attempt_id") or "")
        except LedgerError:
            old = None
        if old is not None and old.get("status") not in (
                "terminal_failed", "result_missing", "lost"):
            return False
        if not rec.get("session_id"):
            return True
        try:
            att = self.ledger.get_attempt(task.get("current_attempt_id") or "")
        except LedgerError:
            att = None
        if att is not None and att.get("status") not in (
                "terminal_failed", "result_missing", "lost", "accepted"):
            return False
        return True

    # ---- 切换 -------------------------------------------------------------
    def _begin_switch(self, rec: dict, task: dict, now: int) -> dict:
        targets = self._pick_target(rec, task)
        if not targets:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "no-target", now)
        target = targets[0]
        decision = fo.switch_allowed(
            switches_used=int(rec.get("switches_used") or 0),
            max_switches=int((self._policy_for(task["project"], rec["task_id"]) or {})
                             .get("max_switches") or fo.DEFAULT_MAX_SWITCHES),
            authorized=self._authorize_recovery(task["project"], rec["task_id"],
                                                route=rec.get("route")),
            target_available=not (self.failover.is_route_blocked(
                fo.route_key(target["providerID"], target["id"]) or "x", now=now) or {}).get(
                "blocked"),
            same_route=fo.base_route(rec.get("route")) == fo.base_route(
                fo.route_key(target["providerID"], target["id"])),
        )
        if not decision["allowed"]:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              decision["reason"], now)
        operation_id = self.failover.new_id("OP-", 14)
        operation = {
            "kind": "operation", "operation_id": operation_id,
            "episode_id": rec["episode_id"], "session_id": rec.get("session_id"),
            "status": "switch_issued", "revision": 0,
            "possible_submitted_at": now, "created_at": now, "updated_at": now,
            "metadata": {"target": target, "source": rec.get("route")},
        }
        self.failover.upsert_record("operation", operation_id, operation,
                                    expected_revision=None)
        # 事务外切换（一次 POST；未知由确认步只读补证，不重发）。
        switch_out = self._call_switch(rec, target)
        rec["switches_used"] = int(rec.get("switches_used") or 0) + 1
        if switch_out.get("accepted"):
            rec["status"] = fo.EPISODE_SWITCH_REQUESTED
            rec["metadata"] = {**dict(rec.get("metadata") or {}),
                               "operation_id": operation_id, "target": target}
            self._update(rec)
            return {"advanced": True, "status": rec["status"], "operation_id": operation_id}
        # switch 明确失败（ApiError 拒绝）= 非 unknown：转人工，不重发。
        return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "switch-rejected", now)

    def _call_switch(self, rec: dict, target: dict) -> dict:
        if not self.client or not rec.get("session_id"):
            return {"accepted": False, "reason": "no-session-or-client"}
        try:
            out = self.client.switch_model(
                rec["session_id"], provider=target["providerID"],
                model=target["id"], variant=target.get("variant"))
            return {"accepted": bool(out.get("accepted"))}
        except TransportError:
            # 结果未知：保留 unknown 语义（确认步会只读补证，但本步不重发）。
            return {"accepted": True, "unknown": True}
        except ApiError:
            return {"accepted": False, "reason": "api-rejected"}

    def _pick_target(self, rec: dict, task: dict) -> list[dict]:
        policy = self._policy_for(task["project"], rec["task_id"]) or {}
        routes = policy.get("fallback_routes") or []
        targets = []
        for r in routes:
            if not isinstance(r, str) or "/" not in r:
                continue
            provider, model = r.split("/", 1)
            targets.append({"providerID": provider, "id": model})
        return targets

    def _confirm_switch(self, rec: dict, task: dict, now: int) -> dict:
        target = (rec.get("metadata") or {}).get("target")
        if not isinstance(target, dict):
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "no-target", now)
        observed = self.client.session_model(rec["session_id"]) if self.client else None
        if fo.models_equal(observed, target):
            rec["status"] = fo.EPISODE_SWITCH_CONFIRMED
            rec["updated_at"] = now
            self._update(rec)
            return {"advanced": True, "status": rec["status"]}
        if self._confirm_attempts(rec, now):
            return {"active": True, "status": fo.EPISODE_SWITCH_UNKNOWN}
        return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "switch-unconfirmed", now)

    def _confirm_attempts(self, rec: dict, now: int) -> bool:
        md = rec.get("metadata") or {}
        attempts = int(md.get("confirm_attempts") or 0) + 1
        rec["metadata"] = {**md, "confirm_attempts": attempts,
                           "last_confirm_at": now}
        self._update(rec)
        return attempts < fo.DEFAULT_CONFIRM_QUERIES

    # ---- 接续（复用共享派单） ----------------------------------------------
    def _begin_continuation(self, rec: dict, task: dict, now: int) -> dict:
        old = self.ledger.get_attempt(rec.get("old_attempt_id") or "")
        directory = old.get("directory") if isinstance(old, dict) else None
        if not directory:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "no-directory", now)
        target = (rec.get("metadata") or {}).get("target") or {}
        target_route = fo.route_key(target.get("providerID"), target.get("id"))
        feedback = (f"上一尝试 {rec.get('old_attempt_id')} 因提供商故障 "
                    f"({rec.get('category')}，实际路线 {rec.get('route')}) 失败，"
                    f"已同 session 切换至 {target_route}；"
                    "请接续原任务目标，不要重做已验收部分，不要回退模型。")
        try:
            out = self._dispatch(
                task_id=task["id"], directory=directory,
                coordinator=task.get("coordinator_session"),
                session=rec.get("session_id"),
                model=target_route,
                agent=(old or {}).get("agent"),
                prompt=feedback, work_mode="latency-first")
        except Exception as exc:
            return {"active": True, "status": fo.EPISODE_SWITCH_CONFIRMED,
                    "reason": f"dispatch:{type(exc).__name__}"}
        rec["status"] = fo.EPISODE_CONTINUATION_REQUESTED
        rec["metadata"] = {**dict(rec.get("metadata") or {}),
                           "successor_attempt_id": out.get("attempt_id"),
                           "successor_message_id": out.get("message_id")}
        rec["updated_at"] = now
        self._update(rec)
        return {"advanced": True, "status": rec["status"],
                "successor_attempt_id": out.get("attempt_id")}

    def _observe_resumed(self, rec: dict, task: dict, now: int) -> dict:
        successor = (rec.get("metadata") or {}).get("successor_attempt_id")
        if not successor:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "no-successor-observed", now)
        try:
            att = self.ledger.get_attempt(successor)
        except LedgerError:
            att = None
        if isinstance(att, dict) and att.get("status") in (
                "terminal_succeeded", "result_collected", "accepted",
                "verification_failed", "terminal_failed"):
            rec["status"] = fo.EPISODE_RESUMED
            rec["metadata"] = {**dict(rec.get("metadata") or {}),
                               "resumed_attempt": successor, "resumed_at": now}
            rec["updated_at"] = now
            self._update(rec)
            return {"advanced": True, "status": fo.EPISODE_RESUMED}
        # 尚无后继进展：保持在观察态（不推进；超时由 supervisor/期限处理）。
        if rec["status"] == fo.EPISODE_CONTINUATION_REQUESTED:
            rec["status"] = fo.EPISODE_CONTINUATION_UNKNOWN
            rec["updated_at"] = now
            self._update(rec)
        return {"active": True, "status": rec["status"]}


class FoStateError(RuntimeError):
    """恢复编排内部状态异常。"""


__all__ = ["FaultRecoveryService", "FoStateError"]
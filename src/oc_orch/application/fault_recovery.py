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
            try:
                observed = self.client.session_model(session_id) if self.client else None
            except Exception:
                observed = None
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
        stale_attempt = task.get("current_attempt_id") not in (None, attempt_id)
        if cls["hard"] and not cls["unknown_route"] and not stale_attempt:
            try:
                self.failover.set_route_blocked(
                    cls["route"] or "route_unknown",
                    reason=f"provider-fault:{cls['category']}:{cls['message'][:200]}",
                    until_ms=None, now=origin_at)
            except Exception:
                pass
        # 瞬态且未耗尽：不进入故障 episode（走既有有界重试，F02）。
        if cls["transient"]:
            if not transients_exhausted:
                return {"action": "retry", "route": cls["route"],
                        "category": cls["category"], "episode_id": None}
            retry_after = cls.get("retry_after_ms")
            if isinstance(retry_after, int) and retry_after > 0 \
                    and self._now_ms() < origin_at + retry_after:
                return {"action": "retry", "reason": "trusted-retry-after",
                        "route": cls["route"], "category": cls["category"],
                        "episode_id": None}
        route = cls["route"]
        episode_id = fo.episode_id(project, task_id, route)
        if stale_attempt:
            existing = self.failover.get_record("episode", episode_id)
            if existing is not None:
                md = dict(existing.get("metadata") or {})
                late = list(md.get("late_observations") or [])[-7:]
                late.append({"attempt_id": attempt_id, "event_id": event_id,
                             "message_id": message_id, "origin_at": origin_at})
                existing["metadata"] = {**md, "late_observations": late}
                existing["updated_at"] = self._now_ms()
                self.failover.upsert_record(
                    "episode", episode_id, existing,
                    expected_revision=int(existing["revision"]))
            return {"action": "stale-observation-kept", "route": route,
                    "episode_id": episode_id if existing is not None else None}
        now = self._now_ms()
        policy = self._policy_for(project, task_id)
        frozen_policy = self._freeze_policy(policy)
        target_route = next(iter((frozen_policy or {}).get("fallback_routes") or []), None)
        rec = {
            "episode_id": episode_id, "project": project, "task_id": task_id,
            "session_id": session_id, "route": route,
            "category": cls["category"], "status": fo.EPISODE_OPEN,
            "switches_used": 0, "attempts_used": 1,
            "old_attempt_id": attempt_id, "created_at": now, "updated_at": now,
            "metadata": {"error_message": cls["message"][:300],
                         "origin_at": origin_at, "event_id": event_id,
                         "message_id": message_id,
                         "policy_snapshot": frozen_policy},
        }
        existing = self.failover.get_record("episode", episode_id)
        if existing is not None and existing.get("status") in fo.TERMINAL_EPISODE_STATES:
            # 已终态不复活；只补历史证据（F04 晚到事件）。
            md = dict(existing.get("metadata") or {})
            late = list(md.get("late_observations") or [])[-7:]
            late.append({"attempt_id": attempt_id, "event_id": event_id,
                         "message_id": message_id, "origin_at": origin_at})
            existing["metadata"] = {**md, "late_observations": late}
            existing["updated_at"] = now
            self.failover.upsert_record(
                "episode", episode_id, existing,
                expected_revision=int(existing["revision"]))
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
                                                route=cls["route"],
                                                target_route=target_route))
        if cls["unknown_route"]:
            allowed = False
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

    @staticmethod
    def _freeze_policy(policy: dict | None) -> dict | None:
        if not isinstance(policy, dict):
            return None
        return {key: policy.get(key) for key in (
            "revision", "enabled", "scope", "scope_id", "source_routes",
            "fallback_routes", "max_switches", "max_attempts", "notify_updates")}

    def _authorize_recovery(self, project: str, task_id: str,
                            route: str | None,
                            target_route: str | None = None) -> bool:
        policy = self._policy_for(project, task_id)
        if policy is None or not policy.get("enabled"):
            return False
        if route is not None:
            sources = policy.get("source_routes")
            if isinstance(sources, list) and sources:
                base = fo.base_route(route)
                if all(fo.base_route(r) != base for r in sources):
                    return False
        target_route = target_route or next(iter(policy.get("fallback_routes") or []), None)
        if not isinstance(target_route, str) or "/" not in target_route:
            return False
        try:
            out = self._authorize(scope="task" if task_id else "project",
                                  scope_id=task_id or project,
                                  model=target_route.split("#", 1)[0],
                                  # 后继派单固定使用 latency-first。资源授权只接受
                                  # 已有工作模式；"failover" 不是授权 preset，传它
                                  # 会让真实组合根 fail-closed，即使显式资源已授权。
                                  preset="latency-first")
            return bool(out.get("allowed"))
        except Exception:
            return False

    # ---- 恢复调谐 ---------------------------------------------------------
    def tick(self, *, project: str | None = None, limit: int = 10,
             now: int | None = None,
             episode_id: str | None = None) -> dict:
        """有界恢复推进：open/中间态 episode 各推进一步（幂等）。"""
        now = now if isinstance(now, int) else self._now_ms()
        if episode_id:
            one = self.failover.get_record("episode", episode_id)
            if one is None or project and one.get("project") != project:
                listed = {"records": [], "more": False,
                          "invalid_records": 0}
            else:
                listed = {"records": [one], "more": False,
                          "invalid_records": 0}
        else:
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
            reason = self._quiescence_block(rec, task)
            if reason:
                return self._keep(rec, fo.EPISODE_WAITING_QUIESCENCE,
                                  reason, now)
            return self._begin_switch(rec, task, now)
        if state == fo.EPISODE_SWITCH_REQUESTED:
            return self._confirm_switch(rec, task, now)
        if state == fo.EPISODE_SWITCH_CONFIRMED:
            return self._begin_continuation(rec, task, now)
        if state in (fo.EPISODE_CONTINUATION_REQUESTED,
                     fo.EPISODE_CONTINUATION_UNKNOWN):
            return self._begin_continuation(rec, task, now)
        if state == fo.EPISODE_SWITCH_UNKNOWN:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "switch-unknown-or-budget", now)
        return {"active": True, "last": state}

    def _keep(self, rec: dict, target: str, reason: str, now: int) -> dict:
        rec["status"] = target
        rec["updated_at"] = now
        rec["metadata"] = {**dict(rec.get("metadata") or {}),
                           "last_block_reason": reason}
        self._update(rec)
        return {"blocked": target == fo.EPISODE_BLOCKED_MANUAL,
                "reason": reason, "status": target}

    def _update(self, rec: dict) -> None:
        out = self.failover.upsert_record(
            rec["kind"], rec["episode_id"], rec,
            expected_revision=int(rec["revision"]))
        # Keep the in-flight transition snapshot at the committed revision.
        # A caller may need to perform a second state write in the same tick
        # (for example, consume the final confirmation query then block manual).
        if isinstance(out, dict) and isinstance(out.get("record"), dict):
            rec.clear()
            rec.update(out["record"])

    def _quiescence_block(self, rec: dict, task: dict) -> str | None:
        """Fail closed unless the failed worker session is demonstrably idle.

        A terminal attempt row alone is not enough: OpenCode may still be executing,
        the inbox may contain unrelated work, or the session may be shared with a
        coordinator/another task. Any missing, malformed, or unavailable observation
        leaves the episode waiting for a later reconciliation.
        """
        session_id = rec.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            return "session-unknown"
        if session_id == task.get("coordinator_session"):
            return "coordinator-session"

        # A coordinator or a second Task bound to the same session is outside the
        # automatic recovery scope, even when that other Task is currently idle.
        try:
            tasks = self.ledger.list_tasks(limit=1001)
            if not isinstance(tasks, list) or len(tasks) >= 1001:
                return "task-ownership-unknown"
            if any(t.get("id") != task.get("id")
                   and t.get("coordinator_session") == session_id for t in tasks):
                return "coordinator-session"
            attempts = self.ledger.attempts_for_session(session_id, limit=201)
            if not isinstance(attempts, list) or len(attempts) >= 201:
                return "session-ownership-unknown"
            if any(a.get("task_id") != task.get("id") for a in attempts):
                return "multi-task-session"
        except Exception:
            return "session-ownership-unknown"

        old_id = rec.get("old_attempt_id")
        try:
            old = self.ledger.get_attempt(old_id or "")
        except Exception:
            return "failed-attempt-unknown"
        if not isinstance(old, dict) or old.get("id") != old_id:
            return "failed-attempt-unknown"
        if old.get("status") not in ("terminal_failed", "result_missing", "lost"):
            return "failed-attempt-not-terminal"
        if old.get("dispatch_state") not in ("confirmed", "unknown"):
            return "dispatch-outcome-unknown"
        if old.get("dispatch_state") == "unknown":
            return "dispatch-outcome-unknown"
        if old.get("stop_requested_at") and not old.get("stop_confirmed_at"):
            return "stop-unconfirmed"
        if not old.get("site_released_at"):
            return "write-site-not-released"
        current_id = task.get("current_attempt_id")
        if current_id != old_id:
            return "current-attempt-changed"
        try:
            has_pending_input = self.failover.has_pending_input(session_id)
        except Exception:
            return "managed-input-state-unknown"
        if has_pending_input:
            return "managed-input-pending"

        try:
            session = self.client.get_session(session_id) if self.client else None
            if not isinstance(session, dict) or session.get("id") != session_id:
                return "session-observation-unknown"
            model = session.get("model")
            observed_route = (fo.route_key(model.get("providerID"), model.get("id"))
                              if isinstance(model, dict) else None)
            if not observed_route or fo.base_route(observed_route) != \
                    fo.base_route(rec.get("route")):
                return "session-model-changed"

            active = self.client.active_sessions()
            if not isinstance(active, dict):
                return "active-state-unknown"
            if session_id in active:
                return "session-active"

            inbox = self.client.inbox(session_id)
            forms = self.client.forms(session_id)
            directory = ((session.get("location") or {}).get("directory")
                         or old.get("directory"))
            if not isinstance(directory, str) or not directory:
                return "session-directory-unknown"
            permissions = self.client.permissions(directory)
            shells = self.client.shells(directory)
            children = self.client.list_sessions(parent_id=session_id, limit=101)
            if not all(isinstance(v, list) for v in
                       (inbox, forms, permissions, shells, children)):
                return "session-queue-unknown"
            if len(children) >= 101:
                return "child-session-coverage-unknown"
            if inbox:
                return "session-inbox-not-empty"
            if forms:
                return "session-form-pending"
            if permissions:
                return "session-permission-pending"
            active_children = [child for child in children
                               if not isinstance(child, dict)
                               or child.get("time", {}).get("idle") is None
                               or child.get("time", {}).get("busy") is None
                               or child.get("time", {}).get("idle", 0)
                               <= child.get("time", {}).get("busy", 0)]
            if active_children:
                return "child-session-present"
            if any(not isinstance(sh, dict) or sh.get("status") not in
                   ("completed", "failed", "exited", "terminated", "killed")
                   for sh in shells):
                return "shell-active-or-unknown"
        except Exception:
            return "session-observation-unknown"
        return None

    def _quiescent(self, rec: dict, task: dict, now: int) -> bool:
        """Compatibility predicate for callers that need a simple idle answer."""
        return self._quiescence_block(rec, task) is None

    # ---- 切换 -------------------------------------------------------------
    def _begin_switch(self, rec: dict, task: dict, now: int) -> dict:
        # Re-read all local and remote gates immediately before recording the
        # possible submission. OpenCode has no remote CAS, so any detected external
        # activity/model change must stop the switch and remain visible.
        latest_task = self.ledger.get_task(rec["task_id"])
        if latest_task.get("status") in ("paused", "cancelled", "accepted") \
                or latest_task.get("closed_at") is not None:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "task-controlled-before-switch", now)
        reason = self._quiescence_block(rec, latest_task)
        if reason:
            return self._keep(rec, fo.EPISODE_WAITING_QUIESCENCE, reason, now)
        task = latest_task
        targets = self._pick_target(rec, task)
        if not targets:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "no-target", now)
        target = targets[0]
        decision = fo.switch_allowed(
            switches_used=int(rec.get("switches_used") or 0),
            max_switches=int(((rec.get("metadata") or {}).get("policy_snapshot")
                              or self._policy_for(task["project"], rec["task_id"])
                              or {}).get("max_switches") or fo.DEFAULT_MAX_SWITCHES),
            authorized=self._authorize_recovery(task["project"], rec["task_id"],
                                                route=rec.get("route"),
                                                target_route=(
                                                    f"{target['providerID']}/{target['id']}")),
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
        claim = self.failover.prepare_switch(
            episode_id=rec["episode_id"], operation_id=operation_id,
            expected_revision=int(rec["revision"]), target=target,
            owner="supervisor", now=now)
        if not claim.get("claimed"):
            return {"active": True, "reason": claim.get("reason"),
                    "status": (claim.get("episode") or {}).get("status",
                               rec.get("status"))}
        rec = claim["episode"]
        # possible + per-session claim 已提交；外部 POST 只允许此处发生一次。
        switch_out = self._call_switch(rec, target)
        if switch_out.get("unknown"):
            self.failover.record_operation_result(
                operation_id, status="switch-unknown",
                detail={"result": switch_out.get("reason", "transport-unknown")})
        if switch_out.get("accepted"):
            if not switch_out.get("unknown"):
                self.failover.record_operation_result(
                    operation_id, status="switch-accepted",
                    detail={"accepted_at": now})
            return {"advanced": True, "status": rec["status"], "operation_id": operation_id}
        self.failover.record_operation_result(
            operation_id, status="switch-rejected",
            detail={"result": switch_out.get("reason", "rejected")})
        out = self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "switch-rejected", now)
        self.failover.release_session_lock(
            rec["session_id"], operation_id=operation_id,
            actor="supervisor", reason="switch API 明确拒绝，确认未提交")
        return out

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
        except ApiError as exc:
            if exc.status >= 500 or exc.status in (408, 429):
                return {"accepted": True, "unknown": True,
                        "reason": f"http-{exc.status}-unknown"}
            return {"accepted": False, "reason": "api-rejected"}
        except Exception as exc:
            return {"accepted": True, "unknown": True,
                    "reason": f"{type(exc).__name__}-unknown"}

    def _pick_target(self, rec: dict, task: dict) -> list[dict]:
        policy = ((rec.get("metadata") or {}).get("policy_snapshot")
                  or self._policy_for(task["project"], rec["task_id"]) or {})
        routes = policy.get("fallback_routes") or []
        targets = []
        for r in routes:
            if not isinstance(r, str) or "/" not in r:
                continue
            provider, model = r.split("/", 1)
            variant = None
            if "#" in model:
                model, variant = model.split("#", 1)
            target = {"providerID": provider, "id": model}
            if variant:
                target["variant"] = variant
            targets.append(target)
        return targets

    def _confirm_switch(self, rec: dict, task: dict, now: int) -> dict:
        target = (rec.get("metadata") or {}).get("target")
        if not isinstance(target, dict):
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "no-target", now)
        md = dict(rec.get("metadata") or {})
        if now < int(md.get("next_confirm_at") or 0):
            return {"active": True, "status": rec["status"],
                    "reason": "confirmation-interval"}
        if now >= int(md.get("confirm_deadline_at") or
                      (md.get("possible_submitted_at", now) + fo.DEFAULT_CONFIRM_DEADLINE_MS)):
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "switch-confirm-deadline", now)
        try:
            observed = self.client.session_model(rec["session_id"]) if self.client else None
        except Exception:
            observed = None
        if fo.models_equal(observed, target):
            rec["status"] = fo.EPISODE_SWITCH_CONFIRMED
            rec["updated_at"] = now
            self._update(rec)
            operation_id = (rec.get("metadata") or {}).get("operation_id")
            if operation_id:
                self.failover.record_operation_result(
                    operation_id, status="switch-confirmed",
                    detail={"confirmed_at": now})
            return {"advanced": True, "status": rec["status"]}
        old_route = fo.base_route(rec.get("route"))
        observed_route = None
        if isinstance(observed, dict) and observed.get("providerID") and observed.get("id"):
            observed_route = fo.base_route(fo.route_key(
                observed["providerID"], observed["id"], observed.get("variant")))
        if observed_route and observed_route != old_route \
                and not fo.models_equal(observed, target):
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "external-model-change", now)
        if self._confirm_attempts(rec, now):
            return {"active": True, "status": fo.EPISODE_SWITCH_UNKNOWN}
        return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "switch-unconfirmed", now)

    def _confirm_attempts(self, rec: dict, now: int) -> bool:
        md = rec.get("metadata") or {}
        attempts = int(md.get("confirm_queries") or 0) + 1
        rec["metadata"] = {**md, "confirm_queries": attempts,
                           "confirm_attempts": attempts,
                           "last_confirm_at": now,
                           "next_confirm_at": now + fo.DEFAULT_CONFIRM_INTERVAL_MS}
        self._update(rec)
        return attempts < fo.DEFAULT_CONFIRM_QUERIES

    # ---- 接续（复用共享派单） ----------------------------------------------
    def _begin_continuation(self, rec: dict, task: dict, now: int) -> dict:
        old = self.ledger.get_attempt(rec.get("old_attempt_id") or "")
        directory = old.get("directory") if isinstance(old, dict) else None
        if not directory:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL, "no-directory", now)
        old_deadline = old.get("deadline_at") if isinstance(old, dict) else None
        if isinstance(old_deadline, int) and now >= old_deadline:
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "task-deadline-expired", now)
        old_metadata = (old.get("metadata") or {}) if isinstance(old, dict) else {}
        write_scope = old_metadata.get("write_scope") or task.get("write_scope") or [directory]
        deliverables = old_metadata.get("deliverables") or []
        checks = old_metadata.get("checks") or []
        target = (rec.get("metadata") or {}).get("target") or {}
        target_route = fo.route_key(target.get("providerID"), target.get("id"))
        feedback = (f"上一尝试 {rec.get('old_attempt_id')} 因提供商故障 "
                    f"({rec.get('category')}，实际路线 {rec.get('route')}) 失败，"
                    f"已同 session 切换至 {target_route}；"
                    "请接续原任务目标，不要重做已验收部分，不要回退模型。")
        md = dict(rec.get("metadata") or {})
        operation_id = md.get("operation_id")
        if not isinstance(operation_id, str):
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "operation-identity-missing", now)
        successor_existing_id = md.get("successor_attempt_id")
        if task.get("current_attempt_id") not in (
                rec.get("old_attempt_id"), successor_existing_id):
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "current-attempt-changed-before-continuation", now)
        target_auth_route = f"{target.get('providerID')}/{target.get('id')}"
        if not self._authorize_recovery(
                task["project"], rec["task_id"], rec.get("route"),
                target_route=target_auth_route):
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "authorization-revoked-before-continuation", now)
        try:
            observed_model = self.client.session_model(rec.get("session_id")) \
                if self.client else None
        except Exception:
            observed_model = None
        if not fo.models_equal(observed_model, target):
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "target-model-not-confirmed-before-continuation", now)
        attempt_id = md.get("successor_attempt_id") or self.failover.new_id("A-", 12)
        message_id = md.get("successor_message_id") or self.failover.new_id("msg_ocorch", 16)
        prepared = self.failover.prepare_continuation(
            episode_id=rec["episode_id"], operation_id=operation_id,
            expected_revision=int(rec["revision"]), attempt_id=attempt_id,
            message_id=message_id, now=now)
        if not prepared.get("prepared"):
            return {"active": True, "reason": prepared.get("reason"),
                    "status": rec.get("status")}
        rec = prepared["episode"]
        try:
            existing = self.ledger.get_attempt(attempt_id)
        except LedgerError:
            existing = None
        if existing is not None:
            # 原身份已经在台账中：只等共享调谐器按原 message_id 对账，绝不再 POST。
            if existing.get("dispatch_state") == "confirmed":
                return self._observe_resumed(rec, task, now)
            observed = self._observe_resumed(rec, task, now)
            return {"active": True, "status": observed.get("status", rec["status"]),
                    "reason": "successor-dispatch-unknown",
                    "successor_attempt_id": attempt_id,
                    "successor_message_id": message_id}
        try:
            out = self._dispatch(
                task_id=task["id"], directory=directory,
                coordinator=task.get("coordinator_session"),
                session=rec.get("session_id"),
                model=target_route,
                agent=(old or {}).get("agent"),
                scope=write_scope, deliverables=deliverables, checks=checks,
                _deadline_at=old_deadline,
                stall_after=(old or {}).get("stall_after"),
                prompt=feedback, work_mode="latency-first",
                _attempt_id=attempt_id, _message_id=message_id,
                _failover_operation_id=operation_id,
                _failover_episode_id=rec["episode_id"])
        except Exception as exc:
            return {"active": True, "status": rec["status"],
                    "reason": f"dispatch:{type(exc).__name__}",
                    "successor_attempt_id": attempt_id,
                    "successor_message_id": message_id}
        if (out.get("attempt_id"), out.get("message_id")) != (attempt_id, message_id):
            return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                              "successor-identity-mismatch", now)
        return {"advanced": True, "status": rec["status"],
                "successor_attempt_id": attempt_id,
                "successor_message_id": message_id}

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
                "terminal_failed", "result_missing", "lost",
                "verification_failed"):
            episode_id = rec["episode_id"]
            session_id = rec.get("session_id")
            self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                       "fallback-execution-failed", now)
            lock = (self.failover.get_session_lock(session_id)
                    if isinstance(session_id, str) and session_id else None)
            if lock and lock["episode"].get("episode_id") == rec["episode_id"] \
                    and att.get("site_released_at"):
                self.failover.release_session_lock(
                    session_id, operation_id=lock["operation_id"],
                    actor="supervisor", reason="备用执行已确认失败并释放现场")
            return {"blocked": True, "status": fo.EPISODE_BLOCKED_MANUAL,
                    "reason": "fallback-execution-failed",
                    "episode_id": episode_id}
        effective = (isinstance(att, dict)
                     and att.get("status") in ("result_collected",)
                     and att.get("result_collected_at") and att.get("result_path"))
        if effective:
            target = (rec.get("metadata") or {}).get("target") or {}
            try:
                actual_model = self.client.session_model(rec.get("session_id"))
            except Exception:
                actual_model = None
            if not fo.models_equal(actual_model, target):
                return self._keep(rec, fo.EPISODE_BLOCKED_MANUAL,
                                  "resumed-model-not-confirmed", now)
            rec["status"] = fo.EPISODE_RESUMED
            rec["metadata"] = {**dict(rec.get("metadata") or {}),
                               "resumed_attempt": successor, "resumed_at": now}
            rec["updated_at"] = now
            self._update(rec)
            lock = self.failover.get_session_lock(rec.get("session_id") or "")
            if lock and lock["episode"].get("episode_id") == rec["episode_id"]:
                self.failover.release_session_lock(
                    rec["session_id"], operation_id=lock["operation_id"],
                    actor="supervisor", reason="后继执行已观察；会话恢复占用完成")
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

"""S4 显式受管输入共享用例（CLI/Web/Supervisor 共用）。

``ManagedInputs(ledger, *, client, clock, target_gate)``：

- ``ledger``：``ManagedLedgerPort``（B 的 ManagedInputLedger，组合根装配）；
- ``client``：**惰性工厂**（可调用返回 OpenCodeClient）；record/列表/空白名单/
  allow_remote=False 的观察绝不调用它；
- ``clock``：注入时钟；``target_gate``：目标闸门窄端口。

安全不变量（用户 2026-11-09 确认，离线切片）：

1. 只有显式 intent 进入本路径；**record 绝对不构造 Client、不读目标、不请求模型**。
2. send 以 request_id 持久幂等并冻结正文与原 message_id；**重复 request 不新 POST、
   不隐式重试**。execute 显式 resume=True；park 显式 resume=False 且到期只告警。
3. 只向显式 owner 白名单目标发送；send 不隐式注册。execute 会 resume 并消费队列，
   故非空队列 fail-closed；不通过新增 prompt 绕过队头。
4. flush/cancel/resume_target 必须 confirm+owner+expected_revision(+reason)；
   claim CAS 先行，外部 I/O 在事务外；unknown 只对账原 ID，不重复 PATCH/DELETE/POST。
5. flush 要求完整队列可信且唯一自有目标，PATCH 原 ID=steer（不 POST 新输入）；
   foreign/其他 park/未知形状 fail-closed。DELETE 204/no-op 不假取消：原 ID 已在历史
   仍 delivered，缺证据保持 unknown。
6. 观察只对显式白名单做定点 inbox，不扫全局 sessions/active、不跟 children；
   有界 limit/每目标预算；查询失败不当空。park 到期只告警、绝不 cancel/flush/执行。
"""

from __future__ import annotations

from typing import Any, Callable
from uuid import uuid4

from ..domain import managed_inputs as rules
from ..domain.errors import ActionError, LedgerError
from ..domain.rules import TASK_TERMINAL
from .managed_input_port import (
    ManagedLedgerPort,
    ManagedResponsibilityPort,
    ManagedTargetGatePort,
)
from .ports import Clock
from .delivery_action import authorize_input

_MANAGED_SOURCE = "managed_input"
_OBSERVE_TARGET_BUDGET = 50
_TRACKED_REQUEST_PREFIX = "managed-send-tracked:"
_DEFAULT_RESPONSIBILITY_DEADLINE_MS = 24 * 60 * 60 * 1000
_DEFAULT_NEXT_CHECK_MS = 5 * 60 * 1000
_CANCEL_UNKNOWN_DEADLINE_MS = 15 * 60 * 1000


class ManagedInputs:
    def __init__(self, ledger: ManagedLedgerPort, *, client: Callable[[], Any],
                 clock: Clock, target_gate: ManagedTargetGatePort,
                 responsibilities: ManagedResponsibilityPort | None = None):
        self._ledger = ledger
        self._client = client
        self.clock = clock
        self._gate = target_gate
        self._responsibilities = responsibilities
        # 有序白名单观察游标（按 session_id，轮转覆盖，避免前缀饥饿）。
        self._watch_cursor: str | None = None

    # ---- 未收敛操作保护（跨 operation 未知不可重发） ------------------------
    @staticmethod
    def _unresolved_operation(row: dict) -> str | None:
        """返回任一 claimed/unknown 未收敛操作名；已收敛（done/None）返回 None。"""
        ops = ((row.get("metadata") or {}).get("managed_input") or {}) \
            .get("operations") or {}
        for op in ("send", "flush", "cancel"):
            claim = ops.get(op)
            if isinstance(claim, dict) and claim.get("state") in ("claimed", "unknown"):
                return op
        return None

    def _claim(self, row: dict, now: int, *, operation: str, owner: str,
               expected_revision: int, reason: str) -> dict:
        """始终调用 B claim：owner+expected_revision 先验，再判未收敛操作。

        B 在事务内先校验 owner/revision（错误一律 raise，不给错误 owner/旧 revision
        成功回执），再对任一 claimed/unknown 未收敛操作返回 changed=False。因此这里
        不做本地 unresolved 短路，避免绕过 B 的事务先验。
        """
        return self._ledger.claim_managed_operation(
            row["input_id"], operation=operation, owner=owner,
            expected_revision=expected_revision, reason=reason, now=now)

    def _noop_unknown(self, row: dict, now: int) -> dict:
        """未收敛/未知：只按原 ID 只读对账，不 claim、不外部写。

        对账证据必须归属**真正已存在的未收敛操作**（其 operation_revision），
        不能用新 operation 名（找不到 claim → 永不收敛）。
        """
        operation = self._unresolved_operation(row)
        observation = self._read_delivery(row)
        if operation is not None and observation["indicator"] == "delivery":
            row = self._record(row, now, indicator="delivery",
                               via="unresolved:reconcile", operation=operation)
        return {"ok": True, "changed": False,
                "reason": "operation-unresolved",
                "input": rules.receipt_view(row, now=now, include_body=True)}

    # ---- 内部辅助 -----------------------------------------------------------
    @staticmethod
    def _require_managed(row: dict, input_id: str) -> dict:
        if row.get("source") != _MANAGED_SOURCE:
            raise LedgerError(
                "INPUT_NOT_MANAGED",
                f"输入 {input_id} 不是受管输入（source={row.get('source')!r}）")
        return row

    @staticmethod
    def _managed_block(row: dict) -> dict:
        raw = row.get("metadata")
        if isinstance(raw, dict):
            block = raw.get("managed_input")
            return block if isinstance(block, dict) else {}
        return {}

    def _claim_revision(self, row: dict, operation: str) -> int | None:
        """取某操作 claim 的 operation_revision（证据归属用），缺失返回 None。"""
        ops = self._managed_block(row).get("operations") or {}
        claim = ops.get(operation)
        rev = claim.get("operation_revision") if isinstance(claim, dict) else None
        return rev if isinstance(rev, int) and not isinstance(rev, bool) else None

    def _owned_message_ids(self, session_id: str) -> set[str]:
        rows = self._ledger.list_managed_inputs(session_id=session_id, limit=200)
        return {r.get("message_id") for r in rows if r.get("message_id")}

    def _record(self, row: dict, now: int, *, indicator: str, via: str,
                operation: str, operation_revision: int | None = None) -> dict:
        """按 B 证据契约记录一次单调观察，返回更新后的输入行。

        ``indicator`` ∈ {admission, delivery, cancelled, unknown}；operation 与
        operation_revision 必须对应既有 claim。
        """
        rev = (operation_revision if operation_revision is not None
               else self._claim_revision(row, operation))
        if rev is None:
            return row  # 无可归属 claim：不写证据（避免伪造归属）。
        evidence = {
            "message_id": row.get("message_id"),
            "operation": operation,
            "operation_revision": rev,
            "via": via,
            indicator: True,
        }
        result = self._ledger.record_managed_observation(
            row["input_id"], evidence=evidence, now=now)
        return result.get("input") or row

    def _require_owned_target(self, session_id: str, owner: str) -> None:
        watch = self._ledger.get_managed_watch(session_id)
        if watch is None or not watch.get("enabled", False):
            raise ActionError(
                "whitelist", f"目标 session {session_id} 未登记为启用中的管理白名单；"
                "send 不隐式注册，请先 register_watch", {"session_id": session_id})
        registered = watch.get("owner")
        if registered != owner:
            raise ActionError(
                "whitelist", f"目标 {session_id} 由 owner={registered!r} 登记，"
                f"{owner} 无发送权", {"session_id": session_id,
                                     "registered_owner": registered})

    def _gate_snapshot(self, session_id: str) -> Any:
        try:
            return self._gate.get(session_id)
        except Exception as exc:  # 坏记录/未来 schema：fail-closed，不得放行。
            raise ActionError(
                "gate", f"目标闸门状态不可读，fail-closed 拒绝执行发送：{exc}",
                {"session_id": session_id}) from exc

    def _find_managed_by_message(self, session_id: str,
                                 message_id: str) -> dict | None:
        """按原 message_id 查找同 session 受管输入（回执幂等，不新 POST）。"""
        if not message_id:
            return None
        rows = self._ledger.list_managed_inputs(session_id=session_id, limit=500)
        for row in rows:
            if row.get("message_id") == message_id:
                return self._ledger.get_managed_input(row["input_id"])
        return None

    def _find_managed_by_request(self, session_id: str,
                                 request_id: str) -> dict | None:
        """按显式 request_id 查找同 session 受管输入（兼容旧 send 幂等）。"""
        if not request_id:
            return None
        for row in self._ledger.list_managed_inputs(session_id=session_id,
                                                    limit=500):
            meta = row.get("metadata") or {}
            block = meta.get("managed_input") if isinstance(meta, dict) else None
            if isinstance(block, dict) and block.get("request_id") == request_id:
                return self._ledger.get_managed_input(row["input_id"])
        return None

    @staticmethod
    def _same_tracking_identity(existing: dict, planned: dict) -> bool:
        """重复 request/message 的稳定归属指纹；显式 deadline/materials 必须相同。

        默认 deadline/materials 重试保持幂等；显式字段被静默改变视为冲突。
        """
        keys = ("tracked", "task_id", "attempt_id", "generation",
                "result_version", "work_kind", "target_session", "boundary")
        if bool(existing.get("same_candidate")) != bool(planned.get("same_candidate")):
            return False
        if any(existing.get(k) != planned.get(k) for k in keys):
            return False

        def _explicit_deadline(t):
            return t.get("deadline") if t.get("deadline_source") == "explicit" else None

        def _explicit_materials(t):
            if t.get("materials_source") != "explicit":
                return None
            mats = t.get("materials")
            return tuple(sorted(repr(m) for m in mats)) if isinstance(mats, list) else ()

        return (_explicit_deadline(existing) == _explicit_deadline(planned)
                and _explicit_materials(existing) == _explicit_materials(planned))

    @staticmethod
    def _tracking_payload(plan: dict) -> dict:
        tracking = {
            "tracked": bool(plan.get("tracked")),
            "task_id": plan.get("task_id"),
            "attempt_id": plan.get("attempt_id"),
            "generation": plan.get("generation"),
            "result_version": plan.get("result_version"),
            "work_kind": plan.get("work_kind"),
            "target_session": plan.get("target_session"),
            "deadline": plan.get("deadline"),
            "next_check_at": plan.get("next_check_at"),
            "materials": plan.get("materials") or [],
            "materials_source": plan.get("materials_source") or "default",
            "deadline_source": plan.get("deadline_source") or "default",
            "attribution_sources": {
                "task": plan.get("task_source"),
                "attempt": plan.get("attempt_source"),
                "owner": plan.get("owner_source"),
                "target_session": plan.get("target_source"),
                "deadline": plan.get("deadline_source"),
                "work_kind": plan.get("work_kind_source"),
            },
        }
        if plan.get("same_candidate"):
            tracking["same_candidate"] = True
        if not plan.get("tracked"):
            tracking["boundary"] = plan.get("boundary")
        return tracking

    def _duplicate_receipt(self, row: dict, plan: dict, *, now: int) -> dict:
        tracking = rules.tracking_block(row)
        merged = dict(plan)
        merged.update({
            "responsibility_id": tracking.get("responsibility_id"),
            "target_session": tracking.get("target_session",
                                           merged.get("target_session")),
            "wait_condition": tracking.get("wait_condition"),
            "next_check_at": tracking.get("next_check_at",
                                          merged.get("next_check_at")),
            "deadline": tracking.get("deadline", merged.get("deadline")),
            "task_id": tracking.get("task_id", merged.get("task_id")),
            "attempt_id": tracking.get("attempt_id", merged.get("attempt_id")),
            "work_kind": tracking.get("work_kind", merged.get("work_kind")),
            "tracked": bool(tracking.get("tracked", merged.get("tracked"))),
            "boundary": tracking.get("boundary", merged.get("boundary")),
        })
        view = rules.receipt_view(row, now=now, include_body=True)
        return {"ok": True, "created": False, "duplicate": True,
                "tracking": (rules.TRACKED if merged.get("tracked")
                             else rules.UNTRACKED),
                "tracked": bool(merged.get("tracked")),
                "resume": True,
                "reason": "already-recorded-or-sent",
                "input": view, "receipt": self._receipt(view, merged)}

    # ---- 托管发送：base 台账 / 归属解析 / 责任写入 -----------------------
    def _base_ledger(self) -> Any:
        """取得只读业务台账（禁止 latest 猜测，只用于 attempts/task/lease 查询）。

        明确来源：优先责任 store 暴露的 ``base``；否则用 ManagedInputLedger 的
        ``base``；最后才接受本身实现 attempts_for_session 的窄 fake。
        """
        store = self._responsibilities
        base = getattr(store, "base", None) if store is not None else None
        if base is not None:
            return base
        base = getattr(self._ledger, "base", None)
        if base is not None:
            return base
        if hasattr(self._ledger, "attempts_for_session"):
            return self._ledger
        raise ActionError(
            "ledger", "无法取得业务台账 base：需要 responsibilities.base 或 "
            "ManagedInputLedger.base 才能解析任务/尝试归属")

    @staticmethod
    def _responsibility_record(result: Any) -> dict:
        if isinstance(result, dict) and isinstance(result.get("responsibility"), dict):
            return result["responsibility"]
        return result if isinstance(result, dict) else {}

    @staticmethod
    def _result_version(attempt: dict) -> str | None:
        meta = attempt.get("metadata") if isinstance(attempt, dict) else None
        result = meta.get("result") if isinstance(meta, dict) else None
        if not isinstance(result, dict):
            return None
        # 真实 app.collect 写 metadata.result.version；兼容旧 result_version 字段。
        version = result.get("version") or result.get("result_version")
        return version if isinstance(version, str) and version else None

    def _resolve_tracking_plan(
        self, session_id: str, *, owner: str | None, task_id: str | None,
        attempt_id: str | None, target_session: str | None, deadline: int | None,
        materials, work_kind: str | None, now: int,
    ) -> dict:
        """解析唯一任务/尝试与接续目标；歧义在发送前拒绝，不猜 latest。"""
        declared_work = rules.normalize_work_kind(work_kind)
        if declared_work not in (None, rules.WORK_OBSERVE, rules.WORK_WRITE):
            raise ActionError(
                "usage", f"非法 work_kind：{work_kind!r}",
                {"session_id": session_id})
        base = self._base_ledger()
        coverage_limit = 50
        explicit_attempt = None
        explicit_task = None
        try:
            if attempt_id is not None:
                explicit_attempt = base.get_attempt(attempt_id)
                if not isinstance(explicit_attempt, dict):
                    raise ActionError(
                        "ownership", f"尝试 {attempt_id} 不存在",
                        {"attempt_id": attempt_id})
                explicit_task = base.get_task(explicit_attempt["task_id"])
                if not isinstance(explicit_task, dict) or \
                        explicit_task.get("status") in TASK_TERMINAL:
                    raise ActionError(
                        "ownership", f"尝试 {attempt_id} 所属任务已终态/不存在",
                        {"attempt_id": attempt_id,
                         "task_id": explicit_attempt.get("task_id")})
                raw_attempts = [explicit_attempt]
            elif task_id is not None:
                explicit_task = base.get_task(task_id)
                if not isinstance(explicit_task, dict) or \
                        explicit_task.get("status") in TASK_TERMINAL:
                    raise ActionError(
                        "ownership", f"任务 {task_id} 已终态/不存在",
                        {"task_id": task_id})
                task_attempts = list(base.list_attempts(
                    task_id=task_id, limit=coverage_limit + 1))
                if len(task_attempts) > coverage_limit:
                    raise ActionError(
                        "ownership_coverage",
                        f"任务 {task_id} 的尝试超过有界查询窗口 "
                        f"{coverage_limit}；为避免漏判归属，请显式指定 attempt_id",
                        {"task_id": task_id, "session_id": session_id,
                         "coverage_limit": coverage_limit,
                         "required": "explicit_attempt_id"})
                raw_attempts = [a for a in task_attempts
                                if isinstance(a, dict)
                                and a.get("session_id") == session_id]
                if not raw_attempts:
                    raise ActionError(
                        "ownership",
                        f"任务 {task_id} 在 session {session_id} 没有尝试；"
                        "拒绝凭 latest 猜测归属",
                        {"task_id": task_id, "session_id": session_id})
            else:
                raw_attempts = list(base.attempts_for_session(
                    session_id, limit=coverage_limit + 1))
                if len(raw_attempts) > coverage_limit:
                    raise ActionError(
                        "ownership_coverage",
                        f"目标 session {session_id} 的尝试超过有界查询窗口 "
                        f"{coverage_limit}，可能截断多个任务；不能静默猜唯一。"
                        "请显式指定 task_id/attempt_id，或选择 untracked 纯通信",
                        {"session_id": session_id,
                         "coverage_limit": coverage_limit,
                         "required": "explicit_task_or_untracked"})
        except ActionError:
            raise
        except Exception as exc:
            raise ActionError(
                "ledger", f"无法读取目标 session 的尝试归属，拒绝托管发送：{exc}",
                {"session_id": session_id}) from exc

        candidates: dict[str, dict] = {}
        for att in raw_attempts:
            if not isinstance(att, dict) or not att.get("task_id"):
                continue
            try:
                task = base.get_task(att["task_id"])
            except Exception:
                continue
            if not isinstance(task, dict) or task.get("status") in TASK_TERMINAL:
                continue
            entry = candidates.setdefault(
                task["id"], {"task": task, "attempts": []})
            entry["attempts"].append(att)

        if not candidates:
            if declared_work == rules.WORK_WRITE:
                raise ActionError(
                    "write_requires_managed_attempt",
                    "目标 session 没有有效非终态任务/尝试，不能声明 write；"
                    "纯 untracked 不授予任务写权",
                    {"session_id": session_id, "untracked_required": True})
            return {
                "tracked": False,
                "boundary": rules.UNTRACKED_BOUNDARY,
                "boundary_source": "no-non-terminal-managed-task",
                "owner": owner or target_session or session_id,
                "owner_source": ("explicit" if owner else
                                 "target_session" if target_session else
                                 "send_session"),
                "target_session": target_session,
                "target_source": "explicit" if target_session else "none",
                "task_id": None, "attempt_id": None,
                "generation": None, "result_version": None,
                "project": "", "work_kind": declared_work,
                "work_kind_source": "explicit" if declared_work else "undeclared",
                "materials": rules.normalize_materials(materials),
                "materials_source": "explicit" if materials is not None else "default",
                "deadline": deadline,
                "deadline_source": "explicit" if deadline is not None else "none",
            }

        if explicit_attempt is not None:
            chosen = candidates.get(explicit_attempt["task_id"])
            if chosen is None:
                raise ActionError(
                    "ownership",
                    f"尝试 {explicit_attempt.get('id')} 所属任务不在目标 session 的"
                    "有效候选中",
                    {"attempt_id": explicit_attempt.get("id"),
                     "session_id": session_id})
            task_source = "explicit-attempt"
        elif task_id is not None:
            chosen = candidates.get(task_id)
            if chosen is None:
                raise ActionError(
                    "ownership",
                    f"目标 session {session_id} 没有任务 {task_id} 的有效非终态尝试；"
                    "拒绝凭 latest 猜测归属",
                    {"session_id": session_id, "task_id": task_id})
            task_source = "explicit-task"
        elif len(candidates) == 1:
            chosen = next(iter(candidates.values()))
            task_source = "single-non-terminal-task"
        else:
            raise ActionError(
                "ambiguous_ownership",
                f"目标 session {session_id} 关联多个非终态任务，发送前必须显式"
                "指定 task_id 或选择 untracked 纯通信模式",
                {"session_id": session_id,
                 "task_ids": sorted(candidates)})

        task = chosen["task"]
        attempts = chosen["attempts"]
        if explicit_attempt is not None:
            att = explicit_attempt
            if att.get("task_id") != task["id"] or \
                    (att.get("session_id") not in (None, session_id)):
                raise ActionError(
                    "ownership",
                    f"尝试 {att.get('id')} 不属于任务 {task['id']} / session "
                    f"{session_id}",
                    {"attempt_id": att.get("id"), "task_id": task["id"],
                     "session_id": session_id})
            attempt_source = "explicit-attempt"
        elif len(attempts) == 1:
            att = attempts[0]
            attempt_source = "single-session-attempt"
        elif task.get("current_attempt_id") in {a.get("id") for a in attempts}:
            att = next(a for a in attempts
                       if a.get("id") == task["current_attempt_id"])
            attempt_source = "task.current_attempt_id"
        else:
            raise ActionError(
                "ambiguous_ownership",
                f"目标 session 对任务 {task['id']} 有多个尝试且 current 不在此 "
                "session；发送前必须显式指定 attempt_id",
                {"session_id": session_id, "task_id": task["id"],
                 "attempt_ids": sorted(a.get("id") for a in attempts)})

        # 停止请求/确认不得靠新输入绕过；已显式释放现场的只读核收另按 observe
        # 规则处理（write 仍拒绝）。
        stop_pending = (
            att.get("site_released_at") is None and (
                att.get("status") == "stop_requested"
                or att.get("stop_requested_at") is not None
                or att.get("stop_confirmed_at") is not None))
        if stop_pending:
            raise ActionError(
                "stopped",
                f"尝试 {att.get('id')} 处于停止请求/确认未释放现场；不能用新输入"
                "绕过停止。先完成停止与写权释放，或经 Orchestration.rework",
                {"task_id": task["id"], "attempt_id": att.get("id"),
                 "status": att.get("status"),
                 "stop_requested_at": att.get("stop_requested_at"),
                 "stop_confirmed_at": att.get("stop_confirmed_at")})

        # work_kind=None 表示未声明：只有 active/current/未释放尝试可继承已有
        # 执行写范围；封存/释放/非 current 必须在发送前明确 observe/read_only
        # 或走 rework，绝不默认成只读或写。
        if declared_work is None:
            inherited = rules.write_send_block_reason(att, task) is None
            if not inherited:
                raise ActionError(
                    "work_kind_required",
                    f"目标尝试 {att.get('id')} 已封存/释放/非 current，未声明 "
                    "work_kind；请显式选择 observe/read_only 做只读核收，或经 "
                    "Orchestration.rework 以新 attempt 重新认领写范围",
                    {"task_id": task["id"], "attempt_id": att.get("id"),
                     "attempt_status": att.get("status"),
                     "current_attempt_id": task.get("current_attempt_id"),
                     "options": ["observe", "read_only"],
                     "rework_required": True})
            effective_work = rules.WORK_WRITE
            work_source = "inherited-current-active-attempt"
        else:
            effective_work = declared_work
            work_source = "explicit"
        if effective_work == rules.WORK_WRITE:
            block = rules.write_send_block_reason(att, task)
            if block is not None:
                raise ActionError(
                    "write_requires_rework",
                    f"已封存/释放/非 current 尝试不能直接写入（{block}）；"
                    "修改交付物必须经 Orchestration.rework 以新 attempt 重新认领"
                    "写范围并封存基线，不能重开旧终态",
                    {"task_id": task["id"], "attempt_id": att.get("id"),
                     "reason": block, "rework_required": True})

        # 接续目标：优先当前 coordinator 租约，其次任务记录；无则发送前拒绝。
        project = task.get("project") or ""
        if not project:
            raise ActionError(
                "ownership", f"任务 {task['id']} 缺少 project，无法登记后续责任；"
                "发送前请修正任务归属",
                {"task_id": task["id"], "session_id": session_id})
        if target_session is not None:
            target, target_source = target_session, "explicit"
        else:
            lease = None
            try:
                lease = base.coordinator_lease(project)
            except Exception:
                lease = None
            holder = lease.get("holder_session") if isinstance(lease, dict) else None
            if isinstance(holder, str) and holder:
                target, target_source = holder, "coordinator_lease"
            elif isinstance(task.get("coordinator_session"), str) \
                    and task.get("coordinator_session"):
                target = task["coordinator_session"]
                target_source = "task.coordinator_session"
            else:
                raise ActionError(
                    "ambiguous_ownership",
                    f"任务 {task['id']} 无 coordinator 租约/协调者，无法确定结果"
                    "接续目标；发送前请显式提供 target_session",
                    {"task_id": task["id"], "session_id": session_id})

        resolved_owner = owner
        owner_source = "explicit" if owner else "target_session"
        resolved_owner = resolved_owner or target
        owner_source = owner_source if owner else "target_session"
        att_deadline = att.get("deadline_at")
        deadline_warning = None
        if deadline is not None:
            resolved_deadline, deadline_source = deadline, "explicit"
        elif isinstance(att_deadline, int) and not isinstance(att_deadline, bool) \
                and att_deadline > now:
            resolved_deadline, deadline_source = att_deadline, "attempt.deadline_at"
        elif isinstance(att_deadline, int) and not isinstance(att_deadline, bool):
            # 已过期 attempt 期限不能阻塞只读核收/新责任；采用合理默认并显式告知
            # 期限需更新，绝不把 task.next_check_at 这类滚动对账字段当处理期限。
            resolved_deadline = now + _DEFAULT_RESPONSIBILITY_DEADLINE_MS
            deadline_source = "default:24h:expired-attempt-deadline"
            deadline_warning = (
                f"attempt.deadline_at={att_deadline} 已过期；本次采用默认 24h "
                "新责任期限，请显式更新 deadline")
        else:
            resolved_deadline = now + _DEFAULT_RESPONSIBILITY_DEADLINE_MS
            deadline_source = "default:24h"
        next_check = min(resolved_deadline, now + _DEFAULT_NEXT_CHECK_MS) \
            if resolved_deadline is not None else now + _DEFAULT_NEXT_CHECK_MS
        return {
            "tracked": True,
            "boundary": None,
            "boundary_source": None,
            "owner": resolved_owner,
            "owner_source": owner_source,
            "target_session": target,
            "target_source": target_source,
            "task_id": task["id"],
            "attempt_id": att.get("id"),
            "generation": att.get("generation"),
            "result_version": self._result_version(att),
            "project": project,
            "work_kind": effective_work,
            "work_kind_source": work_source,
            "inherited_work_scope": bool(declared_work is None),
            "materials": rules.normalize_materials(materials),
            "materials_source": "explicit" if materials is not None else "default",
            "deadline": resolved_deadline,
            "deadline_source": deadline_source,
            "deadline_warning": deadline_warning,
            "next_check_at": next_check,
            "next_check_source": ("default:5m" if next_check == now + _DEFAULT_NEXT_CHECK_MS
                                  else "deadline"),
            "task_source": task_source,
            "attempt_source": attempt_source,
            "attempt_status": att.get("status"),
            "task_status": task.get("status"),
        }

    def _preflight_managed_send(self, session_id: str, *, delivery: str,
                                row: dict | None = None) -> None:
        """发送前复核人工控制/闸门与完整队列；不得借静默降级绕过冷却。"""
        blocked = rules.gateway_allows(self._gate_snapshot(session_id))
        if blocked is not None:
            raise ActionError(
                "gate", f"目标闸门阻止发送（人工确认不绕过）：{blocked}",
                {"session_id": session_id, "reason": blocked,
                 "input_id": (row or {}).get("input_id")})
        if delivery not in rules.DELIVERY_MODES:
            raise ActionError("usage", f"非法 delivery：{delivery!r}",
                              {"session_id": session_id})
        inbox = self._read_inbox(session_id, row or {})
        decision = rules.execute_queue_allowed(
            inbox, self._owned_message_ids(session_id))
        if not decision["allowed"]:
            raise ActionError(
                "queue", f"执行发送会消费队列中的其他输入：{decision['reason']}；"
                "先人工处理完整队列，不新建 prompt 绕过队头",
                {"input_id": (row or {}).get("input_id"),
                 "session_id": session_id, "reason": decision["reason"],
                 "queue": decision.get("detail")})

    def _recheck_current(self, plan: dict, *, session_id: str) -> None:
        """发送前复核任务/尝试 current 与写权边界（不打断/偷写范围）。"""
        if not plan.get("tracked"):
            return
        base = self._base_ledger()
        try:
            task = base.get_task(plan["task_id"])
            att = base.get_attempt(plan["attempt_id"])
        except Exception as exc:
            raise ActionError(
                "ownership", f"发送前无法复核任务/尝试 current：{exc}",
                {"task_id": plan.get("task_id"),
                 "attempt_id": plan.get("attempt_id")}) from exc
        if task.get("status") in TASK_TERMINAL:
            raise ActionError(
                "ownership", f"任务 {task['id']} 已终态 {task.get('status')}，"
                "不能继续登记托管发送", {"task_id": task["id"]})
        if att.get("session_id") not in (None, session_id) or \
                att.get("task_id") != task.get("id"):
            raise ActionError(
                "ownership", "发送前尝试归属已变化，拒绝发送",
                {"task_id": task["id"], "attempt_id": att.get("id")})
        if plan.get("same_candidate") or plan.get("work_kind") == rules.WORK_WRITE:
            block = rules.write_send_block_reason(att, task)
            if block and plan.get("same_candidate"):
                raise ActionError("ownership", f"同候选补充需要当前活动 attempt（{block}）",
                                  {"task_id": task["id"], "attempt_id": att.get("id")})
            if block is not None:
                raise ActionError(
                    "write_requires_rework",
                    f"发送前复核写权失败（{block}）；需经 Orchestration.rework "
                    "重新认领写范围",
                    {"task_id": task["id"], "attempt_id": att.get("id"),
                     "reason": block, "rework_required": True})

    def _open_send_responsibility(self, row: dict, plan: dict, *, now: int) -> dict:
        if self._responsibilities is None:
            raise ActionError(
                "responsibility",
                "托管发送需要后续责任 store；未装配时不得静默降级为纯通信",
                {"input_id": row["input_id"]})
        wait_condition = "managed-input-execution"
        next_action = (
            "等待 worker 执行结束并核收结果；结构化完成/缺材料/需决策交回责任人，"
            "ACK 不关闭本责任" if plan["work_kind"] == rules.WORK_OBSERVE else
            "等待 worker 执行并收集交付证据；验收仍须经 collect/verify，"
            "ACK 不关闭本责任")
        evidence = {
            "task_id": plan["task_id"],
            "attempt_id": plan["attempt_id"],
            "generation": plan["generation"],
            "result_version": plan["result_version"],
            "session_id": row["session_id"],
            "message_id": row["message_id"],
            "input_id": row["input_id"],
            "work_kind": plan["work_kind"],
            "materials": plan["materials"],
            "attribution_sources": {
                "task": plan.get("task_source"),
                "attempt": plan.get("attempt_source"),
                "owner": plan.get("owner_source"),
                "target_session": plan.get("target_source"),
                "deadline": plan.get("deadline_source"),
                "next_check_at": plan.get("next_check_source"),
                "work_kind": plan.get("work_kind_source"),
            },
        }
        try:
            result = self._responsibilities.open_responsibility(
                plan["owner"], plan["project"], plan["task_id"],
                plan["attempt_id"], plan["generation"],
                plan["result_version"], row["input_id"], row["session_id"],
                row["message_id"], plan["target_session"],
                state="waiting_execution", wait_kind="execution",
                wait_condition=wait_condition, next_action=next_action,
                next_check_at=plan["next_check_at"],
                deadline=plan["deadline"],
                condition_key=f"execution-ended:{row['input_id']}",
                evidence=evidence, max_budget=3, actor=plan["owner"])
        except Exception as exc:
            raise ActionError(
                "responsibility",
                f"发送前登记后续责任失败，未发 prompt：{exc}",
                {"input_id": row["input_id"], "task_id": plan["task_id"],
                 "attempt_id": plan["attempt_id"]}) from exc
        rec = self._responsibility_record(result)
        rid = rec.get("responsibility_id") or rec.get("id")
        rev = rec.get("revision")
        if not (isinstance(rid, str) and rid) or \
                not (isinstance(rev, int) and not isinstance(rev, bool) and rev >= 1):
            raise ActionError(
                "responsibility",
                "责任 store 未返回有效 responsibility_id/revision；拒绝继续发送",
                {"input_id": row["input_id"], "result": rec})
        plan["responsibility_id"] = rid
        plan["responsibility_revision"] = rev
        plan["wait_condition"] = wait_condition
        plan["next_action"] = next_action
        return rec

    def _bind_responsibility(self, row: dict, plan: dict, *, now: int) -> dict:
        out = self._ledger.bind_managed_responsibility(
            row["input_id"],
            responsibility_id=plan["responsibility_id"],
            responsibility_revision=plan["responsibility_revision"],
            now=now, wait_condition=plan.get("wait_condition"),
            next_check_at=plan.get("next_check_at"),
            deadline=plan.get("deadline"), actor=plan["owner"])
        return out.get("input") or row

    def _update_responsibility_duty(self, row: dict, *, now: int,
                                    fields: dict) -> dict | None:
        """按输入 tracking 中的责任 ID/revision CAS 更新，并把新 revision 绑回。"""
        tracking = rules.tracking_block(row)
        rid = tracking.get("responsibility_id")
        rev = tracking.get("responsibility_revision")
        if self._responsibilities is None or not isinstance(rid, str) or not rid \
                or not isinstance(rev, int) or isinstance(rev, bool):
            return None
        try:
            payload = dict(fields)
            actor = payload.pop("actor", "system")
            out = self._responsibilities.update_responsibility(
                rid, expected_revision=rev, fields=payload, actor=actor)
        except Exception as exc:
            return {"error": str(exc), "responsibility_id": rid}
        rec = self._responsibility_record(out)
        new_rev = rec.get("revision")
        if isinstance(new_rev, int) and not isinstance(new_rev, bool):
            bound = self._ledger.bind_managed_responsibility(
                row["input_id"], responsibility_id=rid,
                responsibility_revision=new_rev, now=now,
                wait_condition=fields.get("wait_condition"),
                next_check_at=fields.get("next_check_at"),
                deadline=fields.get("deadline"), actor=actor)
            return bound.get("input")
        return rec

    def _receipt(self, view: dict, plan: dict | None) -> dict:
        """send_tracked 统一收据：四态 + 责任接续字段（C 可直接渲染）。"""
        receipt = dict(view)
        if plan is not None:
            receipt.update({
                "tracking": rules.TRACKED if plan.get("tracked") else rules.UNTRACKED,
                "responsibility_id": plan.get("responsibility_id"),
                "target_session": plan.get("target_session"),
                "wait_condition": plan.get("wait_condition"),
                "next_check_at": plan.get("next_check_at"),
                "deadline": plan.get("deadline"),
                "deadline_warning": plan.get("deadline_warning"),
                "task_id": plan.get("task_id"),
                "attempt_id": plan.get("attempt_id"),
                "work_kind": plan.get("work_kind"),
                "inference": {
                    "task": plan.get("task_source"),
                    "attempt": plan.get("attempt_source"),
                    "owner": plan.get("owner_source"),
                    "target_session": plan.get("target_source"),
                    "deadline": plan.get("deadline_source"),
                    "work_kind": plan.get("work_kind_source"),
                },
            })
            if not plan.get("tracked"):
                receipt["boundary"] = plan.get("boundary")
        return receipt

    def send_tracked(
        self, session_id, *, text, message_id=None, owner=None, task_id=None,
        attempt_id=None, target_session=None, deadline=None, materials=None,
        work_kind=None, delivery="queue", untracked=False, request_id=None,
        require_tracked=False, same_candidate=False,
    ) -> dict:
        """向已托管 worker 发送 execute 输入，默认持久登记输入与后续责任。

        ``work_kind=None`` 表示未声明：active/current attempt 可继承已有执行
        写范围；封存/释放/非 current 必须显式 observe/read_only 或 rework。
        无托管任务/显式 untracked 时保留原 send 纯通信兼容，但返回
        ``tracking='untracked'`` 与明确 boundary，绝不授予任务写权/静默降级。
        归属歧义、闸门/队列/current 冲突、封存后写发送均在 prompt 前拒绝。
        ``request_id`` 仅供旧 send 委托时保留原幂等键；显式 message_id 优先。
        """
        if request_id is not None and (not isinstance(request_id, str)
                                       or not request_id.strip()):
            raise ActionError("usage", "request_id 必须是非空字符串/None",
                              {"session_id": session_id})
        check = rules.validate_tracked_send(
            session_id, text=text, delivery=delivery, work_kind=work_kind,
            untracked=untracked, task_id=task_id, attempt_id=attempt_id,
            target_session=target_session, owner=owner, message_id=message_id,
            deadline=deadline, materials=materials)
        if not check["valid"]:
            raise ActionError("usage", check["reason"], {"session_id": session_id})
        now = self.clock.now_ms()
        declared_work = rules.normalize_work_kind(work_kind)

        if untracked:
            target = target_session
            plan = {
                "tracked": False,
                "boundary": rules.UNTRACKED_BOUNDARY,
                "boundary_source": "explicit-untracked",
                "owner": owner or target or session_id,
                "owner_source": ("explicit" if owner else
                                 "target_session" if target else "send_session"),
                "target_session": target,
                "target_source": "explicit" if target else "none",
                "task_id": None, "attempt_id": None, "generation": None,
                "result_version": None, "project": "",
                "work_kind": declared_work,
                "work_kind_source": ("explicit" if declared_work
                                     else "undeclared"),
                "materials": rules.normalize_materials(materials),
                "materials_source": "explicit" if materials is not None else "default",
                "deadline": deadline,
                "deadline_source": "explicit" if deadline is not None else "none",
            }
        else:
            plan = self._resolve_tracking_plan(
                session_id, owner=owner, task_id=task_id,
                attempt_id=attempt_id, target_session=target_session,
                deadline=deadline, materials=materials, work_kind=work_kind,
                now=now)
            if require_tracked and not plan.get("tracked"):
                raise ActionError(
                    "ownership",
                    "目标已无有效非终态托管 task；按系统 send 默认追踪要求拒绝"
                    "静默降级纯通信。请显式 untracked 或重新确认任务归属",
                    {"session_id": session_id, "required": "tracked",
                     "untracked_required": True})

        if same_candidate:
            if not attempt_id or not plan.get("tracked") or untracked:
                raise ActionError("usage", "--same-candidate 需要显式当前 --attempt 和托管执行输入",
                                  {"session_id": session_id})
            plan["same_candidate"] = True
        tracking = self._tracking_payload(plan)
        idem_key = (request_id
                    or (f"{_TRACKED_REQUEST_PREFIX}{message_id}"
                        if message_id
                        else f"{_TRACKED_REQUEST_PREFIX}{uuid4().hex[:16]}"))

        # 回执幂等：同一原 message_id/request_id 已有非 prepared 事实时只返回
        # 原回执，不重做 preflight（原消息可能仍在队列）也不新 POST。
        existing = None
        if message_id:
            existing = self._find_managed_by_message(session_id, message_id)
        if existing is None and request_id:
            existing = self._find_managed_by_request(session_id, request_id)
        if existing is not None:
            if (existing.get("body") != text
                    or existing.get("owner") != plan["owner"]
                    or not self._same_tracking_identity(
                        rules.tracking_block(existing), tracking)):
                raise LedgerError(
                    "REQUEST_ID_CONFLICT",
                    "同一原 message_id/request_id 已记录不同正文/owner/归属；"
                    "不换身份复用或重投",
                    {"input_id": existing["input_id"]})
            if existing.get("admission_state") != "prepared":
                return self._duplicate_receipt(existing, plan, now=now)

        # 预检在任何输入/责任持久化之前；随后再复核一次，避免长窗口漂移。
        self._preflight_managed_send(session_id, delivery=delivery)
        self._recheck_current(plan, session_id=session_id)

        prepared = self._ledger.prepare_managed_input(
            session_id, intent=rules.INTENT_EXECUTE, text=text,
            owner=plan["owner"], request_id=idem_key,
            project=plan.get("project") or "", now=now,
            message_id=message_id, tracking=tracking)
        row = prepared["input"]
        created = bool(prepared.get("created"))
        if not created:
            admission = row.get("admission_state")
            if admission != "prepared":
                view = rules.receipt_view(row, now=now, include_body=True)
                tracking_row = rules.tracking_block(row)
                return {"ok": True, "created": False, "duplicate": True,
                        "tracking": (rules.TRACKED if plan.get("tracked")
                                     else rules.UNTRACKED),
                        "tracked": bool(plan.get("tracked")),
                        "resume": True,
                        "reason": "already-prepared-or-sent",
                        "input": view, "receipt": self._receipt(view, {
                            **plan,
                            "wait_condition": tracking_row.get(
                                "wait_condition"),
                            "responsibility_id": tracking_row.get(
                                "responsibility_id"),
                            "next_check_at": tracking_row.get(
                                "next_check_at"),
                        })}

        # prepare 后再次复核完整队列/闸门与 current；失败不登记新责任、不发 prompt。
        self._preflight_managed_send(session_id, delivery=delivery, row=row)
        self._recheck_current(plan, session_id=session_id)

        if plan.get("tracked"):
            resp = self._open_send_responsibility(row, plan, now=now)
            plan["responsibility"] = resp
            row = self._bind_responsibility(row, plan, now=now)
        else:
            plan["responsibility_id"] = None
            plan["wait_condition"] = None
            plan["next_check_at"] = None

        claim = self._claim(row, now, operation="send", owner=plan["owner"],
                            expected_revision=row["revision"],
                            reason=(f"managed-send-tracked:{plan['work_kind']}"
                                    if plan.get("tracked")
                                    else "managed-send-untracked"))
        if not claim.get("changed"):
            if plan.get("tracked"):
                self._update_responsibility_duty(row, now=now, fields={
                    "state": "waiting_dependency",
                    "wait_kind": "send_claim_unresolved",
                    "wait_condition": plan.get("wait_condition"),
                    "next_action": "发送 claim 未取得（存在未收敛操作）；保原 ID "
                                   "对账，不重投",
                    "next_check_at": now + _DEFAULT_NEXT_CHECK_MS,
                    "deadline": plan.get("deadline"),
                    "actor": plan["owner"]})
            raise ActionError(
                "unknown_response",
                "发送未取得 claim（存在未收敛操作）；保原输入/责任身份，不换 ID 重投",
                {"input_id": row["input_id"], "session_id": session_id,
                 "message_id": row.get("message_id"),
                 "responsibility_id": plan.get("responsibility_id"),
                 "reason": claim.get("reason")})
        row = claim["input"]
        claim_rev = claim.get("revision")

        # claim 后、prompt 前最后一次 current/写权复核；失败按 unknown 保留原身份，
        # 绝不发出一条基于旧 current 的托管写/执行输入。
        try:
            self._recheck_current(plan, session_id=session_id)
        except ActionError as exc:
            row = self._record(row, now, indicator="unknown",
                               via="send-tracked:current-changed",
                               operation="send", operation_revision=claim_rev)
            if plan.get("tracked"):
                self._update_responsibility_duty(row, now=now, fields={
                    "state": "waiting_human",
                    "wait_kind": "ownership_changed",
                    "wait_condition": f"task:{plan.get('task_id')}:current",
                    "next_action": "发送前 current/写权已变化，未发 prompt；"
                                   "重新确认归属或经 rework 后重试",
                    "next_check_at": now + _DEFAULT_NEXT_CHECK_MS,
                    "deadline": plan.get("deadline"),
                    "actor": plan.get("owner", "system")})
            raise

        authorize_input(self._base_ledger(), self._client(), self.clock, row,
                        operation="prompt", owner=plan["owner"])
        try:
            self._client().prompt(
                session_id, message_id=row["message_id"],
                text=rules.execution_prompt(row), delivery=delivery,
                resume=True)
        except Exception as exc:
            row = self._record(row, now, indicator="unknown",
                               via="send-tracked:prompt-error",
                               operation="send", operation_revision=claim_rev)
            if plan.get("tracked"):
                self._update_responsibility_duty(row, now=now, fields={
                    "state": "waiting_dependency",
                    "wait_kind": "send_result_unknown",
                    "wait_condition": plan.get("wait_condition"),
                    "next_action": "发送结果未知：按原 session/message/责任身份"
                                   "对账；不换 ID 重投，超期转人工",
                    "next_check_at": now + _DEFAULT_NEXT_CHECK_MS,
                    "deadline": (plan.get("deadline")
                                 or now + _DEFAULT_RESPONSIBILITY_DEADLINE_MS),
                    "evidence": {"send_unknown_at": now,
                                 "via": "send-tracked:prompt-error"},
                    "actor": plan["owner"]})
            raise ActionError(
                "unknown_response",
                "发送结果未知，已保原输入/message_id/责任 ID 供对账（不换 ID 重投）",
                {"input_id": row["input_id"], "session_id": session_id,
                 "message_id": row.get("message_id"),
                 "responsibility_id": plan.get("responsibility_id")}) from exc
        row = self._record(row, now, indicator="admission",
                           via="send-tracked:prompt", operation="send",
                           operation_revision=claim_rev)
        view = rules.receipt_view(row, now=now, include_body=True)
        return {"ok": True, "created": True, "duplicate": False,
                "tracking": (rules.TRACKED if plan.get("tracked")
                             else rules.UNTRACKED),
                "tracked": bool(plan.get("tracked")),
                "resume": True,
                "input": view,
                "receipt": self._receipt(view, plan),
                "responsibility": plan.get("responsibility")}

    # ---- send --------------------------------------------------------------
    def send(self, session_id, *, intent, text, owner, request_id,
             project="", expires_at=None, flush_condition=None,
             task_id=None, attempt_id=None, target_session=None,
             deadline=None, materials=None, work_kind=None,
             untracked=False, same_candidate=False) -> dict:
        """显式 intent 共享入口；execute 对已托管 task 必须委托 send_tracked。

        - execute + 明确托管 task：不要求白名单，登记输入与后续责任；
        - execute + 无托管 task：保留显式 owner/watch 外部分支，但 tracking
          明确 untracked/non-task 边界，不静默宣称 task 托管；
        - park + 托管 task：保存 task/attempt/target/work_kind tracking，flush
          转 execute 时再登记后续责任；封存/停止写权在 park/flush 前拒绝；
        - record 仍纯本地留档。
        ``request_id`` 幂等与原 message 身份在委托 send_tracked 时保留。
        """
        check = rules.validate_send_payload(
            session_id, intent=intent, text=text, owner=owner,
            request_id=request_id, project=project, expires_at=expires_at,
            flush_condition=flush_condition)
        if not check["valid"]:
            raise ActionError("usage", check["reason"], {"session_id": session_id})
        if same_candidate:
            if intent != rules.INTENT_EXECUTE or untracked or not attempt_id:
                raise ActionError("usage", "--same-candidate 需要托管 execute 和显式当前 --attempt",
                                  {"session_id": session_id})
            return self.send_tracked(
                session_id, text=text, owner=owner, request_id=request_id,
                task_id=task_id, attempt_id=attempt_id, target_session=target_session,
                deadline=deadline, materials=materials, work_kind=work_kind,
                require_tracked=True, same_candidate=True)
        if untracked and intent != rules.INTENT_EXECUTE:
            raise ActionError(
                "usage", "untracked 仅适用于 execute 纯通信；park/record 不支持",
                {"session_id": session_id, "intent": intent})
        now = self.clock.now_ms()

        # execute 显式 untracked：纯通信，不需白名单/任务归属。
        if intent == rules.INTENT_EXECUTE and untracked:
            return self.send_tracked(
                session_id, text=text, owner=owner, request_id=request_id,
                task_id=task_id, attempt_id=attempt_id,
                target_session=target_session, deadline=deadline,
                materials=materials, work_kind=work_kind,
                untracked=True)

        plan = None
        if intent in (rules.INTENT_EXECUTE, rules.INTENT_PARK):
            plan = self._resolve_tracking_plan(
                session_id, owner=owner, task_id=task_id,
                attempt_id=attempt_id, target_session=target_session,
                deadline=deadline, materials=materials,
                work_kind=work_kind, now=now)

        # 已托管 task 的 execute 一律委托统一追踪入口，绝不走旧白名单静默执行。
        if intent == rules.INTENT_EXECUTE and plan is not None \
                and plan.get("tracked"):
            return self.send_tracked(
                session_id, text=text, owner=owner, request_id=request_id,
                task_id=task_id, attempt_id=attempt_id,
                target_session=target_session, deadline=deadline,
                materials=materials, work_kind=work_kind, untracked=False,
                require_tracked=True)

        # 旧外部分支（无托管 task）仍要求显式 owner/watch；tracking 明确标注
        # untracked/非 task 边界，输入核收责任归 owner。已托管 task 的 park 也不
        # 依赖 watch，避免白名单把托管 worker 默认拒绝。
        if intent != rules.INTENT_RECORD and not (plan and plan.get("tracked")):
            self._require_owned_target(session_id, owner)
        tracking = None
        if plan is not None:
            tracking = self._tracking_payload(plan)
            if not plan.get("tracked"):
                tracking["boundary"] = getattr(
                    rules, "EXTERNAL_INPUT_BOUNDARY",
                    "external-owner-watch: 非任务托管外部分支；核收责任归 owner")
        prepared = self._ledger.prepare_managed_input(
            session_id, intent=intent, text=text, owner=owner,
            request_id=request_id, project=project, expires_at=expires_at,
            flush_condition=flush_condition, now=now, tracking=tracking)
        row = prepared["input"]
        created = bool(prepared.get("created"))
        resume = rules.RESUME_BY_INTENT[intent]
        if not created:
            # 重复 request_id：返回原回执，不新 POST、不隐式重试。
            return {"ok": True, "created": False, "duplicate": True,
                    "resume": resume,
                    "input": rules.receipt_view(row, now=now, include_body=True)}
        if intent == rules.INTENT_RECORD:
            # 纯本地留档：全程不构造 Client、不读目标。
            return {"ok": True, "created": True, "resume": resume,
                    "input": rules.receipt_view(row, now=now, include_body=True)}
        if intent == rules.INTENT_EXECUTE:
            # 执行会 resume 并消费队列：先核对闸门与完整 inbox，非空即拒绝。
            blocked = rules.gateway_allows(self._gate_snapshot(session_id))
            if blocked is not None:
                raise ActionError(
                    "gate", f"目标闸门阻止 execute：{blocked}；先人工处理，不新建 prompt 绕过",
                    {"input_id": row["input_id"], "session_id": session_id,
                     "reason": blocked})
            inbox = self._read_inbox(session_id, row)
            decision = rules.execute_queue_allowed(
                inbox, self._owned_message_ids(session_id))
            if not decision["allowed"]:
                raise ActionError(
                    "queue", f"execute 会消费队列中的其他输入：{decision['reason']}；"
                    "先人工处理队列，不新建 prompt 绕过队头",
                    {"input_id": row["input_id"], "session_id": session_id,
                     "reason": decision["reason"], "queue": decision.get("detail")})
        # claim send（CAS 一次赢家）：只有 changed 赢家可做外部 POST。
        claim = self._claim(row, now, operation="send", owner=owner,
                            expected_revision=row["revision"],
                            reason=f"managed-send:{intent}")
        if not claim.get("changed"):
            raise ActionError(
                "unknown_response",
                "发送未取得 claim（存在未收敛操作）；保原输入身份，不换 ID 重投",
                {"input_id": row["input_id"], "session_id": session_id,
                 "reason": claim.get("reason")})
        row = claim["input"]
        claim_rev = claim.get("revision")
        authorize_input(self._base_ledger(), self._client(), self.clock, row,
                        operation="prompt", owner=owner)
        try:
            self._client().prompt(session_id, message_id=row["message_id"],
                                  text=row.get("body") or text, delivery="queue",
                                  resume=resume)
        except Exception as exc:  # 响应未知：保原 message_id，不换 ID 重投。
            row = self._record(row, now, indicator="unknown",
                               via=f"send:{intent}:prompt-error", operation="send",
                               operation_revision=claim_rev)
            raise ActionError(
                "unknown_response",
                "发送结果未知，已保原输入与 message_id 供对账（不换 ID 重投）",
                {"input_id": row["input_id"], "session_id": session_id,
                 "message_id": row.get("message_id")}) from exc
        row = self._record(row, now, indicator="admission",
                           via=f"send:{intent}:prompt", operation="send",
                           operation_revision=claim_rev)
        return {"ok": True, "created": True, "resume": resume,
                "tracking": (rules.TRACKED if tracking and tracking.get("tracked")
                             else rules.UNTRACKED if tracking else None),
                "input": rules.receipt_view(row, now=now, include_body=True)}

    def _read_inbox(self, session_id: str, row: dict) -> list:
        """事务外读取完整目标 inbox；查询失败≠空队列（fail-closed 拒绝）。"""
        try:
            return list(self._client().inbox(session_id))
        except Exception as exc:
            raise ActionError(
                "queue", f"inbox 查询失败不能视为空队列，拒绝继续（原输入保留）：{exc}",
                {"input_id": row.get("input_id"), "session_id": session_id}) from exc

    # ---- 读取 --------------------------------------------------------------
    def get(self, input_id) -> dict:
        row = self._ledger.get_managed_input(input_id)
        if row is None:
            raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
        return {"ok": True, "input": rules.receipt_view(
            row, now=self.clock.now_ms(), include_body=True)}

    def list_inputs(self, *, session_id=None, limit=100) -> dict:
        managed = self._ledger.list_managed_inputs(
            session_id=session_id, limit=limit)
        legacy = self._ledger.list_legacy_inputs(
            session_id=session_id, limit=limit)
        rows = sorted(managed + legacy,
                      key=lambda r: (r.get("created_at") or 0,
                                     r.get("input_id") or ""))[:limit]
        now = self.clock.now_ms()
        return {"ok": True,
                "inputs": [rules.receipt_view(r, now=now) for r in rows]}

    def list_legacy(self, *, session_id=None, limit=100) -> dict:
        """独立只读 legacy-receipt 列表；C 可显式展示，不自动重放。"""
        rows = self._ledger.list_legacy_inputs(
            session_id=session_id, limit=limit)
        now = self.clock.now_ms()
        return {"ok": True, "readonly": True,
                "inputs": [rules.receipt_view(r, now=now) for r in rows]}

    _RECONCILE_BACKOFF_MS = 60 * 1000

    def reconcile_registered_inputs_page(self, *, after_id=None,
                                         limit=100) -> dict:
        """有界游标页：按原 ID 安全对账已登记 managed 输入（不 flush/cancel）。

        - 每行 60s 持久退避（last_checked_at/observed_at；首次无事实立即观察），
          查询成功与失败都 note_managed_checked 节流，避免 C 执行窗口期间反复
          2×GET；游标始终前进（keyset page coverage 按 len(rows) 计）。
        - C 持久 ``next_after_id`` 作为公平轮转游标；不重放 legacy、不新 prompt。
        """
        rows = self._ledger.list_pending_managed_inputs_page(
            after_id=after_id, limit=limit)
        effective = (min(limit, 500) if isinstance(limit, int)
                     and not isinstance(limit, bool) and limit >= 1 else 100)
        now = self.clock.now_ms()
        out = []
        last_id = after_id
        checked = 0
        skipped = 0
        for row in rows:
            input_id = row.get("input_id")
            last_id = input_id or last_id
            meta = row.get("metadata") or {}
            block = meta.get("managed_input") if isinstance(meta, dict) else {}
            last_checked = None
            if isinstance(block, dict):
                last_checked = block.get("last_checked_at")
            if last_checked is None:
                last_checked = row.get("observed_at")
            if isinstance(last_checked, int) and not isinstance(last_checked, bool) \
                    and now - last_checked < self._RECONCILE_BACKOFF_MS:
                skipped += 1
                out.append({"input_id": input_id, "skipped": True,
                            "reason": "observed-backoff",
                            "last_checked_at": last_checked})
                continue
            checked += 1
            try:
                res = self.reconcile(input_id)
                out.append(res.get("input") or res)
            except Exception as exc:  # 单条失败不伪装成功；游标仍前进且持久节流
                out.append({"input_id": input_id, "ok": False,
                            "error": str(exc)})
            finally:
                try:
                    self._ledger.note_managed_checked(input_id, now=now)
                except Exception:
                    pass
        return {"ok": True, "inputs": out, "count": len(rows),
                "checked": checked, "skipped": skipped,
                "next_after_id": last_id,
                "has_more": len(rows) >= effective}

    # ---- reconcile ---------------------------------------------------------
    def reconcile(self, input_id) -> dict:
        row = self._ledger.get_managed_input(input_id)
        if row is None:
            raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
        now = self.clock.now_ms()
        if row.get("source") == "legacy-receipt" or \
                (row.get("metadata") or {}).get("legacy_receipt"):
            # 历史回执只按原 ID 查证；不写证据、不伪造 owner/托管，不重放。
            obs = self._read_delivery(row)
            view = rules.receipt_view(row, now=now, include_body=True)
            view["observed_delivery"] = {
                None: "pending-or-unknown",
                "delivery": "delivered",
                "unknown": "unknown",
            }.get(obs.get("indicator"), "unknown")
            view["observed_via"] = obs.get("via")
            return {"ok": True, "readonly": True, "input": view,
                    "observed_delivery": view["observed_delivery"],
                    "observed": obs}
        row = self._require_managed(row, input_id)
        if row.get("intent") == rules.INTENT_RECORD:
            return {"ok": True, "input": rules.receipt_view(
                row, now=now, include_body=True)}
        if self._persisted_delivery(row) == rules.DELIVERY_CANCELLED:
            # 已可信取消：终态不因再次对账被翻回。
            return {"ok": True, "input": rules.receipt_view(
                row, now=now, include_body=True)}
        observation = self._read_delivery(row)
        if observation["indicator"]:
            row = self._record(row, now, indicator=observation["indicator"],
                               via=observation["via"], operation="send")
        return {"ok": True, "input": rules.receipt_view(
            row, now=now, include_body=True)}

    def _persisted_delivery(self, row: dict) -> str:
        return rules.delivery_state(row)

    @staticmethod
    def _trusted_user_message(msg: Any, *, session_id: str,
                              message_id: str) -> bool:
        """严格 get_message 证据门：原 ID、type=user、正整数 created。

        scoped GET 已固定 session；user 对象可无 ``sessionID``，但若存在则必须
        匹配。旧 session outcome、同 ID 的非 user/损坏对象、缺失/零/负数时间都
        不证明本输入已 delivered。
        """
        if not isinstance(msg, dict):
            return False
        if msg.get("id") != message_id or msg.get("type") != "user":
            return False
        # 真实 V2 scoped GET /api/session/{sid}/message/{mid} 的 user 对象可能不含
        # sessionID（本会话实测为 {id,metadata,time,text,type}）。URL 已固定 sid；
        # 字段缺失可接受，但存在时必须严格匹配，避免跨 session 伪造。
        if "sessionID" in msg and msg.get("sessionID") != session_id:
            return False
        time_obj = msg.get("time")
        created = time_obj.get("created") if isinstance(time_obj, dict) else None
        return (isinstance(created, int) and not isinstance(created, bool)
                and created > 0)

    def _read_delivery(self, row: dict) -> dict:
        """定点读原 ID：inbox→pending，可信 user message→delivered，否则 unknown。

        查询失败不当作空/已投递；只有严格校验通过的 user message 才确认
        delivered。inbox 消失且无可信投递/取消证据只保持 unknown，绝不猜 cancelled。
        """
        session_id, message_id = row["session_id"], row.get("message_id")
        in_inbox: bool | None = None
        try:
            in_inbox = any((i or {}).get("id") == message_id
                           for i in self._client().inbox(session_id))
        except Exception:
            in_inbox = None
        msg_state = "error"
        if message_id:
            try:
                msg = self._client().get_message(session_id, message_id)
                if msg is None:
                    msg_state = "absent"
                elif self._trusted_user_message(
                        msg, session_id=session_id, message_id=message_id):
                    msg_state = "found"
                else:
                    msg_state = "invalid"
            except Exception:
                msg_state = "error"
        if in_inbox:
            return {"indicator": None, "via": "reconcile:in-inbox"}
        if msg_state == "found":
            return {"indicator": "delivery", "via": "reconcile:user-message"}
        if in_inbox is False and msg_state in ("absent", "invalid"):
            # 两者均可信且均无有效投递证据 → 保持 unknown（不伪造 delivered/cancelled）。
            return {"indicator": "unknown", "via": "reconcile:absent"}
        return {"indicator": None, "via": "reconcile:untrusted"}

    def recover_pending_execute(self, input_id, *, grace_ms) -> dict:
        """原 ID 有限恢复：规格允许的超时 execute 用 PATCH steer 原消息唤醒。

        只 task-tracked 且 effective execute（send_tracked 或 park 已 flush）：
        - 超过 grace、REST 确认目标 inactive、原 ID 仍在 inbox 且唯一自有目标；
        - target gate/current/task pause/cancel/stop/封存 write/未知 send/flush
          claim 均 fail-closed（blocked + owner 下一动作），不新建 prompt；
        - 成功后 mark_managed_flushed 持久（跨重启不再重复 PATCH）；未知结果
          只记 unknown（claim 不再收敛，也不重 patch）；
        - 刻意 park/foreign/其它自排队 不自动 flush。
        """
        if not isinstance(grace_ms, int) or isinstance(grace_ms, bool) or grace_ms < 0:
            raise ActionError("usage", "grace_ms 必须是非负整数", {"input_id": input_id})
        row = self._ledger.get_managed_input(input_id)
        if row is None:
            raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
        row = self._require_managed(row, input_id)
        now = self.clock.now_ms()
        owner = row.get("owner")
        session_id, message_id = row.get("session_id"), row.get("message_id")

        def blocked(reason: str, extra=None):
            detail = {"input_id": input_id, "session_id": session_id,
                      "owner": owner, "reason": reason,
                      "next_action": "转人工诊断/对账：不自动新建 prompt，不绕停止/封存写权"}
            if extra:
                detail.update(extra)
            return {"ok": True, "recovered": False, "blocked": True,
                    "reason": reason, **detail,
                    "input": rules.receipt_view(row, now=now, include_body=True)}

        tracking = rules.tracking_block(row)
        block = rules._managed_block(row) if hasattr(rules, "_managed_block") else {}
        if not block:
            return blocked("tracking-corrupt")
        if not tracking.get("tracked"):
            return blocked("not-task-tracked")
        flush = block.get("flush")
        effective = (flush.get("effective_intent") if isinstance(flush, dict)
                     else None) or row.get("intent")
        if effective != rules.INTENT_EXECUTE:
            return blocked("not-effective-execute")
        if isinstance(flush, dict) and flush.get("flushed_at") is not None:
            return blocked("already-recovered",
                           {"flushed_at": flush.get("flushed_at")})
        # 宽限锚点用首次事实（admitted/possible/prepared/created），重复观察
        # 绝不推迟；observed_at/updated_at 每轮对账刷新，不能作为宽限依据。
        start_at = (row.get("admitted_at") or row.get("possible_at")
                    or row.get("prepared_at") or row.get("created_at") or 0)
        age = now - (start_at or 0)
        if age <= grace_ms:
            return blocked("within-grace", {"age_ms": age, "grace_ms": grace_ms})
        try:
            gate_block = rules.gateway_allows(self._gate_snapshot(session_id))
        except ActionError as exc:
            return blocked(f"gate:{exc.code}", {"error": str(exc)})
        if gate_block is not None:
            return blocked(gate_block)
        if not self._target_inactive(session_id):
            return blocked("target-active")
        try:
            base = self._base_ledger()
            task = base.get_task(tracking.get("task_id"))
            if task.get("status") == "paused":
                return blocked("task-paused", {"task_status": task.get("status")})
            plan = self._resolve_tracking_plan(
                session_id, owner=owner, task_id=tracking.get("task_id"),
                attempt_id=tracking.get("attempt_id"),
                target_session=tracking.get("target_session"),
                deadline=None, materials=None,
                work_kind=tracking.get("work_kind"), now=now)
            self._recheck_current(plan, session_id=session_id)
        except ActionError as exc:
            return blocked(exc.code, {"error": str(exc)})
        except Exception as exc:
            return blocked("current-check-failed", {"error": str(exc)})
        try:
            queue_decision = rules.flush_queue_allowed(
                self._read_inbox(session_id, row),
                self._owned_message_ids(session_id), message_id)
        except ActionError as exc:
            return blocked(f"queue:{exc.code}", {"error": str(exc)})
        if not queue_decision["allowed"]:
            return blocked(f"queue:{queue_decision['reason']}",
                           {"queue": queue_decision.get("detail")})
        claim = self._claim(row, now, operation="flush", owner=owner,
                            expected_revision=row["revision"],
                            reason="recover-pending-execute")
        if not claim.get("changed"):
            return blocked("operation-unresolved",
                           {"reason": claim.get("reason")})
        row = claim["input"]
        claim_rev = claim.get("revision")
        # 紧邻副作用前最后一次复核队列与 active（不新建 prompt 逃过 race）。
        try:
            gate_block = rules.gateway_allows(self._gate_snapshot(session_id))
            if gate_block is not None:
                raise ActionError("gate", gate_block, {"session_id": session_id})
            if not self._target_inactive(session_id):
                raise ActionError("active", "目标恢复时已进入活跃",
                                  {"session_id": session_id})
            again = self._read_inbox(session_id, row)
            q2 = rules.flush_queue_allowed(
                again, self._owned_message_ids(session_id), message_id)
            if not q2["allowed"]:
                raise ActionError("queue", q2["reason"],
                                  {"queue": q2.get("detail")})
            self._recheck_current(plan, session_id=session_id)
        except Exception as exc:
            row = self._record(row, now, indicator="unknown",
                               via="recover:final-recheck-failed",
                               operation="flush", operation_revision=claim_rev)
            return blocked("final-recheck-failed", {"error": str(exc)})
        try:
            authorize_input(self._base_ledger(), self._client(), self.clock, row,
                            operation="steer", owner=owner)
            self._client().update_inbox_delivery(
                session_id, message_id, delivery="steer")
        except Exception as exc:
            row = self._record(row, now, indicator="unknown",
                               via="recover:patch-unknown",
                               operation="flush", operation_revision=claim_rev)
            return blocked("patch-unknown", {"error": str(exc)})
        marked = self._ledger.mark_managed_flushed(
            input_id, now=now, flush_operation_revision=claim_rev,
            effective_intent=rules.INTENT_EXECUTE, effective_resume=True,
            actor=owner)
        row = marked.get("input") or row
        row = self._record(row, now, indicator="admission",
                           via="recover:steer", operation="flush",
                           operation_revision=claim_rev)
        view = rules.receipt_view(row, now=now, include_body=True)
        return {"ok": True, "recovered": True, "blocked": False,
                "reason": "recovered-steer-original-id",
                "input_id": input_id, "session_id": session_id,
                "owner": owner, "responsibility_id": tracking.get(
                    "responsibility_id"),
                "input": view}

    def _target_inactive(self, session_id: str) -> bool:
        """REST active_sessions 确认目标不在活动集合；查询失败 fail-closed。"""
        try:
            active = self._client().active_sessions()
        except Exception:
            return False
        if not isinstance(active, dict):
            return False
        return session_id not in active
    def flush(self, input_id, *, owner, confirm, expected_revision, reason) -> dict:
        check = rules.validate_confirmation(
            object_id=input_id, confirm=confirm, owner=owner,
            expected_revision=expected_revision, reason=reason)
        if not check["valid"]:
            raise ActionError("usage", check["reason"], {"input_id": input_id})
        row = self._ledger.get_managed_input(input_id)
        if row is None:
            raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
        row = self._require_managed(row, input_id)
        if row.get("intent") == rules.INTENT_RECORD:
            raise ActionError("usage", "record 仅留档，不支持 flush",
                              {"input_id": input_id})
        now = self.clock.now_ms()
        tracking = rules.tracking_block(row)
        plan = None
        if bool(tracking.get("tracked")) and tracking.get("task_id"):
            stored_deadline = tracking.get("deadline")
            deadline_arg = (stored_deadline
                            if isinstance(stored_deadline, int)
                            and not isinstance(stored_deadline, bool)
                            and stored_deadline > now else None)
            plan = self._resolve_tracking_plan(
                row["session_id"], owner=owner,
                task_id=tracking.get("task_id"),
                attempt_id=tracking.get("attempt_id"),
                target_session=tracking.get("target_session"),
                deadline=deadline_arg,
                materials=tracking.get("materials"),
                work_kind=tracking.get("work_kind"), now=now)
            # park → flush 转 execute 前复核停止/封存/写权与 current。
            self._recheck_current(plan, session_id=row["session_id"])
        # 闸门 + 完整队列在 claim 之前 fail-closed（不借人工 flag 绕过、不 STUCK claim）。
        blocked = rules.gateway_allows(self._gate_snapshot(row["session_id"]))
        if blocked is not None:
            raise ActionError(
                "gate", f"目标闸门阻止 flush（人工确认不绕过）：{blocked}",
                {"input_id": input_id, "session_id": row["session_id"],
                 "reason": blocked})
        inbox = self._read_inbox(row["session_id"], row)
        decision = rules.flush_queue_allowed(
            inbox, self._owned_message_ids(row["session_id"]),
            row.get("message_id"))
        if not decision["allowed"]:
            if decision["reason"] == "target-absent":
                # 无内容可 steer：按原 ID 复核投递，不假装执行、不 PATCH。
                if self._persisted_delivery(row) != rules.DELIVERY_DELIVERED:
                    obs = self._read_delivery(row)
                    if obs["indicator"]:
                        row = self._record(row, now, indicator=obs["indicator"],
                                           via=f"flush:absent:{obs['via']}",
                                           operation="send")
                return {"ok": True, "changed": False, "reason": "target-absent",
                        "input": rules.receipt_view(row, now=now, include_body=True)}
            raise ActionError(
                "queue", f"flush 要求完整队列可信且唯一自有目标，冲突：{decision['reason']}；"
                "foreign/其他 park/未知项 fail-closed，先人工处置",
                {"input_id": input_id, "session_id": row["session_id"],
                 "reason": decision["reason"], "queue": decision.get("detail")})
        claim = self._claim(row, now, operation="flush", owner=owner,
                            expected_revision=expected_revision, reason=reason)
        if not claim.get("changed"):
            return self._noop_unknown(claim.get("input") or row, now)
        row = claim["input"]
        claim_rev = claim.get("revision")
        if plan is not None:
            # claim 后、PATCH 前最终复核；失败不触发 execute。
            try:
                self._recheck_current(plan, session_id=row["session_id"])
            except ActionError:
                row = self._record(row, now, indicator="unknown",
                                   via="flush:current-changed",
                                   operation="flush",
                                   operation_revision=claim_rev)
                raise
            try:
                resp = self._open_send_responsibility(row, plan, now=now)
                plan["responsibility"] = resp
                row = self._bind_responsibility(row, plan, now=now)
            except ActionError:
                # 责任登记失败绝不继续 PATCH；保原输入/操作可见。
                row = self._record(row, now, indicator="unknown",
                                   via="flush:responsibility-failed",
                                   operation="flush",
                                   operation_revision=claim_rev)
                raise
        authorize_input(self._base_ledger(), self._client(), self.clock, row,
                        operation="steer", owner=owner)
        try:
            self._client().update_inbox_delivery(
                row["session_id"], row["message_id"], delivery="steer")
        except Exception as exc:  # 结果未知：保原操作/message_id，不重复 PATCH。
            row = self._record(row, now, indicator="unknown",
                               via="flush:patch-unknown", operation="flush",
                               operation_revision=claim_rev)
            if plan is not None:
                self._update_responsibility_duty(row, now=now, fields={
                    "state": "waiting_dependency",
                    "wait_kind": "flush_result_unknown",
                    "wait_condition": plan.get("wait_condition"),
                    "next_action": "flush(PATCH steer) 结果未知：按原 ID 对账，"
                                   "不重复 PATCH；超期转人工",
                    "next_check_at": now + _DEFAULT_NEXT_CHECK_MS,
                    "deadline": (plan.get("deadline")
                                 or now + _DEFAULT_RESPONSIBILITY_DEADLINE_MS),
                    "actor": owner})
            raise ActionError(
                "unknown_response",
                "flush（PATCH steer）结果未知，保原操作与 message_id，不重复 PATCH",
                {"input_id": input_id, "session_id": row["session_id"],
                 "message_id": row.get("message_id"),
                 "responsibility_id": plan.get("responsibility_id")
                 if plan else None}) from exc
        row = self._record(row, now, indicator="admission",
                           via="flush:steer", operation="flush",
                           operation_revision=claim_rev)
        # 成功 flush 持久真实 flushed_at/effective 执行状态（任何受管 park，
        # 含未托管 watch 外部分支）；到期扫描据此排除，不再 fake park-expired。
        marked = self._ledger.mark_managed_flushed(
            row["input_id"], now=now,
            flush_operation_revision=claim_rev,
            effective_intent=rules.INTENT_EXECUTE,
            effective_resume=True, actor=owner)
        row = marked.get("input") or row
        return {"ok": True, "changed": True,
                "resume": True,
                "tracking": (rules.TRACKED if plan is not None
                             else rules.UNTRACKED if tracking else None),
                "responsibility_id": (plan.get("responsibility_id")
                                      if plan is not None else None),
                "input": rules.receipt_view(row, now=now, include_body=True)}

    # ---- cancel ------------------------------------------------------------
    def cancel(self, input_id, *, owner, confirm, expected_revision, reason) -> dict:
        check = rules.validate_confirmation(
            object_id=input_id, confirm=confirm, owner=owner,
            expected_revision=expected_revision, reason=reason)
        if not check["valid"]:
            raise ActionError("usage", check["reason"], {"input_id": input_id})
        row = self._ledger.get_managed_input(input_id)
        if row is None:
            raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
        row = self._require_managed(row, input_id)
        if row.get("intent") == rules.INTENT_RECORD:
            raise ActionError("usage", "record 仅留档，不支持 cancel",
                              {"input_id": input_id})
        now = self.clock.now_ms()
        session_id, message_id = row["session_id"], row.get("message_id")
        claim = self._claim(row, now, operation="cancel", owner=owner,
                            expected_revision=expected_revision, reason=reason)
        if not claim.get("changed"):
            return self._noop_unknown(claim.get("input") or row, now)
        row = claim["input"]
        claim_rev = claim.get("revision")
        try:
            self._client().cancel_inbox(session_id, message_id)
        except Exception:
            # DELETE 响应未知：不凭“成功且无异常”造 cancelled，交由对账判定。
            pass
        # 当前 Client cancel 返回 None，不支持原子取消证明。只有原 ID 可信历史
        # 命中才 delivered；否则保持 unknown（显式可信取消事件尚未提供）。
        duty = None
        observation = self._read_delivery(row)
        persisted = self._persisted_delivery(row)
        if observation["indicator"] == "delivery":
            row = self._record(row, now, indicator="delivery",
                               via="cancel:delivered-message", operation="cancel",
                               operation_revision=claim_rev)
            delivery = rules.DELIVERY_DELIVERED
        elif persisted in (rules.DELIVERY_DELIVERED, rules.DELIVERY_CANCELLED):
            # 既有 delivered/cancelled 事实不因再次 cancel 倒退为 unknown。
            delivery = persisted
        else:
            row = self._record(row, now, indicator="unknown",
                               via="cancel:no-atomic-cancel-proof",
                               operation="cancel", operation_revision=claim_rev)
            delivery = rules.DELIVERY_UNKNOWN
            # unknown 必须可见且有期限/超限责任；ACK/无异常不能当作取消成功。
            duty = self._update_responsibility_duty(row, now=now, fields={
                "state": "waiting_dependency",
                "wait_kind": "cancel_reconcile",
                "wait_condition": f"input:{row.get('message_id')}:cancel-evidence",
                "next_action": "按原 ID 复核取消与投递证据；DELETE no-op/缺失证据"
                               "保持 unknown，超限转人工，不伪造 cancelled",
                "next_check_at": now + _DEFAULT_NEXT_CHECK_MS,
                "deadline": (rules.tracking_block(row).get("deadline")
                             or now + _CANCEL_UNKNOWN_DEADLINE_MS),
                "evidence": {"cancel_unknown_at": now,
                             "via": "cancel:no-atomic-cancel-proof"},
                "actor": owner})
        result = {"ok": True, "changed": True, "delivery": delivery,
                  "unknown": delivery == rules.DELIVERY_UNKNOWN,
                  "input": rules.receipt_view(row, now=now, include_body=True)}
        if delivery == rules.DELIVERY_UNKNOWN and duty:
            if isinstance(duty, dict) and duty.get("error"):
                result["responsibility_error"] = duty["error"]
            else:
                result["responsibility_updated"] = True
        return result

    # ---- 执行/可信取消证据（runtime 归属观察，不关闭业务责任） -------------
    _OBS_META_KEYS = frozenset({
        "event_id", "event_type", "observed_at", "observed_ms", "source",
        "detail", "reference", "sequence",
    })

    @classmethod
    def _sanitize_observation_meta(cls, evidence: Any) -> dict:
        """只允许非语义观察元数据；状态指示/取消/归属必须走显式参数。"""
        if evidence is None:
            return {}
        if not isinstance(evidence, dict):
            raise ActionError("usage", "evidence 必须是 dict/None",
                              {"evidence_type": type(evidence).__name__})
        forbidden = sorted(set(evidence) - cls._OBS_META_KEYS)
        if forbidden:
            raise ActionError(
                "usage",
                f"evidence 不允许的字段：{forbidden}；取消/执行/ACK 必须用显式"
                "参数，不能由任意 reason/dict 自动关闭责任",
                {"forbidden": forbidden, "allowed": sorted(cls._OBS_META_KEYS)})
        return dict(evidence)

    @staticmethod
    def _attribution_mismatch(row: dict, value: dict, *, label: str) -> None:
        tracking = rules.tracking_block(row)
        for key in ("attempt_id", "generation", "result_version"):
            if key not in value:
                continue
            expected = tracking.get(key)
            if expected is not None and value.get(key) != expected:
                raise ActionError(
                    "usage",
                    f"{label}.{key}={value.get(key)!r} 与输入归属 {expected!r} "
                    "不一致；旧/其他尝试证据不能写入本输入",
                    {"input_id": row.get("input_id"), "field": key,
                     "expected": expected, "received": value.get(key)})

    @classmethod
    def _validate_execution_evidence(cls, row: dict, execution: Any) -> dict:
        if not isinstance(execution, dict):
            raise ActionError("usage", "execution 必须是 dict", {})
        outcome = execution.get("outcome")
        if outcome not in rules.EXECUTION_OUTCOMES:
            raise ActionError(
                "usage",
                f"execution.outcome 必须是 {'/'.join(rules.EXECUTION_OUTCOMES)}"
                f"（收到 {outcome!r}）；unknown/reason 不得自动关闭业务责任",
                {"allowed": list(rules.EXECUTION_OUTCOMES)})
        if "message_id" in execution and \
                execution.get("message_id") != row.get("message_id"):
            raise ActionError(
                "usage", "execution.message_id 与输入原 message_id 不一致",
                {"expected": row.get("message_id"),
                 "received": execution.get("message_id")})
        cls._attribution_mismatch(row, execution, label="execution")
        tracking = rules.tracking_block(row)
        attributed = any(k in execution for k in
                         ("attempt_id", "generation", "result_version"))
        if tracking.get("attempt_id") is not None and not attributed:
            raise ActionError(
                "usage",
                "execution 必须含 attempt_id/generation/result_version 之一，"
                "以证明归属本输入；只有 outcome 不足以关闭/推进责任",
                {"input_id": row.get("input_id"),
                 "expected_attempt_id": tracking.get("attempt_id")})
        return dict(execution)

    @classmethod
    def _validate_business_ack(cls, row: dict, ack: Any) -> dict:
        if not isinstance(ack, dict):
            raise ActionError("usage", "business_ack 必须是 dict", {})
        bools = [k for k in ("value", "acked") if k in ack]
        if not bools or any(not isinstance(ack.get(k), bool) for k in bools):
            raise ActionError(
                "usage",
                "business_ack 必须含 bool value/acked；任意 reason 文本不得当作 ACK",
                {"allowed": ["value", "acked"]})
        cls._attribution_mismatch(row, ack, label="business_ack")
        return dict(ack)

    def _claim_for_evidence(self, row: dict, *, prefer: str) -> tuple[str, int]:
        order = (prefer, "send", "cancel", "flush")
        for op in order:
            rev = self._claim_revision(row, op)
            if rev is not None:
                return op, rev
        raise ActionError(
            "ledger", "输入没有可归属操作 claim，拒绝登记无归属执行/取消证据",
            {"input_id": row.get("input_id")})

    def record_managed_observation(self, input_id, *, execution=None,
                                   business_ack=None, cancelled=False,
                                   trusted=False, via="runtime:observation",
                                   evidence=None) -> dict:
        """runtime 写入已 delivery 后的 execution/business_ack 归属证据。

        只按原 message_id 与已有 send/cancel claim 归属；不改变 admission_state，
        不自动 ACK、不关闭责任。``cancelled=True`` 必须 ``trusted=True``（SSE/
        显式可信取消），inbox 消失/DELETE no-op 不得走本入口伪造取消。
        ``evidence`` 只允许非语义元数据；execution 必须含合法 outcome 与
        attempt_id/generation/result_version 归属，business_ack 必须含 bool
        value/acked；任意 reason 文本不能触发取消/关闭。
        """
        row = self._ledger.get_managed_input(input_id)
        if row is None:
            raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
        row = self._require_managed(row, input_id)
        if row.get("intent") == rules.INTENT_RECORD:
            raise ActionError("usage", "record 仅留档，无执行/取消观察",
                              {"input_id": input_id})
        if not isinstance(cancelled, bool) or not isinstance(trusted, bool):
            raise ActionError("usage", "cancelled/trusted 必须是 bool",
                              {"input_id": input_id})
        if not isinstance(via, str) or not via.strip():
            raise ActionError("usage", "via 必须是非空字符串",
                              {"input_id": input_id})
        if cancelled and not trusted:
            raise ActionError(
                "usage", "取消证据必须 trusted=True；不能以本入口伪造取消",
                {"input_id": input_id})
        if cancelled and (execution is not None or business_ack is not None):
            raise ActionError(
                "usage", "cancelled 不能与 execution/business_ack 同一观察混用；"
                "取消与执行/ACK 分别记账",
                {"input_id": input_id})
        execution = (self._validate_execution_evidence(row, execution)
                     if execution is not None else None)
        business_ack = (self._validate_business_ack(row, business_ack)
                        if business_ack is not None else None)
        meta = self._sanitize_observation_meta(evidence)
        payload = {
            "message_id": row["message_id"],
            "via": via,
        }
        payload.update(meta)
        if execution is not None:
            payload["execution"] = execution
        if business_ack is not None:
            payload["business_ack"] = business_ack
        if cancelled:
            payload["cancelled"] = True
            payload["trusted"] = True
            op, rev = self._claim_for_evidence(row, prefer="cancel")
        else:
            op, rev = self._claim_for_evidence(row, prefer="send")
        payload["operation"] = op
        payload["operation_revision"] = rev
        now = self.clock.now_ms()
        result = self._ledger.record_managed_observation(
            input_id, evidence=payload, now=now)
        updated = result.get("input") or row
        duty = None
        if cancelled and result.get("changed"):
            duty = self._update_responsibility_duty(updated, now=now, fields={
                "state": "cancelled",
                "wait_kind": "cancelled",
                "wait_condition": f"input:{updated.get('message_id')}:cancelled",
                "next_action": "输入已由可信取消证据确证；保留审计，不重投",
                "evidence": {"cancelled_at": now, "via": via},
                "actor": "runtime"})
        view = rules.receipt_view(updated, now=now, include_body=True)
        out = {"ok": True, "changed": bool(result.get("changed")),
               "recorded": bool(result.get("recorded", True)),
               "conflict": bool(result.get("conflict")),
               "reason": result.get("reason"),
               "input": view}
        if isinstance(duty, dict) and duty.get("error"):
            out["responsibility_error"] = duty["error"]
        return out

    def acknowledge(self, input_id, *, value: bool, owner, confirm,
                    expected_revision, reason) -> dict:
        """共享业务 ACK：确认原 input/owner/revision 后写 bool 业务确认。

        不关闭后续责任、不 accepted、不改 execution；value=False 是显式未确认，
        不能因 dict 真值显示 acknowledged。
        """
        check = rules.validate_confirmation(
            object_id=input_id, confirm=confirm, owner=owner,
            expected_revision=expected_revision, reason=reason)
        if not check["valid"]:
            raise ActionError("usage", check["reason"], {"input_id": input_id})
        if not isinstance(value, bool):
            raise ActionError("usage", "value 必须是 bool",
                              {"input_id": input_id})
        # 窄事务 CAS：owner/状态/expected_revision 在 BEGIN IMMEDIATE 内复核，
        # revision+1 + audit；错误不写入。runtime 观察不能覆盖显式 ACK。
        result = self._ledger.acknowledge_managed_input(
            input_id, value=value, owner=owner,
            expected_revision=expected_revision, reason=reason,
            now=self.clock.now_ms())
        view = rules.receipt_view(
            result.get("input") or {}, now=self.clock.now_ms(),
            include_body=True)
        return {"ok": True, "changed": True,
                "revision": result.get("revision"),
                "business_ack": view.get("business_ack"),
                "input": view}

    def record_business_report(self, input_id, *, report: dict,
                               source_message_id: str,
                               via: str = "explicit") -> dict:
        """持久 worker 结构化接续报告；不自动业务 ACK/accepted/关闭责任。

        必须先有原输入 delivery 与 execution 证据；schema/id/tracking 校验在
        domain/managed_inputs.py 纯规则中完成。C 之后消费 evidence.continuation
        并由责任 store 发布/核收。
        """
        row = self._ledger.get_managed_input(input_id)
        if row is None:
            raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
        row = self._require_managed(row, input_id)
        if row.get("intent") == rules.INTENT_RECORD:
            raise ActionError("usage", "record 仅留档，不支持续接报告",
                              {"input_id": input_id})
        now = self.clock.now_ms()
        view = rules.receipt_view(row, now=now)
        if view.get("delivery") != rules.DELIVERY_DELIVERED:
            raise ActionError(
                "gate", "续接报告要求原输入已确认 delivered；inbox 消失/HTTP "
                        "成功不足，不能凭 report 文本推进",
                {"input_id": input_id, "delivery": view.get("delivery")})
        if view.get("execution") not in rules.EXECUTION_OUTCOMES:
            raise ActionError(
                "gate", "续接报告要求已有可归属 execution 证据；succeeded/空 inbox "
                        "不得替代",
                {"input_id": input_id, "execution": view.get("execution")})
        check = rules.validate_continuation_report(
            row, report, source_message_id)
        if not check["valid"]:
            raise ActionError("usage", check["reason"],
                              {"input_id": input_id,
                               "errors": check.get("errors")})
        persisted = self._ledger.record_managed_continuation(
            input_id, report=check["report"],
            source_message_id=source_message_id, via=via, now=now)
        updated = persisted.get("input") or row
        out_view = rules.receipt_view(updated, now=now, include_body=True)
        return {"ok": True, "report": check["report"],
                "continuation": persisted.get("continuation"),
                "business_ack": out_view.get("business_ack"),
                "input": out_view}

    # ---- resume_target -----------------------------------------------------
    def resume_target(self, session_id, *, owner, confirm, expected_revision,
                      reason) -> dict:
        """兼容旧入口：委托统一 ``clear_manual``（不做 resume_manual 双实现）。

        等价守卫：confirm=session、owner/reason、expected_revision 必须匹配当前
        快照；只接受 source=manual，provider 冷却/半开/耗尽态拒绝。审计写到
        base ledger；绝不清额度预算或触发 prompt。
        """
        check = rules.validate_confirmation(
            object_id=session_id, confirm=confirm, owner=owner,
            expected_revision=expected_revision, reason=reason)
        if not check["valid"]:
            raise ActionError("usage", check["reason"], {"session_id": session_id})
        snapshot = self._gate_snapshot(session_id)
        if not isinstance(snapshot, dict):
            raise ActionError(
                "gate", "无活动闸门，不能执行人工恢复",
                {"session_id": session_id})
        if snapshot.get("source") != "manual" or \
                snapshot.get("state") != "manual-blocked":
            raise ActionError(
                "gate", "只允许解除 source=manual 的人工控制；provider 冷却/"
                        "半开探测/耗尽态保持原状",
                {"session_id": session_id,
                 "source": snapshot.get("source"),
                 "state": snapshot.get("state")})
        if snapshot.get("revision") != expected_revision:
            raise ActionError(
                "gate", f"闸门 revision={snapshot.get('revision')} != 期望 "
                        f"{expected_revision}；并发变化或过期快照",
                {"session_id": session_id,
                 "revision": snapshot.get("revision")})
        result = self._gate.clear_manual(session_id, reason=reason)
        if not (isinstance(result, dict) and result.get("cleared")):
            raise ActionError(
                "gate", "人工恢复未生效：只解除明确 manual 控制，不清额度预算，"
                        "不触发 prompt",
                {"session_id": session_id,
                 "reason": result.get("reason") if isinstance(result, dict) else None})
        base = getattr(self._ledger, "base", None)
        if base is not None and hasattr(base, "audit"):
            base.audit(owner, "target-gate.clear-manual", "session", session_id,
                       {"reason": reason, "gate_reason": result.get("note"),
                        "via": "managed_inputs.resume_target"})
        return {"ok": True, "session_id": session_id, "changed": True,
                "gate": result.get("gate"),
                "reason": result.get("reason")}

    # ---- 白名单 ------------------------------------------------------------
    def register_watch(self, session_id, *, owner, project, max_pending_age_ms) -> dict:
        errors = []
        if not (isinstance(session_id, str) and session_id.strip()):
            errors.append("session_id 必须是非空字符串")
        if not (isinstance(owner, str) and owner.strip()):
            errors.append("owner 必须是非空字符串")
        if not (isinstance(project, str) and project.strip()):
            errors.append("project 必须是非空字符串")
        if not (isinstance(max_pending_age_ms, int)
                and not isinstance(max_pending_age_ms, bool)
                and max_pending_age_ms >= 0):
            errors.append("max_pending_age_ms 必须是非负整数")
        if errors:
            raise ActionError("usage", "; ".join(errors), {"session_id": session_id})
        result = self._ledger.register_managed_watch(
            session_id, owner=owner, project=project,
            max_pending_age_ms=max_pending_age_ms, now=self.clock.now_ms())
        return {"ok": True, "changed": bool(result.get("changed")),
                "watch": rules.watch_view(result.get("watch"))}

    def unregister_watch(self, session_id, *, owner, expected_revision, confirm) -> dict:
        check = rules.validate_confirmation(
            object_id=session_id, confirm=confirm, owner=owner,
            expected_revision=expected_revision, require_reason=False)
        if not check["valid"]:
            raise ActionError("usage", check["reason"], {"session_id": session_id})
        result = self._ledger.unregister_managed_watch(
            session_id, owner=owner, expected_revision=expected_revision,
            now=self.clock.now_ms())
        return {"ok": True, "watch": rules.watch_view(result.get("watch"))}

    def list_watches(self, *, limit=100) -> dict:
        rows = self._ledger.list_managed_watches(limit=limit)
        return {"ok": True, "watches": [rules.watch_view(r) for r in rows]}

    # ---- observe_registered ------------------------------------------------
    def observe_registered(self, *, limit=20, allow_remote=True) -> dict:
        """本地 park 到期始终处理；allow_remote=False 时绝不构造 Client/发 GET。"""
        now = self.clock.now_ms()
        expiry = self._ledger.expire_managed_parks(now=now, limit=limit)
        alerts = self._expiry_alerts(expiry, now=now)
        if not allow_remote:
            # 不推进远端游标：服务不可用不消耗轮转位置。
            return {"ok": True, "observed": [], "alerts": alerts,
                    "expiry": self._expiry_stats(expiry)}
        observed = []
        for watch in self._next_watch_page(limit):
            obs = self._observe_target(watch, now)
            if obs is not None:
                observed.append(obs)
        return {"ok": True, "observed": observed, "alerts": alerts,
                "expiry": self._expiry_stats(expiry)}

    # ---- 白名单轮转（有界、不前缀饥饿、不拉全量） --------------------------
    def _list_watches_page(self, *, after: str | None, limit: int) -> list[dict]:
        """读取一页启用白名单（B 的 SQL 在 LIMIT 前按 enabled 过滤）。"""
        return list(self._ledger.list_managed_watches(
            limit=limit, after_session=after, enabled_only=True))

    def _next_watch_page(self, limit: int) -> list[dict]:
        """tail page +（必要时）head page，总条数 <= limit，轮转覆盖全部 enabled。"""
        limit = 500 if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1 \
            else min(limit, 500)
        tail = self._list_watches_page(after=self._watch_cursor, limit=limit)
        page = list(tail)
        if len(page) < limit:
            head = self._list_watches_page(after=None, limit=limit)
            seen = {w.get("session_id") for w in page}
            for item in head:
                if len(page) >= limit:
                    break
                if item.get("session_id") in seen:
                    continue
                seen.add(item.get("session_id"))
                page.append(item)
        self._watch_cursor = page[-1].get("session_id") if page else None
        return page

    def _recheck_watch(self, watch: dict) -> dict | None:
        """GET 前重读登记：enabled+revision+owner 全部一致才可观察。"""
        try:
            current = self._ledger.get_managed_watch(watch.get("session_id"))
        except LedgerError:
            return None
        if current is None or not current.get("enabled"):
            return None
        if current.get("revision") != watch.get("revision"):
            return None
        if current.get("owner") != watch.get("owner"):
            return None
        return current

    @staticmethod
    def _expiry_stats(expiry) -> dict:
        if not isinstance(expiry, dict):
            return {"checked": 0, "alerts": 0, "marked": 0, "expired": []}
        return {"checked": expiry.get("checked", 0),
                "alerts": expiry.get("alerts", 0),
                "marked": expiry.get("marked", 0),
                "expired": list(expiry.get("expired") or [])}

    def _expiry_alerts(self, expiry, *, now: int) -> list[dict]:
        out: list[dict] = []
        ids = (expiry.get("expired") if isinstance(expiry, dict) else None) or []
        for input_id in ids:
            row = self._ledger.get_managed_input(input_id)
            if row is None:
                continue
            block = self._managed_block(row)
            out.append({
                "session_id": row.get("session_id"),
                "input_id": input_id,
                "owner": row.get("owner"),
                "project": block.get("project") or "",
                "kind": "park-expired",
                "expires_at": block.get("expires_at"),
                "next_action": "到期只告警：人工决定唤醒，绝不自动 flush/cancel/执行",
            })
        return out

    def _observe_target(self, watch: dict, now: int) -> dict | None:
        # 每目标 GET 前复核登记（批次 A 回调撤销 B 时，B 不得 GET/返回伪观测）。
        watch = self._recheck_watch(watch)
        if watch is None:
            return None
        session_id = watch.get("session_id")
        base = {"session_id": session_id, "owner": watch.get("owner"),
                "project": watch.get("project"),
                "watch_revision": watch.get("revision")}
        try:
            raw_items = list(self._client().inbox(session_id))
        except Exception as exc:
            obs = {**base, "trusted": False,
                   "blocking_reason": "inbox-query-failed",
                   "pending": None, "pending_total": None,
                   "pending_lower_bound": None, "truncated": False,
                   "coverage": "unknown", "oldest_age_ms": None,
                   "oldest_message_id": None, "aged_out": False,
                   "pending_condition_key": rules.pending_condition_key([]),
                   "next_action": "查询未知不等于空闲：重查后补证，不自动处置"}
            # 错误信息只落在内部日志语义；观测对象不外泄原始 error/正文。
            _ = exc
            return obs if self._note_watch(watch, now, obs) else None
        total = len(raw_items)
        truncated = total > _OBSERVE_TARGET_BUDGET
        items = raw_items[:_OBSERVE_TARGET_BUDGET]
        known = self._owned_message_ids(session_id)
        oldest: int | None = None
        oldest_id: str | None = None
        foreign: list[str] = []
        unknown = 0
        pending_ids: list[str] = []
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("id"), str) \
                    or not item.get("id"):
                unknown += 1
                continue
            item_id = item["id"]
            pending_ids.append(item_id)
            if item_id not in known:
                # 显式登记的真正外部分支：只决定 blocking/未知意图，绝不丢时间事实。
                foreign.append(item_id)
            # 时间事实：time 先做对象 shape 校验；坏/缺失/非正 → age unknown，不猜造。
            time_obj = item.get("time")
            created = time_obj.get("created") if isinstance(time_obj, dict) else None
            if isinstance(created, int) and not isinstance(created, bool) and created > 0:
                age = max(0, now - created)
                if oldest is None or age > oldest:
                    oldest = age
                    oldest_id = item_id
        blocking = None
        if foreign:
            blocking = "foreign-input:" + ",".join(str(x)[:24] for x in foreign[:5])
        elif unknown:
            blocking = f"unknown-shape:{unknown}"
        threshold = watch.get("max_pending_age_ms")
        aged_out = (isinstance(threshold, int) and not isinstance(threshold, bool)
                    and oldest is not None and oldest > threshold)
        obs = {**base, "trusted": True,
               # 截断时 pending 不是精确总数：只给 total/lower_bound/coverage。
               "pending": total if not truncated else None,
               "pending_total": total,
               "pending_lower_bound": len(items) if truncated else total,
               "truncated": truncated,
               "coverage": "partial" if truncated else "full",
               "oldest_age_ms": oldest, "oldest_message_id": oldest_id,
               "oldest_age_lower_bound_ms": oldest if truncated else None,
               "aged_out": bool(aged_out), "blocking_reason": blocking,
               # 稳定去重条件：ID 集合摘要（不含动态 age/正文）。
               "pending_condition_key": (rules.pending_condition_key(pending_ids)
                                         + (":truncated" if truncated else "")),
               "next_action": ("阻塞/超阈值或覆盖不足：转 owner 告警，人工决定处置，"
                               "不自动 flush/cancel" if (aged_out or blocking or truncated)
                               else "等待：继续定点观察")}
        return obs if self._note_watch(watch, now, obs) else None

    def _note_watch(self, watch: dict, now: int, observation: dict) -> bool:
        """写回观察；返回是否已持久（False 则不计入 observed、无告警）。"""
        try:
            result = self._ledger.record_managed_watch_observation(
                watch.get("session_id"),
                expected_revision=watch.get("revision"),
                observation=observation, now=now)
        except LedgerError:
            # 观察期间已撤销/版本变化：CAS 拒绝，跳过且绝不重新启用。
            return False
        return not (isinstance(result, dict) and result.get("recorded") is False)


__all__ = ["ManagedInputs"]

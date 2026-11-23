"""业务台账：任务、执行尝试、回合、事件、通知、告警、协调者租约。

设计要点：
- 任务（business objective）≠ 执行尝试（attempt）≠ session 回合（turn）≠
  结果提交（collect）≠ 验收（verify）。
- worker 只写交付物；协调者 verify 通过后才 accepted。
- 所有状态变化带审计；只允许通过本类写入。
- 通知与事实分离：reconcile 写事实并入队，supervisor 单独发送唤醒。
"""

from __future__ import annotations

import glob as globmod
import json
import os
import sqlite3
from typing import Any, Callable, Iterable

from ..domain.errors import LedgerError
from ..domain import execution as _exec_rules
from ..domain.rules import (
    ALLOWED_POST_TERMINAL,
    ATTEMPT_ACTIVE,
    ATTEMPT_STATUSES,
    ATTEMPT_TERMINAL,
    ATTEMPT_TRANSITION_EXTRA,
    RESULT_ARTIFACT_SCHEMA,
    SITE_HOLDING,
    STATUS_TASK_MAP,
    TASK_GUARD_COLUMNS,
    TASK_STATUSES,
    TASK_TERMINAL,
    TASK_TRANSITION_EXTRA,
    VERIFIABLE_STATES,
    VERIFICATION_GUARD_COLUMNS,
    verification_gate_rows,
)
from .sqlite_db import transaction
from .input_ledger import InputLedger
from ..domain.notification_policy import (
    CLEANUP_REQUIRED as P_CLEANUP,
    DEFER as P_DEFER,
    SEND as P_SEND,
    SUPPRESS as P_SUPPRESS,
    R_SUPERSEDED as P_R_SUPERSEDED,
    R_TASK_CANCELLED as P_R_TASK_CANCELLED,
    R_TASK_PAUSED as P_R_TASK_PAUSED,
    notification_decision,
)
from ..util import (
    iso,
    json_dumps,
    json_loads,
    normalize_path,
    now_ms,
    paths_overlap,
    sha256_file,
    sha256_text,
    short_id,
)


def _row(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


def _task_from_row(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["acceptance"] = json_loads(d.get("acceptance"), [])
    d["write_scope"] = json_loads(d.get("write_scope"), [])
    d["metadata"] = json_loads(d.get("metadata"), {})
    return d


def _attempt_from_row(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["session_meta"] = json_loads(d.get("session_meta"), {})
    d["metadata"] = json_loads(d.get("metadata"), {})
    d["verification"] = json_loads(d.get("verification"), None)
    return d


def _notification_from_row(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["payload"] = json_loads(d.get("payload"), {})
    d["metadata"] = json_loads(d.get("metadata"), {})
    return d


# 受保护执行终态证据子段（tasks 3.1）：通用 metadata 替换不得改写；
# 唯一合法写入口是 record_terminal 窄事务（含等价 REST/seal 内部 SQL）。
_PROTECTED_EXECUTION_SECTIONS = ("terminal",)
# 白名单观察子字段：record_side_effect_observation 允许合并的键。
_SIDE_EFFECT_OBSERVATION_KEYS = (
    "active_shells", "active_children", "side_effects_checked_at",
    "side_effect_checks",
)


def _assert_terminal_block_unchanged(current_att: dict,
                                     new_metadata: dict) -> None:
    """比较更新前后 execution 受保护子段；变化即拒绝（相同值放行）。"""
    if not isinstance(new_metadata, dict):
        return
    current_exec = (current_att.get("metadata") or {}).get("execution") or {}
    new_exec = (new_metadata or {}).get("execution") or {}
    for section in _PROTECTED_EXECUTION_SECTIONS:
        if current_exec.get(section) != new_exec.get(section):
            raise LedgerError(
                "EXECUTION_PROTECTED",
                f"execution.{section} 是受保护执行终态证据，"
                "只能经 record_terminal 窄事务写入；通用字段更新不得替换",
                {"section": section,
                 "current": current_exec.get(section),
                 "incoming": new_exec.get(section)},
            )


def _alert_from_row(row: sqlite3.Row) -> dict:
    return dict(row)


def deliverable_kind(target: str) -> str:
    if any(ch in target for ch in "*?["):
        return "glob"
    if target.endswith(os.sep) or target.endswith("/"):
        return "dir"
    return "file"


_EXPECTED_CURRENT_UNSET = object()


def _assert_dispatch_authority_unchanged(att: dict, metadata) -> None:
    old = att.get("metadata") or {}
    for key in ("delivery_plan", "delivery_collaboration"):
        if key in old and (not isinstance(metadata, dict) or key not in metadata
                          or json_dumps(old[key], sort_keys=True) != json_dumps(metadata[key], sort_keys=True)):
            raise LedgerError("DELIVERY_PLAN_IMMUTABLE", "已冻结策略/协作声明不能由通用metadata更新改写")
    if "delivery_plan" in old:
        before = (old.get("dispatch") or {}).get("prompt")
        after = (metadata.get("dispatch") or {}).get("prompt") if isinstance(metadata, dict) else None
        if before != after:
            raise LedgerError("DELIVERY_PLAN_IMMUTABLE", "策略派单的冻结正文不可被通用metadata更新改写")
    if "dispatch_authority" not in old:
        return
    if (not isinstance(metadata, dict) or "dispatch_authority" not in metadata
            or json_dumps(old["dispatch_authority"], sort_keys=True)
            != json_dumps(metadata["dispatch_authority"], sort_keys=True)):
        raise LedgerError("DISPATCH_AUTHORITY_IMMUTABLE", "已冻结派单主体不能由通用metadata更新删除或改写")


class Ledger(InputLedger):
    def __init__(self, conn: sqlite3.Connection, home: str, *, clock=None):
        self.conn = conn
        self.home = home
        # 可选时钟注入：默认 None 时仍走模块级 now_ms()，保留既有测试对
        # oc_orch.ledger.now_ms 的 mock.patch 兼容；注入后所有持久时间戳同源。
        self.clock = clock

    def _now_ms(self) -> int:
        """持久时间戳来源：注入 clock 时用 clock.now_ms()，否则回退模块级
        now_ms()（默认可被 oc_orch.ledger.now_ms 的测试 patch 替换）。"""
        if self.clock is not None:
            return self.clock.now_ms()
        return now_ms()

    def delivery_policy_enabled(self) -> bool:
        from .delivery_guard import check_delivery_guard

        return check_delivery_guard(self.conn)

    def authorize_delivery_attempt(self, attempt_id: str, *, observation: dict) -> dict | None:
        from .delivery_action import DeliveryActions

        return DeliveryActions(self).attempt(attempt_id, observation=observation)

    def delivery_attempt_required(self, attempt_id: str) -> bool:
        from .delivery_action import DeliveryActions

        return DeliveryActions(self).attempt_required(attempt_id)

    def delivery_input_required(self, input_id: str) -> bool:
        from .delivery_action import DeliveryActions

        return DeliveryActions(self).input_required(input_id)

    def authorize_delivery_input(self, input_id: str, **intent) -> dict | None:
        from .delivery_action import DeliveryActions

        return DeliveryActions(self).input(input_id, **intent)

    def check_project_dispatch(self, project: str) -> None:
        from .continuity_guard import check_project_dispatch

        check_project_dispatch(self.conn, self.clock, project)

    def check_dispatch_authority(self, project: str, session_id: str | None,
                                 *, expected: dict | None = None) -> dict:
        from .continuity_guard import check_dispatch_authority

        return check_dispatch_authority(self.conn, self.clock, project, session_id,
                                        expected=expected)

    def check_attempt_dispatch(self, attempt_id: str) -> None:
        """最后本地发送门；不持事务做网络，不改变已飞出请求的真实回执。"""
        att = self.get_attempt(attempt_id)
        task = self.get_task(att["task_id"])
        if (task["current_attempt_id"] != attempt_id
                or task["status"] in ("paused", "cancelled", "accepted")
                or task.get("closed_at") is not None
                or att["stop_requested_at"] is not None or att["site_released_at"] is not None):
            raise LedgerError("DISPATCH_CONTROL_CHANGED", "当前尝试/任务已受人工控制；不发送")
        authority = att["metadata"].get("dispatch_authority")
        if authority is None:
            # 未冻结主体的历史未决派单仍可观察原回执；有租约时不补造历史授权。
            self.check_project_dispatch(task["project"])
            if self.coordinator_lease(task["project"]) is not None:
                raise LedgerError("DISPATCH_AUTHORITY_UNKNOWN", "历史派单缺少租约锚点，仅允许原身份补证")
            return
        if not isinstance(authority, dict) or authority.get("schema") != 1:
            raise LedgerError("DISPATCH_AUTHORITY_UNKNOWN", "派单主体快照损坏；拒绝发送")
        self.check_dispatch_authority(task["project"], authority.get("session_id"),
                                       expected=authority)
        if task.get("coordinator_session") != authority.get("session_id"):
            raise LedgerError("COORDINATOR_ROUTE_CHANGED", "任务协调归属已变化；不发送旧派单")

    # ---- 基础 -----------------------------------------------------
    def _audit(self, conn, actor: str, action: str, entity: str, entity_id: str,
               from_state: str | None, to_state: str | None, detail: Any = None) -> None:
        conn.execute(
            "INSERT INTO audit(at, actor, action, entity, entity_id, from_state, to_state, detail) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (self._now_ms(), actor, action, entity, entity_id, from_state, to_state,
             json_dumps(detail) if detail is not None else None),
        )

    def audit(self, actor: str, action: str, entity: str, entity_id: str,
              detail: Any = None) -> None:
        with transaction(self.conn):
            self._audit(self.conn, actor, action, entity, entity_id, None, None, detail)

    def get_meta(self, key: str, default=None):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        with transaction(self.conn):
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # ---- 任务 -----------------------------------------------------
    def create_task(
        self,
        *,
        project: str,
        title: str,
        objective: str = "",
        acceptance: Iterable[str] = (),
        write_scope: Iterable[str] = (),
        coordinator_session: str | None = None,
        priority: int = 0,
        task_id: str | None = None,
        metadata: dict | None = None,
        actor: str = "coordinator",
    ) -> dict:
        tid = task_id or short_id("T-", 12)
        if not tid or any(ch.isspace() for ch in tid):
            raise LedgerError("INVALID_TASK_ID", f"非法任务 ID：{tid!r}")
        ts = self._now_ms()
        with transaction(self.conn):
            existing = self.conn.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
            if existing:
                raise LedgerError("TASK_EXISTS", f"任务已存在：{tid}")
            self.conn.execute(
                "INSERT INTO tasks(id, project, title, objective, acceptance, status, "
                "coordinator_session, write_scope, priority, created_at, updated_at, metadata) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (tid, project, title, objective, json_dumps(list(acceptance)), "draft",
                 coordinator_session, json_dumps(list(write_scope)), priority, ts, ts,
                 json_dumps(metadata or {})),
            )
            self._audit(self.conn, actor, "task.create", "task", tid, None, "draft",
                        {"title": title, "project": project})
            from .clock import SystemClock
            from .delivery_policy import DeliveryPolicyStore

            DeliveryPolicyStore(self.conn, self.clock or SystemClock()).inherit_new_task_in_tx(
                tid, actor=actor)
        return self.get_task(tid)

    def get_task(self, task_id: str) -> dict:
        row = self.conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not row:
            raise LedgerError("TASK_NOT_FOUND", f"任务不存在：{task_id}")
        return _task_from_row(row)

    def list_tasks(self, *, status: str | None = None, project: str | None = None,
                   limit: int = 200) -> list[dict]:
        sql = "SELECT * FROM tasks"
        clauses, args = [], []
        if status:
            clauses.append("status=?")
            args.append(status)
        if project:
            clauses.append("project=?")
            args.append(project)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        args.append(limit)
        return [_task_from_row(r) for r in self.conn.execute(sql, args).fetchall()]

    def patch_task(self, task_id: str, *, fields: dict, actor: str = "coordinator",
                   reason: str | None = None) -> dict:
        task = self.get_task(task_id)
        allowed = {
            "title", "objective", "acceptance", "write_scope", "coordinator_session",
            "priority", "next_check_at", "metadata",
        }
        sets, args = [], []
        for key, value in fields.items():
            if key not in allowed:
                raise LedgerError("FIELD_NOT_ALLOWED", f"不允许修改字段：{key}")
            if key in ("acceptance", "write_scope", "metadata"):
                value = json_dumps(value)
            sets.append(f"{key}=?")
            args.append(value)
        if not sets:
            return task
        sets.append("updated_at=?")
        args.append(self._now_ms())
        args.append(task_id)
        with transaction(self.conn):
            self.conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id=?", args)
            self._audit(self.conn, actor, "task.patch", "task", task_id, None, None,
                        {"fields": sorted(fields), "reason": reason})
        return self.get_task(task_id)

    def transition_task(self, task_id: str, status: str, *, reason: str | None = None,
                        actor: str = "system", extra: dict | None = None,
                        force: bool = False) -> dict:
        if status not in TASK_STATUSES:
            raise LedgerError("INVALID_STATUS", f"非法任务状态：{status}")
        task = self.get_task(task_id)
        old = task["status"]
        if old == status and not extra:
            return task
        if extra:
            for key in extra:
                if key not in TASK_TRANSITION_EXTRA:
                    raise LedgerError(
                        "FIELD_NOT_ALLOWED",
                        f"transition_task 不允许注入字段：{key}",
                    )
        # 直接迁移到 accepted 必须拒绝（无验证记录不得 accepted）；已 accepted 的
        # 重复投递在上面幂等分支返回。force 不豁免。
        if status == "accepted":
            raise LedgerError(
                "VERIFY_REQUIRED",
                "task 状态 accepted 必须经由统一入口 submit_verification 写入",
            )
        # 已关闭/终态任务不可经通用状态操作改写（force 不豁免）；closed_at 非空
        # 即使 status 被改动也视为已关闭保护。
        if (old in TASK_TERMINAL or task.get("closed_at") is not None) and old != status:
            raise LedgerError(
                "TASK_TERMINAL",
                f"任务 {task_id} 已关闭/终态 {old}，不能通过通用状态操作改写",
            )
        sets = ["status=?", "updated_at=?"]
        args: list[Any] = [status, self._now_ms()]
        if status == "cancelled":
            sets.append("closed_at=?")
            args.append(self._now_ms())
        if reason is not None and status in ("blocked", "stalled", "rejected"):
            sets.append("blocked_reason=?")
            args.append(reason)
        elif status in ("draft", "dispatched", "running", "paused", "result_pending",
                        "result_received", "accepted"):
            sets.append("blocked_reason=NULL")
        if extra:
            for key, value in extra.items():
                if key in ("metadata", "acceptance", "write_scope"):
                    value = json_dumps(value)
                sets.append(f"{key}=?")
                args.append(value)
        args.append(task_id)
        with transaction(self.conn):
            # 事务内复核：accepted 与已关闭任务保护不能被锁外旧态绕过。
            cur = self.conn.execute(
                "SELECT status, closed_at, current_attempt_id FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            if cur is not None:
                if ((cur["status"] in TASK_TERMINAL or cur["closed_at"] is not None)
                        and cur["status"] != status):
                    raise LedgerError(
                        "TASK_TERMINAL",
                        f"任务 {task_id} 已关闭/终态 {cur['status']}，不能改写",
                    )
                if status == "rejected":
                    prior = self.conn.execute(
                        "SELECT status, verification FROM attempts WHERE id=?",
                        (cur["current_attempt_id"],),
                    ).fetchone()
                    evidence = json_loads(prior["verification"], {}) if prior else {}
                    if (not prior or prior["status"] not in ("verification_failed", "rejected")
                            or not isinstance(evidence, dict) or evidence.get("result") != "fail"):
                        raise LedgerError("VERIFY_REQUIRED", "任务 rejected 必须对应当前尝试的失败验收记录")
            self.conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id=?", args)
            self._audit(self.conn, actor, "task.transition", "task", task_id, old, status,
                        {"reason": reason})
        return self.get_task(task_id)

    def close_task(self, task_id: str, *, status: str = "cancelled", reason: str,
                   actor: str = "coordinator") -> dict:
        if status not in TASK_TERMINAL:
            raise LedgerError("INVALID_CLOSE_STATUS", f"关闭状态必须是 {TASK_TERMINAL}")
        return self.transition_task(task_id, status, reason=reason, actor=actor,
                                    extra={"closed_at": self._now_ms()})

    # ---- 执行尝试 -------------------------------------------------
    def _scope_for(self, scope: list[str], directory: str) -> list[str]:
        return [normalize_path(p, directory) for p in (scope or [directory])]

    def _site_conflicts(self, *, task_id: str, scope: list[str]) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM attempts WHERE site_released_at IS NULL"
        ).fetchall()
        conflicts = []
        for row in rows:
            att = _attempt_from_row(row)
            other_scope = att["metadata"].get("write_scope") or [att["directory"]]
            for a in scope:
                for b in other_scope:
                    if paths_overlap(a, b):
                        conflicts.append({
                            "attempt_id": att["id"],
                            "task_id": att["task_id"],
                            "session_id": att["session_id"],
                            "status": att["status"],
                            "scope": b,
                            "stop_confirmed_at": att["stop_confirmed_at"],
                            "generation": att["generation"],
                        })
                        break
                if conflicts and conflicts[-1]["attempt_id"] == att["id"]:
                    break
        return conflicts

    def create_attempt(
        self,
        task_id: str,
        *,
        attempt_id: str | None = None,
        directory: str,
        session_id: str | None = None,
        agent: str | None = None,
        model: str | None = None,
        variant: str | None = None,
        generation: int | None = None,
        status: str = "creating",
        dispatch_state: str = "prepared",
        dispatch_msg_id: str | None = None,
        deadline_at: int | None = None,
        stall_after: int | None = None,
        deliverables: Iterable[str | dict] = (),
        write_scope: Iterable[str] | None = None,
        metadata: dict | None = None,
        takeover_confirmed: bool = False,
        actor: str = "coordinator",
        prepare_metadata: Callable[[int], dict] | None = None,
        expected_current: str | None | object = _EXPECTED_CURRENT_UNSET,
        coordinator: str | None = None,
        dispatch_authority: dict | None = None,
    ) -> dict:
        directory = normalize_path(directory)
        aid = attempt_id or short_id("A-", 12)
        # 认领原子性：任务状态、代次、写权冲突与 attempt/deliverables/current
        # 指针/审计同处一个 BEGIN IMMEDIATE。scope 依赖 task.write_scope，
        # 因此 task 必须在事务内读取，不能用事务外旧值做决策。ts 也在守卫与
        # 冲突决策完成后生成，保证代次与 created_at 使用一致时间快照。
        with transaction(self.conn):
            task = self.get_task(task_id)
            if task["status"] in TASK_TERMINAL:
                raise LedgerError("TASK_TERMINAL", f"任务 {task_id} 已处于 {task['status']}")
            if status in ("creating", "dispatching"):
                if task["status"] == "paused":
                    raise LedgerError("TASK_PAUSED", "任务已显式暂停，拒绝新尝试认领")
                self.check_project_dispatch(task["project"])
            if expected_current is not _EXPECTED_CURRENT_UNSET and task["current_attempt_id"] != expected_current:
                raise LedgerError("NOT_CURRENT", "当前尝试已变化，拒绝基于旧快照的派单/返工")
            if status in ("accepted", "rejected", "verification_failed"):
                raise LedgerError("VERIFY_REQUIRED", "新尝试不能预设验收结论；请经由 submit_verification")
            if generation is None:
                row = self.conn.execute(
                    "SELECT COALESCE(MAX(generation),0)+1 AS g FROM attempts WHERE task_id=?",
                    (task_id,),
                ).fetchone()
                generation = int(row["g"])
            scope = self._scope_for(list(write_scope or task["write_scope"]), directory)
            conflicts = self._site_conflicts(task_id=task_id, scope=scope)
            # 已确认停止仍须显式 release；停止请求或单独的历史确认字段不授予写权。
            if conflicts and not takeover_confirmed:
                raise LedgerError(
                    "SITE_BUSY",
                    "写入范围仍被其他尝试持有；需先确认旧执行停止并释放或显式 takeover",
                    {"conflicts": conflicts, "requested_scope": scope},
                )
            ts = self._now_ms()
            meta = dict(metadata or {})
            if prepare_metadata is not None:
                # 只允许纯值构造；网络、文件扫描、命令执行仍在事务外。
                meta.update(prepare_metadata(generation))
            if self.delivery_policy_enabled():
                from .clock import SystemClock
                from .delivery_policy import DeliveryPolicyStore
                from ..domain.delivery_policy import fingerprint, render_instructions
                from ..domain.delivery_action import collaboration

                policy = DeliveryPolicyStore(self.conn, self.clock or SystemClock()).task_snapshot(task_id)
                if policy["managed"]:
                    meta["delivery_collaboration"] = collaboration(meta.get("delivery_collaboration"))
                    dispatch = meta.get("dispatch")
                    if isinstance(dispatch, dict) and dispatch.get("prompt"):
                        meta["dispatch"] = {**dispatch,
                            "prompt": dispatch["prompt"] + "\n\n" + render_instructions(policy["version"])}
                    meta["delivery_plan"] = {"policy_revision": policy["revision"],
                        "scope_id": task_id, "model": model, "session_id": session_id,
                        "message_id": dispatch_msg_id,
                        "body_hash": fingerprint((meta.get("dispatch") or {}).get("prompt"))}
            if status in ("creating", "dispatching"):
                subject = coordinator or meta.get("coordinator") or task.get("coordinator_session")
                meta["dispatch_authority"] = self.check_dispatch_authority(
                    task["project"], subject, expected=dispatch_authority)
                if subject is not None:
                    meta["coordinator"] = subject
                    if task.get("coordinator_session") != subject:
                        self.conn.execute(
                            "UPDATE tasks SET coordinator_session=? WHERE id=?", (subject, task_id))
                        self._audit(self.conn, actor, "task.patch", "task", task_id,
                                    None, None, {"fields": ["coordinator_session"],
                                                 "reason": "dispatch-claim",
                                                 "from_session": task.get("coordinator_session"),
                                                 "to_session": subject})
            meta["write_scope"] = scope
            if conflicts and takeover_confirmed:
                meta["site_takeover_from"] = [c["attempt_id"] for c in conflicts]
            self.conn.execute(
                "INSERT INTO attempts(id, task_id, generation, session_id, directory, agent, "
                "model, variant, status, dispatch_state, dispatch_msg_id, started_at, "
                "dispatched_at, last_progress_at, deadline_at, stall_after, created_at, "
                "updated_at, metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (aid, task_id, generation, session_id, directory, agent, model, variant,
                 status, dispatch_state, dispatch_msg_id,
                 ts if status in ("dispatched", "running") else None,
                 ts if dispatch_state == "confirmed" else None,
                 ts, deadline_at, stall_after, ts, ts, json_dumps(meta)),
            )
            for spec in deliverables:
                if isinstance(spec, str):
                    item = {"kind": deliverable_kind(spec), "target": spec, "required": True}
                else:
                    item = dict(spec)
                    item.setdefault("kind", deliverable_kind(item["target"]))
                    item.setdefault("required", True)
                self.conn.execute(
                    "INSERT OR IGNORE INTO deliverables(attempt_id, kind, target, required) "
                    "VALUES(?,?,?,?)",
                    (aid, item["kind"], item["target"], 1 if item["required"] else 0),
                )
            self.conn.execute(
                "UPDATE tasks SET current_attempt_id=?, updated_at=? WHERE id=?",
                (aid, ts, task_id),
            )
            if task["status"] in ("draft", "rejected"):
                self.conn.execute(
                    "UPDATE tasks SET status='dispatched', blocked_reason=NULL, "
                    "updated_at=? WHERE id=?",
                    (ts, task_id),
                )
                self._audit(self.conn, actor, "task.auto", "task", task_id,
                            task["status"], "dispatched",
                            {"attempt": aid, "reason": "attempt-create"})
            self._audit(self.conn, actor, "attempt.create", "attempt", aid, None, status,
                        {"task_id": task_id, "generation": generation,
                         "scope": scope, "takeover": bool(conflicts)})
        return self.get_attempt(aid)

    def get_attempt(self, attempt_id: str) -> dict:
        row = self.conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
        if not row:
            raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
        return _attempt_from_row(row)

    def record_dispatch(self, attempt_id: str, state: str, *, started: bool = False,
                        task_status: str | None = None, actor: str = "coordinator",
                        reason: str = "dispatch-observed") -> dict:
        """原子记录网络事实；确认晚到不得回退 current、执行进度或人工控制。"""
        if state not in ("session_created", "confirmed", "unknown"):
            raise LedgerError("INVALID_DISPATCH_STATE", f"非法派单事实：{state}")
        if task_status not in (None, "dispatched", "running"):
            raise LedgerError("INVALID_STATUS", "派单确认不能写入验收或人工控制状态")
        with transaction(self.conn):
            att = self.get_attempt(attempt_id)
            task = self.get_task(att["task_id"])
            if att["dispatch_state"] == "confirmed" and state != "confirmed":
                return att  # 已有确定事实不能被另一请求的未知结果覆盖。
            ts = self._now_ms()
            status = att["status"]
            can_advance = (status in ("creating", "dispatching", "dispatch_unknown", "dispatched")
                           and att["stop_requested_at"] is None
                           and att["site_released_at"] is None
                           and task["status"] not in ("paused", "cancelled", "accepted")
                           and task["closed_at"] is None)
            if can_advance and state in ("confirmed", "unknown"):
                status = "dispatched" if state == "confirmed" else "dispatch_unknown"
            self.conn.execute(
                "UPDATE attempts SET dispatch_state=?,status=?,updated_at=?,"
                "dispatched_at=COALESCE(dispatched_at,?),started_at=COALESCE(started_at,?) WHERE id=?",
                (state, status, ts, ts if state == "confirmed" else None,
                 ts if state == "confirmed" and started else None, attempt_id),
            )
            self._audit(self.conn, actor, "attempt.dispatch", "attempt", attempt_id,
                        att["status"], status, {"state": state, "reason": reason})
            if (can_advance and state == "confirmed" and task_status
                    and task["current_attempt_id"] == attempt_id
                    and task["status"] != "running"):
                self.conn.execute(
                    "UPDATE tasks SET status=?,updated_at=?,blocked_reason=NULL WHERE id=?",
                    (task_status, ts, task["id"]),
                )
                self._audit(self.conn, actor, "task.auto", "task", task["id"],
                            task["status"], task_status, {"attempt": attempt_id, "reason": reason})
        return self.get_attempt(attempt_id)

    def list_attempts(self, *, task_id: str | None = None, status: str | None = None,
                      limit: int = 200) -> list[dict]:
        sql = "SELECT * FROM attempts"
        clauses, args = [], []
        if task_id:
            clauses.append("task_id=?")
            args.append(task_id)
        if status:
            clauses.append("status=?")
            args.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        return [_attempt_from_row(r) for r in self.conn.execute(sql, args).fetchall()]

    def open_attempts(self, *, task_id: str | None = None) -> list[dict]:
        placeholders = ",".join("?" for _ in ATTEMPT_ACTIVE)
        rows = self.conn.execute(
            f"SELECT * FROM attempts WHERE status IN ({placeholders}) "
            + ("AND task_id=? " if task_id is not None else "") +
            "ORDER BY created_at ASC",
            (*ATTEMPT_ACTIVE, task_id) if task_id is not None else tuple(ATTEMPT_ACTIVE),
        ).fetchall()
        return [_attempt_from_row(r) for r in rows]

    def attempts_for_session(self, session_id: str, *, limit: int = 20) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM attempts WHERE session_id=? ORDER BY created_at DESC LIMIT ?",
            (session_id, limit),
        ).fetchall()
        return [_attempt_from_row(r) for r in rows]

    def latest_attempt_for_session(self, session_id: str) -> dict | None:
        rows = self.attempts_for_session(session_id, limit=1)
        return rows[0] if rows else None

    def update_attempt(self, attempt_id: str, *, fields: dict, actor: str = "system",
                       reason: str | None = None) -> dict:
        self.get_attempt(attempt_id)
        allowed = {
            "session_id", "status", "dispatch_state", "dispatch_msg_id", "session_meta",
            "started_at", "dispatched_at", "last_progress_at", "last_observed_at",
            "deadline_at", "stall_after", "terminal_reason", "stop_requested_at",
            "stop_confirmed_at", "site_released_at", "result_collected_at",
            "result_path", "result_summary", "verification", "metadata",
        }
        sets, args = [], []
        for key, value in fields.items():
            if key not in allowed:
                raise LedgerError("FIELD_NOT_ALLOWED", f"不允许修改尝试字段：{key}")
            if key == "status" and value in ("terminal_succeeded", "terminal_failed",
                                             "terminal_interrupted"):
                att_row_gate = self.conn.execute(
                    "SELECT dispatch_msg_id, metadata FROM attempts WHERE id=?",
                    (attempt_id,)).fetchone()
                bound = (att_row_gate is not None
                         and (att_row_gate["dispatch_msg_id"] is not None
                              or (_attempt_from_row(att_row_gate)["metadata"].get(
                                  "execution") or {}).get("dispatch", {}).get(
                                  "sealed_at")))
                if bound:
                    raise LedgerError(
                        "TERMINAL_GATE_REQUIRED",
                        "已绑定派单的尝试，其执行终态必须经 record_terminal 提交",
                    )
            if key in ("session_meta", "metadata", "verification"):
                if key == "metadata":
                    _assert_dispatch_authority_unchanged(self.get_attempt(attempt_id), value)
                # 3.1 受保护执行终态证据：通用入口不得整体替换 metadata 中
                # 的 execution.terminal（唯一合法写入口是 record_terminal 窄
                # 事务）。相同值随原调用传回允许（无变化的观察回写不拦）。
                if key == "metadata" and value is not None:
                    _assert_terminal_block_unchanged(
                        self.get_attempt(attempt_id), value)
                value = json_dumps(value) if value is not None else None
            sets.append(f"{key}=?")
            args.append(value)
        if not sets:
            return self.get_attempt(attempt_id)
        sets.append("updated_at=?")
        args.append(self._now_ms())
        args.append(attempt_id)
        with transaction(self.conn):
            # F2：验收状态/验证证据只能经由 submit_verification 一致写入；守卫在
            # 真实写事务内复核，防锁外读旧态把已验收尝试改回或单独替换证据。
            current = self.get_attempt(attempt_id)
            if current["metadata"].get("delivery_plan"):
                if any(key in fields and fields[key] != current[key] for key in ("session_id", "dispatch_msg_id")):
                    raise LedgerError("DELIVERY_IDENTITY_IMMUTABLE", "已冻结策略派单不能换session/message身份重投")
                if "metadata" in fields:
                    _assert_dispatch_authority_unchanged(current, fields["metadata"])
            cur = self.conn.execute(
                "SELECT status FROM attempts WHERE id=?", (attempt_id,)
            ).fetchone()
            new_status = fields.get("status")
            if new_status in ("accepted", "rejected", "verification_failed"):
                raise LedgerError(
                    "VERIFY_REQUIRED",
                    "验收结论必须经由统一入口 submit_verification 写入，"
                    "通用字段更新不得直接写入验收状态",
                )
            if cur is not None and cur["status"] == "accepted" and new_status not in (None, "accepted"):
                raise LedgerError(
                    "ACCEPTED_IMMUTABLE",
                    f"尝试 {attempt_id} 已验收，其状态不能通过通用字段修改",
                )
            if "verification" in fields:
                raise LedgerError(
                    "VERIFY_REQUIRED",
                    "验证证据必须随 submit_verification 一致写入，不能独立替换",
                )
            self.conn.execute(f"UPDATE attempts SET {', '.join(sets)} WHERE id=?", args)
            self._audit(self.conn, actor, "attempt.patch", "attempt", attempt_id, None, None,
                        {"fields": sorted(fields), "reason": reason})
        return self.get_attempt(attempt_id)

    def transition_attempt(self, attempt_id: str, status: str, *, reason: str | None = None,
                           actor: str = "system", force: bool = False,
                           extra: dict | None = None) -> dict:
        if status not in ATTEMPT_STATUSES:
            raise LedgerError("INVALID_STATUS", f"非法尝试状态：{status}")
        att = self.get_attempt(attempt_id)
        old = att["status"]
        task_row = self.conn.execute(
            "SELECT * FROM tasks WHERE id=?", (att["task_id"],)
        ).fetchone()
        is_current = bool(task_row and task_row["current_attempt_id"] == attempt_id)
        if extra:
            for key, value in extra.items():
                if key not in ATTEMPT_TRANSITION_EXTRA:
                    raise LedgerError("FIELD_NOT_ALLOWED",
                                      f"transition_attempt 不允许注入字段：{key}")
                if key == "metadata" and value is not None:
                    # 3.1：extra 注入同样不得改写 execution.terminal
                    _assert_terminal_block_unchanged(att, value)
                if key == "metadata":
                    _assert_dispatch_authority_unchanged(att, value)
        # 验收结论只允许经由 submit_verification；终态重复投递保持幂等 no-op。
        if status in ("accepted", "rejected", "verification_failed"):
            if old == status and not extra:
                return att
            if not is_current:
                raise LedgerError(
                    "NOT_CURRENT",
                    f"尝试 {attempt_id} 不是任务当前尝试；旧结果不能推进当前尝试",
                )
            raise LedgerError(
                "VERIFY_REQUIRED",
                "attempt 验收结论必须经由统一入口 submit_verification 写入",
            )
        if old == status and not extra:
            return att
        # 执行终态只能经 record_terminal 提交；绑定真实派单（有派单消息或已
        # 封存执行边界）的尝试不得再通过通用迁移伪造执行完成（D3）。无绑定
        # 的纯台账夹具/legacy 行保持可构造（M2 兼容读取）。
        if status in ("terminal_succeeded", "terminal_failed", "terminal_interrupted"):
            bound = (att.get("dispatch_msg_id") is not None
                     or (att["metadata"].get("execution") or {}).get(
                         "dispatch", {}).get("sealed_at"))
            if bound:
                raise LedgerError(
                    "TERMINAL_GATE_REQUIRED",
                    "已绑定派单的尝试，其执行终态必须经 record_terminal 提交；"
                    "通用状态迁移不得伪造执行完成",
                )
        if old in ("accepted", "rejected", "cancelled", "lost"):
            # 保留晚到事件的无写入 no-op；force 也不得重开已验收尝试。
            if not force:
                return att
            raise LedgerError(
                "IMMUTABLE_STATE",
                f"尝试 {attempt_id} 已处于终态 {old}，不能通过通用状态迁移改写",
            )
        if (old in ATTEMPT_TERMINAL and status in ATTEMPT_TERMINAL
                and old != status and not force
                and (old, status) not in ALLOWED_POST_TERMINAL):
            # 首个终态优先；旧尝试晚到的事件不能改写结果
            return att
        sets = ["status=?", "updated_at=?"]
        args: list[Any] = [status, self._now_ms()]
        if status in ATTEMPT_TERMINAL and not att["terminal_at"]:
            sets.append("terminal_at=?")
            args.append(self._now_ms())
        if status in ("terminal_succeeded", "terminal_failed", "terminal_interrupted"):
            sets.append("terminal_kind=?")
            args.append(status.replace("terminal_", ""))
        if status == "running":
            sets.append("last_progress_at=?")
            args.append(self._now_ms())
        if status == "stop_requested":
            sets.append("stop_requested_at=?")
            args.append(self._now_ms())
        if extra:
            for key, value in extra.items():
                if key in ("session_meta", "metadata"):
                    value = json_dumps(value)
                sets.append(f"{key}=?")
                args.append(value)
        args.append(attempt_id)
        with transaction(self.conn):
            # 真实写事务内复核终态保护，防锁外读旧态在写入窗口被并发改为 accepted。
            cur_st = self.conn.execute(
                "SELECT status FROM attempts WHERE id=?", (attempt_id,)
            ).fetchone()
            if cur_st is not None and cur_st["status"] in ("accepted", "rejected", "cancelled", "lost"):
                if not force or (cur_st["status"] == status and not extra):
                    return self.get_attempt(attempt_id)
                raise LedgerError(
                    "IMMUTABLE_STATE",
                    f"尝试 {attempt_id} 已处于终态 {cur_st['status']}，不能改写",
                )
            self.conn.execute(f"UPDATE attempts SET {', '.join(sets)} WHERE id=?", args)
            self._audit(self.conn, actor, "attempt.transition", "attempt", attempt_id,
                        old, status, {"reason": reason})
            # 只有当前尝试才能推进任务状态
            task_row = self.conn.execute(
                "SELECT * FROM tasks WHERE id=?", (att["task_id"],)
            ).fetchone()
            if task_row:
                task = _task_from_row(task_row)
                if (task.get("current_attempt_id") == attempt_id
                        and task["status"] not in ("paused", "cancelled", "accepted")):
                    mapping = STATUS_TASK_MAP.get(status)
                    if mapping:
                        tstatus, treason = mapping
                        tsets = ["status=?", "updated_at=?"]
                        targs: list[Any] = [tstatus, self._now_ms()]
                        if treason:
                            tsets.append("blocked_reason=?")
                            targs.append(treason)
                        elif tstatus in ("running", "dispatched", "result_pending",
                                         "result_received", "accepted"):
                            tsets.append("blocked_reason=NULL")
                        targs.append(att["task_id"])
                        self.conn.execute(
                            f"UPDATE tasks SET {', '.join(tsets)} WHERE id=?", targs
                        )
                        if tstatus != task["status"]:
                            self._audit(self.conn, actor, "task.auto", "task",
                                        att["task_id"], task["status"], tstatus,
                                        {"attempt": attempt_id, "reason": treason})
        return self.get_attempt(attempt_id)

    def release_site(self, attempt_id: str, *, confirmed: bool,
                     external_stopped: bool, actor: str, note: str = "") -> dict:
        att = self.get_attempt(attempt_id)
        if att["status"] in ("accepted", "rejected", "cancelled", "lost"):
            pass
        if not confirmed:
            raise LedgerError(
                "NOT_CONFIRMED",
                "释放写入现场需确认执行已停止；仅受理停止请求不等于实际停止",
            )
        fields: dict[str, Any] = {"site_released_at": self._now_ms()}
        if att["stop_confirmed_at"] is None:
            fields["stop_confirmed_at"] = self._now_ms()
        meta = dict(att["metadata"])
        meta["external_side_effects_verified"] = bool(external_stopped)
        meta["site_release_note"] = note
        fields["metadata"] = meta
        return self.update_attempt(attempt_id, fields=fields, actor=actor,
                                   reason="site-release-confirmed")

    def stop_attempt(self, attempt_id: str, *, actor: str) -> dict:
        att = self.get_attempt(attempt_id)
        if att["site_released_at"]:
            return att
        return self.transition_attempt(
            attempt_id, "stop_requested", reason="stop-requested", actor=actor,
            extra={"stop_requested_at": self._now_ms()},
        )

    def confirm_attempt_stopped(self, attempt_id: str, *, actor: str,
                                external_stopped: bool = True, note: str = "") -> dict:
        return self.release_site(attempt_id, confirmed=True,
                                 external_stopped=external_stopped, actor=actor, note=note)

    # ---- 验收（统一受约束入口） ----------------------------------
    @staticmethod
    def _guard_rows(conn: sqlite3.Connection, attempt_id: str):
        """事务内/事务外一致的守卫快照读取；返回 (attempt, task, deliverables) 行。"""
        att = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
        if att is None:
            return None, None, []
        task = conn.execute("SELECT * FROM tasks WHERE id=?", (att["task_id"],)).fetchone()
        dels = conn.execute(
            "SELECT * FROM deliverables WHERE attempt_id=? ORDER BY id", (attempt_id,)
        ).fetchall()
        return att, task, dels

    @staticmethod
    def _guard_fingerprint(att, task, dels) -> str:
        """证据版本守卫指纹：比较相关行值/checked_at/metadata，不把毫秒 updated_at 当唯一版本。"""
        payload = {
            "attempt": {c: att[c] for c in VERIFICATION_GUARD_COLUMNS},
            "task": {c: task[c] for c in TASK_GUARD_COLUMNS} if task is not None else None,
            "deliverables": [
                {"id": d["id"], "kind": d["kind"], "target": d["target"],
                 "status": d["status"], "checked_at": d["checked_at"],
                 "evidence": json_loads(d["evidence"], None), "required": d["required"]}
                for d in dels
            ],
        }
        return sha256_text(json_dumps(payload))

    def verification_snapshot(self, attempt_id: str) -> dict:
        """只读证据守卫快照：attempt/task 关键列与交付物行的指纹。

        prepare 在事务外（只读快照）采集；submit 在事务内用同一 _guard_rows 复核。
        """
        in_tx = self.conn.in_transaction
        if not in_tx:
            self.conn.execute("BEGIN")
        try:
            att, task, dels = self._guard_rows(self.conn, attempt_id)
        finally:
            if not in_tx:
                self.conn.execute("ROLLBACK")
        if att is None:
            raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
        if task is None:
            raise LedgerError("TASK_NOT_FOUND", f"任务不存在：{att['task_id']}")
        return {
            "attempt_id": attempt_id,
            "task_id": att["task_id"],
            "fingerprint": self._guard_fingerprint(att, task, dels),
            "snapshot_at": self._now_ms(),
            "attempt": _attempt_from_row(att),
            "task": _task_from_row(task) if task is not None else None,
            "deliverables": [dict(d) for d in dels],
        }

    def submit_verification(self, attempt_id: str, *, verification: dict,
                            result: str, reason: str = "", force: bool = False,
                            actor: str = "coordinator") -> dict:
        """统一验收提交入口：同一 BEGIN IMMEDIATE 内复核并一致写入。

        只用 SQL 快照做门禁与写入，不执行文件/命令/网络 I/O。正常 pass 需要
        当前尝试 + 未暂停/取消/关闭 + 已收集标记（result_collected_at/result_path
        非空）+ 有效结果产物证据 + required 交付物/已声明检查通过；force 需要
        非空原因并持久化未满足项，但绝不豁免 current/人工控制/已关闭守卫，也不把
        active/未终态尝试变得可接受。fail 同样原子提交，拒绝覆盖已 accepted。
        重复的既有 accepted/verification_failed 历史按只读幂等返回（不覆盖）。
        """
        if result not in ("pass", "fail"):
            raise LedgerError("INVALID_RESULT", f"验收结果必须是 pass/fail：{result!r}")
        verification = verification if isinstance(verification, dict) else {}
        with transaction(self.conn):
            att_row, task_row, del_rows = self._guard_rows(self.conn, attempt_id)
            if att_row is None:
                raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
            if task_row is None:
                raise LedgerError("TASK_NOT_FOUND", f"任务不存在：{attempt_id}")
            att = _attempt_from_row(att_row)
            task = _task_from_row(task_row)
            is_current = task["current_attempt_id"] == attempt_id
            # 1) current 归属（force 不豁免；幂等读也先判断）
            if not is_current:
                raise LedgerError(
                    "NOT_CURRENT",
                    f"尝试 {attempt_id} 不是任务当前尝试；旧结果不能推进当前尝试",
                )
            # 2) 人工控制 / 已关闭保护（force 不豁免）
            if (task["status"] in ("cancelled", "paused")
                    or task.get("closed_at") is not None):
                raise LedgerError(
                    "TASK_PROTECTED",
                    f"任务 {task['id']} 处于 {task['status']}，验收被人工控制/关闭阻止",
                    {"task_status": task["status"], "closed_at": task.get("closed_at")},
                )
            # 3) 幂等只读：仅当 attempt 与 task 都处于 accepted 关闭态时读原记录
            if att["status"] == "accepted":
                if task["status"] == "accepted" and result == "pass":
                    return {"ok": True, "changed": False, "already": True,
                            "result": result, "attempt": att, "task": task,
                            "verification": att["verification"]}
                raise LedgerError(
                    "TASK_PROTECTED",
                    f"尝试 {attempt_id} 已 accepted，但任务 {task['id']} 未处于 "
                    f"accepted 关闭状态（{task['status']}）；拒绝改写",
                    {"attempt_status": att["status"], "task_status": task["status"]},
                )
            # 4) 已关闭（accepted）任务的结果不可改写
            if task["status"] == "accepted":
                raise LedgerError(
                    "TASK_PROTECTED",
                    f"任务 {task['id']} 已关闭（accepted），验收结果不可改写",
                    {"task_status": task["status"]},
                )
            # 5) 幂等只读：既有 verification_failed + 再次 fail 按原记录返回
            if att["status"] == "verification_failed" and result == "fail":
                if task["status"] != "rejected":
                    raise LedgerError("TASK_PROTECTED", "尝试与任务的失败验收状态不一致")
                return {"ok": True, "changed": False, "already": True,
                        "result": result, "attempt": att, "task": task,
                        "verification": att["verification"]}
            # 6) 可验收状态（force 不豁免 active/未终态）
            if att["status"] not in VERIFIABLE_STATES:
                raise LedgerError(
                    "NOT_READY",
                    f"尝试当前状态 {att['status']} 不能验收；需先完成结果收集",
                    {"status": att["status"]},
                )
            # 7) 证据版本守卫：prepare 采样指纹与提交时台账必须一致
            guard = verification.get("guard")
            submitted_fp = guard.get("fingerprint") if isinstance(guard, dict) else None
            if (not isinstance(guard, dict)
                    or submitted_fp != self._guard_fingerprint(att_row, task_row, del_rows)
                    or type(guard.get("snapshot_at")) is not int):
                raise LedgerError(
                    "EVIDENCE_CHANGED",
                    "验收证据快照缺失或与台账不一致；请重新准备验收后重试",
                )
            # 8) 交付物/检查门禁：只信事务内最新 DB 行，不信任 prepared 清单
            dl = [dict(r) for r in del_rows]
            gate = verification_gate_rows(dl)
            missing, unverified = gate["missing"], gate["unverified"]
            # 9) 收集标记 + 结果产物证据关联（仅 DB/快照复核；不重读文件）
            collected_marks = (type(att["result_collected_at"]) is int
                               and att["result_collected_at"] > 0 and bool(att["result_path"]))
            result_rs = verification.get("result_snapshot")
            # 9.1) 版本一致门（T35）：正常 pass 必须引用当前已提交版本；
            #      另一 collect 提交了 V2 时旧准备（V1）不得通过。
            stored_version = ((att["metadata"] or {}).get("result") or {}).get("version")
            prepared_version = (result_rs or {}).get("result_version") if isinstance(
                result_rs, dict) else None
            version_ok = (isinstance(stored_version, str) and bool(stored_version)
                          and prepared_version == stored_version)
            # 9.2) 来源门（T21/F4）：可证明为旧的未声明沿用产物与来源未知
            #     （缺基线、扫描不完整、legacy 观察不可证）都拒正常通过。
            #     legacy 例外同样走 force 留痕（M2：显式补证/有因例外），
            #     不因"status 是 present"放行。
            provenance_rows = ((att["metadata"] or {}).get("deliverable_provenance")
                               or [])
            stale_items = [p.get("target") for p in provenance_rows
                           if p.get("required") and p.get("provenance") in (
                               "unchanged", "unknown")
                           and p.get("kind") in ("file", "dir", "glob")]
            rs_ok = bool(isinstance(result_rs, dict)
                         and result_rs.get("exists") is True
                         and result_rs.get("schema") == "oc-orch.result.v1"
                         and result_rs.get("path") == att["result_path"]
                         and result_rs.get("attempt_id") == attempt_id
                         and result_rs.get("task_id") == task["id"]
                         and result_rs.get("generation") == att["generation"]
                         and result_rs.get("session_id") == att["session_id"]
                         and result_rs.get("directory") == att["directory"]
                         and result_rs.get("collected_at") == att["result_collected_at"]
                         and all(type(result_rs.get(k)) is int and result_rs[k] > 0
                                 for k in ("checked_at", "artifact_collected_at", "size"))
                         and isinstance(result_rs.get("sha256"), str)
                         and len(result_rs["sha256"]) == 64
                         and all(c in "0123456789abcdef" for c in result_rs["sha256"]))
            if result == "pass":
                if not force and not collected_marks:
                    raise LedgerError(
                        "NOT_READY",
                        "结果收集未完成（缺失 result_collected_at/result_path）；"
                        "正常通过需先 collect（或显式 --force 记录原因）",
                        {"status": att["status"]},
                    )
                if (missing or unverified) and not force:
                    raise LedgerError(
                        "AUTH_GATE_FAIL",
                        "必需交付物缺失或已声明检查未通过；不能正常通过",
                        {"missing": [d["target"] for d in missing],
                         "unverified": [d["target"] for d in unverified]},
                    )
                if stale_items and not force:
                    raise LedgerError(
                        "STALE_GATE_FAIL",
                        "存在未声明沿用的旧产物或来源未知交付物（stale）；"
                        "需显式 carry 或产物更新后重新 collect",
                        {"stale": stale_items},
                    )
                if not force and not rs_ok:
                    raise LedgerError(
                        "NOT_READY",
                        "结果产物证据无效或与台账路径不一致；需重新 collect/检查（或 --force）",
                        {"result_path": att["result_path"]},
                    )
                if not force and not version_ok:
                    raise LedgerError(
                        "RESULT_CHANGED",
                        "结果版本与当前已提交版本不一致（可能已有新 collect）；"
                        "请重新读取当前结果后验收",
                        {"stored_version": stored_version,
                         "prepared_version": prepared_version},
                    )
                if force and not (reason or "").strip():
                    raise LedgerError(
                        "FORCE_REASON_REQUIRED",
                        "人工例外（force）必须提供非空原因并保存未满足项",
                    )
                new_att_status = "accepted"
                new_task_status = "accepted"
                audit_action = "verify.pass"
            else:
                if not (reason or "").strip():
                    raise LedgerError("REASON_REQUIRED", "验收失败需要原因（--reason）")
                new_att_status = "verification_failed"
                new_task_status = "rejected"
                audit_action = "verify.fail"
            meta = att["metadata"] or {}
            side: dict[str, Any] = {}
            for key in ("active_shells", "active_children"):
                if meta.get(key):
                    side[key] = meta[key]
            if side and not force:
                raise LedgerError("SIDE_EFFECTS_ACTIVE", "仍有后台 shell/子 session；需确认停止或有因 force", side)
            unmet: list[dict[str, Any]] = []
            unmet += [{"code": "deliverable_missing", "target": d["target"]}
                      for d in missing]
            unmet += [{"code": "check_unverified", "target": d["target"]}
                      for d in unverified]
            if stale_items:
                unmet.append({"code": "deliverable_stale", "targets": stale_items})
            if not version_ok:
                unmet.append({"code": "result_version_changed",
                              "stored_version": stored_version,
                              "prepared_version": prepared_version})
            if side:
                unmet.append({"code": "side_effects", **side})
            if result == "pass":
                if not collected_marks:
                    unmet.append({"code": "collect_incomplete",
                                  "status": att["status"],
                                  "result_collected_at": att["result_collected_at"],
                                  "result_path": att["result_path"]})
                elif not rs_ok:
                    unmet.append({"code": "result_invalid",
                                  "status": att["status"],
                                  "path": att["result_path"],
                                  "error": verification.get("result_error") or "invalid-snapshot"})
            ts = self._now_ms()
            final = {
                "schema": "oc-orch.verification.v1",
                "attempt_id": attempt_id,
                "task_id": task["id"],
                "generation": att["generation"],
                "result": result,
                "classification": (
                    "forced" if (force and result == "pass")
                    else "normal_effective" if result == "pass"
                    else "fail"
                ),
                "verified_at": ts,
                "actor": actor,
                "evidence": list(verification.get("evidence") or []),
                "notes": verification.get("notes") or "",
                "forced": bool(force),
                "force_reason": reason if force else None,
                "reason": reason if result == "fail" else None,
                "missing": [d["target"] for d in missing],
                "unverified": [d["target"] for d in unverified],
                "deliverables": [{**d, "evidence": json_loads(d["evidence"], None)} for d in dl],
                "collected_at": att["result_collected_at"],
                "result_path": att["result_path"],
                "result_summary": (att.get("result_summary") or "")[:2000],
                "result_snapshot": result_rs,
                "side_effects": side or None,
                "unmet": unmet,
                "guard": {
                    "fingerprint": submitted_fp,
                    "snapshot_at": guard["snapshot_at"],
                },
            }
            self.conn.execute(
                "UPDATE attempts SET status=?, verification=?, "
                "terminal_at=COALESCE(terminal_at, ?), updated_at=? WHERE id=?",
                (new_att_status, json_dumps(final), ts, ts, attempt_id),
            )
            if new_task_status == "accepted":
                self.conn.execute(
                    "UPDATE tasks SET status=?, accepted_at=COALESCE(accepted_at, ?), "
                    "blocked_reason=NULL, updated_at=? WHERE id=?",
                    ("accepted", ts, ts, task["id"]),
                )
            else:
                self.conn.execute(
                    "UPDATE tasks SET status=?, blocked_reason=?, updated_at=? WHERE id=?",
                    ("rejected", (reason or "")[:500], ts, task["id"]),
                )
            detail = {
                "reason": reason,
                "forced": bool(force),
                "force_reason": reason if force else None,
                "actor": actor,
                "missing": [d["target"] for d in missing],
                "unverified": [d["target"] for d in unverified],
                "classification": final["classification"],
                "unmet": unmet,
            }
            self._audit(self.conn, actor, audit_action, "attempt", attempt_id,
                        att["status"], new_att_status, detail)
            self._audit(self.conn, actor, audit_action, "task", task["id"],
                        task["status"], new_task_status, detail)
            outcome = {"ok": True, "changed": True, "already": False, "result": result,
                       "attempt": self.get_attempt(attempt_id),
                       "task": self.get_task(task["id"]), "verification": final}
        return outcome

    # ---- 交付物 ---------------------------------------------------
    def add_deliverables(self, attempt_id: str, specs: Iterable[str | dict]) -> None:
        with transaction(self.conn):
            for spec in specs:
                if isinstance(spec, str):
                    item = {"kind": deliverable_kind(spec), "target": spec, "required": True}
                else:
                    item = dict(spec)
                    item.setdefault("kind", deliverable_kind(item["target"]))
                    item.setdefault("required", True)
                self.conn.execute(
                    "INSERT OR IGNORE INTO deliverables(attempt_id, kind, target, required) "
                    "VALUES(?,?,?,?)",
                    (attempt_id, item["kind"], item["target"], 1 if item["required"] else 0),
                )

    def deliverables(self, attempt_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM deliverables WHERE attempt_id=? ORDER BY id", (attempt_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def update_deliverable(self, deliverable_id: int, *, status: str, evidence: Any) -> None:
        with transaction(self.conn):
            self.conn.execute(
                "UPDATE deliverables SET status=?, evidence=?, checked_at=? WHERE id=?",
                (status, json_dumps(evidence), self._now_ms(), deliverable_id),
            )

    def check_deliverables(self, attempt_id: str, *,
                           persist: bool = True) -> list[dict]:
        """只读检查文件/目录/glob；command 类型留给 verify --run-checks。

        - persist=True（默认，兼容既有检查入口）：逐项独立事务写回
          deliverables 行（旧行为，CLI/诊断路径）。
        - persist=False（正式收集，F6/D5）：只返回**候选基础行**，不执行
          内容哈希（sha256_file）或 glob 全枚举——file/dir 仅做有界 stat
          判定 present/missing，glob 留 expected 交由 compare_provenance
          的严格扫描（含预算/完整性判定）产出权威 status/sha/manifest；
          command/note 保留 DB 既有 evidence/status（检查证据权威归
          run_checks/verify，防止候选 unverified 覆盖已写 verified 证据）。
          候选行经 commit_collection(prepared_deliverables=...) 在同一短
          事务内原子提交/回滚，交付证据不再先于正式结果独立落库。
        """
        att = self.get_attempt(attempt_id)
        out = []
        for d in self.deliverables(attempt_id):
            target = d["target"]
            if not os.path.isabs(target):
                target = os.path.join(att["directory"], target)
            target = os.path.expanduser(target)
            candidate = dict(d)
            if not persist:
                # 只出候选基础行（有界 stat，不内容哈希/glob 枚举）
                if d["kind"] == "file":
                    candidate["status"] = ("present" if os.path.isfile(target)
                                           else "missing")
                elif d["kind"] == "dir":
                    candidate["status"] = ("present" if os.path.isdir(target)
                                           else "missing")
                elif d["kind"] == "glob":
                    candidate["status"] = "expected"  # 交由 compare 严格扫描
                # command/note：保留 DB 既有 status/evidence/checked_at
                candidate["checked_at"] = None
                out.append(candidate)
                continue
            # persist=True：旧诊断行为（完整物理证据写回）
            status = "expected"
            evidence: dict[str, Any] = {"path": target}
            if d["kind"] == "file":
                if os.path.isfile(target):
                    st = os.stat(target)
                    status = "present"
                    evidence.update({
                        "size": st.st_size,
                        "mtime": int(st.st_mtime * 1000),
                        "sha256": sha256_file(target),
                    })
                else:
                    status = "missing"
            elif d["kind"] == "dir":
                status = "present" if os.path.isdir(target) else "missing"
            elif d["kind"] == "glob":
                matches = sorted(globmod.glob(target, recursive=True))
                files = [m for m in matches if os.path.isfile(m)]
                status = "present" if matches else "missing"
                evidence["matches"] = matches[:50]
                evidence["match_count"] = len(matches)
                evidence["file_count"] = len(files)
                if files:
                    evidence["sha256"] = {m: sha256_file(m) for m in files[:10]}
            elif d["kind"] == "command":
                status = "unverified"
            elif d["kind"] == "note":
                status = "manual"
            candidate["status"] = status
            candidate["evidence"] = evidence
            candidate["checked_at"] = None
            self.update_deliverable(d["id"], status=status, evidence=evidence)
            candidate["checked_at"] = self._now_ms()
            out.append(candidate)
        return out

    def missing_deliverables(self, attempt_id: str) -> list[dict]:
        return [d for d in self.deliverables(attempt_id)
                if d["required"] and d["status"] in ("expected", "missing", "unverified")]

    # ---- 执行边界与终态（受保护窄入口，D1/D3） ----------------------
    # ---- 外部副作用观察合并（A/D4：事务内窄 merge，不覆盖并发事实） ----
    def record_side_effect_observation(self, attempt_id: str, *,
                                       observation: dict,
                                       actor: str = "supervisor") -> dict:
        """合并式保存外部副作用观察（D4 三态），替代旧 att 整块 metadata 覆盖。

        - 仅允许 active_shells / active_children / side_effects_checked_at /
          side_effect_checks 四个观察子字段（白名单）；
        - 单事务内重读 metadata 再 merge：保留 execution（含 terminal/epoch/
          baseline）、collection、result、stop、carry 等并发写下的其他子段；
        - 审计与合并同事务；
        - 返回更新后的 attempt（与 get_attempt 同形）。
        """
        if not isinstance(observation, dict) or not observation:
            raise LedgerError(
                "FIELD_NOT_ALLOWED",
                "observation 必须是非空对象，且只允许 "
                f"{list(_SIDE_EFFECT_OBSERVATION_KEYS)} 观察子字段",
            )
        unknown = [k for k in observation
                   if k not in _SIDE_EFFECT_OBSERVATION_KEYS]
        if unknown:
            raise LedgerError(
                "FIELD_NOT_ALLOWED",
                f"观察子字段不在白名单：{sorted(unknown)}",
                {"allowed": list(_SIDE_EFFECT_OBSERVATION_KEYS)},
            )
        with transaction(self.conn):
            att_row = self.conn.execute(
                "SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if att_row is None:
                raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
            att = _attempt_from_row(att_row)
            merged_meta = dict(att["metadata"] or {})
            for key, value in observation.items():
                merged_meta[key] = value
            self.conn.execute(
                "UPDATE attempts SET metadata=?, updated_at=? WHERE id=?",
                (json_dumps(merged_meta), self._now_ms(), attempt_id),
            )
            self._audit(self.conn, actor, "attempt.side-effect-observation",
                        "attempt", attempt_id, None, None,
                        {"fields": sorted(observation)})
        return self.get_attempt(attempt_id)

    @staticmethod
    def _merge_execution(att: dict, execution: dict | None = None,                         **sections) -> dict:
        """合并 attempt.metadata.execution；返回新 metadata。

        execution= 传整体新边界（替换）；其余 kwargs 只合并对应子段，
        避免覆盖并发写入的 stop/观察/通知信息。
        """
        meta = dict(att["metadata"] or {})
        ex = dict(execution) if execution is not None else dict(
            meta.get("execution") or {})
        for key, value in sections.items():
            if value:
                ex[key] = value
        meta["execution"] = ex
        return meta

    def seal_execution(self, attempt_id: str, *, mode: str,
                       session_before: dict, baseline: dict | None = None,
                       allow_send: bool = True, actor: str = "coordinator",
                       reason: str = "dispatch-baseline-seal") -> dict:
        """派单前封存执行边界（D1）：一旦封存不可重新解释。

        事务内复核 current 与人工控制；已封存的尝试幂等返回原基线，
        不得用发送后的观察重建基线。
        """
        with transaction(self.conn):
            att_row = self.conn.execute(
                "SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if att_row is None:
                raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
            task_row = self.conn.execute(
                "SELECT * FROM tasks WHERE id=?", (att_row["task_id"],)).fetchone()
            task = _task_from_row(task_row) if task_row is not None else None
            att = _attempt_from_row(att_row)
            existing = (att["metadata"].get("execution") or {})
            if (existing.get("dispatch") or {}).get("sealed_at"):
                self._audit(self.conn, actor, "execution.seal-suppressed", "attempt",
                            attempt_id, None, None, {"reason": "already-sealed"})
                return {"attempt": att, "sealed": False, "already": True}
            if (task is None or task["current_attempt_id"] != attempt_id
                    or task["status"] in ("paused", "cancelled", "accepted")
                    or task.get("closed_at") is not None):
                raise LedgerError(
                    "TASK_PROTECTED",
                    f"任务已受人工控制或尝试 {attempt_id} 不是当前尝试，不能封存基线",
                )
            execution = _exec_rules.blank_execution(
                mode, session_id=att["session_id"], message_id=att["dispatch_msg_id"],
                generation=att["generation"], sealed_at=self._now_ms(),
                session_before=session_before, baseline=baseline or {},
                allow_send=allow_send)
            meta = self._merge_execution(att, execution=execution)
            self.conn.execute(
                "UPDATE attempts SET metadata=?, updated_at=? WHERE id=?",
                (json_dumps(meta), self._now_ms(), attempt_id),
            )
            self._audit(self.conn, actor, "execution.seal", "attempt", attempt_id,
                        None, att["status"],
                        {"mode": mode, "allow_send": allow_send,
                         "reason": reason,
                         "session_before": session_before,
                         "baseline": bool(baseline)})
        return {"attempt": self.get_attempt(attempt_id), "sealed": True,
                "already": False}

    def record_execution_started(self, attempt_id: str, *, started_at: int,
                                 event_id: str | None, seq: int | None = None,
                                 source: str = "sse",
                                 actor: str = "supervisor") -> dict:
        """记录可归属的执行开始（D1 epoch + turn 身份），同一事务收紧：
        - 不产生第二个 open turn；同 start_event_id 幂等；
        - REST 补建（start_event_id 为 NULL）的 open/closed turn 仅补身份不重开；
        - 已终态尝试只留审计事实，不改写状态。
        """
        if not isinstance(started_at, int) or started_at <= 0:
            return {"result": "rejected", "reason": "invalid-started-at"}
        with transaction(self.conn):
            att_row = self.conn.execute(
                "SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if att_row is None:
                raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
            att = _attempt_from_row(att_row)
            if att["status"] in ATTEMPT_TERMINAL:
                self._audit(self.conn, actor, "execution.started-ignored-terminal",
                            "attempt", attempt_id, att["status"], att["status"],
                            {"event_id": event_id, "started_at": started_at})
                return {"result": "ignored-terminal", "attempt": att}
            execution = att["metadata"].get("execution") or _exec_rules.blank_execution(
                "legacy", session_id=att["session_id"],
                message_id=att["dispatch_msg_id"], generation=att["generation"])
            epoch = dict(execution.get("epoch") or {})
            turn_result = "accepted"
            if epoch.get("started_at") == started_at and epoch.get("event_id") == event_id:
                turn_result = "duplicate"
            elif epoch.get("started_at") and started_at < int(epoch["started_at"]):
                epoch = dict(epoch, superseded=epoch.get("started_at"))
            # ---- turn 身份（收紧，见 D3） ----
            open_rows = self.conn.execute(
                "SELECT * FROM turns WHERE attempt_id=? AND end_event_id IS NULL "
                "ORDER BY id DESC", (attempt_id,)).fetchall()
            if open_rows:
                turn = open_rows[0]
                if turn["start_event_id"] is None:
                    # REST 补建的区间：迟到 started 仅补身份，不建第二个 open turn
                    self.conn.execute(
                        "UPDATE turns SET start_event_id=?, started_at=? WHERE id=?",
                        (event_id, started_at, turn["id"]),
                    )
                elif turn["start_event_id"] != event_id:
                    turn_result = "rejected-second-open-turn"
                # 同 start_event_id（或已补身份）：幂等
            else:
                prior = self.conn.execute(
                    "SELECT 1 FROM turns WHERE attempt_id=? AND start_event_id=? "
                    "AND end_event_id IS NOT NULL",
                    (attempt_id, event_id)).fetchone()
                if prior:
                    turn_result = "duplicate"
                else:
                    self.conn.execute(
                        "INSERT OR IGNORE INTO turns(attempt_id, start_event_id, "
                        "started_at) VALUES(?,?,?)", (attempt_id, event_id, started_at),
                    )
            epoch_out = epoch if epoch.get("started_at") == started_at else {
                "started_at": started_at, "source": source, "event_id": event_id,
                "seq": seq, "observed_at": self._now_ms(),
                "superseded": epoch.get("started_at") if epoch.get("started_at") else None,
            }
            execution = dict(execution, epoch={k: v for k, v in epoch_out.items()
                                               if v is not None})
            meta = self._merge_execution(att, execution=execution)
            status = att["status"]
            if status in ("creating", "dispatching", "dispatch_unknown", "dispatched",
                          "provider_retry", "idle_unconfirmed", "stalled"):
                status = "running"
                self.conn.execute(
                    "UPDATE attempts SET status=?, updated_at=?, "
                    "last_progress_at=? WHERE id=?",
                    (status, self._now_ms(), started_at, attempt_id),
                )
            self.conn.execute(
                "UPDATE attempts SET started_at=COALESCE(started_at,?), metadata=?, "
                "updated_at=?, last_observed_at=? WHERE id=?",
                (started_at, json_dumps(meta), self._now_ms(), self._now_ms(),
                 attempt_id),
            )
            self._audit(self.conn, actor, "execution.started", "attempt", attempt_id,
                        att["status"], status,
                        {"event_id": event_id, "started_at": started_at,
                         "turn": turn_result, "seq": seq})
            att = self.get_attempt(attempt_id)
        return {"result": turn_result, "attempt": att}

    def record_execution_unconfirmed(self, attempt_id: str, *, reason: str,
                                     evidence: dict | None = None,
                                     actor: str = "supervisor") -> dict:
        """合并持久化 unconfirmed 证据（D1.unconfirmed：稳定原因码 + 下一动作）。"""
        with transaction(self.conn):
            att_row = self.conn.execute(
                "SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if att_row is None:
                raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
            att = _attempt_from_row(att_row)
            execution = dict(att["metadata"].get("execution") or {})
            prev = execution.get("unconfirmed") or {}
            unconfirmed = {
                "reason": reason,
                "first_at": (prev.get("first_at") if prev.get("reason") == reason
                             else None) or self._now_ms(),
                "last_at": self._now_ms(),
                "evidence": evidence or {},
                "next_action": _exec_rules.unconfirmed_next_action(reason),
                "occurrences": (int(prev.get("occurrences") or 0) + 1
                                if prev.get("reason") == reason else 1),
            }
            execution["unconfirmed"] = unconfirmed
            merged = self._merge_execution(att, execution=execution)
            self.conn.execute(
                "UPDATE attempts SET metadata=?, updated_at=? WHERE id=?",
                (json_dumps(merged), self._now_ms(), attempt_id),
            )
        return self.get_attempt(attempt_id)

    def _task_advance_in_tx(self, att: dict, task: dict, status: str,
                            actor: str, reason: str | None) -> bool:
        """事务内按 STATUS_TASK_MAP 推进任务状态；人工控制/current 不越权。"""
        if (task.get("current_attempt_id") != att["id"]
                or task["status"] in ("paused", "cancelled", "accepted")):
            return False
        mapping = STATUS_TASK_MAP.get(status)
        if not mapping:
            return False
        tstatus, treason = mapping
        tsets = ["status=?", "updated_at=?"]
        targs: list[Any] = [tstatus, self._now_ms()]
        if treason:
            tsets.append("blocked_reason=?")
            targs.append(treason)
        elif tstatus in ("running", "dispatched", "result_pending", "result_received",
                         "accepted"):
            tsets.append("blocked_reason=NULL")
        targs.append(task["id"])
        self.conn.execute(f"UPDATE tasks SET {', '.join(tsets)} WHERE id=?", targs)
        if tstatus != task["status"]:
            self._audit(self.conn, actor, "task.auto", "task", task["id"],
                        task["status"], tstatus,
                        {"attempt": att["id"], "reason": treason or reason})
        return True

    def record_terminal(self, attempt_id: str, *, outcome: str, source: str,
                        event_id: str | None = None, seq: int | None = None,
                        reason: str | None = None, actor: str = "supervisor",
                        candidate: dict | None = None) -> dict:
        """统一窄终态事务（D3）：归属校验、turn 关闭、attempt 任务推进、审计
        在同一 BEGIN IMMEDIATE 内完成；拒绝时不收集、不发布、不释放写权。

        返回 {"result": accepted|duplicate|rejected, "reason": ...,
              "attempt": ..., "guarded": bool, "verdict": ...}。
        """
        mapping = {"succeeded": "terminal_succeeded", "failed": "terminal_failed",
                   "interrupted": "terminal_interrupted"}
        status = mapping.get(outcome)
        if status is None:
            return {"result": "rejected", "reason": "invalid-outcome",
                    "attempt": self.get_attempt(attempt_id), "guarded": False,
                    "verdict": None}
        with transaction(self.conn):
            att_row = self.conn.execute(
                "SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if att_row is None:
                raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
            task_row = self.conn.execute(
                "SELECT * FROM tasks WHERE id=?", (att_row["task_id"],)).fetchone()
            att = _attempt_from_row(att_row)
            task = _task_from_row(task_row) if task_row is not None else None
            execution = att["metadata"].get("execution")
            cand = dict(candidate or {})
            cand.setdefault("outcome", outcome)
            cand.setdefault("source", source)
            cand.setdefault("event_id", event_id)
            cand.setdefault("seq", seq)
            for key in ("ended_at", "busy_at", "started_at"):
                cand[key] = _exec_rules._int(cand.get(key))
            verdict = _exec_rules.attribute(execution, cand)
            # 已有台账终态：等价来源幂等；不同结果保留审计不改写
            if att.get("terminal_at") is not None:
                if att.get("terminal_kind") == outcome and verdict["status"] in (
                        _exec_rules.STATUS_DUPLICATE,) or (
                        att.get("terminal_kind") == outcome
                        and verdict["status"] == _exec_rules.STATUS_CONFIRMED
                        and "terminal" in (execution or {})):
                    self._audit(self.conn, actor, "terminal.duplicate", "attempt",
                                attempt_id, att["status"], att["status"],
                                {"reason": "already-terminal",
                                 "candidate_end": cand.get("ended_at")})
                    return {"result": "duplicate", "reason": "already-terminal",
                            "attempt": att, "guarded": False, "verdict": verdict,
                            "task_advanced": False}
                self._audit(self.conn, actor, "terminal.rejected", "attempt",
                            attempt_id, att["status"], att["status"],
                            {"reason": "already-terminal-other", "source": source,
                             "event_id": event_id, "ended_at": cand.get("ended_at")})
                return {"result": "rejected", "reason": "already-terminal-other",
                        "attempt": att, "guarded": False, "verdict": verdict,
                        "task_advanced": False}
            if verdict["status"] != _exec_rules.STATUS_CONFIRMED:
                unconfirmed = {
                    "reason": verdict["reason"],
                    "evidence": verdict.get("evidence"),
                    "source": source, "event_id": event_id,
                    "ended_at": cand.get("ended_at"),
                }
                execution_out = dict(execution or {})
                prev = execution_out.get("unconfirmed") or {}
                meta = dict(att["metadata"])
                execution_out["unconfirmed"] = {
                    "reason": verdict["reason"],
                    "first_at": (prev.get("first_at") if prev.get("reason") ==
                                 verdict["reason"] else None) or self._now_ms(),
                    "last_at": self._now_ms(),
                    "evidence": verdict.get("evidence"),
                    "source": source,
                    "next_action": _exec_rules.unconfirmed_next_action(verdict["reason"]),
                }
                meta["execution"] = execution_out
                self.conn.execute(
                    "UPDATE attempts SET metadata=?, updated_at=?, "
                    "last_observed_at=? WHERE id=?",
                    (json_dumps(meta), self._now_ms(), self._now_ms(), attempt_id),
                )
                self._audit(self.conn, actor, "terminal.rejected", "attempt",
                            attempt_id, att["status"], att["status"],
                            {"reason": verdict["reason"], "source": source,
                             "event_id": event_id, "ended_at": cand.get("ended_at"),
                             "classification": "stale" if verdict["status"] ==
                             _exec_rules.STATUS_STALE else "unconfirmed"})
                return {"result": "rejected", "reason": verdict["reason"],
                        "attempt": self.get_attempt(attempt_id), "guarded": False,
                        "verdict": verdict, "task_advanced": False}

            # ---- 归属已确认：turn 关闭与 attempt/task/审计一致提交 ----
            ts = self._now_ms()
            ended = cand.get("ended_at")
            open_rows = self.conn.execute(
                "SELECT * FROM turns WHERE attempt_id=? AND end_event_id IS NULL "
                "ORDER BY id DESC", (attempt_id,)).fetchall()
            interval = cand.get("interval") or {}
            epoch = (execution or {}).get("epoch") or {}
            if open_rows:
                turn = open_rows[0]
                if turn["started_at"] is not None and ended < int(turn["started_at"]):
                    self._audit(self.conn, actor, "terminal.rejected", "attempt",
                                attempt_id, att["status"], att["status"],
                                {"reason": "turn-reversed",
                                 "started_at": turn["started_at"],
                                 "ended_at": ended})
                    raise LedgerError(
                        "TURN_REVERSED",
                        f"回合开始 {turn['started_at']} 晚于候选结束 {ended}；"
                        "拒绝逆序关闭 turn",
                    )
                self.conn.execute(
                    "UPDATE turns SET end_event_id=?, ended_at=?, outcome=? WHERE id=?",
                    (event_id or f"obs-{ended}", ended, outcome, turn["id"]),
                )
            elif epoch.get("started_at"):
                self.conn.execute(
                    "INSERT INTO turns(attempt_id, start_event_id, end_event_id, "
                    "started_at, ended_at, outcome) VALUES(?,?,?,?,?,?)",
                    (attempt_id, epoch.get("event_id"), event_id or f"obs-{ended}",
                     int(epoch["started_at"]), ended, outcome),
                )
            elif interval.get("proven"):
                # 归属已由区间证明但开始时间不可得：保留 nullable started_at，
                # 不虚构 start=end（D3）。
                self.conn.execute(
                    "INSERT INTO turns(attempt_id, start_event_id, end_event_id, "
                    "started_at, ended_at, outcome) VALUES(?,?,?,NULL,?,?)",
                    (attempt_id, None, event_id or f"obs-{ended}", ended, outcome),
                )
            else:
                self._audit(self.conn, actor, "terminal.rejected", "attempt",
                            attempt_id, att["status"], att["status"],
                            {"reason": "no-open-turn-no-interval", "ended_at": ended})
                raise LedgerError(
                    "NO_TURN_BOUNDARY",
                    "缺少 open turn 且无区间证明；不能用 start=end 绕过归属校验",
                )
            terminal_evidence = {
                "outcome": outcome, "end_at": ended, "source": source,
                "event_id": event_id, "seq": seq, "evidence_version": 1,
                "recorded_at": ts,
            }
            execution_out = dict(execution or {})
            execution_out["terminal"] = terminal_evidence
            execution_out["unconfirmed"] = {}
            meta = dict(att["metadata"])
            meta["execution"] = execution_out
            sets = ["status=?", "terminal_at=?", "terminal_kind=?", "updated_at=?",
                    "metadata=?", "last_observed_at=?"]
            args: list[Any] = [status, ended, outcome, ts, json_dumps(meta), ts]
            if reason:
                sets.append("terminal_reason=?")
                args.append(reason)
            if ended > (att.get("last_progress_at") or 0):
                sets.append("last_progress_at=?")
                args.append(ended)
            args.append(attempt_id)
            self.conn.execute(
                f"UPDATE attempts SET {', '.join(sets)} WHERE id=?", args)
            task_advanced = False
            if task is not None:
                task_advanced = self._task_advance_in_tx(att, task, status, actor,
                                                         reason or f"session-{outcome}")
            self._audit(self.conn, actor, "terminal.accepted", "attempt", attempt_id,
                        att["status"], status,
                        {"outcome": outcome, "ended_at": ended, "source": source,
                         "event_id": event_id, "task_advanced": task_advanced})
            att = self.get_attempt(attempt_id)
        return {"result": "accepted", "reason": outcome, "attempt": att,
                "guarded": task is not None and task_advanced is False,
                "verdict": verdict, "task_advanced": task_advanced}

    # ---- 结果收集事务（D7/D8；任务 4.4/5.2） ------------------------
    COLLECTION_GUARD_FIELDS = (
        "status", "generation", "task_id", "session_id", "terminal_at",
        "terminal_kind", "result_collected_at", "result_path", "metadata",
    )

    def _collection_guard_fingerprint(self, att_row, task_row) -> str:
        att = _attempt_from_row(att_row)
        meta = att["metadata"] or {}
        payload = {
            "attempt": {c: att[c] for c in self.COLLECTION_GUARD_FIELDS},
            "result_version": (meta.get("result") or {}).get("version"),
            "collection_state": (meta.get("collection") or {}).get("state"),
            "task": ({c: task_row[c] for c in
                      ("status", "current_attempt_id", "closed_at")}
                     if task_row is not None else None),
        }
        return sha256_text(json_dumps(payload))

    def collection_guard(self, attempt_id: str) -> dict:
        """收集前守卫快照：current/人工控制/代次/执行证据/既有结果版本。"""
        in_tx = self.conn.in_transaction
        if not in_tx:
            self.conn.execute("BEGIN")
        try:
            att_row = self.conn.execute(
                "SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            task_row = None
            if att_row is not None:
                task_row = self.conn.execute(
                    "SELECT * FROM tasks WHERE id=?", (att_row["task_id"],)
                ).fetchone()
        finally:
            if not in_tx:
                self.conn.execute("ROLLBACK")
        if att_row is None:
            raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
        if task_row is None:
            raise LedgerError("TASK_NOT_FOUND", f"任务不存在：{attempt_id}")
        meta = _attempt_from_row(att_row)["metadata"] or {}
        return {
            "attempt_id": attempt_id,
            "task_id": att_row["task_id"],
            "fingerprint": self._collection_guard_fingerprint(att_row, task_row),
            "snapshot_at": self._now_ms(),
            "prior_version": (meta.get("result") or {}).get("version"),
            "attempt": _attempt_from_row(att_row),
            "task": _task_from_row(task_row),
        }

    def _collection_notification_target(self, task: dict) -> str | None:
        target = task.get("coordinator_session")
        if target:
            return target
        lease = self.conn.execute(
            "SELECT holder_session FROM coordinator_leases WHERE project=?",
            (task["project"],)).fetchone()
        return lease["holder_session"] if lease else None

    # commit_collection 允许写入的业务状态白名单（N1：收集事务不写验收结论；
    # accepted/rejected/verification_failed 只能经 submit_verification）。
    COLLECT_ALLOWED_ATTEMPT_STATUS = ("result_collected",)
    COLLECT_ALLOWED_TASK_STATUS = ("result_received",)
    COLLECT_FORBIDDEN_STATUS = ("accepted", "rejected", "verification_failed",
                                "cancelled", "lost")

    def _persist_prepared_deliverables_in_tx(self, attempt_id: str,
                                             prepared: list[dict],
                                             collected_at: int) -> int:
        """事务内原子提交交付候选行（F6）：与结果指针/metadata/通知同事务。

        - 候选必须按行 id 精确匹配本 attempt 的 deliverables（未知行拒绝）；
        - command/note 不在此提交（检查证据权威归 run_checks/verify，
          防止候选 unverified/manual 覆盖已写入的检查证据）；
        - checked_at 使用本次收集的 collected_at，与结果时间同源。
        """
        known = {row["id"]: row for row in self.conn.execute(
            "SELECT * FROM deliverables WHERE attempt_id=?", (attempt_id,))}
        written = 0
        for cand in prepared:
            if not isinstance(cand, dict):
                raise LedgerError(
                    "DELIVERABLE_CANDIDATE_INVALID",
                    "交付候选必须是对象行", {"candidate": repr(cand)[:100]})
            row = known.get(cand.get("id"))
            if row is None:
                raise LedgerError(
                    "DELIVERABLE_CANDIDATE_INVALID",
                    f"交付候选行 {cand.get('id')!r} 不属于尝试 {attempt_id}",
                    {"candidate": cand})
            if row["target"] != cand.get("target") or row["kind"] != cand.get("kind"):
                raise LedgerError(
                    "DELIVERABLE_CANDIDATE_INVALID",
                    f"交付候选与台账行不一致：{cand.get('target')!r}",
                    {"candidate": cand})
            if cand.get("kind") in ("command", "note"):
                continue
            status = cand.get("status")
            if status not in ("present", "missing", "expected", "failed",
                              "verified", "unverified", "manual"):
                raise LedgerError(
                    "DELIVERABLE_CANDIDATE_INVALID",
                    f"交付候选状态非法：{status!r}",
                    {"candidate": cand})
            self.conn.execute(
                "UPDATE deliverables SET status=?, evidence=?, checked_at=? "
                "WHERE id=?",
                (status, json_dumps(cand.get("evidence")), collected_at,
                 cand["id"]),
            )
            written += 1
        return written

    def commit_collection(self, attempt_id: str, *,
                          result_version: str, content_fingerprint: str,
                          collected_at: int, version_path: str, summary: str,
                          collection_state: str = "complete",
                          attempt_status: str = "result_collected",
                          task_status: str | None = None,
                          task_blocked_reason: str | None = None,
                          carry: list[dict] | None = None,
                          provenance_summary: list[dict] | None = None,
                          execution_boundary: dict | None = None,
                          prepared_deliverables: list[dict] | None = None,
                          notification: dict | None = None,
                          guard: dict | None = None,
                          actor: str = "supervisor",
                          reason: str = "collect") -> dict:
        """结果正式提交窄事务（D8）：guard 复核后一致写入结果指针、状态、
        交付候选行、carry、metadata 与通知责任（D9）。文件/网络 I/O 均在
        事务外完成。

        - 同指纹并发：返回已有版本及原 collected_at（duplicate=True）；
        - guard 已变化且候选不同版本：COLLECTION_CONFLICT，不覆盖更晚结果；
        - legacy（无 protocol 版本）允许首次升级为新版本（M2）；
        - N1：收集事务禁止写入验收结论（accepted/rejected/verification_failed/
          cancelled/lost）；必须携带合法 guard 与结果身份；验收唯一入口是
          submit_verification；
        - F6：prepared_deliverables 候选行在本事务内提交，失败整体回滚，
          不留下“结果 V1 + 交付证据 V2”的混合状态。
        """
        # ---- N1：参数级状态白名单（先于任何写入） ----------------------
        if attempt_status is not None and attempt_status not in \
                self.COLLECT_ALLOWED_ATTEMPT_STATUS:
            raise LedgerError(
                "COLLECT_STATUS_FORBIDDEN",
                f"收集事务不得把尝试状态写为 {attempt_status!r}；"
                "验收结论只能经 submit_verification 提交",
                {"attempt_status": attempt_status,
                 "allowed": list(self.COLLECT_ALLOWED_ATTEMPT_STATUS)})
        if task_status is not None and task_status not in \
                self.COLLECT_ALLOWED_TASK_STATUS:
            raise LedgerError(
                "COLLECT_STATUS_FORBIDDEN",
                f"收集事务不得把任务状态写为 {task_status!r}",
                {"task_status": task_status,
                 "allowed": list(self.COLLECT_ALLOWED_TASK_STATUS)})
        # ---- N1：结果身份必须完整合法 ----------------------------------
        if (not isinstance(result_version, str) or not result_version
                or not isinstance(content_fingerprint, str)
                or not content_fingerprint
                or not isinstance(version_path, str) or not version_path
                or type(collected_at) is not int or collected_at <= 0):
            raise LedgerError(
                "COLLECTION_IDENTITY_INVALID",
                "正式收集必须携带合法结果身份（version/fingerprint/path/"
                "collected_at）",
                {"result_version": result_version,
                 "collected_at": collected_at})
        # ---- N1：必须携带本 attempt 的合法 guard 快照 -------------------
        if (not isinstance(guard, dict)
                or not isinstance(guard.get("fingerprint"), str)
                or not guard["fingerprint"]
                or guard.get("attempt_id") != attempt_id
                or not (guard.get("prior_version") is None
                        or isinstance(guard.get("prior_version"), str))):
            raise LedgerError(
                "COLLECTION_GUARD_REQUIRED",
                "正式收集必须携带 collection_guard 快照（准备于采证之前）",
                {"guard": guard})
        if prepared_deliverables is not None and (
                not isinstance(prepared_deliverables, list)):
            raise LedgerError(
                "DELIVERABLE_CANDIDATE_INVALID",
                "prepared_deliverables 必须是候选行列表")
        with transaction(self.conn):
            att_row = self.conn.execute(
                "SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if att_row is None:
                raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
            task_row = self.conn.execute(
                "SELECT * FROM tasks WHERE id=?", (att_row["task_id"],)
            ).fetchone()
            if task_row is None:
                raise LedgerError("TASK_NOT_FOUND", f"任务不存在：{attempt_id}")
            att = _attempt_from_row(att_row)
            task = _task_from_row(task_row)
            meta = att["metadata"] or {}
            stored_result = meta.get("result") or {}
            stored_version = stored_result.get("version")
            # 1) current 与人工控制（收集不越权）
            if task["current_attempt_id"] != attempt_id:
                raise LedgerError("NOT_CURRENT",
                                  f"尝试 {attempt_id} 不是当前尝试；过时收集不能提交")
            if (task["status"] in ("paused", "cancelled")
                    or task.get("closed_at") is not None):
                raise LedgerError("TASK_PROTECTED",
                                  f"任务 {task['id']} 受人工控制/关闭，收集提交被阻止")
            # 2) 尝试状态：不可在 active/验收终态上收集
            if att["status"] in ("accepted", "rejected", "verification_failed",
                                 "cancelled", "lost"):
                raise LedgerError("IMMUTABLE_STATE",
                                  f"尝试 {attempt_id} 已处于 {att['status']}，不能收集")
            if att["status"] not in ("terminal_succeeded", "result_collected",
                                     "result_missing"):
                raise LedgerError("NOT_READY",
                                  f"尝试 {attempt_id} 状态 {att['status']} 不能形成正式结果")
            # 3) 并发/版本守卫
            if stored_version == result_version:
                # 同指纹：幂等返回已有版本与原时间，不重写交付行、不重发
                notif_row = None
                if notification:
                    notif_row = self._enqueue_notification_in_tx(
                        self.conn, dedupe_key=notification["dedupe_key"],
                        **{k: v for k, v in notification.items()
                           if k != "dedupe_key"})
                self._audit(self.conn, actor, "collection.duplicate", "attempt",
                            attempt_id, att["status"], att["status"],
                            {"version": result_version, "reason": reason})
                return {"ok": True, "duplicate": True, "version": result_version,
                        "collected_at": att.get("result_collected_at"),
                        "attempt": att, "task": task, "notification": notif_row,
                        "notification_ids": (
                            [notif_row["id"]] if notif_row else [])}
            if (guard.get("fingerprint") != self._collection_guard_fingerprint(
                    att_row, task_row)
                    or guard.get("prior_version") != stored_version):
                raise LedgerError(
                    "COLLECTION_CONFLICT",
                    "收集准备后台账已变化（并发收集/状态推进）；"
                    "请重读当前结果后重试，不以最后写入者覆盖",
                    {"stored_version": stored_version,
                     "prepared_prior": guard.get("prior_version")})
            # 4) 正式写入（只修改本用例拥有的 metadata 子段）
            ts = self._now_ms()
            new_meta = dict(meta)
            new_meta["result"] = {
                "schema": "oc-orch.result.ref.v1",
                "version": result_version,
                "content_fingerprint": content_fingerprint,
                "path": version_path,
                "collection_state": collection_state,
                "collected_at": collected_at,
                "notification_dedupe_key": notification["dedupe_key"] if notification else None,
            }
            prior_collection = meta.get("collection") or {}
            new_meta["collection"] = {
                "state": collection_state, "count": 0,
                "next_collect_at": None, "last_error": None,
                "updated_at": ts,
            }
            if prior_collection.get("recovery_execution"):
                # 恢复证据跨补收集版本保留（M2/T29 可审计）
                new_meta["collection"]["recovery_execution"] = \
                    prior_collection["recovery_execution"]
            if collection_state != "complete":
                raise LedgerError(
                    "COLLECTION_STATE_INVALID",
                    f"正式收集的 collection_state 必须是 complete：{collection_state!r}")
            if carry is not None:
                new_meta["carry_records"] = carry
            if provenance_summary is not None:
                new_meta["deliverable_provenance"] = provenance_summary
            if execution_boundary is not None:
                # legacy 恢复证据：真实成功区间 + 原损坏记录引用（F2/M2）
                new_meta["collection_recovery"] = execution_boundary
                new_meta["collection"]["recovery_execution"] = execution_boundary
            sets = ["result_collected_at=?", "result_path=?", "result_summary=?",
                    "metadata=?", "updated_at=?", "last_observed_at=?"]
            args: list[Any] = [collected_at, version_path, summary,
                               json_dumps(new_meta), ts, ts]
            if attempt_status and attempt_status != att["status"]:
                if (att["status"], attempt_status) not in ALLOWED_POST_TERMINAL and \
                        attempt_status not in ("result_collected",):
                    raise LedgerError("INVALID_TRANSITION",
                                      f"收集不得把 {att['status']} 改为 {attempt_status}")
                sets.append("status=?")
                args.append(attempt_status)
            args.append(attempt_id)
            self.conn.execute(
                f"UPDATE attempts SET {', '.join(sets)} WHERE id=?", args)
            old_task_status = task["status"]
            task_advanced = False
            if task_status and task_status != task["status"]:
                if task["status"] in ("paused", "cancelled", "accepted"):
                    raise LedgerError("TASK_PROTECTED",
                                      f"任务 {task['id']} 受人工控制，不能推进")
                tsets = ["status=?", "updated_at=?"]
                targs: list[Any] = [task_status, ts]
                if task_blocked_reason:
                    tsets.append("blocked_reason=?")
                    targs.append(task_blocked_reason)
                else:
                    tsets.append("blocked_reason=NULL")
                targs.append(task["id"])
                self.conn.execute(
                    f"UPDATE tasks SET {', '.join(tsets)} WHERE id=?", targs)
                task_advanced = True
                self._audit(self.conn, actor, "task.auto", "task", task["id"],
                            old_task_status, task_status,
                            {"attempt": attempt_id, "reason": reason})
            # 4.1) F6：交付候选行与结果/任务同一事务原子提交（回滚不留混合证据）
            deliverables_written = 0
            if prepared_deliverables:
                deliverables_written = self._persist_prepared_deliverables_in_tx(
                    attempt_id, prepared_deliverables, collected_at)
            # 5) 通知责任同事务提交（D9）
            notif_row = None
            if notification:
                notif_row = self._enqueue_notification_in_tx(
                    self.conn, dedupe_key=notification["dedupe_key"],
                    **{k: v for k, v in notification.items()
                       if k != "dedupe_key"})
                if (notif_row.get("target_session") is None
                        and notification.get("target_session") is None):
                    # 无路由：标记 route_pending（投递由补路由恢复；不烧预算）
                    nm = dict(notif_row.get("metadata") or {})
                    nm["route_pending"] = 1
                    self.conn.execute(
                        "UPDATE notifications SET metadata=? WHERE id=?",
                        (json_dumps(nm), notif_row["id"]),
                    )
            self._audit(self.conn, actor, "collection.commit", "attempt",
                        attempt_id, att["status"],
                        attempt_status or att["status"],
                        {"version": result_version, "reason": reason,
                         "task_advanced": task_advanced,
                         "deliverables_written": deliverables_written,
                         "recovery": bool(execution_boundary),
                         "notification": notif_row["id"] if notif_row else None})
            att = self.get_attempt(attempt_id)
            task = self.get_task(task["id"])
        return {"ok": True, "duplicate": False, "version": result_version,
                "collected_at": collected_at, "attempt": att, "task": task,
                "notification": notif_row,
                "notification_ids": ([notif_row["id"]] if notif_row else [])}

    def record_collection_pending(self, attempt_id: str, *, reason: str,
                                  next_collect_at: int | None,
                                  max_attempts: int,
                                  interval_ms: int,
                                  carry: list[dict] | None = None,
                                  actor: str = "supervisor") -> dict:
        """记录补收集预算（D7）：count/next_at 持久化；耗尽转 result_missing。

        重复旧事件不重置预算；人工控制/current 变化时静默保留（审计留痕）。
        返回 {"attempt", "exhausted": bool}。
        """
        with transaction(self.conn):
            att_row = self.conn.execute(
                "SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
            if att_row is None:
                raise LedgerError("ATTEMPT_NOT_FOUND", f"尝试不存在：{attempt_id}")
            task_row = self.conn.execute(
                "SELECT * FROM tasks WHERE id=?", (att_row["task_id"],)
            ).fetchone()
            att = _attempt_from_row(att_row)
            task = _task_from_row(task_row) if task_row is not None else None
            if (task is None or task["current_attempt_id"] != attempt_id
                    or task["status"] in ("paused", "cancelled", "accepted")
                    or task.get("closed_at") is not None
                    or att["status"] in ("accepted", "rejected",
                                         "verification_failed", "cancelled", "lost")):
                self._audit(self.conn, actor, "collection.pending-suppressed",
                            "attempt", attempt_id, None, None,
                            {"reason": reason[:200]})
                return {"attempt": att, "exhausted": False, "suppressed": True}
            prev = (att["metadata"] or {}).get("collection") or {}
            count = int(prev.get("count") or 0) + 1
            exhausted = count >= max_attempts
            # 已有有效版本（result_collected_at + result_path）时失败/覆盖不足
            # 只记失败责任（last_error/next_collect_at），不把 collection.state
            # 从 complete 降级，也不用失败覆盖已有结果（T25/D7）。
            has_effective_result = bool(att.get("result_collected_at")
                                        and att.get("result_path"))
            if has_effective_result:
                state = "complete"
            else:
                state = "exhausted" if exhausted else "pending"
            new_meta = dict(att["metadata"] or {})
            new_meta["collection"] = {
                "state": state,
                "count": count,
                "next_collect_at": next_collect_at,
                "last_error": (reason or "")[:500],
                "interval_ms": interval_ms,
                "max_attempts": max_attempts,
                "first_at": prev.get("first_at") or self._now_ms(),
                "updated_at": self._now_ms(),
            }
            if prev.get("recovery_execution"):
                new_meta["collection"]["recovery_execution"] = prev[
                    "recovery_execution"]
            if carry is not None:
                new_meta["carry_records"] = carry
            sets = ["metadata=?", "updated_at=?"]
            args: list[Any] = [json_dumps(new_meta), self._now_ms()]
            if exhausted and att["status"] == "terminal_succeeded":
                sets.append("status=?")
                args.append("result_missing")
            args.append(attempt_id)
            self.conn.execute(
                f"UPDATE attempts SET {', '.join(sets)} WHERE id=?", args)
            if exhausted and att["status"] == "terminal_succeeded" and task is not None:
                # 任务保持待结果（新契约），仅记录阻塞原因
                self.conn.execute(
                    "UPDATE tasks SET blocked_reason=?, updated_at=? WHERE id=? "
                    "AND status NOT IN ('accepted','cancelled','paused')",
                    ("result_missing", self._now_ms(), task["id"]),
                )
            self._audit(self.conn, actor, "collection.pending", "attempt",
                        attempt_id, att["status"],
                        "result_missing" if exhausted and
                        att["status"] == "terminal_succeeded" else att["status"],
                        {"count": count, "exhausted": exhausted,
                         "kept_effective_result": has_effective_result,
                         "reason": (reason or "")[:200],
                         "next_collect_at": next_collect_at})
        return {"attempt": self.get_attempt(attempt_id), "exhausted": exhausted,
                "suppressed": False}

    def attempts_after(self, *, status: str | None = None, after_id: str = "",
                       limit: int = 200) -> list[dict]:
        """keyset 分页（稳定游标待办恢复；T33 不因固定列表上限饥饿）。"""
        sql = "SELECT * FROM attempts WHERE id>?" + (
            " AND status=?" if status else "")
        sql += " ORDER BY id ASC LIMIT ?"
        args: list[Any] = [after_id]
        if status:
            args.append(status)
        args.append(limit)
        return [_attempt_from_row(r) for r in self.conn.execute(sql, args).fetchall()]

    def carry_records(self, attempt_id: str) -> list[dict]:
        att = self.get_attempt(attempt_id)
        return list((att["metadata"] or {}).get("carry_records") or [])

    def fill_notification_routes(self, project: str, target: str) -> int:
        """补齐 route_pending 通知的路由（coordinator 出现后；任务 5.4）。"""
        with transaction(self.conn):
            rows = self.conn.execute(
                "SELECT id, metadata FROM notifications WHERE project=? "
                "AND target_session IS NULL AND state='pending' "
                "ORDER BY created_at", (project,)).fetchall()
            for row in rows:
                meta = json_loads(row["metadata"], {})
                meta["route_pending"] = 0
                meta["route_filled_at"] = self._now_ms()
                self.conn.execute(
                    "UPDATE notifications SET target_session=?, metadata=?, "
                    "next_attempt_at=? WHERE id=?",
                    (target, json_dumps(meta), self._now_ms(), row["id"]),
                )
            if rows:
                self._audit(self.conn, "supervisor", "notification.route-filled",
                            "project", project, None, target,
                            {"count": len(rows)})
        return len(rows)

    # ---- 回合 -----------------------------------------------------
    def open_turn(self, attempt_id: str, event_id: str, started_at: int) -> dict:
        """收紧后的 open_turn：不制造第二个 open turn；结果供调用方处理。

        返回 {"result": accepted|duplicate|rejected|merged, "reason": ...}。
        """
        with transaction(self.conn):
            open_rows = self.conn.execute(
                "SELECT * FROM turns WHERE attempt_id=? AND end_event_id IS NULL "
                "ORDER BY id DESC", (attempt_id,)).fetchall()
            if open_rows:
                turn = open_rows[0]
                if turn["start_event_id"] == event_id:
                    return {"result": "duplicate", "reason": "open-turn-same-event"}
                if turn["start_event_id"] is None:
                    self.conn.execute(
                        "UPDATE turns SET start_event_id=?, started_at=? WHERE id=?",
                        (event_id, started_at, turn["id"]))
                    return {"result": "merged", "reason": "backfilled-identity"}
                return {"result": "rejected", "reason": "second-open-turn"}
            prior = self.conn.execute(
                "SELECT 1 FROM turns WHERE attempt_id=? AND start_event_id=? AND "
                "end_event_id IS NOT NULL", (attempt_id, event_id)).fetchone()
            if prior:
                return {"result": "duplicate", "reason": "closed-turn-same-event"}
            self.conn.execute(
                "INSERT OR IGNORE INTO turns(attempt_id, start_event_id, started_at) "
                "VALUES(?,?,?)", (attempt_id, event_id, started_at))
        return {"result": "accepted", "reason": "created"}

    def close_turn(self, attempt_id: str, event_id: str, ended_at: int,
                   outcome: str) -> dict:
        """收紧后的 close_turn：拒绝逆序与无凭据补造；返回明确结果。

        返回 {"result": accepted|duplicate|rejected, "reason": ...}。
        """
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM turns WHERE attempt_id=? AND end_event_id IS NULL "
                "ORDER BY id DESC LIMIT 1", (attempt_id,)).fetchone()
            if row:
                if row["end_event_id"] == event_id:
                    return {"result": "duplicate", "reason": "turn-closed-same-event"}
                if row["started_at"] is not None and ended_at < int(row["started_at"]):
                    return {"result": "rejected", "reason": "turn-reversed"}
                self.conn.execute(
                    "UPDATE turns SET end_event_id=?, ended_at=?, outcome=? WHERE id=?",
                    (event_id, ended_at, outcome, row["id"]),
                )
                return {"result": "accepted", "reason": "closed-open-turn"}
            return {"result": "rejected", "reason": "no-open-turn"}

    def turns(self, attempt_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM turns WHERE attempt_id=? ORDER BY id", (attempt_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ---- 事件 -----------------------------------------------------
    def record_event(self, *, event_id: str, type_: str, session_id: str | None,
                     task_id: str | None, attempt_id: str | None,
                     seq: int | None, version: int | None, created_ms: int | None,
                     payload: dict) -> dict:
        observed = self._now_ms()
        stale = 0
        if attempt_id and created_ms:
            att = self.get_attempt(attempt_id)
            base = att.get("started_at") or att.get("dispatched_at")
            if base and created_ms < base - 2000:
                stale = 1
        with transaction(self.conn):
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO events(event_id, type, session_id, task_id, "
                "attempt_id, seq, version, created_ms, observed_at, stale, payload) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (event_id, type_, session_id, task_id, attempt_id, seq, version,
                 created_ms, observed, stale, json_dumps(payload)),
            )
            duplicate = cur.rowcount == 0
            if duplicate:
                self.conn.execute(
                    "UPDATE events SET duplicate=duplicate+1 WHERE event_id=?", (event_id,)
                )
        return {"duplicate": duplicate, "stale": bool(stale), "observed_at": observed}

    def event_seen(self, event_id: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM events WHERE event_id=?", (event_id,)
        ).fetchone() is not None

    def pending_terminal_events(self, attempt_id: str) -> list[dict]:
        """本尝试已保存、尚未被确认为终态的 execution.* 事件（T07 重启补证）。"""
        rows = self.conn.execute(
            "SELECT event_id, type, created_ms, payload FROM events "
            "WHERE attempt_id=? AND type LIKE 'session.execution.%' "
            "ORDER BY created_ms", (attempt_id,),
        ).fetchall()
        out = []
        for r in rows:
            payload = json_loads(r["payload"], {})
            outcome = payload.get("terminal")
            if outcome in ("succeeded", "failed", "interrupted"):
                out.append({"event_id": r["event_id"], "created_ms": r["created_ms"],
                            "terminal": outcome})
        return out

    # ---- 通知：有效性策略落地（domain.notification_policy；D1/19 类）---------
    _DUE_SWEEP_BATCH = 64          # 旧代次清理每批上限
    _DUE_SWEEP_MAX_ROUNDS = 8      # 每次 due 清理轮次上限（有界载入）
    _DUE_CANDIDATE_FETCH = 256     # 单次 due 候选取数上限（不再整队列 fetchall）
    _DEFER_RECONCILE_INTERVAL_MS = 5 * 60 * 1000  # defer 可见待决的有界补证间隔

    def _lookup_contexts_in_tx(self, conn: sqlite3.Connection,
                               notifs: list[dict]) -> tuple[dict, dict]:
        """批量读取批次成员涉及的 task/attempt 行，避免逐行查询。"""
        task_ids = sorted({n.get("task_id") for n in notifs if n.get("task_id")})
        attempt_ids = sorted({n.get("attempt_id") for n in notifs if n.get("attempt_id")})
        tasks: dict[str, dict] = {}
        attempts: dict[str, dict] = {}
        if task_ids:
            ph = ",".join("?" for _ in task_ids)
            for r in conn.execute(f"SELECT * FROM tasks WHERE id IN ({ph})", (*task_ids,)):
                tasks[r["id"]] = _task_from_row(r)
        if attempt_ids:
            ph = ",".join("?" for _ in attempt_ids)
            for r in conn.execute(f"SELECT * FROM attempts WHERE id IN ({ph})", (*attempt_ids,)):
                attempts[r["id"]] = _attempt_from_row(r)
        return tasks, attempts

    def _insert_alert_in_tx(self, conn: sqlite3.Connection, *, severity: str,
                            kind: str, title: str, detail: str, dedupe_key: str,
                            task_id: str | None = None,
                            attempt_id: str | None = None) -> bool:
        """事务内去重告警；生成稳定有效 alert ID（可定点 ACK）。"""
        if conn.execute("SELECT 1 FROM alerts WHERE dedupe_key=? AND state='new'",
                        (dedupe_key,)).fetchone():
            return False
        aid = short_id("AL-", 12)
        conn.execute(
            "INSERT INTO alerts(id, severity, kind, title, detail, task_id, attempt_id, "
            "dedupe_key, state, created_at) VALUES(?,?,?,?,?,?,?,?,'new',?)",
            (aid, severity, kind, title, detail[:4000], task_id, attempt_id,
             dedupe_key, self._now_ms()),
        )
        self._audit(conn, "system", "alert.create", "alert", aid, None, "new",
                    {"severity": severity, "kind": kind})
        return True

    def _cleanup_responsibility_in_tx(self, conn: sqlite3.Connection,
                                      n: dict) -> str | None:
        """旧 attempt 是否仍有必须保留的清理责任；返回稳定原因或 None。"""
        att = conn.execute(
            "SELECT site_released_at, stop_requested_at, stop_confirmed_at, metadata "
            "FROM attempts WHERE id=?", (n.get("attempt_id"),)).fetchone()
        if att is None:
            return None
        if att["site_released_at"] is None:
            return "site-still-held"
        if att["stop_requested_at"] is not None and att["stop_confirmed_at"] is None:
            return "stop-unconfirmed"
        meta = json_loads(att["metadata"], {})
        if meta.get("active_shells") or meta.get("active_children"):
            return "side-effects-active"
        return None

    def _record_cleanup_responsibility(self, conn: sqlite3.Connection, n: dict,
                                       cleanup: str,
                                       current_attempt_id: str) -> bool:
        """抑制后的去重清理责任：以 (attempt, 清理条件) 去重；有效 alert ID、
        正常 severity；不保留通知 pending、不 ACK、不释放写权。"""
        dedupe = f"cleanup-required:{n['attempt_id']}:{cleanup}"
        return self._insert_alert_in_tx(
            conn, severity="warning", kind="notification_cleanup_required",
            title=f"旧执行仍持有清理责任：{cleanup}",
            detail=(f"attempt={n.get('attempt_id')} kind={n.get('kind')} "
                    f"cleanup={cleanup} current={current_attempt_id}"),
            dedupe_key=dedupe, task_id=n.get("task_id"),
            attempt_id=n.get("attempt_id"))

    def _mark_notification_dead(self, conn: sqlite3.Connection, n: dict,
                                last_error: str, dec: dict) -> None:
        """策略抑制终态：dead，保留证据、不递增预算、不当已读 ACK。"""
        conn.execute(
            "UPDATE notifications SET state='dead', last_error=?, claim_owner=NULL, "
            "claim_at=NULL WHERE id=? AND state IN ('pending','sent','inflight')",
            (last_error[:800], n["id"]),
        )
        self._audit(conn, "system", "notification.policy-dead",
                    "notification", n["id"], n.get("state"), "dead",
                    {"kind": n.get("kind"), "reason": dec.get("reason"),
                     "task_id": n.get("task_id"), "attempt_id": n.get("attempt_id"),
                     "current_attempt_id": dec.get("current_attempt_id")})

    def _mark_notification_deferred(self, conn: sqlite3.Connection, n: dict,
                                    reason: str, now: int, dec: dict) -> None:
        """defer 可见待决责任：保持 pending、不发送、last_error 记录原因、
        有界补证期限、去重诊断告警（未知/不可归属等必须可查询可见）。"""
        meta = dict(n.get("metadata") or {})
        meta["policy_deferred"] = {"action": "defer", "reason": reason}
        conn.execute(
            "UPDATE notifications SET last_error=?, next_attempt_at=?, metadata=?, "
            "claim_owner=NULL, claim_at=NULL WHERE id=?",
            (f"deferred:{reason}"[:800], now + self._DEFER_RECONCILE_INTERVAL_MS,
             json_dumps(meta), n["id"]),
        )
        self._audit(conn, "system", "notification.policy-defer",
                    "notification", n["id"], n.get("state"), "pending",
                    {"reason": reason, "kind": n.get("kind")})
        self._insert_alert_in_tx(
            conn, severity="warning", kind="notification_policy_deferred",
            title=f"通知待补证：{reason}",
            detail=(f"n={n.get('id')} kind={n.get('kind')} reason={reason} "
                    f"task={n.get('task_id') or '-'} attempt={n.get('attempt_id') or '-'}"),
            dedupe_key=f"policy-defer:{n.get('id')}:{reason}",
            task_id=n.get("task_id"), attempt_id=n.get("attempt_id"))

    def _decide_notification(self, conn: sqlite3.Connection, n: dict,
                             tasks: dict, attempts: dict, *, now: int) -> str:
        """strategy 落地：返回 keep/dead/deferred；副作用与调用方同事务。

        - send（含 legacy-unscoped / coordinator-live）→ keep；
        - suppress：task-cancelled 保留交给 supervisor 显式 ACK 回收（不静默
          dead、不把清理任务静默吞掉）；其余（superseded / condition-resolved /
          task-accepted / task-closed / site-released）→ dead；superseded 且旧
          attempt 仍持写权/停止未确认/活动副作用 → 另记清理责任；

        - cleanup_required → dead + 清理责任；
        - defer：task-paused 保留（supervisor defer）；其余 → 可见待决补证。
        """
        if n.get("state") not in ("pending", "sent", "inflight"):
            return "keep"
        task = tasks.get(n.get("task_id"))
        attempt = attempts.get(n.get("attempt_id"))
        dec = notification_decision(n, task=task, attempt=attempt)
        action, reason = dec["action"], dec.get("reason") or ""
        if action == P_SEND:
            return "keep"
        if action == P_SUPPRESS:
            if reason == P_R_TASK_CANCELLED:
                return "keep"  # 人工取消回收 ACK：现有系统语义保持
            self._mark_notification_dead(conn, n, f"{action}:{reason}", dec)
            if reason == P_R_SUPERSEDED and n.get("attempt_id"):
                cleanup = self._cleanup_responsibility_in_tx(conn, n)
                if cleanup:
                    self._record_cleanup_responsibility(
                        conn, n, cleanup, dec.get("current_attempt_id") or "unknown")
            return "dead"
        if action == P_CLEANUP:
            self._mark_notification_dead(conn, n, f"{action}:{reason}", dec)
            self._record_cleanup_responsibility(
                conn, n, reason, dec.get("current_attempt_id") or "unknown")
            return "dead"
        if reason == P_R_TASK_PAUSED:
            return "keep"  # supervisor 负责 defer（保留既有计数/顺序语义）
        self._mark_notification_deferred(conn, n, reason, now, dec)
        return "deferred"

    def _sweep_superseded_in_tx(self, conn: sqlite3.Connection, *, now: int) -> int:
        """有界清理 SQL 可证的旧代次积压：每批/每轮有上限，不整队载入。"""
        changed = 0
        for _ in range(self._DUE_SWEEP_MAX_ROUNDS):
            rows = conn.execute(
                "SELECT n.* FROM notifications n JOIN tasks t ON t.id=n.task_id "
                "WHERE n.state IN ('pending','sent') AND n.next_attempt_at<=? "
                "AND n.attempt_id IS NOT NULL AND n.task_id IS NOT NULL "
                "AND t.current_attempt_id IS NOT NULL "
                "AND t.current_attempt_id != n.attempt_id "
                "ORDER BY n.priority DESC, n.created_at ASC, n.id ASC LIMIT ?",
                (now, self._DUE_SWEEP_BATCH),
            ).fetchall()
            if not rows:
                break
            notifs = [_notification_from_row(r) for r in rows]
            tasks, attempts = self._lookup_contexts_in_tx(conn, notifs)
            round_changed = 0
            for n in notifs:
                if self._decide_notification(conn, n, tasks, attempts,
                                             now=now) != "keep":
                    round_changed += 1
            changed += round_changed
            if round_changed == 0:
                break
        return changed

    def _enqueue_notification_in_tx(
        self, conn: sqlite3.Connection, *,
        kind: str, target_session: str | None, dedupe_key: str,
        payload: dict, project: str | None = None,
        task_id: str | None = None, attempt_id: str | None = None,
        priority: int = 0, max_attempts: int = 3,
        next_attempt_at: int | None = None, metadata: dict | None = None,
    ) -> dict:
        """事务内通知入队 helper（commit_collection 同事务复用；不新开事务）。

        产生端策略：payload 非 object 按 invalid-notification 立即标记为
        defer 可见待决（不发送、有界补证），正常持久 payload 必须 object。
        """
        ts = self._now_ms()
        due = next_attempt_at if next_attempt_at is not None else ts
        meta_out = dict(metadata or {})
        last_error_out = None
        payload_object = isinstance(payload, dict)
        if not payload_object:
            meta_out["policy_deferred"] = {"action": "defer",
                                           "reason": "invalid-notification"}
            last_error_out = "deferred:invalid-notification"
            due = ts + self._DEFER_RECONCILE_INTERVAL_MS
        nid = short_id("N-", 12)
        prior = conn.execute(
            "SELECT * FROM notifications WHERE dedupe_key=? "
            "ORDER BY created_at DESC LIMIT 1", (dedupe_key,),
        ).fetchone()
        if prior and prior["state"] in ("acked", "dead"):
            # 同一事实只通知一次；重试/重入不得复活已消费/已终止通知。
            self._audit(conn, "system", "notification.suppressed-terminal",
                        "notification", prior["id"], prior["state"], prior["state"],
                        {"kind": kind, "dedupe_key": dedupe_key})
            return _notification_from_row(prior)
        existing = conn.execute(
            "SELECT * FROM notifications WHERE dedupe_key=? AND state IN "
            "('pending','inflight','sent')", (dedupe_key,),
        ).fetchone()
        if existing:
            if not payload_object:
                # 坏 payload 不合并正文；仅写 defer 可见补证标记与期限。
                conn.execute(
                    "UPDATE notifications SET last_error=?, next_attempt_at=?, "
                    "metadata=? WHERE id=?",
                    (last_error_out, due, json_dumps(meta_out), existing["id"]),
                )
                return _notification_from_row(conn.execute(
                    "SELECT * FROM notifications WHERE id=?", (existing["id"],)
                ).fetchone())
            old_payload = json_loads(existing["payload"], {})
            merged = dict(old_payload)
            merged.update(payload)
            if "events" in old_payload and "events" in payload:
                merged["events"] = old_payload["events"] + payload["events"]
            # 只有 pending 才允许提前；sent/inflight 的 next_attempt_at 是
            # ack/nudge 期限，不能被同一事实的再次入队重置为“现在”。
            same_result = bool(payload.get("result_version") and (
                payload.get("result_version") == old_payload.get("result_version")
                or payload.get("bridged_from_notification") == existing["id"]))
            next_due = (min(int(existing["next_attempt_at"]), due)
                        if existing["state"] == "pending" and not same_result
                        else int(existing["next_attempt_at"]))
            conn.execute(
                "UPDATE notifications SET payload=?, priority=MAX(priority,?), "
                "next_attempt_at=? WHERE id=?",
                (json_dumps(merged), priority, next_due, existing["id"]),
            )
            return _notification_from_row(conn.execute(
                "SELECT * FROM notifications WHERE id=?", (existing["id"],)
            ).fetchone())
        conn.execute(
            "INSERT INTO notifications(id, project, kind, target_session, task_id, "
            "attempt_id, dedupe_key, payload, state, priority, next_attempt_at, "
            "created_at, max_attempts, metadata, last_error) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (nid, project, kind, target_session, task_id, attempt_id, dedupe_key,
             json_dumps(payload), "pending", priority, due, ts, max_attempts,
             json_dumps(meta_out), last_error_out),
        )
        self._audit(conn, "system", "notification.enqueue", "notification",
                    nid, None, "pending",
                    {"kind": kind, "task": task_id, "attempt": attempt_id,
                     "target": target_session})
        return _notification_from_row(conn.execute(
            "SELECT * FROM notifications WHERE id=?", (nid,)).fetchone())

    def enqueue_notification(
        self,
        *,
        kind: str,
        target_session: str | None,
        dedupe_key: str,
        payload: dict,
        project: str | None = None,
        task_id: str | None = None,
        attempt_id: str | None = None,
        priority: int = 0,
        max_attempts: int = 3,
        next_attempt_at: int | None = None,
        metadata: dict | None = None,
    ) -> dict:
        with transaction(self.conn):
            return self._enqueue_notification_in_tx(
                self.conn, kind=kind, target_session=target_session,
                dedupe_key=dedupe_key, payload=payload, project=project,
                task_id=task_id, attempt_id=attempt_id, priority=priority,
                max_attempts=max_attempts, next_attempt_at=next_attempt_at,
                metadata=metadata)

    def get_notification(self, nid: str) -> dict:
        row = self.conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
        if not row:
            raise LedgerError("NOTIFICATION_NOT_FOUND", f"通知不存在：{nid}")
        return _notification_from_row(row)

    def list_notifications(self, *, target_session: str | None = None,
                           state: str | None = None, limit: int = 100) -> list[dict]:
        sql = "SELECT * FROM notifications"
        clauses, args = [], []
        if target_session:
            clauses.append("target_session=?")
            args.append(target_session)
        if state:
            clauses.append("state=?")
            args.append(state)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        return [_notification_from_row(r) for r in self.conn.execute(sql, args).fetchall()]

    def legacy_result_notification(self, attempt_id: str) -> dict | None:
        """定点读取唯一旧结果通知，不受全局列表上限影响；重名历史不猜测。"""
        rows = self.conn.execute(
            "SELECT * FROM notifications WHERE dedupe_key=? LIMIT 2",
            (f"result-ready:{attempt_id}",)).fetchall()
        return _notification_from_row(rows[0]) if len(rows) == 1 else None

    def due_notifications(self, *, now: int, limit: int = 50) -> list[dict]:
        """到期待投递通知；有效性策略在事务内落地（D1 / 19 类已登记 kind）。

        - **有界清理旧积压**：先对 SQL 可证 superseded 的旧代次分批 sweep
          （每批 _DUE_SWEEP_BATCH、至多 _DUE_SWEEP_MAX_ROUNDS 轮），标记
          dead / 清理责任 / 待补证 defer；单次载入有上限，不整队列 fetchall；
        - **候选 SQL 资格预筛**排除 superseded（旧积压不占窗，健康目标的新
          有效结果不饥饿，至多两轮 reconcile 即发起投递）；再按策略过滤：
          send/legacy-unscoped 保留；condition-resolved/superseded → dead；
          cleanup → dead + 清理责任；task-paused 保留（supervisor defer）；
          task-cancelled 保留（显式 ACK 回收，不静默 dead）；
        - paused 任务排序在后并保留既有计数语义；cancelled/closed 仍返回由
          Supervisor ack 回收。
        - 单次至多取 max(limit*2, _DUE_CANDIDATE_FETCH) 行候选，处理后按原序
          返回 kept[:limit]。
        """
        with transaction(self.conn):
            self._sweep_superseded_in_tx(self.conn, now=now)
        fetch = max(limit * 2, self._DUE_CANDIDATE_FETCH)
        rows = self.conn.execute(
            "SELECT n.* FROM notifications n LEFT JOIN tasks t ON t.id=n.task_id "
            "WHERE n.state IN ('pending','sent') AND n.next_attempt_at<=? "
            "AND n.target_session IS NOT NULL "
            "AND (t.id IS NULL OR n.attempt_id IS NULL OR n.task_id IS NULL "
            "     OR t.current_attempt_id IS NULL "
            "     OR t.current_attempt_id = n.attempt_id "
            "     OR t.status IN ('paused','cancelled')) "
            "ORDER BY CASE WHEN t.status='paused' THEN 0 ELSE 1 END DESC, "
            "n.priority DESC, n.created_at ASC, n.id ASC LIMIT ?",
            (now, fetch),
        ).fetchall()
        if not rows:
            return []
        notifs = [_notification_from_row(r) for r in rows]
        kept: list[dict] = []
        with transaction(self.conn):
            tasks, attempts = self._lookup_contexts_in_tx(self.conn, notifs)
            for n in notifs:
                if self._decide_notification(self.conn, n, tasks, attempts,
                                             now=now) == "keep":
                    kept.append(n)
        return kept[:limit]

    def claim_notifications(self, ids: list[str], owner: str) -> list[dict]:
        if not ids:
            return []
        ts = self._now_ms()
        placeholders = ",".join("?" for _ in ids)
        with transaction(self.conn):
            # D1/S1：claim 事务内复核（due→claim 窗口 current 可再变化）。
            raw = self.conn.execute(
                "SELECT n.* FROM notifications n LEFT JOIN tasks t ON t.id=n.task_id "
                f"WHERE n.id IN ({placeholders}) AND (t.id IS NULL OR "
                "(t.status NOT IN ('paused','cancelled','accepted') AND t.closed_at IS NULL))",
                (*ids,),
            ).fetchall()
            notifs = [_notification_from_row(r) for r in raw]
            tasks, attempts = self._lookup_contexts_in_tx(self.conn, notifs)
            keep_ids: list[str] = []
            for n in notifs:
                if self._decide_notification(self.conn, n, tasks, attempts,
                                             now=ts) == "keep":
                    if n["state"] in ("pending", "sent"):
                        keep_ids.append(n["id"])
            if not keep_ids:
                return []
            keep_ph = ",".join("?" for _ in keep_ids)
            self.conn.execute(
                f"UPDATE notifications SET state='inflight', claim_owner=?, claim_at=? "
                f"WHERE id IN ({keep_ph}) AND state IN ('pending','sent')",
                (owner, ts, *keep_ids),
            )
            claimed = self.conn.execute(
                f"SELECT * FROM notifications WHERE id IN ({keep_ph}) "
                f"AND claim_owner=? AND state='inflight'",
                (*keep_ids, owner),
            ).fetchall()
        return [_notification_from_row(r) for r in claimed]

    def recover_inflight(self, *, older_than_ms: int = 120000) -> int:
        cutoff = self._now_ms() - older_than_ms
        with transaction(self.conn):
            cur = self.conn.execute(
                "UPDATE notifications SET state='pending', claim_owner=NULL, claim_at=NULL, "
                "last_error='recovered-after-crash' WHERE state='inflight' AND claim_at<?",
                (cutoff,),
            )
        return cur.rowcount

    def mark_notification_sent(self, nid: str, *, message_id: str,
                               next_attempt_at: int, owner: str) -> None:
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM notifications WHERE id=?", (nid,)
            ).fetchone()
            # ACK 可在事务外的发送/查询窗口先完成；晚到结果不能重开责任。
            if not row or row["state"] in ("acked", "dead"):
                return
            # R5：同一物理 message 的晚到/重复确认绝不重复计 attempts（不同
            # message 的再次发送仍允许，兼容既有 ACK 超时夹具的换新消息语义）。
            if row["state"] == "sent" and row["message_id"] == message_id:
                return
            self.conn.execute(
                "UPDATE notifications SET state='sent', attempts=attempts+1, "
                "message_id=?, sent_at=?, next_attempt_at=?, claim_owner=NULL, "
                "claim_at=NULL, last_error=NULL WHERE id=?",
                (message_id, self._now_ms(), next_attempt_at, nid),
            )
            self._audit(self.conn, "supervisor", "notification.sent", "notification",
                        nid, "inflight", "sent", {"message_id": message_id, "owner": owner})

    def mark_notification_resumed(self, nid: str, *, message_id: str,
                                  next_attempt_at: int, reason: str,
                                  owner: str) -> bool:
        """原消息仍在 inbox 时把通知恢复为 sent；不重复计 attempts。

        用于空闲目标上“自有排队唤醒”的原 ID 恢复（PATCH steer）。物理发送
        早已发生过，恢复只更新身份与 ACK 窗口；acking/dead 不重开，inflight
        交给在飞发送者，message_id 不匹配时拒绝覆盖。
        """
        now = self._now_ms()
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM notifications WHERE id=?", (nid,)
            ).fetchone()
            if not row or row["state"] in ("acked", "dead", "inflight"):
                return False
            if row["message_id"] != message_id:
                return False
            from_state = row["state"]
            self.conn.execute(
                "UPDATE notifications SET state='sent', sent_at=?, next_attempt_at=?, "
                "claim_owner=NULL, claim_at=NULL, last_error=NULL WHERE id=?",
                (now, next_attempt_at, nid),
            )
            self._audit(self.conn, "supervisor", "notification.resumed", "notification",
                        nid, from_state, "sent",
                        {"message_id": message_id, "reason": reason[:200],
                         "owner": owner})
            return True

    def mark_notification_retry(self, nid: str, *, error: str, next_attempt_at: int,
                                owner: str, count: bool = True) -> None:
        with transaction(self.conn):
            row = self.conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
            if not row or row["state"] in ("acked", "dead"):
                return
            attempts = row["attempts"] + (1 if count else 0)
            state = "pending" if attempts < row["max_attempts"] else "dead"
            self.conn.execute(
                "UPDATE notifications SET state=?, attempts=?, next_attempt_at=?, "
                "claim_owner=NULL, claim_at=NULL, last_error=? WHERE id=?",
                (state, attempts, next_attempt_at, error[:800], nid),
            )
            self._audit(self.conn, "supervisor", "notification.retry", "notification",
                        nid, "inflight", state, {"error": error[:300], "attempts": attempts})

    def set_notification_message(self, nid: str, *, message_id: str) -> None:
        """发送前写入候选 message id；响应丢失时可由 inbox 事件确认。"""
        with transaction(self.conn):
            self.conn.execute(
                "UPDATE notifications SET message_id=? WHERE id=? AND state='inflight'",
                (message_id, nid),
            )

    def defer_notification(self, nid: str, *, next_attempt_at: int,
                           reason: str) -> None:
        with transaction(self.conn):
            self.conn.execute(
                "UPDATE notifications SET next_attempt_at=?, claim_owner=NULL, "
                "claim_at=NULL, state=CASE WHEN state='inflight' THEN 'pending' ELSE state END "
                "WHERE id=?",
                (next_attempt_at, nid),
            )
            self._audit(self.conn, "supervisor", "notification.defer", "notification",
                        nid, None, None, {"reason": reason})

    def mark_notification_dead(self, nid: str, *, error: str, owner: str) -> None:
        with transaction(self.conn):
            cur = self.conn.execute(
                "UPDATE notifications SET state='dead', last_error=?, claim_owner=NULL, "
                "claim_at=NULL WHERE id=? AND state!='acked'",
                (error[:800], nid),
            )
            if cur.rowcount:
                self._audit(self.conn, "supervisor", "notification.dead", "notification",
                            nid, "inflight", "dead", {"error": error[:300], "owner": owner})

    def expire_notifications(self, *, now: int, ack_timeout: int) -> list[dict]:
        """已投递但超过 ack 期限且用尽次数的通知 → dead，并返回需要告警的项。"""
        rows = self.conn.execute(
            "SELECT * FROM notifications WHERE state='sent' AND attempts>=max_attempts "
            "AND COALESCE(sent_at,0) + ? <= ?",
            (ack_timeout, now),
        ).fetchall()
        out = []
        for row in rows:
            with transaction(self.conn):
                cur = self.conn.execute(
                    "UPDATE notifications SET state='dead', last_error='unacked-timeout' "
                    "WHERE id=? AND state='sent'",
                    (row["id"],),
                )
                if cur.rowcount:
                    self._audit(self.conn, "system", "notification.expired", "notification",
                                row["id"], "sent", "dead",
                                {"attempts": row["attempts"]})
            if cur.rowcount:
                out.append(_notification_from_row(row))
        return out

    def ack_notifications(self, ids: list[str], *, actor: str) -> int:
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with transaction(self.conn):
            cur = self.conn.execute(
                f"UPDATE notifications SET state='acked', acked_at=? "
                f"WHERE id IN ({placeholders}) AND state IN ('pending','sent','inflight','dead')",
                (self._now_ms(), *ids),
            )
            self._audit(self.conn, actor, "notification.ack", "notification", ",".join(ids),
                        None, "acked", {"count": len(ids)})
        return cur.rowcount

    def ack_notifications_for_task(self, task_id: str, *, actor: str) -> int:
        rows = self.conn.execute(
            "SELECT id FROM notifications WHERE task_id=? AND state IN "
            "('pending','sent','inflight','dead')",
            (task_id,),
        ).fetchall()
        return self.ack_notifications([r["id"] for r in rows], actor=actor)

    def ack_notifications_for_attempt(self, attempt_id: str, *, actor: str) -> int:
        rows = self.conn.execute(
            "SELECT id FROM notifications WHERE attempt_id=? AND state IN "
            "('pending','sent','inflight','dead')",
            (attempt_id,),
        ).fetchall()
        return self.ack_notifications([r["id"] for r in rows], actor=actor)

    def requeue_notification(self, nid: str, *, next_attempt_at: int,
                            actor: str) -> dict:
        with transaction(self.conn):
            self.conn.execute(
                "UPDATE notifications SET state='pending', next_attempt_at=?, "
                "claim_owner=NULL, claim_at=NULL, last_error=NULL WHERE id=?",
                (next_attempt_at, nid),
            )
            self._audit(self.conn, actor, "notification.requeue", "notification",
                        nid, None, "pending", {})
        return self.get_notification(nid)

    def retarget_tasks(self, project: str, session_id: str, *, actor: str) -> int:
        with transaction(self.conn):
            cur = self.conn.execute(
                "UPDATE tasks SET coordinator_session=?, updated_at=? WHERE project=? "
                "AND status NOT IN ('accepted','cancelled')",
                (session_id, self._now_ms(), project),
            )
            self._audit(self.conn, actor, "coordinator.retarget-tasks", "project",
                        project, None, session_id, {"count": cur.rowcount})
        return cur.rowcount

    # ---- provider 维度退避 ---------------------------------------
    def record_provider_state(self, model: str, *, category: str, status: int | None,
                              until_ms: int, reason: str) -> None:
        key = "provider_state"
        with transaction(self.conn):
            row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            data = json_loads(row["value"], {}) if row else {}
            data[model] = {
                "until": until_ms,
                "category": category,
                "status": status,
                "reason": reason[:300],
                "updated_at": self._now_ms(),
            }
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json_dumps(data)),
            )
            self._audit(self.conn, "supervisor", "provider.state", "model", model,
                        None, category, {"until": until_ms, "status": status})

    def clear_provider_state(self, model: str, *, reason: str) -> None:
        key = "provider_state"
        with transaction(self.conn):
            row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            data = json_loads(row["value"], {}) if row else {}
            if model in data:
                old = data.pop(model)
                self.conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, json_dumps(data)),
                )
                self._audit(self.conn, "supervisor", "provider.clear", "model", model,
                            old.get("category"), None, {"reason": reason})

    def provider_states(self) -> dict:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key='provider_state'"
        ).fetchone()
        return json_loads(row["value"], {}) if row else {}

    # ---- 告警 -----------------------------------------------------
    def add_alert(self, *, severity: str, kind: str, title: str, detail: str,
                  dedupe_key: str, task_id: str | None = None,
                  attempt_id: str | None = None) -> dict | None:
        aid = short_id("AL-", 12)
        ts = self._now_ms()
        with transaction(self.conn):
            existing = self.conn.execute(
                "SELECT * FROM alerts WHERE dedupe_key=? AND state='new'", (dedupe_key,)
            ).fetchone()
            if existing:
                return None
            self.conn.execute(
                "INSERT INTO alerts(id, severity, kind, title, detail, task_id, attempt_id, "
                "dedupe_key, state, created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (aid, severity, kind, title, detail[:4000], task_id, attempt_id,
                 dedupe_key, "new", ts),
            )
            self._audit(self.conn, "system", "alert.create", "alert", aid, None, "new",
                        {"severity": severity, "kind": kind})
        return self.get_alert(aid)

    def get_alert(self, alert_id: str) -> dict:
        row = self.conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
        if not row:
            raise LedgerError("ALERT_NOT_FOUND", f"告警不存在：{alert_id}")
        return _alert_from_row(row)

    def list_alerts(self, *, state: str | None = "new", limit: int = 100) -> list[dict]:
        sql = "SELECT * FROM alerts"
        args: list[Any] = []
        if state:
            sql += " WHERE state=?"
            args.append(state)
        sql += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        return [_alert_from_row(r) for r in self.conn.execute(sql, args).fetchall()]

    def ack_alert(self, alert_id: str, *, actor: str) -> dict:
        with transaction(self.conn):
            self.conn.execute(
                "UPDATE alerts SET state='acked', acked_at=? WHERE id=? AND state='new'",
                (self._now_ms(), alert_id),
            )
            self._audit(self.conn, actor, "alert.ack", "alert", alert_id, "new", "acked")
        return self.get_alert(alert_id)

    def mark_alert_delivered(self, alert_id: str) -> None:
        with transaction(self.conn):
            self.conn.execute(
                "UPDATE alerts SET delivered_at=? WHERE id=?", (self._now_ms(), alert_id)
            )

    # ---- 协调者租约 -----------------------------------------------
    def coordinator_lease(self, project: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM coordinator_leases WHERE project=?", (project,)
        ).fetchone()
        return dict(row) if row else None

    def claim_coordinator(self, *, project: str, session_id: str, actor: str,
                          takeover: bool = False, old_confirmed_stopped: bool = False,
                          old_session_active: bool = False, ttl: int = 300) -> dict:
        ts = self._now_ms()
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM coordinator_leases WHERE project=?", (project,)
            ).fetchone()
            if row is None:
                self.conn.execute(
                    "INSERT INTO coordinator_leases(project, holder_session, epoch, "
                    "acquired_at, renewed_at, expires_at) VALUES(?,?,?,?,?,?)",
                    (project, session_id, 1, ts, ts, ts + ttl * 1000),
                )
                self._audit(self.conn, actor, "coordinator.claim", "coordinator", project,
                            None, session_id, {"epoch": 1})
            else:
                current = dict(row)
                if current["holder_session"] != session_id:
                    expired = current["expires_at"] <= ts
                    if old_session_active and not (old_confirmed_stopped and takeover):
                        raise LedgerError(
                            "COORDINATOR_ACTIVE",
                            f"项目 {project} 的协调者 {current['holder_session']} 仍可能活跃；"
                            "先确认其停止，或等待租约过期",
                            {"lease": current},
                        )
                    if not (expired or takeover):
                        raise LedgerError(
                            "LEASE_HELD",
                            f"项目 {project} 的协调者租约由 {current['holder_session']} 持有",
                            {"lease": current},
                        )
                    if takeover and not old_confirmed_stopped:
                        raise LedgerError(
                            "TAKEOVER_NOT_CONFIRMED",
                            "接管必须确认旧协调者已停止或已隔离",
                        )
                    new_epoch = current["epoch"] + 1
                    self.conn.execute(
                        "UPDATE coordinator_leases SET holder_session=?, epoch=?, "
                        "acquired_at=?, renewed_at=?, expires_at=?, failure_count=0, "
                        "recovery_blocked=0 WHERE project=?",
                        (session_id, new_epoch, ts, ts, ts + ttl * 1000, project),
                    )
                    # 未消费的通知改投新协调者
                    self.conn.execute(
                        "UPDATE notifications SET target_session=?, next_attempt_at=? "
                        "WHERE project=? AND target_session=? "
                        "AND state IN ('pending','sent')",
                        (session_id, ts, project, current["holder_session"]),
                    )
                    self.conn.execute(
                        "UPDATE tasks SET coordinator_session=?, updated_at=? WHERE project=? "
                        "AND status NOT IN ('accepted','cancelled')",
                        (session_id, ts, project),
                    )
                    self._audit(self.conn, actor, "coordinator.takeover", "coordinator",
                                project, current["holder_session"], session_id,
                                {"epoch": new_epoch, "old_session_active": old_session_active,
                                 "confirmed_stopped": old_confirmed_stopped})
                else:
                    self.conn.execute(
                        "UPDATE coordinator_leases SET renewed_at=?, expires_at=? WHERE project=?",
                        (ts, ts + ttl * 1000, project),
                    )
            return dict(self.conn.execute(
                "SELECT * FROM coordinator_leases WHERE project=?", (project,)
            ).fetchone())

    def release_coordinator(self, project: str, *, actor: str) -> None:
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM coordinator_leases WHERE project=?", (project,)
            ).fetchone()
            if row:
                self.conn.execute(
                    "DELETE FROM coordinator_leases WHERE project=?", (project,)
                )
                self._audit(self.conn, actor, "coordinator.release", "coordinator",
                            project, row["holder_session"], None, {})

    def defer_notifications_for_session(self, session_id: str, *,
                                        next_attempt_at: int, reason: str) -> int:
        with transaction(self.conn):
            cur = self.conn.execute(
                "UPDATE notifications SET state='pending', next_attempt_at=?, "
                "claim_owner=NULL, claim_at=NULL, last_error=? "
                "WHERE target_session=? AND state IN ('pending','inflight','sent')",
                (next_attempt_at, reason[:300], session_id),
            )
            self._audit(self.conn, "supervisor", "notification.defer-session",
                        "session", session_id, None, None,
                        {"count": cur.rowcount, "reason": reason})
        return cur.rowcount

    def resurface_pending_notifications(self, session_id: str, *,
                                        next_attempt_at: int, reason: str,
                                        actor: str = "operator") -> int:
        """把尚未投递的 pending 通知提前到指定时刻；不触碰已发送的 sent。

        人工解锁 manual 闸门后使用：只回收 ``state='pending'``，避免把已送达
        未 ACK 的通知重新投递造成重复输入。
        """
        with transaction(self.conn):
            cur = self.conn.execute(
                "UPDATE notifications SET next_attempt_at=?, "
                "claim_owner=NULL, claim_at=NULL, last_error=? "
                "WHERE target_session=? AND state='pending'",
                (next_attempt_at, reason[:300], session_id),
            )
            if cur.rowcount:
                self._audit(self.conn, actor, "notification.resurface",
                            "session", session_id, None, str(cur.rowcount),
                            {"reason": reason[:300]})
        return cur.rowcount

    def record_coordinator_failure(self, project: str, *, reason: str,
                                   key: str | None = None) -> dict:
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM coordinator_leases WHERE project=?", (project,)
            ).fetchone()
            if not row:
                raise LedgerError("NO_LEASE", f"项目 {project} 没有协调者租约")
            now = self._now_ms()
            if key:
                meta_key = f"coord_failure_key:{project}"
                prev = self.conn.execute(
                    "SELECT value FROM meta WHERE key=?", (meta_key,)
                ).fetchone()
                if prev and prev["value"] == key:
                    self._audit(self.conn, "supervisor", "coordinator.failure-duplicate",
                                "coordinator", project, None, str(row["failure_count"]),
                                {"reason": reason[:300], "key": key})
                    return dict(row)
                self.conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (meta_key, key),
                )
            count = row["failure_count"] + 1
            blocked = 1 if count >= 2 else row["recovery_blocked"]
            self.conn.execute(
                "UPDATE coordinator_leases SET failure_count=?, last_failure_at=?, "
                "recovery_blocked=? WHERE project=?",
                (count, now, blocked, project),
            )
            self._audit(self.conn, "supervisor", "coordinator.failure", "coordinator",
                        project, None, str(count), {"reason": reason[:300]})
            return dict(self.conn.execute(
                "SELECT * FROM coordinator_leases WHERE project=?", (project,)
            ).fetchone())

    def reset_coordinator_failure(self, project: str, *, reason: str) -> None:
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM coordinator_leases WHERE project=?", (project,)
            ).fetchone()
            if row and (row["failure_count"] or row["recovery_blocked"]):
                self.conn.execute(
                    "UPDATE coordinator_leases SET failure_count=0, recovery_blocked=0 "
                    "WHERE project=?",
                    (project,),
                )
                self._audit(self.conn, "supervisor", "coordinator.reset", "coordinator",
                            project, str(row["failure_count"]), "0", {"reason": reason})

    def block_coordinator_recovery(self, project: str, *, reason: str) -> None:
        with transaction(self.conn):
            self.conn.execute(
                "UPDATE coordinator_leases SET recovery_blocked=1 WHERE project=?",
                (project,),
            )
            self._audit(self.conn, "supervisor", "coordinator.recovery-blocked",
                        "coordinator", project, None, "blocked", {"reason": reason})

    def list_coordinator_leases(self, *, session_id: str | None = None) -> list[dict]:
        sql, args = "SELECT * FROM coordinator_leases", ()
        if session_id is not None:
            sql += " WHERE holder_session=?"
            args = (session_id,)
        return [dict(row) for row in self.conn.execute(sql, args).fetchall()]

    def notification_for_message(self, message_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM notifications WHERE message_id=? AND state IN ('pending','inflight')",
            (message_id,),
        ).fetchone()
        return _notification_from_row(row) if row else None

    def list_audit(self, limit: int = 50) -> list[dict]:
        rows = self.conn.execute(
            "SELECT at,actor,action,entity,entity_id,from_state,to_state,detail "
            "FROM audit ORDER BY id DESC LIMIT ?", (max(1, min(limit, 500)),),
        ).fetchall()
        return [{**dict(row), "detail": (row["detail"] or "")[:1000]} for row in rows]

    # ---- 快照 -----------------------------------------------------
    def counts(self) -> dict:
        out = {}
        for table in ("tasks", "attempts", "notifications", "alerts", "events"):
            out[table] = self.conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        out["notifications_pending"] = self.conn.execute(
            "SELECT COUNT(*) AS c FROM notifications WHERE state IN ('pending','inflight','sent')"
        ).fetchone()["c"]
        out["alerts_new"] = self.conn.execute(
            "SELECT COUNT(*) AS c FROM alerts WHERE state='new'"
        ).fetchone()["c"]
        return out

    def export(self) -> dict:
        """台账 JSON 导出（**不是**可恢复备份；恢复请用 SQLite backup API）。

        ``schema`` 保持 ``oc-orch.export.v1``：既有键
        （tasks/attempts/deliverables/turns/notifications/alerts/leases/audit）
        原样保留，新增 ``meta``/``events``/``session_inputs``/
        ``responsibilities`` 为**追加**字段（见 ``additive_keys``），并显式声明
        ``recoverable_backup=False``——JSON 导出缺少 WAL/索引/事务事实，不能当
        备份恢复使用。
        """
        from .. import SCHEMA_VERSION

        def rows(sql: str) -> list[dict]:
            return [dict(r) for r in self.conn.execute(sql).fetchall()]

        meta = {r["key"]: r["value"] for r in self.conn.execute(
            "SELECT key, value FROM meta ORDER BY key").fetchall()}
        events = []
        for row in self.conn.execute(
                "SELECT * FROM events ORDER BY observed_at, event_id").fetchall():
            item = dict(row)
            item["payload"] = json_loads(item.get("payload"), {})
            events.append(item)
        inputs = []
        for row in self.conn.execute(
                "SELECT * FROM session_inputs ORDER BY created_at, input_id").fetchall():
            item = dict(row)
            item["notification_ids"] = json_loads(item.get("notification_ids"), [])
            item["metadata"] = json_loads(item.get("metadata"), {})
            inputs.append(item)
        responsibilities = []
        for row in self.conn.execute(
                "SELECT * FROM responsibilities ORDER BY created_at, id").fetchall():
            item = dict(row)
            item["evidence"] = json_loads(item.get("evidence"), {})
            responsibilities.append(item)

        return {
            "schema": "oc-orch.export.v1",
            "schema_version": SCHEMA_VERSION,
            "exported_at": self._now_ms(),
            "recoverable_backup": False,
            "additive_keys": ["meta", "events", "session_inputs",
                              "responsibilities"],
            "tasks": [_task_from_row(r) for r in self.conn.execute(
                "SELECT * FROM tasks ORDER BY created_at"
            ).fetchall()],
            "attempts": [_attempt_from_row(r) for r in self.conn.execute(
                "SELECT * FROM attempts ORDER BY created_at"
            ).fetchall()],
            "deliverables": rows("SELECT * FROM deliverables ORDER BY id"),
            "turns": rows("SELECT * FROM turns ORDER BY id"),
            "notifications": [_notification_from_row(r) for r in self.conn.execute(
                "SELECT * FROM notifications ORDER BY created_at"
            ).fetchall()],
            "alerts": rows("SELECT * FROM alerts ORDER BY created_at"),
            "leases": rows("SELECT * FROM coordinator_leases"),
            "audit": rows("SELECT * FROM audit ORDER BY id"),
            # ---- additive（v1 兼容） -----------------------------------
            "meta": meta,
            "events": events,
            "session_inputs": inputs,
            "responsibilities": responsibilities,
        }

    def set_next_checks(self, ts: int) -> int:
        """派生调度字段：非终态任务的下一次补偿检查时间。"""
        with transaction(self.conn):
            cur = self.conn.execute(
                "UPDATE tasks SET next_check_at=? WHERE status NOT IN "
                "('accepted','cancelled','paused')",
                (ts,),
            )
        return cur.rowcount

    def status_snapshot(self, *, supervisor: dict | None = None,
                        service: dict | None = None) -> dict:
        tasks = self.list_tasks(limit=200)
        attempts = {a["id"]: a for a in self.list_attempts(limit=500)}
        out_tasks = []
        now = self._now_ms()
        for t in tasks:
            cur = attempts.get(t.get("current_attempt_id"))
            out_tasks.append({
                "id": t["id"],
                "project": t["project"],
                "title": t["title"],
                "status": t["status"],
                "blocked_reason": t.get("blocked_reason"),
                "coordinator_session": t.get("coordinator_session"),
                "current_attempt": cur["id"] if cur else None,
                "current_attempt_status": cur["status"] if cur else None,
                "session_id": cur.get("session_id") if cur else None,
                "generation": cur.get("generation") if cur else None,
                "last_progress_at": cur.get("last_progress_at") if cur else None,
                "deadline_at": cur.get("deadline_at") if cur else None,
                "next_check_at": t.get("next_check_at"),
                "overdue": bool(t.get("next_check_at") and t["next_check_at"] < now),
                "accepted_at": t.get("accepted_at"),
            })
        return {
            "schema": "oc-orch.status.v1",
            "generated_at": now,
            "supervisor": supervisor or {},
            "service": service or {},
            "counts": self.counts(),
            "tasks": out_tasks,
            "attempts": list(attempts.values()),
            "notifications": self.list_notifications(state="pending", limit=50),
            "sent_unacked": self.list_notifications(state="sent", limit=50),
            "alerts": self.list_alerts(state="new", limit=50),
            "provider_backoffs": self.provider_states(),
            "leases": [dict(r) for r in self.conn.execute(
                "SELECT * FROM coordinator_leases"
            ).fetchall()],
        }

    # ---- 调度者只读聚合（coordinator-first Web） ---------------------
    # 只读查询：为“调度者入口”页面提供全量归属/计数事实，不改领域状态、
    # 不受既有快照限量（200/500）影响。具体对象详情仍走 get_task/
    # list_attempts/deliverables 等既有读方法。

    def task_coordinator_rows(self) -> list[dict]:
        """全量轻量任务行，用于归属聚合与全量计数（不被 200 项快照截断）。

        返回 id/project/title/status/blocked_reason/coordinator_session/
        updated_at/last_progress_at/current_attempt_id 的只读投影。
        """
        rows = self.conn.execute(
            "SELECT id, project, title, status, blocked_reason, "
            "coordinator_session, updated_at, current_attempt_id FROM tasks"
        ).fetchall()
        return [dict(r) for r in rows]

    def distinct_coordinator_sessions(self) -> list[str]:
        """全部任务显式绑定的调度者 session ID（去重、任意状态）。

        与租约持有者并集后才构成“全部已知调度者”候选集合；worker session
        不在此列（worker 只出现在 attempts.session_id，不自动成为调度者）。
        """
        rows = self.conn.execute(
            "SELECT DISTINCT coordinator_session AS sid FROM tasks "
            "WHERE coordinator_session IS NOT NULL AND coordinator_session <> ''"
        ).fetchall()
        return [r["sid"] for r in rows]

    def notification_target_sessions(self) -> list[str]:
        """通知目标 session 去重列表（**全量、无任意截断**；按 sid 稳定排序）。

        用于调度者发现的一路补充来源：仅代表“有调度通知/台账关联的已知对象”
        （生产创建通知均经 coordinator_for / coordinator 恢复与权限 / task→coordinator
        路径路由，可作为关联证据；**单独的“收到过通知”历史不等于在线或当前负责**，
        在线/负责由 active 快照与显式绑定/租约事实判定），
        不单独证明其是 coordinator（worker 若被显式 target_session 投递过通知也会
        出现在这里；归属到哪个调度者由页面 sources 明确标注）。不排序会导致历史
        行数不同时 DISTINCT 结果顺序不稳定，因此这里固定按 sid 排序。
        """
        rows = self.conn.execute(
            "SELECT DISTINCT target_session AS sid FROM notifications "
            "WHERE target_session IS NOT NULL AND target_session <> '' "
            "ORDER BY sid"
        ).fetchall()
        return [r["sid"] for r in rows]

    def notification_target_counts(self) -> dict[str, int]:
        """按目标 session 统计**未终结**通知条数（pending/inflight/sent；全量无截断）。

        计数仅供“通知 N”徽标展示；调度者发现使用 notification_target_sessions()
        的全量 DISTINCT，不依赖本计数是否非零。
        """
        rows = self.conn.execute(
            "SELECT target_session AS sid, COUNT(*) AS c FROM notifications "
            "WHERE target_session IS NOT NULL AND target_session <> '' "
            "AND state IN ('pending','inflight','sent') "
            "GROUP BY target_session ORDER BY sid"
        ).fetchall()
        return {r["sid"]: int(r["c"]) for r in rows if r["sid"]}

    def list_unbound_tasks_page(self, *, after_id: str | None = None,
                                limit: int = 100) -> dict:
        """未绑定任务的 keyset 分页（按 id 升序稳定次序；全量，无任意行数上限）。

        “未绑定”= 任务无显式 coordinator_session，且其 project 无任何项目租约
        持有者（与 _effective_coordinator 归属语义一致）。返回
        {rows, full_count, next_after_id, truncated}；fetch limit+1 判更多，不断言
        “只剩 N 项”。只读查询，不经快照 200 项限制。
        """
        lim = max(1, min(int(limit), 500))
        args: list = []
        after_clause = ""
        if after_id is not None:
            after_clause = "AND t.id > ? "
            args.append(after_id)
        rows = self.conn.execute(
            "SELECT * FROM tasks t "
            "WHERE (t.coordinator_session IS NULL OR t.coordinator_session = '') "
            "AND NOT EXISTS (SELECT 1 FROM coordinator_leases l "
            "                WHERE l.project = t.project "
            "                AND l.holder_session IS NOT NULL) "
            + after_clause +
            "ORDER BY t.id ASC LIMIT ?",
            (*args, lim + 1)).fetchall()
        page = [_task_from_row(r) for r in rows]
        truncated = len(page) > lim
        page = page[:lim]
        next_after_id = page[-1]["id"] if truncated and page else None
        return {"rows": page, "full_count": self.count_unbound_tasks(),
                "next_after_id": next_after_id, "truncated": truncated}

    def count_unbound_tasks(self) -> int:
        """未绑定任务全量精确计数（供总览“全部未绑定”入口标注，全量不截断）。"""
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM tasks t "
            "WHERE (t.coordinator_session IS NULL OR t.coordinator_session = '') "
            "AND NOT EXISTS (SELECT 1 FROM coordinator_leases l "
            "                WHERE l.project = t.project "
            "                AND l.holder_session IS NOT NULL)"
        ).fetchone()
        return int(row["c"]) if row else 0

    def list_notifications_for_task(self, task_id: str, *, limit: int = 50) -> list[dict]:
        """指向某任务的通知（任意状态，最近优先）。"""
        rows = self.conn.execute(
            "SELECT * FROM notifications WHERE task_id=? "
            "ORDER BY created_at DESC LIMIT ?", (task_id, max(1, min(int(limit), 500))),
        ).fetchall()
        return [_notification_from_row(r) for r in rows]

    def count_attempts_for_task(self, task_id: str) -> int:
        """某任务全部尝试的精确计数（分页/截断提示用；只读）。"""
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM attempts WHERE task_id=?", (task_id,)
        ).fetchone()
        return int(row["c"]) if row else 0

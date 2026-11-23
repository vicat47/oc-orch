"""对话控制持久化：meta 命名空间 + 短事务，schema1 严格校验、原子 CAS 与审计。

逻辑持久格式（只使用现有 ``meta`` 表，**不新建表、不改全局 SQLite schema**，
也不删除 target-gate 命名空间的 provider 门）：

- schema 键 ``coordinator-control:schema`` = ``"1"``；
- 项目记录 ``coordinator-control:project:{project}``（JSON）：
  ``schema/project/status/revision/session_id/operation_id/last_operation/
  last_fingerprint/reason/evidence/updated_at``；
- session 记录 ``coordinator-control:session:{session_id}``（JSON）：
  ``schema/session_id/revision/hold/barrier/watermark/updated_at``；
- **稳定 operation 索引** ``coordinator-control:operation:{operation_id}``
  （JSON）：``schema/operation_id/action/project/session_id/fingerprint/
  reason/evidence_hash/revision_after/applied_at``。幂等以索引为准：同
  operation_id + 同指纹 → ``already-applied``（不改当前状态）；同 id + 参数
  变更 → ``operation-conflict``；索引与应用同事务写入并审计、跨重启保持，
  避免“pause(op1)->resume(op2)->pause(op3) 后旧 op1 重放成为新暂停或冒认
  新出现”。

状态变更与 ``audit`` 写入同事务（entity=project|session，from/to 快照态），
崩溃不留“已改状态却无审计”。

安全不变量（与 target-gate 一致）：

- 写参数前置守卫：缺失/类型错误返回稳定 ``refused`` 原因（fail-closed，
  不写、不改、不造默认放行）；
- 每次读取统一严格校验；坏字段/损坏 JSON 抛 :class:`CoordinatorControlStateError`，
  5 类写入方法在事务内回滚，不把损坏记录当无控制放行；
- ``pause/resume`` 同事务复核 coordinator_leases 全部相关语义：无租约、租约
  已过期（``expires_at``）、recovery_blocked（停用/被阻断）、非 holder、或
  expected_epoch 与当前代次不符 → 拒绝（C07/C08：停用、过期、交接后旧协调者
  的晚到操作不再取得调度权）；
- 控制读取（get_project/get_session/permits）不做网络、无副作用，父在同一
  BEGIN IMMEDIATE 事务内重查可安全复用。

返回形状统一为：成功（含幂等）``{"executed": True, "refused": None,
"applied": bool, "reason", "revision", "epoch", "snapshot", ...}``；拒绝
``executed=False, refused=<代码>, refused_detail``。损坏记录抛异常（状态未知）。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from ..domain import coordinator_control as rules
from ..domain.coordinator_control import CoordinatorControlStateError
from .sqlite_db import meta_get, meta_set, transaction

SCHEMA_KEY = "coordinator-control:schema"
PROJECT_KEY_PREFIX = "coordinator-control:project:"
SESSION_KEY_PREFIX = "coordinator-control:session:"
OPERATION_KEY_PREFIX = "coordinator-control:operation:"

# 稳定拒绝代码（domain 常量 + 租约语义，adapter 直接引用；供测试/上层断言）。
INVALID_EPOCH = rules.R_INVALID_EPOCH
LEASE_MISSING = "lease-missing"
LEASE_RECOVERY_BLOCKED = "lease-recovery-blocked"
LEASE_EXPIRED = "lease-expired"
LEASE_HOLDER_MISMATCH = "lease-holder-mismatch"
EPOCH_MISMATCH = "epoch-mismatch"


def _is_epoch(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _is_rev(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _validate_operation_record(rec: Any, operation_id: str) -> None:
    """持久 operation 索引记录验证；任何坏字段抛 CoordinatorControlStateError。"""
    if not isinstance(rec, dict):
        raise CoordinatorControlStateError(
            f"coordinator-control operation 记录非法（{operation_id}）：非 dict")
    if rec.get("schema") != rules.CONTROL_SCHEMA:
        raise CoordinatorControlStateError(
            f"coordinator-control operation schema 非法（{operation_id}）："
            f"{rec.get('schema')!r}")
    if rec.get("operation_id") != operation_id or not rules.is_nk_str(operation_id):
        raise CoordinatorControlStateError(
            f"coordinator-control operation 记录非法：operation_id 不匹配")
    if rec.get("action") not in ("pause", "resume"):
        raise CoordinatorControlStateError(
            f"coordinator-control operation 记录非法（{operation_id}）："
            f"action={rec.get('action')!r}")
    for key in ("project", "session_id", "fingerprint", "reason"):
        if not isinstance(rec.get(key), str):
            raise CoordinatorControlStateError(
                f"coordinator-control operation 记录非法（{operation_id}）："
                f"{key} 非 str")
    if not rules.is_nk_str(rec.get("fingerprint")):
        raise CoordinatorControlStateError(
            f"coordinator-control operation 记录非法（{operation_id}）："
            "fingerprint 缺失")
    if rec.get("evidence_hash") is not None and not isinstance(
            rec.get("evidence_hash"), str):
        raise CoordinatorControlStateError(
            f"coordinator-control operation 记录非法（{operation_id}）："
            "evidence_hash 非 str/None")
    if not _is_rev(rec.get("revision_after")):
        raise CoordinatorControlStateError(
            f"coordinator-control operation 记录非法（{operation_id}）："
            "revision_after 非非负整数")
    if rules.valid_ms(rec.get("applied_at")) is None:
        raise CoordinatorControlStateError(
            f"coordinator-control operation 记录非法（{operation_id}）："
            "applied_at 超界/非法")


class CoordinatorControlLedger:
    """项目调谐意图 / session 回合屏障与 hold 的 SQLite/meta 实现。

    ``conn`` 为已有台账连接（与 Ledger 共用），``clock`` 提供 ``now_ms()``。
    不做网络、不执行子进程，也不读取 OpenCode 服务。只读方法无副作用，可在
    父的 BEGIN IMMEDIATE 事务内安全重查。
    """

    def __init__(self, conn: sqlite3.Connection, clock: Any):
        self._conn = conn
        self._clock = clock

    # ---- 内部辅助 ----------------------------------------------------
    def _now_ms(self) -> int:
        return int(self._clock.now_ms())

    def _read_schema(self) -> None:
        raw = meta_get(self._conn, SCHEMA_KEY)
        if raw is None:
            return
        try:
            current = int(raw)
        except (TypeError, ValueError) as exc:
            raise CoordinatorControlStateError(
                f"coordinator-control schema 记录损坏：{raw!r}") from exc
        if current != rules.CONTROL_SCHEMA:
            raise CoordinatorControlStateError(
                f"coordinator-control schema 不兼容：{current} != "
                f"{rules.CONTROL_SCHEMA}")

    def _ensure_schema(self) -> None:
        self._read_schema()
        if meta_get(self._conn, SCHEMA_KEY) is None:
            meta_set(self._conn, SCHEMA_KEY, str(rules.CONTROL_SCHEMA))

    @staticmethod
    def _project_key(project: str) -> str:
        return f"{PROJECT_KEY_PREFIX}{project}"

    @staticmethod
    def _session_key(session_id: str) -> str:
        return f"{SESSION_KEY_PREFIX}{session_id}"

    @staticmethod
    def _operation_key(operation_id: str) -> str:
        return f"{OPERATION_KEY_PREFIX}{operation_id}"

    def _load_project(self, project: str) -> dict | None:
        raw = meta_get(self._conn, self._project_key(project))
        if raw is None:
            return None
        try:
            rec = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise CoordinatorControlStateError(
                f"coordinator-control 项目记录损坏(JSON)：{project}") from exc
        rules.validate_project_record(rec, project)
        return rec

    def _load_session(self, session_id: str) -> dict | None:
        raw = meta_get(self._conn, self._session_key(session_id))
        if raw is None:
            return None
        try:
            rec = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise CoordinatorControlStateError(
                f"coordinator-control session 记录损坏(JSON)：{session_id}") from exc
        rules.validate_session_record(rec, session_id)
        return rec

    def _load_operation(self, operation_id: str) -> dict | None:
        """读稳定 operation 索引；损坏一律 fail-closed（不当作新操作放行）。"""
        raw = meta_get(self._conn, self._operation_key(operation_id))
        if raw is None:
            return None
        try:
            rec = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise CoordinatorControlStateError(
                f"coordinator-control operation 记录损坏(JSON)：{operation_id}"
            ) from exc
        _validate_operation_record(rec, operation_id)
        return rec

    def _store_project(self, rec: dict) -> None:
        rules.validate_project_record(rec, rec.get("project") or "")
        meta_set(self._conn, self._project_key(rec["project"]),
                 json.dumps(rec, ensure_ascii=False))

    def _store_session(self, rec: dict) -> None:
        rules.validate_session_record(rec, rec.get("session_id") or "")
        meta_set(self._conn, self._session_key(rec["session_id"]),
                 json.dumps(rec, ensure_ascii=False))

    def _store_operation(self, rec: dict) -> None:
        _validate_operation_record(rec, rec.get("operation_id") or "")
        meta_set(self._conn, self._operation_key(rec["operation_id"]),
                 json.dumps(rec, ensure_ascii=False))

    def _audit(self, actor: str, action: str, entity: str, entity_id: str,
               from_state: Any, to_state: Any, detail: dict | None) -> None:
        self._conn.execute(
            "INSERT INTO audit(at, actor, action, entity, entity_id, "
            "from_state, to_state, detail) VALUES(?,?,?,?,?,?,?,?)",
            (self._now_ms(), actor, action, entity, entity_id, from_state,
             to_state,
             json.dumps(detail, ensure_ascii=False) if detail is not None
             else None),
        )

    def _lease(self, project: str) -> dict | None:
        row = self._conn.execute(
            "SELECT holder_session, epoch, expires_at, recovery_blocked "
            "FROM coordinator_leases WHERE project=?", (project,)).fetchone()
        return dict(row) if row else None

    def _lease_guard(self, project: str, session_id: str,
                     expected_epoch: int | None) -> str | None:
        """事务内租约守卫：返回 None 表示通过，否则稳定拒绝原因。

        维护停用 / 过期 / 交接语义（C07/C08）：不只比对 holder/epoch，还拒绝
        已过期与 recovery_blocked（恢复被阻断/协调者停用）的租约。
        """
        lease = self._lease(project)
        if lease is None:
            return LEASE_MISSING
        if lease.get("recovery_blocked"):
            return LEASE_RECOVERY_BLOCKED
        expires_at = lease.get("expires_at")
        if expires_at is not None and expires_at <= self._now_ms():
            return LEASE_EXPIRED
        if lease["holder_session"] != session_id:
            return LEASE_HOLDER_MISMATCH
        if expected_epoch is not None:
            if not _is_epoch(expected_epoch):
                return INVALID_EPOCH
            if expected_epoch != lease["epoch"]:
                return EPOCH_MISMATCH
        return None

    # ---- 只读（无网络、无副作用；父可在事务内重查） ------------------
    def get_project(self, project: str) -> dict:
        """项目控制快照；无记录返回清晰默认（running / revision=0）。"""
        if not rules.is_nk_str(project):
            raise CoordinatorControlStateError(
                "coordinator-control get_project 需要非空 project")
        self._read_schema()
        return self._project_snap(project, self._load_project(project))

    def get_session(self, session_id: str) -> dict:
        """session 回合/hold 快照；无记录返回清晰默认（revision=0）。"""
        if not rules.is_nk_str(session_id):
            raise CoordinatorControlStateError(
                "coordinator-control get_session 需要非空 session_id")
        self._read_schema()
        return self._session_snap(session_id, self._load_session(session_id))

    def _project_snap(self, project: str, rec: dict | None) -> dict:
        if rec is not None:
            return rules.project_snapshot(rec, self._lease(project))
        snap = rules.project_default(project)
        lease = self._lease(project)
        if lease:
            snap["holder_session"] = lease["holder_session"]
            snap["epoch"] = lease["epoch"]
        return snap

    def _session_snap(self, session_id: str, rec: dict | None) -> dict:
        # projects = 该 session 作为租约 holder 的**项目**集合（不是 session 本身）
        projects = [row["project"] for row in self._conn.execute(
            "SELECT project FROM coordinator_leases WHERE holder_session=?",
            (session_id,)).fetchall()]
        if rec is not None:
            return rules.session_snapshot(rec, projects)
        return rules.session_default(session_id, projects)

    def coordinator_lease(self, project: str) -> dict | None:
        """当前项目租约（holder_session/epoch 等）只读快照。"""
        self._read_schema()
        return self._lease(project)

    def project_operation(self, operation_id: str, project: str, session_id: str) -> dict | None:
        """只读不可变操作回执；跨项目/session 不泄露，不存在不表示未执行。"""
        self._read_schema()
        record = self._load_operation(operation_id)
        if record is None or record["project"] != project or record["session_id"] != session_id:
            return None
        return {key: record[key] for key in (
            "operation_id", "project", "session_id", "action", "revision_after", "applied_at")}

    # ---- 项目意图：暂停/继续（原子 CAS + 租约 + operation 索引 + 审计） -
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
    ) -> dict:
        """显式暂停项目自动调谐：只阻新自动派单/唤醒，不中断在途 worker。"""
        return self._project_mutate(
            "pause", project, session_id=session_id, operation_id=operation_id,
            reason=reason, expected_revision=expected_revision,
            expected_epoch=expected_epoch, evidence=evidence)

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
    ) -> dict:
        """显式继续项目自动调谐：幂等；只改项目维度，不清 hold/屏障/provider 门。"""
        return self._project_mutate(
            "resume", project, session_id=session_id, operation_id=operation_id,
            reason=reason, expected_revision=expected_revision,
            expected_epoch=expected_epoch, evidence=evidence)

    def _project_mutate(self, action: str, project: str, *, session_id: str,
                        operation_id: str, reason: str, expected_revision: int,
                        expected_epoch: int | None, evidence: dict | None) -> dict:
        def refuse(code: str, detail: dict | None, snap: dict, rev: int,
                   epoch: int | None) -> dict:
            return {
                "executed": False, "refused": code, "refused_detail": detail,
                "applied": False, "reason": code,
                "project": project, "session_id": session_id,
                "operation_id": operation_id, "revision": rev,
                "epoch": epoch, "snapshot": snap,
            }

        with transaction(self._conn):
            self._ensure_schema()
            arg_error = rules.check_project_arguments(
                project=project, session_id=session_id,
                operation_id=operation_id, reason=reason,
                expected_revision=expected_revision,
                expected_epoch=expected_epoch, evidence=evidence)
            rec = self._load_project(project)
            snap = self._project_snap(project, rec)
            cur_rev = rec["revision"] if rec else 0
            lease = self._lease(project)
            epoch = lease["epoch"] if lease else None
            if arg_error is not None:
                # 参数错误优先返回（fail-closed），避免误报租约/CAS。
                return refuse(arg_error, {"argument_error": arg_error},
                              snap, cur_rev, epoch)
            guard = self._lease_guard(project, session_id, expected_epoch)
            if guard is not None:
                # 拒绝也必须返回当前真实快照，便于上层如实报告“未执行”。
                return refuse(guard, {"lease_guard": guard, "project": project,
                                      "session_id": session_id},
                              snap, cur_rev, epoch)
            # 稳定 operation 索引（幂等仲裁；损坏 fail-closed；跨项目/跨动作
            # 同 ID 因指纹不同 → operation-conflict）
            history_op = self._load_operation(operation_id)
            applied_fingerprint = history_op["fingerprint"] if history_op else None
            kwargs = dict(
                project=project, session_id=session_id,
                operation_id=operation_id, reason=reason,
                expected_revision=expected_revision, evidence=evidence,
                applied_fingerprint=applied_fingerprint,
                now=self._now_ms())
            if action == "pause":
                decision = rules.decide_project_pause(rec, **kwargs)
            else:
                decision = rules.decide_project_resume(rec, **kwargs)
            if not decision["accepted"]:
                return refuse(decision["reason"],
                              {"record_level_reason": decision["reason"],
                               "current_revision": cur_rev},
                              snap, cur_rev, epoch)
            if decision["changed"]:
                next_rec = decision["next"]
                self._store_project(next_rec)
                if action == "resume":
                    # 恢复提交后让本项目积压进入下一次正常调谐；只调整pending
                    # 到期，不改ACK/次数/在途身份，不提前恢复其他暂停项目。
                    self._conn.execute(
                        "UPDATE notifications SET next_attempt_at=MIN(next_attempt_at,?) "
                        "WHERE project=? AND target_session=? AND state='pending'",
                        (self._now_ms(), project, session_id))
                # 稳定 operation 索引 + 审计同事务（崩溃不留“状态已改但无索引”）
                self._store_operation({
                    "schema": rules.CONTROL_SCHEMA,
                    "operation_id": operation_id,
                    "action": action,
                    "project": project,
                    "session_id": session_id,
                    "fingerprint": next_rec["last_fingerprint"],
                    "reason": next_rec["reason"],
                    "evidence_hash": (rules.sha256_text(json.dumps(
                        next_rec["evidence"], ensure_ascii=False,
                        sort_keys=True))
                        if next_rec["evidence"] is not None else None),
                    "revision_after": next_rec["revision"],
                    "applied_at": self._now_ms(),
                })
                self._audit(
                    session_id, f"coordinator-control.{action}", "project",
                    project, decision["from_state"], decision["to_state"],
                    {"operation_id": operation_id,
                     "session_id": session_id,
                     "reason": next_rec["reason"],
                     "has_evidence": next_rec["evidence"] is not None,
                     "revision_before": cur_rev,
                     "revision_after": next_rec["revision"],
                     "epoch": epoch},
                )
                final_rec = next_rec
            else:
                final_rec = rec  # 幂等：状态未变，不回写、不审计
            final_snap = self._project_snap(project, final_rec)
            return {"executed": True, "refused": None, "refused_detail": None,
                    "applied": decision["changed"], "reason": decision["reason"],
                    "project": project, "session_id": session_id,
                    "operation_id": operation_id,
                    "revision": final_snap["revision"],
                    "epoch": epoch, "snapshot": final_snap}

    # ---- session 回合 hold / 用户屏障（原子 CAS + 审计） ---------------
    def hold_session(self, session_id: str, *, event_id: str, origin_at: int,
                     reason: str) -> dict:
        """回合级 hold：停止当前回答；自动 wake/超时不自清，不清 barrier。"""
        with transaction(self._conn):
            self._ensure_schema()
            rec = self._load_session(session_id)
            snap = self._session_snap(session_id, rec)
            decision = rules.decide_hold(
                rec, session_id=session_id, event_id=event_id,
                origin_at=origin_at, reason=reason, now=self._now_ms())
            return self._session_result(session_id, rec, decision, snap,
                                        action="hold-session",
                                        detail={"event_id": event_id,
                                                "origin_at": origin_at})

    def begin_user_turn(self, session_id: str, *, message_id: str,
                        origin_at: int, operation_id: str,
                        trusted: bool = False) -> dict:
        """建立/更新用户回合屏障：trusted 必须严格 `is True`（fail-closed）。"""
        with transaction(self._conn):
            return self.begin_user_turn_in_tx(session_id, message_id=message_id,
                origin_at=origin_at, operation_id=operation_id, trusted=trusted)

    def begin_user_turn_in_tx(self, session_id, *, message_id, origin_at, operation_id, trusted=False):
        """显式提交把原意图、单次许可及屏障放在同一写事务；不在事务内做网络。"""
        if not self._conn.in_transaction:
            raise RuntimeError("begin_user_turn_in_tx requires transaction")
        self._ensure_schema()
        rec = self._load_session(session_id)
        snap = self._session_snap(session_id, rec)
        decision = rules.decide_begin_turn(rec, session_id=session_id, message_id=message_id,
            origin_at=origin_at, operation_id=operation_id, trusted=trusted, now=self._now_ms())
        return self._session_result(session_id, rec, decision, snap, action="begin-user-turn",
            detail={"message_id": message_id, "origin_at": origin_at, "operation_id": operation_id,
                    "trusted": bool(trusted)})

    def finish_user_turn(self, session_id: str, *, message_id: str,
                         idle_id: str, idle_at: int,
                         expected_revision: int) -> dict:
        """以精确原输入 + 唯一终态证据关闭当前用户回合（CAS 硬约束）。"""
        with transaction(self._conn):
            return self.finish_user_turn_in_tx(session_id, message_id=message_id,
                idle_id=idle_id, idle_at=idle_at, expected_revision=expected_revision)

    def finish_user_turn_in_tx(self, session_id, *, message_id, idle_id, idle_at, expected_revision):
        """原输入结束回执可与屏障解除一起提交，崩溃不丢完成身份。"""
        if not self._conn.in_transaction:
            raise RuntimeError("finish_user_turn_in_tx requires transaction")
        self._ensure_schema()
        rec = self._load_session(session_id)
        snap = self._session_snap(session_id, rec)
        decision = rules.decide_finish_turn(rec, session_id=session_id, message_id=message_id,
            idle_id=idle_id, idle_at=idle_at, expected_revision=expected_revision, now=self._now_ms())
        return self._session_result(session_id, rec, decision, snap, action="finish-user-turn",
            detail={"message_id": message_id, "idle_id": idle_id, "idle_at": idle_at})

    def release_hold(self, session_id: str, *, operation_id: str, reason: str,
                     expected_revision: int, evidence: dict | None = None,
                     actor: str = "operator") -> dict:
        """显式运维解除回合 hold：只清可解除的回合中断 hold，不碰 barrier/项目。

        与 begin/finish 共用同一 session 记录与 CAS；无 hold 幂等成功，不写
        不审计；有 barrier、未知原因或旧 revision 一律拒绝。
        """
        with transaction(self._conn):
            self._ensure_schema()
            rec = self._load_session(session_id)
            snap = self._session_snap(session_id, rec)
            decision = rules.decide_release_hold(
                rec, session_id=session_id, operation_id=operation_id,
                reason=reason, expected_revision=expected_revision,
                evidence=evidence, now=self._now_ms())
            return self._session_result(
                session_id, rec, decision, snap, action="release-hold",
                detail={"operation_id": operation_id,
                        "reason": reason, "evidence": evidence,
                        "actor": actor})

    def register_completed_turn(self, session_id: str, *, message_id: str,
                                origin_at: int, operation_id: str,
                                idle_id: str, idle_at: int,
                                expected_revision: int, reason: str,
                                evidence: dict | None = None,
                                actor: str = "operator") -> dict:
        """把“已完成的真实用户回合”一次性登记并结束（begin+finish 同事务）。

        调用方必须先完成消息窗口核验；本方法只做控制层 CAS/顺序/维度检查。
        两步任一拒绝都不写记录，因此不会留下半截 barrier。成功时若原 hold
        早于该回合，finish 会同时解除对应 hold（C10），但不清项目暂停/provider。
        """
        with transaction(self._conn):
            self._ensure_schema()
            rec = self._load_session(session_id)
            prior = self._session_snap(session_id, rec)

            def refuse(code: str, phase: str) -> dict:
                return {"executed": False, "refused": code,
                        "refused_detail": {"phase": phase,
                                           "operation_id": operation_id,
                                           "message_id": message_id},
                        "applied": False, "reason": code,
                        "session_id": session_id,
                        "revision": prior.get("revision", 0),
                        "snapshot": prior}

            cur_rev = prior.get("revision", 0)
            if (type(expected_revision) is not int or expected_revision < 0
                    or expected_revision != cur_rev):
                return refuse(rules.R_REVISION_CHANGED, "begin")
            begin = rules.decide_begin_turn(
                rec, session_id=session_id, message_id=message_id,
                origin_at=origin_at, operation_id=operation_id,
                trusted=True, now=self._now_ms())
            if not begin["accepted"]:
                return refuse(begin["reason"], "begin")
            mid_rec = begin["next"] if begin["changed"] else rec
            if mid_rec is None:
                return refuse(rules.R_NO_ACTIVE_TURN, "begin")
            finish = rules.decide_finish_turn(
                mid_rec, session_id=session_id, message_id=message_id,
                idle_id=idle_id, idle_at=idle_at,
                expected_revision=mid_rec[rules.S_KEY_REVISION],
                now=self._now_ms())
            if not finish["accepted"]:
                return refuse(finish["reason"], "finish")
            if not finish["changed"]:
                return refuse(rules.R_NO_ACTIVE_TURN, "finish")
            final_rec = finish["next"]
            self._store_session(final_rec)
            self._audit(
                actor, "coordinator-control.register-completed-turn",
                "session", session_id, finish["from_state"],
                finish["to_state"],
                {"operation_id": operation_id, "reason": reason,
                 "message_id": message_id, "idle_id": idle_id,
                 "idle_at": idle_at, "origin_at": origin_at,
                 "hold_release": finish["reason"],
                 "evidence": evidence})
            final_snap = self._session_snap(session_id, final_rec)
            return {"executed": True, "refused": None,
                    "refused_detail": None, "applied": True,
                    "reason": finish["reason"],
                    "session_id": session_id,
                    "revision": final_snap["revision"],
                    "snapshot": final_snap}

    def _session_result(self, session_id: str, rec: dict | None,
                        decision: dict, prior_snap: dict, *, action: str,
                        detail: dict | None) -> dict:
        def refuse(code: str) -> dict:
            return {"executed": False, "refused": code,
                    "refused_detail": detail,
                    "applied": False, "reason": code,
                    "session_id": session_id,
                    "revision": prior_snap.get("revision", 0),
                    "snapshot": prior_snap}

        if not decision["accepted"]:
            return refuse(decision["reason"])
        if decision["changed"]:
            next_rec = decision["next"]
            self._store_session(next_rec)
            self._audit("coordinator-control", f"coordinator-control.{action}",
                        "session", session_id, decision["from_state"],
                        decision["to_state"], detail)
            final_rec = next_rec
        else:
            final_rec = rec  # 幂等：不回写、不审计
        final_snap = self._session_snap(session_id, final_rec)
        return {"executed": True, "refused": None, "refused_detail": None,
                "applied": decision["changed"], "reason": decision["reason"],
                "session_id": session_id, "revision": final_snap["revision"],
                "snapshot": final_snap}

    # ---- 组合判定（只读） --------------------------------------------
    def permits(self, project: str, session_id: str) -> dict:
        """自动动作必要条件的组合判定（只读，不授予权利）。

        阻塞原因（blocking）：project-paused / user-turn-active / turn-held；
        provider 执行条件与任务/写权/路由守卫不在本端口判定，由 runtime 组合。
        """
        self._read_schema()
        p = self._load_project(project)
        s = self._load_session(session_id)
        p_snap = self._project_snap(project, p)
        s_snap = self._session_snap(session_id, s)
        decision = rules.decide_permits(p_snap, s_snap)
        return {"permits": decision["permits"], "blocking": decision["blocking"],
                "project": project, "session_id": session_id,
                "project_snapshot": p_snap, "session_snapshot": s_snap}

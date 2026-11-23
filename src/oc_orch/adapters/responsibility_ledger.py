"""S4-A 后续责任存储：``ResponsibilityStore(base_ledger)`` 组合对象。

责任不是 Task / Attempt / Turn / Notification：它表达“谁在等待什么、什么事实
到达后交给谁处理”。Notification 的 ACK 只关闭收件责任；Task accepted 仍须既
有 verify 门禁。本模块只维护责任事实，不做发送、不触发 prompt、不写
Task/Attempt 状态。

组合方式与旧 ``ManagedInputLedger(conn, base_ledger)`` 一致：**不并入 Ledger
mixin**，构造时只保存 base/conn/home，**不自动 DDL**（表由 schema v3 的
``sqlite_db.init`` 建立；缺表以 ``RESPONSIBILITY_SCHEMA_MISSING`` 明确拒绝）。

状态机、替代判定、期限/预算判定**唯一权威在** :mod:`oc_orch.domain.responsibility`；
本模块只做事务内取证、CAS 与持久化，不另造一套规则。

安全不变量：

- **身份一致性**：登记/更新/转交都在事务内核对 project、attempt 归属
  （attempt.task_id/session/generation）、current attempt 与
  generation/result_version、输入回执的 session/message。归属不符即拒绝；
  已被新代次/结果版本取代的责任只能记 ``waiting_human``（历史），不能当作
  有效推进；无法证明归属（attribution-unrecorded）才按“无归属可校验”放行。
- **业务终结靠事实**：``done/cancelled`` 只能来自任务真实
  ``accepted``/``cancelled``（domain evidence），或经 ``superseded_by``
  指定的**已验证替代后仍有效**的后继责任（转 ``handed_over``）。任何
  ``manual-close``/``duplicate``/``transferred``/``superseded:*`` 文本原因都
  不构成证据；人工放弃应走 ``task cancel`` 或登记 ``waiting_human``。
- **人工控制不可自动解除**：离开 ``waiting_human`` 需要
  ``release_manual_confirmation == <责任 id>`` 的显式确认（并由 domain 记录
  ``manual-released`` 证据）；普通 update 的 ``actor`` 字符串本身不是权限。
- **转交不扩权**：后继不得跨 task/project、不得降低 ``budget_used``、不得提高
  ``max_budget``、不得延长 ``deadline``、不得改写 ``result_version``；旧责任
  在同一事务内变为 ``handed_over`` 并指向后继，任一步失败整体回滚。
- **发布 = 状态 + 通知同事务**：只有存在可核验的待核收事实（执行区间已结束
  或带身份绑定的结构化 continuation 报告）或明确的缺失责任诊断时才发布；发布
  把它规范为 ``awaiting_review`` 并在同事务入标准 notification。去重键 = 责任
  id + condition_key + 条件版本，不含每轮 now；ACK 后同一事实不反复新建通知。
- **公平扫描**：``list_open_responsibilities_page`` / ``list_unfinished_tasks_page``
  提供 keyset 分页（id 升序、``after_id`` 互斥），调用方持持久游标回绕；
  ``has_pending_task_notifications`` 供缺失责任检查判断“当前归属是否已有待办”，
  调用方不做外层 SQL。
- 预算持久且 ``budget_used`` 单调不减、``max_budget`` 不得自动提高；跨重启
  不重置。

历史兼容：2→3 迁移只建结构，不回填责任、不猜历史输入已执行、不重播历史消息。
"""

from __future__ import annotations

import sqlite3
from typing import Any

from ..domain import responsibility as duty
from ..domain import dependency_wait as depwait
from ..domain.errors import LedgerError
from ..util import json_dumps, json_loads, now_ms, short_id
from .sqlite_db import transaction

# ---- 状态（唯一权威：domain.responsibility） --------------------------------
STATE_HANDLING = duty.STATE_HANDLING
STATE_WAITING_INPUT = duty.STATE_WAITING_INPUT
STATE_WAITING_EXECUTION = duty.STATE_WAITING_EXECUTION
STATE_WAITING_DEPENDENCY = duty.STATE_WAITING_DEPENDENCY
STATE_WAITING_HUMAN = duty.STATE_WAITING_HUMAN
STATE_AWAITING_REVIEW = duty.STATE_AWAITING_REVIEW
STATE_HANDED_OVER = duty.STATE_HANDED_OVER
STATE_DONE = duty.STATE_DONE
STATE_CANCELLED = duty.STATE_CANCELLED

STATES = duty.STATES
OPEN_STATES = duty.OPEN_STATES
TERMINAL_STATES = duty.TERMINAL_STATES

# 活跃推进态（不含 waiting_human）：进入它们要过人工/代次守卫。
_PROGRESS_STATES = frozenset(duty.OPEN_STATES - {duty.STATE_WAITING_HUMAN})
# 可自动发布接续通知的状态：waiting_human = 已转人工，不自动唤醒。
_PUBLISHABLE_STATES = frozenset(duty.OPEN_STATES - {duty.STATE_WAITING_HUMAN})

# update_responsibility 白名单：owner 与身份字段（task/attempt/generation/
# result_version/input/session/message/condition）都不可改写。
UPDATABLE_FIELDS = frozenset({
    "state", "wait_kind", "wait_condition", "next_action", "next_check_at",
    "deadline", "target_session", "evidence", "budget_used", "max_budget",
})
# domain.transition 返回的 updates 中允许落库的列。
_STATE_COLUMNS = frozenset({
    "state", "wait_kind", "wait_condition", "next_check_at", "closed_at",
    "close_reason", "successor_id",
})

MISSING_CONDITION_KEY = "missing-responsibility"
CONTINUATION_KIND = "continuation_ready"

# 既有 S4 原输入/执行责任明确自管的 wait_kind：这些行虽然 state 可能落成
# waiting_dependency，但由 S4 原输入补证/执行区间对账处理（send_result_unknown/
# cancel_reconcile/execution/input_execution），结构化依赖扫描必须排除，不能当
# 历史字符串依赖转人工。纯 dependency 历史字符串仍由依赖扫描转人工。
_S4_OWNED_DEPENDENCY_WAIT_KINDS = frozenset({
    "send_result_unknown", "cancel_reconcile", "execution", "input_execution",
})

# 有效时钟边界（毫秒），与 target_gate.MAX_CLOCK_MS 同量级。
MAX_CLOCK_MS = 8 * 10 ** 12
DEFAULT_LIMIT_MAX = 500


# ---- 校验辅助 ----------------------------------------------------------------
def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_int_not_bool(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _valid_clock(value: Any, what: str) -> int:
    if not _is_int_not_bool(value) or not (0 <= value <= MAX_CLOCK_MS):
        raise LedgerError(
            "RESPONSIBILITY_INVALID",
            f"{what} 必须是有效时钟（0..{MAX_CLOCK_MS}）内的整数（收到 {value!r}）")
    return value


def _optional_clock(value: Any, what: str) -> int | None:
    if value is None:
        return None
    return _valid_clock(value, what)


def _optional_str(value: Any, what: str) -> str | None:
    if value is None:
        return None
    if not _non_empty_str(value):
        raise LedgerError(
            "RESPONSIBILITY_INVALID", f"{what} 必须是非空字符串或 None（收到 {value!r}）")
    return value


def _optional_int(value: Any, what: str) -> int | None:
    if value is None:
        return None
    if not _is_int_not_bool(value) or value < 0:
        raise LedgerError(
            "RESPONSIBILITY_INVALID",
            f"{what} 必须是非负整数或 None（收到 {value!r}）")
    return value


def _bounded_limit(limit: Any) -> int:
    if not _is_int_not_bool(limit) or limit < 1:
        raise LedgerError(
            "RESPONSIBILITY_INVALID", f"limit 必须是 >=1 的整数（收到 {limit!r}）")
    return min(int(limit), DEFAULT_LIMIT_MAX)


def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    data = dict(row)
    data["evidence"] = json_loads(data.get("evidence"), {})
    if not isinstance(data["evidence"], dict):
        data["evidence"] = {}
    return data


def condition_version(generation: Any, result_version: Any) -> str:
    """稳定“条件版本”串：用于通知去重与展示，不含任何时间戳。"""
    gen = "-" if generation is None else str(generation)
    rev = "-" if not result_version else str(result_version)
    return f"g{gen}.r{rev}"


def _with_created(row: dict | None, *, created: bool) -> dict:
    """责任行 + 非持久调用元数据（id 别名/created/responsibility 副本）。"""
    data = dict(row or {})
    data["responsibility_id"] = data.get("id")
    data["created"] = bool(created)
    data["responsibility"] = dict(data)
    return data


def _publish_result(row: dict, *, published: bool, notification: dict | None,
                    target: str | None, reason: str) -> dict:
    """publish 返回值：责任行字段 + 发布事实（published/notification/target）。"""
    data = dict(row)
    data["responsibility_id"] = data.get("id")
    data.update({"published": bool(published), "notification": notification,
                 "target": target, "reason": reason, "route": reason,
                 "responsibility": dict(row)})
    return data


def _transition_error(dec: dict, rid: str, *, what: str) -> LedgerError:
    code = dec.get("code") or "transition-not-allowed"
    if code == "manual-control-holds":
        return LedgerError(
            "RESPONSIBILITY_MANUAL_HOLD",
            f"责任 {rid} 处于 waiting_human：离开人工等待需要显式 "
            "release_manual_confirmation=<责任 id>（actor 字符串本身不是权限）",
            {"domain_code": code, "operation": what})
    return LedgerError(
        "RESPONSIBILITY_TRANSITION_REFUSED",
        f"责任 {rid} 状态推进被拒绝（domain: {code}）",
        {"domain_code": code, "operation": what})


class ResponsibilityStore:
    """后续责任窄台账（组合对象；不并入 Ledger，不自动 DDL）。"""

    def __init__(self, base_ledger: Any, conn: sqlite3.Connection | None = None):
        self.base = base_ledger
        self.conn = conn if conn is not None else getattr(base_ledger, "conn", None)
        if self.conn is None:
            raise LedgerError(
                "RESPONSIBILITY_INVALID", "ResponsibilityStore 需要 conn 或 base_ledger.conn")
        self.home = getattr(base_ledger, "home", None)
        self._schema_ok = False

    # ---- 基础设施 ---------------------------------------------------------
    def _now(self) -> int:
        clock = getattr(self.base, "_now_ms", None)
        return clock() if callable(clock) else now_ms()

    def _audit(self, conn, actor: str, action: str, rid: str,
               from_state, to_state, detail: Any) -> None:
        self.base._audit(conn, actor, action, "responsibility", rid,
                         from_state, to_state, detail)

    def _require_schema(self) -> None:
        """只读检查表是否存在；缺表明确拒绝，**绝不自动 DDL**。"""
        if self._schema_ok:
            return
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='responsibilities'"
        ).fetchone()
        if row is None:
            raise LedgerError(
                "RESPONSIBILITY_SCHEMA_MISSING",
                "台账缺少 responsibilities 表：需以 schema v3 初始化（本存储不做 DDL）")
        self._schema_ok = True

    # ---- 只读 -------------------------------------------------------------
    def get_responsibility(self, rid: str) -> dict | None:
        if not _non_empty_str(rid):
            raise LedgerError("RESPONSIBILITY_INVALID", "id 必须是非空字符串")
        self._require_schema()
        row = self.conn.execute(
            "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone()
        return _row_to_dict(row)

    def list_open_responsibilities(self, *, task_id: str | None = None,
                                   owner: str | None = None,
                                   limit: int = 200) -> list[dict]:
        self._require_schema()
        sql = ("SELECT * FROM responsibilities WHERE closed_at IS NULL")
        args: list[Any] = []
        if task_id is not None:
            if not _non_empty_str(task_id):
                raise LedgerError("RESPONSIBILITY_INVALID", "task_id 必须是非空字符串")
            sql += " AND task_id=?"
            args.append(task_id)
        if owner is not None:
            if not _non_empty_str(owner):
                raise LedgerError("RESPONSIBILITY_INVALID", "owner 必须是非空字符串")
            sql += " AND owner=?"
            args.append(owner)
        sql += " ORDER BY next_check_at ASC, id ASC LIMIT ?"
        args.append(_bounded_limit(limit))
        rows = self.conn.execute(sql, args).fetchall()
        return [d for d in (_row_to_dict(r) for r in rows) if d is not None]

    def list_due_responsibilities(self, *, now: int,
                                  limit: int = 200) -> list[dict]:
        self._require_schema()
        ts = _valid_clock(now, "now")
        rows = self.conn.execute(
            "SELECT * FROM responsibilities WHERE closed_at IS NULL "
            "AND next_check_at <= ? ORDER BY next_check_at ASC, id ASC LIMIT ?",
            (ts, _bounded_limit(limit))).fetchall()
        return [d for d in (_row_to_dict(r) for r in rows) if d is not None]

    def list_open_responsibilities_page(self, *, after_id: str | None = None,
                                        session_id: str | None = None,
                                        limit: int = 100) -> list[dict]:
        """未关闭责任的 keyset 分页（按 ``id`` 升序，``id > after_id``）。

        供调谐循环持久游标扫描：不依赖外层 SQL，也不靠不断提高 limit 覆盖全部
        行。``session_id`` 可选过滤（某一 worker/target 的责任）。调用方在尾页
        后把游标复位为 ``None`` 即回绕；本方法无状态、无全局 session 扫描。
        """
        self._require_schema()
        lim = _bounded_limit(limit)
        sql = "SELECT * FROM responsibilities WHERE closed_at IS NULL"
        args: list[Any] = []
        if session_id is not None:
            if not _non_empty_str(session_id):
                raise LedgerError(
                    "RESPONSIBILITY_INVALID", "session_id 必须是非空字符串或 None")
            sql += " AND session_id=?"
            args.append(session_id)
        if after_id is not None:
            if not _non_empty_str(after_id):
                raise LedgerError(
                    "RESPONSIBILITY_INVALID", "after_id 必须是非空字符串或 None")
            sql += " AND id > ?"
            args.append(after_id)
        sql += " ORDER BY id ASC LIMIT ?"
        args.append(lim)
        rows = self.conn.execute(sql, args).fetchall()
        return [d for d in (_row_to_dict(r) for r in rows) if d is not None]

    def list_unfinished_tasks_page(self, *, after_id: str | None = None,
                                   project: str | None = None,
                                   session_id: str | None = None,
                                    limit: int = 100) -> list[dict]:
        """未完成任务（``status NOT IN ('accepted','cancelled')`` 且未关闭）
        的 keyset 分页，按 ``id`` 升序。

        只扫本台账已登记 Task（不扫全局 session）；``acceptance``/
        ``write_scope``/``metadata`` 反序列化为原始结构，供缺失责任检查使用。
        调用方在尾页后把游标复位为 ``None`` 回绕，避免永久漏尾。
        """
        self._require_schema()
        lim = _bounded_limit(limit)
        sql = ("SELECT * FROM tasks WHERE status NOT IN ('accepted','cancelled') "
               "AND closed_at IS NULL")
        args: list[Any] = []
        for column, value in (("project", project), ("coordinator_session", session_id)):
            if value is not None:
                if not _non_empty_str(value):
                    raise LedgerError("RESPONSIBILITY_INVALID", f"{column} 需要非空字符串")
                sql += f" AND {column}=?"
                args.append(value)
        if after_id is not None:
            if not _non_empty_str(after_id):
                raise LedgerError(
                    "RESPONSIBILITY_INVALID", "after_id 必须是非空字符串或 None")
            sql += " AND id > ?"
            args.append(after_id)
        sql += " ORDER BY id ASC LIMIT ?"
        args.append(lim)
        out = []
        for row in self.conn.execute(sql, args).fetchall():
            data = dict(row)
            data["acceptance"] = json_loads(data.get("acceptance"), [])
            data["write_scope"] = json_loads(data.get("write_scope"), [])
            data["metadata"] = json_loads(data.get("metadata"), {})
            out.append(data)
        return out

    def has_pending_task_notifications(self, task_id: str, *,
                                       result_version: str | None = None) -> int:
        """任务是否仍有**适用当前归属**的待处理通知（0 = 无，>0 = 计数）。

        供缺失责任检查使用（C 不做外层 SQL）：只数 ``pending/inflight/sent``，
        任务有 current attempt 时只算任务级（attempt_id IS NULL）或该 current
        attempt 的通知，旧 attempt 的未清通知不算“当前已有待办”。给
        ``result_version`` 时进一步要求 payload 未标版本或版本相同。

        返回 int；``bool(结果)`` 即“有/无”。任务不存在 → RESPONSIBILITY_TASK_NOT_FOUND。
        """
        self._require_schema()
        if not _non_empty_str(task_id):
            raise LedgerError("RESPONSIBILITY_INVALID", "task_id 必须是非空字符串")
        if result_version is not None:
            result_version = _optional_str(result_version, "result_version")
        task = self._task_in_tx(self.conn, task_id)
        if task is None:
            raise LedgerError(
                "RESPONSIBILITY_TASK_NOT_FOUND", f"任务不存在：{task_id}")
        sql = ("SELECT COUNT(*) AS c FROM notifications WHERE task_id=? "
               "AND state IN ('pending','inflight','sent')")
        args: list[Any] = [task_id]
        current = task.get("current_attempt_id")
        if _non_empty_str(current):
            sql += " AND (attempt_id IS NULL OR attempt_id=?)"
            args.append(current)
        if result_version is not None:
            sql += (" AND (json_extract(payload,'$.result_version') IS NULL "
                    "OR json_extract(payload,'$.result_version')=?)")
            args.append(result_version)
        row = self.conn.execute(sql, args).fetchone()
        return int(row["c"] if row is not None else 0)

    # ---- 事务内取证 -------------------------------------------------------
    def _task_in_tx(self, conn, task_id: str) -> dict | None:
        row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["metadata"] = json_loads(data.get("metadata"), {})
        return data

    def _attempt_in_tx(self, conn, attempt_id: str | None) -> dict | None:
        if not attempt_id:
            return None
        row = conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["metadata"] = json_loads(data.get("metadata"), {})
        return data

    def _input_in_tx(self, conn, input_id: str | None) -> dict | None:
        if not input_id:
            return None
        row = conn.execute(
            "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)).fetchone()
        return dict(row) if row is not None else None

    def _snapshot_in_tx(self, conn, task: dict | None) -> dict:
        """当前归属快照，供 domain.staleness 统一判定。"""
        task = task or {}
        attempt = self._attempt_in_tx(conn, task.get("current_attempt_id"))
        result = ((attempt or {}).get("metadata") or {}).get("result") or {}
        version = result.get("version") if isinstance(result, dict) else None
        return {"current_attempt_id": task.get("current_attempt_id"),
                "generation": (attempt or {}).get("generation"),
                "result_version": version if _non_empty_str(version) else None,
                "successor_id": None}

    def _staleness_in_tx(self, conn, row: dict, *, task: dict | None = None) -> dict:
        """用 domain.staleness 判定归属，但只喂**本责任实际相关**的当前字段。

        输入型责任（``input_id`` 已绑定、``result_version`` 未绑定）关心的是
        “这个输入属于哪个 attempt/generation 的执行”，与后来收集到的交付物
        结果版本无关：把 current 的 result_version 置空再比较，避免把正常的
        “先发输入、后收集结果”误判成 result-version-unbound。责任一旦记录了
        结果版本，仍按 domain 严格比较；attempt/generation 永远严格比较。
        """
        task = task if task is not None else self._task_in_tx(conn, row["task_id"])
        current = self._snapshot_in_tx(conn, task)
        if _non_empty_str(row.get("input_id")) and row.get("result_version") is None:
            current = {**current, "result_version": None}
        return duty.staleness(row, current=current)

    @staticmethod
    def _unattributed(dec: dict) -> bool:
        """没有任何归属可校验（新责任/缺失诊断）不算“已被取代”。"""
        return dec.get("code") == "attribution-unrecorded"

    def _guard_progress_fresh(self, conn, row: dict, *, what: str) -> None:
        dec = self._staleness_in_tx(conn, row)
        if dec.get("status") != "fresh" and not self._unattributed(dec):
            raise LedgerError(
                "RESPONSIBILITY_STALE",
                f"责任 {row['id']} 归属已变化/无法证明（domain: {dec.get('code')}）："
                f"不能作为{what}；旧响应不能推进新责任",
                {"domain_code": dec.get("code"), "status": dec.get("status")})

    def _check_open_identity_in_tx(
        self, conn, *, task: dict, task_id: str, project: str,
        attempt_id: str | None, generation: int | None,
        result_version: str | None, session_id: str | None,
        message_id: str | None, input_id: str | None,
    ) -> tuple[str | None, int | None, str | None]:
        """事务内核对项目/attempt/代次/版本/输入归属；不符即拒绝。

        返回规范化后的 (attempt_id, generation, result_version)。
        """
        if task.get("project") != project:
            raise LedgerError(
                "RESPONSIBILITY_PROJECT_MISMATCH",
                f"project={project!r} 与任务 {task_id} 所属 "
                f"{task.get('project')!r} 不一致",
                {"task_id": task_id, "task_project": task.get("project")})
        attempt = None
        if attempt_id is not None:
            attempt = self._attempt_in_tx(conn, attempt_id)
            if attempt is None or attempt.get("task_id") != task_id:
                raise LedgerError(
                    "RESPONSIBILITY_ATTEMPT_MISMATCH",
                    f"attempt {attempt_id} 不属于任务 {task_id}",
                    {"task_id": task_id, "attempt_id": attempt_id})
            planned = attempt.get("session_id")
            if _non_empty_str(session_id) and _non_empty_str(planned) \
                    and planned != session_id:
                raise LedgerError(
                    "RESPONSIBILITY_SESSION_MISMATCH",
                    f"session {session_id} 与 attempt {attempt_id} 的 "
                    f"{planned} 不一致",
                    {"attempt_id": attempt_id, "attempt_session": planned})
            if generation is not None and attempt.get("generation") != generation:
                raise LedgerError(
                    "RESPONSIBILITY_GENERATION_MISMATCH",
                    f"generation={generation} 与 attempt {attempt_id} 的 "
                    f"{attempt.get('generation')} 不一致",
                    {"attempt_id": attempt_id,
                     "attempt_generation": attempt.get("generation")})
            if generation is None:
                generation = attempt.get("generation")
            stored = (((attempt.get("metadata") or {}).get("result") or {})
                      .get("version"))
            stored = stored if _non_empty_str(stored) else None
            if result_version is not None and result_version != stored:
                raise LedgerError(
                    "RESPONSIBILITY_RESULT_VERSION_MISMATCH",
                    f"result_version={result_version!r} 与 attempt {attempt_id} "
                    f"当前结果版本 {stored!r} 不一致",
                    {"attempt_id": attempt_id, "stored": stored})
        else:
            snapshot = self._snapshot_in_tx(conn, task)
            if generation is not None and generation != snapshot.get("generation"):
                raise LedgerError(
                    "RESPONSIBILITY_GENERATION_MISMATCH",
                    f"generation={generation} 与任务 {task_id} 当前代次 "
                    f"{snapshot.get('generation')} 不一致",
                    {"task_id": task_id, "current": snapshot.get("generation")})
            if result_version is not None \
                    and result_version != snapshot.get("result_version"):
                raise LedgerError(
                    "RESPONSIBILITY_RESULT_VERSION_MISMATCH",
                    f"result_version={result_version!r} 与任务当前版本 "
                    f"{snapshot.get('result_version')!r} 不一致",
                    {"task_id": task_id})
        if input_id is not None:
            receipt = self._input_in_tx(conn, input_id)
            if receipt is not None:
                if _non_empty_str(session_id) \
                        and receipt.get("session_id") != session_id:
                    raise LedgerError(
                        "RESPONSIBILITY_INPUT_MISMATCH",
                        f"输入 {input_id} 属于 session {receipt.get('session_id')}，"
                        f"不是 {session_id}",
                        {"input_id": input_id})
                if _non_empty_str(message_id) \
                        and receipt.get("message_id") != message_id:
                    raise LedgerError(
                        "RESPONSIBILITY_INPUT_MISMATCH",
                        f"输入 {input_id} 的原 message="
                        f"{receipt.get('message_id')!r}，不是 {message_id!r}",
                        {"input_id": input_id})
        return attempt_id, generation, result_version

    def _guard_open_allowed(self, task: dict | None, state: str) -> None:
        """任务控制守卫：终结任务不开新责任；暂停只允许 waiting_human。"""
        if task is None:
            raise LedgerError(
                "RESPONSIBILITY_TASK_NOT_FOUND", "责任必须绑定真实存在的任务")
        if task.get("status") in ("accepted", "cancelled") \
                or task.get("closed_at") is not None:
            raise LedgerError(
                "RESPONSIBILITY_TASK_TERMINAL",
                "任务已 accepted/cancelled/关闭：不得新开推进责任",
                {"task_status": task.get("status")})
        if task.get("status") == "paused" and state != duty.STATE_WAITING_HUMAN:
            raise LedgerError(
                "RESPONSIBILITY_TASK_PAUSED",
                "任务暂停中：只允许登记 waiting_human，不自动解除人工控制",
                {"task_status": task.get("status"), "state": state})

    def _find_open_in_tx(self, conn, *, task_id: str, condition_key: str,
                         generation: int | None,
                         result_version: str | None) -> dict | None:
        row = conn.execute(
            "SELECT * FROM responsibilities WHERE task_id=? AND condition_key=? "
            "AND generation IS ? AND result_version IS ? AND closed_at IS NULL",
            (task_id, condition_key, generation, result_version)).fetchone()
        return _row_to_dict(row)

    # ---- 登记 -------------------------------------------------------------
    def open_responsibility(
        self, owner: str, project: str, task_id: str,
        attempt_id: str | None = None, generation: int | None = None,
        result_version: str | None = None, input_id: str | None = None,
        session_id: str | None = None, message_id: str | None = None,
        target_session: str | None = None, *, state: str = STATE_WAITING_INPUT,
        wait_kind: str, wait_condition: str, next_action: str,
        next_check_at: int, deadline: int, condition_key: str,
        evidence: dict | None = None, max_budget: int = 3,
        actor: str = "coordinator",
    ) -> dict:
        """登记一条后续责任；返回**责任行本身**（不是包装对象）。

        返回 dict 在责任行字段之外只额外带三个非持久键：``responsibility_id``
        （``id`` 别名）、``created``、``responsibility``（同一行副本）。

        前 10 个参数允许位置传递（owner/project/task_id/attempt_id/
        generation/result_version/input_id/session_id/message_id/
        target_session），其余必须关键字。

        事务内核对：任务存在且 project 一致；attempt 属于该任务且 session/
        generation/结果版本一致；输入回执（若存在）的 session/message 一致；
        归属**已被取代**（domain.staleness == stale）时只能以 ``waiting_human``
        记为历史。“无法证明/未记录归属”（unconfirmed/attribution-unrecorded）
        不阻断新身份登记——显式身份字段的一致性已由上面的核对保证。
        """
        self._require_schema()
        if not _non_empty_str(owner):
            raise LedgerError("RESPONSIBILITY_INVALID", "owner 必须是非空字符串")
        if not _non_empty_str(project):
            raise LedgerError("RESPONSIBILITY_INVALID", "project 必须是非空字符串")
        if not _non_empty_str(task_id):
            raise LedgerError("RESPONSIBILITY_INVALID", "task_id 必须是非空字符串")
        if not _non_empty_str(wait_kind):
            raise LedgerError("RESPONSIBILITY_INVALID", "wait_kind 必须是非空字符串")
        if not _non_empty_str(wait_condition):
            raise LedgerError("RESPONSIBILITY_INVALID", "wait_condition 必须是非空字符串")
        if not _non_empty_str(next_action):
            raise LedgerError("RESPONSIBILITY_INVALID", "next_action 必须是非空字符串")
        if not _non_empty_str(condition_key):
            raise LedgerError("RESPONSIBILITY_INVALID", "condition_key 必须是非空字符串")
        if state not in OPEN_STATES:
            raise LedgerError(
                "RESPONSIBILITY_INVALID",
                f"open 只能登记未关闭状态（收到 {state!r}）；关闭请用 close",
                {"allowed": sorted(OPEN_STATES)})
        attempt_id = _optional_str(attempt_id, "attempt_id")
        input_id = _optional_str(input_id, "input_id")
        session_id = _optional_str(session_id, "session_id")
        message_id = _optional_str(message_id, "message_id")
        target_session = _optional_str(target_session, "target_session")
        generation = _optional_int(generation, "generation")
        result_version = _optional_str(result_version, "result_version")
        if evidence is not None and not isinstance(evidence, dict):
            raise LedgerError("RESPONSIBILITY_INVALID", "evidence 必须是 dict 或 None")
        if not _is_int_not_bool(max_budget) or max_budget < 0:
            raise LedgerError("RESPONSIBILITY_INVALID", "max_budget 必须是非负整数")
        due = _valid_clock(next_check_at, "next_check_at")
        limit_at = _valid_clock(deadline, "deadline")
        ts = self._now()
        payload = json_dumps(evidence or {})
        rid = short_id("R-", 12)
        with transaction(self.conn):
            task = self._task_in_tx(self.conn, task_id)
            if task is None:
                raise LedgerError(
                    "RESPONSIBILITY_TASK_NOT_FOUND", f"任务不存在：{task_id}")
            attempt_id, generation, result_version = self._check_open_identity_in_tx(
                self.conn, task=task, task_id=task_id, project=project,
                attempt_id=attempt_id, generation=generation,
                result_version=result_version, session_id=session_id,
                message_id=message_id, input_id=input_id)
            existing = self._find_open_in_tx(
                self.conn, task_id=task_id, condition_key=condition_key,
                generation=generation, result_version=result_version)
            if existing is not None:
                # 同一条件的稳定身份：返回原行，不新建业务责任。
                return _with_created(existing, created=False)
            probe = {"id": rid, "task_id": task_id, "attempt_id": attempt_id,
                     "generation": generation, "result_version": result_version,
                     "successor_id": None}
            dec = self._staleness_in_tx(self.conn, probe, task=task)
            if dec.get("status") == "stale" \
                    and state != duty.STATE_WAITING_HUMAN:
                raise LedgerError(
                    "RESPONSIBILITY_STALE",
                    f"归属已被新代次/结果版本取代（domain: {dec.get('code')}）："
                    "只能以 waiting_human 记录历史，不能登记为有效推进",
                    {"domain_code": dec.get("code"), "task_id": task_id})
            self._guard_open_allowed(task, state)
            self.conn.execute(
                "INSERT INTO responsibilities(id, project, task_id, attempt_id, "
                "generation, result_version, input_id, session_id, message_id, "
                "owner, target_session, state, wait_kind, wait_condition, "
                "next_action, next_check_at, deadline, condition_key, revision, "
                "budget_used, max_budget, evidence, prior_id, successor_id, "
                "created_at, updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,?,?,NULL,NULL,?,?)",
                (rid, project, task_id, attempt_id, generation, result_version,
                 input_id, session_id, message_id, owner, target_session, state,
                 wait_kind, wait_condition, next_action, due, limit_at,
                 condition_key, max_budget, payload, ts, ts),
            )
            self._audit(self.conn, actor, "responsibility.open", rid, None, state,
                        {"task_id": task_id, "attempt_id": attempt_id,
                         "owner": owner, "condition_key": condition_key,
                         "condition_version": condition_version(
                             generation, result_version),
                         "next_check_at": due, "deadline": limit_at,
                         "max_budget": max_budget})
            fresh = self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone()
            return _with_created(_row_to_dict(fresh), created=True)

    # ---- 更新 -------------------------------------------------------------
    def update_responsibility(self, rid: str, *, expected_revision: int,
                              fields: dict[str, Any],
                              actor: str = "supervisor",
                              release_manual_confirmation: str | None = None
                              ) -> dict:
        """CAS 更新白名单字段；身份字段与 owner 不可改写。

        - 状态变化交给 ``domain.responsibility.transition``：离开
          ``waiting_human`` 必须 ``release_manual_confirmation == rid``
          （显式人工确认，而非 actor 字符串）；
        - 进入活跃推进态前重查 current/代次/结果版本，已被取代 → 拒绝；
        - ``budget_used`` 单调不减，``max_budget`` 不得自动提高；
        - 身份字段（task/attempt/generation/result_version/input/session/
          message/condition）与 ``owner`` 不在白名单，需 transfer。
        """
        self._require_schema()
        if not isinstance(fields, dict) or not fields:
            raise LedgerError("RESPONSIBILITY_INVALID", "fields 必须是非空 dict")
        unknown = set(fields) - UPDATABLE_FIELDS
        if unknown:
            raise LedgerError(
                "RESPONSIBILITY_INVALID",
                f"不可更新的责任字段：{sorted(unknown)}"
                "（身份/owner 变更请走 transfer；result_version 不可洗白）",
                {"allowed": sorted(UPDATABLE_FIELDS)})
        if not _is_int_not_bool(expected_revision) or expected_revision < 1:
            raise LedgerError(
                "RESPONSIBILITY_REVISION_CONFLICT",
                f"expected_revision 必须是正整数（收到 {expected_revision!r}）")
        ts = self._now()
        with transaction(self.conn):
            row = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            if row is None:
                raise LedgerError("RESPONSIBILITY_NOT_FOUND", f"责任不存在：{rid}")
            if row.get("closed_at") is not None:
                raise LedgerError(
                    "RESPONSIBILITY_TERMINAL",
                    f"责任 {rid} 已关闭（{row.get('close_reason')}），不能改写",
                    {"state": row.get("state")})
            if row["revision"] != expected_revision:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} revision={row['revision']} != 期望 {expected_revision}",
                    {"revision": row["revision"], "expected": expected_revision})

            updates: dict[str, Any] = {}
            for key in ("wait_kind", "wait_condition", "next_action"):
                if key in fields:
                    if not _non_empty_str(fields[key]):
                        raise LedgerError(
                            "RESPONSIBILITY_INVALID", f"{key} 必须是非空字符串")
                    updates[key] = fields[key]
            if "next_check_at" in fields:
                updates["next_check_at"] = _valid_clock(
                    fields["next_check_at"], "next_check_at")
            if "deadline" in fields:
                updates["deadline"] = _valid_clock(fields["deadline"], "deadline")
            if "target_session" in fields:
                updates["target_session"] = _optional_str(
                    fields["target_session"], "target_session")
            if "evidence" in fields:
                if not isinstance(fields["evidence"], dict):
                    raise LedgerError("RESPONSIBILITY_INVALID", "evidence 必须是 dict")
                updates["evidence"] = json_dumps(fields["evidence"])

            budget_used = int(row.get("budget_used") or 0)
            max_budget = int(row.get("max_budget") or 0)
            if "budget_used" in fields:
                value = fields["budget_used"]
                if not _is_int_not_bool(value) or value < 0:
                    raise LedgerError(
                        "RESPONSIBILITY_INVALID", "budget_used 必须是非负整数")
                if value < budget_used:
                    raise LedgerError(
                        "RESPONSIBILITY_INVALID",
                        "budget_used 不得回退（预算跨重启不重置）",
                        {"stored": budget_used, "candidate": value})
                budget_used = value
            if "max_budget" in fields:
                value = fields["max_budget"]
                if not _is_int_not_bool(value) or value < 0:
                    raise LedgerError(
                        "RESPONSIBILITY_INVALID", "max_budget 必须是非负整数")
                if value > max_budget:
                    raise LedgerError(
                        "RESPONSIBILITY_BUDGET_INCREASE_REFUSED",
                        "max_budget 不得在既有责任上自动提高；需要新预算请新建"
                        "责任身份（新条件/新代次）",
                        {"stored": max_budget, "candidate": value})
                max_budget = value
            if "max_budget" in fields or "budget_used" in fields:
                if budget_used > max_budget:
                    raise LedgerError(
                        "RESPONSIBILITY_BUDGET_EXHAUSTED",
                        f"责任 {rid} 预算 {budget_used}/{max_budget} 已耗尽："
                        "转人工责任，不无限唤醒",
                        {"budget_used": budget_used, "max_budget": max_budget})
                updates["budget_used"] = budget_used
                updates["max_budget"] = max_budget

            new_state = row["state"]
            if "state" in fields:
                to_state = fields["state"]
                if to_state not in OPEN_STATES:
                    raise LedgerError(
                        "RESPONSIBILITY_INVALID",
                        f"update 只能进入未关闭状态；{to_state!r} 请用 close/transfer",
                        {"allowed": sorted(OPEN_STATES)})
                new_state = to_state
                if to_state != row["state"]:
                    evidence: set[str] = set()
                    transition_actor = actor
                    if row["state"] == duty.STATE_WAITING_HUMAN \
                            and to_state in _PROGRESS_STATES:
                        if release_manual_confirmation != rid:
                            raise LedgerError(
                                "RESPONSIBILITY_MANUAL_HOLD",
                                "责任处于 waiting_human：离开人工等待需要显式 "
                                f"release_manual_confirmation={rid!r}"
                                "（人工确认，不用 actor 字符串代替）",
                                {"state": row["state"], "target": to_state})
                        evidence.add(duty.EV_MANUAL_RELEASED)
                        transition_actor = "human"
                    if to_state in _PROGRESS_STATES:
                        dec_stale = self._staleness_in_tx(self.conn, row)
                        if dec_stale.get("status") != "fresh" \
                                and not self._unattributed(dec_stale):
                            raise LedgerError(
                                "RESPONSIBILITY_STALE",
                                f"责任 {rid} 归属已变化/无法证明"
                                f"（domain: {dec_stale.get('code')}）：不能推进为活跃责任",
                                {"domain_code": dec_stale.get("code")})
                    task = self._task_in_tx(self.conn, row["task_id"])
                    if to_state in _PROGRESS_STATES:
                        self._guard_open_allowed(task, to_state)
                    dec = duty.transition(
                        row, to_state=to_state, now_ms=ts,
                        reason=(updates.get("wait_condition")
                                or row.get("wait_condition")
                                or f"state:{to_state}"),
                        actor=transition_actor, evidence=evidence,
                        wait_kind=(updates.get("wait_kind")
                                   or row.get("wait_kind")),
                        wait_condition=(updates.get("wait_condition")
                                        or row.get("wait_condition")),
                        next_check_at=updates.get(
                            "next_check_at", row.get("next_check_at")))
                    if not dec.get("allowed"):
                        raise _transition_error(dec, rid, what="update")
                    for key, value in (dec.get("updates") or {}).items():
                        if key in _STATE_COLUMNS:
                            updates[key] = value
                    new_state = to_state

            sets = ", ".join(f"{key}=?" for key in updates)
            args = list(updates.values())
            cur = self.conn.execute(
                f"UPDATE responsibilities SET {sets + ', ' if sets else ''}"
                "revision=revision+1, updated_at=? "
                "WHERE id=? AND revision=? AND closed_at IS NULL",
                (*args, ts, rid, expected_revision))
            if cur.rowcount != 1:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} 并发修改（CAS 失败），不覆盖新责任")
            self._audit(self.conn, actor, "responsibility.update", rid,
                        row["state"], new_state,
                        {"fields": sorted(updates), "revision": expected_revision + 1})
            fresh = self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone()
            return _row_to_dict(fresh)

    # ---- 显式人工恢复 -----------------------------------------------------
    def resume_responsibility(self, rid: str, *, expected_revision: int,
                              confirm: str, reason: str,
                              actor: str = "human") -> dict:
        """人工确认后离开 ``waiting_human``：回到 ``handling``。

        权限来自 ``confirm == rid`` 的显式确认（不是 actor 字符串）；不重开
        终态、不授予写权、不自动执行。返回责任行。
        """
        if confirm != rid:
            raise LedgerError(
                "RESPONSIBILITY_INVALID",
                f"resume 需要 confirm=<责任 id> 的显式确认（收到 {confirm!r}）")
        if not _non_empty_str(reason):
            raise LedgerError("RESPONSIBILITY_INVALID", "resume 需要非空 reason")
        row = self.get_responsibility(rid)
        if row is None:
            raise LedgerError("RESPONSIBILITY_NOT_FOUND", f"责任不存在：{rid}")
        return self.update_responsibility(
            rid, expected_revision=expected_revision,
            fields={"state": duty.STATE_HANDLING,
                    "wait_kind": "resumed", "wait_condition": reason,
                    "next_action": reason},
            actor=actor, release_manual_confirmation=rid)

    # ---- 转交 -------------------------------------------------------------
    def _validate_successor(self, successor: Any, previous: dict, *,
                            task: dict) -> dict:
        if not isinstance(successor, dict):
            raise LedgerError("RESPONSIBILITY_INVALID", "successor 必须是 dict")
        for scoped in ("task_id", "project"):
            if scoped in successor and successor[scoped] not in (None, previous[scoped]):
                raise LedgerError(
                    "RESPONSIBILITY_TRANSFER_SCOPE_REFUSED",
                    f"转交不得改变 {scoped}（{previous[scoped]!r} → "
                    f"{successor[scoped]!r}）：跨任务/跨项目必须新开责任",
                    {"previous": previous[scoped]})
        attempt_id = successor.get("attempt_id", previous["attempt_id"])
        if attempt_id is not None:
            attempt = self._attempt_in_tx(self.conn, attempt_id)
            if attempt is None or attempt.get("task_id") != previous["task_id"]:
                raise LedgerError(
                    "RESPONSIBILITY_TRANSFER_SCOPE_REFUSED",
                    f"后继 attempt {attempt_id} 不属于任务 {previous['task_id']}",
                    {"attempt_id": attempt_id})
        generation = _optional_int(
            successor.get("generation", previous["generation"]), "generation")
        if generation is not None and attempt_id is not None:
            attempt = self._attempt_in_tx(self.conn, attempt_id)
            if attempt is not None and attempt.get("generation") != generation:
                raise LedgerError(
                    "RESPONSIBILITY_GENERATION_MISMATCH",
                    f"后继 generation={generation} 与 attempt 代次不一致")
        result_version = _optional_str(
            successor.get("result_version", previous["result_version"]),
            "result_version")
        if result_version != previous["result_version"]:
            raise LedgerError(
                "RESPONSIBILITY_TRANSFER_IDENTITY_REFUSED",
                "转交不得改写 result_version：旧责任不能被洗成新结果版本",
                {"previous": previous["result_version"],
                 "candidate": result_version})
        merged = {
            "owner": successor.get("owner", previous["owner"]),
            "project": previous["project"],
            "task_id": previous["task_id"],
            "attempt_id": attempt_id,
            "generation": generation,
            "result_version": result_version,
            "input_id": successor.get("input_id", previous["input_id"]),
            "session_id": successor.get("session_id", previous["session_id"]),
            "message_id": successor.get("message_id", previous["message_id"]),
            "target_session": successor.get("target_session",
                                            previous["target_session"]),
            "state": successor.get("state", duty.STATE_HANDLING),
            "wait_kind": successor.get("wait_kind"),
            "wait_condition": successor.get("wait_condition"),
            "next_action": successor.get("next_action"),
            "next_check_at": successor.get("next_check_at",
                                           previous["next_check_at"]),
            "deadline": successor.get("deadline", previous["deadline"]),
            "condition_key": successor.get("condition_key",
                                           previous["condition_key"]),
            "evidence": successor.get("evidence") or {},
            "max_budget": successor.get("max_budget", previous["max_budget"]),
            "budget_used": successor.get("budget_used", previous["budget_used"]),
        }
        for key in ("wait_kind", "wait_condition", "next_action"):
            if not _non_empty_str(merged[key]):
                raise LedgerError(
                    "RESPONSIBILITY_INVALID", f"successor.{key} 必须是非空字符串")
        if not _non_empty_str(merged["owner"]):
            raise LedgerError("RESPONSIBILITY_INVALID", "successor.owner 必须是非空字符串")
        if merged["state"] not in OPEN_STATES:
            raise LedgerError(
                "RESPONSIBILITY_INVALID",
                f"successor.state 必须是未关闭状态（收到 {merged['state']!r}）")
        if not isinstance(merged["evidence"], dict):
            raise LedgerError("RESPONSIBILITY_INVALID", "successor.evidence 必须是 dict")
        for key in ("max_budget", "budget_used"):
            if not _is_int_not_bool(merged[key]) or merged[key] < 0:
                raise LedgerError(
                    "RESPONSIBILITY_INVALID", f"successor.{key} 必须是非负整数")
        if merged["budget_used"] < int(previous.get("budget_used") or 0):
            raise LedgerError(
                "RESPONSIBILITY_TRANSFER_BUDGET_REFUSED",
                "转交不得降低 budget_used：预算不能靠新 id 重置",
                {"previous": previous.get("budget_used"),
                 "candidate": merged["budget_used"]})
        if merged["max_budget"] > int(previous.get("max_budget") or 0):
            raise LedgerError(
                "RESPONSIBILITY_TRANSFER_BUDGET_REFUSED",
                "转交不得提高 max_budget：需要新预算请新建责任身份",
                {"previous": previous.get("max_budget"),
                 "candidate": merged["max_budget"]})
        if merged["budget_used"] > merged["max_budget"]:
            raise LedgerError(
                "RESPONSIBILITY_BUDGET_EXHAUSTED",
                "successor 预算已耗尽：转交不得静默重置预算",
                {"budget_used": merged["budget_used"],
                 "max_budget": merged["max_budget"]})
        merged["next_check_at"] = _valid_clock(
            merged["next_check_at"], "successor.next_check_at")
        merged["deadline"] = _valid_clock(merged["deadline"], "successor.deadline")
        prev_deadline = previous.get("deadline")
        if _is_int_not_bool(prev_deadline) and merged["deadline"] > prev_deadline:
            raise LedgerError(
                "RESPONSIBILITY_TRANSFER_DEADLINE_REFUSED",
                "转交不得延长 deadline：超期责任必须转人工或被关闭",
                {"previous": prev_deadline, "candidate": merged["deadline"]})
        for key in ("input_id", "session_id", "message_id", "target_session"):
            merged[key] = _optional_str(merged[key], f"successor.{key}")
        return merged

    def transfer_responsibility(self, rid: str, *, expected_revision: int,
                                successor: dict, reason: str,
                                actor: str = "coordinator",
                                release_manual_confirmation: str | None = None
                                ) -> dict:
        """单事务转交：旧责任 ``handed_over`` + 后继已持久登记。

        后继不得跨 task/project、不得降低 ``budget_used``、不得提高
        ``max_budget``、不得延长 ``deadline``、不得改写 ``result_version``；
        ``waiting_human`` 只能转给另一个 ``waiting_human``，除非显式
        ``release_manual_confirmation == rid``。返回 ``{"previous","successor"}``。
        """
        self._require_schema()
        if not _non_empty_str(reason):
            raise LedgerError("RESPONSIBILITY_INVALID", "transfer 需要非空 reason")
        ts = self._now()
        with transaction(self.conn):
            row = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            if row is None:
                raise LedgerError("RESPONSIBILITY_NOT_FOUND", f"责任不存在：{rid}")
            if row.get("closed_at") is not None:
                raise LedgerError(
                    "RESPONSIBILITY_TERMINAL",
                    f"责任 {rid} 已关闭（{row.get('close_reason')}），不能转交")
            if row["revision"] != expected_revision:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} revision={row['revision']} != 期望 {expected_revision}")
            if row.get("successor_id"):
                raise LedgerError(
                    "RESPONSIBILITY_TERMINAL",
                    f"责任 {rid} 已有后继 {row['successor_id']}，不重复转交")
            task = self._task_in_tx(self.conn, row["task_id"])
            if task is None:
                raise LedgerError(
                    "RESPONSIBILITY_TASK_NOT_FOUND",
                    f"任务不存在：{row['task_id']}")
            if task.get("status") in ("accepted", "cancelled") \
                    or task.get("closed_at") is not None:
                raise LedgerError(
                    "RESPONSIBILITY_TASK_TERMINAL",
                    "任务已终结：应关闭责任而不是转交推进责任",
                    {"task_status": task.get("status")})
            merged = self._validate_successor(successor, row, task=task)
            evidence: set[str] = set()
            previous_actor = actor
            if merged["state"] in _PROGRESS_STATES:
                dec_stale = self._staleness_in_tx(self.conn, row, task=task)
                if dec_stale.get("status") != "fresh" \
                        and not self._unattributed(dec_stale):
                    raise LedgerError(
                        "RESPONSIBILITY_STALE",
                        f"责任 {rid} 归属已变化/无法证明"
                        f"（domain: {dec_stale.get('code')}）：不能转交为活跃责任",
                        {"domain_code": dec_stale.get("code")})
            if row["state"] == duty.STATE_WAITING_HUMAN \
                    and merged["state"] != duty.STATE_WAITING_HUMAN:
                if release_manual_confirmation != rid:
                    raise LedgerError(
                        "RESPONSIBILITY_MANUAL_HOLD",
                        "waiting_human 只能转交给另一个等待人工的责任；"
                        "转为活跃需显式 release_manual_confirmation=<责任 id>",
                        {"state": row["state"], "target": merged["state"]})
                evidence.add(duty.EV_MANUAL_RELEASED)
                previous_actor = "human"
            if task.get("status") == "paused" \
                    and merged["state"] in _PROGRESS_STATES:
                raise LedgerError(
                    "RESPONSIBILITY_TASK_PAUSED",
                    "任务暂停中：后继只能是 waiting_human")
            new_id = short_id("R-", 12)
            dec = duty.transition(
                row, to_state=duty.STATE_HANDED_OVER, now_ms=ts, reason=reason,
                actor=previous_actor, evidence=evidence,
                successor_id=new_id, successor_owner=merged["owner"],
                wait_kind=row.get("wait_kind"),
                wait_condition=row.get("wait_condition"),
                next_check_at=row.get("next_check_at"))
            if not dec.get("allowed"):
                raise _transition_error(dec, rid, what="transfer")
            updates = {k: v for k, v in (dec.get("updates") or {}).items()
                       if k in _STATE_COLUMNS}
            updates["close_reason"] = reason  # 保留调用方原因供审计追溯
            sets = ", ".join(f"{key}=?" for key in updates)
            cur = self.conn.execute(
                f"UPDATE responsibilities SET {sets}, revision=revision+1, "
                "updated_at=? WHERE id=? AND revision=? AND closed_at IS NULL",
                (*updates.values(), ts, rid, expected_revision))
            if cur.rowcount != 1:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} 并发修改（CAS 失败），不改写新责任")
            self.conn.execute(
                "INSERT INTO responsibilities(id, project, task_id, attempt_id, "
                "generation, result_version, input_id, session_id, message_id, "
                "owner, target_session, state, wait_kind, wait_condition, "
                "next_action, next_check_at, deadline, condition_key, revision, "
                "budget_used, max_budget, evidence, prior_id, successor_id, "
                "created_at, updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?,NULL,?,?)",
                (new_id, merged["project"], merged["task_id"], merged["attempt_id"],
                 merged["generation"], merged["result_version"], merged["input_id"],
                 merged["session_id"], merged["message_id"], merged["owner"],
                 merged["target_session"], merged["state"], merged["wait_kind"],
                 merged["wait_condition"], merged["next_action"],
                 merged["next_check_at"], merged["deadline"],
                 merged["condition_key"], merged["budget_used"],
                 merged["max_budget"], json_dumps(merged["evidence"]), rid, ts, ts),
            )
            self._audit(self.conn, actor, "responsibility.transfer", rid,
                        row["state"], duty.STATE_HANDED_OVER,
                        {"successor": new_id, "reason": reason,
                         "successor_owner": merged["owner"],
                         "successor_target": merged["target_session"]})
            previous = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            created = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (new_id,)).fetchone())
            return {"previous": previous, "successor": created}

    # ---- 关闭 -------------------------------------------------------------
    def close_responsibility(self, rid: str, *, expected_revision: int,
                             reason: str, actor: str = "coordinator",
                             superseded_by: str | None = None) -> dict:
        """CAS 关闭责任：只有**事实**能终结业务推进。

        - 任务真实 ``accepted`` → ``done``（domain evidence task-accepted-verified）；
        - 任务真实 ``cancelled`` → ``cancelled``（task-cancelled evidence）；
        - ``superseded_by=<责任 id>``：需该后继仍 open、同任务，且本责任已被
          domain.staleness 判定为 stale（当前替代已确证）→ 旧责任 ``handed_over``；
        - 其余（``manual-close``/``duplicate``/``transferred``/任意
          ``superseded:*`` 文本、ACK、succeeded、空 inbox）一律拒绝：
          人工放弃请走 ``task cancel``，或登记 ``waiting_human``。
        - 任务不存在 / ``closed_at`` 非空但状态非终态 → **不**当 accepted 证据；
          缺失任务转 ``waiting_human`` 可见诊断后返回，不伪造成功关闭。
        """
        self._require_schema()
        if not _non_empty_str(reason):
            raise LedgerError("RESPONSIBILITY_INVALID", "close 需要非空 reason")
        ts = self._now()
        with transaction(self.conn):
            row = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            if row is None:
                raise LedgerError("RESPONSIBILITY_NOT_FOUND", f"责任不存在：{rid}")
            if row.get("closed_at") is not None:
                raise LedgerError(
                    "RESPONSIBILITY_TERMINAL",
                    f"责任 {rid} 已关闭（{row.get('close_reason')}）",
                    {"state": row.get("state")})
            if row["revision"] != expected_revision:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} revision={row['revision']} != 期望 {expected_revision}")
            task = self._task_in_tx(self.conn, row["task_id"])
            if task is None:
                # 任务不存在不是 accepted 证据：转可见人工诊断，责任保持 open。
                return self._mark_waiting_human_in_tx(
                    self.conn, row, reason="task-missing",
                    detail=("责任绑定的 Task 不存在：保留责任并转人工诊断；"
                            "不得当作 terminal 成功关闭"),
                    ts=ts)
            status = task.get("status")
            evidence: set[str] = set()
            to_state = None
            successor_id = None
            successor_owner = None
            if status == "accepted":
                to_state = duty.STATE_DONE
                evidence.add(duty.EV_TASK_ACCEPTED)
            elif status == "cancelled":
                to_state = duty.STATE_CANCELLED
                evidence.add(duty.EV_TASK_CANCELLED)
            elif superseded_by is not None:
                to_state, successor_id, successor_owner = \
                    self._verified_supersession_in_tx(row, task, superseded_by)
            else:
                raise LedgerError(
                    "RESPONSIBILITY_CLOSE_REFUSED",
                    "任务未 accepted/cancelled 且没有已验证的替代后继："
                    "ACK、execution succeeded、空 inbox 与文本原因都不构成关闭依据；"
                    "closed_at 本身不是 accepted 证据。人工放弃请 task cancel "
                    "或登记 waiting_human",
                    {"task_status": status, "closed_at": task.get("closed_at"),
                     "options": ["task cancel", "waiting_human", "transfer",
                                 "close with superseded_by=<verified successor id>"]})
            dec = duty.transition(
                row, to_state=to_state, now_ms=ts, evidence=evidence,
                reason=reason, actor=actor, successor_id=successor_id,
                successor_owner=successor_owner,
                wait_kind=row.get("wait_kind"),
                wait_condition=row.get("wait_condition"),
                next_check_at=row.get("next_check_at"))
            if not dec.get("allowed"):
                raise _transition_error(dec, rid, what="close")
            updates = {k: v for k, v in (dec.get("updates") or {}).items()
                       if k in _STATE_COLUMNS}
            updates["close_reason"] = reason
            sets = ", ".join(f"{key}=?" for key in updates)
            cur = self.conn.execute(
                f"UPDATE responsibilities SET {sets}, revision=revision+1, "
                "updated_at=? WHERE id=? AND revision=? AND closed_at IS NULL",
                (*updates.values(), ts, rid, expected_revision))
            if cur.rowcount != 1:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} 并发修改（CAS 失败），不关闭新责任")
            self._audit(self.conn, actor, "responsibility.close", rid,
                        row["state"], updates.get("state"),
                        {"reason": reason, "task_status": status,
                         "successor": successor_id,
                         "domain_code": dec.get("code"),
                         "evidence": sorted(evidence)})
            return _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())

    def _verified_supersession_in_tx(self, row: dict, task: dict,
                                     superseded_by: str) -> tuple[str, str, str]:
        """校验“当前替代已确证且后继责任仍有效”，返回 handed_over 目标。"""
        if not _non_empty_str(superseded_by) or superseded_by == row["id"]:
            raise LedgerError(
                "RESPONSIBILITY_INVALID",
                "superseded_by 必须是另一个责任 id（不能指向自己）")
        target = _row_to_dict(self.conn.execute(
            "SELECT * FROM responsibilities WHERE id=?", (superseded_by,)).fetchone())
        if target is None or target.get("closed_at") is not None:
            raise LedgerError(
                "RESPONSIBILITY_CLOSE_REFUSED",
                f"替代责任 {superseded_by} 不存在或已关闭：没有有效后继承担推进",
                {"superseded_by": superseded_by})
        if target["task_id"] != row["task_id"]:
            raise LedgerError(
                "RESPONSIBILITY_CLOSE_REFUSED",
                "替代责任不属于同一任务：不能作为本责任的业务替代",
                {"superseded_by": superseded_by})
        dec = self._staleness_in_tx(self.conn, row, task=task)
        if dec.get("status") != "stale":
            raise LedgerError(
                "RESPONSIBILITY_CLOSE_REFUSED",
                f"本责任尚未被当前替代（domain: {dec.get('code')}）："
                "不能用 superseded 文本关闭未完成业务",
                {"domain_code": dec.get("code"), "status": dec.get("status")})
        snapshot = self._snapshot_in_tx(self.conn, task)
        if snapshot.get("current_attempt_id") is not None:
            if target.get("attempt_id") != snapshot.get("current_attempt_id"):
                raise LedgerError(
                    "RESPONSIBILITY_CLOSE_REFUSED",
                    "替代责任未绑定当前 attempt：不是有效后继",
                    {"superseded_by": superseded_by,
                     "current_attempt_id": snapshot.get("current_attempt_id")})
        if not _non_empty_str(target.get("owner")):
            raise LedgerError(
                "RESPONSIBILITY_CLOSE_REFUSED", "替代责任缺少责任人")
        return (duty.STATE_HANDED_OVER, target["id"], target["owner"])

    # ---- 发布（责任状态 + 通知同事务） -----------------------------------
    def _resolve_route_in_tx(self, conn, row: dict) -> tuple[str | None, str]:
        """复核当前协调者：任务字段优先，其次项目租约 holder。"""
        task = self._task_in_tx(conn, row["task_id"])
        task_coord = (task or {}).get("coordinator_session")
        holder = None
        try:
            lease = self.base.coordinator_lease((task or {}).get("project")
                                                or row["project"])
        except Exception:
            lease = None
        if isinstance(lease, dict):
            holder = lease.get("holder_session")
        stored = row.get("target_session")
        if _non_empty_str(task_coord):
            if stored and stored != task_coord:
                return task_coord, "task-coordinator-override"
            return task_coord, "task-coordinator"
        if _non_empty_str(stored):
            return stored, "responsibility"
        if _non_empty_str(holder):
            return holder, "lease-holder"
        return None, "no-route"

    def _mark_waiting_human_in_tx(self, conn, row: dict, *, reason: str,
                                  detail: str, ts: int) -> dict:
        """把责任转为明确人工等待 + 去重告警；不再自动发送。"""
        evidence = dict(row.get("evidence") or {})
        evidence["blocked"] = {"reason": reason, "at": ts, "detail": detail[:400]}
        dec = duty.transition(
            row, to_state=duty.STATE_WAITING_HUMAN, now_ms=ts,
            reason=reason, actor="supervisor",
            wait_kind="human", wait_condition=reason,
            next_check_at=row.get("next_check_at"))
        updates: dict[str, Any] = {"evidence": json_dumps(evidence),
                                   "next_action": f"转入人工责任（{reason}）：{detail[:200]}"}
        if dec.get("allowed"):
            for key, value in (dec.get("updates") or {}).items():
                if key in _STATE_COLUMNS:
                    updates[key] = value
        else:
            updates["state"] = duty.STATE_WAITING_HUMAN
        sets = ", ".join(f"{key}=?" for key in updates)
        cur = conn.execute(
            f"UPDATE responsibilities SET {sets}, revision=revision+1, updated_at=? "
            "WHERE id=? AND revision=?",
            (*updates.values(), ts, row["id"], row["revision"]))
        if cur.rowcount != 1:
            raise LedgerError(
                "RESPONSIBILITY_REVISION_CONFLICT",
                f"责任 {row['id']} 并发修改（人工转派 CAS 失败）")
        dedupe = (f"resp-blocked:{row['id']}:{row['condition_key']}:"
                  f"{condition_version(row.get('generation'), row.get('result_version'))}"
                  f":{reason}")
        if hasattr(self.base, "_insert_alert_in_tx"):
            self.base._insert_alert_in_tx(
                conn, severity="warning", kind="responsibility_blocked",
                title=f"接续责任受阻：{row['task_id']}",
                detail=(f"责任 {row['id']}（task={row['task_id']} owner={row['owner']} "
                        f"wait={row['wait_kind']}/{row['wait_condition']}）"
                        f"原因={reason}；{detail}"),
                dedupe_key=dedupe, task_id=row["task_id"],
                attempt_id=row.get("attempt_id"))
        return _row_to_dict(conn.execute(
            "SELECT * FROM responsibilities WHERE id=?", (row["id"],)).fetchone())

    @staticmethod
    def _continuation_traceable(row: dict, fact: dict) -> bool:
        """结构化接续事实必须可追溯到本责任绑定的原输入/身份。

        至少提供一个绑定键（input_id/message_id/session_id/responsibility_id），
        且提供的每个键都必须与责任行一致；不凭任意 dict/kind 放行。
        """
        bindings = []
        if _non_empty_str(fact.get("responsibility_id")):
            bindings.append(("responsibility_id", "id"))
        for key in ("input_id", "message_id", "session_id"):
            if _non_empty_str(fact.get(key)):
                bindings.append((key, key))
        if not bindings:
            return False
        for fact_key, row_key in bindings:
            if not _non_empty_str(row.get(row_key)):
                return False
            if fact[fact_key] != row[row_key]:
                return False
        return True

    @classmethod
    def _review_fact(cls, row: dict) -> str | None:
        """发布门：必须有可核验的待核收事实或明确缺失责任诊断。

        只认结构化事实，不认 reason 文本：
        - ``evidence.execution.ended_at``（本输入执行区间已结束）；
        - ``condition_key == 'missing-responsibility'`` 或
          ``evidence.reason == 'no-open-responsibility'``（缺失责任诊断）；
        - ``evidence.continuation``：除 ``kind`` 外必须带可与责任行核对的
          input/message/session/responsibility 绑定（见 ``_continuation_traceable``），
          任意 dict 或纯文本不放行；
        - ``evidence.dependency``（依赖条件已满足，C13）：必须同时有
          ``satisfied=True``、条件身份与证据版本；只能由
          ``record_dependency_review`` 写入（其事务内已复核 current/代次/版本与
          归属），任何手工文本都不构成待核收事实。
        """
        ev = row.get("evidence") or {}
        execution = ev.get("execution")
        if isinstance(execution, dict) and execution.get("ended_at") is not None:
            return "execution-ended"
        if row.get("condition_key") == MISSING_CONDITION_KEY \
                or ev.get("reason") == "no-open-responsibility":
            return "missing-diagnostic"
        continuation = ev.get("continuation")
        if isinstance(continuation, dict) and _non_empty_str(continuation.get("kind")) \
                and cls._continuation_traceable(row, continuation):
            return "continuation-fact"
        dep = ev.get(depwait.DEPENDENCY_KEY)
        if isinstance(dep, dict) and dep.get("satisfied") is True \
                and _non_empty_str(dep.get("condition_identity")) \
                and _non_empty_str(dep.get("evidence_version")):
            return "dependency-satisfied"
        return None

    def publish_responsibility(self, rid: str, *, expected_revision: int,
                               kind: str = CONTINUATION_KIND,
                               payload: dict,
                               actor: str = "supervisor") -> dict:
        """把可发布的开放责任规范为 ``awaiting_review`` 并同事务发布通知。

        - 发布前必须有可核验的待核收事实/缺失诊断（``_review_fact``）；否则
          ``RESPONSIBILITY_NOT_REVIEWABLE``，不写任何通知；
        - 状态与 notification 在同一事务：任一步失败整体回滚；
        - 期限/预算耗尽 → 转 waiting_human + 告警，不发布；
        - 已被新代次/结果版本取代 → ``RESPONSIBILITY_STALE``，不发布；
        - 无路由 → waiting_human + 去重告警，不发消息；
        - 去重键 = 责任 id + condition_key + 条件版本（不含 now），ACK 后同一
          事实不会反复新建通知。
        """
        self._require_schema()
        if not _non_empty_str(kind):
            raise LedgerError("RESPONSIBILITY_INVALID", "kind 必须是非空字符串")
        if not isinstance(payload, dict):
            raise LedgerError("RESPONSIBILITY_INVALID", "payload 必须是 dict")
        enqueue = getattr(self.base, "_enqueue_notification_in_tx", None)
        if not callable(enqueue):
            raise LedgerError(
                "RESPONSIBILITY_INVALID",
                "base_ledger 缺少 _enqueue_notification_in_tx：无法在同一事务发布通知")
        ts = self._now()
        with transaction(self.conn):
            row = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            if row is None:
                raise LedgerError("RESPONSIBILITY_NOT_FOUND", f"责任不存在：{rid}")
            if row.get("closed_at") is not None:
                raise LedgerError(
                    "RESPONSIBILITY_TERMINAL",
                    f"责任 {rid} 已关闭（{row.get('close_reason')}），不发布接续")
            if row["revision"] != expected_revision:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} revision={row['revision']} != 期望 {expected_revision}")
            if row["state"] not in _PUBLISHABLE_STATES:
                raise LedgerError(
                    "RESPONSIBILITY_NOT_REVIEWABLE",
                    f"状态 {row['state']} 不自动发布接续通知"
                    "（waiting_human 表示已明确转人工，不自动唤醒）",
                    {"allowed": sorted(_PUBLISHABLE_STATES)})
            fact = self._review_fact(row)
            if fact is None:
                raise LedgerError(
                    "RESPONSIBILITY_NOT_REVIEWABLE",
                    "没有可核验的待核收事实或缺失责任诊断：不能凭待办文本发布"
                    "接续通知（需 execution ended_at / missing-responsibility / "
                    "结构化 continuation 事实）",
                    {"state": row["state"], "wait_kind": row.get("wait_kind")})
            task = self._task_in_tx(self.conn, row["task_id"])
            if task is None:
                raise LedgerError(
                    "RESPONSIBILITY_TASK_NOT_FOUND",
                    f"任务不存在：{row['task_id']}")
            if task.get("status") in ("accepted", "cancelled") \
                    or task.get("closed_at") is not None:
                raise LedgerError(
                    "RESPONSIBILITY_TASK_TERMINAL",
                    "任务已 accepted/cancelled/关闭：接续待办应关闭，不再发送",
                    {"task_status": task.get("status")})
            if task.get("status") == "paused":
                updated = self._mark_waiting_human_in_tx(
                    self.conn, row, reason="task-paused",
                    detail="任务处于暂停：接续待办保留但需人工决定后恢复", ts=ts)
                return _publish_result(updated, published=False, notification=None,
                                       target=None, reason="task-paused")
            decision = duty.deadline_decision(row, now_ms=ts)
            if decision.get("exhausted"):
                updated = self._mark_waiting_human_in_tx(
                    self.conn, row, reason=decision.get("code") or "deadline-exhausted",
                    detail=(f"期限/预算耗尽（deadline={row.get('deadline')} "
                            f"budget={row.get('budget_used')}/{row.get('max_budget')}）；"
                            "不重置期限或预算"), ts=ts)
                return _publish_result(updated, published=False, notification=None,
                                       target=None,
                                       reason=decision.get("code") or "deadline-exhausted")
            dec_stale = self._staleness_in_tx(self.conn, row, task=task)
            if dec_stale.get("status") != "fresh" \
                    and not self._unattributed(dec_stale):
                raise LedgerError(
                    "RESPONSIBILITY_STALE",
                    f"责任 {rid} 已被新代次/结果版本取代"
                    f"（domain: {dec_stale.get('code')}）：不发布旧接续",
                    {"domain_code": dec_stale.get("code")})
            target, route = self._resolve_route_in_tx(self.conn, row)
            if not _non_empty_str(target):
                updated = self._mark_waiting_human_in_tx(
                    self.conn, row, reason="no-route",
                    detail="没有可核验的当前协调者（任务 coordinator 与租约 holder 均缺失）",
                    ts=ts)
                return _publish_result(updated, published=False, notification=None,
                                       target=None, reason="no-route")

            # 状态与通知同一事务：先规范为 awaiting_review（domain.transition）。
            state_updates: dict[str, Any] = {}
            if row["state"] != duty.STATE_AWAITING_REVIEW:
                dec = duty.transition(
                    row, to_state=duty.STATE_AWAITING_REVIEW, now_ms=ts,
                    reason=f"continuation-published:{fact}", actor=actor,
                    wait_kind="review",
                    wait_condition=row.get("wait_condition") or fact,
                    next_check_at=row.get("next_check_at"))
                if not dec.get("allowed"):
                    raise _transition_error(dec, rid, what="publish")
                state_updates = {k: v for k, v in (dec.get("updates") or {}).items()
                                 if k in _STATE_COLUMNS}

            condition = condition_version(
                row.get("generation"), row.get("result_version"))
            dedupe_key = f"resp:{row['id']}:{row['condition_key']}:{condition}"
            # 依赖条件满足事实：必须复用 record_dependency_review 的**原**
            # review dedupe（责任 + 条件身份 + 证据版本），禁止普通 publish 生成
            # 第二种去重键；已入队的直接幂等复用，不写、不改 revision。
            if fact == "dependency-satisfied":
                dep_ev = row.get("evidence") or {}
                dep_fact = dep_ev.get(depwait.DEPENDENCY_KEY) \
                    if isinstance(dep_ev, dict) else None
                dep_fact = dep_fact if isinstance(dep_fact, dict) else {}
                cond_raw = dep_fact.get("condition")
                ev_version = dep_fact.get("evidence_version")
                if isinstance(cond_raw, dict) and _non_empty_str(ev_version):
                    try:
                        normalized = depwait.normalize_condition(cond_raw)
                    except ValueError:
                        normalized = None
                    if normalized is not None:
                        review_ded = depwait.dedupe_review_key(
                            rid, normalized, ev_version)
                        # 相同只读 validator：上游/current/期限/证据版本发送前复核，
                        # 已 ACK/已发送的旧复核也保原状（validator 只读，不写通知）。
                        self.check_dependency_review_notification({
                            "dedupe_key": review_ded,
                            "metadata": {"responsibility_id": rid,
                                         "condition_identity": dep_fact.get(
                                             "condition_identity"),
                                         "evidence_version": ev_version},
                            "payload": {"classification": "DEPENDENCY_SATISFIED",
                                        "evidence_version": ev_version},
                        })
                        existing = self.conn.execute(
                            "SELECT id, target_session, dedupe_key FROM notifications "
                            "WHERE dedupe_key=? ORDER BY created_at DESC LIMIT 1",
                            (review_ded,)).fetchone()
                        if existing is not None:
                            return _publish_result(
                                row, published=False, notification=dict(existing),
                                target=existing["target_session"],
                                reason="review-already-recorded")
                        dedupe_key = review_ded
            prior = self.conn.execute(
                "SELECT 1 FROM notifications WHERE dedupe_key=? LIMIT 1",
                (dedupe_key,)).fetchone()
            body = dict(payload)
            body.update({
                "responsibility_id": row["id"],
                "condition_key": row["condition_key"],
                "condition_version": condition,
                "task_id": row["task_id"],
                "attempt_id": row.get("attempt_id"),
                "generation": row.get("generation"),
                "result_version": row.get("result_version"),
                "owner": row["owner"],
                "wait_kind": row["wait_kind"],
                "wait_condition": row["wait_condition"],
                "next_action": row["next_action"],
                "deadline": row.get("deadline"),
                "review_fact": fact,
            })
            if state_updates:
                sets = ", ".join(f"{key}=?" for key in state_updates)
                cur = self.conn.execute(
                    f"UPDATE responsibilities SET {sets}, revision=revision+1, "
                    "updated_at=? WHERE id=? AND revision=?",
                    (*state_updates.values(), ts, row["id"], expected_revision))
                if cur.rowcount != 1:
                    raise LedgerError(
                        "RESPONSIBILITY_REVISION_CONFLICT",
                        f"责任 {rid} 并发修改（状态 CAS 失败），不发布接续")
            notif = enqueue(
                self.conn, kind=kind, target_session=target,
                dedupe_key=dedupe_key, payload=body, project=row["project"],
                task_id=row["task_id"], attempt_id=row.get("attempt_id"),
                metadata={"responsibility_id": row["id"],
                          "condition_key": row["condition_key"],
                          "condition_version": condition,
                          "route": route, "review_fact": fact})
            self._audit(self.conn, actor, "responsibility.publish", rid,
                        row["state"], duty.STATE_AWAITING_REVIEW,
                        {"kind": kind, "notification": notif.get("id"),
                         "target": target, "route": route,
                         "dedupe_key": dedupe_key, "review_fact": fact,
                         "state_changed": bool(state_updates),
                         "duplicate": prior is not None})
            fresh = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            if fact == "dependency-satisfied":
                # 把通知引用写回满足事实（同一事务），后续 publish/record 幂等复用。
                fe = dict(fresh.get("evidence") or {})
                fd = dict(fe.get(depwait.DEPENDENCY_KEY)
                          if isinstance(fe.get(depwait.DEPENDENCY_KEY), dict) else {})
                if fd.get("notification_id") != notif.get("id"):
                    fd["notification_id"] = notif.get("id")
                    fd["review_dedupe_key"] = dedupe_key
                    fe[depwait.DEPENDENCY_KEY] = fd
                    self.conn.execute(
                        "UPDATE responsibilities SET evidence=? WHERE id=? AND revision=?",
                        (json_dumps(fe), rid, fresh["revision"]))
            return _publish_result(fresh, published=prior is None,
                                   notification=notif, target=target,
                                   reason=route)


# ---- 依赖等待（C13–C16；增量，不删除旧逻辑） ------------------------------
    def list_dependency_waits_page(self, *, after_id: str | None = None,
                                   limit: int = 100) -> list[dict]:
        """未关闭依赖等待责任的 keyset 分页，按 ``id`` 升序。

        覆盖三类行（runtime 普通 tick 会对这些行跳过，本页即普通依赖对账的
        唯一来源）：

        - ``state='waiting_dependency'`` 或 ``wait_kind='dependency'``（未满足）；
        - ``evidence.dependency.satisfied=true`` 且未关闭（已满足、含暂停积累的
          awaiting_review 尚未发布行——任务 resume 后由 reconciliation 重新核验
          并以原 dedupe 发布一次）。

        供调谐循环持久游标公平扫描：不扫全局 session、不抬 limit；尾部回绕由
        调用方把 ``after_id`` 复位为 None。截断可由 ``count_dependency_waits``
        对比确认（读到的行数 < 总数即无法证明全图无环）。
        """
        self._require_schema()
        lim = _bounded_limit(limit)
        sql = ("SELECT * FROM responsibilities WHERE closed_at IS NULL "
               "AND (state=? OR wait_kind=? OR "
               "json_extract(evidence,'$.dependency.satisfied') = 1) "
               "AND COALESCE(wait_kind,'') NOT IN (?,?,?,?)")
        args: list[Any] = [
            duty.STATE_WAITING_DEPENDENCY, "dependency",
            *sorted(_S4_OWNED_DEPENDENCY_WAIT_KINDS),
        ]
        if after_id is not None:
            if not _non_empty_str(after_id):
                raise LedgerError(
                    "RESPONSIBILITY_INVALID", "after_id 必须是非空字符串或 None")
            sql += " AND id > ?"
            args.append(after_id)
        sql += " ORDER BY id ASC LIMIT ?"
        args.append(lim)
        rows = self.conn.execute(sql, args).fetchall()
        return [d for d in (_row_to_dict(r) for r in rows) if d is not None]

    def count_dependency_waits(self) -> int:
        """未关闭依赖等待责任总数（与 list_dependency_waits_page 同一谓词）。

        用于检测 keyset 分页截断：读不到的行不能当作“全图无环/无依赖”。
        """
        self._require_schema()
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM responsibilities WHERE closed_at IS NULL "
            "AND (state=? OR wait_kind=? OR "
            "json_extract(evidence,'$.dependency.satisfied') = 1) "
            "AND COALESCE(wait_kind,'') NOT IN (?,?,?,?)",
            (duty.STATE_WAITING_DEPENDENCY, "dependency",
             *sorted(_S4_OWNED_DEPENDENCY_WAIT_KINDS))).fetchone()
        return int(row["c"]) if row else 0

    def _dependency_task_facts_in_tx(self, conn, task_ids) -> dict[str, dict]:
        """事务内（或只读连接上）的任务事实快照；供 record/reconcile 用同一
        BEGIN IMMEDIATE 快照重新评估，防止网络/读写竞态用旧证据。

        ``effective``（可核验的有效验收，只信真实 submit_verification 产物）要求
        全部成立：

        - 任务是当前尝试所在任务且 ``status='accepted'``、``accepted_at`` 非空；
        - 当前 attempt 就是 ``task.current_attempt_id``（归属）；\n
        - verification 为 ``oc-orch.verification.v1``、result=pass、非 force；\n
        - verification 绑定的结果版本（``result_snapshot.result_version``）与\n
          当前收集结果版本（``metadata.result.version``）完全一致。\n
        任一缺失 → ``effective=False``（仅字符串 accepted / 旧代结果 / force\n
        一律不算有效验收）。
        """
        if not isinstance(task_ids, (list, tuple)) or not task_ids:
            return {}
        seen: set[str] = set()
        ordered: list[str] = []
        for raw in task_ids:
            if not _non_empty_str(raw):
                continue
            key = raw.strip()
            if key not in seen:
                seen.add(key)
                ordered.append(key)
        facts: dict[str, dict] = {}
        if not ordered:
            return facts
        placeholders = ", ".join("?" for _ in ordered)
        rows = conn.execute(
            f"SELECT * FROM tasks WHERE id IN ({placeholders})",
            tuple(ordered)).fetchall()
        attempts: dict[str, dict] = {}
        for r in rows:
            data = dict(r)
            cur = data.get("current_attempt_id")
            if _non_empty_str(cur):
                attempts[cur] = None
        if attempts:
            att_ph = ", ".join("?" for _ in attempts)
            for att_row in conn.execute(
                    f"SELECT * FROM attempts WHERE id IN ({att_ph})",
                    tuple(attempts)).fetchall():
                att = dict(att_row)
                att["metadata"] = json_loads(att.get("metadata"), {})
                att["verification"] = json_loads(att.get("verification"), None)
                attempts[att["id"]] = att
        for r in rows:
            data = dict(r)
            task_id = data.get("id")
            cur_id = data.get("current_attempt_id")
            attempt = attempts.get(cur_id) if cur_id else None
            verification = (attempt or {}).get("verification")
            stored_version = None
            if attempt is not None:
                stored = ((attempt.get("metadata") or {}).get("result") or {})
                sv = stored.get("version") if isinstance(stored, dict) else None
                stored_version = sv if _non_empty_str(sv) else None
            bound_version = None
            if isinstance(verification, dict):
                rs = verification.get("result_snapshot")
                if isinstance(rs, dict) and _non_empty_str(rs.get("result_version")):
                    bound_version = rs["result_version"]
                elif _non_empty_str(verification.get("result_version")):
                    bound_version = verification["result_version"]
            current_attribution = bool(
                attempt is not None and cur_id is not None
                and attempt.get("id") == cur_id)
            effective = bool(
                data.get("status") == "accepted"
                and data.get("accepted_at") is not None
                and current_attribution
                and isinstance(verification, dict)
                and verification.get("schema") == "oc-orch.verification.v1"
                and verification.get("result") == "pass"
                and not bool(verification.get("forced"))
                and _non_empty_str(bound_version)
                and bound_version == stored_version)
            facts[task_id] = {
                "exists": True, "status": data.get("status"),
                "accepted_at": data.get("accepted_at"),
                "closed_at": data.get("closed_at"),
                "current_attempt_id": cur_id,
                "generation": (attempt or {}).get("generation"),
                "result_version": stored_version,
                "verify_result_version": bound_version,
                "effective": effective,
                "verification": verification,
            }
        for task_id in ordered:
            if task_id not in facts:
                facts[task_id] = {"exists": False, "status": None,
                                  "accepted_at": None, "closed_at": None,
                                  "current_attempt_id": None, "generation": None,
                                  "result_version": None,
                                  "verify_result_version": None,
                                  "effective": False, "verification": None}
        return facts

    def dependency_task_facts(self, task_ids) -> dict[str, dict]:
        """当前台账中一批任务的事实快照（只读；不做网络/Client 调用）。

        ``effective`` 只认真实 submit_verification 产物（见
        ``_dependency_task_facts_in_tx``）：当前 attempt 归属 + accepted 状态 +
        非 force 的 pass + 结果版本与 verification 绑定一致。
        """
        self._require_schema()
        return self._dependency_task_facts_in_tx(self.conn, task_ids)

    def ensure_dependency_wait(
        self, owner: str, project: str, task_id: str, condition: dict, *,
        attempt_id: str | None = None, generation: int | None = None,
        result_version: str | None = None, target_session: str | None = None,
        deadline: int, next_check_at: int, next_action: str,
        actor: str = "coordinator", max_budget: int = 3,
    ) -> dict:
        """登记/幂等复用一条依赖等待责任（C13/C14）。

        结构化条件写入 ``evidence.dependency.condition``，``wait_condition`` 保留
        稳定摘要（非空字符串，供既有展示/审计）。同一 (task, condition_key,
        generation, result_version) 幂等返回原行；条件版本变化（正常化后的
        condition_identity 不同）→ 新身份（新条件版本即新等待事实）。
        """
        normalized = depwait.normalize_condition(condition)
        wait_key = depwait.condition_identity(normalized)
        description = depwait.describe_condition(normalized)
        return self.open_responsibility(
            owner, project, task_id, attempt_id=attempt_id, generation=generation,
            result_version=result_version, target_session=target_session,
            state=duty.STATE_WAITING_DEPENDENCY, wait_kind="dependency",
            wait_condition=description, next_action=next_action,
            next_check_at=next_check_at, deadline=deadline,
            condition_key=wait_key, evidence={
                depwait.DEPENDENCY_KEY: {
                    "schema": depwait.CONDITION_SCHEMA,
                    "condition": normalized,
                    "condition_identity": wait_key,
                    "satisfied": False,
                }
            },
            max_budget=max_budget, actor=actor)

    def record_dependency_review(
        self, rid: str, *, expected_revision: int, now: int | None = None,
        actor: str = "supervisor") -> dict:
        """依赖条件满足：事务内**重读真实事实重新评估**后以原 dedupe 登记复核。

        不信任调用方传入的 condition/evidence_version：本方法只认责任行
        ``evidence.dependency.condition`` 的原登记条件，并在同一 BEGIN IMMEDIATE
        内重新读取全部依赖 Task 的 current/generation/result_version/有效 verify
        事实，用 ``domain.evaluate_task_condition`` 重新评估；**必须** satisfied
        且证据版本匹配（评估在事务内重新计算）才写入满足事实并发布。任何
        伪造条件、条件身份不匹配、评估为 waiting/unknown/broken/invalid 或
        证据已在事务内变化（staleness）一律拒绝且不写任何通知。

        只登记复核通知——不直接派单、不 prompt、不代替验收。语义：\n
        - 已登记满足事实且**已发布**（``evidence.dependency.notification_id``\n
          或原 ``review_dedupe_key`` 已存在通知）→ 幂等复用该通知，不写、不减\n
          revision、不重复审计；\n
        - 已满足但**未发布**（暂停积累 awaiting_review / 恢复待发）→ 任务 resume\n
          后重新核验原条件 + 原证据版本，用**原 dedupe** 发布一次（不改状态，\n
          state 已是 awaiting_review）；\n
        - 暂停任务只积累满足事实并规范为 awaiting_review，**不创建唤醒通知**；\n
        - 职责上本方法是唯一写入 ``evidence.dependency.satisfied`` 的入口，调用方\n
          不能凭任意文本/dict 伪造本事实。

        返回 ``{"recorded", "published", "notification", "dedupe_key", "state",
        "reason", "row"}``。
        """
        self._require_schema()
        ts = now if (isinstance(now, int) and not isinstance(now, bool)) else self._now()
        enqueue = getattr(self.base, "_enqueue_notification_in_tx", None)
        if not callable(enqueue):
            raise LedgerError(
                "RESPONSIBILITY_INVALID",
                "base_ledger 缺少 _enqueue_notification_in_tx：无法同一事务发布复核通知")
        with transaction(self.conn):
            row = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            if row is None:
                raise LedgerError("RESPONSIBILITY_NOT_FOUND", f"责任不存在：{rid}")
            if row.get("closed_at") is not None:
                raise LedgerError(
                    "RESPONSIBILITY_TERMINAL",
                    f"责任 {rid} 已关闭（{row.get('close_reason')}），不登记复核")
            if row["revision"] != expected_revision:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} revision={row['revision']} != 期望 {expected_revision}")
            if row["state"] == duty.STATE_WAITING_HUMAN:
                return {"recorded": False, "published": False, "notification": None,
                        "state": row["state"], "reason": "manual-hold",
                        "row": _row_to_dict(self.conn.execute(
                            "SELECT * FROM responsibilities WHERE id=?",
                            (rid,)).fetchone())}
            evidence = dict(row.get("evidence") or {})
            dep = dict(evidence.get(depwait.DEPENDENCY_KEY)
                       if isinstance(evidence.get(depwait.DEPENDENCY_KEY), dict)
                       else {})
            condition_raw = dep.get("condition")
            if not isinstance(condition_raw, dict):
                return {"recorded": False, "published": False, "notification": None,
                        "state": row["state"], "reason": "condition-unstructured",
                        "row": row}
            try:
                normalized = depwait.normalize_condition(condition_raw)
            except ValueError as exc:
                return {"recorded": False, "published": False, "notification": None,
                        "state": row["state"],
                        "reason": "condition-invalid", "detail": str(exc)[:200],
                        "row": row}
            cid = depwait.condition_identity(normalized)
            if row.get("condition_key") != cid:
                return {"recorded": False, "published": False, "notification": None,
                        "state": row["state"], "reason": "condition-identity-mismatch",
                        "expected": cid, "stored": row.get("condition_key"),
                        "row": row}
            if _non_empty_str(dep.get("condition_identity")) \
                    and dep.get("condition_identity") != cid:
                return {"recorded": False, "published": False, "notification": None,
                        "state": row["state"], "reason": "condition-identity-mismatch",
                        "expected": cid, "stored": dep.get("condition_identity"),
                        "row": row}

            # 已登记满足且已发布 → 幂等复用原通知，不写、不改 revision。
            existing_notif = None
            if dep.get("satisfied") is True:
                by_id = dep.get("notification_id")
                by_key = dep.get("review_dedupe_key")
                if _non_empty_str(by_key):
                    found = self.conn.execute(
                        "SELECT * FROM notifications WHERE dedupe_key=? "
                        "ORDER BY created_at DESC LIMIT 1", (by_key,)).fetchone()
                    if found is not None:
                        existing_notif = dict(found)
                elif _non_empty_str(by_id):
                    found = self.conn.execute(
                        "SELECT * FROM notifications WHERE id=? LIMIT 1",
                        (by_id,)).fetchone()
                    if found is not None:
                        existing_notif = dict(found)
                if existing_notif is not None and dep.get("satisfied") is True:
                    # 已发布也要先复核：上游/current/期限/证据版本变化则拒（不外发）。
                    self.check_dependency_review_notification({
                        "dedupe_key": by_key,
                        "metadata": {"responsibility_id": rid,
                                     "condition_identity": cid,
                                     "evidence_version":
                                         dep.get("evidence_version")},
                        "payload": {"classification": "DEPENDENCY_SATISFIED"},
                    })
                    return {"recorded": True, "published": False,
                            "notification": existing_notif,
                            "state": row["state"], "reason": "already-published",
                            "dedupe_key": by_key, "row": row}

            task = self._task_in_tx(self.conn, row["task_id"])
            if task is None:
                updated = self._mark_waiting_human_in_tx(
                    self.conn, row, reason="task-missing",
                    detail="依赖等待责任绑定的 Task 不存在：转人工诊断，不登记复核",
                    ts=ts)
                return {"recorded": False, "published": False, "notification": None,
                        "state": updated["state"], "reason": "task-missing",
                        "row": updated}
            if task.get("status") in ("accepted", "cancelled") \
                    or task.get("closed_at") is not None:
                raise LedgerError(
                    "RESPONSIBILITY_TASK_TERMINAL",
                    "任务已 accepted/cancelled/关闭：复核待办应关闭，不再登记",
                    {"task_status": task.get("status")})
            paused_task = task.get("status") == "paused"
            decision = duty.deadline_decision(row, now_ms=ts)
            if decision.get("exhausted"):
                updated = self._mark_waiting_human_in_tx(
                    self.conn, row,
                    reason=decision.get("code") or "deadline-exhausted",
                    detail=f"依赖满足但期限/预算已耗尽（{row.get('deadline')} "
                           f"{row.get('budget_used')}/{row.get('max_budget')}）；不重置",
                    ts=ts)
                return {"recorded": False, "published": False, "notification": None,
                        "state": updated["state"],
                        "reason": decision.get("code") or "deadline-exhausted",
                        "row": updated}
            dec_stale = self._staleness_in_tx(self.conn, row, task=task)
            if dec_stale.get("status") != "fresh" \
                    and not self._unattributed(dec_stale):
                raise LedgerError(
                    "RESPONSIBILITY_STALE",
                    f"责任 {rid} 已被新代次/结果版本取代"
                    f"（domain: {dec_stale.get('code')}）：不登记旧复核",
                    {"domain_code": dec_stale.get("code")})

            # 事务内重读真实依赖事实并重新评估（拒 stale/任意版本）。
            facts = self._dependency_task_facts_in_tx(
                self.conn, normalized.get("task_ids"))
            evaluation = depwait.evaluate_task_condition(normalized, facts)
            if evaluation["status"] != depwait.SATISFIED:
                return {"recorded": False, "published": False, "notification": None,
                        "state": row["state"], "reason": "condition-not-satisfied",
                        "status": evaluation["status"],
                        "evaluation_reason": evaluation.get("reason"),
                        "row": row}
            evidence_version = evaluation.get("evidence_version")
            if not _non_empty_str(evidence_version):
                return {"recorded": False, "published": False, "notification": None,
                        "state": row["state"], "reason": "evidence-version-missing",
                        "row": row}
            review_dedupe = depwait.dedupe_review_key(
                rid, normalized, evidence_version)
            # 同原条件/原证据的 dedupe 已存在通知（含并发窗口）→ 幂等复用。
            prior = self.conn.execute(
                "SELECT id, state, target_session FROM notifications "
                "WHERE dedupe_key=? ORDER BY created_at DESC LIMIT 1",
                (review_dedupe,)).fetchone()
            if prior is not None:
                return {"recorded": True, "published": False,
                        "notification": dict(prior),
                        "state": row["state"], "reason": "already-published",
                        "dedupe_key": review_dedupe, "row": row}

            review = {
                "schema": depwait.CONDITION_SCHEMA,
                "condition": normalized,
                "condition_identity": cid,
                "condition_key": depwait.record_condition_key(rid, normalized),
                "satisfied": True,
                "evidence_version": evidence_version,
                "review_dedupe_key": review_dedupe,
                "satisfied_at": ts,
                "task_facts": {k: {"status": v.get("status"),
                                   "result_version": v.get("result_version"),
                                   "verify_result_version": v.get(
                                       "verify_result_version"),
                                   "effective": bool(v.get("effective"))}
                               for k, v in facts.items()},
            }
            evidence[depwait.DEPENDENCY_KEY] = review
            state_updates: dict[str, Any] = {}
            updates: dict[str, Any] = {"evidence": json_dumps(evidence),
                                       "next_action": (
                                           "依赖条件已达有效验收证据：协调者核收后继/"
                                           "结果，不自动 accepted；暂停仍不唤醒")}
            if row["state"] != duty.STATE_AWAITING_REVIEW:
                dec = duty.transition(
                    row, to_state=duty.STATE_AWAITING_REVIEW, now_ms=ts,
                    actor=actor, reason="dependency-satisfied",
                    wait_kind="review",
                    wait_condition=depwait.describe_condition(normalized),
                    next_check_at=ts)
                if not dec.get("allowed"):
                    raise _transition_error(dec, rid, what="dependency-review")
                for key, value in (dec.get("updates") or {}).items():
                    if key in _STATE_COLUMNS:
                        state_updates[key] = value
            updates.update(state_updates)
            sets = ", ".join(f"{key}=?" for key in updates)
            cur = self.conn.execute(
                f"UPDATE responsibilities SET {sets}, revision=revision+1, "
                "updated_at=? WHERE id=? AND revision=?",
                (*updates.values(), ts, rid, expected_revision))
            if cur.rowcount != 1:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} 并发修改（复核 CAS 失败），不重复登记")
            fresh = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            if paused_task:
                # 暂停只积累复核事实不唤醒：事实已落库、状态已到 awaiting_review，
                # 恢复后由父级统一守卫再发布（这里不动通知、不额外审计）。
                return {"recorded": True, "published": False, "notification": None,
                        "state": fresh["state"], "reason": "task-paused",
                        "dedupe_key": review_dedupe, "row": fresh}
            target, route = self._resolve_route_in_tx(self.conn, row)
            if not _non_empty_str(target):
                updated = self._mark_waiting_human_in_tx(
                    self.conn, fresh, reason="no-route",
                    detail="依赖满足但没有可核验的当前协调者：转人工指定责任人", ts=ts)
                return {"recorded": True, "published": False, "notification": None,
                        "state": updated["state"], "reason": "no-route",
                        "dedupe_key": review_dedupe, "row": updated}
            payload = {
                "classification": "DEPENDENCY_SATISFIED",
                "summary": ("显式依赖条件已满足（结构化验收证据），生成去重复核"
                            "待办；由当前合法协调者核收，不自动 accepted"),
                "task_id": row["task_id"],
                "attempt_id": row.get("attempt_id"),
                "generation": row.get("generation"),
                "result_version": row.get("result_version"),
                "owner": row["owner"],
                "condition_identity": cid,
                "condition_version": condition_version(
                    row.get("generation"), row.get("result_version")),
                "evidence_version": evidence_version,
                "next_action": "核收依赖满足事实并判断后继/交付物；不按标题猜关联",
            }
            notif = enqueue(
                self.conn, kind=CONTINUATION_KIND,
                target_session=target, dedupe_key=review_dedupe, payload=payload,
                project=row["project"], task_id=row["task_id"],
                attempt_id=row.get("attempt_id"),
                metadata={"responsibility_id": row["id"],
                          "condition_identity": cid,
                          "evidence_version": evidence_version,
                          "route": route})
            # 把通知引用写回满足事实（同一事务；后续轮次靠它幂等跳过）。
            fresh_evidence = dict(fresh.get("evidence") or {})
            fresh_dep = dict(fresh_evidence.get(depwait.DEPENDENCY_KEY)
                             if isinstance(fresh_evidence.get(
                                 depwait.DEPENDENCY_KEY), dict) else {})
            fresh_dep["notification_id"] = notif.get("id")
            fresh_dep["review_dedupe_key"] = review_dedupe
            fresh_evidence[depwait.DEPENDENCY_KEY] = fresh_dep
            self.conn.execute(
                "UPDATE responsibilities SET evidence=?, updated_at=? "
                "WHERE id=? AND revision=?",
                (json_dumps(fresh_evidence), ts, rid, fresh["revision"]))
            self._audit(self.conn, actor, "responsibility.dependency-review", rid,
                        row["state"], duty.STATE_AWAITING_REVIEW,
                        {"condition_identity": cid,
                         "evidence_version": evidence_version,
                         "notification": notif.get("id"), "route": route,
                         "duplicate": bool(prior)})
            final = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            return {"recorded": True,
                    "published": True,
                    "notification": notif,
                    "dedupe_key": review_dedupe,
                    "state": final["state"],
                    "reason": route,
                    "row": final}

    def check_dependency_review_notification(self, notice) -> None:
        """只读校验：一条可能对外发送的依赖复核通知在发送前是否仍有效。

        供父级 runtime guard（mark_possible / continuity_guard，经本
        ResponsibilityStore 调用）在网络前复核；**只读、不启事务、不做网络**，
        可在父的同一 BEGIN IMMEDIATE 内直接调用（复用该事务的连接快照）。

        - 普通非 dep 通知（无 responsibility_id+condition_identity 绑定、payload
          无 ``DEPENDENCY_SATISFIED`` 分类）→ 直接返回 None，no-op；
        - dep 通知：必须绑定原责任 id / 原 review dedupe / 原证据版本，且责任未
          关闭、非 waiting_human；等待任务 current/generation/result_version 归属
          仍是本责任；**重新读取原上游依赖事实**并 `evaluate_task_condition` 必须
          satisfied 且证据版本与发布的 evidence_version 一致；期限/预算未耗尽。
          任一不满足 → 抛 ``LedgerError(DEPENDENCY_REVIEW_STALE, ...)``，code 稳定；
        - 缺绑定 / 证据无法解读 → fail-closed 抛错（不静默放行）。
        """
        self._require_schema()
        if not isinstance(notice, dict):
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE", "通知必须是 dict 才能校验依赖复核",
                {"notice": type(notice).__name__})
        metadata = notice.get("metadata") if isinstance(notice.get("metadata"), dict) else {}
        payload = notice.get("payload") if isinstance(notice.get("payload"), dict) else {}
        rid = (metadata.get("responsibility_id") or payload.get("responsibility_id")
               or notice.get("responsibility_id"))
        cid = metadata.get("condition_identity")
        bind_rid = _non_empty_str(rid)
        bind_cid = _non_empty_str(cid)
        dep_classified = payload.get("classification") == "DEPENDENCY_SATISFIED"
        if not (bind_rid and bind_cid) and not dep_classified:
            return  # 普通非 dep 通知 no-op
        if not bind_rid or not bind_cid:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                "依赖复核通知缺少 responsibility_id/condition_identity 绑定（fail-closed）",
                {"responsibility_id": rid, "condition_identity": cid})
        row = self.get_responsibility(rid)
        if row is None:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE", f"依赖复核责任不存在：{rid}",
                {"responsibility_id": rid})
        if row.get("closed_at") is not None:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 已关闭（{row.get('close_reason')}）：复核已失效力",
                {"state": row.get("state")})
        if row.get("state") == duty.STATE_WAITING_HUMAN:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 已转人工（manual-hold）：不自动发送依赖复核",
                {"state": row.get("state")})
        evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
        dep = evidence.get(depwait.DEPENDENCY_KEY)
        dep = dep if isinstance(dep, dict) else {}
        if dep.get("satisfied") is not True:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 的满足事实已被撤销/未登记：不发送",
                {"satisfied": dep.get("satisfied")})
        row_cid = dep.get("condition_identity") or row.get("condition_key")
        if cid != row_cid:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 条件身份已被替换：通知绑定 {cid!r} != 当前 {row_cid!r}",
                {"notification_condition_identity": cid, "row": row_cid})
        condition_raw = dep.get("condition")
        if not isinstance(condition_raw, dict):
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 原登记条件缺失/不可解读（fail-closed）",
                {"rid": rid})
        try:
            normalized = depwait.normalize_condition(condition_raw)
        except ValueError as exc:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 原条件无效（{str(exc)[:120]}），fail-closed",
                {"rid": rid}) from exc
        if _non_empty_str(row.get("condition_key")) \
                and row["condition_key"] != depwait.condition_identity(normalized):
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} condition_key 与已登记条件不一致",
                {"condition_key": row.get("condition_key")})
        expected_evidence_version = dep.get("evidence_version")
        notice_evidence_version = metadata.get("evidence_version") or payload.get(
            "evidence_version")
        if _non_empty_str(notice_evidence_version) \
                and notice_evidence_version != expected_evidence_version:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 证据版本不匹配：通知 {notice_evidence_version!r} "
                f"!= 责任 {expected_evidence_version!r}",
                {})
        if not _non_empty_str(expected_evidence_version):
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 缺证据版本（fail-closed），不发送", {"rid": rid})
        expected_dedupe = dep.get("review_dedupe_key") or depwait.dedupe_review_key(
            rid, normalized, expected_evidence_version)
        notice_dedupe = notice.get("dedupe_key") or notice.get("dedupe")
        if _non_empty_str(notice_dedupe) and notice_dedupe != expected_dedupe:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 复核 dedupe 不匹配：通知 {notice_dedupe!r} "
                f"!= 责任 {expected_dedupe!r}",
                {"expected": expected_dedupe})
        task = self._task_in_tx(self.conn, row["task_id"])
        if task is None:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 的等待任务不存在", {"task_id": row.get("task_id")})
        if task.get("status") in ("accepted", "cancelled") \
                or task.get("closed_at") is not None:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"等待任务 {row.get('task_id')} 已收口：不应再发送依赖复核",
                {"task_status": task.get("status")})
        dec = self._staleness_in_tx(self.conn, row, task=task)
        if dec.get("status") != "fresh" and not self._unattributed(dec):
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 等待任务 current/generation/result_version 已变化"
                f"（domain: {dec.get('code')}）：不发送旧复核",
                {"domain_code": dec.get("code"), "status": dec.get("status")})
        decision = duty.deadline_decision(row, now_ms=self._now())
        if decision.get("exhausted"):
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 期限/预算已耗尽（{decision.get('code')}）：不发送",
                {"code": decision.get("code")})
        facts = self._dependency_task_facts_in_tx(self.conn, normalized.get("task_ids"))
        evaluation = depwait.evaluate_task_condition(normalized, facts)
        if evaluation["status"] != depwait.SATISFIED:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"上游依赖有效验收已被撤销/变化（{evaluation['reason']}）：不发送",
                {"status": evaluation["status"]})
        if evaluation.get("evidence_version") != expected_evidence_version:
            raise LedgerError(
                "DEPENDENCY_REVIEW_STALE",
                f"责任 {rid} 上游证据版本已变化：当前 "
                f"{evaluation.get('evidence_version')!r} != 发布 "
                f"{expected_evidence_version!r}",
                {"current": evaluation.get("evidence_version"),
                 "published": expected_evidence_version})
        return None

    def escalate_dependency_human(self, rid: str, *, expected_revision: int,
                                  reason: str, detail: str, now: int | None = None,
                                  next_check_at: int | None = None) -> dict:
        """失效/循环/未知/到期依赖 → 稳定人工责任（C15），不无限顺延。

        单事务：把责任规范为 ``waiting_human`` + 写入 ``evidence.blocked``（含
        dependency 原因与条件），并落一条按责任+原因去重的告警。owner/deadline/
        next_action 保留（可更新 next_action 说明下一步），预算不重置。
        """
        self._require_schema()
        if not _non_empty_str(reason):
            raise LedgerError("RESPONSIBILITY_INVALID", "escalate 需要非空 reason")
        ts = now if (isinstance(now, int) and not isinstance(now, bool)) else self._now()
        with transaction(self.conn):
            row = _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())
            if row is None:
                raise LedgerError("RESPONSIBILITY_NOT_FOUND", f"责任不存在：{rid}")
            if row.get("closed_at") is not None:
                raise LedgerError(
                    "RESPONSIBILITY_TERMINAL",
                    f"责任 {rid} 已关闭（{row.get('close_reason')}），不转人工")
            if row["revision"] != expected_revision:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} revision={row['revision']} != 期望 {expected_revision}")
            evidence = dict(row.get("evidence") or {})
            blocked = dict(evidence.get("blocked")
                           if isinstance(evidence.get("blocked"), dict) else {})
            blocked.update({"reason": reason, "at": ts,
                            "detail": detail[:400], "dependency": True})
            evidence["blocked"] = blocked
            updates: dict[str, Any] = {"evidence": json_dumps(evidence)}
            if _non_empty_str(detail):
                updates["next_action"] = (f"人工处理（{reason}）：{detail[:200]}")
            if next_check_at is not None:
                updates["next_check_at"] = _valid_clock(next_check_at,
                                                        "next_check_at")
            if row["state"] != duty.STATE_WAITING_HUMAN:
                dec = duty.transition(
                    row, to_state=duty.STATE_WAITING_HUMAN, now_ms=ts,
                    actor="supervisor", reason=reason, wait_kind="human",
                    wait_condition=reason, next_check_at=updates.get(
                        "next_check_at", row.get("next_check_at")))
                if dec.get("allowed"):
                    for key, value in (dec.get("updates") or {}).items():
                        if key in _STATE_COLUMNS:
                            updates[key] = value
                else:
                    updates["state"] = duty.STATE_WAITING_HUMAN
            sets = ", ".join(f"{key}=?" for key in updates)
            cur = self.conn.execute(
                f"UPDATE responsibilities SET {sets}, revision=revision+1, "
                "updated_at=? WHERE id=? AND revision=?",
                (*updates.values(), ts, rid, expected_revision))
            if cur.rowcount != 1:
                raise LedgerError(
                    "RESPONSIBILITY_REVISION_CONFLICT",
                    f"责任 {rid} 并发修改（转人工 CAS 失败）")
            condition_identity = None
            dep = evidence.get(depwait.DEPENDENCY_KEY)
            if isinstance(dep, dict) and dep.get("condition_identity"):
                condition_identity = dep.get("condition_identity")
            dedupe = (f"dep-human:{rid}:{reason}:{condition_identity or '-'}:"
                      f"{row.get('condition_key')}")
            if hasattr(self.base, "_insert_alert_in_tx"):
                self.base._insert_alert_in_tx(
                    self.conn, severity="high",
                    kind="responsibility_blocked",
                    title=f"依赖等待转人工：{row['task_id']}",
                    detail=(f"责任 {rid} task={row['task_id']} owner={row['owner']} "
                            f"原因={reason}；{detail}"),
                    dedupe_key=dedupe, task_id=row["task_id"],
                    attempt_id=row.get("attempt_id"))
            self._audit(self.conn, "supervisor",
                        "responsibility.dependency-human", rid,
                        row["state"], duty.STATE_WAITING_HUMAN,
                        {"reason": reason,
                         "condition_identity": condition_identity})
            return _row_to_dict(self.conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone())


__all__ = [
    "ResponsibilityStore", "condition_version", "CONTINUATION_KIND",
    "MISSING_CONDITION_KEY", "STATES", "OPEN_STATES", "UPDATABLE_FIELDS",
    "MAX_CLOCK_MS",
    "STATE_HANDLING", "STATE_WAITING_INPUT", "STATE_WAITING_EXECUTION",
    "STATE_WAITING_DEPENDENCY", "STATE_WAITING_HUMAN", "STATE_AWAITING_REVIEW",
    "STATE_HANDED_OVER", "STATE_DONE", "STATE_CANCELLED",
]

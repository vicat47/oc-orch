"""业务 incident 存储与源重验：meta 自有命名空间 + 短事务（roadmap R3）。

只复用 ``meta`` 表与 :func:`oc_orch.adapters.sqlite_db.transaction`，**不建新业务表、
不改公共 schema**；每个 incident 持久化为 ``incident:item:{incident_id}`` 单条
JSON，schema 版本记录在 ``incident:schema``；公平游标持久在
``incident:cursor:{kind}``（来源扫描/恢复扫描各自独立，只读 list 不写游标）。
对 responsibilities/tasks/attempts/notifications 只做**有界只读 SELECT**（可信
覆盖；读不到 → fail-closed 不产出可行动事件）。

安全不变量（父核收清单）：

- **严格校验**：``_load``/``_store`` 前后统一过 ``_validate_record``；任何坏字段
  抛 :class:`IncidentStateError`（fail-closed）。
- **短事务 + CAS**：每次写入在 ``BEGIN IMMEDIATE`` 内重读并 CAS；网络从未出现在
  本层。
- **源重验五态**：``recheck_binding(conn, binding)`` 返回 active/resolved/stale/
  paused/unknown。只读同一 SQLite conn（禁止任意网络回调）。它既是 incident
  恢复扫描的依据，也作为 :class:`IncidentSourceValidator` 注入 ``EmailOutbox``，
  使“入 outbox”与“真实发送 claim”都在**同一事务内**完成源归属/条件版本/人工
  控制以及项目暂停/回合 hold/provider 冷却重验：
  - resolved：只有独立确定底层条件已解除（责任关闭/任务收口/no_progress 已
    有推进机制）才在同一事务重验后 CAS 恢复；
  - read-failed/缺数据 → unknown，保留 open、不发恢复邮件；
  - 项目暂停/会话 hold/provider 冷却 → paused，**不是故障恢复**，绝不把
    paused 抑制当 recovered，其他项目冷却不抑制本项目；
  - 状态/条件键/签名/归属/换代变化 → stale（抑制、不恢复、不发送）。
- **恢复事件 claim 重验底层源**：incident-kind 绑定加载 incident 后进一步重验其
  ``source_binding`` 仍为 definitely-resolved，不只信本地 ``state=recovered``。
- **重开**：已 recovered/suppressed 的 incident 重遇同一条件状态或新条件 →
  以新版本重开（不吞第二次故障、不复用旧 event_key）。
- 严格参数类型（limit 必须 int>=1，bool/非整数拒收）；keyset 公平游标避免
  head-LIMIT 饿死尾项。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from ..domain import operator_incident as inc
from ..domain import responsibility as duty
from ..domain.errors import LedgerError
from ..domain.coordinator_control import CoordinatorControlStateError
from ..domain.operator_email import (
    CONDITION_HUMAN,
    CONDITION_PAUSE,
    CONDITION_UNKNOWN,
)
from ..domain.rules import TASK_STATUSES, TASK_TERMINAL, ATTEMPT_STATUSES, ATTEMPT_TERMINAL
from ..util import json_loads
from .coordinator_control import CoordinatorControlLedger
from .sqlite_db import meta_get, meta_set, read_snapshot, transaction
from .target_gate import TargetGate, TargetGateStateError

INCIDENT_SCHEMA = 1
INCIDENT_SCHEMA_KEY = "incident:schema"
INCIDENT_KEY_PREFIX = "incident:item:"
INCIDENT_CURSOR_PREFIX = "incident:cursor:"

MAX_CLOCK_MS = 8 * 10 ** 12

# no_progress 解除=resolved / 未知=unknown / 正常暂停=paused 的 reason 归集。
_RESOLVED_NP_REASONS = frozenset({"open-attempt", "open-responsibility",
                                  "pending-notification"})
_PAUSED_NP_REASONS = frozenset({"task-paused", "project-paused"})
_UNKNOWN_NP_REASONS = frozenset({"task-status-unknown",
                                 "responsibilities-read-failed",
                                 "notifications-read-failed",
                                 "attempt-read-failed", "attempt-missing"})


class IncidentStateError(RuntimeError):
    """incident 记录损坏、结构非法或 schema 不兼容。

    调用方必须把该错误视为“状态未知、不得发送/不得重验放行”：保留原记录转
    可见人工处置，而不是当作无记录。fail-closed。
    """


# ---- 校验辅助 ----------------------------------------------------------------
def _nk(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_int_not_bool(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _valid_ms(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if 0 <= value <= MAX_CLOCK_MS else None
    if isinstance(value, str) and value.strip().isdigit():
        num = int(value.strip())
        return num if num <= MAX_CLOCK_MS else None
    return None


def _strict_limit(limit: Any) -> int:
    """严格参数：只接受 int>=1（bool/None/非整数一律拒收，不静默扩 scope）。"""
    if not _is_int_not_bool(limit) or not 1 <= limit <= 200:
        raise ValueError("limit 必须为 1..200 的整数")
    return int(limit)


def _strict_after(value: Any) -> str | None:
    if value is None:
        return None
    if not _nk(value):
        raise ValueError("after_id 必须是非空字符串或 None")
    return value


def _item_key(incident_id: str) -> str:
    return f"{INCIDENT_KEY_PREFIX}{incident_id}"


def _cursor_key(kind: str) -> str:
    return f"{INCIDENT_CURSOR_PREFIX}{kind}"


def _validate_record(rec: dict) -> None:
    """持久 incident 记录结构性验证；任何坏字段抛 IncidentStateError。"""

    def bad(msg: str):
        raise IncidentStateError(
            f"incident 记录非法（{rec.get('incident_id')!r}）：{msg}")

    if type(rec.get("schema")) is not int or rec["schema"] != INCIDENT_SCHEMA:
        bad(f"schema={rec.get('schema')!r}")
    incident_id = rec.get("incident_id")
    if not _nk(incident_id):
        bad("incident_id 缺失")
    if not _nk(rec.get("project")):
        bad("project 缺失")
    if rec.get("incident_type") not in inc.INCIDENT_TYPES:
        bad(f"incident_type={rec.get('incident_type')!r}")
    if rec.get("source_kind") not in inc.SOURCE_KINDS:
        bad(f"source_kind={rec.get('source_kind')!r}")
    if not _nk(rec.get("source_id")):
        bad("source_id 缺失")
    if not _nk(rec.get("condition_key")):
        bad("condition_key 缺失")
    if not _nk(rec.get("condition_state")):
        bad("condition_state 缺失")
    if rec.get("condition_kind") not in (CONDITION_HUMAN, CONDITION_PAUSE,
                                         CONDITION_UNKNOWN):
        bad(f"condition_kind={rec.get('condition_kind')!r}")
    if not _nk(rec.get("source_signature")):
        bad("source_signature 缺失")
    if not _is_int_not_bool(rec.get("condition_version")) \
            or rec["condition_version"] < 1:
        bad("condition_version 非法")
    if not _is_int_not_bool(rec.get("escalation_level")) \
            or rec["escalation_level"] < 0:
        bad("escalation_level 非法")
    if rec.get("state") not in inc.INCIDENT_STATES:
        bad(f"state={rec.get('state')!r}")
    if not _is_int_not_bool(rec.get("revision")) or rec["revision"] < 1:
        bad("revision 非法")
    binding = rec.get("source_binding")
    if not isinstance(binding, dict):
        bad("source_binding 非 dict")
    for key in inc.BINDING_REQUIRED_KEYS:
        if not _nk(binding.get(key)):
            bad(f"source_binding.{key} 缺失")
    for key in inc.BINDING_OPTIONAL_KEYS:
        value = binding.get(key)
        if value is not None and not _nk(value):
            bad(f"source_binding.{key} 必须是非空字符串或 None")
    if binding["source_kind"] not in inc.SOURCE_KINDS:
        bad("source_binding.source_kind 非法")
    if (binding["source_kind"] not in (inc.SOURCE_KIND_RESPONSIBILITY, inc.SOURCE_KIND_TASK)
            or any(binding[key] != rec.get(field) for key, field in (
                ("source_kind", "source_kind"), ("source_id", "source_id"),
                ("source_project", "project"), ("source_task_id", "task_id"),
                ("source_incident_type", "incident_type"),
                ("source_condition_key", "condition_key"),
                ("source_state", "condition_state"), ("source_version", "source_signature")))):
        bad("source_binding 与 incident 身份不一致")
    for key in ("first_seen_at", "last_observed_at", "created_at", "updated_at"):
        if _valid_ms(rec.get(key)) is None:
            bad(f"{key} 非法：{rec.get(key)!r}")
    for key in ("deadline", "recovered_at"):
        value = rec.get(key)
        if value is not None and _valid_ms(value) is None:
            bad(f"{key} 非法：{value!r}")
    if not isinstance(rec.get("summary"), dict):
        bad("summary 非 dict")
    if not isinstance(rec.get("metadata"), dict):
        bad("metadata 非 dict")
    if rec.get("recipient_scope") is not None \
            and not isinstance(rec["recipient_scope"], str):
        bad("recipient_scope 非 str")
    if rec["updated_at"] < rec["created_at"]:
        bad("updated_at < created_at")
    if rec.get("state") == inc.INCIDENT_RECOVERED \
            and _valid_ms(rec.get("recovered_at")) is None:
        bad("recovered 状态缺少 recovered_at")
    if rec.get("state") != inc.INCIDENT_RECOVERED \
            and rec.get("recovered_at") is not None:
        bad("非 recovered 状态不应携带 recovered_at")


def _load(conn: sqlite3.Connection, incident_id: str) -> dict | None:
    raw = meta_get(conn, _item_key(incident_id))
    if raw is None:
        return None
    try:
        rec = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise IncidentStateError(
            f"incident 记录损坏（JSON）：{incident_id}") from exc
    if not isinstance(rec, dict):
        raise IncidentStateError(
            f"incident 记录损坏（非 dict）：{incident_id}")
    _validate_record(rec)
    if rec["incident_id"] != incident_id:
        raise IncidentStateError("incident 键与记录身份不一致")
    return rec


def _store(conn: sqlite3.Connection, rec: dict) -> None:
    _validate_record(rec)
    meta_set(conn, _item_key(rec["incident_id"]),
             json.dumps(rec, ensure_ascii=False))


def _snapshot(rec: dict) -> dict:
    return dict(rec)


def _verdict(state: str, reason: str) -> dict:
    return {"state": state, "ok": state == inc.VERDICT_ACTIVE, "reason": reason}


# ---- 源重验（纯本地，同一 conn 短读） ---------------------------------------
class IncidentSourceValidator:
    """注入 EmailOutbox 的纯本地 source 校验器（窄端口）。

    约定：``validator(conn, item) -> {"state", "ok", "reason"}``。返回
    ``ok is True``（= active）才允许创建/升级/认领；其余五态在 outbox 同一事务
    内保守拒绝（stale/resolved → 可见终态；paused/unknown → 不写等待/保留）。
    绝无网络、子进程或外部回调。
    """

    def __init__(self, store: "IncidentStore"):
        self._store = store

    def validate(self, conn, item: dict) -> dict:
        binding = _binding_from_flat(item)
        if binding is None:
            return _verdict(inc.VERDICT_UNKNOWN, "source-binding-missing")
        # 绑定必须与 outbox 行的 project 一致（跨项目篡改直接拒绝）。
        if isinstance(item, dict) and item.get("project") is not None \
                and item.get("project") != binding.get("source_project"):
            return _verdict(inc.VERDICT_STALE, "project-mismatch")
        if item.get("incident_type") != binding.get("source_incident_type"):
            return _verdict(inc.VERDICT_STALE, "incident-type-mismatch")
        try:
            return self._store.recheck_binding(conn, binding)
        except (sqlite3.Error, IncidentStateError, LedgerError, TargetGateStateError,
                CoordinatorControlStateError):
            return _verdict(inc.VERDICT_UNKNOWN, "recheck-read-failed")

    def __call__(self, conn, item: dict) -> dict:
        return self.validate(conn, item)


def _binding_from_flat(item: Any) -> dict | None:
    if not isinstance(item, dict):
        return None
    if isinstance(item.get("source_binding"), dict):
        return item["source_binding"]
    if any(item.get(k) is not None for k in inc.BINDING_REQUIRED_KEYS):
        binding = {k: item.get(k) for k in inc.BINDING_REQUIRED_KEYS}
        for k in inc.BINDING_OPTIONAL_KEYS:
            binding[k] = item.get(k)
        if all(_nk(binding.get(k)) for k in inc.BINDING_REQUIRED_KEYS):
            return binding
    return None


# ---- 存储 --------------------------------------------------------------------
class IncidentStore:
    """meta 命名空间持久 incident 存储 + 有界台账扫描（只读 SELECT）。"""

    def __init__(self, conn: sqlite3.Connection, clock: Any, *,
                 base: Any = None):
        self.conn = conn
        self._clock = clock
        self._base = base  # 可选 Ledger（provider_states 等复用）

    # ---- 基础设施 ---------------------------------------------------------
    def _now_ms(self) -> int:
        return int(self._clock.now_ms())

    def _read_schema(self) -> None:
        raw = meta_get(self.conn, INCIDENT_SCHEMA_KEY)
        if raw is None:
            return
        try:
            current = int(raw)
        except (TypeError, ValueError) as exc:
            raise IncidentStateError(
                f"incident schema 记录损坏：{raw!r}") from exc
        if current != INCIDENT_SCHEMA:
            raise IncidentStateError(
                f"incident schema 不兼容：{current} != {INCIDENT_SCHEMA}")

    def _ensure_schema(self) -> None:
        self._read_schema()
        if meta_get(self.conn, INCIDENT_SCHEMA_KEY) is None:
            meta_set(self.conn, INCIDENT_SCHEMA_KEY, str(INCIDENT_SCHEMA))

    def get_cursor(self, kind: str) -> str | None:
        if not _nk(kind):
            raise IncidentStateError("游标 kind 必须是非空字符串")
        value = meta_get(self.conn, _cursor_key(kind))
        return value if isinstance(value, str) and value else None

    def set_cursor(self, kind: str, value: str | None) -> None:
        if not _nk(kind):
            raise IncidentStateError("游标 kind 必须是非空字符串")
        if value is None or not _nk(value):
            meta_set(self.conn, _cursor_key(kind), "")
            return
        meta_set(self.conn, _cursor_key(kind), value)

    # ---- 控制面读取（同一 conn，纯本地） -----------------------------------
    @staticmethod
    def _project_status(conn: sqlite3.Connection, clock: Any,
                         project: str) -> str | None:
        control = CoordinatorControlLedger(conn, clock)
        snap = control.get_project(project)
        if not isinstance(snap, dict) or snap.get("status") not in ("running", "paused"):
            raise IncidentStateError("project-control-unreadable")
        return snap["status"]

    @staticmethod
    def _session_blocked(conn: sqlite3.Connection, clock: Any,
                         session_id: str | None) -> bool:
        if not session_id:
            return False
        control = CoordinatorControlLedger(conn, clock)
        snap = control.get_session(session_id)
        if not isinstance(snap, dict):
            raise IncidentStateError("session-control-unreadable")
        gate = TargetGate(conn, clock).get(session_id)
        return (snap.get("hold") is not None or snap.get("barrier") is not None
                or bool(gate and gate["state"] in ("blocked", "probe-ready", "probe-outstanding")))

    @staticmethod
    def _provider_blocked(conn: sqlite3.Connection, model: str | None,
                          now: int) -> bool:
        raw = meta_get(conn, "provider_state")
        if raw is None:
            return False
        try:
            data = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise IncidentStateError("provider-state-unreadable") from exc
        if not isinstance(data, dict):
            raise IncidentStateError("provider-state-invalid")
        if not model:
            return False
        if model not in data:
            return False
        state = data[model]
        if not isinstance(state, dict):
            raise IncidentStateError("provider-model-state-invalid")
        until = state.get("until")
        if type(until) is not int or not 0 <= until <= MAX_CLOCK_MS:
            raise IncidentStateError("provider-deadline-invalid")
        return until > now

    @staticmethod
    def _coordinator_session(conn: sqlite3.Connection,
                             project: str,
                             task_session: str | None) -> str | None:
        row = conn.execute(
            "SELECT holder_session FROM coordinator_leases WHERE project=?",
            (project,)).fetchone()
        session = row["holder_session"] if row else task_session
        if session is not None and not _nk(session):
            raise IncidentStateError("coordinator-session-invalid")
        return session

    def _actual_context(self, conn, task: dict) -> tuple[str | None, str | None, dict | None]:
        """路由与模型只取真实台账，不以调用方可删改的binding提示作为门禁依据。"""
        if not _nk(task.get("project")) or task.get("status") not in TASK_STATUSES:
            raise IncidentStateError("task-state-invalid")
        session = self._coordinator_session(conn, task["project"], task.get("coordinator_session"))
        attempt = None
        aid = task.get("current_attempt_id")
        if aid is not None:
            if not _nk(aid):
                raise IncidentStateError("current-attempt-invalid")
            row = conn.execute("SELECT * FROM attempts WHERE id=?", (aid,)).fetchone()
            if row is None:
                raise IncidentStateError("current-attempt-missing")
            attempt = dict(row)
            if (attempt["task_id"] != task["id"] or type(attempt["generation"]) is not int
                    or attempt["generation"] < 1 or attempt["status"] not in ATTEMPT_STATUSES):
                raise IncidentStateError("attempt-attribution-invalid")
            try:
                attempt["metadata"] = json.loads(attempt["metadata"])
            except (TypeError, ValueError) as exc:
                raise IncidentStateError("attempt-metadata-invalid") from exc
            if not isinstance(attempt["metadata"], dict):
                raise IncidentStateError("attempt-metadata-invalid")
            result = attempt["metadata"].get("result")
            if (result is not None and not isinstance(result, dict)
                    or isinstance(result, dict) and result.get("version") is not None
                    and not _nk(result["version"])):
                raise IncidentStateError("attempt-result-invalid")
        model = attempt.get("model") if attempt else None
        if model is not None and not _nk(model):
            raise IncidentStateError("attempt-model-invalid")
        return session, model, attempt

    @staticmethod
    def _responsibility_attribution(cur: dict, task: dict, attempt: dict | None) -> dict | None:
        if cur["project"] != task["project"] or cur["task_id"] != task["id"]:
            return _verdict(inc.VERDICT_STALE, "task-project-changed")
        if cur.get("attempt_id") is None and all(cur.get(k) is None for k in (
                "generation", "result_version", "successor_id")):
            return (_verdict(inc.VERDICT_STALE, "new-attempt-unbound")
                    if attempt else None)
        for key in ("attempt_id", "result_version"):
            if cur.get(key) is not None and not _nk(cur[key]):
                return _verdict(inc.VERDICT_UNKNOWN, "responsibility-attribution-invalid")
        if cur.get("generation") is not None and (
                type(cur["generation"]) is not int or cur["generation"] < 1):
            return _verdict(inc.VERDICT_UNKNOWN, "responsibility-generation-invalid")
        result = (attempt or {}).get("metadata", {}).get("result")
        if result is not None and not isinstance(result, dict):
            return _verdict(inc.VERDICT_UNKNOWN, "result-metadata-invalid")
        version = (result or {}).get("version")
        if version is not None and not _nk(version):
            return _verdict(inc.VERDICT_UNKNOWN, "result-version-invalid")
        # 与责任台账同口径：输入型责任未绑定结果时不因正常collect变历史。
        if _nk(cur.get("input_id")) and cur.get("result_version") is None:
            version = None
        decision = duty.staleness(cur, current={
            "current_attempt_id": task.get("current_attempt_id"),
            "generation": (attempt or {}).get("generation"), "result_version": version})
        if decision["status"] == "fresh":
            return None
        return _verdict(inc.VERDICT_STALE if decision["status"] == "stale"
                        else inc.VERDICT_UNKNOWN, decision["code"])

    def _normal_blocked(self, conn: sqlite3.Connection, *, project: str,
                        session: str | None, model: str | None) -> bool:
        """本项目/本会话/本模型维度上的“正常”阻断（暂停/回合 hold/provider 冷却）。"""
        if self._project_status(conn, self._clock, project) == "paused":
            return True
        if self._session_blocked(conn, self._clock, session):
            return True
        if self._provider_blocked(conn, model, self._now_ms()):
            return True
        return False

    # ---- 只读 -------------------------------------------------------------
    def get_incident(self, incident_id: str) -> dict | None:
        self._read_schema()
        rec = _load(self.conn, incident_id)
        return _snapshot(rec) if rec else None

    def list_incidents(self, *, project: str | None = None,
                       state: str | None = None, limit: int = 100) -> list[dict]:
        """兼容列表；完整度和续页信息见 inspect_incidents，不写运行时游标。"""
        return self._filtered_scan(project=project, state=state, limit=limit)

    def _filtered_scan(self, *, project: str | None, state: str | None,
                       limit: int) -> list[dict]:
        return self.inspect_incidents(project=project, state=state, limit=limit)["incidents"]

    def inspect_incidents(self, *, project: str | None = None, state: str | None = None,
                          limit: int = 100, after_id: str | None = None) -> dict:
        """先按scope过滤再分页；总数为持久候选总数，不冒充200条样本的长度。

        损坏/不可归属记录明确损失覆盖，不能静默等同“没有待办”。SQL聚合不把
        全库记录搬入Python；所有查询共用只读WAL快照，参数不推进公平扫描游标。
        """
        lim, after = _strict_limit(limit), _strict_after(after_id)
        if project is not None and not _nk(project):
            raise ValueError("project 必须为非空字符串或 None")
        if state is not None and state not in inc.INCIDENT_STATES:
            raise ValueError("state 非法")
        where = "key LIKE ?"
        args: list[Any] = [INCIDENT_KEY_PREFIX + "%"]
        for field, value in (("project", project), ("state", state)):
            if value is not None:
                where += f" AND CASE WHEN json_valid(value) THEN json_extract(value,'$.{field}') END=?"
                args.append(value)
        with read_snapshot(self.conn):
            self._read_schema()
            total = self.conn.execute(f"SELECT COUNT(*) FROM meta WHERE {where}", args).fetchone()[0]
            unattributed = self.conn.execute(
                "SELECT COUNT(*) FROM meta WHERE key LIKE ? AND CASE WHEN json_valid(value) "
                "THEN json_type(value,'$.project') IS NOT 'text' ELSE 1 END",
                (INCIDENT_KEY_PREFIX + "%",)).fetchone()[0]
            page_where, page_args = where, list(args)
            if after is not None:
                page_where += " AND key > ?"
                page_args.append(_item_key(after))
            rows = self.conn.execute(
                f"SELECT key FROM meta WHERE {page_where} ORDER BY key LIMIT ?", (*page_args, lim+1)
            ).fetchall()
            more = len(rows) > lim
            page = rows[:lim]
            incidents, invalid = [], 0
            for row in page:
                try:
                    rec = _load(self.conn, row["key"][len(INCIDENT_KEY_PREFIX):])
                except IncidentStateError:
                    invalid += 1
                    continue
                if rec is not None:
                    incidents.append(_snapshot(rec))
            return {"incidents": incidents, "total": total,
                    "coverage": {"complete": not more and not invalid and not unattributed,
                                 "scope_complete": after is None and not more and not invalid and not unattributed,
                                 "next_after_id": page[-1]["key"][len(INCIDENT_KEY_PREFIX):] if more else None,
                                 "invalid_records": invalid, "unattributed_records": unattributed,
                                 "observed": len(incidents), "total_is_exact": project is None or not unattributed}}

    def list_incidents_page(self, *, state: str | None, after_id: str | None,
                            limit: int) -> tuple[list[dict], str | None, bool]:
        """恢复扫描专用 keyset 分页（不写游标，游标由 service 持久）。

        返回 ``(incidents, next_after, truncated)``。按 ``incident:item:{id}``
        键序翻页；next_after 为尾项 id（页满时），否则 None（回绕）。
        """
        self._read_schema()
        lim = _strict_limit(limit)
        after = _strict_after(after_id)
        sql = "SELECT key FROM meta WHERE key LIKE ?"
        args: list[Any] = [INCIDENT_KEY_PREFIX + "%"]
        if after:
            sql += " AND key > ?"
            args.append(_item_key(after))
        sql += " ORDER BY key ASC LIMIT ?"
        args.append(lim + 1)
        rows = self.conn.execute(sql, args).fetchall()
        truncated = len(rows) > lim
        rows = rows[:lim]
        out = []
        last = None
        for row in rows:
            key = row["key"]
            incident_id = key[len(INCIDENT_KEY_PREFIX):]
            if not incident_id:
                continue
            last = incident_id
            try:
                rec = _load(self.conn, incident_id)
            except IncidentStateError:
                # 保留可见未知占位并推进keyset，既不恢复坏记录也不饿死尾项。
                out.append({"incident_id": incident_id, "source_binding": None})
                continue
            if rec is None:
                continue
            if state is not None and rec.get("state") != state:
                continue
            out.append(_snapshot(rec))
        next_after = last if (truncated and last) else None
        return out, next_after, truncated

    def count_open(self, *, project: str | None = None,
                   state: str | None = None) -> int:
        return self.inspect_incidents(project=project, state=state, limit=1)["total"]

    # ---- 写入（短事务内重读 + CAS） --------------------------------------
    def upsert(self, *, incident_id: str, source: dict, now: int,
               recipient_scope: str = "", summary: dict | None = None,
               metadata: dict | None = None) -> dict:
        """创建/观察/升级/重开 incident（合并决策在 domain.decide）。"""
        src = inc.normalize_source(source)
        if not _nk(incident_id):
            raise IncidentStateError("incident_id 必须是非空字符串")
        observed_at = _valid_ms(src.get("observed_at")) or now
        now_ms = _valid_ms(now)
        if now_ms is None:
            raise IncidentStateError(f"now 非法：{now!r}")
        sig = inc.source_signature(src)
        with transaction(self.conn):
            self._read_schema()
            rec = _load(self.conn, incident_id)
            verdict = self.recheck_binding(self.conn, inc.build_binding(src))
            if verdict["state"] not in (inc.VERDICT_ACTIVE, inc.VERDICT_PAUSED):
                return {"action": "unknown" if verdict["state"] == inc.VERDICT_UNKNOWN else "skipped",
                        "created": False, "changed": False, "reason": verdict["reason"],
                        "incident": _snapshot(rec) if rec else None}
            self._ensure_schema()
            if rec is None:
                decision = inc.decide(src, None, now=now_ms)
                rec = {
                    "schema": INCIDENT_SCHEMA,
                    "incident_id": incident_id,
                    "project": src["project"],
                    "incident_type": src["incident_type"],
                    "source_kind": src["source_kind"],
                    "source_id": src["source_id"],
                    "task_id": src["task_id"],
                    "condition_key": src["condition_key"],
                    "condition_state": src["condition_state"],
                    "condition_kind": src["condition_kind"],
                    "source_signature": sig,
                    "source_binding": inc.build_binding(src),
                    "condition_version": decision["condition_version"],
                    "escalation_level": src["escalation_level"],
                    "state": inc.INCIDENT_OPEN,
                    "owner": src["owner"],
                    "next_action": src["next_action"],
                    "blocked_reason": src["blocked_reason"],
                    "impact": src["impact"],
                    "deadline": src.get("deadline"),
                    "recipient_scope": str(recipient_scope or ""),
                    "first_seen_at": observed_at,
                    "last_observed_at": observed_at,
                    "revision": 1,
                    "recovered_at": None,
                    "recovered_evidence": "",
                    "summary": dict(summary) if isinstance(summary, dict) else {},
                    "metadata": dict(metadata) if isinstance(metadata, dict) else {},
                    "created_at": now_ms,
                    "updated_at": now_ms,
                }
                _store(self.conn, rec)
                return {"action": "create", "created": True, "changed": True,
                        "condition_version": rec["condition_version"],
                        "escalation_level": rec["escalation_level"],
                        "reason": "first-observation", "incident": _snapshot(rec)}
            decision = inc.decide(src, rec, now=now_ms)
            action = decision["action"]
            changed = False
            if action == "reopen":
                # 重开：以新版本打开，清恢复痕迹，刷新签名/绑定（不复用旧 event_key）。
                rec["state"] = inc.INCIDENT_OPEN
                rec["recovered_at"] = None
                rec["recovered_evidence"] = ""
                changed = True
            if decision.get("signature_changed", False):
                rec["source_signature"] = sig
                rec["source_binding"] = inc.build_binding(src)
                for f in ("condition_key", "task_id", "deadline",
                          "condition_state"):
                    if src.get(f) is not None:
                        rec[f] = src[f]
                changed = True
            if decision["condition_version"] != rec.get("condition_version"):
                rec["condition_version"] = decision["condition_version"]
                changed = True
            if decision["escalation_level"] != rec.get("escalation_level"):
                rec["escalation_level"] = decision["escalation_level"]
                changed = True
            for f in ("owner", "next_action", "blocked_reason", "impact",
                      "condition_kind"):
                value = src.get(f)
                if value and rec.get(f) != value:
                    rec[f] = value
                    changed = True
            if action == "noop" and not changed:
                return {"action": "noop", "created": False, "changed": False,
                        "condition_version": rec["condition_version"],
                        "escalation_level": rec["escalation_level"],
                        "reason": decision["reason"],
                        "incident": _snapshot(rec)}
            if observed_at > rec.get("last_observed_at", 0):
                rec["last_observed_at"] = observed_at
                changed = True
            if isinstance(summary, dict) and summary:
                rec["summary"] = dict(summary)
                changed = True
            if changed:
                rec["revision"] += 1
                rec["updated_at"] = max(now_ms, int(rec.get("updated_at") or 0))
                _store(self.conn, rec)
            return {"action": action, "created": False, "changed": changed,
                    "condition_version": rec["condition_version"],
                    "escalation_level": rec["escalation_level"],
                    "reason": decision["reason"],
                    "incident": _snapshot(rec)}

    def mark_suppressed(self, *, incident_id: str, expected_revision: int,
                        reason: str = "", now: int) -> dict:
        """源仍存在但绑定失效（stale）→ 抑制（不是 recovered；不重置签名）。"""
        if not _nk(incident_id):
            raise IncidentStateError("incident_id 必须是非空字符串")
        now_ms = _valid_ms(now)
        if now_ms is None:
            raise IncidentStateError(f"now 非法：{now!r}")
        with transaction(self.conn):
            self._read_schema()
            rec = _load(self.conn, incident_id)
            if rec is None:
                return {"applied": False, "reason": "missing", "incident": None}
            if rec["state"] != inc.INCIDENT_OPEN:
                return {"applied": False, "reason": "not-open",
                        "incident": _snapshot(rec)}
            if not _is_int_not_bool(expected_revision) \
                    or rec["revision"] != expected_revision:
                return {"applied": False, "reason": "revision-changed",
                        "incident": _snapshot(rec)}
            verdict = self.recheck_binding(self.conn, rec["source_binding"])
            if verdict["state"] != inc.VERDICT_STALE:
                return {"applied": False, "reason": "source-not-stale",
                        "source_state": verdict["state"], "incident": _snapshot(rec)}
            self._ensure_schema()
            rec["state"] = inc.INCIDENT_SUPPRESSED
            rec["recovered_evidence"] = str(reason or "")[:200]
            rec["revision"] += 1
            rec["updated_at"] = now_ms
            _store(self.conn, rec)
            return {"applied": True, "reason": "suppressed",
                    "incident": _snapshot(rec)}

    def recover_incident(self, *, incident_id: str, expected_revision: int,
                         evidence: str = "", now: int) -> dict:
        """恢复 incident（至多一次；revision CAS；失败返回拒绝原因）。"""
        if not _nk(incident_id):
            raise IncidentStateError("incident_id 必须是非空字符串")
        now_ms = _valid_ms(now)
        if now_ms is None:
            raise IncidentStateError(f"now 非法：{now!r}")
        with transaction(self.conn):
            self._read_schema()
            rec = _load(self.conn, incident_id)
            if rec is None:
                return {"applied": False, "reason": "missing", "incident": None}
            if rec["state"] == inc.INCIDENT_RECOVERED:
                return {"applied": False, "reason": "already-recovered",
                        "incident": _snapshot(rec)}
            if rec["state"] != inc.INCIDENT_OPEN:
                return {"applied": False, "reason": "not-recoverable",
                        "incident": _snapshot(rec)}
            if not _is_int_not_bool(expected_revision) \
                    or rec["revision"] != expected_revision:
                return {"applied": False, "reason": "revision-changed",
                        "incident": _snapshot(rec)}
            # 外层sweep只是候选观察；必须在持有写锁的同一事务内重验，避免
            # “曾经resolved”覆盖新阻塞。失败不写状态或伪造恢复回执。
            verdict = self.recheck_binding(self.conn, rec["source_binding"])
            if verdict["state"] != inc.VERDICT_RESOLVED:
                return {"applied": False, "reason": "source-not-resolved",
                        "source_state": verdict["state"], "incident": _snapshot(rec)}
            self._ensure_schema()
            rec["state"] = inc.INCIDENT_RECOVERED
            rec["recovered_at"] = now_ms
            rec["recovered_evidence"] = str(evidence or "")[:200]
            rec["revision"] += 1
            rec["updated_at"] = now_ms
            _store(self.conn, rec)
            return {"applied": True, "reason": "recovered",
                    "incident": _snapshot(rec)}

    def make_source_validator(self) -> IncidentSourceValidator:
        return IncidentSourceValidator(self)

    # ---- 源重验（同一 conn 内只读；禁止网络/子进程） ----------------------
    def recheck_binding(self, conn: sqlite3.Connection,
                        binding: dict) -> dict:
        """源重验五态判定（active/resolved/stale/paused/unknown）。

        - ``responsibility``：行存在（missing→unknown）、project/task 归属一致、
          未关闭；任务收口（accepted/cancelled/closed）→ resolved（旧责任不再
          发信）；项目暂停/会话 hold/provider 冷却 → paused；状态/条件键/
          签名（含换代/期限变化）不符 → stale；全部吻合 → active。
        - ``task``：存在（missing→unknown）、未收口；暂停 → paused；no_progress
          已解除（attempt/责任/通知已出现）→ resolved；读不到 → unknown；
          状态/签名不符 → stale；仍 no_progress → active。
        - ``incident``（恢复事件）：incident 记录存在且仍 recovered，并重验其
          底层 source 仍为 definitely-resolved（不允许只信本地 state）。
        """
        if not isinstance(binding, dict):
            return _verdict(inc.VERDICT_UNKNOWN, "invalid-binding")
        kind = binding.get("source_kind")
        sid = binding.get("source_id")
        sig = binding.get("source_version")
        ckey = binding.get("source_condition_key")
        state = binding.get("source_state")
        project = binding.get("source_project")
        task_id = binding.get("source_task_id")
        session = binding.get("source_session")
        model = binding.get("source_model")
        if (kind not in inc.SOURCE_KINDS
                or not all(_nk(binding.get(k)) for k in inc.BINDING_REQUIRED_KEYS)
                or any(binding.get(k) is not None and not _nk(binding[k])
                       for k in inc.BINDING_OPTIONAL_KEYS)
                or binding["source_incident_type"] not in inc.INCIDENT_TYPES):
            return _verdict(inc.VERDICT_UNKNOWN, "binding-fields-invalid")
        if (kind == inc.SOURCE_KIND_RESPONSIBILITY and binding["source_incident_type"] not in (
                inc.INCIDENT_WAITING_HUMAN, inc.INCIDENT_RESPONSIBILITY_DUE)
                or kind == inc.SOURCE_KIND_TASK and binding["source_incident_type"] != inc.INCIDENT_NO_PROGRESS):
            return _verdict(inc.VERDICT_STALE, "incident-kind-mismatch")
        try:
            if kind == inc.SOURCE_KIND_RESPONSIBILITY:
                current = self._resp_dict(sid, conn)
                if current is None:
                    return _verdict(inc.VERDICT_UNKNOWN, "responsibility-missing")
                if current["project"] != project:
                    return _verdict(inc.VERDICT_STALE, "project-changed")
                if current["task_id"] != task_id:
                    return _verdict(inc.VERDICT_STALE, "attribution-changed")
                if current["condition_key"] != ckey:
                    return _verdict(inc.VERDICT_STALE, "condition-key-changed")
                trow = conn.execute(
                    "SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
                if trow is None:
                    return _verdict(inc.VERDICT_UNKNOWN, "task-missing")
                task = dict(trow)
                if task["project"] != project:
                    return _verdict(inc.VERDICT_STALE, "task-project-changed")
                actual_session, actual_model, attempt = self._actual_context(conn, task)
                # 删除或伪造可选提示不能删除真实台账的hold/cooldown。
                if self._normal_blocked(conn, project=project,
                                        session=actual_session, model=actual_model) or task["status"] == "paused":
                    return _verdict(inc.VERDICT_PAUSED, "normal-pause-or-cooldown")
                if current["closed_at"] is not None:
                    return _verdict(inc.VERDICT_RESOLVED, "responsibility-closed")
                if task["closed_at"] is not None or task["status"] in TASK_TERMINAL:
                    return _verdict(inc.VERDICT_RESOLVED, "task-settled")
                if current["state"] != state:
                    return _verdict(inc.VERDICT_STALE, "responsibility-state-changed")
                if current["condition_key"] != ckey:
                    return _verdict(inc.VERDICT_STALE, "condition-key-changed")
                incident_type = binding["source_incident_type"]
                if incident_type == inc.INCIDENT_WAITING_HUMAN:
                    if current["state"] != duty.STATE_WAITING_HUMAN:
                        return _verdict(inc.VERDICT_STALE, "not-waiting-human")
                elif incident_type == inc.INCIDENT_RESPONSIBILITY_DUE:
                    if not duty.deadline_decision(current, now_ms=self._now_ms())["exhausted"]:
                        return _verdict(inc.VERDICT_STALE, "no-longer-exhausted")
                else:
                    return _verdict(inc.VERDICT_STALE, "incident-kind-mismatch")
                sig_now = inc.source_signature(self._resp_source(
                    current, task_status=task["status"], now=self._now_ms(), verified=True,
                    incident_type=incident_type, source_session=actual_session,
                    source_model=actual_model))
                if (session is not None and session != actual_session
                        or model is not None and model != actual_model):
                    return _verdict(inc.VERDICT_STALE, "source-route-changed")
                if sig_now != sig:
                    return _verdict(inc.VERDICT_STALE, "source-version-changed")
                attribution = self._responsibility_attribution(current, task, attempt)
                if attribution is not None:
                    return attribution
                return _verdict(inc.VERDICT_ACTIVE, "verified")
            if kind == inc.SOURCE_KIND_TASK:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE id=?", (sid,)).fetchone()
                if row is None:
                    return _verdict(inc.VERDICT_UNKNOWN, "task-missing")
                task = _normalized_task(row)
                if task.get("project") != project:
                    return _verdict(inc.VERDICT_STALE, "project-changed")
                if sid != task_id or binding["source_incident_type"] != inc.INCIDENT_NO_PROGRESS:
                    return _verdict(inc.VERDICT_STALE, "task-attribution-changed")
                actual_session, actual_model, attempt = self._actual_context(conn, task)
                if self._normal_blocked(conn, project=project,
                                        session=actual_session, model=actual_model):
                    return _verdict(inc.VERDICT_PAUSED, "normal-pause-or-cooldown")
                if task["status"] in TASK_TERMINAL \
                        or task.get("closed_at") is not None:
                    return _verdict(inc.VERDICT_RESOLVED, "task-settled")
                if task["status"] == "paused":
                    return _verdict(inc.VERDICT_PAUSED, "task-paused")
                still, reason = self._task_no_progress_state(conn, task)
                if not still:
                    if reason in _RESOLVED_NP_REASONS:
                        return _verdict(inc.VERDICT_RESOLVED, reason)
                    if reason in _PAUSED_NP_REASONS:
                        return _verdict(inc.VERDICT_PAUSED, reason)
                    if reason in _UNKNOWN_NP_REASONS:
                        return _verdict(inc.VERDICT_UNKNOWN, reason)
                    return _verdict(inc.VERDICT_STALE, reason or "not-no-progress")
                if state and task["status"] != state:
                    return _verdict(inc.VERDICT_STALE, "task-state-changed")
                sig_now = inc.source_signature(self._task_source(
                    task, source_session=actual_session, source_model=actual_model, attempt=attempt))
                if (session is not None and session != actual_session
                        or model is not None and model != actual_model):
                    return _verdict(inc.VERDICT_STALE, "source-route-changed")
                if not sig_now or sig_now != sig:
                    return _verdict(inc.VERDICT_STALE, "source-version-changed")
                return _verdict(inc.VERDICT_ACTIVE, "verified")
            if kind == inc.SOURCE_KIND_INCIDENT:
                rec = _load(conn, sid)
                if rec is None:
                    return _verdict(inc.VERDICT_UNKNOWN, "incident-missing")
                if rec["state"] != inc.INCIDENT_RECOVERED:
                    return _verdict(inc.VERDICT_STALE, "incident-not-recovered")
                expected = inc.recovery_binding(rec, recovered_at=rec["recovered_at"])
                if any(binding.get(k) != expected.get(k) for k in (
                        *inc.BINDING_REQUIRED_KEYS, *inc.BINDING_OPTIONAL_KEYS)):
                    return _verdict(inc.VERDICT_STALE, "recovery-episode-changed")
                underlying = rec.get("source_binding")
                if not isinstance(underlying, dict):
                    return _verdict(inc.VERDICT_UNKNOWN, "underlying-binding-missing")
                inner = self.recheck_binding(conn, underlying)
                if inner["state"] == inc.VERDICT_RESOLVED:
                    return _verdict(inc.VERDICT_ACTIVE, "still-resolved")
                if inner["state"] == inc.VERDICT_PAUSED:
                    return _verdict(inc.VERDICT_PAUSED, "paused")
                if inner["state"] in (inc.VERDICT_ACTIVE, inc.VERDICT_STALE):
                    return _verdict(inc.VERDICT_STALE, "no-longer-resolved")
                return _verdict(inc.VERDICT_UNKNOWN, "underlying-unknown")
        except (sqlite3.Error, IncidentStateError, LedgerError, TargetGateStateError,
                CoordinatorControlStateError, ValueError, TypeError):
            return _verdict(inc.VERDICT_UNKNOWN, "recheck-read-failed")
        return _verdict(inc.VERDICT_UNKNOWN, "unsupported-source-kind")

    # ---- 有界公平扫描（keyset 游标；next_after/truncated 诚实报告） -------
    def _scan_responsibility(self, cur: dict, *, now: int, incident_type: str,
                             exhausted_reason: str | None = None) -> tuple[dict | None, str | None]:
        try:
            task = self._task_row(cur["task_id"])
            if task is None:
                return None, "task-read-failed"
            if task["project"] != cur["project"]:
                return None, "task-project-changed"
            if task["status"] in TASK_TERMINAL or task.get("closed_at") is not None:
                return None, "task-settled"
            session, model, attempt = self._actual_context(self.conn, task)
            attribution = self._responsibility_attribution(cur, task, attempt)
            if attribution is not None:
                return None, attribution["reason"]
            blocked = (task["status"] == "paused" or self._normal_blocked(
                self.conn, project=cur["project"], session=session, model=model))
            return self._resp_source(
                cur, task_status=task["status"], now=now, incident_type=incident_type,
                exhausted_reason=exhausted_reason,
                escalation_override=2 if exhausted_reason else None,
                condition_kind=CONDITION_PAUSE if blocked else CONDITION_HUMAN,
                source_session=session, source_model=model), None
        except (sqlite3.Error, IncidentStateError, LedgerError, TargetGateStateError,
                CoordinatorControlStateError, TypeError, ValueError):
            return None, "source-context-unknown"

    def list_waiting_human_sources_page(self, *, limit: int, after_id: str | None = None
                                        ) -> tuple[list[tuple[dict | None, str | None]],
                                              str | None, bool]:
        lim = _strict_limit(limit)
        after = _strict_after(after_id)
        sql = ("SELECT * FROM responsibilities WHERE closed_at IS NULL "
               "AND state='waiting_human'")
        args: list[Any] = []
        if after:
            sql += " AND id > ?"
            args.append(after)
        sql += " ORDER BY id ASC LIMIT ?"
        args.append(lim + 1)
        rows = self.conn.execute(sql, args).fetchall()
        truncated = len(rows) > lim
        rows = rows[:lim]
        out = []
        last = None
        for row in rows:
            last = row["id"]
            cur = self._resp_dict(row["id"])
            if cur is None:
                out.append((None, "responsibility-read-failed"))
                continue
            out.append(self._scan_responsibility(
                cur, now=self._now_ms(), incident_type=inc.INCIDENT_WAITING_HUMAN))
        next_after = last if (truncated and last) else None
        return out, next_after, truncated

    def list_due_sources_page(self, *, now: int, limit: int, after_id: str | None = None
                              ) -> tuple[list[tuple[dict | None, str | None]],
                                    str | None, bool]:
        lim = _strict_limit(limit)
        after = _strict_after(after_id)
        ts = _valid_ms(now)
        if ts is None:
            raise IncidentStateError(f"now 非法：{now!r}")
        sql = ("SELECT * FROM responsibilities WHERE closed_at IS NULL "
               "AND next_check_at <= ? AND state IN ('waiting_dependency',"
               "'waiting_input','waiting_execution','handling','awaiting_review')")
        args: list[Any] = [ts]
        if after:
            sql += " AND id > ?"
            args.append(after)
        sql += " ORDER BY id ASC LIMIT ?"
        args.append(lim + 1)
        rows = self.conn.execute(sql, args).fetchall()
        truncated = len(rows) > lim
        rows = rows[:lim]
        out = []
        last = None
        for row in rows:
            last = row["id"]
            cur = self._resp_dict(row["id"])
            if cur is None:
                out.append((None, "responsibility-read-failed"))
                continue
            decision = duty.deadline_decision(cur, now_ms=ts)
            if not decision.get("exhausted"):
                out.append((None, "not-exhausted"))
                continue
            out.append(self._scan_responsibility(
                cur, now=ts, incident_type=inc.INCIDENT_RESPONSIBILITY_DUE,
                exhausted_reason=decision.get("code")))
        next_after = last if (truncated and last) else None
        return out, next_after, truncated

    def list_no_progress_sources_page(self, *, limit: int, after_id: str | None = None
                                      ) -> tuple[list[tuple[dict | None, str | None]],
                                            str | None, bool]:
        lim = _strict_limit(limit)
        after = _strict_after(after_id)
        sql = ("SELECT * FROM tasks WHERE status NOT IN ('accepted','cancelled') "
               "AND closed_at IS NULL")
        args: list[Any] = []
        if after:
            sql += " AND id > ?"
            args.append(after)
        sql += " ORDER BY id ASC LIMIT ?"
        args.append(lim + 1)
        rows = self.conn.execute(sql, args).fetchall()
        truncated = len(rows) > lim
        rows = rows[:lim]
        out = []
        last = None
        for row in rows:
            last = row["id"]
            task = _normalized_task(row)
            if task["status"] not in TASK_STATUSES:
                out.append((None, "task-status-unknown"))
                continue
            try:
                session, model, attempt = self._actual_context(self.conn, task)
                blocked = self._normal_blocked(
                    self.conn, project=task["project"], session=session, model=model)
            except (sqlite3.Error, IncidentStateError, LedgerError, TargetGateStateError,
                    CoordinatorControlStateError):
                out.append((None, "source-context-unknown"))
                continue
            if blocked:
                out.append((None, "normal-pause-or-cooldown"))
                continue
            still, reason = self._task_no_progress_state(self.conn, task)
            if not still:
                out.append((None, reason))
                continue
            source = self._task_source(task, source_session=session, source_model=model, attempt=attempt)
            out.append((source, None))
        next_after = last if (truncated and last) else None
        return out, next_after, truncated

    def list_waiting_human_sources(self, *, limit: int
                                   ) -> list[tuple[dict | None, str | None]]:
        """兼容头页包装（keyset 版本见 list_waiting_human_sources_page）。"""
        entries, _next, _trunc = self.list_waiting_human_sources_page(limit=limit)
        return entries

    def list_due_sources(self, *, now: int, limit: int
                         ) -> list[tuple[dict | None, str | None]]:
        """兼容头页包装（keyset 版本见 list_due_sources_page）。"""
        entries, _next, _trunc = self.list_due_sources_page(now=now, limit=limit)
        return entries

    def list_no_progress_sources(self, *, limit: int
                                 ) -> list[tuple[dict | None, str | None]]:
        """兼容头页包装（keyset 版本见 list_no_progress_sources_page）。"""
        entries, _next, _trunc = self.list_no_progress_sources_page(limit=limit)
        return entries

    # ---- 内部：行读取 -------------------------------------------------------
    def _resp_dict(self, rid: str,
                   conn: sqlite3.Connection | None = None) -> dict | None:
        conn = conn or self.conn
        try:
            row = conn.execute(
                "SELECT * FROM responsibilities WHERE id=?", (rid,)).fetchone()
        except sqlite3.Error:
            return None
        if row is None:
            return None
        return _normalized_resp(row)

    def _task_row(self, task_id: str) -> dict | None:
        if not task_id:
            return None
        try:
            row = self.conn.execute(
                "SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        except sqlite3.Error:
            return None
        return _normalized_task(row) if row else None

    def _task_status(self, task_id: str) -> str | None:
        if not task_id:
            return None
        try:
            row = self.conn.execute(
                "SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
        except sqlite3.Error:
            return None
        return row["status"] if row else None

    def _attempt_model(self, attempt_id: str | None) -> str | None:
        if not attempt_id:
            return None
        try:
            row = self.conn.execute(
                "SELECT model FROM attempts WHERE id=?", (attempt_id,)).fetchone()
        except sqlite3.Error:
            return None
        return row["model"] if row and isinstance(row["model"], str) else None

    def _task_no_progress_state(self, conn: sqlite3.Connection,
                                task: dict) -> tuple[bool, str]:
        """确证 no_progress 的条件；读不到/缺行 → unknown（拒绝，不造 no_progress）。"""
        if task["status"] not in TASK_STATUSES:
            return False, "task-status-unknown"
        if task["status"] in TASK_TERMINAL or task.get("closed_at") is not None:
            return False, "task-settled"
        if task["status"] == "paused":
            return False, "task-paused"
        cur_attempt = task.get("current_attempt_id")
        if cur_attempt:
            try:
                att = conn.execute(
                    "SELECT status, site_released_at FROM attempts WHERE id=?",
                    (cur_attempt,)).fetchone()
            except sqlite3.Error:
                return False, "attempt-read-failed"
            if att is None:
                return False, "attempt-missing"
            if att["status"] not in ATTEMPT_TERMINAL \
                    and att["site_released_at"] is None:
                return False, "open-attempt"
        try:
            responsibility = conn.execute(
                "SELECT 1 FROM responsibilities WHERE task_id=? "
                "AND closed_at IS NULL LIMIT 1", (task["id"],)).fetchone()
        except sqlite3.Error:
            return False, "responsibilities-read-failed"
        if responsibility is not None:
            return False, "open-responsibility"
        try:
            pending = conn.execute(
                "SELECT COUNT(*) AS n FROM notifications "
                "WHERE task_id=? AND state IN ('pending','inflight','sent')",
                (task["id"],)).fetchone()["n"]
        except sqlite3.Error:
            return False, "notifications-read-failed"
        if pending:
            return False, "pending-notification"
        return True, "no-progress"

    def _resp_source(self, cur: dict, *, task_status: str | None, now: int,
                     incident_type: str = inc.INCIDENT_WAITING_HUMAN,
                     exhausted_reason: str | None = None,
                     escalation_override: int | None = None,
                     verified: bool = False,
                     condition_kind: str | None = None,
                     source_session: str | None = None,
                     source_model: str | None = None) -> dict:
        """责任行 → 归一化源快照（含归属/会话/模型与签名用字段）。"""
        deadline = cur.get("deadline")
        esc = 0
        if exhausted_reason:
            esc = 2
        elif isinstance(deadline, int) and now >= deadline:
            esc = 1
        if escalation_override is not None:
            esc = max(esc, escalation_override)
        if condition_kind is None:
            if verified:
                cond = CONDITION_HUMAN  # 重验路径：只关心状态/签名一致性
            elif task_status == "paused":
                cond = CONDITION_PAUSE
            elif task_status in TASK_STATUSES and task_status not in TASK_TERMINAL:
                cond = CONDITION_HUMAN
            else:
                cond = CONDITION_UNKNOWN
        else:
            cond = condition_kind
        reason = cur.get("blocked_reason") or ""
        if not reason:
            if exhausted_reason:
                reason = f"等待超限（{exhausted_reason}）：需人工决策，不无限顺延"
            elif incident_type == inc.INCIDENT_WAITING_HUMAN:
                reason = "责任等待人工处理（manual hold），不得自动解除"
            else:
                reason = "责任等待复核/处置"
        return {
            "source_kind": "responsibility",
            "source_id": cur["id"],
            "project": cur["project"],
            "incident_type": incident_type,
            "task_id": cur["task_id"] or "",
            "condition_key": cur["condition_key"] or cur["id"],
            "condition_state": cur["state"],
            "generation": cur.get("generation"),
            "result_version": cur.get("result_version"),
            "owner": cur.get("owner") or "manual:unassigned",
            "next_action": (cur.get("next_action")
                            or f"处理责任 {cur['id']}（owner={cur.get('owner')}）"),
            "blocked_reason": reason,
            "impact": "",
            "condition_kind": cond,
            "escalation_level": esc,
            "deadline": deadline,
            "first_evidence_at": cur.get("created_at") or now,
            "observed_at": now,
            "task_status": task_status,
            "responsibility_states": (["waiting_human"]
                                      if incident_type == inc.INCIDENT_WAITING_HUMAN
                                      else [cur["state"]]),
            "current_attempt_id": cur.get("attempt_id"),
            "source_project": cur["project"],
            "source_task_id": cur["task_id"] or "",
            "source_session": source_session,
            "source_model": source_model,
            "recovered": False,
        }

    def _task_source(self, task: dict, source_session: str | None = None,
                     source_model: str | None = None, attempt: dict | None = None) -> dict:
        """任务行 → no_progress 源快照（签名与 recheck 同一口径）。"""
        return {
            "source_kind": "task",
            "source_id": task["id"],
            "project": task["project"] or "",
            "incident_type": inc.INCIDENT_NO_PROGRESS,
            "task_id": task["id"],
            "condition_key": f"task-no-progress:{task['id']}",
            "condition_state": task["status"],
            "generation": (attempt or {}).get("generation"),
            "result_version": ((attempt or {}).get("metadata", {}).get("result") or {}).get("version"),
            "owner": (task.get("coordinator_session")
                      or "manual:unassigned"),
            "next_action": "登记缺失责任/推进责任；空通知不等于已处理（C21）",
            "blocked_reason": "已托管未完成工作确证无推进责任；不靠刷新心跳保活",
            "impact": "",
            "condition_kind": CONDITION_HUMAN,
            "escalation_level": 0,
            "deadline": None,
            "first_evidence_at": task.get("created_at") or 0,
            "observed_at": 0,  # 由服务端取 now 覆盖
            "task_status": task["status"],
            "responsibility_states": (),
            "current_attempt_id": task.get("current_attempt_id"),
            "source_project": task["project"] or "",
            "source_task_id": task["id"],
            "source_session": source_session,
            "source_model": source_model,
            "recovered": False,
        }


def _normalized_resp(row) -> dict:
    data = dict(row)
    data["evidence"] = json_loads(data.get("evidence"), {})
    if not isinstance(data["evidence"], dict):
        data["evidence"] = {}
    return data


def _normalized_task(row) -> dict:
    data = dict(row)
    for key in ("objective", "acceptance", "write_scope", "metadata"):
        if key in data:
            parsed = json_loads(data[key], None)
            data[key] = parsed if parsed is not None else data[key]
    return data


__all__ = [
    "INCIDENT_SCHEMA", "INCIDENT_SCHEMA_KEY", "INCIDENT_KEY_PREFIX",
    "IncidentStateError", "IncidentSourceValidator", "IncidentStore",
    "_strict_limit", "_binding_from_flat",
]
